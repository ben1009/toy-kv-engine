"""Freeze path-conditioned I/O latency modes before a fresh V7/V8 evaluation.

This is an observational diagnostic, not identification of controller state.
Capture instrumentation and the production engine are deliberately unchanged.
"""

import argparse
from bisect import bisect_left
from collections import Counter, defaultdict, deque
import hashlib
import json
import math
from pathlib import Path
import statistics
import time

import wal_extent_trace as ext
import wal_trace_regimes as trace


SCHEMA = "kv-engine.wal-path-calibration.v1"
POLICY = {
    "invalid_trace": "quarantine_entire_run_as_unknown", "window_samples": 16, "max_window_ns": 250_000_000,
    "confirm_windows": 5, "guard_ns": 20_000_000,
    "train_min_windows": 12, "train_min_per_mode": 4,
    "train_min_per_format_mode": 2, "validation_min_per_format_mode": 2,
    "validation_min_coverage": .80, "validation_min_persistence_ns": 100_000_000,
    "namespace_median_separation": 4., "namespace_quantile_separation": 2.,
    "distance_weights": [2., 1., 1.], "max_fit_iterations": 50,
    "radius_floor": .35, "radius_p95_scale": 1.5,
    "envelope_expansion": 2., "assignment_margin": .20,
    "interval_min_ns": 90_000_000, "interval_max_ns": 110_000_000,
    "min_io_observations": 16, "min_supported_fraction": .95,
    "dominant_class_fraction": .80, "max_pair_class_tv": .10,
    "min_episode_ns": 500_000_000, "min_percentile_samples": 100,
    "pairs": [[0, 1], [2, 3]],
    "scope": "conditional latency-mode diagnostic; no RFC qualification",
}
FEATURES = ["first_namespace_write_us", "post_flush_write_us", "flush_to_write_gap_us"]
SOURCES = ["wal_state_calibration.py", "capture_wal_trace.py", "wal_extent_trace.py", "wal_trace_regimes.py"]
COUNTERS = ["sync_count", "reported_commit_groups", "worker_notification_requests", "worker_eventfd_writes"]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def save_new(path, value):
    with path.open("x") as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def source_hashes():
    return {name: sha(Path(__file__).with_name(name)) for name in SOURCES}


def declare(directory, binary):
    directory.mkdir(parents=True, exist_ok=False)
    plan = {"schema": SCHEMA, "policy": POLICY, "source_sha256": source_hashes(),
            "binary_sha256": sha(binary), "binary": str(binary.resolve()),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "declared_monotonic_ns": time.monotonic_ns(), "declared_unix_ns": time.time_ns(),
            "cohorts": ["training", "validation", "evaluation"],
            "capture_order": ["v7", "v8", "v8", "v7"],
            "workloads": [{"name": "point", "num": 20000, "writers": 1, "batch": 1},
                          {"name": "batch64", "num": 750000, "writers": 16, "batch": 64}],
            "cpu_mask": list(range(16)), "counter_interval_ms": 100,
            "model_selection": "I/O features only; no outcome-based fitting or retries",
            "episode_selection": "highest throughput eligible episode per arm; p99/CPU from that episode",
            "RFC_performance_qualified": False}
    save_new(directory / "study-plan.json", plan)
    for name in SOURCES:
        (directory / ("source-" + name)).write_bytes(Path(__file__).with_name(name).read_bytes())
    return plan


def route(sample, workload):
    """Routing uses path/shape, never latency, format, or the old gap bucket."""
    if not sample["eligible"]:
        return None
    control, role = sample["first_namespace_write_control"], sample["first_namespace_write_role"]
    if role == "DATA" and control == 0:
        first = "WAL_ordinary"
    elif role == "NON_WAL" and control in (0, 0x4000):
        first = "non_WAL_FUA" if control else "non_WAL_ordinary"
    else:
        return None
    shape = sample["shape"]
    return canonical({"workload": workload, "first_write": first,
                      "first_write_size": ext.size_bucket(sample["first_namespace_write_bytes"]),
                      "fua_before_DATA": bool(sample["intervening_fua"]),
                      "ordinary_before_DATA": bool(sample["intervening_other_writes"]),
                      **{key: shape[key] for key in ("size", "queue", "peak", "flush_queue", "intervening_reads")}})


def observed_ns(sample):
    # The existing record has the first namespace write's duration but not its
    # start. That start is no later than DATA submission. Use this conservative
    # completion upper bound, so classification cannot consume a future CQE.
    return max(sample["time_ns"], sample["write_start_ns"] + math.ceil(sample["first_namespace_write_us"] * 1000))


def vector(samples):
    result = []
    for key in FEATURES:
        values = [s[key] for s in samples]
        if any(not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0 for x in values):
            raise ValueError("Invalid I/O latency feature")
        result.append(math.log1p(statistics.median(values)))
    return result


def windows(run):
    """Non-overlapping training windows; no cross-run windows or sparse bridges."""
    pending, output = defaultdict(list), defaultdict(list)
    for sample in sorted(run["samples"], key=observed_ns):
        key = route(sample, run["workload"])
        if key is None:
            continue
        group = pending[key]
        group[:] = [s for s in group if observed_ns(sample) - s["flush_start_ns"] <= POLICY["max_window_ns"]]
        if observed_ns(sample) - sample["flush_start_ns"] > POLICY["max_window_ns"]:
            continue
        group.append(sample)
        if len(group) == POLICY["window_samples"]:
            output[key].append({"vector": vector(group), "format": run["format"],
                                "name": run["name"], "start_ns": min(s["flush_start_ns"] for s in group), "end_ns": observed_ns(group[-1])})
            group.clear()
    return output


def distance(a, b):
    return sum(w * abs(x - y) for w, x, y in zip(POLICY["distance_weights"], a, b)) / sum(POLICY["distance_weights"])


def quantile(values, fraction):
    return sorted(values)[int((len(values) - 1) * fraction)]


def counts(rows, assignments):
    return {mode: dict(Counter(row["format"] for row, label in zip(rows, assignments) if label == mode))
            for mode in ("low", "high", "unknown")}


def assign(model, value):
    ds = [(distance(value, model[mode]["center"]), mode) for mode in ("low", "high")]
    ds.sort()
    best, label = ds[0]
    spec = model[label]
    if best > spec["radius"] or ds[1][0] - best < POLICY["assignment_margin"]:
        return "unknown"
    if any(x < lo or x > hi for x, lo, hi in zip(value, spec["lower"], spec["upper"])):
        return "unknown"
    return label


def fit_class(rows):
    if len(rows) < POLICY["train_min_windows"]:
        return {"accepted": False, "reason": "insufficient_training_windows", "windows": len(rows)}
    ordered = sorted(rows, key=lambda r: r["vector"][0])
    centers = [ordered[int((len(rows) - 1) * q)]["vector"] for q in (.25, .75)]
    for _ in range(POLICY["max_fit_iterations"]):
        groups = [[], []]
        for row in rows:
            index = min(range(2), key=lambda i: distance(row["vector"], centers[i]))
            groups[index].append(row)
        if any(len(group) < POLICY["train_min_per_mode"] for group in groups):
            return {"accepted": False, "reason": "insufficient_mode_support", "windows": len(rows)}
        revised = [[statistics.median(r["vector"][j] for r in group) for j in range(3)] for group in groups]
        if revised == centers:
            break
        centers = revised
    else:
        return {"accepted": False, "reason": "fit_did_not_converge", "windows": len(rows)}
    # Class labels are local I/O latency modes, not global SSD states.
    groups.sort(key=lambda group: statistics.median(r["vector"][0] for r in group))
    low, high = [[math.expm1(r["vector"][0]) for r in group] for group in groups]
    if (statistics.median(high) < POLICY["namespace_median_separation"] * statistics.median(low)
            or quantile(low, .90) * POLICY["namespace_quantile_separation"] >= quantile(high, .10)):
        return {"accepted": False, "reason": "no_separated_namespace_modes", "windows": len(rows)}
    model = {"accepted": False, "reason": "pending_validation", "windows": len(rows)}
    for label, group in zip(("low", "high"), groups):
        per_format = Counter(r["format"] for r in group)
        if any(per_format[fmt] < POLICY["train_min_per_format_mode"] for fmt in ("v7", "v8")):
            return {"accepted": False, "reason": "mode_is_format_specific_or_sparse", "windows": len(rows)}
        center = [statistics.median(r["vector"][j] for r in group) for j in range(3)]
        expand = math.log(POLICY["envelope_expansion"])
        model[label] = {"center": center,
                        "radius": max(POLICY["radius_floor"], POLICY["radius_p95_scale"] * quantile(
                            [distance(r["vector"], center) for r in group], .95)),
                        "lower": [quantile([r["vector"][j] for r in group], .05) - expand for j in range(3)],
                        "upper": [quantile([r["vector"][j] for r in group], .95) + expand for j in range(3)],
                        "training_format_windows": dict(per_format)}
    return model


def persistence(rows, labels):
    longest, current = defaultdict(int), None
    for row, label in zip(rows, labels):
        key = (row["format"], label)
        if label == "unknown":
            current = None
            continue
        if (current is None or current["key"] != key or current["name"] != row["name"]
                or row["start_ns"] - current["end"] > POLICY["max_window_ns"]):
            current = {"key": key, "name": row["name"], "start": row["start_ns"], "end": row["end_ns"]}
        else:
            current["end"] = row["end_ns"]
        longest[key] = max(longest[key], current["end"] - current["start"])
    return {mode: {fmt: longest[fmt, mode] for fmt in ("v7", "v8")} for mode in ("low", "high")}


def validate_class(model, rows):
    result = dict(model)
    result["validation_windows"] = len(rows)
    if model["reason"] != "pending_validation":
        return result
    labels = [assign(model, row["vector"]) for row in rows]
    distribution = counts(rows, labels)
    result["validation_format_windows"] = distribution
    result["validation_persistence_ns"] = persistence(rows, labels)
    result["validation_coverage"] = sum(label != "unknown" for label in labels) / len(rows) if rows else 0
    if any(distribution[mode].get(fmt, 0) < POLICY["validation_min_per_format_mode"]
           for mode in ("low", "high") for fmt in ("v7", "v8")):
        result["reason"] = "validation_missing_shared_modes"
    elif result["validation_coverage"] < POLICY["validation_min_coverage"]:
        result["reason"] = "validation_out_of_distribution"
    elif any(result["validation_persistence_ns"][mode][fmt] < POLICY["validation_min_persistence_ns"]
             for mode in ("low", "high") for fmt in ("v7", "v8")):
        result["reason"] = "validation_modes_not_persistent"
    else:
        result.update(accepted=True, reason="held_out_latency_modes_repeat")
    return result


def identity(plan):
    return {key: plan[key] for key in ("binary_sha256", "boot_id", "cpu_mask", "kernel", "perf", "device",
                                      "runner_sha256", "analyzer_sha256", "extent_helper_sha256", "workloads", "order", "policy", "clock", "events", "uid")}


def check_study(directory):
    plan = read(directory / "study-plan.json")
    if plan["schema"] != SCHEMA or plan["policy"] != POLICY or plan["source_sha256"] != source_hashes():
        raise ValueError("Study policy/source changed; declare a new study instead of refitting")
    return plan


def load_cohort(directory, study):
    plan = read(directory / "plan.json")
    if (plan["protocol"] != ext.EXTENT_PROTOCOL or plan["binary_sha256"] != study["binary_sha256"]
            or plan["boot_id"] != study["boot_id"] or plan["cpu_mask"] != study["cpu_mask"]
            or plan["workloads"] != study["workloads"] or plan["order"] != study["capture_order"]
            or plan["policy"] != ext.EXTENT_POLICY):
        raise ValueError("Cohort differs from predeclared capture protocol")
    expected_sources = {"runner_sha256": "capture_wal_trace.py", "analyzer_sha256": "wal_trace_regimes.py",
                        "extent_helper_sha256": "wal_extent_trace.py"}
    if any(plan[field] != study["source_sha256"][source] for field, source in expected_sources.items()):
        raise ValueError("Capture source differs from study")
    runs, hashes = [], {"plan.json": sha(directory / "plan.json"), "formats.json": sha(directory / "formats.json")}
    for workload in plan["workloads"]:
        for position, fmt in enumerate(plan["order"]):
            name = f"{workload['name']}-{position}-{fmt}"
            paths = {suffix: directory / (name + suffix) for suffix in (
                ".perf", "-capture.json", "-benchmark.json", "-timeline.json", "-extent-analysis.json")}
            for path in paths.values():
                hashes[path.name] = sha(path)
            tl, result = read(paths["-timeline.json"]), read(paths["-extent-analysis.json"])
            benchmark, capture = read(paths["-benchmark.json"]), read(paths["-capture.json"])
            expected = {"num": workload["num"], "threads": workload["writers"], "batch_size": workload["batch"],
                        "wal_format_version": int(fmt[1:]), "wal_io_mode": "parallel", "value_size": 1024}
            if (capture["returncode"] or any(benchmark["params"].get(k) != v for k, v in expected.items())
                    or tl["interval_ms"] != study["counter_interval_ms"]
                    or tl["clock"] != "CLOCK_MONOTONIC" or tl["schema"] != "kv-engine.wal-benchmark-timeline.v1"
                    or sum(r["puts"] for r in tl["requests"]) != workload["num"]):
                raise ValueError("Cohort workload/timeline mismatch")
            start, end = tl["counter_points"][0]["sample_begin_ns"], tl["counter_points"][-1]["sample_end_ns"]
            if start <= study["declared_monotonic_ns"]:
                raise ValueError("Exploratory/predeclaration capture cannot be reused")
            quality = not result["summary"]["metadata"]["lost"] and result["summary"]["trace_quality_ok"]
            samples = result["samples"] if quality else [
                {**s, "eligible": False, "reasons": [*s["reasons"], "quarantined_trace_quality"]}
                for s in result["samples"]]
            if any(not start <= s["flush_start_ns"] <= s["time_ns"] <= end for s in result["samples"]):
                raise ValueError("I/O observation outside capture")
            if result["samples"] != sorted(result["samples"], key=lambda s: s["time_ns"]):
                raise ValueError("Unordered I/O observations")
            runs.append({"name": name, "workload": workload["name"], "format": fmt, "position": position,
                         "start_ns": start, "end_ns": end, "samples": samples, "timeline": tl,
                         "summary": result["summary"]})
    if any(a["end_ns"] >= b["start_ns"] for a, b in zip(runs, runs[1:])):
        raise ValueError("Capture runs overlap or are out of order")
    return runs, {"path": str(directory.resolve()), "sha256": hashes, "identity": identity(plan)}


def grouped_windows(runs):
    grouped = defaultdict(list)
    for run in runs:
        for key, rows in windows(run).items():
            grouped[key].extend(rows)
    return grouped


def derive_classes(train, valid):
    a, b = grouped_windows(train), grouped_windows(valid)
    return {key: validate_class(fit_class(a.get(key, [])), b.get(key, [])) for key in sorted(a.keys() | b.keys())}


def verify(directory, training=None, validation=None):
    study = check_study(directory)
    path = directory / "frozen-model.json"
    model = read(path)
    if (model["schema"] != SCHEMA or model["source_sha256"] != source_hashes()
            or model["study_sha256"] != sha(directory / "study-plan.json")
            or model["policy"] != POLICY or model["features"] != FEATURES):
        raise ValueError("Frozen model/source/study identity mismatch")
    cohorts = []
    for phase, override in (("training", training), ("validation", validation)):
        runs, proof = load_cohort(override or Path(model[phase]["path"]), study)
        if proof["sha256"] != model[phase]["sha256"] or proof["identity"] != model[phase]["identity"]:
            raise ValueError("Frozen calibration input changed")
        cohorts.append(runs)
    if (cohorts[0][-1]["end_ns"] >= cohorts[1][0]["start_ns"]
            or cohorts[1][-1]["end_ns"] >= model["frozen_monotonic_ns"]
            or model["training"]["identity"] != model["validation"]["identity"]):
        raise ValueError("Calibration/validation/freeze chronology mismatch")
    if derive_classes(*cohorts) != model["classes"]:
        raise ValueError("Replayed model coefficients differ from frozen model")
    return {"model_sha256": sha(path), "replayed": True,
            "accepted_classes": sum(m["accepted"] for m in model["classes"].values())}


def fit(directory, training, validation):
    study = check_study(directory)
    train, train_proof = load_cohort(training, study)
    valid, valid_proof = load_cohort(validation, study)
    if train_proof["identity"] != valid_proof["identity"] or train[-1]["end_ns"] >= valid[0]["start_ns"]:
        raise ValueError("Validation must be a later independent cohort with identical capture identity")
    classes = derive_classes(train, valid)
    model = {"schema": SCHEMA, "study_sha256": sha(directory / "study-plan.json"), "policy": POLICY,
             "source_sha256": source_hashes(), "training": train_proof, "validation": valid_proof,
             "classes": classes, "features": FEATURES,
             "frozen_monotonic_ns": time.monotonic_ns(), "frozen_unix_ns": time.time_ns(),
             "RFC_performance_qualified": False}
    if model["frozen_monotonic_ns"] <= valid[-1]["end_ns"]:
        raise ValueError("Model freeze must follow validation")
    save_new(directory / "frozen-model.json", model)
    return {"model_sha256": sha(directory / "frozen-model.json"), "classes": len(classes),
            "accepted_classes": sum(m["accepted"] for m in classes.values()),
            "reasons": dict(Counter(m["reason"] for m in classes.values()))}


def classify_run(run, model):
    """Causal rolling confirmation; no labels before confirmation or over gaps."""
    queues, statuses, labeled = defaultdict(deque), {}, []
    for sample in sorted(run["samples"], key=observed_ns):
        key = route(sample, run["workload"])
        spec = model["classes"].get(key, {})
        row = {"time_ns": observed_ns(sample), "start_ns": sample["flush_start_ns"], "class": key, "state": "unknown"}
        if not spec.get("accepted"):
            row["reason"] = "uncalibrated_class" if key else "unroutable_observation"
            labeled.append(row)
            continue
        group = queues[key]
        recent = [s for s in group if observed_ns(sample) - s["flush_start_ns"] <= POLICY["max_window_ns"]]
        group.clear()
        group.extend(recent)
        if observed_ns(sample) - sample["flush_start_ns"] > POLICY["max_window_ns"]:
            statuses.pop(key, None)
            row["reason"] = "unconfirmed_or_guarded"
            labeled.append(row)
            continue
        if not group:
            statuses.pop(key, None)
        group.append(sample)
        while len(group) > POLICY["window_samples"]:
            group.popleft()
        label = assign(spec, vector(group)) if len(group) == POLICY["window_samples"] else "unknown"
        previous = statuses.get(key)
        if label == "unknown":
            statuses.pop(key, None)
        elif not previous or previous["label"] != label:
            statuses[key] = {"label": label, "count": 1, "confirmed_ns": None}
        else:
            previous["count"] += 1
        status = statuses.get(key)
        if status and status["count"] >= POLICY["confirm_windows"]:
            if status["confirmed_ns"] is None:
                status["confirmed_ns"] = observed_ns(sample)
            if sample["flush_start_ns"] >= status["confirmed_ns"] + POLICY["guard_ns"]:
                row["state"] = label
        row["reason"] = "" if row["state"] != "unknown" else "unconfirmed_or_guarded"
        labeled.append(row)
    return labeled


def interval_diagnostic(run, labeled):
    # Existing conservation/latency code owns request assignment. New state joins
    # must reject an entire bucket, never remove its slow/cross-boundary requests.
    rows = trace.metric_intervals(run["timeline"], [], ext.EXTENT_POLICY)
    times = [r["time_ns"] for r in labeled]
    requests = sorted(run["timeline"]["requests"], key=lambda r: r["end_ns"])
    request_ends = [r["end_ns"] for r in requests]
    for index, row in enumerate(rows):
        first = run["timeline"]["counter_points"][index]
        spans = requests[bisect_left(request_ends, row["start_ns"]):bisect_left(request_ends, row["end_ns"])]
        envelope_start = min([first["sample_begin_ns"]] + [r["start_ns"] for r in spans])
        # Include the nearest bracketing observations and a guard at both ends.
        left = bisect_left(times, envelope_start - POLICY["guard_ns"]) - 1
        right = bisect_left(times, row["end_ns"] + POLICY["guard_ns"])
        observations = labeled[max(0, left):right + 1]
        observed = Counter(s["class"] for s in observations if s["class"] is not None)
        supported = [s for s in observations if s["state"] != "unknown"]
        state_counts = Counter(s["state"] for s in supported)
        reason = ""
        duration = row["end_ns"] - row["start_ns"]
        if not POLICY["interval_min_ns"] <= duration <= POLICY["interval_max_ns"]:
            reason = "counter_interval_out_of_range"
        elif left < 0 or right >= len(labeled):
            reason = "unbracketed_interval"
        elif len(observations) < POLICY["min_io_observations"]:
            reason = "sparse_io_observations"
        elif any(b["time_ns"] - a["time_ns"] > POLICY["max_window_ns"] for a, b in zip(observations, observations[1:])):
            reason = "io_observation_gap"
        elif any(s["start_ns"] < envelope_start - POLICY["max_window_ns"] for s in observations):
            reason = "stale_io_observation"
        elif len(supported) / len(observations) < POLICY["min_supported_fraction"]:
            reason = "insufficient_calibrated_coverage"
        elif any(s["state"] == "unknown" and s["reason"] == "unconfirmed_or_guarded" for s in observations):
            reason = "unconfirmed_calibrated_observation"
        elif len(state_counts) != 1:
            reason = "mixed_latency_modes"
        elif not observed or max(observed.values()) / len(observations) < POLICY["dominant_class_fraction"]:
            reason = "mixed_path_classes"
        else:
            # Labels from different classes are not an assertion of a global
            # shared SSD state. The dominant class must itself be calibrated.
            dominant = max(observed, key=observed.get)
            if any(s["state"] == "unknown" for s in observations if s["class"] == dominant):
                reason = "dominant_class_unconfirmed"
        row.update({"state": next(iter(state_counts)) if not reason else "unknown", "rejection_reason": reason,
                    "io_observations": len(observations), "supported_observations": len(supported),
                    "class_histogram": dict(observed), "dominant_class": max(observed, key=observed.get) if observed else None,
                    "request_envelope_start_ns": envelope_start})
    return rows


def episodes(run, intervals):
    groups, current = [], []
    for row in intervals:
        same = current and row["state"] == current[-1]["state"] and row["dominant_class"] == current[-1]["dominant_class"]
        if current and (row["state"] == "unknown" or not same):
            groups.append(current)
            current = []
        if row["state"] != "unknown":
            current.append(row)
    if current:
        groups.append(current)
    output = []
    for group in groups:
        start, end = group[0]["start_ns"], group[-1]["end_ns"]
        if end - start < POLICY["min_episode_ns"]:
            continue
        requests = [r for r in run["timeline"]["requests"] if start <= r["end_ns"] < end]
        values = sorted((r["end_ns"] - r["start_ns"]) / 1e6 for r in requests)
        histogram = Counter()
        for row in group:
            histogram.update(row["class_histogram"])
        duration = (end - start) / 1e9
        output.append({"name": run["name"], "format": run["format"], "workload": run["workload"],
                       "state": group[0]["state"], "dominant_class": group[0]["dominant_class"],
                       "start_ns": start, "end_ns": end, "duration_s": duration,
                       "puts": sum(r["puts"] for r in requests), "puts_s": sum(r["puts"] for r in requests) / duration,
                       "latency_samples": len(values), "p99_ms": quantile(values, .99) if len(values) >= POLICY["min_percentile_samples"] else None,
                       "process_cpu_s": sum(r["process_cpu_s"] for r in group),
                       **{key: sum(r[key] for r in group) for key in COUNTERS},
                       "class_histogram": dict(histogram)})
    return output


def histogram_tv(a, b):
    na, nb = sum(a.values()), sum(b.values())
    return sum(abs(a.get(k, 0) / na - b.get(k, 0) / nb) for k in a.keys() | b.keys()) / 2


def compare(runs, results):
    output = []
    for workload in ("point", "batch64"):
        names = {r["position"]: r["name"] for r in runs if r["workload"] == workload}
        for positions in POLICY["pairs"]:
            a, b = [results[names[position]]["episodes"] for position in positions]
            classes = {(e["state"], e["dominant_class"]) for e in a + b}
            for state, key in sorted(classes):
                arms = [[e for e in group if e["state"] == state and e["dominant_class"] == key] for group in (a, b)]
                row = {"workload": workload, "positions": positions, "state": state, "class": key,
                       "qualified": False, "reason": "missing_same_class_mode_in_one_arm"}
                if all(arms):
                    best = [max(arm, key=lambda e: e["puts_s"]) for arm in arms]
                    tv = histogram_tv(*(e["class_histogram"] for e in best))
                    row.update(best_episodes=best, class_total_variation=tv)
                    if tv <= POLICY["max_pair_class_tv"] and all(e["p99_ms"] is not None for e in best):
                        by_format = {e["format"]: e for e in best}
                        row.update(qualified=True, reason="conditional_same_class_latency_mode",
                                   v8_over_v7_puts=by_format["v8"]["puts_s"] / by_format["v7"]["puts_s"],
                                   v8_over_v7_p99=by_format["v8"]["p99_ms"] / by_format["v7"]["p99_ms"])
                    else:
                        row["reason"] = "incompatible_class_mixture_or_sparse_percentile"
                output.append(row)
    return output


def evaluate(directory, evaluation):
    study = check_study(directory)
    model_path = directory / "frozen-model.json"
    model = read(model_path)
    if (model["schema"] != SCHEMA or model["source_sha256"] != source_hashes()
            or model["study_sha256"] != sha(directory / "study-plan.json") or model["policy"] != POLICY):
        raise ValueError("Frozen model/source/study identity mismatch")
    runs, proof = load_cohort(evaluation, study)
    if (proof["identity"] != model["training"]["identity"]
            or runs[0]["start_ns"] <= model["frozen_monotonic_ns"]):
        raise ValueError("Evaluation must be fresh and follow model freeze with identical capture identity")
    results = {}
    for run in runs:
        labels = classify_run(run, model)
        intervals = interval_diagnostic(run, labels)
        results[run["name"]] = {"labels": labels, "intervals": intervals, "episodes": episodes(run, intervals),
                                "interval_reasons": dict(Counter(r["rejection_reason"] for r in intervals)),
                                "observation_states": dict(Counter(r["state"] for r in labels))}
    comparisons = compare(runs, results)
    summary = {"schema": SCHEMA, "model_sha256": sha(model_path), "evaluation": proof,
               "accepted_classes": sum(m["accepted"] for m in model["classes"].values()),
               "runs": results, "comparisons": comparisons,
               "qualified_pairs": sum(r["qualified"] for r in comparisons),
               "RFC_performance_qualified": False,
               "scope": "Path-conditioned observed latency modes; no independent identification of controller state."}
    save_new(directory / "evaluation.json", summary)
    csv_rows = [{"name": name, **row, "class_histogram": canonical(row["class_histogram"])}
                for name, result in results.items() for row in result["intervals"]]
    trace.write_csv(directory / "evaluated-intervals.csv", csv_rows, list(csv_rows[0]))
    return {"accepted_classes": summary["accepted_classes"], "qualified_pairs": summary["qualified_pairs"],
            "runs": {name: {k: result[k] for k in ("interval_reasons", "observation_states")}
                     for name, result in results.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    declaration = sub.add_parser("declare")
    declaration.add_argument("--study-dir", type=Path, required=True)
    declaration.add_argument("--binary", type=Path, required=True)
    fitting = sub.add_parser("fit")
    fitting.add_argument("--study-dir", type=Path, required=True)
    fitting.add_argument("--training", type=Path, required=True)
    fitting.add_argument("--validation", type=Path, required=True)
    replay = sub.add_parser("verify")
    replay.add_argument("--study-dir", type=Path, required=True)
    replay.add_argument("--training", type=Path)
    replay.add_argument("--validation", type=Path)
    scoring = sub.add_parser("evaluate")
    scoring.add_argument("--study-dir", type=Path, required=True)
    scoring.add_argument("--evaluation", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "declare":
        result = declare(args.study_dir, args.binary)
    elif args.command == "fit":
        result = fit(args.study_dir, args.training, args.validation)
    elif args.command == "verify":
        result = verify(args.study_dir, args.training, args.validation)
    else:
        verify(args.study_dir)
        result = evaluate(args.study_dir, args.evaluation)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
