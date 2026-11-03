from collections import Counter, defaultdict
from pathlib import Path
import json
import re

p = Path(__file__).resolve().parent
all_results = {}
for path in sorted(p.glob("cpu-*-stacks.txt")):
    samples = []
    current = None
    for line in path.read_text().splitlines():
        match = re.match(r"^\S.*?\s+(\d+)/(\d+)\s+\[(\d+)\]\s+(\d+\.\d+): cpu-clock:u:", line)
        if match:
            if current:
                samples.append(current)
            current = {"pid": int(match[1]), "tid": int(match[2]), "cpu": int(match[3]), "stack": []}
        elif current and line.startswith("\t"):
            parts = line.strip().split(" ", 1)
            if len(parts) == 2:
                current["stack"].append(parts[1])
    if current:
        samples.append(current)
    phases = json.loads((p / (path.stem.removesuffix("-stacks") + "-phases.json")).read_text())
    hash_index = phases["phase_names"].index("v7_logical_hash")
    ordered_sha_threads = {t["tid"] for t in phases["threads"] if t["buckets"][hash_index]["calls"]}
    counters = defaultdict(Counter)
    tids = defaultdict(set)
    for sample in samples:
        leaf = sample["stack"][0] if sample["stack"] else "[missing]"
        category = "blake3_hash_many" if "blake3_hash_many" in leaf else ("blake3_other" if "blake3" in leaf else ("sha256" if "sha256" in leaf else ("crc32" if "crc32" in leaf else "other")))
        kind = "P" if sample["cpu"] < 16 else "E"
        counters["all"][kind] += 1
        counters[category][kind] += 1
        tids[category].add(sample["tid"])
        if category == "sha256" and sample["tid"] in ordered_sha_threads:
            counters["sha256_on_ordered_packer"][kind] += 1
            tids["sha256_on_ordered_packer"].add(sample["tid"])
    result = {k: {"P_samples": v["P"], "E_samples": v["E"], "E_pct": 100 * v["E"] / sum(v.values()), "threads": len(tids[k]) if k != "all" else len({r["tid"] for r in samples})} for k, v in counters.items()}
    all_results[path.stem] = result
    print(path.stem, json.dumps(result))
(p / "cpu-sample-summary.json").write_text(json.dumps({
    "method": "Leaf-symbol sample counts by recorded CPU; P=kernel cpu_core 0-15, E=cpu_atom 16-31. Sample shares, not jobs/bytes or elapsed time; original records lack CPU fields. Ordered-packer SHA subset matches thread IDs with observed v7_logical_hash calls.",
    "samples": all_results,
}, indent=2) + "\n")
