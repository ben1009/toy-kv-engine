"""Recompute command flags/competing I/O from compressed native perf records."""

from collections import Counter
from pathlib import Path
import gzip
import hashlib
import importlib.util
import json
import os
import re
import sys


p = Path(__file__).resolve().parent
prefix = Path(__file__).name.removesuffix("-analyze.py")
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("nvme_perf_decode", p / f"{prefix}-decode.py")
decoder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(decoder)
r = json.loads((p / f"{prefix}-measurements.json").read_text())
setup_id = int(re.search(r"^ID: (\d+)$", (p / f"{prefix}-nvme_setup_cmd-format.txt").read_text(), re.M)[1])
assert r["full"]["restored_exactly"] and r["smoke"]["restored_exactly"]
assert r["full"]["original_apst_value"] == r["full"]["final_apst_value"] == 1
assert r["full"]["original_vwc_value"] == r["full"]["final_vwc_value"] == 1
original_table = bytes.fromhex(r["full"]["original_apst_table_hex"])
assert len(original_table) == 256
assert hashlib.sha256(original_table).hexdigest() == r["full"]["final_apst_table_sha256"]
assert [f["value"] for f in r["full"]["feature_readbacks"]] == [0, 1]
assert all(f["table_sha256"] == r["full"]["final_apst_table_sha256"] for f in r["full"]["feature_readbacks"])
assert r["confirmation"]["apst_and_vwc_unchanged"] and r["confirmation"]["apst_value"] == 1

captures = []
for cohort in ["smoke", "full"]:
    captures.extend((cohort, c["name"], c["record"], c["trace_metadata"])
                    for c in r[cohort]["captures"])
captures.append(("confirmation", "confirmation", r["confirmation"]["record"], r["confirmation"]["trace_metadata"]))
rows = []
for cohort, name, record, old_meta in captures:
    trace = r["traces"][f"{cohort}/{name}"]
    raw = gzip.decompress((p / trace["archive"]).read_bytes())
    assert hashlib.sha256(raw).hexdigest() == trace["sha256"]
    memfd = os.memfd_create("decode-nvme-fua", 0)
    try:
        cursor = 0
        while cursor < len(raw):
            cursor += os.write(memfd, raw[cursor:])
        events, meta = decoder.decode(f"/proc/self/fd/{memfd}", setup_id)
    finally:
        os.close(memfd)
    assert json.loads(json.dumps(meta)) == old_meta and meta["lost"] == 0
    assert record["uid"] == 1000 and record["cpu_mask"] == [8]
    apst = 0 if name == "apst-disabled" else 1
    for index, phase in enumerate(record["results"]):
        io = [e for e in events if e["disk"] == "nvme0n1" and e["qid"] > 0
              and phase["start_ns"] <= e["time_ns"] <= phase["end_ns"]]
        writes = [e for e in io if e["opcode"] == 1 and e["pid"] == phase["pid"]]
        assert len(writes) == len(phase["write_ns"]) == phase["operations"]
        controls = {hex(k): n for k, n in Counter(e["control"] for e in writes).items()}
        assert controls == phase["verified_nvme_write_controls"]
        assert controls == {"0x4000" if phase["mode"] == "dsync1" else "0x0": phase["operations"]}
        other = [e for e in io if e["opcode"] == 1 and e["pid"] != phase["pid"]]
        row = {"cohort": cohort, "capture": name, "phase": index, "apst": apst,
               "mode": phase["mode"], "operations": phase["operations"], "ops_s": phase["ops_s"],
               "write": phase["write"], "sync": phase["sync"], "durable_request": phase["durable_request"],
               "first_after_sync": phase.get("first_after_sync"), "nvme_controls": controls,
               "global_namespace_flushes": sum(e["opcode"] == 0 for e in io),
               "other_host_writes": len(other),
               "other_host_write_bytes": sum((e["nlb_zero_based"] + 1) * 512 for e in other),
               "global_reads": sum(e["opcode"] == 2 for e in io),
               "over_5ms_write_indices": [i for i, n in enumerate(phase["write_ns"]) if n > 5_000_000]}
        assert row["write"]["over_5ms"] == len(row["over_5ms_write_indices"])
        rows.append(row)
summary = {"firmware": "004C", "rows": rows,
           "full_probe_count": sum(q["cohort"] != "smoke" for q in rows),
           "full_probe_write_mib": sum(q["operations"] for q in rows if q["cohort"] != "smoke") / 256,
           "traced_nvme_write_commands": sum(q["operations"] for q in rows),
           "all_target_writes_and_FUA_bits_verified": True, "all_reported_sample_loss_zero": True,
           "original_apst_and_table_restored_exactly": True, "volatile_cache_unchanged": True,
           "raw_device_writes": False, "normal_engine_performance_qualification": False,
           "limitation": "Single ordered experiment plus a confirmation in the observed slow state. APST need not be enabled for this slowdown; no FUA cure is demonstrated. Does not identify the internal NAND/FTL/firmware task or the fast-to-slow trigger."}
(p / f"{prefix}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, indent=2))
