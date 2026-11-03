"""Read controller capabilities, SMART and current APST; no feature writes."""

from pathlib import Path
import datetime
import hashlib
import json
import os
import struct

from nvme_readonly import read_admin


p = Path(__file__).resolve().parent
assert os.geteuid() == 0
fd = os.open("/dev/nvme0", os.O_RDONLY)
try:
    _, data = read_admin(fd, 0x06, length=4096, cdw10=1)
    controller = {"model": data[24:64].decode().strip(), "firmware": data[64:72].decode().strip(),
                  "mdts_encoding": data[77], "nvme_version": hex(struct.unpack_from("<I", data, 80)[0]),
                  "oacs": hex(struct.unpack_from("<H", data, 256)[0]),
                  "number_of_power_states": data[263] + 1, "apsta": data[265],
                  "oncs": hex(struct.unpack_from("<H", data, 520)[0]), "vwc": data[525]}
    assert controller["model"] == "SOLIDIGM SSDPFKNU010TZ" and controller["firmware"] == "004C"
    _, smart = read_admin(fd, 0x02, length=512, nsid=0xFFFFFFFF, cdw10=(127 << 16) | 2)
    health = {"critical_warning": smart[0], "temperature_kelvin": struct.unpack_from("<H", smart, 1)[0],
              "available_spare_pct": smart[3], "endurance_used_pct": smart[5]}
    for name, offset in [("data_units_read", 32), ("data_units_written", 48), ("power_cycles", 112),
                         ("power_on_hours", 128), ("unsafe_shutdowns", 144),
                         ("media_errors", 160), ("error_log_entries", 176)]:
        health[name] = int.from_bytes(smart[offset:offset + 16], "little")
    apst_value, apst_table = read_admin(fd, 0x0A, length=256, cdw10=0x0C)
    vwc_value, _ = read_admin(fd, 0x0A, cdw10=0x06)
    report = {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "controller": controller, "smart": health,
              "current_apst_value": apst_value, "current_apst_table_hex": apst_table.hex(),
              "current_apst_table_sha256": hashlib.sha256(apst_table).hexdigest(),
              "current_vwc_value": vwc_value, "feature_or_device_settings_changed": False}
    for event in ["nvme_setup_cmd", "nvme_complete_rq"]:
        f = Path(f"/sys/kernel/tracing/events/nvme/{event}/format")
        (p / f"{event}-format.txt").write_text(f.read_text())
        os.chown(p / f"{event}-format.txt", 1000, 1000)
    (p / "inspection.json").write_text(json.dumps(report, indent=2) + "\n")
    os.chown(p / "inspection.json", 1000, 1000)
    small = {**report, "current_apst_table_hex": "[saved in local inspection.json]"}
    print(json.dumps(small, indent=2))
finally:
    os.close(fd)
