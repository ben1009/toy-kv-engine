"""Decode raw NVMe setup payloads, including control bits, from perf.data."""

from collections import Counter
from pathlib import Path
import json
import struct


def decode(path, setup_id=1642):
    b = Path(path).read_bytes()
    assert b[:8] == b"PERFILE2"
    header = struct.unpack_from("<13Q", b)
    stride, attrs_offset, attrs_size, data_offset, data_size = header[2:7]
    by_id = {}
    for offset in range(attrs_offset, attrs_offset + attrs_size, stride):
        kind, size, config, period, sample_type = struct.unpack_from("<IIQQQ", b, offset)
        ids_offset, ids_size = struct.unpack_from("<QQ", b, offset + stride - 16)
        for at in range(ids_offset, ids_offset + ids_size, 8):
            by_id[struct.unpack_from("<Q", b, at)[0]] = (kind, config, sample_type)
    events, record_types = [], Counter()
    lost = 0
    offset = data_offset
    while offset < data_offset + data_size:
        kind, misc, size = struct.unpack_from("<IHH", b, offset)
        assert size >= 8 and offset + size <= data_offset + data_size
        record_types[kind] += 1
        if kind == 2:
            lost += struct.unpack_from("<Q", b, offset + 16)[0]
        elif kind == 13:
            lost += struct.unpack_from("<Q", b, offset + 8)[0]
        elif kind == 9:
            pos = offset + 8
            identity = struct.unpack_from("<Q", b, pos)[0]
            attr_kind, config, sample_type = by_id[identity]
            assert sample_type & (1 << 16)
            pos += 8
            assert sample_type & ~((1 << 16) | 1 | 2 | 4 | 8 | 64 | 128 | 256 | 512 | 1024) == 0
            if sample_type & 1:
                pos += 8  # instruction pointer
            pid = tid = timestamp = cpu = None
            if sample_type & 2:
                pid, tid = struct.unpack_from("<II", b, pos)
                pos += 8
            if sample_type & 4:
                timestamp = struct.unpack_from("<Q", b, pos)[0]
                pos += 8
            for bit in [8, 64, 512]:
                if sample_type & bit:
                    pos += 8
            if sample_type & 128:
                cpu = struct.unpack_from("<I", b, pos)[0]
                pos += 8
            if sample_type & 256:
                pos += 8
            if sample_type & 1024:
                raw_size = struct.unpack_from("<I", b, pos)[0]
                raw = b[pos + 4:pos + 4 + raw_size]
                assert pos + 4 + raw_size <= offset + size
                if attr_kind == 2 and config == setup_id:
                    assert len(raw) >= 85 and struct.unpack_from("<H", raw)[0] == setup_id
                    events.append({"pid": pid, "tid": tid, "common_pid": struct.unpack_from("<i", raw, 4)[0],
                                   "time_ns": timestamp, "cpu": cpu,
                                   "disk": raw[8:40].split(b"\0", 1)[0].decode(),
                                   "controller": struct.unpack_from("<i", raw, 40)[0],
                                   "qid": struct.unpack_from("<i", raw, 44)[0],
                                   "opcode": raw[48], "cid": struct.unpack_from("<H", raw, 52)[0],
                                   "nsid": struct.unpack_from("<I", raw, 56)[0],
                                   "start_lba": struct.unpack_from("<Q", raw, 61)[0],
                                   "nlb_zero_based": struct.unpack_from("<H", raw, 69)[0],
                                   "control": struct.unpack_from("<H", raw, 71)[0]})
        offset += size
    assert offset == data_offset + data_size
    return events, {"lost": lost, "record_types": dict(record_types), "setup_events": len(events)}


if __name__ == "__main__":
    import sys
    events, meta = decode(sys.argv[1])
    print(json.dumps({**meta, "write_control_values": dict(Counter(e["control"] for e in events if e["opcode"] == 1)),
                      "probe_write_controls": dict(Counter(e["control"] for e in events if e["opcode"] == 1 and e["pid"] == 2929107))}, indent=2))
