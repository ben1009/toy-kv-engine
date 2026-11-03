"""Read native profiler/library metadata after measurements; never runs workloads."""

from pathlib import Path
import ctypes
import hashlib
import json
import platform
import re
import subprocess


p = Path(__file__).resolve().parent


def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT)


class DlInfo(ctypes.Structure):
    _fields_ = [("dli_fname", ctypes.c_char_p), ("dli_fbase", ctypes.c_void_p),
                ("dli_sname", ctypes.c_char_p), ("dli_saddr", ctypes.c_void_p)]


libc = ctypes.CDLL("libc.so.6")
libc.dladdr.argtypes = [ctypes.c_void_p, ctypes.POINTER(DlInfo)]
symbols = {}
for name in ["memcpy", "memmove", "memset", "memcmp", "bcmp", "strlen", "strcmp",
             "malloc", "calloc", "realloc", "free", "pthread_mutex_lock", "pthread_cond_wait"]:
    address = ctypes.cast(getattr(libc, name), ctypes.c_void_p).value
    info = DlInfo()
    assert libc.dladdr(address, ctypes.byref(info))
    symbols[name] = {"library": info.dli_fname.decode(),
                     "resolved_elf_offset": hex(address - info.dli_fbase)}

library = Path("/usr/lib/libc.so.6")
evidence = {
    "purpose": "Resolve native libc IFUNC addresses and conservative sampled instruction sites; private stripped function names remain unknown.",
    "libc_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
    "libc_build_id": re.search(r"Build ID: (\w+)", run("readelf", "-n", str(library)))[1],
    "ifunc_and_exported_symbol_offsets": symbols,
    "mimalloc_package": run("pacman", "-Q", "mimalloc").strip(),
    "mimalloc_library": str(Path("/usr/lib/libmimalloc.so.2").resolve()),
    "mimalloc_library_sha256": hashlib.sha256(Path("/usr/lib/libmimalloc.so.2").read_bytes()).hexdigest(),
    "kernel": platform.release(),
    "cpu_core_mask": Path("/sys/devices/cpu_core/cpus").read_text().strip(),
    "cpu_atom_mask": Path("/sys/devices/cpu_atom/cpus").read_text().strip(),
    "collector_allowed_cpus": sorted(__import__("os").sched_getaffinity(0)),
    "perf_version": run("perf", "version").strip(),
    "settings_changed": False,
}
(p / "native-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
disassembly = []
for start, end in [(0xA47F0, 0xA4840), (0xA5FC0, 0xA6020), (0x16D040, 0x16D060)]:
    disassembly.append(run("objdump", "-d", "-M", "intel", f"--start-address={hex(start)}",
                           f"--stop-address={hex(end)}", str(library)))
(p / "native-disassembly.txt").write_text("\n".join(disassembly))

captures = [(p, name) for name in ["00-v7", "01-v8", "02-v8", "03-v7"]]
captures += [(p / "allocator-screen", name) for name in
             ["00-system", "01-mimalloc", "02-mimalloc", "03-system"]]
for folder, name in captures:
    perf = folder / f"{name}.perf"
    (folder / f"{name}-perf-header.txt").write_text(run("perf", "report", "--header-only", "--stdio", "-i", str(perf)))
    stats = run("perf", "report", "--stdio", "--call-graph", "none", "--percent-limit", "100", "-i", str(perf))
    (folder / f"{name}-perf-stats.txt").write_text(stats)
    assert re.search(r"Total Lost Samples: 0\b", stats)
    with (folder / f"{name}-mmap-decode.stderr").open("w") as err:
        child = subprocess.Popen(["perf", "script", "--show-mmap-events", "-i", str(perf)],
                                 text=True, stdout=subprocess.PIPE, stderr=err)
        mappings = [line.rstrip() for line in child.stdout if "PERF_RECORD_MMAP2" in line
                    and "/usr/lib/libc.so.6" in line]
        assert child.wait() == 0
    assert mappings
    (folder / f"{name}-libc-maps.json").write_text(json.dumps(mappings, indent=2) + "\n")
print(json.dumps({"captures": len(captures), "libc_build_id": evidence["libc_build_id"],
                  "mimalloc_package": evidence["mimalloc_package"], "settings_changed": False}))
