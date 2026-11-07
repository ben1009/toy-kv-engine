#!/usr/bin/env python3
"""Local regression checks for storage bootstrap and the bench-user shell."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class GcpBootstrapTests(unittest.TestCase):
    def run_storage_bootstrap(self, count=2, *, repeat=False, **scenario):
        source = (ROOT / "infra/gcp/test-environment/startup-script.sh.tftpl").read_text(encoding="utf-8")
        body = source[source.index("mapfile -t SSD_LINKS"):].replace("$${", "${")
        defaults = {
            "MOCK_LINK_COUNT": count, "MOCK_BAD_NAME": 0, "MOCK_MISSING_DEVICE": 0,
            "MOCK_SMALL_DEVICE": 0, "MOCK_ARRAY_PRESENT": 0, "MOCK_REASSEMBLE": 0,
            "MOCK_RAID_SIGNATURE": 0, "MOCK_ARRAY_SIGNATURE": "", "MOCK_RAW_SIGNATURE": "",
            "MOCK_FILESYSTEM": "", "MOCK_MOUNTED": 0,
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "mdadm.conf").touch()
            (root / "fstab").write_text("UUID=boot / ext4 defaults 0 1\n", encoding="utf-8")
            body = body.replace("/etc/mdadm/mdadm.conf", str(root / "mdadm.conf"))
            body = body.replace("/etc/fstab", str(root / "fstab"))
            script = (
                f'set -Eeuo pipefail\nTEST_ROOT={shlex.quote(temp)}\nTEST_USER=tester\n'
                f'SSD_COUNT={count}\nSSD_MOUNT="$TEST_ROOT/mount"\n'
                f'source {shlex.quote(str(ROOT / "scripts/fixtures/gcp_bootstrap_commands.sh"))}\n'
                + body + ("\n" + body if repeat else "")
            )
            result = subprocess.run(
                ["bash", "-c", script], text=True, capture_output=True, check=False,
                env={**os.environ, **{key: str(value) for key, value in (defaults | scenario).items()}},
            )
            events = (root / "events").read_text() if (root / "events").exists() else ""
            metadata = root / "mount/environment.txt"
            return result, events, metadata.read_text() if metadata.exists() else "", (root / "fstab").read_text()

    def test_storage_bootstrap_creates_single_and_raid_layouts_and_is_repeatable(self):
        for count in (1, 2):
            with self.subTest(count=count):
                result, events, metadata, fstab = self.run_storage_bootstrap(count, repeat=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(events.count("formatted "), 1)
                self.assertEqual(events.count("raid-created"), count - 1)
                self.assertEqual(events.count("mounted"), 1)
                self.assertIn(f"local_ssd_count={count}", metadata)
                self.assertIn("filesystem_uuid=mock-filesystem-uuid", metadata)
                self.assertEqual(fstab.count("mock-filesystem-uuid"), 1)
                self.assertIn("UUID=boot / ext4 defaults 0 1", fstab)

    def test_existing_filesystem_and_reassembled_raid_are_preserved(self):
        for array, reassemble in ((1, 0), (0, 1)):
            with self.subTest(array=array, reassemble=reassemble):
                result, events, metadata, _ = self.run_storage_bootstrap(
                    MOCK_FILESYSTEM="ext4", MOCK_ARRAY_PRESENT=array, MOCK_REASSEMBLE=reassemble,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("formatted", events)
                self.assertNotIn("raid-created", events)
                self.assertIn("filesystem_uuid=mock-filesystem-uuid", metadata)

    def test_bad_device_inventory_and_signatures_never_format_or_publish_ready(self):
        cases = [{"MOCK_LINK_COUNT": 1}, {"MOCK_BAD_NAME": 1}, {"MOCK_MISSING_DEVICE": 1},
                 {"MOCK_SMALL_DEVICE": 1}, {"MOCK_RAID_SIGNATURE": 1},
                 {"MOCK_RAW_SIGNATURE": "ext4"}, {"MOCK_FILESYSTEM": "xfs"},
                 {"MOCK_ARRAY_PRESENT": 1, "MOCK_ARRAY_SIGNATURE": "LVM2_member"}]
        for scenario in cases:
            with self.subTest(scenario=scenario):
                result, events, metadata, _ = self.run_storage_bootstrap(**scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("formatted", events)
                self.assertEqual(metadata, "")

    def test_mount_from_another_filesystem_never_changes_ownership_or_publishes_ready(self):
        result, events, metadata, _ = self.run_storage_bootstrap(
            MOCK_FILESYSTEM="ext4", MOCK_ARRAY_PRESENT=1,
            MOCK_MOUNTED=1, MOCK_MOUNTED_UUID="boot-filesystem-uuid",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected Local SSD filesystem", result.stderr)
        self.assertNotIn("formatted", events)
        self.assertNotIn("chown", events)
        self.assertEqual(metadata, "")

    def test_tool_installation_marker_is_repeatable_and_never_hides_failure(self):
        source = (ROOT / "infra/gcp/test-environment/startup-script.sh.tftpl").read_text(encoding="utf-8")
        body = source[source.index('if ! id "$TEST_USER"'):source.index("mapfile -t SSD_LINKS")]
        for rust, failure, previous in (
            (True, None, None), (False, None, None), (True, "apt", None),
            (True, "download", None), (True, "installer", None),
            (True, None, ("tester", False)), (True, None, ("previous-user", True)),
        ):
            with self.subTest(rust=rust, failure=failure, previous=previous), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                commands = root / "bin"
                commands.mkdir()
                downloader = commands / "curl"
                downloader.write_text(
                    '#!/usr/bin/env bash\n'
                    'if [[ "$MOCK_CURL_FAIL" == 1 ]]; then\n'
                    '    echo "simulated Rust download failure" >&2\n'
                    '    exit 22\n'
                    'fi\n'
                    'printf \'exit %s\\n\' "$MOCK_INSTALLER_EXIT"\n',
                    encoding="utf-8",
                )
                downloader.chmod(0o755)
                script = (
                    f'set -Eeuo pipefail\nTEST_ROOT={shlex.quote(temp)}\nTEST_USER=tester\n'
                    f'BOOTSTRAP_MARKER="$TEST_ROOT/state/bootstrap-complete"\nINSTALL_RUST={str(rust).lower()}\n'
                    f'source {shlex.quote(str(ROOT / "scripts/fixtures/gcp_bootstrap_commands.sh"))}\n'
                )
                if previous:
                    script += f'TEST_USER={previous[0]}\nINSTALL_RUST={str(previous[1]).lower()}\n' + body + "\n"
                script += f'TEST_USER=tester\nINSTALL_RUST={str(rust).lower()}\n' + body + "\n" + body
                script = script.replace("/var/lib/gcp-test-environment", f"{temp}/state")
                result = subprocess.run(
                    ["bash", "-c", script], text=True, capture_output=True, check=False,
                    env={**os.environ, "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
                         "MOCK_USER_EXISTS": "0", "MOCK_PERF_AVAILABLE": "1",
                         "MOCK_APT_FAIL": str(int(failure == "apt")),
                         "MOCK_CURL_FAIL": str(int(failure == "download")),
                         "MOCK_INSTALLER_EXIT": "42" if failure == "installer" else "0"},
                )
                events = (root / "events").read_text()
                self.assertEqual((root / "state/bootstrap-complete").exists(), failure is None)
                if failure:
                    self.assertEqual(result.returncode, {"apt": 1, "download": 22, "installer": 42}[failure])
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    if previous is None:
                        self.assertEqual(events.count("apt-get -o DPkg::Lock::Timeout=60 update"), 1)
                    self.assertEqual(events.count("rustup"), int(rust) + int(bool(previous and previous[1])))

    def test_apt_retries_lock_errors_for_update_and_both_installs(self):
        source = (ROOT / "infra/gcp/test-environment/startup-script.sh.tftpl").read_text(encoding="utf-8")
        body = source[source.index('if ! id "$TEST_USER"'):source.index("mapfile -t SSD_LINKS")]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            script = (
                f'set -Eeuo pipefail\nTEST_ROOT={shlex.quote(temp)}\nTEST_USER=tester\n'
                f'BOOTSTRAP_MARKER="$TEST_ROOT/state/bootstrap-complete"\nINSTALL_RUST=false\n'
                f'source {shlex.quote(str(ROOT / "scripts/fixtures/gcp_bootstrap_commands.sh"))}\n'
                + body.replace("/var/lib/gcp-test-environment", f"{temp}/state")
            )
            result = subprocess.run(
                ["bash", "-c", script], text=True, capture_output=True, check=False,
                env={**os.environ, "MOCK_USER_EXISTS": "0", "MOCK_PERF_AVAILABLE": "1",
                     "MOCK_APT_FAIL": "0", "MOCK_APT_UPDATE_LOCK_FAILURES": "1",
                     "MOCK_APT_INSTALL_LOCK_FAILURES": "1", "MOCK_APT_PERF_LOCK_FAILURES": "1"},
            )
            events = (root / "events").read_text()

        self.assertEqual(result.returncode, 0, result.stderr)
        apt_events = [line for line in events.splitlines() if line.startswith("apt-get ")]
        self.assertEqual(sum(line.endswith(" update") for line in apt_events), 2)
        self.assertEqual(sum("install -y --no-install-recommends bpftrace" in line for line in apt_events), 2)
        self.assertEqual(sum("install -y --no-install-recommends linux-tools-gcp" in line for line in apt_events), 2)
        self.assertEqual(events.count("sleep 10"), 3)

    def run_filesystem_setup(self, device: str, *, filesystem: str = "", signature: str = ""):
        source = (ROOT / "infra/gcp/test-environment/startup-script.sh.tftpl").read_text(encoding="utf-8")
        start = source.index('FILESYSTEM_TYPE="')
        end = source.index('\ninstall -d "$SSD_MOUNT"', start)
        script = r"""set -Eeuo pipefail
FILESYSTEM_DEVICE="$TEST_DEVICE"
SSD_DEVICES=(/dev/nvme-test-0 /dev/nvme-test-1)
blkid() { printf '%s' "$TEST_FILESYSTEM"; }
wipefs() {
    if [[ "${@: -1}" == "$FILESYSTEM_DEVICE" ]]; then
        printf '%s' "$TEST_SIGNATURE"
    else
        printf 'linux_raid_member'
    fi
}
mkfs.ext4() { printf 'formatted:%s\n' "${@: -1}"; }
""" + source[start:end].replace("$${", "${")
        return subprocess.run(
            ["bash", "-c", script], text=True, capture_output=True, check=False,
            env={**os.environ, "TEST_DEVICE": device, "TEST_FILESYSTEM": filesystem,
                 "TEST_SIGNATURE": signature},
        )

    def test_fresh_raid_formats_array_despite_expected_member_metadata(self):
        result = self.run_filesystem_setup("/dev/md/test-ssd")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "formatted:/dev/md/test-ssd")

    def test_fresh_single_ssd_formats_device(self):
        result = self.run_filesystem_setup("/dev/nvme-test-0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "formatted:/dev/nvme-test-0")

    def test_existing_ext4_is_not_reformatted(self):
        result = self.run_filesystem_setup("/dev/md/test-ssd", filesystem="ext4")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_other_signatures_and_filesystems_are_rejected(self):
        for device in ("/dev/md/test-ssd", "/dev/nvme-test-0"):
            for filesystem, signature in (("", "LVM2_member"), ("xfs", "xfs")):
                with self.subTest(device=device, filesystem=filesystem, signature=signature):
                    result = self.run_filesystem_setup(device, filesystem=filesystem, signature=signature)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("formatted:", result.stdout)

    def test_both_plans_use_storage_metadata_passed_to_bench_user(self):
        source = (ROOT / "scripts/gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        start = source.index("runuser -u bench -- env \\\n")
        end = source.index("    bash -s <<'BENCH_USER_SCRIPT'\n", start)
        handoff = source[start:end].replace(
            "runuser -u bench -- env", "env -i PATH=" + shlex.quote(os.environ["PATH"]), 1,
        )
        marker = 'python3 - "$BENCH_RESULTS/benchmark-plan.json"'
        plan_blocks = []
        offset = 0
        while (start := source.find(marker, offset)) != -1:
            offset = source.index("\nPY\n", start) + len("\nPY\n")
            plan_blocks.append(source[start:offset])
        self.assertEqual(len(plan_blocks), 2)

        for suite, block in zip(("rfc026-full", "batch64"), plan_blocks):
            for vcpus, disks in ((8, 1), (16, 2)):
                with self.subTest(suite=suite, vcpus=vcpus), tempfile.TemporaryDirectory() as temp:
                    outer = {
                        "COMMIT": "a" * 40, "NUM": "750000", "PROJECT": "test-project",
                        "ZONE": "us-central1-a", "INSTANCE": "test-host", "RESULTS": temp,
                        "WORK": temp, "SAFE_DEADLINE_EPOCH": "2000000000",
                        "MACHINE_TYPE": f"c4-standard-{vcpus}-lssd", "SSD_COUNT": str(disks),
                        "SSD_TOTAL_GIB": str(disks * 375), "WRITERS_CSV": "1,4,8", "SUITE": suite,
                        "BOOT_LOG": f"{temp}/bootstrap.log", "WAL_DEVICE": "/dev/test-ssd",
                    }
                    assignments = "\n".join(f"{key}={shlex.quote(value)}" for key, value in outer.items())
                    inner = (
                        "set -Eeuo pipefail\n"
                        "export PYTHONDONTWRITEBYTECODE=1\n"
                        f"export PYTHONPATH={shlex.quote(str(ROOT / 'scripts'))}\n"
                        'ACTUAL_COMMIT="$BENCH_COMMIT"\n'
                        f"BINARY_SHA256={'b' * 64}\n" + block
                    )
                    script = (
                        "set -Eeuo pipefail\n" + assignments + "\n" + handoff
                        + "    bash -s <<'BENCH_USER_SCRIPT'\n" + inner + "BENCH_USER_SCRIPT\n"
                    )
                    result = subprocess.run(["bash", "-c", script], text=True, capture_output=True, check=False)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    plan = json.loads((Path(temp) / "benchmark-plan.json").read_text(encoding="utf-8"))
                    self.assertEqual(plan["suite"], suite)
                    self.assertEqual(plan["vcpus"], vcpus)
                    self.assertEqual(plan["local_ssd_count"], disks)
                    self.assertEqual(plan["local_ssd_total_gib"], disks * 375)


if __name__ == "__main__":
    unittest.main()
