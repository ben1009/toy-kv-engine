# RFC 024: physical SSD environment mitigation attempts, 2026-10-04

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Neither of two additional reversible interventions stabilized the unchanged
parallel-WAL benchmark on this physical SSD. All 30 runs completed and their
original system settings were restored. No mitigation is retained as a fix.
The user requires physical-SSD measurements; RAM-backed ext4 is out of scope.

## CPU policy and affinity

The original environment uses the `powersave` governor with
`balance_performance` energy preference and allows all 32 logical CPUs. The
machine has eight performance cores with SMT and sixteen efficiency cores.
The treatment selects the `performance` governor and restricts the benchmark
to CPUs 0–15, the performance cores and their siblings.

The same retained executable is used in both environments, with 1 KiB values,
200,000 scored puts, and latency sampling every ten puts. Four writers use
the 1 MiB rotation workload; sixteen use a 1 GiB SST target. Two balanced
ABBA/BAAB blocks per case give four scored runs per environment and case,
plus four 50,000-put warmups: 20 runs total. Logs and executables reside in
RAM during timing; fresh databases reside on ext4. Runs are serial, with
five seconds idle between them.

The predeclared stability criterion requires all four scored runs per
environment/case to stay within a throughput max/min ratio of 1.05 and a p99
max/min ratio of 1.10. These are stability screens, not optimization scores.

| Writers | Environment | Throughput range (puts/s) | p99 range (ms) | Stability |
| --- | --- | ---: | ---: | --- |
| 4 | Original | 4,337–9,906 | 1.354–22.743 | Failed |
| 4 | Performance policy + P cores | 4,613–10,743 | 1.265–27.352 | Failed |
| 16 | Original | 23,127–23,996 | 1.524–1.905 | Failed |
| 16 | Performance policy + P cores | 29,056–30,898 | 1.440–1.865 | Failed |

The treatment changes throughput but does not prevent the large four-writer
stall or stabilize sixteen-writer tail latency. Governor and energy-preference
values for every policy match their recorded original values afterward.
Affinity applies only to benchmark processes. The treatment combines policy
and affinity; this experiment does not isolate their individual effects.

Artifacts: `target/rfc024-environment-20261004-f08mom9m/`.

## Host-memory buffer

HMB was enabled before the experiment. Linux 6.18 exposes a reversible HMB
sysfs control whose disable path releases the host buffer after a successful
controller command, and whose enable path sets it up again.
[Linux 6.18 NVMe PCI driver](https://raw.githubusercontent.com/torvalds/linux/v6.18/drivers/nvme/host/pci.c).

After CPU settings were restored, a second fixed experiment compared HMB on
and off through `/sys/class/nvme/nvme0/hmb`. It used the same binary and
four-writer rotation workload, two balanced blocks, four scored runs per arm,
and two warmups: ten runs total. The same stability thresholds applied.
Write-cache, power-management, and durability settings were not changed.

| HMB | Throughput range (puts/s) | p99 range (ms) | Stability |
| --- | ---: | ---: | --- |
| Enabled | 9,624–9,817 | 1.394–1.554 | Failed p99 |
| Disabled | 4,312–9,656 | 1.473–17.343 | Failed throughput and p99 |

The slow state recurred with HMB disabled. Disabling HMB is not a demonstrated
mitigation. The driver-reported enabled state was restored to `1` afterward.
The guard checks this state after each transition and during final restoration;
it does not claim to restore internal controller cache contents.

Artifacts: `target/rfc024-hmb-environment-20261004-lnhhlj2d/`.
Raw JSON outputs were checked against all recorded results, restoration was
verified, and owned database/RAM-stage cleanup completed. Each artifact
directory includes a SHA-256 manifest.

## Prepared next step: one-time LTS boot

The running and installed default kernel is `6.18.9-arch1-2`.
`6.12.71-1-lts`, its initramfs, and a valid systemd-boot entry already exist.
`bootctl list` confirms the normal kernel is still the default and selected
entry. No boot selection has been changed and no reboot has been issued.

The concrete next isolation step is a one-time LTS boot:

```sh
sudo systemctl reboot --boot-loader-entry=2026-02-13_14-18-55_linux-lts.conf
```

After reconnecting, run outside the io_uring-restricting sandbox:

```sh
python3 target/rfc024-lts-stability-20261004-05j9w2zd/run.py
```

The prepared runner requires the exact LTS kernel and refuses to overwrite an
existing experiment. It uses the saved retained binary, four 200,000-put runs
per writer count (4/8/16), one 50,000-put warmup per case, rotating case order,
RAM logs, fresh ext4 databases, and the same 5% throughput/10% p99 stability
limits. No kernel setting is modified by that runner.

A reboot resets device state as well as changing the kernel. A passing LTS
session would therefore be a candidate usable test environment, not proof that
the newer kernel caused the issue. Confirmation requires continued stability
and a controlled return to the original kernel. The reboot requires user
approval because it interrupts the host and the active agent session.

## Completed LTS stability test

After the explicitly approved reboot, `uname -r` confirmed `6.12.71-1-lts`.
The first runner invocation stopped before any benchmark because this older
kernel does not expose `/sys/class/nvme/nvme0/hmb`. The runner was adjusted to
record that metadata as unavailable; no HMB setting was changed or inferred.
The fixed 15-run protocol then completed: three warmups and four scored runs
at each writer count. The candidate SHA-256 remained
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.

| Writers | Throughput range (puts/s) | p99 range (ms) | Throughput max/min | p99 max/min | Stability |
| --- | ---: | ---: | ---: | ---: | --- |
| 4 | 4,324–9,983 | 1.345–21.090 | 2.309 | 15.675 | Failed both |
| 8 | 15,181–15,479 | 1.297–1.346 | 1.020 | 1.038 | Passed this session |
| 16 | 23,646–24,105 | 1.528–1.765 | 1.019 | 1.155 | Failed p99 |

The 4-writer runs alternated fast/slow/slow/fast without intervention.
Booting LTS did not eliminate the unstable state. This is not a qualified
before/after performance comparison or proof of an internal SSD cause.
Eight writers passed this fixed session, but that alone does not establish
persistent host stability. No host tuning settings were changed during the
benchmark. The host remains on LTS until a later reboot; the original default
boot entry was not changed.

Artifacts: `target/rfc024-lts-stability-20261004-05j9w2zd/`.
All 15 raw JSON outputs match the recorded results; completion and database
cleanup were verified. An artifact SHA-256 manifest preserves the runner,
protocol, binary, outputs, and summary.

## LTS I/O scheduler mitigation test

Tested `none` versus `mq-deadline` in fixed ABBA/BAAB order, four runs
per scheduler, using the same retained candidate on physical ext4. Each run
used four writers, 200,000 puts, 1 KiB values, 1 MiB SST rotation, and latency
sampling every ten operations; five seconds separated runs. No warmup was
excluded. Unlike the prior runner, the executable and small output files
resided on the SSD; results are within-session controls, not directly paired
with the earlier LTS test. The guard restored the original `none` scheduler.

| Scheduler | Throughput range (puts/s) | p99 range (ms) | Stability |
| --- | ---: | ---: | --- |
| none | 9,650–9,804 | 1.309–1.739 | Failed p99 (1.329x) |
| mq-deadline | 9,742–9,917 | 1.293–1.446 | Failed p99 (1.119x) |

Neither scheduler met the preset 5% throughput/10% p99 stability limits.
The large throughput collapse did not occur in either arm, so this experiment
cannot establish prevention of that state. No scheduler change was retained.
Host inspection showed 453 GiB available on the benchmark filesystem and no
obvious heavy competing process in the snapshot; this does not exclude transient
background I/O. No background services or durability settings were disabled.

Artifacts: `target/rfc024-scheduler-stability-20261004/`. All eight raw outputs
were verified against the recorded throughput values, and restoration was
verified. The environment remains unqualified for the full performance gate.

## Prepared transport isolation: direct NVMe instead of VMD

Read-only inspection confirms the P41 Plus is behind Intel VMD `8086:a77f`.
The LTS initramfs already contains `nvme`, `nvme-core`, `nvme-auth`, and `ext4`;
root is selected by PARTUUID and fstab uses filesystem UUIDs. These prerequisites
support trying direct NVMe, but do not guarantee the firmware exposes a toggle
or that boot succeeds after changing it. No RAID filesystem is shown in the
current mount/lsblk inventory.

The next proposed diagnostic requires the operator to disable VMD remapping
for this SSD in firmware, then explicitly select the same LTS kernel at boot.
Record the original firmware setting first; restore it if boot fails. Do not
create/delete RAID volumes, initialize the SSD, or format partitions. No BIOS
setting or boot entry was changed during preparation.

`target/rfc024-direct-nvme-stability-20261004/run.py` and the identical saved
candidate are prepared. The runner refuses to run if VMD remains in the NVMe
controller's sysfs ancestry or the kernel differs from `6.12.71-1-lts`.
It retains the fixed 4/8/16-writer protocol and stability limits. It has been
syntax checked, not benchmarked. A passing session would still need confirmation;
a reboot also resets controller state. This is an untested isolation step,
not a demonstrated fix or a claim that VMD causes the stalls.

## CPU idle-latency constraint: promising screen, failed validation

An SSH-safe test held `/dev/cpu_dma_latency` open with a signed 32-bit zero
request during treatment runs. Closing the descriptor removes the request;
see [Linux PM QoS documentation](https://docs.kernel.org/power/pm_qos_interface.html).
Unlike a governor change or a busy loop on one CPU, this requests a system-wide
CPU latency bound. It is not a guaranteed device-latency bound.

The eight-run ABBA/BAAB screen used four writers, 200,000 puts per run, and
1 MiB SST rotation. Default runs ranged from 4,352 to 9,967 puts/s and
1.237–16.920 ms p99. Constrained runs ranged from 10,550 to 11,214 puts/s
and 1.129–1.209 ms p99. Their throughput spread was still 6.3%, above the 5%
limit. Sysfs usage counters recorded zero additional entries into states 1–3
on every CPU during each treatment run, versus many entries in controls.
The short screen suggested a useful lead, not a qualified fix.

A separate fixed validation held the request continuously across the standard
15-run 4/8/16-writer protocol (three warmups, twelve scored runs), with the
unchanged candidate and physical ext4 databases. The inherited runner's
`no_setting_changes` field describes the child only; `guard.py` and
`qos-guard.json` document the parent's temporary CPU QoS constraint.

| Writers | Throughput range (puts/s) | p99 range (ms) | Stability |
| --- | ---: | ---: | --- |
| 4 | 8,498–10,526 | 1.176–3.521 | Failed both |
| 8 | 18,244–18,982 | 1.255–1.280 | Passed this session |
| 16 | 16,002–27,948 | 1.485–16.169 | Failed both |

The final sixteen-writer run collapsed despite the continuously held request.
This rejects CPU idle-latency control as a sufficient environment fix. It does
not prove the absence of a performance effect, nor qualify an optimization gain.
The request was released after both experiments; no persistent CPU, firmware,
boot, or durability changes were made. VMD bypass remains untested and cannot
be performed safely through this SSH-only session without firmware access and
a recovery route.

Artifacts: `target/rfc024-cpu-latency-stability-20261004/` and
`target/rfc024-qos-validation-20261004/`. All 23 raw benchmark outputs were
checked against recorded results; completion/release and artifact hashes were
recorded. The full performance environment remains unqualified.

## SSH firmware-control discovery and health check

Read-only checks found that this OptiPlex Micro Plus 7010 exposes Dell's
`dell-wmi-sysman` firmware-attribute interface. This corrects the earlier
assumption that the storage-mode setting necessarily requires a BIOS UI.
`EmbSataRaid` is displayed as `SATA/NVMe Operation`; its current value is `Raid`,
its supported values are `Disabled;Ahci;Raid;`, `pending_reboot` is `0`, and
BIOS Admin authentication `is_enabled` is `0`. No firmware write was attempted.
The kernel ABI documents firmware attribute reads/writes:
[firmware attributes ABI](https://github.com/torvalds/linux/blob/master/Documentation/ABI/testing/sysfs-class-firmware-attributes).

The concrete candidate operation, NOT executed, is to write `Ahci` to:
`/sys/class/firmware-attributes/dell-wmi-sysman/attributes/EmbSataRaid/current_value`,
verify readback, and reboot once into the same LTS entry. `Raid` is the recorded
original value. The direct-NVMe runner must verify that VMD is actually absent
before measuring; exposing the setting does not prove a successful transition.
Root PARTUUID, filesystem UUIDs, and NVMe/ext4 initramfs drivers were already
checked. A boot failure would still require a console/local recovery route;
a script on the root filesystem cannot guarantee rollback if Linux cannot boot.
This disruptive storage-mode test is awaiting informed approval and is not a
verified performance fix.

Outside benchmark timing, `smartctl -x -j` reports firmware `004C`, 48 C,
critical warning 0, media errors 0, error-log entries 0, warning-temperature time
0, critical-temperature time 0, spare 100%, and endurance used 4%. These counters
do not explain the latency transitions or prove that all device internals are
healthy. The current boot's warning/error journal shows firmware/legacy IRQ
routing messages but no NVMe timeout/reset or ext4 corruption message. It also
reports an unclean FAT boot volume; no filesystem repair was attempted on the
mounted boot partition, and this is not evidence of the ext4 benchmark stall.

## Completed direct-NVMe validation after approved storage-mode reboot

The user confirmed local recovery availability and explicitly approved switching
`Raid` to `Ahci` and rebooting. The guarded command required an `Ahci` readback
before requesting the one-time LTS boot. The tool connection terminated during
shutdown. After reconnecting, Linux reported `6.12.71-1-lts`; the SSD resolved to
`/sys/devices/pci0000:00/0000:00:01.1/0000:02:00.0`. PCI enumeration showed the
P41 Plus directly at `02:00.0`, with no VMD controller, and the runner's ancestry
guard passed. Thus the transport change actually took effect.

All fifteen planned runs completed using the identical candidate binary,
physical ext4 databases, RAM executable/log staging, and unchanged thresholds.

| Writers | Throughput range (puts/s) | p99 range (ms) | Stability |
| --- | ---: | ---: | --- |
| 4 | 4,340–9,533 | 1.272–17.140 | Failed both |
| 8 | 17,159–17,664 | 1.124–1.218 | Passed this session |
| 16 | 23,670–24,358 | 1.549–1.835 | Failed p99 |

Four-writer throughput followed slow/slow/fast/fast without intervention.
Bypassing VMD did not prevent the severe slowdown. VMD is therefore not a
necessary condition for the reproduced benchmark instability; this does not
identify the controller's internal cause or rule out all host influences.
Eight writers passed this session, not a general guarantee of stable storage.

The host remains in the approved direct-NVMe/Ahci configuration on LTS. No
restoration reboot or further firmware change was issued. The original storage
mode is `Raid`; reverting it requires another approved reboot. The normal boot
entry was not permanently changed.

Artifacts: `target/rfc024-direct-nvme-stability-20261004/`. All fifteen raw JSON
outputs match recorded results, the retained binary SHA-256 matches, database
cleanup completed, and an artifact manifest was saved. The inherited protocol
caveat about reboot/kernel causality should be read here as a transport
comparison caveat: the kernel was held constant and reboot also resets device
state. The full performance environment remains unqualified.

## Buffered WAL follow-up

The fixed same-binary direct/buffered comparison also failed to establish a
stable environment. See [buffered WAL diagnostic](rfc-024-buffered-wal-diagnostic-20261004.md)
for all 26 runs, verified file flags, unchanged durability calls, and limitations.
