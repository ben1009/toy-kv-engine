# GCP Free Trial WAL benchmark runner

`scripts/gcp-free-trial-wal-bench.py` provisions a disposable
`c4-standard-8-lssd` VM, mounts its instance-local Local SSD, builds one pinned
toy-kv-engine commit, and runs the V4/V7/V8 batch64 matrix with 1, 4, and 8
writers. It collects the raw JSON, stderr, commands, host metadata, and summary
before deleting the VM.

The matrix has 36 runs: for each writer count, it runs two reversed six-run
blocks (`V4, V7, V8, V8, V7, V4` and `V8, V7, V4, V4, V7, V8`). V4 uses
ordinary V4 WAL; V7 and V8 use PITR WAL. Each run uses 750,000 writes by
default, 1 KiB values, and 64-entry batches. The two blocks reduce order bias;
the output is a same-VM capture, not a storage-state-qualified V7/V8 comparison.

## Free Trial and safety limits

The runner never changes billing, enables APIs, adjusts quotas, or requests
quota increases. Before a real run, check the Cloud Billing Overview page and
confirm that the selected project is attached to a **Free Trial** billing
account. Pass `--confirm-free-trial` only after that check. The CLI does not
expose a reliable Free Trial-versus-Paid account-type field, so this step is an
explicit operator attestation. Do not upgrade the billing account to Paid.

Before creating any VM or firewall rule, the read-only preflight checks:

- the selected project is active and the Compute Engine and Cloud Quotas APIs
  are already enabled;
- the requested zone is up and exposes `c4-standard-8-lssd` as 8 vCPUs with
  exactly one 375 GB Local SSD;
- a ready Ubuntu 24.04 boot image is resolved and pinned by image name for the
  VM creation;
- C4 CPU and applicable Local SSD quota limits cover the new VM after
  accounting for existing instances in the region (family-scoped quota counts
  C4 use; legacy regional quota counts all Local SSD use);
- at least one regional VM instance quota and one ephemeral external IPv4
  address quota remain;
- at least 50 GB of regional Hyperdisk Balanced quota remains for the C4 boot
  disk, at least one global firewall quota remains, and any applicable
  `CPUS_ALL_REGIONS` quota has room.

If quota data, dimensions, or machine-type metadata are missing, the runner
fails closed. Existing C4 reservations in the region also stop preflight because
their quota consumption needs separate accounting. GCP does not expose a
read-only guarantee of currently free host
capacity. A passing check confirms zone support and quota headroom, but instance
creation can still fail if the zone is depleted or Free Trial access is
restricted. The runner does not retry by changing zones or billing state.

Provisioning requires an explicit IPv4 CIDR no broader than `/24` for SSH (a
single public IP as `/32` is preferred). Before creating resources, the runner
audits project VPC firewall rules and global and regional effective firewall
policies on the selected network. It refuses to proceed if an applicable SSH
allow rule permits a broader source, and repeats the audit immediately before
opening each SSH/SCP connection. This requires `compute.firewalls.list`,
`compute.networks.getEffectiveFirewalls`, and
`compute.networks.getRegionEffectiveFirewalls`; default networks with a
target-all `0.0.0.0/0` SSH rule must be narrowed or replaced with an isolated
VPC first. The audit is a point-in-time check: a project or organization actor
with firewall-policy permissions could change the effective policy after a
check. Do not run while another actor is changing firewall policy; use a
dedicated network or an enforced policy when concurrent policy changes cannot
be ruled out.
The VM blocks project-wide SSH keys, has no service account, and receives a
temporary firewall rule targeted only at its unique network tag; the rule is
removed during teardown. The VM has a server-side maximum runtime (default 180
minutes; the allowed range is 60–180 minutes) with automatic deletion, a 50 GB
auto-delete Hyperdisk Balanced boot disk, and one automatically attached Local
SSD from the `-lssd` machine type. The runner
formats only `/dev/disk/by-id/google-local-nvme-ssd-0`, after verifying that
the symlink resolves to a sufficiently large, unmounted block device with no
existing filesystem signature. It refuses to guess a device by size or to
format any other disk.

Local SSD data is ephemeral. Results are copied back to the local results
directory before VM deletion. `bench-results/` is ignored by Git. If result
transfer still fails after two attempts, the runner preserves the local
collection-attempt directories, transfer diagnostics, bootstrap log if
available, and orchestration logs, then proceeds with VM and firewall cleanup.
The runner prints the local artifact directory on success and failure. A
complete remote benchmark result cannot be returned when every transfer
attempt fails; `artifact-collection.json` records that condition and the local
diagnostics remain available. On normal completion, failed benchmark runs, or
when the benchmark never started, cleanup attempts to delete the VM and
firewall rule, then describes each resource and records `absent`, `present`, or
`unknown` in `lifecycle.json`. A resource that is present or cannot be verified
causes the run to report cleanup failure. A force-kill or host failure can
still leave the temporary firewall rule behind; the lifecycle record contains
its name for manual cleanup, and the VM has a server-side deletion deadline.
Before retrying, the runner refuses to provision while any non-terminated
`walbench-*` VM remains in the project, preventing an interrupted benchmark
from silently creating a second VM.

## Requirements and preflight

Install the Google Cloud CLI, its beta component, and authenticate it. For
component-managed CLI installs, add beta with `gcloud components install beta`.
The project must already
have `compute.googleapis.com` and `cloudquotas.googleapis.com` enabled. The
runner uses `gcloud beta compute instances create` to set a server-enforced
delete deadline; its preflight checks that the installed CLI exposes the
required flags. Local `ssh`, `scp`, and `ssh-keygen` are also required for a
full run. Use an existing VPC network with outbound internet access and
permissions to create/delete a VM and temporary firewall rule. For a
custom-mode VPC, pass `--subnet SUBNETWORK_NAME`; preflight verifies that the
subnetwork belongs to the selected network and requested region.

The active identity needs read access to the project, enabled services, quota
and Compute Engine inventory; access to C4 machine types and the Ubuntu image;
and permission to create/delete the VM and boot disk, create/delete the
temporary firewall rule, use the selected network/subnetwork, and read VM
guest attributes. Applicable IAM permission names include
`resourcemanager.projects.get`, `serviceusage.services.list`,
`cloudquotas.quotas.get`, `compute.projects.get`, `compute.zones.get`,
`compute.regions.get`, `compute.machineTypes.get`, `compute.images.get`,
`compute.images.useReadOnly`, `compute.networks.get`,
`compute.subnetworks.get`, `compute.instances.list`,
`compute.instances.create`, `compute.instances.get`,
`compute.instances.getGuestAttributes`, `compute.instances.delete`,
`compute.disks.create`, `compute.disks.delete`, `compute.networks.use`,
`compute.subnetworks.use`, `compute.reservations.list`,
`compute.firewalls.list`, `compute.networks.getEffectiveFirewalls`,
`compute.networks.getRegionEffectiveFirewalls`, `compute.firewalls.create`, and
`compute.firewalls.delete`. Project policies can require additional
permissions. Use a least-privilege custom role or
existing roles that grant these operations; the runner itself never grants
roles or enables APIs.

First run a read-only preflight. This does not create any cloud resources and
does not need the Free Trial attestation:

```bash
python3 scripts/gcp-free-trial-wal-bench.py \
  --project YOUR_PROJECT_ID \
  --zone us-central1-a \
  --commit 0123456789abcdef0123456789abcdef01234567 \
  --preflight-only
```

After confirming the Free Trial status in Billing Overview and choosing your
current public IPv4 CIDR, run the benchmark:

```bash
python3 scripts/gcp-free-trial-wal-bench.py \
  --project YOUR_PROJECT_ID \
  --zone us-central1-a \
  --commit 0123456789abcdef0123456789abcdef01234567 \
  --ssh-source-range 203.0.113.10/32 \
  --estimated-hourly-cost-usd YOUR_CURRENT_ESTIMATE \
  --max-estimated-spend-usd YOUR_SPEND_CEILING \
  --confirm-free-trial
```

Replace the example commit, CIDR, and cost values. Use a full 40-character
commit SHA. The two cost arguments are mandatory for a full run: look up
current pricing for the selected region and estimate the VM, Local SSD, boot
disk, external IP, and expected egress. The runner refuses to start if that
estimate multiplied by the maximum VM runtime exceeds the supplied ceiling.
This is a local guard based on operator input, not a Google Cloud billing cap;
it cannot constrain unmodeled charges. It never switches the account to Paid.

The default result directory is `bench-results/gcp-wal-<UTC timestamp>`; pass
`--results-dir` to choose another local path. The path must not already exist;
the runner reserves it atomically and refuses to share it with another run.
`--num` changes writes per run
(it must be at least 64). `--max-runtime-minutes` accepts 60–180 minutes and
sets the VM auto-delete deadline. The runner exits before creating resources
when quota or machine support cannot be verified.

The runner enables guest attributes and obtains the VM's SSH host keys from
the Compute Engine API before connecting. SSH and SCP use strict host-key
checking against that API response. The runner polls guest attributes at a
bounded rate. See [guest attributes and SSH
host-key verification](https://docs.cloud.google.com/compute/docs/metadata/manage-guest-attributes).

## Collected artifacts

The local result directory includes the preflight quota report, GCP command
log, VM identity and termination deadline, remote session log, collected
Local SSD contents, build SHA/toolchain, benchmark plan, each run's command,
raw JSON and stderr, 100 ms timeline captures when supported by the benchmark,
`summary.tsv`, `comparison.tsv` (per-writer V4/V7/V8 medians and throughput,
p99, and CPU percentage deltas against V4),
`summary.json`, and lifecycle/cleanup status. A comparison is descriptive for
this capture only; it does not assert that the device stayed in one storage
state. A nonzero benchmark exit still triggers artifact collection and VM
teardown attempts. If results transfer remains incomplete, the runner still
deletes the VM and temporary firewall rule, so remote results cannot be
recovered later; incomplete collection or failed teardown returns a nonzero
status.

## Cloud references

- [C4 machine types](https://docs.cloud.google.com/compute/docs/general-purpose-machines)
- [Local SSD quotas](https://docs.cloud.google.com/compute/resource-usage)
- [Linux Local SSD device names and formatting](https://docs.cloud.google.com/compute/docs/disks/add-local-ssd)
- [Automatically limit VM runtime](https://docs.cloud.google.com/compute/docs/instances/limit-vm-runtime)
- [Google Cloud Free Trial FAQs](https://cloud.google.com/signup-faqs)
