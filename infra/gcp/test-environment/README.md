# Reusable GCP test environment

This Terraform root creates a reusable, general-purpose GCP test host with
bundled Local SSD storage. It does not build or run a particular benchmark, clone
a repository, enable APIs, change quotas, or modify billing. After setup, SSH to
the host and run the tests you need.

The defaults target `c4-standard-16-lssd` in a Free Trial project: 16 vCPUs,
two bundled NVMe Local SSDs, a dedicated VPC and subnet, and a 50 GiB
Hyperdisk Balanced boot disk. The startup script validates the Local SSDs and
mounts an ext4 filesystem at `/mnt/test-ssd`; use `/mnt/test-ssd/work` for
working data and `/mnt/test-ssd/results` for artifacts. The Local SSD is
ephemeral; its contents do not survive VM deletion or recreation.

## Requirements

- Terraform 1.7 or later (the bundled tests use provider mocks).
- Google Cloud CLI authenticated to the intended project, with Application
  Default Credentials available to the Terraform provider.
- `compute.googleapis.com` and `cloudquotas.googleapis.com` already enabled.
- A public SSH key and your current public IPv4 address.
- IAM read access to project, machine-type, image, disk, instance, and quota
  inventory, plus permission to create/delete the VM, boot disk, custom VPC,
  subnet, and SSH firewall rule. Project policies can require more.
- The project must remain on Free Trial billing. This setup never enables Paid
  billing and never enables APIs or requests quota changes.

Check Billing Overview yourself before provisioning. Terraform requires an
explicit `confirm_free_trial = true` attestation but cannot determine the
project's billing-account type. The cost check is a local guard using your
estimate; it is not a GCP billing cap.

Authenticate both the CLI preflight and Terraform provider without changing
billing or enabling APIs:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login
```

## Read-only capacity preflight

Run the preflight before Terraform. It checks zone and machine-type support,
bundled Local SSD metadata, relevant regional and global quota, existing VM and
disk usage, and boot-disk quota. It does not create resources or enable APIs.
It cannot guarantee that GCP currently has free host capacity.

The boot disk explicitly uses 3,000 IOPS and 140 MiB/s, the
[Hyperdisk Balanced baseline](https://docs.cloud.google.com/compute/docs/disks/hd-types/hyperdisk-balanced#baseline_performance_and_quota).
These settings consume no performance quota; the preflight checks capacity
quota. Tests and results use the separate Local SSD workspace.

```bash
python3 infra/gcp/test-environment/preflight.py \
  --project YOUR_PROJECT_ID \
  --zone us-central1-a \
  --machine-type c4-standard-16-lssd \
  --boot-disk-gb 50 \
  --output /tmp/gcp-test-environment-preflight.json
```

If the check fails or cannot account for a quota, stop before applying the
Terraform configuration. The preflight supports C4 Local SSD machines because
that is the configured environment target.

## Configure Terraform

From this directory, create an untracked `terraform.tfvars` file. Look up the
current regional hourly estimate for the VM and attached storage before setting
the cost values.

```hcl
project_id               = "YOUR_PROJECT_ID"
region                   = "us-central1"
zone                     = "us-central1-a"
machine_type             = "c4-standard-16-lssd"
expected_vcpus           = 16
expected_local_ssd_count = 2
ssh_source_cidr          = "YOUR_PUBLIC_IPV4/32"
ssh_public_key_path      = "/home/YOU/.ssh/id_ed25519.pub"
estimated_hourly_cost_usd = 1.00 # Replace with the current estimate.
max_estimated_spend_usd   = 24.00 # Must cover hourly estimate × max runtime.
max_runtime_hours         = 24
confirm_free_trial        = true # Only after checking Billing Overview.
```

Use a narrow SSH source range, preferably one public address as `/32`. Do not
commit `terraform.tfvars`, private SSH keys, state files, or plan files.

## Create and connect

After the read-only preflight passes and the project still uses Free Trial
billing:

```bash
terraform init
terraform plan -var-file=terraform.tfvars -out=/tmp/gcp-test-environment.tfplan
terraform apply /tmp/gcp-test-environment.tfplan
terraform output -raw ssh_command
```

Run the printed SSH command. Rust is installed for the `tester` account by
default; run `source ~/.cargo/env` after connecting. The host also has common
build, profiling, storage, and tracing tools. The startup script records its
status in `/mnt/test-ssd/environment.txt` and
`/var/log/gcp-test-environment-bootstrap.log`.

The tool-installation marker records `ssh_username` and `install_rust_toolchain`.
If either setting changes, the next startup or manual bootstrap rerun installs
tools for the new configuration before updating that marker.

The VM is configured for server-side deletion at an absolute deadline derived
from the first Terraform apply and `max_runtime_hours` (24 by default, at most
168). Terraform state stores this lifetime anchor; keep that state. Stopping,
starting, or reapplying the VM does not reset the deadline. To start a new
environment lifetime, destroy and recreate the environment. The VPC, subnet,
and SSH firewall rule remain managed by Terraform after the VM expires:

```bash
terraform destroy -var-file=terraform.tfvars
```

Destroying or expiring the VM loses Local SSD contents. Copy any needed results
off the host first. The Terraform cost guard only checks your supplied estimate
against the configured lifetime; it cannot limit charges for other resources,
network traffic, or an incorrect estimate.

The VM has no attached service account, so workloads on the host cannot obtain
Google Cloud credentials from its metadata server. SSH access uses the supplied
public key and the configured Linux account.

## Automated checks

The `gcp-test-environment` CI job installs a checksum-pinned Terraform CLI, verifies
formatting and the locked provider schema, runs the module's mock-provider
tests, parses the rendered startup script with Bash, and discovers all
`scripts/test_gcp*.py` tests. It needs no GCP credentials and creates no cloud
resources. Run the same checks from the repository root:

```bash
terraform -chdir=infra/gcp/test-environment fmt -check -recursive
terraform -chdir=infra/gcp/test-environment init -backend=false -input=false -lockfile=readonly
terraform -chdir=infra/gcp/test-environment validate
terraform -chdir=infra/gcp/test-environment test
bash -n scripts/gcp-free-trial-wal-bench-remote.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_gcp*.py'
```

These tests require Terraform 1.7 or later for
[provider mocking](https://developer.hashicorp.com/terraform/language/tests/mocking);
CI pins 1.17.1. Terraform also reads the test files during initialization and
validation, so this root requires 1.7 or later even when not running tests.
Keep `.terraform.lock.hcl` in version control when changing provider versions.
The public key under `tests/fixtures` belongs only to the offline tests.

| Area | Automated coverage |
| --- | --- |
| Capacity preflight | Both VM shapes, applicable quotas, existing CPU/SSD/disk usage, reservations, missing metadata, command failures, and read-only gcloud invocation |
| Terraform | Hardware checks, cost and Free Trial guards, input bounds, restricted SSH, boot-disk deletion, VM lifetime, resource links, outputs, and mocked apply/cleanup |
| Bootstrap | One/two SSDs, fresh RAID, reassembly, repeat startup, filesystem preservation, unexpected signatures, missing/undersized devices, mounted UUID verification, download/installer failure and readiness publication |
| Templates and benchmark runner | Actual Terraform rendering for both disk counts and Rust settings, Bash syntax, environment handoff, schedules, result validation and artifact collection |

## Live-host acceptance check

Offline checks do not verify GCP host capacity, IAM policy, package mirrors,
actual kernel RAID behavior, or SSH connectivity. After an intentional
provisioning run, use the printed SSH command to check the host:

```bash
test -s /mnt/test-ssd/environment.txt
cat /mnt/test-ssd/environment.txt
findmnt -n -o SOURCE,FSTYPE,TARGET /mnt/test-ssd
test -w /mnt/test-ssd/work && test -w /mnt/test-ssd/results
sudo mdadm --detail /dev/md/test-ssd # Only for hosts with multiple SSDs.
command -v cc clang cmake fio git jq mdadm nvme python3 trace-cmd bpftrace
source ~/.cargo/env && rustc --version # When install_rust_toolchain is true.
```

For a new bootstrap/provider revision, save a small marker file under the
workspace, reboot the host, reconnect, and verify the marker, mount and
filesystem UUID are unchanged. When the environment is no longer needed, run
`terraform destroy -var-file=terraform.tfvars` and confirm the named VM, boot
disk, SSH rule, subnet and VPC are absent from `gcloud compute` inventory. This
live check uses the existing Free Trial project; it must not upgrade billing or
enable services. It is separate from CI so pull requests cannot create billed
resources automatically.
