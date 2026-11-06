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

- Terraform 1.6 or later.
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

The VM is configured for server-side deletion after `max_runtime_hours` (24 by
default, at most 168). The VPC, subnet, and SSH firewall rule remain managed by
Terraform after the VM expires. Reapply to create a fresh VM, or destroy the
whole environment when finished:

```bash
terraform destroy -var-file=terraform.tfvars
```

Destroying or expiring the VM loses Local SSD contents. Copy any needed results
off the host first. The Terraform cost guard only checks your supplied estimate
against the configured lifetime; it cannot limit charges for other resources,
network traffic, or an incorrect estimate.
