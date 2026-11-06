variable "project_id" {
  description = "Existing GCP project. This configuration does not change billing or enable APIs."
  type        = string
}

variable "region" {
  description = "Region for the VPC subnet and test VM, for example us-central1."
  type        = string
}

variable "zone" {
  description = "Zone for the test VM, for example us-central1-a."
  type        = string
}

variable "machine_type" {
  description = "Machine type with bundled NVMe Local SSDs."
  type        = string
  default     = "c4-standard-16-lssd"
}

variable "expected_vcpus" {
  description = "Expected vCPU count; plan fails if machine-type metadata differs."
  type        = number
  default     = 16
}

variable "expected_local_ssd_count" {
  description = "Expected bundled NVMe Local SSD count; plan fails if metadata differs."
  type        = number
  default     = 2
}

variable "instance_name" {
  description = "Name of the reusable test VM."
  type        = string
  default     = "gcp-test-host"
}

variable "network_name" {
  description = "Name of the dedicated custom VPC managed by this Terraform root."
  type        = string
  default     = "gcp-test-network"
}

variable "subnetwork_name" {
  description = "Name of the regional subnet managed by this Terraform root."
  type        = string
  default     = "gcp-test-subnet"
}

variable "network_cidr" {
  description = "Private IPv4 range for the dedicated test subnet."
  type        = string
  default     = "10.200.0.0/24"
}

variable "ssh_source_cidr" {
  description = "Public IPv4 CIDR allowed to SSH; use your current public IP with /32."
  type        = string

  validation {
    condition     = can(cidrnetmask(var.ssh_source_cidr)) && tonumber(split("/", var.ssh_source_cidr)[1]) >= 24
    error_message = "ssh_source_cidr must be an IPv4 CIDR no broader than /24; /32 is recommended."
  }
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key installed for ssh_username."
  type        = string

  validation {
    condition     = endswith(var.ssh_public_key_path, ".pub")
    error_message = "ssh_public_key_path must point to a .pub file with a matching private key beside it."
  }
}

variable "ssh_username" {
  description = "Linux account used for interactive SSH and test work."
  type        = string
  default     = "tester"

  validation {
    condition     = can(regex("^[a-z_][a-z0-9_-]{0,31}$", var.ssh_username))
    error_message = "ssh_username must be a valid Linux account name."
  }
}

variable "local_ssd_mount" {
  description = "Mount point for the test workspace Local SSD filesystem."
  type        = string
  default     = "/mnt/test-ssd"

  validation {
    condition = startswith(var.local_ssd_mount, "/") && var.local_ssd_mount != "/" &&
      can(regex("^/[A-Za-z0-9._/-]+$", var.local_ssd_mount)) && !strcontains(var.local_ssd_mount, "..")
    error_message = "local_ssd_mount must be a simple non-root absolute path without shell-sensitive characters or '..'."
  }
}

variable "install_rust_toolchain" {
  description = "Install rustup with the minimal stable toolchain for Rust-based test projects."
  type        = bool
  default     = true
}

variable "boot_disk_size_gb" {
  description = "Hyperdisk Balanced boot disk size in GiB."
  type        = number
  default     = 50

  validation {
    condition     = var.boot_disk_size_gb >= 20 && var.boot_disk_size_gb <= 100
    error_message = "boot_disk_size_gb must be between 20 and 100 GiB."
  }
}

variable "max_runtime_hours" {
  description = "Server-enforced VM deletion after this many hours; Terraform network resources remain until destroy."
  type        = number
  default     = 24

  validation {
    condition = var.max_runtime_hours >= 1 && var.max_runtime_hours <= 168 &&
      var.max_runtime_hours == floor(var.max_runtime_hours)
    error_message = "max_runtime_hours must be a whole number between 1 and 168 hours."
  }
}

variable "estimated_hourly_cost_usd" {
  description = "Operator-checked hourly estimate for this VM and expected ancillary usage."
  type        = number

  validation {
    condition     = var.estimated_hourly_cost_usd > 0
    error_message = "estimated_hourly_cost_usd must be positive."
  }
}

variable "max_estimated_spend_usd" {
  description = "Maximum acceptable estimate for one max_runtime_hours VM lifetime."
  type        = number

  validation {
    condition     = var.max_estimated_spend_usd > 0
    error_message = "max_estimated_spend_usd must be positive."
  }
}

variable "confirm_free_trial" {
  description = "Explicitly attest that Billing Overview still shows Free Trial and Paid billing will not be enabled."
  type        = bool
  default     = false

  validation {
    condition     = var.confirm_free_trial
    error_message = "Check Billing Overview and set confirm_free_trial=true; this configuration never upgrades billing."
  }
}
