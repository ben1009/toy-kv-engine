data "google_compute_machine_types" "selected" {
  provider = google-beta
  zone     = var.zone
  filter   = "name = ${var.machine_type}"
}

data "google_compute_image" "ubuntu" {
  provider = google-beta
  family   = "ubuntu-2404-lts-amd64"
  project  = "ubuntu-os-cloud"
}

locals {
  machine_type = one(data.google_compute_machine_types.selected.machine_types)
  local_ssd_count = try(
    tonumber(one(local.machine_type.bundled_local_ssds).partition_count),
    0,
  )
  startup_script = templatefile("${path.module}/startup-script.sh.tftpl", {
    install_rust_toolchain = var.install_rust_toolchain
    local_ssd_count        = local.local_ssd_count
    local_ssd_mount        = var.local_ssd_mount
    ssh_username           = var.ssh_username
  })
}

resource "google_compute_network" "test" {
  provider                = google-beta
  name                    = var.network_name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
  description             = "Reusable network for interactive GCP test environments"
}

resource "google_compute_subnetwork" "test" {
  provider                 = google-beta
  name                     = var.subnetwork_name
  region                   = var.region
  network                  = google_compute_network.test.id
  ip_cidr_range            = var.network_cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "ssh" {
  provider    = google-beta
  name        = "${var.instance_name}-ssh"
  network     = google_compute_network.test.name
  description = "Restricted SSH access for the reusable GCP test host"
  direction   = "INGRESS"
  priority    = 1000

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.ssh_source_cidr]
  target_tags   = ["gcp-test-environment"]
}

resource "google_compute_instance" "test" {
  provider                  = google-beta
  name                      = var.instance_name
  zone                      = var.zone
  machine_type              = var.machine_type
  can_ip_forward            = false
  allow_stopping_for_update = true
  deletion_protection       = false
  tags                      = ["gcp-test-environment"]
  metadata = {
    ssh-keys               = "${var.ssh_username}:${trimspace(file(var.ssh_public_key_path))}"
    block-project-ssh-keys = "TRUE"
    enable-oslogin         = "FALSE"
    serial-port-enable     = "FALSE"
    startup-script         = local.startup_script
  }

  boot_disk {
    auto_delete = true

    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = var.boot_disk_size_gb
      type  = "hyperdisk-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.test.id

    access_config {}
  }

  service_account {
    scopes = []
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "TERMINATE"
    max_run_duration {
      seconds = var.max_runtime_hours * 3600
    }
    instance_termination_action = "DELETE"
  }

  depends_on = [google_compute_firewall.ssh]

  lifecycle {
    precondition {
      condition     = try(local.machine_type.guest_cpus, 0) == var.expected_vcpus
      error_message = "Selected machine type does not match expected_vcpus. Check machine-type availability before applying."
    }

    precondition {
      condition = (
        local.local_ssd_count == var.expected_local_ssd_count &&
        try(one(local.machine_type.bundled_local_ssds).default_interface, "") == "NVME"
      )
      error_message = "Selected machine type must expose the expected bundled NVMe Local SSDs."
    }

    precondition {
      condition     = var.estimated_hourly_cost_usd * var.max_runtime_hours <= var.max_estimated_spend_usd
      error_message = "The estimated VM lifetime cost exceeds max_estimated_spend_usd. Recheck current GCP pricing and limits."
    }
  }
}
