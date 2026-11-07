# All provider operations, including the apply/cleanup test, are mocked.
mock_provider "google-beta" {
  mock_data "google_compute_machine_types" {
    defaults = {
      machine_types = [{
        guest_cpus = 16
        bundled_local_ssds = [{
          partition_count   = 2
          default_interface = "NVME"
        }]
      }]
    }
  }

  mock_data "google_compute_image" {
    defaults = {
      self_link = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/test-image"
    }
  }
}

variables {
  project_id                = "test-project"
  region                    = "us-central1"
  zone                      = "us-central1-a"
  ssh_source_cidr           = "203.0.113.10/32"
  ssh_public_key_path       = "tests/fixtures/test_ssh.pub"
  estimated_hourly_cost_usd = 1
  max_estimated_spend_usd   = 24
  confirm_free_trial        = true
}

run "default_hardware_and_lifetime" {
  command = plan

  assert {
    condition = (
      google_compute_instance.test.machine_type == "c4-standard-16-lssd" &&
      local.local_ssd_count == 2 &&
      google_compute_instance.test.boot_disk[0].auto_delete &&
      google_compute_instance.test.boot_disk[0].initialize_params[0].size == 50 &&
      google_compute_instance.test.boot_disk[0].initialize_params[0].type == "hyperdisk-balanced" &&
      google_compute_instance.test.boot_disk[0].initialize_params[0].provisioned_iops == 3000 &&
      google_compute_instance.test.boot_disk[0].initialize_params[0].provisioned_throughput == 140 &&
      !google_compute_instance.test.deletion_protection &&
      google_compute_instance.test.scheduling[0].max_run_duration[0].seconds == 86400 &&
      google_compute_instance.test.scheduling[0].instance_termination_action == "DELETE"
    )
    error_message = "The default host must have the expected storage and a bounded, auto-deleted lifetime."
  }

  assert {
    condition = (
      !google_compute_network.test.auto_create_subnetworks &&
      google_compute_subnetwork.test.region == "us-central1" &&
      google_compute_subnetwork.test.ip_cidr_range == "10.200.0.0/24" &&
      google_compute_firewall.ssh.source_ranges == toset(["203.0.113.10/32"]) &&
      one(google_compute_firewall.ssh.allow).ports == tolist(["22"]) &&
      one(google_compute_firewall.ssh.allow).protocol == "tcp" &&
      google_compute_instance.test.metadata["block-project-ssh-keys"] == "TRUE" &&
      google_compute_instance.test.metadata["enable-oslogin"] == "FALSE" &&
      length(google_compute_instance.test.service_account[0].scopes) == 0
    )
    error_message = "SSH and the dedicated network must remain restricted to the supplied test-host access."
  }
}

run "eight_cpu_single_ssd" {
  command = plan
  variables {
    machine_type             = "c4-standard-8-lssd"
    expected_vcpus           = 8
    expected_local_ssd_count = 1
  }
  override_data {
    target = data.google_compute_machine_types.selected
    values = {
      machine_types = [{
        guest_cpus         = 8
        bundled_local_ssds = [{ partition_count = 1, default_interface = "NVME" }]
      }]
    }
  }
  assert {
    condition     = local.local_ssd_count == 1 && strcontains(local.startup_script, "SSD_COUNT='1'")
    error_message = "The smaller host must render a single-SSD startup script."
  }
}

run "minimum_disk_and_runtime" {
  command = plan
  variables {
    boot_disk_size_gb = 20
    max_runtime_hours = 1
  }
}

run "maximum_disk_and_runtime" {
  command = plan
  variables {
    boot_disk_size_gb       = 100
    max_runtime_hours       = 168
    max_estimated_spend_usd = 168
  }
}

run "mocked_create_connect_and_cleanup" {
  command = apply
  assert {
    condition = (
      google_compute_subnetwork.test.network == google_compute_network.test.id &&
      google_compute_instance.test.network_interface[0].subnetwork == google_compute_subnetwork.test.id &&
      output.instance_name == "gcp-test-host" && output.zone == "us-central1-a" &&
      output.ssh_command == "ssh -i tests/fixtures/test_ssh tester@${output.external_ip}" &&
      output.destroy_command == "terraform destroy -var-file=terraform.tfvars"
    )
    error_message = "The mock apply must connect the managed resources and produce usable lifecycle commands."
  }
}

run "missing_machine_metadata" {
  command = plan
  override_data {
    target = data.google_compute_machine_types.selected
    values = { machine_types = [] }
  }
  expect_failures = [google_compute_instance.test]
}

run "wrong_cpu_count" {
  command = plan
  variables { expected_vcpus = 8 }
  expect_failures = [google_compute_instance.test]
}

run "wrong_ssd_count" {
  command = plan
  variables { expected_local_ssd_count = 1 }
  expect_failures = [google_compute_instance.test]
}

run "wrong_ssd_interface" {
  command = plan
  override_data {
    target = data.google_compute_machine_types.selected
    values = {
      machine_types = [{
        guest_cpus         = 16
        bundled_local_ssds = [{ partition_count = 2, default_interface = "SCSI" }]
      }]
    }
  }
  expect_failures = [google_compute_instance.test]
}

run "missing_bundled_ssds" {
  command = plan
  override_data {
    target = data.google_compute_machine_types.selected
    values = { machine_types = [{ guest_cpus = 16, bundled_local_ssds = [] }] }
  }
  expect_failures = [google_compute_instance.test]
}

run "cost_limit" {
  command = plan
  variables { max_estimated_spend_usd = 23 }
  expect_failures = [google_compute_instance.test]
}

run "free_trial_attestation_required" {
  command = plan
  variables { confirm_free_trial = false }
  expect_failures = [var.confirm_free_trial]
}

run "fractional_boot_disk" {
  command = plan
  variables { boot_disk_size_gb = 50.5 }
  expect_failures = [var.boot_disk_size_gb]
}

run "undersized_boot_disk" {
  command = plan
  variables { boot_disk_size_gb = 19 }
  expect_failures = [var.boot_disk_size_gb]
}

run "oversized_boot_disk" {
  command = plan
  variables { boot_disk_size_gb = 101 }
  expect_failures = [var.boot_disk_size_gb]
}

run "fractional_runtime" {
  command = plan
  variables { max_runtime_hours = 1.5 }
  expect_failures = [var.max_runtime_hours]
}

run "zero_runtime" {
  command = plan
  variables { max_runtime_hours = 0 }
  expect_failures = [var.max_runtime_hours]
}

run "excessive_runtime" {
  command = plan
  variables { max_runtime_hours = 169 }
  expect_failures = [var.max_runtime_hours]
}

run "nonpositive_hourly_estimate" {
  command = plan
  variables { estimated_hourly_cost_usd = 0 }
  expect_failures = [var.estimated_hourly_cost_usd]
}

run "nonpositive_spend_limit" {
  command = plan
  variables { max_estimated_spend_usd = 0 }
  expect_failures = [var.max_estimated_spend_usd]
}

run "broad_ssh_range" {
  command = plan
  variables { ssh_source_cidr = "0.0.0.0/0" }
  expect_failures = [var.ssh_source_cidr]
}

run "ipv6_ssh_range" {
  command = plan
  variables { ssh_source_cidr = "2001:db8::/64" }
  expect_failures = [var.ssh_source_cidr]
}

run "malformed_ssh_range" {
  command = plan
  variables { ssh_source_cidr = "not-a-cidr" }
  expect_failures = [var.ssh_source_cidr]
}

run "invalid_username" {
  command = plan
  variables { ssh_username = "bad;user" }
  expect_failures = [var.ssh_username]
}

run "root_mount_rejected" {
  command = plan
  variables { local_ssd_mount = "/" }
  expect_failures = [var.local_ssd_mount]
}

run "root_mount_dot_alias_rejected" {
  command = plan
  variables { local_ssd_mount = "/." }
  expect_failures = [var.local_ssd_mount]
}

run "root_mount_dot_slash_alias_rejected" {
  command = plan
  variables { local_ssd_mount = "/./" }
  expect_failures = [var.local_ssd_mount]
}

run "root_mount_slash_alias_rejected" {
  command = plan
  variables { local_ssd_mount = "//" }
  expect_failures = [var.local_ssd_mount]
}

run "mount_dot_component_rejected" {
  command = plan
  variables { local_ssd_mount = "/mnt/./test" }
  expect_failures = [var.local_ssd_mount]
}

run "mount_empty_component_rejected" {
  command = plan
  variables { local_ssd_mount = "/mnt//test" }
  expect_failures = [var.local_ssd_mount]
}

run "mount_traversal_rejected" {
  command = plan
  variables { local_ssd_mount = "/mnt/../etc" }
  expect_failures = [var.local_ssd_mount]
}

run "mount_shell_characters_rejected" {
  command = plan
  variables { local_ssd_mount = "/mnt/test;echo" }
  expect_failures = [var.local_ssd_mount]
}

run "private_key_path_rejected" {
  command = plan
  variables { ssh_public_key_path = "tests/fixtures/test_ssh" }
  expect_failures = [var.ssh_public_key_path]
}
