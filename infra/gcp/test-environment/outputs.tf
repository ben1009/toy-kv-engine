output "project_id" {
  value = var.project_id
}

output "instance_name" {
  value = google_compute_instance.test.name
}

output "zone" {
  value = google_compute_instance.test.zone
}

output "external_ip" {
  value = google_compute_instance.test.network_interface[0].access_config[0].nat_ip
}

output "local_ssd_count" {
  value = local.local_ssd_count
}

output "local_ssd_mount" {
  value = var.local_ssd_mount
}

output "ssh_command" {
  value = "ssh -i ${trimsuffix(var.ssh_public_key_path, ".pub")} ${var.ssh_username}@${google_compute_instance.test.network_interface[0].access_config[0].nat_ip}"
}

output "destroy_command" {
  value = "terraform destroy -var-file=terraform.tfvars"
}
