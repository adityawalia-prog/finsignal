terraform {
  required_providers {
    null = { source = "hashicorp/null" }
    local = { source = "hashicorp/local" }
  }
}

resource "null_resource" "k3d_cluster" {
  provisioner "local-exec" {
    command = <<EOT
      k3d cluster create finsignal \
        --port "8080:80@loadbalancer" \
        --port "9090:9090@loadbalancer" \
        --agents 2
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = "k3d cluster delete finsignal"
  }
}

resource "null_resource" "namespaces" {
  depends_on = [null_resource.k3d_cluster]
  provisioner "local-exec" {
    command = <<EOT
      kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
      kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
      kubectl create namespace logging --dry-run=client -o yaml | kubectl apply -f -
      kubectl create namespace finsignal --dry-run=client -o yaml | kubectl apply -f -
    EOT
  }
}
