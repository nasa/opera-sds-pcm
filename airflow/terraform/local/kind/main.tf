terraform {
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.4"
    }
  }
}

locals {
    host_paths = {
        dags = var.dags_path
        aws  = var.aws_path
    }

  kind_mounts = [
    {
      host_path      = local.host_paths.dags
      container_path = local.host_paths.dags
    },
    {
      host_path      = local.host_paths.aws
      container_path = local.host_paths.aws
    }
  ]
}

resource "kind_cluster" "airflow_cluster" {
  name = var.cluster_name

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"
      
      extra_port_mappings {
        container_port = 30080
        host_port      = var.webserver_port
        protocol       = "TCP"
      }
      
      dynamic "extra_mounts" {
        for_each = local.kind_mounts
        content {
          host_path      = extra_mounts.value.host_path
          container_path = extra_mounts.value.container_path
        }
      }
    }

    node {
      role = "worker"
      
      dynamic "extra_mounts" {
        for_each = local.kind_mounts
        content {
          host_path      = extra_mounts.value.host_path
          container_path = extra_mounts.value.container_path
        }
      }
    }
    
    node {
      role = "worker"
      
      dynamic "extra_mounts" {
        for_each = local.kind_mounts
        content {
          host_path      = extra_mounts.value.host_path
          container_path = extra_mounts.value.container_path
        }
      }
    }
  }
}