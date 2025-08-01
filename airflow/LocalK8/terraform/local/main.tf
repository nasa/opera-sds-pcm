terraform {
  required_version = ">= 1.0"
  
  backend "local" {
    path = "terraform_state/terraform.tfstate"
  }
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.4"
    }
    
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
    
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.24"
    }
  }
}

locals {
  host_paths = {
    dags = "/Users/Verweyen/Projects/LocalK8/dags"
    aws  = "/Users/Verweyen/.aws"
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

  airflow_volumes = [
    {
      name = "local-dags"
      hostPath = {
        path = local.host_paths.dags
        type = "DirectoryOrCreate"
      }
    },
    {
      name = "aws-credentials"
      hostPath = {
        path = local.host_paths.aws
        type = "DirectoryOrCreate"
      }
    }
  ]
  
  airflow_volume_mounts = [
    {
      name = "local-dags"
      mountPath = "/opt/airflow/dags"
      readOnly = false
    },
    {
      name = "aws-credentials"
      mountPath = "/home/airflow/.aws"
      readOnly = true
    }
  ]
  
  # Shared AWS environment variables
  aws_env_vars = [
    {
      name = "AWS_PROFILE"
      value = "saml-pub"
    },
    {
      name = "AWS_SHARED_CREDENTIALS_FILE"
      value = "/home/airflow/.aws/credentials"
    },
    {
      name = "AWS_CONFIG_FILE"
      value = "/home/airflow/.aws/config"
    }
  ]
}

provider "kind" {}

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

provider "kubernetes" {
  host                   = kind_cluster.airflow_cluster.endpoint
  cluster_ca_certificate = kind_cluster.airflow_cluster.cluster_ca_certificate
  client_certificate     = kind_cluster.airflow_cluster.client_certificate
  client_key             = kind_cluster.airflow_cluster.client_key
}

provider "helm" {
  kubernetes {
    host                   = kind_cluster.airflow_cluster.endpoint
    cluster_ca_certificate = kind_cluster.airflow_cluster.cluster_ca_certificate
    client_certificate     = kind_cluster.airflow_cluster.client_certificate
    client_key             = kind_cluster.airflow_cluster.client_key
  }
}

resource "kubernetes_namespace" "airflow" {
  metadata {
    name = var.airflow_namespace
    
    labels = {
      environment = "local"
      application = "airflow"
    }
  }
  
  lifecycle {
    ignore_changes = [metadata]
  }
  
  depends_on = [kind_cluster.airflow_cluster]
}

resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  version    = var.airflow_chart_version
  namespace  = kubernetes_namespace.airflow.metadata[0].name

  values = [templatefile("${path.module}/airflow-values.yaml", {
    load_examples = var.enable_examples ? "True" : "False"
    dags_path = local.host_paths.dags
    aws_path = local.host_paths.aws
    webserver_port = var.webserver_port
    worker_replicas = var.worker_replicas
    webserver_cpu = var.resource_limits.webserver.cpu
    webserver_memory = var.resource_limits.webserver.memory
    worker_cpu = var.resource_limits.worker.cpu
    worker_memory = var.resource_limits.worker.memory
    scheduler_cpu = var.resource_limits.scheduler.cpu
    scheduler_memory = var.resource_limits.scheduler.memory
  })]

  lifecycle {
    ignore_changes = [status]
  }

  depends_on = [
    kind_cluster.airflow_cluster,
    kubernetes_namespace.airflow
  ]

  timeout = 600
} 