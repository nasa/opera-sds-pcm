terraform {
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
  }
}

resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "apache-airflow"
  chart      = "airflow"
  version    = var.airflow_chart_version
  namespace  = var.namespace

  values = [templatefile("${path.module}/airflow-values.yaml", {
    load_examples = var.enable_examples ? "True" : "False"
    worker_cpu = var.resource_limits.worker.cpu
    worker_memory = var.resource_limits.worker.memory
    scheduler_cpu = var.resource_limits.scheduler.cpu
    scheduler_memory = var.resource_limits.scheduler.memory
  })]

  timeout = 600
} 