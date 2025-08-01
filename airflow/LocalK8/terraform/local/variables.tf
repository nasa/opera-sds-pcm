variable "cluster_name" {
  description = "Name of the Kind cluster"
  type        = string
  default     = "airflow-local"
}

variable "airflow_namespace" {
  description = "Kubernetes namespace for Airflow deployment"
  type        = string
  default     = "airflow"
}

variable "airflow_chart_version" {
  description = "Version of the Airflow Helm chart to deploy"
  type        = string
  default     = "~> 1.18.0"
}

variable "worker_replicas" {
  description = "Number of Airflow worker replicas"
  type        = number
  default     = 2
}

variable "webserver_port" {
  description = "Host port for accessing Airflow web UI"
  type        = number
  default     = 8080
}

variable "enable_examples" {
  description = "Enable Airflow example DAGs"
  type        = bool
  default     = true
}

variable "resource_limits" {
  description = "Resource limits for Airflow components"
  type = object({
    worker = object({
      cpu    = string
      memory = string
    })
    scheduler = object({
      cpu    = string
      memory = string
    })
    webserver = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    worker = {
      cpu    = "1000m"
      memory = "5Gi"
    }
    scheduler = {
      cpu    = "1000m"
      memory = "5Gi"
    }
    webserver = {
      cpu    = "1000m"
      memory = "5Gi"
    }
  }
} 