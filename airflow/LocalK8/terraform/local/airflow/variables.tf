variable "webserver_port" {
  description = "Host port for accessing Airflow web UI"
  type        = number
  default     = 8080
}

variable "worker_replicas" {
  description = "Number of Airflow worker replicas"
  type        = number
  default     = 2
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

variable "namespace" {
    description = "kubernetes namespace to deploy airflow onto"
    type = string
}

variable "airflow_chart_version" {
  description = "Version of the Airflow Helm chart to deploy"
  type        = string
  default     = "~> 1.18.0"
}

variable "dags_path" {
    description = "local path to dags folder"
    type = string
}
variable "aws_path" {
    description = "local path to aws creds"
    type = string
}