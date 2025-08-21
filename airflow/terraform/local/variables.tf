variable "cluster_name" {
  description = "Name of the Kind cluster"
  type        = string
  default     = "airflow-local"
}

variable "dags_path" {
    description = "local path to dags folder"
    type = string
}
variable "aws_path" {
    description = "local path to aws creds"
    type = string
}

variable "airflow_namespace" {
  description = "Kubernetes namespace for Airflow deployment"
  type        = string
  default     = "airflow"
}

variable "webserver_port" {
  description = "Host port for accessing Airflow web UI"
  type        = number
  default     = 8080
}