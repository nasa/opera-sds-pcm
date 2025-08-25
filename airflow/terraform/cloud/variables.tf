variable "cluster_name" {
  description = "Name of the Kind cluster"
  type        = string
  default     = "airflow-local"
}

variable "airflow_namespace" {
  description = "Kubernetes namespace for Airflow deployment"
  type        = string
  default     = "opera-dev"
}

variable "webserver_port" {
  description = "Host port for accessing Airflow web UI"
  type        = number
  default     = 8080
}

variable "kube_config_path" {
  description = "path to local .kube config file"
  type = string
}

variable "shared_config_file" {
  description = "path to aws config file"
  type = string
}

variable "shared_credentials_file" {
  description = "path to aws credentials file"
  type = string
}

variable "aws_profile" {
  description = "desired aws profile name"
  type = string 
}