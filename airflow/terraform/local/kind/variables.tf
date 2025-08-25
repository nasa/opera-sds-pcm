variable "dags_path" {
    description = "value"
    type = string
}
variable "aws_path" {
    description = "value"
    type = string
}

variable "cluster_name" {
    description = "value"
    type  = string
}

variable "webserver_port" {
  description = "Host port for accessing Airflow web UI"
  type        = number
  default     = 8080
}