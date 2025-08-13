output "airflow_ui_url" {
  description = "URL to access Airflow web UI"
  value       = "http://localhost:${var.webserver_port}"
}