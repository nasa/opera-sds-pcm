output "cluster_name" {
  description = "Name of the Kind cluster"
  value       = module.airflow_cluster.cluster_name
}

output "cluster_endpoint" {
  description = "Cluster endpoint URL"
  value       = module.airflow_cluster.cluster_endpoint
}

output "kubeconfig_path" {
  description = "Path to kubeconfig file"
  value       = module.airflow_cluster.kubeconfig_path
}

output "airflow_ui_url" {
  description = "URL to access Airflow web UI"
  value       = "http://localhost:${var.webserver_port}"
}

output "kubectl_context" {
  description = "kubectl context name for this cluster"
  value       = "kind-${module.airflow_cluster.cluster_name}"
}

output "useful_commands" {
  description = "Useful commands to interact with the deployment"
  value = {
    "Access Airflow UI"     = "Open http://localhost:${var.webserver_port} in your browser"
    "Check pods"            = "kubectl get pods -n ${var.airflow_namespace} --context kind-${module.airflow_cluster.cluster_name}"
    "Check services"        = "kubectl get svc -n ${var.airflow_namespace} --context kind-${module.airflow_cluster.cluster_name}"
    "View Airflow logs"     = "kubectl logs -l app.kubernetes.io/name=airflow -n ${var.airflow_namespace} --context kind-${module.airflow_cluster.cluster_name}"
    "Port forward (alt)"    = "kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow --context kind-airflow-local"
    "Helm status"           = "helm status airflow -n ${var.airflow_namespace}"
  }
} 