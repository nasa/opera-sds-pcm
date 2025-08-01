output "cluster_name" {
  description = "Name of the Kind cluster"
  value       = kind_cluster.airflow_cluster.name
}

output "cluster_endpoint" {
  description = "Cluster endpoint URL"
  value       = kind_cluster.airflow_cluster.endpoint
}

output "kubeconfig_path" {
  description = "Path to kubeconfig file"
  value       = kind_cluster.airflow_cluster.kubeconfig_path
}

output "airflow_namespace" {
  description = "Kubernetes namespace where Airflow is deployed"
  value       = kubernetes_namespace.airflow.metadata[0].name
}

output "airflow_ui_url" {
  description = "URL to access Airflow web UI"
  value       = "http://localhost:${var.webserver_port}"
}

output "kubectl_context" {
  description = "kubectl context name for this cluster"
  value       = "kind-${kind_cluster.airflow_cluster.name}"
}

output "useful_commands" {
  description = "Useful commands to interact with the deployment"
  value = {
    "Access Airflow UI"     = "Open http://localhost:${var.webserver_port} in your browser"
    "Check pods"            = "kubectl get pods -n ${kubernetes_namespace.airflow.metadata[0].name} --context kind-${kind_cluster.airflow_cluster.name}"
    "Check services"        = "kubectl get svc -n ${kubernetes_namespace.airflow.metadata[0].name} --context kind-${kind_cluster.airflow_cluster.name}"
    "View Airflow logs"     = "kubectl logs -l app.kubernetes.io/name=airflow -n ${kubernetes_namespace.airflow.metadata[0].name} --context kind-${kind_cluster.airflow_cluster.name}"
    "Port forward (alt)"    = "kubectl port-forward svc/airflow-webserver ${var.webserver_port}:${var.webserver_port} -n ${kubernetes_namespace.airflow.metadata[0].name} --context kind-${kind_cluster.airflow_cluster.name}"
    "Helm status"           = "helm status airflow -n ${kubernetes_namespace.airflow.metadata[0].name}"
  }
} 