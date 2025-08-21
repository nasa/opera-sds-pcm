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

output "kubectl_context" {
  description = "kubectl context name for this cluster"
  value       = "kind-${kind_cluster.airflow_cluster.name}"
}

output "cluster_ca_certificate" {
  description = "Cluster CA certificate"
  value       = kind_cluster.airflow_cluster.cluster_ca_certificate
}
output "client_certificate" {
  description = "Cluster client certificate"
  value       = kind_cluster.airflow_cluster.client_certificate
}

output "client_key" {
  description = "Cluster client key"
  value       = kind_cluster.airflow_cluster.client_key
}