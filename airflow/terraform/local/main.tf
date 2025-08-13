module "airflow_cluster" {
  source  = "./kind"
  dags_path = var.dags_path
  aws_path = var.aws_path
  cluster_name = var.cluster_name

}

resource "kubernetes_namespace" "airflow" {
  metadata {
    name = var.airflow_namespace
    
    labels = {
      environment = "local"
      application = "airflow"
    }
  }

  lifecycle {
    ignore_changes = [metadata]
  }

  depends_on = [module.airflow_cluster]
}

module "airflow" {
  source = "./airflow"
  namespace = kubernetes_namespace.airflow.metadata[0].name
  dags_path = var.dags_path
  aws_path = var.aws_path
}


