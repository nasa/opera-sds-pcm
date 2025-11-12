module "airflow" {
  source = "./airflow"
  namespace = var.airflow_namespace
}

module "karpenter" {
  source = "./karpenter"
  cluster_name = var.cluster_name
  cluster_endpoint =  var.cluster_endpoint
}

