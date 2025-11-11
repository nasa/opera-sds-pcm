module "airflow" {
  source = "./airflow"
  namespace = var.airflow_namespace
}

module "karpenter" {
  source = "./karpenter"
  
  cluster_name     = var.cluster_name
  cluster_endpoint = var.cluster_endpoint
  karpenter_irsa_role_arn     = "arn:aws:iam::681612454726:role/am-opera-dev-karpenter-controller"
  node_role_name              = "am-opera-dev-eks-cluster-worker"
  node_instance_profile_arn   = "arn:aws:iam::681612454726:instance-profile/am-opera-dev-eks-cluster-worker"
  security_group_id = "sg-06cd603fe182999ae"
}

