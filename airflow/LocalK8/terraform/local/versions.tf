terraform {
  required_version = ">= 1.0"
  
  backend "local" {
    path = "terraform_state/terraform.tfstate"
  }

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.4"
    }
    
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
    
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.24"
    }
  }
}

provider "kind" {}

provider "kubernetes" {
  host                   = module.airflow_cluster.cluster_endpoint
  cluster_ca_certificate = module.airflow_cluster.cluster_ca_certificate
  client_certificate     = module.airflow_cluster.client_certificate
  client_key             = module.airflow_cluster.client_key
}

provider "helm" {
  kubernetes {
    host                   = module.airflow_cluster.cluster_endpoint
    cluster_ca_certificate = module.airflow_cluster.cluster_ca_certificate
    client_certificate     = module.airflow_cluster.client_certificate
    client_key             = module.airflow_cluster.client_key
  }
}