terraform {
  required_version = ">= 1.5.7"
  
  backend "local" {
    path = "terraform_state/terraform.tfstate"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
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

provider "aws" {
  region = "us-west-2" 

  profile = var.aws_profile
  shared_config_files = [var.shared_config_file]
  shared_credentials_files = [var.shared_credentials_file]
}

data "aws_eks_cluster" "this" {
  name = "opera-dev"
}

data "aws_eks_cluster_auth" "this" {
  name = data.aws_eks_cluster.this.name
}

provider "kubernetes" {
  config_path = var.kube_config_path
  host                   = data.aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.this.token
}

provider "helm" {
  kubernetes {
    config_path = var.kube_config_path
    host                   = data.aws_eks_cluster.this.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
    token                  = data.aws_eks_cluster_auth.this.token
  }
}
