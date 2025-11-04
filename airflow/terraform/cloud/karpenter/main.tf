terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "~> 1.19"
    }
  }
}

data "aws_partition" "current" {}
data "aws_eks_cluster" "cluster" {
  name = var.cluster_name
}

# Karpenter Controller IAM Role 
module "karpenter_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name                           = "${var.cluster_name}-karpenter-controller"
  attach_karpenter_controller_policy  = true
  karpenter_controller_cluster_name   = var.cluster_name
  karpenter_controller_node_iam_role_arns = [module.karpenter_node_iam_role.iam_role_arn]

  oidc_providers = {
    main = {
      provider_arn               = data.aws_eks_cluster.cluster.identity[0].oidc[0].issuer
      namespace_service_accounts = ["${var.karpenter_namespace}:karpenter"]
    }
  }
}

# Karpenter Node IAM Role
module "karpenter_node_iam_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${var.cluster_name}-karpenter-node"
  role_policy_arns = {
    AmazonEKSWorkerNodePolicy          = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSWorkerNodePolicy"
    AmazonEKS_CNI_Policy               = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKS_CNI_Policy"
    AmazonEC2ContainerRegistryReadOnly = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  }
  create_role = true
}

# Instance Profile
resource "aws_iam_instance_profile" "karpenter_node" {
  name = "${var.cluster_name}-karpenter-node"
  role = module.karpenter_node_iam_role.iam_role_name
}

# Tag subnets for discovery 
data "aws_subnets" "cluster" {
  filter {
    name   = "vpc-id"
    values = [data.aws_eks_cluster.cluster.vpc_config[0].vpc_id]
  }
  tags = {
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_ec2_tag" "karpenter_subnet_tag" {
  for_each    = toset(data.aws_subnets.cluster.ids)
  resource_id = each.value
  key         = "karpenter.sh/discovery"
  value       = var.cluster_name
}

# Tag security groups
resource "aws_ec2_tag" "karpenter_sg_tag" {
  resource_id = data.aws_eks_cluster.cluster.vpc_config[0].cluster_security_group_id
  key         = "karpenter.sh/discovery"
  value       = var.cluster_name
}

# Install Karpenter
resource "helm_release" "karpenter" {
  namespace  = var.karpenter_namespace
  name       = "karpenter"
  repository = "oci://public.ecr.aws/karpenter"
  chart      = "karpenter"
  version    = "1.7.0"
  create_namespace = true

  values = [templatefile("${path.module}/karpenter-values.yaml", {
    controller_replicas         = var.controller_replicas
    controller_cpu_request      = var.controller_resources.requests.cpu
    controller_memory_request   = var.controller_resources.requests.memory
    controller_cpu_limit        = var.controller_resources.limits.cpu
    controller_memory_limit     = var.controller_resources.limits.memory
    
    batch_max_duration          = var.batch_settings.max_duration
    batch_idle_duration         = var.batch_settings.idle_duration
    
    # Cluster connection
    cluster_name                = var.cluster_name
    cluster_endpoint            = var.cluster_endpoint
    interruption_queue          = "" # No SQS queue yet
  }),
  # Override service account annotations with IRSA role
  yamlencode({
    serviceAccount = {
      annotations = {
        "eks.amazonaws.com/role-arn" = module.karpenter_irsa.iam_role_arn
      }
    }
  })]

  depends_on = [module.karpenter_irsa]
}

# EC2NodeClass 
resource "kubectl_manifest" "ec2_node_class" {
  yaml_body = yamlencode({
    apiVersion = "karpenter.k8s.aws/v1beta1"
    kind       = "EC2NodeClass"
    metadata = {
      name = "airflow-workers"
    }
    spec = {
      amiFamily = "AL2023"
      role      = module.karpenter_node_iam_role.iam_role_name
      subnetSelectorTerms = [{
        tags = {
          "karpenter.sh/discovery" = var.cluster_name
        }
      }]
      securityGroupSelectorTerms = [{
        tags = {
          "karpenter.sh/discovery" = var.cluster_name
        }
      }]
      userData = <<-EOT
        #!/bin/bash
        /etc/eks/bootstrap.sh ${var.cluster_name}
      EOT
    }
  })

  depends_on = [helm_release.karpenter]
}

# NodePool
resource "kubectl_manifest" "airflow_workers_nodepool" {
  yaml_body = yamlencode({
    apiVersion = "karpenter.sh/v1beta1"
    kind       = "NodePool"
    metadata = {
      name = "airflow-workers"
    }
    spec = {
      template = {
        spec = {
          requirements = [
            {
              key      = "karpenter.sh/capacity-type"
              operator = "In"
              values   = ["spot"]
            },
            {
              key      = "node.kubernetes.io/instance-type"
              operator = "In"
              values   = ["m7i.4xlarge", "m7a.4xlarge", "r7a.2xlarge", "r7i.2xlarge", "r6a.2xlarge", "r6i.2xlarge", "r5.2xlarge"]
            }
          ]
          nodeClassRef = {
            name = "airflow-workers"
          }
          taints = [{
            key    = "airflow-worker"
            value  = "true"
            effect = "NoSchedule"
          }]
        }
      }


      # Resource limits to prevent overscheduling and control costs
      limits = {
        cpu    = var.nodepool_limits.cpu
        memory = var.nodepool_limits.memory
      }
      disruption = {
        consolidationPolicy = var.disruption_settings.consolidation_policy
        expireAfter         = var.disruption_settings.expire_after
        budgets = [
          {
            nodes = "10%"
          }
        ]
      }
    }
  })

  depends_on = [kubectl_manifest.ec2_node_class]
}