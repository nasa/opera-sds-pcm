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

data "aws_eks_cluster" "cluster" {
  name = var.cluster_name
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
    
    # IAM role for service account (pre-created)
    karpenter_irsa_role_arn     = var.karpenter_irsa_role_arn
  })]
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
      role      = var.node_role_name
      instanceProfile = var.node_instance_profile_arn
      amiSelectorTerms = [{
        alias = "al2023@latest"
      }]
      subnetSelectorTerms = [{
        tags = {
          "karpenter.sh/discovery" = var.cluster_name
        }
      }]
      securityGroupSelectorTerms = [{
        id = var.security_group_id
      }]
      tags = {
        Bravo = "pcm"
      }
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