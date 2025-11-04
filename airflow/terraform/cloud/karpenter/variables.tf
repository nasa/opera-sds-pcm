variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "cluster_endpoint" {
  description = "EKS cluster endpoint"
  type        = string
}

variable "cluster_version" {
  description = "EKS cluster version"
  type        = string
  default     = "1.28"
}

variable "karpenter_namespace" {
  description = "Namespace for Karpenter"
  type        = string
  default     = "karpenter"
}

variable "controller_replicas" {
  description = "Number of Karpenter controller replicas"
  type        = number
  default     = 2
}

variable "controller_resources" {
  description = "Resource requests and limits for Karpenter controller"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "1"
      memory = "1Gi"
    }
    limits = {
      cpu    = "1"
      memory = "1Gi"
    }
  }
}

variable "batch_settings" {
  description = "Karpenter batch provisioning settings"
  type = object({
    max_duration  = string
    idle_duration = string
  })
  default = {
    max_duration  = "10s"
    idle_duration = "1s"
  }
}

variable "nodepool_limits" {
  description = "Resource limits for the Airflow worker node pool"
  type = object({
    cpu    = string
    memory = string
  })
  default = {
    cpu    = "1000"
    memory = "1000Gi"
  }
}

variable "disruption_settings" {
  description = "Node disruption settings for cost optimization vs stability"
  type = object({
    consolidation_policy = string
    expire_after        = string
  })
  default = {
    consolidation_policy = "WhenUnderutilized"
    expire_after        = "168h" # 7 days
  }
}