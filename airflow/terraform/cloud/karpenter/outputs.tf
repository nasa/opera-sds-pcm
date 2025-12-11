output "karpenter_controller_role_arn" {
  description = "IAM role ARN for Karpenter controller"
  value       = module.karpenter_irsa.iam_role_arn
}

output "karpenter_node_role_arn" {
  description = "IAM role ARN for Karpenter nodes"  
  value       = module.karpenter_node_iam_role.iam_role_arn
}