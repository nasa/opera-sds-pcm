output "karpenter_controller_role_arn" {
  description = "IAM role ARN for Karpenter controller"
  value       = var.karpenter_irsa_role_arn
}

output "karpenter_node_role_arn" {
  description = "IAM role ARN for Karpenter nodes"
  value       = data.aws_iam_role.node_role.arn
}