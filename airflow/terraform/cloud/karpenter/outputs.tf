output "karpenter_irsa_role_arn" {
  description = "IAM role ARN for Karpenter controller"
  value       = module.karpenter_irsa.iam_role_arn
}

output "karpenter_node_role_arn" {
  description = "IAM role ARN for Karpenter nodes"
  value       = module.karpenter_node_iam_role.iam_role_arn
}

output "karpenter_node_instance_profile" {
  description = "Instance profile name for Karpenter nodes"
  value       = aws_iam_instance_profile.karpenter_node.name
}

output "karpenter_queue_name" {
  description = "SQS queue name for Karpenter interruption handling"
  value       = aws_sqs_queue.karpenter.name
}