resource "aws_cloudwatch_log_group" "amazon-cloudwatch-agent" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/amazon-cloudwatch-agent.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "autoscaling-log-groups-job-worker" {
  for_each          = var.queues
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/${lower(each.key)}.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "autoscaling-log-groups-job-worker-run" {
  for_each          = var.queues
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_${split("-", lower(each.key))[2]}.log"
  retention_in_days = var.lambda_log_retention_in_days
}
