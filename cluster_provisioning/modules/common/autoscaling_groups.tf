resource "aws_cloudwatch_log_group" "autoscaling-log-groups_run" {
  for_each = var.queues
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_${substr(each.key, 17, 100)}.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "autoscaling-log-groups-job-worker" {
  for_each = var.queues
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/${each.key}.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "amazon-cloudwatch-agent" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/amazon-cloudwatch-agent.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_batch_query" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_batch_query.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_pcm_int" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_pcm_int.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_on_demand" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_on_demand.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_sciflo_L3_DSWx_HLS" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L3_DSWx_HLS.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_sciflo_L2_CSLC_S1" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L2_CSLC_S1.log"
  retention_in_days = var.lambda_log_retention_in_days
}
