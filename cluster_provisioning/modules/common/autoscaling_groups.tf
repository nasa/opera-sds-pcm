resource "aws_cloudwatch_log_group" "amazon-cloudwatch-agent" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/amazon-cloudwatch-agent.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "autoscaling-log-groups-job-worker" {
  for_each          = var.queues
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/${lower(each.key)}.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_hlsl30_query" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hlsl30_query.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_hlss30_query" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hlss30_query.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_hls_download" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hls_download.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_slcs1a_query" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slcs1b_query.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_slcs1a_query" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slcs1b_query.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_slc_download" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slc_download.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_ionosphere_download" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_ionosphere_download.log"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_log_group" "run_ionosphere_download" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_ionosphere_download.log"
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

resource "aws_cloudwatch_log_group" "run_sciflo_L2_RTC_S1" {
  name              = "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L2_RTC_S1.log"
  retention_in_days = var.lambda_log_retention_in_days
}