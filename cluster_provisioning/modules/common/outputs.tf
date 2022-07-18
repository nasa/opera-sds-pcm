output "counter" {
  value = local.counter
}

output "dataset_bucket" {
  value = local.dataset_bucket
}

output "code_bucket" {
  value = local.code_bucket
}

output "isl_bucket" {
  value = local.isl_bucket
}

output "triage_bucket" {
  value = local.triage_bucket
}

output "lts_bucket" {
  value = local.lts_bucket
}

output "osl_bucket" {
  value = local.osl_bucket
}

output "key_name" {
  value = local.key_name
}

output "mozart" {
  value = aws_instance.mozart
}

output "mozart_pvt_ip" {
  value = aws_instance.mozart.private_ip
}

output "mozart_pub_ip" {
  value = aws_instance.mozart.private_ip
}

output "metrics" {
  value = aws_instance.metrics
}

output "metrics_pvt_ip" {
  value = aws_instance.metrics.private_ip
}

output "metrics_pub_ip" {
  value = aws_instance.metrics.private_ip
}

output "grq" {
  value = aws_instance.grq
}

output "grq_pvt_ip" {
  value = aws_instance.grq.private_ip
}

output "grq_pub_ip" {
  value = aws_instance.grq.private_ip
}

output "factotum" {
  value = aws_instance.factotum
}

output "factotum_pvt_ip" {
  value = aws_instance.factotum.private_ip
}

output "factotum_pub_ip" {
  value = aws_instance.factotum.private_ip
}

output "cnm_response_topic_arn" {
  value = aws_sns_topic.cnm_response[0].arn
}

output "daac_proxy_cnm_r_sns_count" {
  value = local.daac_proxy_cnm_r_sns_count
}

output "e_misfire_metric_alarm_name" {
  value = local.e_misfire_metric_alarm_name
}

output "aws_cloudwatch_event_rule_hls_download_timer" {
  value = aws_cloudwatch_event_rule.hls_download_timer
}

output "aws_cloudwatch_event_rule_hlsl30_query_timer" {
  value = aws_cloudwatch_event_rule.hlsl30_query_timer
}

output "aws_cloudwatch_event_rule_hlss30_query_timer" {
  value = aws_cloudwatch_event_rule.hlss30_query_timer
}

output "hlsl30_query_timer" {
  value = aws_lambda_function.hlsl30_query_timer
}

output "hlss30_query_timer" {
  value = aws_lambda_function.hlss30_query_timer
}