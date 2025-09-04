output "counter" {
  value = local.counter
}

output "dataset_bucket" {
  value = local.dataset_bucket
}

output "code_bucket" {
  value = local.code_bucket
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
  value = aws_sns_topic.cnm_response.arn
}

output "cnm_response_queue_url" {
  value = aws_sqs_queue.cnm_response.id
}

output "e_misfire_metric_alarm_name" {
  value = local.e_misfire_metric_alarm_name
}

#output "aws_cloudwatch_event_rule_hls_download_timer" {
#  value = aws_cloudwatch_event_rule.hls_download_timer
#}

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

output "slcs1a_query_timer" {
  value = aws_lambda_function.slcs1a_query_timer
}

output "slc_ionosphere_download_timer" {
  value = aws_lambda_function.slc_ionosphere_download_timer
}

output "rtc_query_timer" {
  value = aws_lambda_function.rtc_query_timer
}

output "mozart_instance_id" {
  value = aws_instance.mozart.id
} 

#output "mozart_es_instance_id" {
#  value = aws_instance.mozart_es.id
#} 

output "grq_instance_id" {
  value = aws_instance.grq.id
} 

output "factotum_instance_id" {
  value = aws_instance.factotum.id
} 

output "metrics_instance_id" {
  value = aws_instance.metrics.id
} 

output "es_cluster_mode" {
  value = local.es_cluster_mode
}

