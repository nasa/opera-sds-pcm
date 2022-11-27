output "small_asg_name" {
  value = "${var.project}-${var.venue}-${module.common.counter}-${var.project}-job_worker-small"
}

output "small_queue_name" {
  value = "${var.project}-job_worker-small"
}

output "large_asg_name" {
  value = "${var.project}-${var.venue}-${module.common.counter}-${var.project}-job_worker-large"
}

output "large_queue_name" {
  value = "${var.project}-job_worker-large"
}

output "hlsl30_query_timer" {
  value = module.common.hlsl30_query_timer.function_name
}

output "hlss30_query_timer" {
  value = module.common.hlss30_query_timer.function_name
}

output "slcs1a_query_timer" {
  value = module.common.slcs1a_query_timer.function_name
}

output "cnm_notify_asg_name" {
  value = "${var.project}-${var.venue}-${module.common.counter}-${var.project}-job_worker-cnm_notify"
}

output "cnm_notify_queue_name" {
  value = "${var.project}-job_worker-cnm_notify"
}

output "full_venue" {
  value = "${var.project}-${var.venue}-${module.common.counter}"
}

output "dataset_bucket" {
  value = "${var.project}-${var.environment}-rs-fwd-${var.venue}"
}

output "code_bucket" {
  value = "${var.project}-${var.environment}-cc-fwd-${var.venue}"
}

output "triage_bucket" {
  value = "${var.project}-${var.environment}-triage-fwd-${var.venue}"
}

output "lts_bucket" {
  value = "${var.project}-${var.environment}-lts-fwd-${var.venue}"
}

output "mozart_pvt_ip" {
  value = module.common.mozart.private_ip
}

output "mozart_pub_ip" {
  value = module.common.mozart.private_ip
}

output "metrics_pvt_ip" {
  value = module.common.metrics.private_ip
}

output "metrics_pub_ip" {
  value = module.common.metrics.private_ip
}

output "grq_pvt_ip" {
  value = module.common.grq.private_ip
}

output "grq_pub_ip" {
  value = module.common.grq.private_ip
}

output "factotum_pvt_ip" {
  value = module.common.factotum.private_ip
}

output "factotum_pub_ip" {
  value = module.common.factotum.private_ip
}

