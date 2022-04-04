provider "aws" {
  shared_credentials_file = var.shared_credentials_file
  region                  = var.region
  profile                 = var.profile
}

module "common" {
  source = "../modules/common"

  hysds_release                           = var.hysds_release
  pcm_repo                                = var.pcm_repo
  pcm_branch                              = var.pcm_branch
  product_delivery_repo                   = var.product_delivery_repo
  product_delivery_branch                 = var.product_delivery_branch
  pcm_commons_repo                        = var.pcm_commons_repo
  pcm_commons_branch                      = var.pcm_commons_branch
  bach_api_repo                           = var.bach_api_repo
  bach_api_branch                         = var.bach_api_branch
  bach_ui_repo                            = var.bach_ui_repo
  bach_ui_branch                          = var.bach_ui_branch
  venue                                   = var.venue
  counter                                 = var.counter
  private_key_file                        = var.private_key_file
  git_auth_key                            = var.git_auth_key
  jenkins_api_user                        = var.jenkins_api_user
  keypair_name                            = var.keypair_name
  jenkins_api_key                         = var.jenkins_api_key
  artifactory_fn_api_key                  = var.artifactory_fn_api_key
  ops_password                            = var.ops_password
  shared_credentials_file                 = var.shared_credentials_file
  profile                                 = var.profile
  project                                 = var.project
  region                                  = var.region
  az                                      = var.az
  subnet_id                               = var.subnet_id
  verdi_security_group_id                 = var.verdi_security_group_id
  cluster_security_group_id               = var.cluster_security_group_id
  pcm_cluster_role                        = var.pcm_cluster_role
  pcm_verdi_role                          = var.pcm_verdi_role
  mozart                                  = var.mozart
  metrics                                 = var.metrics
  grq                                     = var.grq
  factotum                                = var.factotum
  ci                                      = var.ci
  common_ci                               = var.common_ci
  autoscale                               = var.autoscale
  lambda_vpc                              = var.lambda_vpc
  lambda_role_arn                         = var.lambda_role_arn
  lambda_job_type                         = var.lambda_job_type
  lambda_job_queue                        = var.lambda_job_queue
  cnm_r_handler_job_type                  = var.cnm_r_handler_job_type
  cnm_r_job_queue                         = var.cnm_r_job_queue
  cnm_r_event_trigger                     = var.cnm_r_event_trigger
  cnm_r_allowed_account                   = var.cnm_r_allowed_account
  daac_delivery_proxy                     = var.daac_delivery_proxy
  daac_endpoint_url                       = var.daac_endpoint_url
  asg_use_role                            = var.asg_use_role
  asg_role                                = var.asg_role
  asg_vpc                                 = var.asg_vpc
  aws_account_id                          = var.aws_account_id
  lambda_package_release                  = var.lambda_package_release
  environment                             = var.environment
  use_artifactory                         = var.use_artifactory
  artifactory_base_url                    = var.artifactory_base_url
  artifactory_repo                        = var.artifactory_repo
  artifactory_mirror_url                  = var.artifactory_mirror_url
  grq_aws_es                              = var.grq_aws_es
  grq_aws_es_host                         = var.grq_aws_es_host
  grq_aws_es_port                         = var.grq_aws_es_port
  grq_aws_es_host_private_verdi           = var.grq_aws_es_host_private_verdi
  use_grq_aws_es_private_verdi            = var.use_grq_aws_es_private_verdi
  use_daac_cnm                            = var.use_daac_cnm
  pge_snapshots_date                      = var.pge_snapshots_date
  pge_release                             = var.pge_release
  crid                                    = var.crid
  cluster_type                            = var.cluster_type
  data_download_timer_trigger_frequency   = var.data_download_timer_trigger_frequency
  data_query_timer_trigger_frequency      = var.data_query_timer_trigger_frequency
  obs_acct_report_timer_trigger_frequency = var.obs_acct_report_timer_trigger_frequency
  rs_fwd_bucket_ingested_expiration       = var.rs_fwd_bucket_ingested_expiration
  dataset_bucket                          = var.dataset_bucket
  code_bucket                             = var.code_bucket
  lts_bucket                              = var.lts_bucket
  triage_bucket                           = var.triage_bucket
  isl_bucket                              = var.isl_bucket
  osl_bucket                              = var.osl_bucket
  use_s3_uri_structure                    = var.use_s3_uri_structure
  inactivity_threshold                    = var.inactivity_threshold
  es_snapshot_bucket                      = var.es_snapshot_bucket
  artifactory_fn_user                     = var.artifactory_fn_user
  earthdata_user                          = var.earthdata_user
  earthdata_pass                          = var.earthdata_pass
}

locals {
  lambda_repo = "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/lambda"
  crid        = lower(var.crid)
  grq_es_url  = "${var.grq_aws_es ? "https" : "http"}://${var.grq_aws_es ? var.grq_aws_es_host : module.common.grq.private_ip}:${var.grq_aws_es ? var.grq_aws_es_port : 9200}"
}

resource "null_resource" "mozart" {
  depends_on = [module.common]

  triggers = {
    private_ip       = module.common.mozart.private_ip
    private_key_file = var.private_key_file
    code_bucket      = module.common.code_bucket
    dataset_bucket   = module.common.dataset_bucket
    triage_bucket    = module.common.triage_bucket
    lts_bucket       = module.common.lts_bucket
  }

  connection {
    type        = "ssh"
    host        = self.triggers.private_ip
    user        = "hysdsops"
    private_key = file(self.triggers.private_key_file)
  }

  provisioner "remote-exec" {
    inline = [
      "set -ex",
      "source ~/.bash_profile",
      "echo restoring snapshots from s3://${var.es_snapshot_bucket}/${var.es_snapshot_restore_bucket_path}",
      "~/mozart/bin/restore_snapshot.sh  \\",
      "   http://${module.common.mozart.private_ip}:9200 \\",
      "   ${local.grq_es_url} \\",
      "   http://${module.common.metrics.private_ip}:9200 \\",
      "   ${var.es_snapshot_bucket} \\",
      "   ${var.es_snapshot_restore_bucket_path} \\",
      "   ${var.es_bucket_role_arn} \\",
      "   ${local.crid} \\",
      "   ${var.cluster_type} || :",
      "echo Your cluster has been provisioned!",
    ]
  }
}
