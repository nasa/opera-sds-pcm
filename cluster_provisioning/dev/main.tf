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
  ops_password                            = var.ops_password
  shared_credentials_file                 = var.shared_credentials_file
  profile                                 = var.profile
  project                                 = var.project
  region                                  = var.region
  az                                      = var.az
  subnet_id                               = var.subnet_id
  public_verdi_security_group_id          = var.public_verdi_security_group_id
  private_verdi_security_group_id         = var.private_verdi_security_group_id
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
  cnm_r_handler_job_type                  = var.cnm_r_handler_job_type
  cnm_r_job_queue                         = var.cnm_r_job_queue
  po_daac_cnm_r_event_trigger             = var.po_daac_cnm_r_event_trigger
  asf_daac_cnm_r_event_trigger            = var.asf_daac_cnm_r_event_trigger
  cnm_r_allowed_account                   = var.cnm_r_allowed_account
  cnm_r_venue                             = var.cnm_r_venue
  trace                                   = var.trace

  po_daac_delivery_proxy                  = var.po_daac_delivery_proxy
  po_daac_endpoint_url                    = var.po_daac_endpoint_url
  asf_daac_delivery_proxy                 = var.asf_daac_delivery_proxy
  asf_daac_endpoint_url                   = var.asf_daac_endpoint_url

  asg_use_role                            = var.asg_use_role
  asg_role                                = var.asg_role
  public_asg_vpc                          = var.public_asg_vpc
  private_asg_vpc                         = var.private_asg_vpc
  aws_account_id                          = var.aws_account_id
  ssm_account_id                          = var.ssm_account_id
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
  use_daac_cnm_r                          = var.use_daac_cnm_r
  pge_releases                            = var.pge_releases
  pge_snapshots_date                      = var.pge_snapshots_date
  pge_sim_mode                            = var.pge_sim_mode
  crid                                    = var.crid
  cluster_type                            = var.cluster_type
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
  artifactory_fn_user                     = var.artifactory_fn_user
  artifactory_fn_api_key                  = var.artifactory_fn_api_key
  dataspace_user                          = var.dataspace_user
  dataspace_pass                          = var.dataspace_pass
  earthdata_user                          = var.earthdata_user
  earthdata_pass                          = var.earthdata_pass
  earthdata_uat_user                      = var.earthdata_uat_user
  earthdata_uat_pass                      = var.earthdata_uat_pass
  disp_s1_hist_status                     = var.disp_s1_hist_status
  asf_cnm_s_id_dev                        = var.asf_cnm_s_id_dev
  asf_cnm_s_id_dev_int                    = var.asf_cnm_s_id_dev_int
  asf_cnm_s_id_test                       = var.asf_cnm_s_id_test
  asf_cnm_s_id_prod                       = var.asf_cnm_s_id_prod
  cnm_r_sqs_arn                           = var.cnm_r_sqs_arn
  es_bucket_role_arn                      = var.es_bucket_role_arn
}

locals {
  lambda_repo = "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/lambda"
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
    inline = [<<-EOF
              set -ex
              source ~/.bash_profile
              cd ~/.sds/files
              ~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json --force
              echo Your cluster has been provisioned!
    EOF
    ]
  }
}

