provider "aws" {
  shared_credentials_file = var.shared_credentials_file
  region                  = var.region
  profile                 = var.profile
}

terraform {
  backend "s3" {
  }
}

module "common" {
  source = "../modules/common"

  hysds_release                         = var.hysds_release
  opera_pcm_repo                        = var.opera_pcm_repo
  opera_pcm_branch                      = var.opera_pcm_branch
  product_delivery_repo                 = var.product_delivery_repo
  product_delivery_branch               = var.product_delivery_branch
  venue                                 = var.venue
  counter                               = var.counter
  private_key_file                      = var.private_key_file
  git_auth_key                          = var.git_auth_key
  jenkins_api_user                      = var.jenkins_api_user
  keypair_name                          = var.keypair_name
  jenkins_api_key                       = var.jenkins_api_key
  ops_password                          = var.ops_password
  shared_credentials_file               = var.shared_credentials_file
  profile                               = var.profile
  project                               = var.project
  region                                = var.region
  az                                    = var.az
  subnet_id                             = var.subnet_id
  verdi_security_group_id               = var.verdi_security_group_id
  cluster_security_group_id             = var.cluster_security_group_id
  pcm_cluster_role                      = var.pcm_cluster_role
  pcm_verdi_role                        = var.pcm_verdi_role
  mozart                                = var.mozart
  metrics                               = var.metrics
  grq                                   = var.grq
  factotum                              = var.factotum
  ci                                    = var.ci
  common_ci                             = var.common_ci
  autoscale                             = var.autoscale
  lambda_vpc                            = var.lambda_vpc
  lambda_role_arn                       = var.lambda_role_arn
  lambda_job_type                       = var.lambda_job_type
  lambda_job_queue                      = var.lambda_job_queue
  cnm_r_handler_job_type                = var.cnm_r_handler_job_type
  cnm_r_job_queue                       = var.cnm_r_job_queue
  cnm_r_event_trigger                   = var.cnm_r_event_trigger
  cnm_r_allowed_account                 = var.cnm_r_allowed_account
  daac_delivery_proxy                   = var.daac_delivery_proxy
  daac_endpoint_url                     = var.daac_endpoint_url
  asg_ami                               = var.asg_ami
  asg_use_role                          = var.asg_use_role
  asg_role                              = var.asg_role
  asg_vpc                               = var.asg_vpc
  aws_account_id                        = var.aws_account_id
  lambda_cnm_r_handler_package_name     = var.lambda_cnm_r_handler_package_name
  lambda_harikiri_handler_package_name  = var.lambda_harikiri_handler_package_name
  lambda_isl_handler_package_name       = var.lambda_isl_handler_package_name
  lambda_package_release                = var.lambda_package_release
  lambda_e-misfire_handler_package_name = var.lambda_e-misfire_handler_package_name
  cop_catalog_url                       = var.cop_catalog_url
  tiurdrop_catalog_url                  = var.tiurdrop_catalog_url
  delete_old_cop_catalog                = var.delete_old_cop_catalog
  delete_old_tiurdrop_catalog           = var.delete_old_tiurdrop_catalog
  rost_catalog_url                      = var.rost_catalog_url
  delete_old_rost_catalog               = var.delete_old_rost_catalog
  environment                           = var.environment
  use_artifactory                       = var.use_artifactory
  artifactory_base_url                  = var.artifactory_base_url
  artifactory_repo                      = var.artifactory_repo
  artifactory_mirror_url                = var.artifactory_mirror_url
  use_daac_cnm                          = var.use_daac_cnm
  use_s3_uri_structure                  = var.use_s3_uri_structure
}

locals {
  lambda_repo = "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/opera/sds/pcm/lambda"
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
      "source ~/mozart/bin/activate",
      "cd ~/.sds/files",
      "~/mozart/ops/hysds/scripts/ingest_dataset.py -f AOI_sacramento_valley ~/mozart/etc/datasets.json",
      "/bin/cp -f ~/mozart/ops/opera-pcm/cluster_provisioning/death-valley/rules/user_rules.json ~/.sds/rules/user_rules.json",
      "sed  -i 's/<DAAC-PROXY>/${var.daac_delivery_proxy}/g' ~/.sds/rules/user_rules.json",
      "cd ~/.sds/files/test",
      "source ~/mozart/bin/activate",
      "curl -XDELETE http://${module.common.grq_pvt_ip}:9200/user_rules",
      "curl -XDELETE http://${module.common.mozart_pvt_ip}:9200/user_rules",
      "fab -f ~/.sds/cluster.py -R mozart,grq create_all_user_rules_index",
      "./import_product_delivery_rules.sh",
      "echo Your cluster has been provisioned!",
    ]
  }
}

