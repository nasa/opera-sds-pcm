provider "aws" {
  shared_credentials_file = var.shared_credentials_file
  region                  = var.region
  profile                 = var.profile
}

module "common" {
  source = "../modules/common"
  amis                                    = var.amis
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
  lambda_job_type                         = var.lambda_job_type
  lambda_job_queue                        = var.lambda_job_queue
  cnm_r_handler_job_type                  = var.cnm_r_handler_job_type
  cnm_r_job_queue                         = var.cnm_r_job_queue
  cnm_r_event_trigger                     = var.cnm_r_event_trigger
  cnm_r_allowed_account                   = var.cnm_r_allowed_account
  cnm_r_venue                             = var.cnm_r_venue
  daac_delivery_proxy                     = var.daac_delivery_proxy
  daac_endpoint_url                       = var.daac_endpoint_url
  asg_use_role                            = var.asg_use_role
  asg_role                                = var.asg_role
  public_asg_vpc                          = var.public_asg_vpc
  private_asg_vpc                         = var.private_asg_vpc
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
  pge_names                               = var.pge_names
  pge_snapshots_date                      = var.pge_snapshots_date
  pge_release                             = var.pge_release
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
  docker_registry_bucket                  = var.docker_registry_bucket
  use_s3_uri_structure                    = var.use_s3_uri_structure
  inactivity_threshold                    = var.inactivity_threshold
  artifactory_fn_user                     = var.artifactory_fn_user
  artifactory_fn_api_key                  = var.artifactory_fn_api_key
  earthdata_user                          = var.earthdata_user
  earthdata_pass                          = var.earthdata_pass
  hls_download_timer_trigger_frequency    = var.hls_download_timer_trigger_frequency
  hlsl30_query_timer_trigger_frequency    = var.hlsl30_query_timer_trigger_frequency
  hlss30_query_timer_trigger_frequency    = var.hlss30_query_timer_trigger_frequency
  purge_es_snapshot                       = var.purge_es_snapshot
  es_snapshot_bucket                      = var.es_snapshot_bucket
  es_bucket_role_arn                      = var.es_bucket_role_arn
  run_smoke_test                          = var.run_smoke_test
}

locals {
  default_source_event_arn = "arn:aws:${var.cnm_r_event_trigger}:${var.region}:${var.aws_account_id}:${var.cnm_r_event_trigger == "kinesis" ? "stream/" : ""}${var.project}-${var.venue}-${module.common.counter}-daac-cnm-response"
  daac_proxy_cnm_r_arn     = "arn:aws:sns:${var.region}:${var.aws_account_id}:${var.project}-${var.venue}-${module.common.counter}-daac-proxy-cnm-response"
  source_event_arn         = local.default_source_event_arn
  grq_es_url               = "${var.grq_aws_es ? "https" : "http"}://${var.grq_aws_es ? var.grq_aws_es_host : module.common.grq.private_ip}:${var.grq_aws_es ? var.grq_aws_es_port : 9200}"
  lambda_repo              = "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/lambda"
  crid                     = lower(var.crid)
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
    osl_bucket       = module.common.osl_bucket
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
              echo "use_daac_cnm is ${var.use_daac_cnm}"
              if [ "${var.run_smoke_test}" = true ]; then
                ~/mozart/ops/${var.project}-pcm/cluster_provisioning/run_smoke_test.sh \
                ${var.project} \
                ${var.environment} \
                ${var.venue} \
                ${module.common.counter} \
                ${var.use_artifactory} \
                ${var.artifactory_base_url} \
                ${var.artifactory_repo} \
                ${var.artifactory_mirror_url} \
                ${var.pcm_repo} \
                ${var.pcm_branch} \
                ${var.product_delivery_repo} \
                ${var.product_delivery_branch} \
                ${module.common.mozart.private_ip} \
                ${module.common.isl_bucket} \
                ${local.source_event_arn} \
                ${var.daac_delivery_proxy} \
                ${var.use_daac_cnm} \
                ${local.crid} \
                ${var.cluster_type} || :
              fi
    EOF
    ]
  }

  provisioner "remote-exec" {
    inline = [<<-EOF
              set -ex
              source ~/.bash_profile
              if [ "${var.run_smoke_test}" = true ]; then
                ~/mozart/ops/${var.project}-pcm/conf/sds/files/test/dump_job_status.py http://127.0.0.1:8888
              fi
    EOF
    ]
  }

  provisioner "remote-exec" {
    inline = [<<-EOF
              set -ex
              source ~/.bash_profile
              if [ "${var.run_smoke_test}" = true ]; then
                pytest ~/mozart/ops/${var.project}-pcm/cluster_provisioning/dev-e2e/check_pcm.py ||:
              fi
    EOF
    ]
  }

  provisioner "remote-exec" {
    inline = [<<-EOF
              set -ex
              source ~/.bash_profile
              if [ "${var.run_smoke_test}" = true ]; then
                python ~/mozart/ops/pcm_commons/pcm_commons/tools/trigger_snapshot.py \
                  --mozart-es http://${module.common.mozart.private_ip}:9200 \
                  --grq-es ${local.grq_es_url} \
                  --metrics-es http://${module.common.metrics.private_ip}:9200 \
                  --repository snapshot-repository \
                  --policy-id hourly-snapshot
              fi
    EOF
    ]
  }

  provisioner "remote-exec" {
    when = destroy
    inline = [<<-EOF
              set -ex
              source ~/.bash_profile
              python ~/mozart/ops/opera-pcm/cluster_provisioning/clear_grq_aws_es.py
              ~/mozart/ops/opera-pcm/cluster_provisioning/purge_aws_resources.sh ${self.triggers.code_bucket} ${self.triggers.dataset_bucket} ${self.triggers.triage_bucket} ${self.triggers.lts_bucket} ${self.triggers.osl_bucket}

    EOF
    ]
  }

#  provisioner "remote-exec" {
#    inline = [
#      "set -ex",
#      "if [ \"${var.run_smoke_test}\" = true ]; then",
#      "  cat /tmp/datasets.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/datasets.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_scnm_s_messages.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_scnm_s_messages.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_cnm_s_passthru.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_cnm_s_passthru.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_cnm_r_passthru.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_cnm_r_passthru.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_stamped_dataset_result.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_stamped_dataset_result.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_cloudwatch_logs.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_cloudwatch_logs.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_tile_predict.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_tile_predict.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "  cat /tmp/check_crex_report.txt",
#      "  SUCCESS=$(grep -c ^SUCCESS /tmp/check_crex_report.txt)",
#      "  if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "cat /tmp/bach_ui_status_code.txt",
#      "SUCCESS=$(grep -c ^SUCCESS /tmp/bach_ui_status_code.txt)",
#      "if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "cat /tmp/bach_api_status_code.txt",
#      "SUCCESS=$(grep -c ^SUCCESS /tmp/bach_api_status_code.txt)",
#      "if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
#      "fi",
#      "if [ \"${var.promote_artifacts}\" = true ]; then",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/${var.artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/adaptation/${var.swot_pcm_branch}?to=/${var.target_artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/adaptation/${var.swot_pcm_branch}\"",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/${var.artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/product_delivery/${var.product_delivery_branch}?to=/${var.target_artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/product_delivery/${var.product_delivery_branch}\"",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/${var.artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/pcm_commons/${var.pcm_commons_branch}?to=/${var.target_artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/pcm_commons/${var.pcm_commons_branch}\"",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/${var.artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/lambdas/${var.lambda_package_release}?to=/${var.target_artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/lambdas/${var.lambda_package_release}\"",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/${var.artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/swot_bach_api/${var.swot_bach_api_branch}/swot_bach_api-${var.swot_bach_api_branch}.tar.gz?to=/${var.target_artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/swot_bach_api/${var.swot_bach_api_branch}/swot_bach_api-${var.swot_bach_api_branch}.tar.gz\"",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/${var.artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/swot_bach_ui/${var.swot_bach_ui_branch}/swot_bach_ui-${var.swot_bach_ui_branch}.tar.gz?to=/${var.target_artifactory_repo}/gov/nasa/jpl/swot/sds/pcm/dependencies/swot_bach_ui/${var.swot_bach_ui_branch}/swot_bach_ui-${var.swot_bach_ui_branch}.tar.gz\"",
#      "  curl -v -u ${var.artifactory_user}:${var.artifactory_api_key} -X POST \"https://cae-artifactory.jpl.nasa.gov/artifactory/api/copy/docker-develop-local/gov/nasa/jpl/swot/sds/pcm/container-iems-sds_swot-pcm/${var.swot_pcm_branch}?to=/docker-stage-local/gov/nasa/jpl/swot/sds/pcm/container-iems-sds_swot-pcm/${var.swot_pcm_branch}\"",
#      "fi",
#    ]
#  }

#  provisioner "local-exec" {
#    command = "if [ \"${var.run_smoke_test}\" = true ]; then scp -o StrictHostKeyChecking=no -q -i ${var.private_key_file} hysdsops@${module.common.mozart.private_ip}:/tmp/check_pcm.xml .; fi"
#  }
}

resource "null_resource" "smoke_test" {
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
              if [ "${var.run_smoke_test}" = true ]; then
                cd /export/home/hysdsops/mozart/ops/${var.project}-pcm

                chmod +x ./cluster_provisioning/run_opera_smoke_tests.sh
                ./cluster_provisioning/run_opera_smoke_tests.sh \
                --mozart-ip=${module.common.mozart.private_ip} \
                --grq-host="grq:9200" \
                --cnm-r-topic-arn="${module.common.cnm_response_topic_arn}" \
                --isl-bucket="${module.common.isl_bucket}" \
                --rs-bucket="${module.common.dataset_bucket}" \
                --L30-input-dir="hls_l2/l30_greenland" \
                --S30-input-dir="hls_l2/s30_louisiana" \
                --L30-data-subscriber-query-lambda=${module.common.hlsl30_query_timer.function_name} \
                --S30-data-subscriber-query-lambda=${module.common.hlss30_query_timer.function_name} \
                --artifactory-fn-api-key=${var.artifactory_fn_api_key} \
                --sample-data-artifactory-dir="${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/testdata_R1.0.0"
              fi
    EOF
    ]
  }
}
