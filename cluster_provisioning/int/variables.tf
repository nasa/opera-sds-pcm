# globals
#
# venue : userId
# counter : 1-n
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#
variable "artifactory_base_url" {
  default = "https://artifactory-fn.jpl.nasa.gov/artifactory"
}

variable "artifactory_repo" {
  default = "general-develop"
}

variable "artifactory_mirror_url" {
  default = "s3://opera-dev/artifactory_mirror"
}

variable "hysds_release" {
  default = ""
}

variable "pcm_repo" {
  default = "github.com/nasa/opera-sds-pcm.git"
}

variable "pcm_branch" {
  default = ""
}

variable "pcm_commons_repo" {
  default = ""
}

variable "pcm_commons_branch" {
  default = ""
}

variable "product_delivery_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git"
}

variable "product_delivery_branch" {
  default = ""
}

variable "bach_api_repo" {
  default = "github.com/nasa/opera-sds-bach-api.git"
}

variable "bach_api_branch" {
  default = ""
}

variable "bach_ui_repo" {
  default = "github.com/nasa/opera-sds-bach-ui.git"
}

variable "bach_ui_branch" {
  default = ""
}

variable "venue" {
  default = ""
}

variable "counter" {
  default = ""
}

variable "private_key_file" {
  default = ""
}

variable "git_auth_key" {
  default = ""
}

variable "jenkins_api_user" {
  default = ""
}

variable "keypair_name" {
  default = ""
}

variable "jenkins_api_key" {
  default = ""
}

variable "jenkins_host" {
  default = ""
}

variable "jenkins_enabled" {
  type = bool
  default = false
}

variable "artifactory_fn_api_key" {
  default = ""
}

variable "ops_password" {
  default = ""
}

variable "shared_credentials_file" {
  default = ""
}

#
# "default" links to [default] profile in "shared_credentials_file" above
#
variable "profile" {
  default = ""
}

variable "project" {
  default = ""
}

variable "region" {
  default = ""
}

variable "az" {
  default = ""
}

variable "grq_aws_es" {
  type = bool
  default = false
}

variable "grq_aws_es_host" {
  default = ""
}

variable "grq_aws_es_host_private_verdi" {
  default = ""
}

variable "grq_aws_es_port" {
  default = 443
}

variable "use_grq_aws_es_private_verdi" {
  default = ""
}

variable "subnet_id" {
  default = ""
}

variable "verdi_security_group_id" {
  default = ""
}

variable "cluster_security_group_id" {
  default = ""
}

variable "pcm_cluster_role" {
}

variable "pcm_verdi_role" {
}

# staging area vars

variable "lambda_vpc" {
  default = ""
}

variable "lambda_role_arn" {
  default = ""
}

variable "lambda_job_type" {
  default = "INGEST_STAGED"
}

variable "lambda_job_queue" {
  default = "opera-job_worker-small"
}

# CNM Response job vars

variable "cnm_r_handler_job_type" {
  default = "process_cnm_response"
}

variable "cnm_r_job_queue" {
  default = "opera-job_worker-rcv_cnm_notify"
}

variable "cnm_r_event_trigger" {
  default = "sqs"
}

variable "cnm_r_allowed_account" {
  default = "*"
}

####### CNM Response job vars #######
variable "daac_delivery_proxy" {
  default = "arn:aws:sqs:us-west-2:337765570207:daac-proxy-for-opera-int"
}

variable "use_daac_cnm" {
  type = bool
  default = false
}

variable "daac_endpoint_url" {
  default = ""
}
# asg vars
variable "asg_use_role" {
  type = bool
  default = true
}

variable "asg_role" {
  default = ""
}

variable "asg_vpc" {
  default = ""
}

variable "aws_account_id" {
  default = ""
}

variable "lambda_package_release" {
  default = ""
}

variable "job_catalog_url" {
  default = ""
}

variable "delete_old_job_catalog" {
  type = bool
  default = false
}

variable "environment" {
  default = ""
}

# ami vars
variable "amis" {

}


# mozart vars
variable "mozart" {

}

# metrics vars
variable "metrics" {

}

# grq vars
variable "grq" {

}

# factotum vars
variable "factotum" {

}

# ci vars
variable "ci" {

}

variable "common_ci" {

}

# autoscale vars
variable "autoscale" {

}

variable "use_artifactory" {
  type = bool
  default = true
}

variable "event_misfire_trigger_frequency" {
  default = "rate(5 minutes)"
}

variable "event_misfire_delay_threshold_seconds" {
  type = number
  default = 60
}

variable "lambda_log_retention_in_days" {
  type = number
  default = 30
}

variable "docker_registry_bucket" {
  default = ""
}

variable "pge_snapshots_date" {
  default = ""
}

variable "pge_names" {
  default = "opera_pge-dswx_hls"
}

variable "pge_release" {
  default = "beta_prep"
}

variable "crid" {
  default = ""
}

variable "cluster_type" {
  default = ""
}

variable "hls_download_timer_trigger_frequency" {}

variable "hlsl30_query_timer_trigger_frequency" {}

variable "hlss30_query_timer_trigger_frequency" {}

variable "obs_acct_report_timer_trigger_frequency" {
  default = "cron(0 0 * * ? *)"
}

variable "rs_fwd_bucket_ingested_expiration" {
  default = 14
}

variable "dataset_bucket" {
  default = ""
}

variable "code_bucket" {
  default = ""
}

variable "lts_bucket" {
  default = ""
}

variable "triage_bucket" {
  default = ""
}

variable "isl_bucket" {
  default = ""
}

variable "osl_bucket" {
  default = ""
}

variable "osl_report_staging_area" {
  default = ""
}

variable "use_s3_uri_structure" {
  type = bool
  default = true
}

variable "inactivity_threshold" {
  type = number
  default = 1800
}

variable "run_smoke_test" {
  type = bool
  default = true
}

variable "purge_es_snapshot" {
  type = bool
  default = false
}

variable "es_snapshot_bucket" {
  default = ""
}

variable "es_bucket_role_arn" {
  default = ""
}

variable "artifactory_fn_user" {
  default = ""
}

variable "earthdata_user" {
  default = ""
}

variable "earthdata_pass" {
  default = ""
}

variable "queues" {
  default = {
    "opera-job_worker-gpu" = {
      "instance_type" = [
        "p2.xlarge",
        "p3.2xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size" = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-small" = {
      "instance_type" = [
        "t2.medium",
        "t3a.medium",
        "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size" = 100
      "total_jobs_metric" = true
    }
    "opera-job_worker-large" = {
      "instance_type" = [
        "t2.medium",
        "t3a.medium",
        "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size" = 100
      "total_jobs_metric" = true
    }
    "opera-job_worker-send_cnm_notify" = {
      "instance_type" = [
        "t2.medium",
        "t3a.medium",
        "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size" = 100
      "total_jobs_metric" = true
    }
    "opera-job_worker-rcv_cnm_notify" = {
      "instance_type" = [
        "t2.medium",
        "t3a.medium",
        "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size" = 100
      "total_jobs_metric" = true
    }
    "opera-workflow_profiler" = {
      "instance_type" = [
        "p2.xlarge",
        "p3.2xlarge",
        "r5.2xlarge",
        "r5.4xlarge",
        "r5.8xlarge",
        "r5.12xlarge",
        "r5.16xlarge",
        "r5.24xlarge",
        "r5.metal"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size" = 10
      "total_jobs_metric" = true
    }
  }
}
