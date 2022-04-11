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
}

variable "pcm_repo" {
}

variable "pcm_branch" {
}

variable "pcm_commons_repo" {
}

variable "pcm_commons_branch" {
}

variable "product_delivery_repo" {
}

variable "product_delivery_branch" {
}

variable "bach_api_repo" {
}

variable "bach_api_branch" {
}

variable "bach_ui_repo" {
}

variable "bach_ui_branch" {
}

variable "venue" {
}

variable "counter" {
}

variable "private_key_file" {
}

variable "git_auth_key" {
}

variable "jenkins_api_user" {
}

variable "keypair_name" {
}

variable "jenkins_api_key" {
}

variable "jenkins_host" {
  default = "https://opera-pcm-ci.jpl.nasa.gov"
}

variable "jenkins_enabled" {
  default = true
}

variable "ops_password" {
}

variable "shared_credentials_file" {
}

variable "profile" {
}

variable "project" {
}

variable "region" {
}

variable "az" {
}

variable "subnet_id" {
}

variable "verdi_security_group_id" {
}

variable "cluster_security_group_id" {
}

variable "pcm_cluster_role" {
}

variable "pcm_verdi_role" {
}

variable "grq_aws_es" {
  //  boolean
}

variable "grq_aws_es_host" {
}

variable "grq_aws_es_port" {
}

variable "grq_aws_es_host_private_verdi" {
}

variable "use_grq_aws_es_private_verdi" {
}

variable "purge_es_snapshot" {
  default = true
}

# ami vars
variable "amis" {
  type = map(string)
  default = {
    # HySDS v4.0.1-beta.8-oraclelinux
    mozart    = "ami-02fcd254c71ff0fa0"  # opera dev mozart - ol8
    metrics   = "ami-0a54a14946e0bb52f"  # opera dev metrics - ol8
    grq       = "ami-0a11c7d42e24fe7d5"  # opera dev grq - ol8
    factotum  = "ami-0ce5e6a66b7732993"  # opera dev factotum - ol8
    ci        = "ami-0caed57c920d65ea8"  # OL8 All-project verdi v4.11
    autoscale = "ami-0caed57c920d65ea8"  # OL8 All-project verdi v4.11
  }
}

variable "mozart" {
}

variable "metrics" {
}

variable "grq" {
}

variable "factotum" {
}

variable "ci" {
}

variable "common_ci" {
}

variable "autoscale" {
}

variable "lambda_vpc" {
}

variable "lambda_role_arn" {
}

variable "es_bucket_role_arn" {
  default = "arn:aws:iam::271039147104:role/am-es-role"
}

variable "es_snapshot_bucket" {
  default = "opera-dev-es-bucket"
}

variable "lambda_job_type" {
}

variable "lambda_job_queue" {
}

variable "cnm_r_handler_job_type" {
}

variable "cnm_r_job_queue" {
}

variable "cnm_r_event_trigger" {
}

variable "cnm_r_event_trigger_values_list" {
  description = "acceptable values for setting cnm_r_event_trigger"
  type        = list(string)
  default     = ["sns", "kinesis", "sqs"]
}

variable "cnm_r_allowed_account" {
}

variable "daac_delivery_proxy" {
}

variable "daac_endpoint_url" {
}

variable "asg_use_role" {
}

variable "asg_role" {
}

variable "asg_vpc" {
}

variable "aws_account_id" {
}

variable "lambda_cnm_r_handler_package_name" {
  default = "lambda-cnm-r-handler"
}

variable "lambda_harikiri_handler_package_name" {
  default = "lambda-harikiri-handler"
}

variable "lambda_isl_handler_package_name" {
  default = "lambda-isl-handler"
}

variable "lambda_timer_handler_package_name" {
  default = "lambda-timer-handler"
}

variable "lambda_data-subscriber-download_handler_package_name" {
  default = "lambda-data-subscriber-download-handler"
}

variable "lambda_data-subscriber-query_handler_package_name" {
  default = "lambda-data-subscriber-query-handler"
}

variable "lambda_report_handler_package_name" {
  default = "lambda-report-handler"
}

variable "lambda_e-misfire_handler_package_name" {
  default = "lambda-event-misfire-handler"
}

variable "lambda_package_release" {
}

variable "queues" {
  default = {
    "opera-job_worker-gpu" = {
      "instance_type" = ["p2.xlarge", "p3.2xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-small" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-large" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-send_cnm_notify" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-rcv_cnm_notify" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-workflow_profiler" = {
      "instance_type" = ["p2.xlarge", "p3.2xlarge", "r5.2xlarge", "r5.4xlarge", "r5.8xlarge", "r5.12xlarge", "r5.16xlarge", "r5.24xlarge", "r5.metal"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    },
    "opera-job_worker-timer" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 100
      "max_size"      = 10
    }
  }
}

variable "environment" {
}

variable "use_artifactory" {
}

variable "event_misfire_trigger_frequency" {
  default = "rate(5 minutes)"
}

variable "event_misfire_delay_threshold_seconds" {
  type    = number
  default = 60
}

variable "use_daac_cnm" {
}

variable "daac_cnm_sqs_arn" {
  type = map(string)
  default = {
    dev  = "arn:aws:sqs:us-west-2:681612454726:opera-dev-daac-cnm-response"
    test = "arn:aws:sqs:us-west-2:399787141461:opera-test-daac-cnm-response"
    int  = "arn:aws:sqs:us-west-2:681612454726:opera-int-daac-cnm-response"
  }
}

variable "lambda_log_retention_in_days" {
  type    = number
  default = 30
}

variable "pge_names" {
  default = "opera_pge-dswx_hls"
}

variable "docker_registry_bucket" {
  default = "opera-pcm-registry-bucket"
}

variable "pge_snapshots_date" {
  default = "20220401-1.0.0-er.3.0"
}

variable "pge_release" {
  default = "1.0.0-er.3.0"
}

variable "crid" {
  default = "D00100"
}

variable "data_download_timer_trigger_frequency" {}

variable "data_query_timer_trigger_frequency" {}

variable "obs_acct_report_timer_trigger_frequency" {}

variable "cluster_type" {}

variable "valid_cluster_type_values" {
  type    = list(string)
  default = ["forward", "reprocessing"]
}

variable "rs_fwd_bucket_ingested_expiration" {
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
  default = "accountability_reports"
}

variable "isl_staging_area" {
  default = "data_subscriber"
}

variable "use_s3_uri_structure" {
  default = false
}

variable "inactivity_threshold" {
  type    = number
  default = 600
}

variable "run_smoke_test" {
  type    = bool
  default = true
}

variable "artifactory_fn_user" {
  description = "Username to use for authenticated Artifactory API calls."
  default = ""
}

variable "artifactory_fn_api_key" {
  description = "Artifactory API key for authenticated Artifactory API calls. Must map to artifactory_username."
}

variable "earthdata_user" {
  default = ""
}

variable "earthdata_pass" {
  default = ""
}
