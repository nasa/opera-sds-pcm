variable "artifactory_base_url" {
  default = "https://artifactory-fn.jpl.nasa.gov/artifactory"
}

variable "artifactory_repo" {
  default = "general"
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

variable "public_verdi_security_group_id" {
}

variable "private_verdi_security_group_id" {
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
    # HySDS v5.0.1 - September 13, 2024 - R3.1
    mozart    = "ami-013c621217878ea41" # mozart v4.26 - 240913
    metrics   = "ami-0a233846db243a8f5" # metrics v4.18 - 240913
    grq       = "ami-028c75c24e63f8476" # grq v4.19 - 240913
    factotum  = "ami-0b68ce027b778aa72" # factotum v4.17 - 240913
    autoscale = "ami-0674df68addd547ae" # verdi v4.17 patchdate - 240913
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
  default = "arn:aws:iam::681612454726:role/am-es-role"
  #  default = "arn:aws:iam::271039147104:role/am-es-role"
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

variable "po_daac_cnm_r_event_trigger" {
}

variable "asf_daac_cnm_r_event_trigger" {
}

variable "cnm_r_event_trigger_values_list" {
  description = "acceptable values for setting *_cnm_r_event_trigger"
  type        = list(string)
  default     = ["sns", "kinesis", "sqs"]
}

variable "cnm_r_allowed_account" {
}

variable "cnm_r_venue" {
}

variable "trace" {
  type = string
}

variable "po_daac_delivery_proxy" {
}

variable "po_daac_endpoint_url" {
}

variable "asf_daac_delivery_proxy" {
}

variable "asf_daac_endpoint_url" {
}

variable "asg_use_role" {
}

variable "asg_role" {
}

variable "public_asg_vpc" {
}

variable "private_asg_vpc" {
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

variable "lambda_data-subscriber-download-slc-ionosphere_handler_package_name" {
  default = "lambda-data-subscriber-download-slc-ionosphere-handler"
}

variable "lambda_report_handler_package_name" {
  default = "lambda-report-handler"
}

variable "lambda_e-misfire_handler_package_name" {
  default = "lambda-event-misfire-handler"
}

variable "lambda_batch-query_handler_package_name" {
  default = "lambda-batch-process-handler"
}

variable "lambda_package_release" {
}

variable "queues" {
  default = {
    "opera-job_worker-hls_data_ingest" = {
      "name"              = "opera-job_worker-hls_data_ingest"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l2_cslc_s1" = {
      "name"              = "opera-job_worker-sciflo-l2_cslc_s1"
      "instance_type"     = ["c7i.2xlarge", "c6a.2xlarge", "c6i.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 300
      "max_size"          = 50
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l2_cslc_s1_hist" = {
      "name"              = "opera-job_worker-sciflo-l2_cslc_s1_hist"
      "instance_type"     = ["c7i.2xlarge", "c6a.2xlarge", "c6i.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 300
      "max_size"          = 100
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l2_rtc_s1" = {
      "name"              = "opera-job_worker-sciflo-l2_rtc_s1"
      "instance_type"     = ["c7i.2xlarge", "c6a.2xlarge", "c6i.2xlarge", "c6a.4xlarge", "c6i.4xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "max_size"          = 25
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l2_rtc_s1_static" = {
      "name"              = "opera-job_worker-sciflo-l2_rtc_s1_static"
      "instance_type"     = ["r6a.2xlarge", "r6i.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "max_size"          = 25
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l3_dswx_hls" = {
      "name"              = "opera-job_worker-sciflo-l3_dswx_hls"
      "instance_type"     = ["c7a.large", "c6a.large", "c6i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 50
      "min_size"          = 0
      "max_size"          = 40
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l3_dswx_s1" = {
      "name"              = "opera-job_worker-sciflo-l3_dswx_s1"
      "instance_type"     = ["c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge", "c7i.8xlarge", "m7i.xlarge", "m7i.2xlarge", "m7i.4xlarge", "m7i.8xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l3_disp_s1" = {
      "instance_type"     = ["c7i.4xlarge", "c6a.4xlarge", "c6i.4xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 600
      "max_size"          = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l3_disp_s1_hist" = {
      "instance_type"     = ["r7i.4xlarge", "r6a.4xlarge", "r6i.4xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 600
      "max_size"          = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-sciflo-l3_dswx_ni" = {
      "name"              = "opera-job_worker-sciflo-l3_dswx_ni"
      "instance_type"     = ["c7i.2xlarge", "c6a.2xlarge", "m7i.2xlarge", "m7a.2xlarge", "c7a.2xlarge", "m6a.2xlarge", "c6i.2xlarge", "c5.2xlarge", "m6i.2xlarge", "c5a.2xlarge", "c5ad.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-send_cnm_notify" = {
      "name"              = "opera-job_worker-send_cnm_notify"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "max_size"          = 40
      "total_jobs_metric" = true
    }
    "opera-job_worker-rcv_cnm_notify" = {
      "name"              = "opera-job_worker-rcv_cnm_notify"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "max_size"          = 20
      "total_jobs_metric" = true
    }
    "opera-job_worker-hls_data_query" = {
      "name"              = "opera-job_worker-hls_data_query"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-hls_data_download" = {
      "name"              = "opera-job_worker-hls_data_download"
      "instance_type"     = ["c5n.large", "m5dn.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-slc_data_query" = {
      "name"              = "opera-job_worker-slc_data_query"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-slc_data_query_hist" = {
      "name"              = "opera-job_worker-slc_data_query_hist"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-slc_data_download" = {
      "name"              = "opera-job_worker-slc_data_download"
      "instance_type"     = ["m6in.2xlarge", "c5n.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-slc_data_download_hist" = {
      "name"              = "opera-job_worker-slc_data_download_hist"
      "instance_type"     = ["m6in.2xlarge", "c5n.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 25
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-slc_data_download_ionosphere" = {
      "name"              = "opera-job_worker-slc_data_download_ionosphere"
      "instance_type"     = ["m6a.large", "m5.large", "m6i.large", "m5ad.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-rtc_data_query" = {
      "name"              = "opera-job_worker-rtc_data_query"
      "instance_type"     = ["m6i.large", "m6a.large", "m5.large", "m5a.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-cslc_data_query" = {
      "name"              = "opera-job_worker-cslc_data_query"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-cslc_data_query_hist" = {
      "name"              = "opera-job_worker-cslc_data_query_hist"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-cslc_data_download" = {
      "name"              = "opera-job_worker-cslc_data_download"
      "instance_type"     = ["c5n.2xlarge", "m5dn.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 250
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-submit_pending_jobs" = {
      "name"              = "opera-job_worker-submit_pending_jobs"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-rtc_data_download" = {
      "name"              = "opera-job_worker-rtc_data_download"
      "instance_type"     = ["c6in.large", "c5n.large", "m6in.large", "m5n.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-cslc_data_download_hist" = {
      "name"              = "opera-job_worker-cslc_data_download_hist"
      "instance_type"     = ["c5n.2xlarge", "m5dn.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 250
      "min_size"          = 0
      "max_size"          = 25
      "total_jobs_metric" = true
      "use_private_vpc"   = false
    }
    "opera-job_worker-ecmwf-merger" = {
      "instance_type"     = ["r5a.4xlarge", "r6a.4xlarge", "r7a.4xlarge", "r5.4xlarge", "r6i.4xlarge", "r7i.4xlarge", "m5a.8xlarge", "m6a.8xlarge", "m7a.8xlarge", "m5.8xlarge", "m6i.8xlarge", "m7i.8xlarge", "m7i-flex.8xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 600
      "max_size"          = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-ecmwf-subsetter" = {
      "instance_type"     = ["r5a.4xlarge", "r6a.4xlarge", "r7a.4xlarge", "r5.4xlarge", "r6i.4xlarge", "r7i.4xlarge", "m5a.8xlarge", "m6a.8xlarge", "m7a.8xlarge", "m5.8xlarge", "m6i.8xlarge", "m7i.8xlarge", "m7i-flex.8xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 600
      "max_size"          = 10
      "total_jobs_metric" = true
    }
    "opera-job_worker-timer" = {
      "name"              = "opera-job_worker-timer"
      "instance_type"     = ["t3a.medium", "t3.medium", "t2.medium", "c6i.large", "t3a.large", "m6a.large", "c6a.large", "c5a.large", "r7i.large", "c7i.large"]
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "max_size"          = 10
      "total_jobs_metric" = false
    }
    "opera-job_worker-pge_smoke_test_amd" = {
      "name"              = "opera-job_worker-pge_smoke_test_amd"
      "instance_type"     = ["r6a.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 250
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-pge_smoke_test_intel" = {
      "name"              = "opera-job_worker-pge_smoke_test_intel"
      "instance_type"     = ["r6i.2xlarge"]
      "root_dev_size"     = 50
      "data_dev_size"     = 250
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = false
      "use_private_vpc"   = false
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

variable "use_daac_cnm_r" {
  default = true
}

variable "cnm_r_sqs_arn" {
  type = map(string)
  default = {
    dev  = "arn:aws:sqs:us-west-2:681612454726:opera-dev-daac-cnm-response"
    int  = "arn:aws:sqs:us-west-2:681612454726:opera-dev-fwd-daac-cnm-response"
    test = "arn:aws:sqs:us-west-2:337765570207:opera-int-daac-cnm-response"
    prod = "arn:aws:sqs:us-west-2:907504701509:opera-ops-daac-cnm-response"
  }
}

variable "lambda_log_retention_in_days" {
  type    = number
  default = 30
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.2"
    "cslc_s1"  = "2.1.1"
    "rtc_s1"   = "2.1.1"
    "dswx_s1"  = "3.0.2"
    "disp_s1"  = "3.0.0-rc.4.0"
    "dswx_ni"  = "4.0.0-er.2.0"
  }
}

variable "docker_registry_bucket" {
  default = "opera-pcm-registry-bucket"
}

variable "pge_snapshots_date" {
  default = "20231023-2.1.0"
}

variable "crid" {
  default = "D00100"
}

variable "hls_provider" {
  default = "LPCLOUD"
}

variable "hls_download_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "hlsl30_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "hlss30_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "slc_provider" {
  default = "ASF"
}

variable "slc_download_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "slcs1a_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "slc_ionosphere_download_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "rtc_provider" {
  default = "ASF"
}

variable "rtc_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "cslc_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "batch_query_timer_trigger_frequency" {
  default = "rate(1 minute)"
}

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

variable "pge_sim_mode" {
  type    = bool
  default = true
}

variable "artifactory_fn_user" {
  description = "Username to use for authenticated Artifactory API calls."
  default     = ""
}

variable "artifactory_fn_api_key" {
  description = "Artifactory API key for authenticated Artifactory API calls. Must map to artifactory_username."
}

variable "dataspace_user" {
  default = ""
}

variable "dataspace_pass" {
  default = ""
}

variable "earthdata_user" {
  default = ""
}

variable "earthdata_pass" {
  default = ""
}

variable "clear_s3_aws_es" {
  type    = bool
  default = true
}
