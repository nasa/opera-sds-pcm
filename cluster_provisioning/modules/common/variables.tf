variable "artifactory_base_url" {
  default = "https://artifactory-fn.jpl.nasa.gov/artifactory"
}

variable "artifactory_repo" {
  default = "general"
}

variable "artifactory_mirror_url" {
  default = "s3://opera-pcm-registry-bucket/pcm/artifactory_mirror"
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

variable "es_snapshot_destroy_action" {
  default = "purge"

  validation {
    condition     = contains(["leave", "purge", "create-new"], var.es_snapshot_destroy_action)
    error_message = "The value of es_snapshot_destroy_action must be one of \"leave\", \"purge\", \"create-new\"."
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
}

variable "es_snapshot_bucket" {
  default = "opera-dev-es-bucket"
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

variable "ssm_account_id" {
}

variable "lambda_cnm_r_handler_package_name" {
  default = "lambda-cnm-r-handler"
}

variable "lambda_harikiri_handler_package_name" {
  default = "lambda-harikiri-handler"
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

variable "lambda_cnm_accountability_handler_package_name" {
  default = "lambda-cnm_accountability-handler"
}

variable "lambda_catalog-ingest_handler_package_name" {
  default = "lambda-catalog-ingest-handler"
}

variable "lambda_grq-on-demand_handler_package_name" {
  default = "lambda-grq-on-demand-handler"
}

variable "lambda_opensearch_shards_monitor_package_name" {
  default = "lambda-opensearch-shards-monitor"
}

variable "lambda_package_release" {
}

variable "queues" {
  default = {
    "opera-job_worker-sciflo-l2_cslc_s1" = {
      "name"              = "opera-job_worker-sciflo-l2_cslc_s1"
      "log_file_name"     = "run_sciflo_L2_CSLC_S1"
      "instance_type"     = ["c5a.2xlarge", "c6a.2xlarge", "c7a.2xlarge", "c7i.2xlarge", "c8a.2xlarge", "c8i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 300
      "max_size"          = 50
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l2_cslc_s1_hist" = {
      "name"              = "opera-job_worker-sciflo-l2_cslc_s1_hist"
      "log_file_name"     = "run_sciflo_L2_CSLC_S1"
      "instance_type"     = ["c5a.2xlarge", "c6a.2xlarge", "c7a.2xlarge", "c7i.2xlarge", "c8a.2xlarge", "c8i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 300
      "max_size"          = 100
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l2_rtc_s1" = {
      "name"          = "opera-job_worker-sciflo-l2_rtc_s1"
      "log_file_name" = "run_sciflo_L2_RTC_S1"
      "instance_type" = ["c5a.2xlarge", "c6a.2xlarge", "c7a.2xlarge", "c7i.2xlarge", "c8a.2xlarge", "c8i.2xlarge",
      "c5a.4xlarge", "c6a.4xlarge", "c6i.4xlarge", "c7a.4xlarge", "c7i.4xlarge", "c8a.4xlarge", "c8i.4xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "max_size"          = 25
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l2_rtc_s1_static" = {
      "name"              = "opera-job_worker-sciflo-l2_rtc_s1_static"
      "log_file_name"     = "run_sciflo_L2_RTC_S1"
      "instance_type"     = ["r5a.2xlarge", "r6a.2xlarge", "r6i.2xlarge", "r7a.2xlarge", "r7i.2xlarge", "r8a.2xlarge", "r8i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "max_size"          = 25
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l3_dswx_hls" = {
      "name"              = "opera-job_worker-sciflo-l3_dswx_hls"
      "log_file_name"     = "run_sciflo_L3_DSWx_HLS"
      "instance_type"     = ["m6a.large", "m6i.large", "m7a.large", "m7i.large", "m8a.large", "m8i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 40
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l3_dswx_s1" = {
      "name"          = "opera-job_worker-sciflo-l3_dswx_s1"
      "log_file_name" = "run_sciflo_L3_DSWx_S1"
      "instance_type" = ["c6i.2xlarge", "c7i.2xlarge", "c8i.2xlarge", "c6i.4xlarge", "c7i.4xlarge", "c8i.4xlarge",
      "m6i.2xlarge", "m7i.2xlarge", "m8i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l3_disp_s1" = {
      "name"              = "opera-job_worker-sciflo-l3_disp_s1"
      "log_file_name"     = "run_sciflo_L3_DISP_S1"
      "instance_type"     = ["m7i.8xlarge", "c6i.8xlarge", "c7i.8xlarge", "c8i.8xlarge", "m6i.8xlarge", "m8i.8xlarge"]
      "user_data"         = "launch_template_user_data_disp_s1.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 500
      "max_size"          = 100
      "total_jobs_metric" = true
      "use_on_demand"     = true
    }
    "opera-job_worker-sciflo-l3_disp_s1_hist" = {
      "name"              = "opera-job_worker-sciflo-l3_disp_s1_hist"
      "log_file_name"     = "run_sciflo_L3_DISP_S1"
      "instance_type"     = ["m7i.8xlarge", "c6i.8xlarge", "c7i.8xlarge", "c8i.8xlarge", "m6i.8xlarge", "m8i.8xlarge"]
      "user_data"         = "launch_template_user_data_disp_s1.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 500
      "max_size"          = 100
      "total_jobs_metric" = true
      "use_on_demand"     = true
    }
    "opera-job_worker-sciflo-l3_disp_s1_static" = {
      "name"              = "opera-job_worker-sciflo-l3_disp_s1_static"
      "log_file_name"     = "run_sciflo_L3_DISP_S1_STATIC"
      "instance_type"     = ["m6a.large", "m6i.large", "m7a.large", "m7i.large", "m8a.large", "m8i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 40
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-evaluator" = {
      "name"              = "opera-job_worker-evaluator"
      "log_file_name"     = "run_evaluator"
      "instance_type"     = ["c7i.large", "c7a.large", "c6a.large", "c6i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 100
      "total_jobs_metric" = true
      "use_on_demand"     = false
      "use_private_vpc"   = false
    }
    "opera-job_worker-evaluator_verdi" = {
      "name"              = "opera-job_worker-evaluator_verdi"
      "log_file_name"     = "run_evaluator"
      "instance_type"     = ["c7i.large", "c7a.large", "c6a.large", "c6i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 100
      "total_jobs_metric" = true
      "use_on_demand"     = false
      "use_private_vpc"   = true
    }
    "opera-job_worker-sciflo-l3_dswx_ni" = {
      "name"              = "opera-job_worker-sciflo-l3_dswx_ni"
      "log_file_name"     = "run_sciflo_L3_DSWx_NI"
      "instance_type"     = ["c5a.4xlarge", "c6a.4xlarge", "c7a.4xlarge", "c8a.4xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 600
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l3_dist_s1" = {
      "name"          = "opera-job_worker-sciflo-l3_dist_s1"
      "log_file_name" = "run_sciflo_L3_DIST_S1"

      // Compute optimized 4x large & GP 2xlarge - about 20/32 GB of memory used
      // Good for 4-3-3 on SAS v2.0.11
      "instance_type" = ["c8a.4xlarge", "c8i.4xlarge", "c7a.4xlarge", "c7i.4xlarge", "c6a.4xlarge", "c6i.4xlarge",
      "m8a.2xlarge", "m8i.2xlarge", "m7a.2xlarge", "m7i.2xlarge", "m6a.2xlarge", "m6i.2xlarge"]

      // General purpose 8x large - works well with 8-6-6 w/ stride=7 & parallel npe=4 (tested on m8a)
      // Last used for 8-6-6 on SAS v2.0.9
      // "instance_type"     = ["m8a.8xlarge", "m7a.8xlarge", "m6a.8xlarge", "m5a.8xlarge"]

      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 50
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l4_tropo" = {
      "name"          = "opera-job_worker-sciflo-l4_tropo"
      "log_file_name" = "run_sciflo_L4_TROPO"
      "instance_type" = ["m5a.4xlarge", "m6a.4xlarge", "m6i.4xlarge", "m7i.4xlarge", "m8a.4xlarge", "m8i.4xlarge",
        "r5a.2xlarge", "r6a.2xlarge", "r6i.2xlarge", "r7a.2xlarge", "r7i.2xlarge", "r8a.2xlarge",
      "r8i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-l3_disp_ni" = {
      "name"          = "opera-job_worker-sciflo-l3_disp_ni"
      "log_file_name" = "run_sciflo_L3_DISP_NI"
      "instance_type" = ["m6a.8xlarge", "m7a.8xlarge", "m8a.8xlarge",
      "r6a.4xlarge", "r7a.4xlarge", "r8a.4xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 900
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_on_demand"     = true
    }
    "opera-job_worker-sciflo-l4_cal_disp" = {
      "name"          = "opera-job_worker-sciflo-l4_cal_disp"
      "log_file_name" = "run_sciflo_L4_CAL_DISP"
      "instance_type" = ["c6i.xlarge", "c7i.xlarge", "c8i.xlarge", "c5a.xlarge", "c6a.xlarge", "c7a.xlarge", "c8a.xlarge",
      "c6i.2xlarge", "c7i.2xlarge", "c8i.2xlarge", "c5a.2xlarge", "c6a.2xlarge", "c7a.2xlarge", "c8a.2xlarge", ]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 50
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-sciflo-product_update" = {
      "name"          = "opera-job_worker-sciflo-product_update"
      "log_file_name" = "run_sciflo_product_update"
      "instance_type" = ["c7i.2xlarge", "c6a.2xlarge", "m7i.2xlarge", "m7a.2xlarge", "c7a.2xlarge", "m6a.2xlarge",
      "c6i.2xlarge", "c5.2xlarge", "m6i.2xlarge", "c5a.2xlarge", "c5ad.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 150
      "min_size"          = 0
      "max_size"          = 150 // Make this large because it needs to bulk process a lot of data
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-large_job_retry" = {
      "name"              = "opera-job_worker-large_job_retry"
      "log_file_name"     = "run_large_job_retry"
      "instance_type"     = ["m6a.8xlarge", "m7a.8xlarge", "m8a.8xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 100
      "data_dev_size"     = 900
      "min_size"          = 0
      "max_size"          = 20
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    # Split by DAAC (not by product) so ops can drain delivery to one DAAC
    # without affecting the other (e.g., scale _asf to 0 during an ASF DAAC
    # outage, keep _podaac running). Per-product granularity still
    # available via OS user_rules-grq trigger-rule disable.
    "opera-job_worker-send_cnm_notify_asf" = {
      "name" = "opera-job_worker-send_cnm_notify_asf"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large",
        "c7gn.large", "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large",
      "c8gn.large", "c8i-flex.large", "c8i.large", "c8id.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "max_size"          = 150
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-send_cnm_notify_podaac" = {
      "name" = "opera-job_worker-send_cnm_notify_podaac"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large",
        "c7gn.large", "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large",
      "c8gn.large", "c8i-flex.large", "c8i.large", "c8id.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "max_size"          = 50
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-rcv_cnm_notify" = {
      "name" = "opera-job_worker-rcv_cnm_notify"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large",
        "c7gn.large", "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large",
      "c8gn.large", "c8i-flex.large", "c8i.large", "c8id.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "max_size"          = 20
      "total_jobs_metric" = true
      "use_on_demand"     = false
    }
    "opera-job_worker-hls_data_query" = {
      "name" = "opera-job_worker-hls_data_query"
      "instance_type" = ["c5.xlarge", "c5a.xlarge", "c5ad.xlarge", "c5d.xlarge", "c6a.xlarge", "c6g.xlarge", "c6gd.xlarge",
        "c6gn.xlarge", "c6i.xlarge", "c6id.xlarge", "c6in.xlarge", "c7a.xlarge", "c7g.xlarge", "c7gd.xlarge",
        "c7gn.xlarge", "c7i-flex.xlarge", "c7i.xlarge", "c8a.xlarge", "c8g.xlarge", "c8gb.xlarge",
      "c8gd.xlarge", "c8gn.xlarge", "c8i-flex.xlarge", "c8i.xlarge", "c8id.xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-hls_data_download" = {
      "name" = "opera-job_worker-hls_data_download"
      "instance_type" = ["c6gd.large", "c7gd.large", "c8g.large", "c6a.large", "c5.large", "c5ad.large", "c8id.large",
        "m8gd.large", "c8gd.large", "c7i-flex.large", "c6g.large", "c8gn.large", "m7gd.large", "m7g.large", "m6g.large",
        "m7i-flex.large", "c8i.large", "c8gb.large", "c5d.large", "c7g.large", "c5a.large", "c6gn.large", "c8a.large",
        "c7i.large", "m8id.large", "c6id.large", "m7i.large", "c7a.large", "m8g.large", "m6id.large", "m6gd.large",
        "c6i.large", "m5a.large", "m5.large", "m6i.large", "m6a.large", "c5n.large", "m5ad.large", "c8i-flex.large",
      "c6in.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-slc_data_query" = {
      "name" = "opera-job_worker-slc_data_query"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large",
        "c7gn.large", "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large",
      "c8gn.large", "c8i-flex.large", "c8i.large", "c8id.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-slc_data_query_hist" = {
      "name" = "opera-job_worker-slc_data_query_hist"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large",
        "c7gn.large", "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large",
      "c8gn.large", "c8i-flex.large", "c8i.large", "c8id.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-slc_data_download" = {
      "name" = "opera-job_worker-slc_data_download"
      "instance_type" = ["c6gd.2xlarge", "c6g.2xlarge", "m6g.2xlarge", "m8gd.2xlarge", "c7gd.2xlarge", "c8gd.2xlarge",
        "c6gn.2xlarge", "m7g.2xlarge", "c8g.2xlarge", "c7g.2xlarge", "c6id.2xlarge", "c8id.2xlarge", "c6a.2xlarge",
        "c8gn.2xlarge", "c6i.2xlarge", "c8gb.2xlarge", "c5.2xlarge", "m6i.2xlarge", "c5ad.2xlarge", "m7gd.2xlarge",
        "c7i.2xlarge", "m8g.2xlarge", "c8i-flex.2xlarge", "m6id.2xlarge", "m5a.2xlarge", "c5d.2xlarge", "m8i-flex.2xlarge",
        "c5a.2xlarge", "m6gd.2xlarge", "c7a.2xlarge", "c8a.2xlarge", "m5.2xlarge", "m7i-flex.2xlarge", "c8i.2xlarge",
      "m8id.2xlarge", "m5d.2xlarge", "c7i-flex.2xlarge", "m6a.2xlarge", "m8a.2xlarge", "m7i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-slc_data_download_hist" = {
      "name" = "opera-job_worker-slc_data_download_hist"
      "instance_type" = ["c6gd.2xlarge", "c6g.2xlarge", "m6g.2xlarge", "m8gd.2xlarge", "c7gd.2xlarge", "c8gd.2xlarge",
        "c6gn.2xlarge", "m7g.2xlarge", "c8g.2xlarge", "c7g.2xlarge", "c6id.2xlarge", "c8id.2xlarge", "c6a.2xlarge",
        "c8gn.2xlarge", "c6i.2xlarge", "c8gb.2xlarge", "c5.2xlarge", "m6i.2xlarge", "c5ad.2xlarge", "m7gd.2xlarge",
        "c7i.2xlarge", "m8g.2xlarge", "c8i-flex.2xlarge", "m6id.2xlarge", "m5a.2xlarge", "c5d.2xlarge", "m8i-flex.2xlarge",
        "c5a.2xlarge", "m6gd.2xlarge", "c7a.2xlarge", "c8a.2xlarge", "m5.2xlarge", "m7i-flex.2xlarge", "c8i.2xlarge",
      "m8id.2xlarge", "m5d.2xlarge", "c7i-flex.2xlarge", "m6a.2xlarge", "m8a.2xlarge", "m7i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 25
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-slc_data_download_ionosphere" = {
      "name"              = "opera-job_worker-slc_data_download_ionosphere"
      "instance_type"     = ["m6a.large", "m5.large", "m6i.large", "m5ad.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-rtc_data_query" = {
      "name"              = "opera-job_worker-rtc_data_query"
      "instance_type"     = ["m6i.large", "m6a.large", "m5.large", "m5a.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-rtc_for_dist_data_query" = {
      "name"              = "opera-job_worker-rtc_for_dist_data_query"
      "instance_type"     = ["m6i.large", "m6a.large", "m5.large", "m5a.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-rtc_for_dist_data_query_hist" = {
      "name" = "opera-job_worker-rtc_for_dist_data_query_hist"
      "instance_type" = ["m8a.large", "m8i-flex.large", "m8i.large", "m7a.large", "m7i-flex.large", "m6i.large",
      "m6a.large", "m5.large", "m5a.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 100
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-dist_s1_hist_on_first" = {
      "name" = "opera-job_worker-dist_s1_hist_on_first"
      "instance_type" = ["m8a.large", "m8i-flex.large", "m8i.large", "m7a.large", "m7i-flex.large", "m6i.large",
      "m6a.large", "m5.large", "m5a.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 20
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-dist_s1_hist_on_publication" = {
      "name" = "opera-job_worker-dist_s1_hist_on_publication"
      "instance_type" = ["m8a.large", "m8i-flex.large", "m8i.large", "m7a.large", "m7i-flex.large", "m6i.large",
      "m6a.large", "m5.large", "m5a.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 20
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-dist_s1_hist_on_complete" = {
      "name" = "opera-job_worker-dist_s1_hist_on_complete"
      "instance_type" = ["m8a.large", "m8i-flex.large", "m8i.large", "m7a.large", "m7i-flex.large", "m6i.large",
      "m6a.large", "m5.large", "m5a.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 20
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-cslc_data_query" = {
      "name" = "opera-job_worker-cslc_data_query"
      "instance_type" = ["c5.xlarge", "c5a.xlarge", "c5ad.xlarge", "c5d.xlarge", "c6a.xlarge", "c6g.xlarge", "c6gd.xlarge",
        "c6gn.xlarge", "c6i.xlarge", "c6id.xlarge", "c6in.xlarge", "c7a.xlarge", "c7g.xlarge", "c7gd.xlarge",
        "c7gn.xlarge", "c7i-flex.xlarge", "c7i.xlarge", "c8a.xlarge", "c8g.xlarge", "c8gb.xlarge",
      "c8gd.xlarge", "c8gn.xlarge", "c8i-flex.xlarge", "c8i.xlarge", "c8id.xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-cslc_data_query_hist" = {
      "name" = "opera-job_worker-cslc_data_query_hist"
      "instance_type" = ["c5.2xlarge", "c5a.2xlarge", "c5ad.2xlarge", "c5d.2xlarge", "c6a.2xlarge", "c6g.2xlarge", "c6gd.2xlarge",
        "c6gn.2xlarge", "c6i.2xlarge", "c6id.2xlarge", "c6in.2xlarge", "c7a.2xlarge", "c7g.2xlarge", "c7gd.2xlarge",
        "c7gn.2xlarge", "c7i-flex.2xlarge", "c7i.2xlarge", "c8a.2xlarge", "c8g.2xlarge", "c8gb.2xlarge",
      "c8gd.2xlarge", "c8gn.2xlarge", "c8i-flex.2xlarge", "c8i.2xlarge", "c8id.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 5
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-gcov_query" = {
      "name" = "opera-job_worker-gcov_query"
      "instance_type" = ["c5.xlarge", "c5a.xlarge", "c5ad.xlarge", "c5d.xlarge", "c6a.xlarge", "c6g.xlarge", "c6gd.xlarge",
        "c6gn.xlarge", "c6i.xlarge", "c6id.xlarge", "c6in.xlarge", "c7a.xlarge", "c7g.xlarge", "c7gd.xlarge",
        "c7gn.xlarge", "c7i-flex.xlarge", "c7i.xlarge", "c8a.xlarge", "c8g.xlarge", "c8gb.xlarge",
      "c8gd.xlarge", "c8gn.xlarge", "c8i-flex.xlarge", "c8i.xlarge", "c8id.xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-gcov_catalog_ingest" = {
      "name"              = "opera-job_worker-gcov_catalog_ingest"
      "instance_type"     = ["c6i.xlarge", "c6a.xlarge", "c7i.xlarge", "c7a.xlarge", "c8i.xlarge", "c8a.xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = false
    }
    "opera-job_worker-gcov_download" = {
      "name"              = "opera-job_worker-gcov_download"
      "instance_type"     = ["m6a.large", "m5.large", "m5ad.large", "m6i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 50
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-cslc_data_download" = {
      "name"              = "opera-job_worker-cslc_data_download"
      "instance_type"     = ["m6a.large", "m5.large", "m5ad.large", "m6i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 50
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-cslc_data_download_hist" = {
      "name"              = "opera-job_worker-cslc_data_download_hist"
      "instance_type"     = ["m6a.large", "m5.large", "m5ad.large", "m6i.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 50
      "min_size"          = 0
      "max_size"          = 25
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-submit_pending_jobs" = {
      "name" = "opera-job_worker-submit_pending_jobs"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large",
        "c7gn.large", "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large",
      "c8gn.large", "c8i-flex.large", "c8i.large", "c8id.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = false
    }
    "opera-job_worker-rtc_data_download" = {
      "name" = "opera-job_worker-rtc_data_download"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large", "c7gn.large",
        "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large", "c8gn.large", "c8i-flex.large",
      "c8i.large", "c8id.large", "c5n.large", "m6in.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 100
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-disp_static_query" = {
      "name" = "opera-job_worker-disp_static_query"
      "instance_type" = ["c5.xlarge", "c5a.xlarge", "c5ad.xlarge", "c5d.xlarge", "c6a.xlarge", "c6g.xlarge", "c6gd.xlarge",
        "c6gn.xlarge", "c6i.xlarge", "c6id.xlarge", "c6in.xlarge", "c7a.xlarge", "c7g.xlarge", "c7gd.xlarge",
        "c7gn.xlarge", "c7i-flex.xlarge", "c7i.xlarge", "c8a.xlarge", "c8g.xlarge", "c8gb.xlarge",
      "c8gd.xlarge", "c8gn.xlarge", "c8i-flex.xlarge", "c8i.xlarge", "c8id.xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 1
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-rtc_for_dist_data_download" = {
      "name" = "opera-job_worker-rtc_for_dist_data_download"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large", "c7gn.large",
        "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large", "c8gn.large", "c8i-flex.large",
      "c8i.large", "c8id.large", "c5n.large", "m6in.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 50
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-rtc_for_dist_data_download_hist" = {
      "name" = "opera-job_worker-rtc_for_dist_data_download_hist"
      "instance_type" = ["c5.large", "c5a.large", "c5ad.large", "c5d.large", "c6a.large", "c6g.large", "c6gd.large",
        "c6gn.large", "c6i.large", "c6id.large", "c6in.large", "c7a.large", "c7g.large", "c7gd.large", "c7gn.large",
        "c7i-flex.large", "c7i.large", "c8a.large", "c8g.large", "c8gb.large", "c8gd.large", "c8gn.large", "c8i-flex.large",
      "c8i.large", "c8id.large", "c5n.large", "m6in.large"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 25
      "min_size"          = 0
      "max_size"          = 50
      "total_jobs_metric" = true
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-ecmwf-merger" = {
      "name"              = "opera-job_worker-ecmwf-merger"
      "instance_type"     = ["r5a.4xlarge", "r6a.4xlarge", "r5.4xlarge", "r6i.4xlarge", "r7i.4xlarge", "r7a.4xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 600
      "max_size"          = 10
      "total_jobs_metric" = true
      "use_on_demand"     = true
    }
    "opera-job_worker-pge_smoke_test_amd" = {
      "name"              = "opera-job_worker-pge_smoke_test_amd"
      "instance_type"     = ["r6a.4xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 900
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
    }
    "opera-job_worker-pge_smoke_test_intel" = {
      "name"              = "opera-job_worker-pge_smoke_test_intel"
      "instance_type"     = ["r6i.2xlarge"]
      "user_data"         = "launch_template_user_data.sh.tmpl"
      "root_dev_size"     = 50
      "data_dev_size"     = 900
      "min_size"          = 0
      "max_size"          = 10
      "total_jobs_metric" = false
      "use_private_vpc"   = false
      "use_on_demand"     = true
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
}

variable "lambda_log_retention_in_days" {
  type    = number
  default = 30
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.4"
    "cslc_s1"  = "2.1.3"
    "rtc_s1"   = "2.1.4"
    "dswx_s1"  = "3.0.4"
    "disp_s1"  = "3.0.10"
    "dswx_ni"  = "4.0.0-rc.2.0"
    "dist_s1"  = "6.0.2"
    "tropo"    = "3.0.0-rc.1.0-tropo"
    "disp_ni"  = "6.0.0-er.2.0"
    "cal_disp" = "7.0.0-er.1.0"
  }
}

variable "docker_registry_bucket" {
  default = "opera-pcm-registry-bucket"
}

variable "pge_snapshots_date" {
  default = "20250729-6.0.0-er.1.0"
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

variable "slcs1c_query_timer_trigger_frequency" {
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

variable "rtc_for_dist_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "cslc_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "gcov_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "gcov_catalog_ingest_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "dswx_ni_expiry_eval_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "batch_query_timer_trigger_frequency" {
  default = "rate(1 minute)"
}

variable "opensearch_shards_monitor_trigger_frequency" {
  default = "cron(0 * * * ? *)"
}

variable "obs_acct_report_timer_trigger_frequency" {}

variable "cluster_type" {}

variable "valid_cluster_type_values" {
  type    = list(string)
  default = ["forward", "reprocessing"]
}

variable "rs_fwd_bucket_expiration_default" {
  type    = number
  default = 14

  validation {
    condition     = var.rs_fwd_bucket_expiration_default > 0
    error_message = "rs_fwd_bucket_expiration_default must be >= 1"
  }
}

variable "rs_fwd_bucket_expiration_base_rules" {
  type = map(object({
    enabled = bool
    days    = number
  }))
  default = {
    inputs : {
      enabled : true,
      days : 14
    },
    tmp : {
      enabled : true,
      days : 14
    }
  }

  validation {
    condition     = sort(keys(var.rs_fwd_bucket_expiration_base_rules)) == sort(["inputs", "tmp"])
    error_message = "rs_fwd_bucket_expiration_base_rules must contain inputs and tmp keys"
  }

  validation {
    condition     = length([for v in values(var.rs_fwd_bucket_expiration_base_rules) : v if v.days < 1]) == 0
    error_message = "days must be >= 1"
  }
}

# To get the latest set of keys for this products map, you can run
#
# grep 'products/' < <datasets.json path> | grep 'DATASET_BUCKET' | sed 's/.*\/products\///' | cut -d '/' -f 1 | uniq
variable "rs_fwd_bucket_expiration_product_rules" {
  type = map(object({
    enabled = bool
    days    = number
  }))
  default = {}

  validation {
    condition     = length([for v in values(var.rs_fwd_bucket_expiration_product_rules) : v if v.days < 1]) == 0
    error_message = "days must be >= 1"
  }
}

variable "rs_fwd_bucket_expiration_product_rule_type" {
  type    = string
  default = "specific"

  validation {
    condition     = contains(["basic", "specific"], var.rs_fwd_bucket_expiration_product_rule_type)
    error_message = "rs_fwd_bucket_expiration_product_rule_type must be either basic or specific"
  }
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

variable "earthdata_uat_user" {
  default = ""
}

variable "earthdata_uat_pass" {
  default = ""
}

# TODO: It doesn't look like this is used anywhere. Can we remove it?
variable "clear_s3_aws_es" {
  type    = bool
  default = true
}

variable "asf_cnm_s_id_dev" {
}

variable "asf_cnm_s_id_dev_int" {
}

variable "asf_cnm_s_id_test" {
}

variable "asf_cnm_s_id_prod" {
}

variable "ami_versions" {
  type    = map(string)
  default = {}
}

variable "default_ami_versions" {
  type = map(string)
  default = {
    mozart    = "v6.0"
    metrics   = "v6.0"
    grq       = "v6.0"
    factotum  = "v6.0"
    autoscale = "v5.4.3"
  }
}

variable "use_cluster_verdi_ssm" {
  type    = bool
  default = false
}

variable "es_cluster_mode" {
  type    = bool
  default = true
}

variable "disp_s1_hist_status" {
  type    = bool
  default = false
}

variable "duplicates_cronjob_enable" {
  type    = bool
  default = false
}

variable "cnm_accountability_reporting" {
  type = object({
    enabled     = bool,
    sender      = string,
    recipients  = list(string),
    cc          = optional(list(string), []),
    bcc         = optional(list(string), []),
    days_back   = optional(number, 1)
    window_size = optional(number, 1)
    schedule    = optional(string, "0 0 * * *")
  })

  default = null

  validation {
    condition = var.cnm_accountability_reporting != null ? !var.cnm_accountability_reporting.enabled || (
            length(var.cnm_accountability_reporting.recipients) > 0 &&
            var.cnm_accountability_reporting.days_back >= 0 && var.cnm_accountability_reporting.window_size >= 1
    ) : true
    error_message = "If enabled, there must be at least one recipient, days_back must be >= 0, and window_size must be >= 1"
  }
}

variable "operator_alarm_email" {
  type        = string
  description = "Email to subscribe to CloudWatch alarms"
  default     = null
}


variable "max_shards_per_node" {
  type    = number
  default = 4500
}
