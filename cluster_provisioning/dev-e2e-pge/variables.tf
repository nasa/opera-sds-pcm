# globals
#
# venue : userId 
# counter : 1-n
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#
variable "artifactory_base_url" {
  default = "https://cae-artifactory.jpl.nasa.gov/artifactory"
}

variable "artifactory_repo" {
  default = "general-develop"
}

variable "artifactory_mirror_url" {
  default = "s3://opera-dev/artifactory_mirror"
}

variable "hysds_release" {
}

variable "opera_pcm_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/opera-pcm.git"
}

variable "opera_pcm_branch" {
  default = "develop"
}

variable "pcm_commons_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git"
}

variable "pcm_commons_branch" {
  default = "develop"
}

variable "product_delivery_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git"
}

variable "product_delivery_branch" {
  default = "develop"
}

variable "bach_api_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/bach-api.git"
}

variable "bach_api_branch" {
  default = "opera"
}

variable "bach_ui_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/bach-ui.git"
}

variable "bach_ui_branch" {
  default = "opera"
}

variable "opera_bach_api_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/opera-bach-api.git"
}

variable "opera_bach_api_branch" {
  default = "develop"
}

variable "opera_bach_ui_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/opera-bach-ui.git"
}

variable "opera_bach_ui_branch" {
  default = "develop"
}

variable "venue" {
}

variable "counter" {
  default = ""
}

variable "private_key_file" {
}

variable "git_auth_key" {
}

variable "jenkins_api_user" {
  default = ""
}

variable "keypair_name" {
  default = ""
}

variable "jenkins_api_key" {
}

variable "ops_password" {
  default = "hysdsops"
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
      "data_dev_size" = 50
      "max_size"      = 10
    }
    "opera-job_worker-large" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-rrst-acct" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-l0a" = {
      # TODO: restore multiple instance types when L0A PGE deterministically generates the same number of products
      #"instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "instance_type" = ["t3.xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-time_extractor" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-datatake-acct" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-track-frame-acct" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-l0b" = {
      "instance_type" = ["m5.24xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 900
      "max_size"      = 10
    }
    "opera-job_worker-network-pair-eval" = {
      "instance_type" = ["t2.medium", "t3a.medium", "t3.medium"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-rslc" = {
      # TODO: p2.xlarge does not produce same quality of product for R2 RSLC PGE
      #"instance_type" = ["p2.xlarge", "p3.2xlarge"]
      "instance_type" = ["p3.2xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-gslc" = {
      "instance_type" = ["r5.4xlarge", "r5b.4xlarge", "r5n.4xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 100
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-gcov" = {
      "instance_type" = ["r5.4xlarge", "r5b.4xlarge", "r5n.4xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 100
      "max_size"      = 10
    }
    "opera-job_worker-sciflo-insar" = {
      "instance_type" = ["p3.2xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 100
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
      "data_dev_size" = 25
      "max_size" = 10
    }

    "opera-job_worker-pta" = {
      "instance_type" = ["c5.4xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 25
      "max_size"      = 10
    }

    "opera-job_worker-net" = {
      "instance_type" = ["c5.4xlarge"]
      "root_dev_size" = 50
      "data_dev_size" = 50
      "max_size"      = 10
    }
  }
}
variable "shared_credentials_file" {
  default = "~/.aws/credentials"
}

#
# "default" links to [default] profile in "shared_credentials_file" above
#
variable "profile" {
  default = "saml-pub"
}

variable "project" {
  default = "opera"
}

variable "region" {
  default = "us-west-2"
}

variable "az" {
  default = "us-west-2a"
}

variable grq_aws_es {
  default = false
}

variable grq_aws_es_host {
  default = "vpce-0d33a52fc8fed6e40-ndiwktos.vpce-svc-09fc53c04147498c5.us-west-2.vpce.amazonaws.com"
}

variable "grq_aws_es_host_private_verdi" {
  default = "vpce-07498e8171c201602-l2wfjtow.vpce-svc-09fc53c04147498c5.us-west-2.vpce.amazonaws.com"
}

variable grq_aws_es_port {
  default = 443
}

variable "use_grq_aws_es_private_verdi" {
  default = true
}

variable "subnet_id" {
  default = "subnet-8ecc5dd3"
}

variable "verdi_security_group_id" {
}

variable "cluster_security_group_id" {
}

variable "pcm_cluster_role" {
  default = {
    name = "am-pcm-dev-cluster-role"
    path = "/"
  }
}

variable "pcm_verdi_role" {
  default = {
    name = "am-pcm-dev-verdi-role"
    path = "/"
  }
}

# mozart vars
variable "mozart" {
  type = map(string)
  default = {
    name          = "mozart"
    instance_type = "r5.4xlarge"
    root_dev_size = 200
    private_ip    = ""
    public_ip     = ""
  }
}

# metrics vars
variable "metrics" {
  type = map(string)
  default = {
    name          = "metrics"
    instance_type = "r5.xlarge"
    private_ip    = ""
    public_ip     = ""
  }
}

# grq vars
variable "grq" {
  type = map(string)
  default = {
    name          = "grq"
    instance_type = "r5.xlarge"
    private_ip    = ""
    public_ip     = ""
  }
}

# factotum vars
variable "factotum" {
  type = map(string)
  default = {
    name          = "factotum"
    instance_type = "c5.xlarge"
    root_dev_size = 50
    data          = "/data"
    data_dev      = "/dev/xvdb"
    data_dev_size = 300
    private_ip    = ""
    public_ip     = ""
  }
}

# ci vars
variable "ci" {
  type = map(string)
  default = {
    name          = "ci"
    instance_type = "c5.xlarge"
    data          = "/data"
    data_dev      = "/dev/xvdb"
    data_dev_size = 100
    private_ip    = ""
    public_ip     = ""
  }
}

variable "common_ci" {
  type = map(string)
  default = {
    name       = "ci"
    private_ip = "100.104.40.248"
    public_ip  = "100.104.40.248"
  }
}

# autoscale vars
variable "autoscale" {
  type = map(string)
  default = {
    name          = "autoscale"
    instance_type = "t2.micro"
    data          = "/data"
    data_dev      = "/dev/xvdb"
    data_dev_size = 300
    private_ip    = ""
    public_ip     = ""
  }
}

# staging area vars

variable "lambda_vpc" {
  default = "vpc-b5a983cd"
}

variable "lambda_role_arn" {
  default = "arn:aws:iam::681612454726:role/am-pcm-dev-lambda-role"
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
  default = "opera-job_worker-small"
}

variable "cnm_r_event_trigger" {
  default = "sqs"
}

variable "cnm_r_allowed_account" {
  default = "*"
}

#The value of daac_delivery_proxy can be
#  arn:aws:sqs:us-west-2:782376038308:daac-proxy-for-opera
#  arn:aws:sqs:us-east-1:206609214770:asf-cumulus-dev-opera-workflow-queue
variable "daac_delivery_proxy" {
  default = "arn:aws:sqs:us-west-2:782376038308:daac-proxy-for-opera"
}

variable "use_daac_cnm" {
  default = false
}

variable "daac_endpoint_url" {
  default = ""
}

# asg vars
variable "asg_use_role" {
  default = "true"
}

variable "asg_role" {
  default = "am-pcm-dev-verdi-role"
}

variable "asg_vpc" {
  default = "vpc-b5a983cd"
}

variable "aws_account_id" {
  default = "681612454726"
}

variable "lambda_package_release" {
  default = "develop"
}

variable "cop_catalog_url" {
  default = ""
}

variable "delete_old_cop_catalog" {
  default = false
}

variable "tiurdrop_catalog_url" {
  default = ""
}

variable "delete_old_tiurdrop_catalog" {
  default = false
}

variable "rost_catalog_url" {
  default = ""
}

variable "delete_old_rost_catalog" {
  default = false
}

variable "pass_catalog_url" {
  default = ""
}

variable "delete_old_pass_catalog" {
  default = false
}

variable "delete_old_observation_catalog" {
  default = false
}

variable "delete_old_track_frame_catalog" {
  default = false
}

variable "delete_old_radar_mode_catalog" {
  default = false
}

variable "environment" {
  default = "dev"
}

variable "use_artifactory" {
  default = false
}

variable "event_misfire_trigger_frequency" {
  default = "rate(5 minutes)"
}

variable "event_misfire_delay_threshold_seconds" {
  type    = number
  default = 60
}

variable "lambda_log_retention_in_days" {
  type    = number
  default = 30
}

variable "pge_snapshots_date" {
  default = "20210805-R2.0.0"
}

variable "opera_pge_release" {
  default = "R2.0.0"
}

variable "crid" {
  default = "D00200"
}

variable "cluster_type" {
  default = "reprocessing"
}

variable "l0a_timer_trigger_frequency" {
  default = "rate(15 minutes)"
}

variable "l0b_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "rslc_timer_trigger_frequency" {
  default = "rate(360 minutes)"
}

variable "network_pair_timer_trigger_frequency" {
  default = "rate(360 minutes)"
}

variable "l0b_urgent_response_timer_trigger_frequency" {
  default = "rate(15 minutes)"
}

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

variable "use_s3_uri_structure" {
  default = true
}

variable "inactivity_threshold" {
  type    = number
  default = 600
}

variable "pge_test_package" {
  default = "testdata_R2.0.0"
}

variable "l0a_test_package" {
  default = "l0a_multi_003.tgz"
}

variable "l0b_test_package" {
  default = "l0b_lsar_only_001.tgz"
}

variable "rslc_test_package" {
  default = "rslc_ALPSRP037370690_002.tgz"
}

variable "l2_test_package" {
  default = "end2end_ALPSRP_Rosamond_001.tgz"
}
