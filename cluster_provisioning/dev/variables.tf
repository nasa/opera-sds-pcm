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
  default = "v5.4.3"
}

variable "pcm_repo" {
  default = "github.com/nasa/opera-sds-pcm.git"
}

variable "pcm_branch" {
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
  default = "github.com/nasa/opera-sds-bach-api.git"
}

variable "bach_api_branch" {
  default = "develop"
}

variable "bach_ui_repo" {
  default = "github.com/nasa/opera-sds-bach-ui.git"
}

variable "bach_ui_branch" {
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

variable "artifactory_fn_api_key" {
}

variable "ops_password" {
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

variable "grq_aws_es" {
  default = false
}

variable "grq_aws_es_host" {
}

variable "grq_aws_es_host_private_verdi" {
}

variable "grq_aws_es_port" {
  default = 443
}

variable "use_grq_aws_es_private_verdi" {
  default = true
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
    instance_type = "r6i.2xlarge"
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
    instance_type = "r6i.xlarge"
    root_dev_size = 50
    private_ip    = ""
    public_ip     = ""
  }
}

# grq vars
variable "grq" {
  type = map(string)
  default = {
    name          = "grq"
    instance_type = "r6i.xlarge"
    root_dev_size = 50
    private_ip    = ""
    public_ip     = ""
  }
}

# factotum vars
variable "factotum" {
  type = map(string)
  default = {
    name          = "factotum"
    instance_type = "r6i.xlarge"
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
    private_ip = "opera-pcm-ci.jpl.nasa.gov"
    public_ip  = "opera-pcm-ci.jpl.nasa.gov"
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
}

variable "lambda_role_arn" {
}

# CNM Response job vars

variable "cnm_r_handler_job_type" {
  default = "process_cnm_response"
}

variable "cnm_r_job_queue" {
  default = "opera-job_worker-rcv_cnm_notify"
}

variable "po_daac_cnm_r_event_trigger" {
  default = "sns"
}

variable "asf_daac_cnm_r_event_trigger" {
  default = "sqs"
}

variable "cnm_r_allowed_account" {
  default = "*"
}

variable "cnm_r_venue" {
  default = "dev"
}

variable "trace" {
  type    = string
  default = "opera-dev"
}

# need to get SNS arn from PO DAAC and define
variable "po_daac_delivery_proxy" {
}

variable "use_daac_cnm_r" {
  default = false
}

variable "po_daac_endpoint_url" {
  default = ""
}

# need to get SNS arn from ASF DAAC and define
variable "asf_daac_delivery_proxy" {
}

variable "asf_daac_endpoint_url" {
  default = ""
}

# asg vars
variable "asg_use_role" {
  default = "true"
}

variable "asg_role" {
  default = "am-pcm-dev-verdi-role"
}

variable "public_asg_vpc" {
}

variable "private_asg_vpc" {
}

variable "aws_account_id" {
}

variable "ssm_account_id" {
}

variable "lambda_package_release" {
  default = "develop"
}

variable "job_catalog_url" {
  default = ""
}

variable "delete_old_job_catalog" {
  type    = bool
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
  default = "20250729-6.0.0-er.1.0"
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.3"
    "cslc_s1"  = "2.1.3"
    "rtc_s1"   = "2.1.3"
    "dswx_s1"  = "3.0.2"
    "disp_s1"  = "3.0.7"
    "dswx_ni"  = "4.0.0-er.4.0"
    "dist_s1"  = "6.0.0-rc.1.0"
    "tropo"    = "3.0.0-er.3.1-tropo"
  }
}

variable "pge_sim_mode" {
  type    = bool
  default = true
}

variable "crid" {
  default = "D00100"
}

variable "cluster_type" {
  default = "reprocessing"
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

variable "run_smoke_test" {
  type    = bool
  default = true
}

variable "purge_es_snapshot" {
  default = true
}

variable "es_snapshot_bucket" {
  default = "opera-dev-es-bucket"
}

variable "es_bucket_role_arn" {
}

variable "artifactory_fn_user" {
  default = ""
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

variable "disp_s1_hist_status" {
  type    = bool
  default = false
}

variable "cnm_r_sqs_arn" {
}

variable "asf_cnm_s_id_dev" {
}

variable "asf_cnm_s_id_dev_int" {
}

variable "asf_cnm_s_id_test" {
}

variable "asf_cnm_s_id_prod" {
}

