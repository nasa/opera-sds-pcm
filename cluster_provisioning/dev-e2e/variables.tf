#m globals
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
  default = "vpce-0d33a52fc8fed6e40-ndiwktos.vpce-svc-09fc53c04147498c5.us-west-2.vpce.amazonaws.com"
}

variable "grq_aws_es_host_private_verdi" {
  default = "vpce-07498e8171c201602-l2wfjtow.vpce-svc-09fc53c04147498c5.us-west-2.vpce.amazonaws.com"
}

variable "grq_aws_es_port" {
  default = 443
}

variable "use_grq_aws_es_private_verdi" {
  default = true
}

variable "subnet_id" {
  default = "subnet-000eb551ad06392c7"
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
    root_dev_size = 100
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
    instance_type = "r5.xlarge"
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
    instance_type = "r6i.4xlarge"
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
  default = "vpc-02676637ea26098a7"
}

variable "lambda_role_arn" {
  default = "arn:aws:iam::681612454726:role/am-pcm-dev-lambda-role"
}

variable "lambda_job_type" {
  default = "INGEST_STAGED"
}

variable "lambda_job_queue" {
  default = "opera-job_worker-hls_data_ingest"
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

#The value of po_daac_delivery_proxy can be
#  arn:aws:sqs:us-west-2:871271927522:asf-w2-cumulus-dev-opera-workflow-queue
variable "po_daac_delivery_proxy" {
  default = "arn:aws:sns:us-west-2:681612454726:daac-proxy-for-opera"
}

variable "use_daac_cnm_r" {
  default = false
}

variable "po_daac_endpoint_url" {
  default = ""
}

#The value of asf_daac_delivery_proxy can be
# for DEV: arn:aws:sqs:us-west-2:871271927522:asf-cumulus-dev-opera-cnm-ingest-queue
# for dev-int:
variable "asf_daac_delivery_proxy" {
  default = "arn:aws:sqs:us-west-2:681612454726:daac-proxy-for-opera"
  #default = "arn:aws:sqs:us-west-2:156214815904:asf-cumulus-int-opera-cnm-ingest-queue"
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
  default = "vpc-02676637ea26098a7"
}

variable "private_asg_vpc" {
  default = "vpc-b5a983cd"
}

variable "aws_account_id" {
  default = "681612454726"
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
  default = "20220401-1.0.0-er.3.0"
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.2"
    "cslc_s1" = "2.0.0"
    "rtc_s1" = "2.0.0"
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

variable "hls_download_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "hlsl30_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "hlss30_query_timer_trigger_frequency" {
  default = "rate(60 minutes)"
}

variable "obs_acct_report_timer_trigger_frequency" {
  default = "cron(0 0 * * ? *)"
}

variable "batch_query_timer_trigger_frequency" {
  default = "rate(1 minute)"
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
  default = 1800
}

variable "run_smoke_test" {
  type    = bool
  default = true
}

variable "queues" {
  default = ""
}

variable "docker_registry_bucket" {
  default = "opera-pcm-registry-bucket"
}

variable "purge_es_snapshot" {
  default = true
}

variable "es_snapshot_bucket" {
  default = "opera-dev-es-bucket"
}

variable "es_bucket_role_arn" {
  default = "arn:aws:iam::681612454726:role/am-es-role"
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

# ami vars
variable "amis" {
  type = map(string)
  default = {
	# HySDS v5.0.0-beta.6 - Aug 3, 2023 - R2
    mozart    = "ami-0c70da559d13163d8" # mozart v4.21 - 230802
    metrics   = "ami-02b0f6841c5d818b6" # metrics v4.15 - 230802
    grq       = "ami-08622ebc2ac35bfe6" # grq v4.16 - 230802
    factotum  = "ami-0b7c82edf1d6ef969" # factotum v4.16 - 230802
    autoscale = "ami-07abb668e56ddc95e" # verdi v4.16 patchdate - 20230702
  }
}

variable "es_user" {}

variable "es_pass" {}

variable "clear_s3_aws_es" {
  type = bool
  default = true
}
