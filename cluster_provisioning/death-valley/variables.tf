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

variable "baseline_pge_branch" {
}

variable "hysds_release" {
}

variable "pcm_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/opera-pcm.git"
}

variable "pcm_repo_git" {
  default = "git@github.jpl.nasa.gov:IEMS-SDS/opera-pcm.git"
}

variable "product_delivery_repo" {
  default = "github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git"
}

variable "product_delivery_branch" {
  default = "develop"
}

variable "pcm_branch" {
  default = "develop"
}

variable "venue" {
  default = "dv"
}

variable "counter" {
  default = ""
}

variable "private_key_file" {
  default = ""
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
    ami           = "ami-084d0134c0ffaf89a"
    instance_type = "r5.2xlarge"
    root_dev_size = 50
    private_ip    = ""
    public_ip     = ""
  }
}

# metrics vars
variable "metrics" {
  type = map(string)
  default = {
    name          = "metrics"
    ami           = "ami-084e463eea4525426"
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
    ami           = "ami-0d417980127d802e5"
    instance_type = "r5.2xlarge"
    private_ip    = ""
    public_ip     = ""
  }
}

# factotum vars
variable "factotum" {
  type = map(string)
  default = {
    name          = "factotum"
    ami           = "ami-041f69e29c640b605"
    instance_type = "m5.8xlarge"
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
    ami           = "ami-044a23d149494c265"
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
    ami           = "ami-0b03255d9d1e3f69f"
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
  default = "opera-job_worker-rcv_cnm_notify"
}

variable "cnm_r_event_trigger" {
  default = "sns"
}

variable "cnm_r_allowed_account" {
  default = "*"
}

variable "daac_delivery_proxy" {
  default = "arn:aws:sqs:us-east-1:206609214770:asf-cumulus-dev-opera-workflow-queue"
}

variable "use_daac_cnm" {
  default = "True"
}

variable "daac_endpoint_url" {
  default = ""
}

# asg vars
variable "asg_ami" {
  default = "ami-0b03255d9d1e3f69f"
}

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

variable "lambda_cnm_r_handler_package_name" {
  default = "lambda-cnm-r-handler"
}

variable "lambda_harikiri_handler_package_name" {
  default = "lambda-harikiri-handler"
}

variable "lambda_isl_handler_package_name" {
  default = "lambda-isl-handler"
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

variable "environment" {
  default = "dev"
}

variable "use_artifactory" {
  default = false
}

variable "lambda_e-misfire_handler_package_name" {
  default = "lambda-event-misfire-handler"
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

