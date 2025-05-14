# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "venue" {
  default = "ci"
}

variable "environment" {
  default = "dev"
}

variable "use_artifactory" {
  default = true
}

variable "counter" {
  default = "6"
}

variable "crid" {
  default = "D00100"
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

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  #default = "reprocessing"
  default = "forward"
}

###### Security  ########
variable "public_verdi_security_group_id" {
}

variable "private_verdi_security_group_id" {
}

variable "cluster_security_group_id" {
}

variable "private_key_file" {
  default = ""
}

variable "keypair_name" {
  #default = "operasds-int-cluster-1"
  default = ""
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

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-dev-cc-fwd-ci"
}

variable "dataset_bucket" {
  default = "opera-dev-rs-fwd-ci"
}

variable "code_bucket" {
  default = "opera-dev-cc-fwd-ci"
}

variable "lts_bucket" {
  default = "opera-dev-lts-fwd-ci"
}

variable "triage_bucket" {
  default = "opera-dev-triage-fwd-ci"
}

variable "isl_bucket" {
  default = "opera-dev-isl-fwd-ci"
}

variable "osl_bucket" {
  default = "opera-dev-osl-fwd-ci"
}

variable "es_snapshot_bucket" {
  default = "opera-dev-cc-fwd-ci"
}

variable "artifactory_repo" {
  default = "general-develop"
}

####### CNM Response job vars #######
variable "use_daac_cnm_r" {
  default = false
}

variable "po_daac_endpoint_url" {
  default = ""
}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20250514-3.1.6"
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.3"
    "cslc_s1"  = "2.1.1"
    "rtc_s1"   = "2.1.1"
    "dswx_s1"  = "3.0.2"
    "disp_s1"  = "3.0.6"
    "dswx_ni"  = "4.0.0-er.3.0"
    "dist_s1"  = "6.0.0-er.1.0"
    "tropo"    = "3.0.0-er.1.0-tropo"
  }
}

variable "hysds_release" {
  default = "v5.0.1"
}

variable "lambda_package_release" {
  default = "3.1.6"
}

variable "pcm_commons_branch" {
  default = "3.1.6"
}

variable "pcm_branch" {
  default = "3.1.6"
}

variable "product_delivery_branch" {
  default = "3.1.6"
}

variable "bach_api_branch" {
  default = "3.1.6"
}

variable "bach_ui_branch" {
  default = "3.1.6"
}

###### Roles ########
variable "asg_use_role" {
  default = "true"
}

variable "asg_role" {
  default = "am-pcm-dev-verdi-role"
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

##### ES ######
variable "grq_aws_es_port" {
  default = 443
}

# mozart vars
variable "mozart" {
  type = map(string)
  default = {
    name          = "mozart"
    instance_type = "r6i.4xlarge"
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
    instance_type = "r5.4xlarge"
    root_dev_size = 200
    private_ip    = ""
    public_ip     = ""
  }
}

# grq vars
variable "grq" {
  type = map(string)
  default = {
    name          = "grq"
    instance_type = "r5.4xlarge"
    root_dev_size = 200
    private_ip    = ""
    public_ip     = ""
  }
}

# factotum vars
variable "factotum" {
  type = map(string)
  default = {
    name          = "factotum"
    instance_type = "r6i.8xlarge"
    root_dev_size = 500
    data          = "/data"
    data_dev      = "/dev/xvdb"
    data_dev_size = 300
    private_ip    = ""
    publicc_ip    = ""
  }
}

# ci vars
variable "ci" {
  type = map(string)
  default = {
    name          = "ci"
    ami           = ""
    instance_type = ""
    data          = ""
    data_dev      = ""
    data_dev_size = ""
    private_ip    = ""
    public_ip     = ""
  }
}

variable "common_ci" {
  type = map(string)
  default = {
    name       = "ci"
    private_ip = ""
    public_ip  = ""
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
  }
}
