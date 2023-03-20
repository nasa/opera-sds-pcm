# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "aws_account_id" {
  default = "681612454726"
}

variable "venue" {
  default = "ci"
}

variable "environment" {
  default = "dev"
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

####### Subnet ###########
variable "subnet_id" {
  default = "subnet-000eb551ad06392c7"
}

####### VPC #########
variable "lambda_vpc" {
  default = "vpc-02676637ea26098a7"
}

variable "public_asg_vpc" {
  default = "vpc-02676637ea26098a7"
}

variable "private_asg_vpc" {
  default = "vpc-b5a983cd"
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

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
    # HySDS v4.1.0-beta.4 with ES 7.10 - Mar 9, 2023
    mozart    = "ami-04f791795ef444889" # mozart v4.19 - 230215
    metrics   = "ami-09abf2a67634a7ab2" # metrics v4.14 - 230223
    grq       = "ami-0586f7f14177b302c" # grq v4.15 - 230223
    factotum  = "ami-0966e4927b40b5036" # factotum v4.15 - 230308
    autoscale = "ami-0805014721e52d629" # verdi v4.16 patchupdate - 230215
  }
}

####### CNM Response job vars #######
variable "po_daac_delivery_proxy" {
  #default = "arn:aws:sns:us-west-2:638310961674:podaac-uat-cumulus-provider-input-sns"
  default = "arn:aws:sns:us-west-2:681612454726:daac-proxy-for-opera"
}

variable "use_daac_cnm_r" {
  default = false
}

variable "po_daac_endpoint_url" {
  default = ""
}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20220826-RC4.0"
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.0"
    "cslc_s1" = "2.0.0-er.5.0"
    "rtc_s1" = "2.0.0-er.5.1"
  }
}

variable "hysds_release" {
  default = "v4.1.0-beta.4"
}

variable "lambda_package_release" {
  default = "2.0.0-rc.4.0"
}

variable "pcm_commons_branch" {
  default = "2.0.0-rc.4.0"
}

variable "pcm_branch" {
  default = "2.0.0-rc.4.0"
}

variable "product_delivery_branch" {
  default = "2.0.0-rc.4.0"
}

variable "bach_api_branch" {
  default = "2.0.0-rc.4.0"
}

variable "bach_ui_branch" {
  default = "2.0.0-rc.4.0"
}

###### Roles ########
variable "asg_use_role" {
  default = "true"
}

variable "asg_role" {
  default = "am-pcm-dev-verdi-role"
  #default = "am-pcm-verdi-role"
}

variable "pcm_cluster_role" {
  default = {
    name = "am-pcm-dev-cluster-role"
    #name = "am-pcm-cluster-role"
    path = "/"
  }
}

variable "pcm_verdi_role" {
  default = {
    name = "am-pcm-dev-verdi-role"
    #name = "am-pcm-verdi-role"
    path = "/"
  }
}

variable "lambda_role_arn" {
  default = "arn:aws:iam::681612454726:role/am-pcm-dev-lambda-role"
  #default = "arn:aws:iam::681612454726:role/am-pcm-lambda-role"
}

##### ES ######
variable "es_bucket_role_arn" {
  default = "arn:aws:iam::681612454726:role/am-es-role"
}

variable "grq_aws_es_host" {
  default = "vpce-0d33a52fc8fed6e40-ndiwktos.vpce-svc-09fc53c04147498c5.us-west-2.vpce.amazonaws.com"
}

variable "grq_aws_es_port" {
  default = 443
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
    instance_type = "r5.8xlarge"
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
