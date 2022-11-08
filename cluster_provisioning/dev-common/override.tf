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
  default = "dev"
}

variable "environment" {
  default = "dev"
}

variable "counter" {
  default = "fwd"
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
  default = "reprocessing"
}

###### Security  ########
variable "public_verdi_security_group_id" {
  # fwd security group
  default = "sg-0471b65df42e14c41"
}

variable "private_verdi_security_group_id" {
  # fwd security group
  default = "sg-00486f537748ee592"
}

variable "cluster_security_group_id" {
  # fwd security group
  default = "sg-0a9f461fb764bf2ec"
}

variable "private_key_file" {
  default = "~/.ssh/opera-dev-fwd.pem"
}

variable "keypair_name" {
  default = "opera-dev-fwd"
}

variable "ops_password" {
  default = ""
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
variable "dataset_bucket" {
  default = "opera-dev-rs-fwd"
}

variable "code_bucket" {
  default = "opera-dev-cc-fwd"
}

variable "lts_bucket" {
  default = "opera-dev-lts-fwd"
}

variable "triage_bucket" {
  default = "opera-dev-triage-fwd"
}

variable "isl_bucket" {
  default = "opera-dev-isl-fwd"
}

variable "osl_bucket" {
  default = "opera-dev-osl-fwd"
}

variable "es_snapshot_bucket" {
  default = "opera-dev-es-bucket"
}

variable "artifactory_repo" {
  #default = "general-stage"
  default = "general-develop"
}

variable "lambda_package_release" {
  default = "2.0.0-er.1.0"
}

variable "pcm_commons_branch" {
  default = "2.0.0-er.1.0"
}

variable "pcm_branch" {
  default = "2.0.0-er.1.0"
}

variable "product_delivery_branch" {
  default = "2.0.0-er.1.0"
}

variable "bach_api_branch" {
  default = "2.0.0-er.1.0"
}

variable "bach_ui_branch" {
  default = "2.0.0-er.1.0"
}

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {

    # HySDS v4.0.1-beta.8-oraclelinux - Universal AMIs (from Susan 10-24-22)
    mozart    = "ami-0a9ee96d965b006f3" # mozart v4.18
    metrics   = "ami-025931942c5155f5c" # metrics v4.13
    grq       = "ami-08c8b2877d50e06c4" # grq v4.14
    factotum  = "ami-0ecd7108ca35acd1c" # factotum v4.14
    autoscale = "ami-022a3064aa856ace1" # verdi v4.14
    ci        = "ami-022a3064aa856ace1" # verdi v4.14
  }
}

# mozart vars
variable "mozart" {
  type = map(string)
  default = {
    name          = "mozart"
    instance_type = "m5.8xlarge"
    root_dev_size = 200
    private_ip    = "100.104.40.20"
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
    private_ip    = "100.104.40.73"
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
    private_ip    = "100.104.40.204"
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
    private_ip    = "100.104.40.173"
    publicc_ip    = ""
  }
}


