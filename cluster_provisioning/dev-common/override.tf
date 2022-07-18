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

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
    # HySDS v4.0.1-beta.8-oraclelinux - Universal AMIs (June 10, 2022)
#    grq       = "ami-0a4ab3a778c395194" # OL8 All-project grq v4.13 - 220610
#    metrics   = "ami-0d5c253305b866dc0" # metrics v4.12 - 220610A
#    mozart    = "ami-00f898f3f2f930aa4" # mozart v4.17 - 220610
#    factotum  = "ami-0d0e97c6690f612d7" # OL8 All-project factotum v4.13 - 220609
#    autoscale = "ami-0d5a7f80daf236d93" # verdi v4.12 patchdate - 220609
#    ci        = "ami-0d5a7f80daf236d93" # verdi v4.12 patchdate - 220609

    # AMI given by Susan on June 24, 2022
    mozart    = "ami-07e0e84f9469ab0db" # mozart v4.17 
    metrics   = "ami-0846bd13fe529f806" # metrics v4.12
    grq       = "ami-0b3852a0f65efed65" # grq v4.13
    factotum  = "ami-00be11af7135dc5c3" # factotum v4.13
    autoscale = "ami-0d5a7f80daf236d93" # verdi v4.12 patchdate - 220609
    ci        = "ami-0d5a7f80daf236d93" # verdi v4.12 patchdate - 220609
  }
}

# mozart vars
variable "mozart" {
  type = map(string)
  default = {
    name          = "mozart"
    instance_type = "r5.4xlarge"
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


