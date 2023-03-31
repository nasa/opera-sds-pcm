# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "aws_account_id" {
  default = "337765570207"
}

variable "venue" {
  default = "int"
}

variable "environment" {
  default = "int"
}

variable "crid" {
  default = "T00100"
}

variable "project" {
  default = "opera"
}

variable "region" {
  default = "us-west-2"
}

variable "az" {
  default = "us-west-2b"
}

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  default = "forward"
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
  default = "subnet-0644c172bef1d690e"
}

####### VPC #########
variable "lambda_vpc" {
  default = "vpc-07cd74102c0dfd9ab"
}

variable "public_asg_vpc" {
  default = "vpc-07cd74102c0dfd9ab"
}

variable "private_asg_vpc" {
  default = "vpc-c1e0dab9"
}

variable "es_snapshot_bucket" {
  default = "opera-int-es-bucket"
}

variable "artifactory_repo" {
  default = "general-stage"
  #default = "general-develop"
}

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
    # HySDS v4.1.0-beta.4 with ES 7.9 - R1
    mozart    = "ami-0a4c8f9c7f5a2daec" # mozart v4.18 - 221107
    metrics   = "ami-0c61e7c8b1bfd14a3" # metrics v4.13 - 221107
    grq       = "ami-0f52442c2bd506303" # grq v4.14 - 221107
    factotum  = "ami-03fdbdb8c7caa736e" # factotum v4.14 - 221107
    autoscale = "ami-003e368c872ea1099" # verdi v4.15 - 221031
 }
}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20230322-1.0.0"
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.1"
  }
}

variable "hysds_release" {
  default = "v4.1.0-beta.4"
}

variable "lambda_package_release" {
  default = "1.0.1"
}

variable "pcm_commons_branch" {
  default = "1.0.1"
}

variable "pcm_branch" {
  default = "1.0.1"
}

variable "product_delivery_branch" {
  default = "1.0.1"
}

variable "bach_api_branch" {
  default = "1.0.1"
}

variable "bach_ui_branch" {
  default = "1.0.1"
}

###### Roles ########
variable "asg_use_role" {
  default = true
}

variable "asg_role" {
  default = "am-pcm-verdi-role"
}

variable "pcm_cluster_role" {
  default = {
    name = "am-pcm-cluster-role"
    path = "/"
  }
}

variable "pcm_verdi_role" {
  default = {
    name = "am-pcm-verdi-role"
    path = "/"
  }
}

variable "lambda_role_arn" {
  default = "arn:aws:iam::337765570207:role/am-pcm-lambda-role"
}

##### ES ######
variable "es_bucket_role_arn" {
  default = "arn:aws:iam::337765570207:role/am-es-role"
}

variable "grq_aws_es_host" {
  default = ""
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
    private_ip    = "100.104.13.10"
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
    private_ip    = "100.104.13.11"
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
    private_ip    = "100.104.13.12"
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
    private_ip    = "100.104.13.13"
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

# Smoke test
variable "run_smoke_test" {
  type = bool
  default = true
}
