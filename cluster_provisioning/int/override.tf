# globals
#
# venue : userId, in int this is 1 
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "aws_account_id" {
  default = "399787141461"
}

variable "venue" {
  default = "int"
}

variable "environment" {
  default = "int"
}

variable "counter" {
  default = "fwd"
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

###### Security  ########
variable "verdi_security_group_id" {
  default = "sg-0c6342d8c098f1491"
}

variable "cluster_security_group_id" {
  default = "sg-06cad001f63628a45"
}

variable "private_key_file" {
  default = "~/.ssh/operasds-int-cluster-1.pem"
}

variable "keypair_name" {
  default = "operasds-int-cluster-1"
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

variable "asg_vpc" {
  default = "vpc-02676637ea26098a7"
}

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-int-cc-fwd"
}

variable "dataset_bucket" {
  default = "opera-int-rs-fwd"
}

variable "code_bucket" {
  default = "opera-int-cc-fwd"
}

variable "lts_bucket" {
  default = "opera-int-lts-fwd"
}

variable "triage_bucket" {
  default = "opera-int-triage-fwd"
}

variable "isl_bucket" {
  default = "opera-int-isl-fwd"
}

variable "osl_bucket" {
  default = "opera-int-osl-fwd"
}

variable "es_snapshot_bucket" {
  default = "opera-int-es-bucket"
}

variable "artifactory_repo" {
  default = "general-stage"
}

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
    # with HySDS v4.0.1-beta.2
    mozart    = "ami-06b161f22c9086917"
    metrics   = "ami-049f536813d215f39"
    grq       = "ami-0d4589279c337e9c1"
    factotum  = "ami-0f40727533013a107"
    ci        = "ami-0601c031b967d1e15"
    autoscale = "ami-0601c031b967d1e15"
  }
}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20220208-develop-ER2.0"
}

variable "pge_release" {
  default = "1.0.0-er.2.0"
}

variable "hysds_release" {
  default = "v4.0.1-beta.2"
}

variable "lambda_package_release" {
  default = "v1.0.0-er.2.0"
}

variable "pcm_commons_branch" {
  default = "v1.0.0-er.2.0"
}

variable "pcm_branch" {
  default = "v1.0.0-er.2.0"
}

variable "product_delivery_branch" {
  default = "v1.0.0-er.2.0"
}

variable "bach_api_branch" {
  default = "v1.0.0-er.2.0"
}

variable "bach_ui_branch" {
  default = "v1.0.0-er.2.0"
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
  default = "arn:aws:iam::399787141461:role/am-pcm-lambda-role"
}

##### ES ######
variable "es_bucket_role_arn" {
  default = "arn:aws:iam::399787141461:role/am-es-role"
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
