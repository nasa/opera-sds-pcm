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

variable "counter" {
  default = "fwd"
  #default = "pop1"
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
  # fwd security group
  default = "sg-0869719f04e735bd6"
  # pop1 security group
  #default = "sg-045ff9d3d16a65ba4"
}

variable "cluster_security_group_id" {
  # fwd security group
  default = "sg-039db67f56d1b12f0"
  # pop1 security group
  #default = "sg-06bf23a69b4d83f66"
}

variable "private_key_file" {
  default = "~/.ssh/operasds-int-cluster-fwd.pem"
  #default = "~/.ssh/operasds-int-cluster-pop1.pem"
}

variable "keypair_name" {
  default = "operasds-int-cluster-fwd"
#  default = "operasds-int-cluster-pop1"
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

variable "asg_vpc" {
  default = "vpc-c1e0dab9"
}

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-int-cc-fwd"
  #default = "opera-int-cc-pop1"
}

variable "dataset_bucket" {
  default = "opera-int-rs-fwd"
  #default = "opera-int-rs-pop1"
}

variable "code_bucket" {
  default = "opera-int-cc-fwd"
  #default = "opera-int-cc-pop1"
}

variable "lts_bucket" {
  default = "opera-int-lts-fwd"
  #default = "opera-int-lts-pop1"
}

variable "triage_bucket" {
  default = "opera-int-triage-fwd"
  #default = "opera-int-triage-pop1"
}

variable "isl_bucket" {
  default = "opera-int-isl-fwd"
  #default = "opera-int-isl-pop1"
}

variable "osl_bucket" {
  default = "opera-int-osl-fwd"
  #default = "opera-int-osl-pop1"
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
	# HySDS v4.0.1-beta8-oraclelinux
    mozart    = "ami-014ef2d57c80be48c"  # opera int mozart - ol8
    metrics   = ""  # opera int metrics - ol8
    grq       = ""  # opera int grq - ol8
    factotum  = ""  # opera int factotum - ol8
    ci        = "ami-0e4564f8e531e816f"  # OL8 All-project verdi v4.12
    autoscale = "ami-0e4564f8e531e816f"  # OL8 All-project verdi v4.12
  }
}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20220401-1.0.0-er.3.0"
}

variable "pge_release" {
  default = "1.0.0-er.3.0"
}

variable "hysds_release" {
  default = "v4.0.1-beta.8-oraclelinux"
}

variable "lambda_package_release" {
  default = "1.0.0-er.3.0"
}

variable "pcm_commons_branch" {
  default = "1.0.0-er.3.0"
}

variable "pcm_branch" {
  default = "1.0.0-er.3.0"
}

variable "product_delivery_branch" {
  default = "1.0.0-er.3.0"
}

variable "bach_api_branch" {
  default = "1.0.0-er.3.0"
}

variable "bach_ui_branch" {
  default = "1.0.0-er.3.0"
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
