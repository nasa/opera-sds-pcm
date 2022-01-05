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
  default = "ops"
}

variable "environment" {
  default = "ops"
}

variable "counter" {
  default = "1"
}

variable "crid" {
  default = "P00200"
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
  default = "forward"
}

###### Security  ########
variable "verdi_security_group_id" {
  default = "sg-0603340fd26385eb5"
}

variable "cluster_security_group_id" {
  default = "sg-053b5a83cc8a12ffc"
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
  default = "subnet-098d683537533a734"
}

####### VPC #########
variable "lambda_vpc" {
  default = "vpc-0ed3505f06778b854"
}

variable "asg_vpc" {
  default = "vpc-08388dc81eebde05b"
}

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = ""
}

variable "dataset_bucket" {
  default = "opera-ops-rs-fwd"
}

variable "code_bucket" {
  default = "opera-ops-cc-fwd"
}

variable "lts_bucket" {
  default = "opera-ops-lts-fwd"
}

variable "triage_bucket" {
  default = "opera-ops-triage-fwd"
}

variable "isl_bucket" {
  default = "opera-ops-isl-fwd"
}

variable "osl_bucket" {
  default = "opera-ops-osl-fwd"
}

variable "es_snapshot_bucket" {
  default = ""
}

variable "artifactory_repo" {
  default = "general-stage"
}

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
    mozart    = "ami-01aa6dbec644a2672"
    metrics   = "ami-0ee90e1f71e532095"
    grq       = "ami-0872577aec2e40df1"
    factotum  = "ami-06158820898dd2dfd"
    ci        = "ami-00baa2004b03f6090"
    autoscale = "ami-00baa2004b03f6090"
  }
}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20210805-R2.0.0"
}

variable "pge_release" {
  default = "R2.0.0"
}

variable "hysds_release" {
  default = "v4.0.1-beta.2"
}

variable "lambda_package_release" {
  default = "release-r2.2.1"
}

variable "pcm_commons_branch" {
  default = "release-r2.2.1"
}

variable "pcm_branch" {
  default = "release-r2.0.0"
}

variable "product_delivery_branch" {
  default = "release-r2.2.1"
}

variable "opera_bach_api_branch" {
  default = "release-r2.0.0"
}

variable "opera_bach_ui_branch" {
  default = "release-r2.0.0"
}

###### Roles ########
variable "asg_use_role" {
  default = "true"
}

variable "asg_role" {
  default = "am-pcm-verdi-role"
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

# ami vars
variable "amis" {
  type = map(string)
  default = {
    mozart    = "ami-04f3e46d5c6b4ce03"
    metrics   = "ami-01036d95384ffaee3"
    grq       = "ami-09fc228c7a28d839c"
    factotum  = "ami-088982c253b1c835a"
    ci        = "ami-07a3d26f784b9e9be"
    autoscale = "ami-00baa2004b03f6090"
  }
}

# mozart vars
variable "mozart" {
  type = map(string)
  default = {
    name          = "mozart"
    instance_type = "r5.4xlarge"
    root_dev_size = 200
    private_ip    = "100.67.0.10"
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
    private_ip    = "100.67.0.11"
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
    private_ip    = "100.67.0.12"
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
    private_ip    = "100.67.0.13"
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
