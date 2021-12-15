# globals
#
# venue : userId, in int this is 1 
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "aws_account_id" {
  default = "271039147104"
}

variable "venue" {
  default = "int"
}

variable "environment" {
  default = "dev"
}

variable "counter" {
  default = "6"
}

variable "crid" {
  default = "D00200"
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
variable "verdi_security_group_id" {
  default = "sg-0dc79d95a7e284bf9"
}

variable "cluster_security_group_id" {
  default = "sg-0656188b92eabb69d"
}

variable "private_key_file" {
  default = "~/.ssh/operasds-int-cluster-1.pem"
}

variable "keypair_name" {
  default = "operasds-int-cluster-1"
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

####### Subnet ###########
variable "subnet_id" {
  default = "subnet-0b69acd65c7bf133b"
}

####### VPC #########
variable "lambda_vpc" {
  default = "vpc-0f27ba0544fa10ee6"
}

variable "asg_vpc" {
  default = "vpc-9b3500ff"
}

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-dev-cc-fwd-int"
}

variable "dataset_bucket" {
  default = "opera-dev-rs-fwd-int"
}

variable "code_bucket" {
  default = "opera-dev-cc-fwd-int"
}

variable "lts_bucket" {
  default = "opera-dev-lts-fwd-int"
}

variable "triage_bucket" {
  default = "opera-dev-triage-fwd-int"
}

variable "isl_bucket" {
  default = "opera-dev-isl-fwd-int"
}

variable "osl_bucket" {
  default = "opera-dev-osl-fwd-int"
}

variable "es_snapshot_bucket" {
  #default = "opera-dev-es-bucket-int"
  default = "opera-dev-cc-fwd-int"
}

variable "artifactory_repo" {
  default = "general-develop"
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

####### CNM Response job vars #######
variable "daac_delivery_proxy" {
  default = "arn:aws:sqs:us-west-2:824481342913:sds-n-cumulus-int-opera-workflow-queue"
}

variable "use_daac_cnm" {
  default = true
}

variable "daac_endpoint_url" {
  default = ""
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

variable "bach_api_branch" {
  default = "release-r2.0.0-nisar"
}

variable "bach_ui_branch" {
  default = "release-r1.0.0-nisar"
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
  default = "am-pcm-verdi-stage-role"
}

variable "pcm_cluster_role" {
  default = {
    name = "am-pcm-cluster-stage-role"
    path = "/"
  }
}

variable "pcm_verdi_role" {
  default = {
    name = "am-pcm-verdi-stage-role"
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

variable "lambda_role_arn" {
  default = "arn:aws:iam::271039147104:role/am-pcm-lambda-stage-role"
}

##### ES ######
variable "es_bucket_role_arn" {
  default = "arn:aws:iam::271039147104:role/am-es-role"
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
