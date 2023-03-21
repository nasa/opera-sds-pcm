# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "aws_account_id" {
  default = "907504701509"
}

variable "venue" {
  default = "ops"
}

variable "environment" {
  default = "ops"
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
  default = "us-west-2a"
}

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  default = "forward"
}

###### Security  ########
variable "public_verdi_security_group_id" {
  default = "sg-09e617d8d3e708df2"
}

variable "private_verdi_security_group_id" {
  default = "sg-0a42f42f56f358971"
}

variable "cluster_security_group_id" {
  default = "sg-029c5fb30fd2f3876"
}


variable "private_key_file" {
  default = "~/.ssh/operasds-ops-fwd.pem"
}

variable "keypair_name" {
  default = "operasds-ops-fwd"
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
  default = "subnet-0009fde6de693b714"
}

####### VPC #########
variable "lambda_vpc" {
  default = "vpc-04ce247929cd21150"
}

variable "public_asg_vpc" {
  default = "vpc-04ce247929cd21150"
}

variable "private_asg_vpc" {
  default = "vpc-05d9f741bae3208ab"
}


##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-ops-cc-fwd"
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
  default = "opera-ops-es-bucket"
}

variable "artifactory_repo" {
  default = "general-stage"
}

<<<<<<< HEAD
variable "use_artifactory" {
  type = bool
  default = true
=======
######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
     # HySDS v4.0.1-beta.8-oraclelinux - Universal AMIs (from Suzan 10-5-22)
     mozart    = "ami-0ea8b5e8245324b0a" # mozart v4.18
     metrics   = "ami-0f575f73bcd1f55e4" # metrics v4.13
     grq       = "ami-0c84c56035af7fb6c" # grq v4.14
     factotum  = "ami-068944cd3359de653" # factotum v4.14
     autoscale = "ami-0922fa62a31e88485" # verdi v4.14
     ci        = "ami-0922fa62a31e88485" # verdi v4.14
  }

}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20230203-1.0.0-rc.7.0"
}

variable "pge_releases" {
   type = map(string)
   default = {
     "dswx_hls" = "1.0.0"
  }
>>>>>>> develop
}

variable "hysds_release" {
  default = "v4.1.0-beta.4"
}

variable "lambda_package_release" {
  default = "1.0.0-rc.9.0"
}

variable "pcm_commons_branch" {
  default = "1.0.0-rc.9.0"
}

variable "pcm_branch" {
  default = "1.0.0-rc.9.0"
}

variable "product_delivery_branch" {
  default = "1.0.0-rc.9.0"
}

variable "bach_api_branch" {
  default = "1.0.0-rc.9.0"
}

variable "bach_ui_branch" {
  default = "1.0.0-rc.9.0"
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
  default = "arn:aws:iam::907504701509:role/am-pcm-lambda-role"
}

##### ES ######
variable "es_bucket_role_arn" {
  default = "arn:aws:iam::907504701509:role/am-es-role"
}

####### CNM Response job vars #######
variable "po_daac_delivery_proxy" {
  default = "arn:aws:sns:us-west-2:907504701509:daac-proxy-for-opera-ops"
  #default = "arn:aws:sns:us-west-2:638310961674:podaac-uat-cumulus-provider-input-sns"
}

variable "asf_daac_delivery_proxy" {
  default = "arn:aws:sns:us-west-2:907504701509:daac-proxy-for-opera-ops"
  #default = "arn:aws:sns:us-west-2:638310961674:podaac-uat-cumulus-provider-input-sns"
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
    instance_type = "r6i.4xlarge"
    root_dev_size = 200
    #private_ip    = "100.104.13.10"
    private_ip    = "100.104.82.20"
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
    #private_ip    = "100.104.13.11"
    private_ip    = "100.104.82.11"
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
    #private_ip    = "100.104.13.12"
    private_ip    = "100.104.82.12"
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
    #private_ip    = "100.104.13.13"
    private_ip    = "100.104.82.13"
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
