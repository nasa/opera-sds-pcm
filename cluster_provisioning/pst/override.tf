# globals
#
# venue : userId, in int this is 1 
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "aws_account_id" {
  default = "483785460105"
}

variable "venue" {
  default = "ci"
}

variable "environment" {
  default = "pst"
}

variable "counter" {
  #default = "fwd"
  default = "pop1"
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
  default = "sg-01b0d3772049cc263"
}

variable "private_verdi_security_group_id" {
  default = "sg-03eed53cf8fbfea3a"
}

variable "cluster_security_group_id" {
  default = "sg-070b0cf50df41767f"
}


variable "private_key_file" {
  default = "~/.ssh/pyoon_pcm_pst.pem"
}

variable "keypair_name" {
  default = "pyoon_pcm_pst"
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
  default = "subnet-005726c648910667e"
}

####### VPC #########
variable "lambda_vpc" {
  default = "vpc-09690c3880fda922e"
}

variable "public_asg_vpc" {
  default = "vpc-09690c3880fda922e"
}

variable "private_asg_vpc" {
  default = "vpc-0ac48391a96eb1291"
}


##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-pst-cc-pop1"
}

variable "dataset_bucket" {
  default = "opera-pst-rs-pop1"
}

variable "code_bucket" {
  default = "opera-pst-cc-pop1"
}

variable "lts_bucket" {
  default = "opera-pst-lts-pop1"
}

variable "triage_bucket" {
  default = "opera-pst-triage-pop1"
}

variable "isl_bucket" {
  default = "opera-pst-isl-pop1"
}

variable "osl_bucket" {
  default = "opera-pst-osl-pop1"
}

variable "es_snapshot_bucket" {
  default = "opera-pst-es-bucket"
}

variable "artifactory_repo" {
  default = "general-stage"
}

######### ami vars #######
variable "amis" {
  type = map(string)
  default = {
  
    # HySDS v4.0.1-beta.8-oraclelinux - Universal AMIs (June 10, 2022)
    grq       = "ami-0a4ab3a778c395194" # OL8 All-project grq v4.13 - 220610
    metrics   = "ami-0d5c253305b866dc0" # metrics v4.12 - 220610A
    mozart    = "ami-00f898f3f2f930aa4" # mozart v4.17 - 220610
    factotum  = "ami-0d0e97c6690f612d7" # OL8 All-project factotum v4.13 - 220609
    autoscale = "ami-0d5a7f80daf236d93" # verdi v4.12 patchdate - 220609
    ci        = "ami-0d5a7f80daf236d93" # verdi v4.12 patchdate - 220609

  }

}

####### Release Branches #############
variable "pge_snapshots_date" {
  default = "20220609-1.0.0-rc.1.0"
}

variable "pge_release" {
  default = "1.0.0-rc.2.0"
}

variable "hysds_release" {
  default = "v4.0.1-beta.8-oraclelinux"
}

variable "lambda_package_release" {
  default = "1.0.0-rc.3.0"
}

variable "pcm_commons_branch" {
  default = "1.0.0-rc.3.0"
}

variable "pcm_branch" {
  default = "1.0.0-rc.3.0"
}

variable "product_delivery_branch" {
  default = "1.0.0-rc.3.0"
}

variable "bach_api_branch" {
  default = "1.0.0-rc.3.0"
}

variable "bach_ui_branch" {
  default = "1.0.0-rc.3.0"
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
  default = "arn:aws:iam::483785460105:role/am-pcm-lambda-role"
}

##### ES ######
variable "es_bucket_role_arn" {
  default = "arn:aws:iam::483785460105:role/am-es-role"
}

####### CNM Response job vars #######
variable "daac_delivery_proxy" {
  default = "arn:aws:sns:us-west-2:483785460105:daac-proxy-for-opera-pst"
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
    instance_type = "r5.4xlarge"
    root_dev_size = 200
    #private_ip    = "100.104.13.10"
    private_ip    = "100.104.62.10"
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
    private_ip    = "100.104.62.11"
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
    private_ip    = "100.104.62.12"
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
    #private_ip    = "100.104.13.13"
    private_ip    = "100.104.62.13"
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
