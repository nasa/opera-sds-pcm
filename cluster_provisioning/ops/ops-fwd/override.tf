variable "counter" {
  default = "fwd"
}

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  default = "forward"
}

variable "clear_s3_aws_es" {
   default = false
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

variable "trace" {
  default = "opera-ops-fwd"
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

# Smoke test
variable "run_smoke_test" {
  type = bool
  default = false
}