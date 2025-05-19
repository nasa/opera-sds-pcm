# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "counter" {
  default = "pop1"
}

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  default = "forward"
}

variable "clear_s3_aws_es" {
   default = false
}

variable "private_key_file" {
  default = "~/.ssh/operasds-ops-pop1.pem"
}

variable "keypair_name" {
  default = "operasds-ops-pop1"
}

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-ops-cc-pop1"
}

variable "dataset_bucket" {
  default = "opera-ops-rs-pop1"
}

variable "code_bucket" {
  default = "opera-ops-cc-pop1"
}

variable "lts_bucket" {
  default = "opera-ops-lts-pop1"
}

variable "triage_bucket" {
  default = "opera-ops-triage-pop1"
}

variable "isl_bucket" {
  default = "opera-ops-isl-pop1"
}

variable "osl_bucket" {
  default = "opera-ops-osl-pop1"
}

variable "es_snapshot_bucket" {
  default = "opera-ops-es-bucket"
}

variable "trace" {
  default = "opera-ops-pop1"
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
    private_ip    = "100.104.82.30"
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
    private_ip    = "100.104.82.31"
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
    private_ip    = "100.104.82.32"
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
    private_ip    = "100.104.82.33"
    publicc_ip    = ""
  }
}

# Smoke test
variable "run_smoke_test" {
  type = bool
  default = false
}
