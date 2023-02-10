# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "counter" {
  default = "fwd"
}

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  default = "forward"
}

###### Security  ########
variable "public_verdi_security_group_id" {
  # fwd security group
  default = "sg-0e60f417ff3c769fb"
}

variable "private_verdi_security_group_id" {
  # fwd security group
  default = "sg-0869719f04e735bd6"
}

variable "cluster_security_group_id" {
  # fwd security group
  default = "sg-039db67f56d1b12f0"
}

variable "private_key_file" {
  default = "~/.ssh/operasds-int-cluster-fwd.pem"
}

variable "keypair_name" {
  default = "operasds-int-cluster-fwd"
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

/*
# mozart vars
variable "mozart" {
  type = map(string)
  default = {
    name          = "mozart"
    instance_type = "r5.4xlarge"
    root_dev_size = 200
    private_ip    = "100.104.49.10"
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
    private_ip    = "100.104.49.11"
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
    private_ip    = "100.104.49.12"
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
    private_ip    = "100.104.49.13"
    publicc_ip    = ""
  }
}
*/
