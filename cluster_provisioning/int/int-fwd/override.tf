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
