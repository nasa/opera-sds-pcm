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

###### Security  ########
variable "public_verdi_security_group_id" {
  # pop1 security group
  default = "sg-06bf23a69b4d83f66"
}

variable "private_verdi_security_group_id" {
  # pop1 security group
  default = "sg-045ff9d3d16a65ba4"
}

variable "cluster_security_group_id" {
  # pop1 security group
  default = "sg-0958a845b83c5b857"
}

variable "private_key_file" {
  default = "~/.ssh/operasds-int-cluster-pop1.pem"
}

variable "keypair_name" {
  default = "operasds-int-cluster-pop1"
}

##### Bucket Names #########
variable "docker_registry_bucket" {
  default = "opera-int-cc-pop1"
}

variable "dataset_bucket" {
  default = "opera-int-rs-pop1"
}

variable "code_bucket" {
  default = "opera-int-cc-pop1"
}

variable "lts_bucket" {
  default = "opera-int-lts-pop1"
}

variable "triage_bucket" {
  default = "opera-int-triage-pop1"
}

variable "isl_bucket" {
  default = "opera-int-isl-pop1"
}

variable "osl_bucket" {
  default = "opera-int-osl-pop1"
}
