variable "project" {
  default = "opera"
}

variable "venue" {
  default = "ci"
}

variable "profile" {
  #default = "saml"
  #int
  default = "saml-pub"
}

variable "verdi_release" {
}

variable "registry_release" {
  default = "2"
}

variable "logstash_release" {
  default = "7.9.3"
}

variable "pge_snapshots_date" {
  default = ""
}

variable "pge_releases" {
  type = map(string)
  default = {
    "dswx_hls" = "1.0.2"
    "cslc_s1" = "2.0.0"
    "rtc_s1" = "2.0.0"
  }
}

variable "private_key_file" {
  #int
  #default="~/.ssh/operasds-int-cluster-1.pem"
}

variable "keypair_name" {
  #default = "operasds-int-cluster-1"
}

variable "shared_credentials_file" {
  default = "~/.aws/credentials"
}

variable "az" {
  default = "us-west-2a"
}

variable "region" {
  default = "us-west-2"
}

variable "public_verdi_security_group_id" {
  default = "sg-008b5ba6ecb1b95b8"
}

variable "private_verdi_security_group_id" {
  default = "sg-08d98b1b7b66f7dea"
}

variable "pcm_verdi_role" {
  default = {
    name = "am-pcm-dev-verdi-role"
    path = "/"
  }
}

variable "verdi" {
  type = map(string)
  default = {
    name = "verdi"
    ami = "ami-0b3b047af6d70bc82" # verdi v4.16 patchdate - 20230816
    instance_type = "t3.medium"
    device_name = "/dev/sda1"
    device_size = 50
    docker_device_name = "/dev/sdf"
    docker_device_size = 25
    private_ip = ""
    public_ip = ""
  }
}

variable "asg_use_role" {
  default = "true"
}

variable "public_asg_vpc" {
  default = "vpc-02676637ea26098a7"
}

variable "private_asg_vpc" {
  default = "vpc-b5a983cd"
}

variable "subnet_id" {
  default = "subnet-000eb551ad06392c7"
}

variable "artifactory_base_url" {
  default = "https://artifactory-fn.jpl.nasa.gov/artifactory"
}

variable "artifactory_repo" {
  default = "general-develop"
#  default = "general"
}

variable "artifactory_mirror_url" {
  default = "s3://opera-dev/artifactory_mirror"
}

variable "docker_user" {
  default = ""
}

variable "docker_pwd" {
  default = ""
}

variable "use_s3_uri_structure" {
  default = true
}

variable "artifactory_fn_user" {
  default = ""
}

variable "artifactory_fn_api_key" {
  default = ""
}
