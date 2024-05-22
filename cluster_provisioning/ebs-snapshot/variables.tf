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
    "cslc_s1"  = "2.1.1"
    "rtc_s1"   = "2.1.1"
    "dswx_s1"  = "3.0.0-rc.2.1"
    "disp_s1"  = "3.0.0-rc.2.1"
  }
}

variable "private_key_file" {
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
}

variable "private_verdi_security_group_id" {
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
}

variable "private_asg_vpc" {
}

variable "subnet_id" {
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
