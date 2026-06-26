# globals
#
# venue : userId, in int this is 1
# counter : 1-n or version
# private_key_file : the equivalent to .ssh/id_rsa or .pem file
#

##### Environments #######
variable "venue" {
  default = "int"
}

variable "environment" {
  default = "int"
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
  default = "us-west-2b"
}

# Specify either forward or reprocessing. When this is set to "reprocessing"
# PCM will disable all timers upon provisioning. Otherwise, they are enabled at start up.
variable "cluster_type" {
  default = "forward"
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

variable "es_snapshot_bucket" {
  default = "opera-int-es-bucket"
}

variable "trace" {
  default = "opera-int-fwd"
}


variable "artifactory_repo" {
  default = "general-stage"
  #default = "general-develop"
}

# We must use artifactor for any non-dev clusters
variable "use_artifactory" {
  default = true
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
    root_dev_size = 400
    private_ip    = "100.104.13.10"
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
    private_ip    = "100.104.13.11"
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
    private_ip    = "100.104.13.12"
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
    private_ip    = "100.104.13.13"
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
  type    = bool
  default = true
}

variable "es_user" {
  default = "None"
}

variable "es_pass" {
  default = "None"
}

variable "rs_fwd_bucket_expiration_default" {
  type    = number
  default = 30

  validation {
    condition     = var.rs_fwd_bucket_expiration_default > 0
    error_message = "rs_fwd_bucket_expiration_default must be >= 1"
  }
}

variable "rs_fwd_bucket_expiration_base_rules" {
  type = map(object({
    enabled = bool
    days    = number
  }))
  default = {
    inputs : {
      enabled : true,
      days : 14
    },
    tmp : {
      enabled : true,
      days : 7
    }
  }

  validation {
    condition     = sort(keys(var.rs_fwd_bucket_expiration_base_rules)) == sort(["inputs", "tmp"])
    error_message = "rs_fwd_bucket_expiration_base_rules must contain inputs and tmp keys"
  }

  validation {
    condition     = length([for v in values(var.rs_fwd_bucket_expiration_base_rules) : v if v.days < 1]) == 0
    error_message = "days must be >= 1"
  }
}

variable "rs_fwd_bucket_expiration_product_rules" {
  type = map(object({
    enabled = bool
    days    = number
  }))
  default = {
    CSLC_S1 = {
      enabled = true,
      days    = 30
    }
    CSLC_S1_STATIC = {
      enabled = true,
      days    = 30
    }
    RTC_S1 = {
      enabled = true,
      days    = 30
    }
    RTC_S1_STATIC = {
      enabled = true,
      days    = 30
    }
    DSWx_HLS = {
      enabled = true,
      days    = 30
    }
    DSWx_S1 = {
      enabled = true,
      days    = 30
    }
    DISP_S1 = {
      enabled = true,
      days    = 30
    }
    DISP_S1_STATIC = {
      enabled = true,
      days    = 30
    }
    DSWx_NI = {
      enabled = true,
      days    = 30
    }
    DIST_S1 = {
      enabled = false,
      days    = 30
    }
    TROPO = {
      enabled = true,
      days    = 30
    }
    DISP_NI = {
      enabled = true,
      days    = 30
    }
    CAL_DISP = {
      enabled = true,
      days    = 30
    }
  }

  validation {
    condition     = length([for v in values(var.rs_fwd_bucket_expiration_product_rules) : v if v.days < 1]) == 0
    error_message = "days must be >= 1"
  }
}

variable "rs_fwd_bucket_expiration_product_rule_type" {
  type    = string
  default = "specific"

  validation {
    condition     = contains(["basic", "specific"], var.rs_fwd_bucket_expiration_product_rule_type)
    error_message = "rs_fwd_bucket_expiration_product_rule_type must be either basic or specific"
  }
}
