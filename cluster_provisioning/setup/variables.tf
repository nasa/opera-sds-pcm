variable "shared_credentials_file" {
  default = "~/.aws/credentials"
}

variable "region" {
  default = "us-west-2"
}

variable "profile" {
  default = "saml-pub"
}

variable "aws_account" {
  default = "681612454726"
}

variable "pcm_verdi_role" {
  default = {
    name = "am-pcm-verdi-role"    # in INT
#    name = "am-pcm-dev-verdi-role" # in DEV
    path = "/"
  }
}
