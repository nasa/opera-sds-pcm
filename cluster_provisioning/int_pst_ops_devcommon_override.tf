# This override file is used by non-dev deployments on venues used by non-developers for
# testing, PST processing, and operations.ß

variable "hysds_release" {
  default = "v5.4.3"
}

variable "lambda_package_release" {
  default = "3.2.3"
}

variable "pcm_commons_branch" {
  default = "3.2.3"
}

variable "pcm_branch" {
  default = "3.2.3"
}

variable "product_delivery_branch" {
  default = "3.2.3"
}

variable "bach_api_branch" {
  default = "3.2.3"
}

variable "bach_ui_branch" {
  default = "3.2.3"
}
