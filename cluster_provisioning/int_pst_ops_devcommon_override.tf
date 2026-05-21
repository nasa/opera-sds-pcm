# This override file is used by non-dev deployments on venues used by non-developers for
# testing, PST processing, and operations.ß

variable "hysds_release" {
  default = "v6.1.2"
}

variable "lambda_package_release" {
  default = "6.0.2"
}

variable "pcm_commons_branch" {
  default = "6.0.2"
}

variable "pcm_branch" {
  default = "6.0.2"
}

variable "product_delivery_branch" {
  default = "6.0.2"
}

variable "bach_api_branch" {
  default = "6.0.2"
}

variable "bach_ui_branch" {
  default = "6.0.2"
}
