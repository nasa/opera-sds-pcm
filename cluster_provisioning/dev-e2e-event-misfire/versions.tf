
terraform {
  required_version = ">= 1.0.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "2.70.1"
    }
    null = {
      source = "hashicorp/null"
    }
  }
}
