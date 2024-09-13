terraform {
  required_version = ">= 1.0.11"
  #  required_version = ">= 0.13"

  required_providers {
    # TODO: remove this pin once this ticket is resolved:
    #   https://github.com/hashicorp/terraform-provider-aws/issues/10297
    aws = {
      source  = "hashicorp/aws"
      version = "3.75.2"
    }
    null = {
      source = "hashicorp/null"
    }
    random = {
      source = "hashicorp/random"
    }
  }
}
