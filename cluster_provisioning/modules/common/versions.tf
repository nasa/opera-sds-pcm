terraform {
  required_version = ">= 1.0.11"

  required_providers {
    # TODO: remove this pin once this ticket is resolved:
    #   https://github.com/hashicorp/terraform-provider-aws/issues/10297
    aws = {
      source  = "hashicorp/aws"
#      version = "3.75.2"
      version = "5.50.0"
    }
    null = {
      source = "hashicorp/null"
      version = ">=3.2.0"
    }
    random = {
      source = "hashicorp/random"
      version = ">=3.5.0"
    }
    template = {
      source  = "hashicorp/template"
    }
  }
}
