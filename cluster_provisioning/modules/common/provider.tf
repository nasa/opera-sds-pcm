provider "aws" {
  shared_credentials_files = [var.shared_credentials_file]
  region                   = var.region
  profile                  = var.profile
}
