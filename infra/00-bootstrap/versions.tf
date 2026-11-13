terraform {
  required_version = ">= 1.11"
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "airlake"
      Env       = "dev"
      ManagedBy = "terraform"
      Owner     = "minh-khue"
    }
  }
}