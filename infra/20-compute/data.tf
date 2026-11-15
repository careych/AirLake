data "aws_caller_identity" "me" {}

data "terraform_remote_state" "foundation" {
  backend = "s3"
  config = {
    bucket = "airlake-tfstate-${data.aws_caller_identity.me.account_id}"
    key    = "foundation/terraform.tfstate"
    region = var.region
  }
}

locals {
  f        = data.terraform_remote_state.foundation.outputs
  subnets  = local.f.subnet_ids
  sg       = local.f.sg_ids
  profiles = local.f.profile_names
}

# ID AMI Amazon Linux 2023 do chủ Organization cấp.
# Không tra tự động được vì SCP chặn ssm:GetParameters và ec2:DescribeImages.
variable "ami_id" {
  type = string
}

variable "kafka_count" {
  type    = number
  default = 3 # hạ xuống 1 nếu phải cắt giảm
}