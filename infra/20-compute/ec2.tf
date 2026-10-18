resource "aws_instance" "kafka" {
  count                  = var.kafka_count
  ami                    = var.ami_id
  instance_type          = var.kafka_instance_type
  subnet_id              = local.subnets[count.index % 2]
  vpc_security_group_ids = [local.sg.kafka]
  iam_instance_profile   = local.profiles.kafka
  user_data              = file("${path.module}/userdata/base.sh")
  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }
  tags = { Name = "airlake-kafka-${count.index + 1}" }
}

resource "aws_instance" "superset" {
  ami                    = var.ami_id
  instance_type          = var.superset_instance_type
  subnet_id              = local.subnets[0]
  vpc_security_group_ids = [local.sg.superset]
  iam_instance_profile   = local.profiles.superset
  user_data              = file("${path.module}/userdata/base.sh")
  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }
  tags = { Name = "airlake-superset" }
}