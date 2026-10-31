locals {
  kafka_private_ips = ["10.0.0.11", "10.0.1.12", "10.0.0.13"]
  active_kafka_ips  = slice(local.kafka_private_ips, 0, var.kafka_count)
  quorum_voters     = join(",", [for i, ip in local.active_kafka_ips : "${i + 1}@${ip}:9093"])
}

resource "aws_instance" "kafka" {
  count                  = var.kafka_count
  ami                    = var.ami_id
  instance_type          = var.kafka_instance_type
  subnet_id              = local.subnets[count.index % 2]
  private_ip             = local.kafka_private_ips[count.index]
  vpc_security_group_ids = [local.sg.kafka]
  iam_instance_profile   = local.profiles.kafka
  user_data = templatefile("${path.module}/userdata/kafka.sh.tftpl", {
    node_id       = count.index + 1
    node_ip       = local.kafka_private_ips[count.index]
    quorum_voters = local.quorum_voters
    cluster_id    = "4L622nShTUiT8YTrQ3hNTg"
    default_rf    = var.kafka_count >= 3 ? 3 : 1
    min_isr       = var.kafka_count >= 3 ? 2 : 1
  })
  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }
  tags = {
    Name    = "airlake-kafka-${count.index + 1}"
    Project = "airlake"
    Role    = "kafka-broker"
  }
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
