# infra/20-compute/trino.tf
# Cụm công cụ truy vấn phân tán Trino trên Auto Scaling Group
# Kết nối bảng Apache Iceberg trên S3 qua AWS Glue Data Catalog

resource "aws_launch_template" "trino" {
  name_prefix            = "airlake-trino-"
  image_id               = var.ami_id
  instance_type          = var.trino_instance_type
  vpc_security_group_ids = [local.sg.trino]
  user_data = base64encode(templatefile("${path.module}/userdata/trino.sh.tftpl", {
    lake_bucket = local.f.lake_bucket
    region      = var.region
  }))
  iam_instance_profile {
    name = local.profiles.trino
  }
  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size = 30
      volume_type = "gp3"
      encrypted   = true
    }
  }
  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "airlake-trino"
      Project = "airlake"
      Role    = "trino-query-engine"
    }
  }
}

# Auto Scaling Group cho Trino (chạy on-demand, tự động tắt đêm để tiết kiệm chi phí)
resource "aws_autoscaling_group" "trino" {
  name                = "airlake-trino"
  min_size            = 0
  max_size            = 2
  desired_capacity    = 1
  vpc_zone_identifier = local.subnets
  launch_template {
    id      = aws_launch_template.trino.id
    version = "$Latest"
  }
  lifecycle {
    ignore_changes = [desired_capacity, min_size, max_size]
  }
}

resource "aws_autoscaling_schedule" "trino_night_off" {
  scheduled_action_name  = "trino-night-off"
  autoscaling_group_name = aws_autoscaling_group.trino.name
  recurrence             = "0 23 * * *"
  time_zone              = "Asia/Ho_Chi_Minh"
  min_size               = 0
  max_size               = 0
  desired_capacity       = 0
}
