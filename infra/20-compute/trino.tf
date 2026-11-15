# infra/20-compute/trino.tf
# Kiến trúc cụm công cụ truy vấn phân tán Trino (Distributed Cluster Architecture)
# Phân tách rõ ràng giữa Coordinator (Điều phối & Discovery) và Worker (Xử lý truy vấn phân tán)
# Kết nối bảng Apache Iceberg trên S3 qua AWS Glue Data Catalog

locals {
  trino_coordinator_ip = "10.0.0.20"
}

# 1. Trino Coordinator (Node điều phối độc lập với IP tĩnh trong VPC)
resource "aws_instance" "trino_coordinator" {
  ami                    = var.ami_id
  instance_type          = var.trino_instance_type
  subnet_id              = local.subnets[0]
  private_ip             = local.trino_coordinator_ip
  vpc_security_group_ids = [local.sg.trino]
  iam_instance_profile   = local.profiles.trino

  user_data = templatefile("${path.module}/userdata/trino.sh.tftpl", {
    lake_bucket    = local.f.lake_bucket
    region         = var.region
    is_coordinator = true
    coordinator_ip = local.trino_coordinator_ip
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
    Name    = "airlake-trino-coordinator"
    Project = "airlake"
    Role    = "trino-coordinator"
  }
}

# 2. Launch Template cho Trino Workers
resource "aws_launch_template" "trino_worker" {
  name_prefix            = "airlake-trino-worker-"
  image_id               = var.ami_id
  instance_type          = var.trino_instance_type
  vpc_security_group_ids = [local.sg.trino]

  user_data = base64encode(templatefile("${path.module}/userdata/trino.sh.tftpl", {
    lake_bucket    = local.f.lake_bucket
    region         = var.region
    is_coordinator = false
    coordinator_ip = local.trino_coordinator_ip
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
      Name    = "airlake-trino-worker"
      Project = "airlake"
      Role    = "trino-worker"
    }
  }
}

# 3. Auto Scaling Group cho Trino Workers (Co giãn động phân tán)
resource "aws_autoscaling_group" "trino" {
  name                = "airlake-trino-workers"
  min_size            = 1
  max_size            = 3
  desired_capacity    = 1
  vpc_zone_identifier = local.subnets

  launch_template {
    id      = aws_launch_template.trino_worker.id
    version = "$Latest"
  }

  lifecycle {
    ignore_changes = [desired_capacity, min_size, max_size]
  }
}

# 4. FinOps: Lịch tự động tắt nhóm Worker vào ban đêm
resource "aws_autoscaling_schedule" "trino_night_off" {
  scheduled_action_name  = "trino-night-off"
  autoscaling_group_name = aws_autoscaling_group.trino.name
  recurrence             = "0 23 * * *"
  time_zone              = "Asia/Ho_Chi_Minh"
  min_size               = 0
  max_size               = 0
  desired_capacity       = 0
}

# 5. FinOps: Lịch tự động bật nhóm Worker vào buổi sáng
resource "aws_autoscaling_schedule" "trino_morning_on" {
  scheduled_action_name  = "trino-morning-on"
  autoscaling_group_name = aws_autoscaling_group.trino.name
  recurrence             = "0 7 * * *"
  time_zone              = "Asia/Ho_Chi_Minh"
  min_size               = 1
  max_size               = 3
  desired_capacity       = 1
}

# 6. Chính sách tự động mở rộng Trino Worker theo ngưỡng CPU (NFR-02)
resource "aws_autoscaling_policy" "trino_cpu_scaling" {
  name                      = "airlake-trino-cpu-scaling"
  autoscaling_group_name    = aws_autoscaling_group.trino.name
  policy_type               = "TargetTrackingScaling"
  estimated_instance_warmup = 180

  target_tracking_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ASGAverageCPUUtilization"
    }
    target_value = 75.0
  }
}
