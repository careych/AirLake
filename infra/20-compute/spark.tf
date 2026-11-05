resource "aws_launch_template" "spark" {
  name_prefix            = "airlake-spark-"
  image_id               = var.ami_id
  instance_type          = var.spark_instance_type
  vpc_security_group_ids = [local.sg.spark]
  user_data              = base64encode(file("${path.module}/userdata/base.sh"))
  iam_instance_profile {
    name = local.profiles.spark
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
    tags          = { Name = "airlake-spark" }
  }
}

# Spot bị SCP chặn nên ASG chạy hoàn toàn On-Demand
resource "aws_autoscaling_group" "spark" {
  name                = "airlake-spark"
  min_size            = 0
  max_size            = 2
  desired_capacity    = 1
  vpc_zone_identifier = local.subnets
  launch_template {
    id      = aws_launch_template.spark.id
    version = "$Latest"
  }
  lifecycle {
    ignore_changes = [desired_capacity, min_size, max_size] # để lịch tắt đêm không bị apply ghi đè
  }
}

resource "aws_autoscaling_schedule" "night_off" {
  scheduled_action_name  = "night-off"
  autoscaling_group_name = aws_autoscaling_group.spark.name
  recurrence             = "0 23 * * *"
  time_zone              = "Asia/Ho_Chi_Minh"
  min_size               = 0
  max_size               = 0
  desired_capacity       = 0
}