# infra/20-compute/monitoring.tf
# Giám sát sức khỏe hạ tầng tính toán EC2 và Auto Scaling Group

resource "aws_cloudwatch_metric_alarm" "kafka_status" {
  count               = var.kafka_count
  alarm_name          = "airlake-kafka-${count.index + 1}-status-check"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "StatusCheckFailed"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "Cảnh báo khi Kafka broker ${count.index + 1} gặp sự cố kiểm tra trạng thái"
  alarm_actions       = [local.f.ops_topic_arn]
  ok_actions          = [local.f.ops_topic_arn]
  treat_missing_data  = "missing"
  dimensions = {
    InstanceId = aws_instance.kafka[count.index].id
  }
}

resource "aws_cloudwatch_metric_alarm" "kafka_cpu" {
  count               = var.kafka_count
  alarm_name          = "airlake-kafka-${count.index + 1}-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Cảnh báo khi Kafka broker ${count.index + 1} có mức sử dụng CPU vượt quá 85%"
  alarm_actions       = [local.f.ops_topic_arn]
  ok_actions          = [local.f.ops_topic_arn]
  treat_missing_data  = "missing"
  dimensions = {
    InstanceId = aws_instance.kafka[count.index].id
  }
}

resource "aws_cloudwatch_metric_alarm" "superset_status" {
  alarm_name          = "airlake-superset-status-check"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "StatusCheckFailed"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "Cảnh báo khi máy chủ Superset gặp sự cố kiểm tra trạng thái"
  alarm_actions       = [local.f.ops_topic_arn]
  ok_actions          = [local.f.ops_topic_arn]
  treat_missing_data  = "missing"
  dimensions = {
    InstanceId = aws_instance.superset.id
  }
}

resource "aws_cloudwatch_metric_alarm" "superset_cpu" {
  alarm_name          = "airlake-superset-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Cảnh báo khi Superset có mức sử dụng CPU vượt quá 85%"
  alarm_actions       = [local.f.ops_topic_arn]
  ok_actions          = [local.f.ops_topic_arn]
  treat_missing_data  = "missing"
  dimensions = {
    InstanceId = aws_instance.superset.id
  }
}

resource "aws_cloudwatch_metric_alarm" "spark_cpu" {
  alarm_name          = "airlake-spark-asg-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Cảnh báo khi nhóm máy chủ Spark Auto Scaling Group có CPU vượt 85%"
  alarm_actions       = [local.f.ops_topic_arn]
  ok_actions          = [local.f.ops_topic_arn]
  treat_missing_data  = "missing"
  dimensions = {
    AutoScalingGroupName = aws_autoscaling_group.spark.name
  }
}

resource "aws_cloudwatch_metric_alarm" "trino_cpu" {
  alarm_name          = "airlake-trino-asg-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Cảnh báo khi nhóm máy chủ Trino Auto Scaling Group có CPU vượt 85%"
  alarm_actions       = [local.f.ops_topic_arn]
  ok_actions          = [local.f.ops_topic_arn]
  treat_missing_data  = "missing"
  dimensions = {
    AutoScalingGroupName = aws_autoscaling_group.trino.name
  }
}
