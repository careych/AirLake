variable "alarm_on_missing" {
  type    = bool
  default = false   # false khi dev; đặt true khi demo để "không có dữ liệu" cũng báo động
}

locals {
  fresh_alarms = {
    "nhanchinh-warn" = { loc = "4946812", sid = "13502151", hours = 1 }
    "nhanchinh-crit" = { loc = "4946812", sid = "13502151", hours = 24 }
    "giaiphong-warn" = { loc = "4946813", sid = "13502154", hours = 1 }
    "giaiphong-crit" = { loc = "4946813", sid = "13502154", hours = 24 }
    "oceanpark-warn" = { loc = "6123215", sid = "14581375", hours = 2 }
    "oceanpark-crit" = { loc = "6123215", sid = "14581375", hours = 24 }
  }
}

resource "aws_cloudwatch_metric_alarm" "freshness" {
  for_each    = local.fresh_alarms
  alarm_name  = "airlake-pm25-${each.key}"
  namespace   = "AirLake"
  metric_name = "SensorAgeHours"
  dimensions = {
    location_id = each.value.loc
    sensors_id  = each.value.sid
    parameter   = "pm25"
  }
  statistic           = "Maximum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = each.value.hours
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = var.alarm_on_missing ? "breaching" : "notBreaching"
  alarm_actions       = [aws_sns_topic.ops.arn]
  ok_actions          = [aws_sns_topic.ops.arn]
}

resource "aws_cloudwatch_log_group" "app" {
  for_each          = toset(["kafka", "spark", "poller"])
  name              = "/airlake/${each.key}"
  retention_in_days = 7
}