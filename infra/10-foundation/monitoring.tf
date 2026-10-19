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

resource "aws_cloudwatch_dashboard" "airlake_operations" {
  dashboard_name = "AirLake-Operations"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric",
        x      = 0,
        y      = 0,
        width  = 15,
        height = 8,
        properties = {
          title   = "Độ tươi dữ liệu trạm quan trắc (Sensor Age in Hours - PM2.5)",
          region  = var.region,
          stat    = "Maximum",
          period  = 300,
          view    = "timeSeries",
          stacked = false,
          metrics = [
            ["AirLake", "SensorAgeHours", "location_id", "4946812", "sensors_id", "13502151", "parameter", "pm25", { "label" : "Nhân Chính (4946812)", "color" : "#1f77b4" }],
            ["AirLake", "SensorAgeHours", "location_id", "4946813", "sensors_id", "13502154", "parameter", "pm25", { "label" : "Giải Phóng (4946813)", "color" : "#ff7f0e" }],
            ["AirLake", "SensorAgeHours", "location_id", "6123215", "sensors_id", "14581375", "parameter", "pm25", { "label" : "OceanPark (6123215)", "color" : "#2ca02c" }]
          ],
          yAxis = {
            left = {
              min   = 0,
              label = "Tuổi dữ liệu (Giờ)"
            }
          },
          annotations = {
            horizontal = [
              {
                value = 1,
                label = "Ngưỡng Cũ / Gián đoạn (1h)",
                color = "#ffbb00"
              },
              {
                value = 24,
                label = "Ngưỡng Cảm biến Chết (24h)",
                color = "#d62728"
              }
            ]
          }
        }
      },
      {
        type   = "alarm",
        x      = 15,
        y      = 0,
        width  = 9,
        height = 8,
        properties = {
          title  = "Trạng thái cảnh báo cảm biến (Freshness Alarms)",
          alarms = [for a in aws_cloudwatch_metric_alarm.freshness : a.arn]
        }
      },
      {
        type   = "metric",
        x      = 0,
        y      = 8,
        width  = 12,
        height = 6,
        properties = {
          title   = "Lưu lượng log ứng dụng (/airlake/*)",
          region  = var.region,
          stat    = "Sum",
          period  = 300,
          view    = "timeSeries",
          metrics = [
            ["AWS/Logs", "IncomingLogEvents", "LogGroupName", "/airlake/kafka", { "label" : "Kafka Logs", "color" : "#1f77b4" }],
            ["AWS/Logs", "IncomingLogEvents", "LogGroupName", "/airlake/spark", { "label" : "Spark Logs", "color" : "#2ca02c" }],
            ["AWS/Logs", "IncomingLogEvents", "LogGroupName", "/airlake/poller", { "label" : "Poller Logs", "color" : "#9467bd" }]
          ]
        }
      },
      {
        type   = "metric",
        x      = 12,
        y      = 8,
        width  = 12,
        height = 6,
        properties = {
          title   = "Hoạt động đẩy metric (CloudWatch PutMetricData)",
          region  = var.region,
          stat    = "SampleCount",
          period  = 300,
          view    = "timeSeries",
          metrics = [
            ["AirLake", "SensorAgeHours", "location_id", "4946812", "sensors_id", "13502151", "parameter", "pm25", { "label" : "Nhân Chính Count" }],
            ["AirLake", "SensorAgeHours", "location_id", "4946813", "sensors_id", "13502154", "parameter", "pm25", { "label" : "Giải Phóng Count" }],
            ["AirLake", "SensorAgeHours", "location_id", "6123215", "sensors_id", "14581375", "parameter", "pm25", { "label" : "OceanPark Count" }]
          ]
        }
      }
    ]
  })
}
