data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "airlake-scheduler-stop-ec2"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

resource "aws_iam_role_policy" "scheduler_stop" {
  name = "stop-ec2"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "ec2:StopInstances"
      Resource  = "*"
      Condition = { StringEquals = { "ec2:ResourceTag/Project" = "airlake" } }
    }]
  })
}

resource "aws_scheduler_schedule" "stop_ec2" {
  name                         = "airlake-stop-ec2"
  schedule_expression          = "cron(0 23 * * ? *)"
  schedule_expression_timezone = "Asia/Ho_Chi_Minh"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      InstanceIds = concat(aws_instance.kafka[*].id, [aws_instance.superset.id])
    })
  }
}