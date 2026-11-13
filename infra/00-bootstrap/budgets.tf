resource "aws_budgets_budget" "monthly" {
  name         = "airlake_monthly"
  budget_type  = "COST"
  time_unit    = "MONTHLY"
  limit_amount = "100"
  limit_unit   = "USD"
  cost_types {
    include_credit = false
    include_refund = false
  }
  dynamic "notification" {
    for_each = [20, 50, 80]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "ABSOLUTE_VALUE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.alert_emails
    }
  }
}

resource "aws_budgets_budget" "daily" {
  name         = "airlake_daily"
  budget_type  = "COST"
  time_unit    = "DAILY"
  limit_amount = "5"
  limit_unit   = "USD"
  cost_types {
    include_credit = false
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.alert_emails
  }
}