resource "aws_lb" "web" {
  name               = "airlake-alb"
  load_balancer_type = "application"
  subnets            = local.subnets
  security_groups    = [local.sg.alb]
}

resource "aws_lb_target_group" "superset" {
  name        = "airlake-superset"
  port        = 8088
  protocol    = "HTTP"
  vpc_id      = local.f.vpc_id
  target_type = "instance"
  health_check {
    path    = "/health"
    matcher = "200"
  }
}

resource "aws_lb_target_group_attachment" "superset" {
  target_group_arn = aws_lb_target_group.superset.arn
  target_id        = aws_instance.superset.id
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.web.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.superset.arn
  }
}

resource "aws_wafv2_web_acl" "web" {
  name  = "airlake-waf"
  scope = "REGIONAL"
  default_action {
    allow {}
  }
  rule {
    name     = "aws-common"
    priority = 1
    override_action {
      none {}
    }
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "aws-common"
      sampled_requests_enabled   = true
    }
  }
  rule {
    name     = "rate-limit"
    priority = 2
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = 2000
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "rate-limit"
      sampled_requests_enabled   = true
    }
  }
  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "airlake-waf"
    sampled_requests_enabled   = true
  }
}

resource "aws_wafv2_web_acl_association" "alb" {
  resource_arn = aws_lb.web.arn
  web_acl_arn  = aws_wafv2_web_acl.web.arn
}

output "alb_dns" {
  value = aws_lb.web.dns_name
}