resource "aws_security_group" "this" {
  for_each = toset(["alb", "superset", "trino", "kafka", "spark"])
  name     = "airlake-${each.key}"
  vpc_id   = aws_vpc.main.id
  tags     = { Name = "airlake-${each.key}" }
}

resource "aws_vpc_security_group_egress_rule" "all" {
  for_each          = aws_security_group.this
  security_group_id = each.value.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

locals {
  # from = CIDR hoặc tên SG nguồn
  ingress = {
    alb_http            = { to = "alb", from = "0.0.0.0/0", port = 80 }
    superset_from_alb   = { to = "superset", from = "alb", port = 8088 }
    trino_from_superset = { to = "trino", from = "superset", port = 8080 }
    trino_self          = { to = "trino", from = "trino", port = 8080 }
    kafka_from_spark    = { to = "kafka", from = "spark", port = 9092 }
    kafka_self_9092     = { to = "kafka", from = "kafka", port = 9092 }
    kafka_self_9093     = { to = "kafka", from = "kafka", port = 9093 }
  }
}

resource "aws_vpc_security_group_ingress_rule" "this" {
  for_each                     = local.ingress
  security_group_id            = aws_security_group.this[each.value.to].id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = can(cidrhost(each.value.from, 0)) ? each.value.from : null
  referenced_security_group_id = can(cidrhost(each.value.from, 0)) ? null : aws_security_group.this[each.value.from].id
}