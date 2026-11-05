# infra/20-compute/outputs.tf

output "kafka_instance_ids" {
  description = "Danh sách ID các EC2 instance chạy Kafka broker"
  value       = aws_instance.kafka[*].id
}

output "kafka_private_ips" {
  description = "Địa chỉ IP nội bộ của các Kafka broker"
  value       = local.active_kafka_ips
}

output "kafka_bootstrap_servers" {
  description = "Chuỗi kết nối bootstrap servers cho Kafka"
  value       = join(",", [for ip in local.active_kafka_ips : "${ip}:9092"])
}

output "trino_asg_name" {
  description = "Tên Auto Scaling Group của cụm Trino"
  value       = aws_autoscaling_group.trino.name
}

output "trino_port" {
  description = "Cổng kết nối HTTP/JDBC của Trino"
  value       = 8080
}
