output "vpc_id" { value = aws_vpc.main.id }
output "subnet_ids" { value = aws_subnet.public[*].id }
output "sg_ids" { value = { for k, v in aws_security_group.this : k => v.id } }
output "profile_names" { value = { for k, v in aws_iam_instance_profile.ec2 : k => v.name } }
output "lake_bucket" { value = aws_s3_bucket.this["lake"].bucket }
output "ops_topic_arn" { value = aws_sns_topic.ops.arn }