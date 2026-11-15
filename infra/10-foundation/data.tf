locals {
  buckets = ["lake", "athena-results", "artifacts"]
}

resource "aws_s3_bucket" "this" {
  for_each = toset(local.buckets)
  bucket   = "airlake-${local.acct}-${each.key}"
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Chỉ bucket kết quả Athena có vòng đời. KHÔNG đặt lifecycle xoá file trong lake/ (Iceberg tự quản lý snapshot và compaction)
resource "aws_s3_bucket_lifecycle_configuration" "athena" {
  bucket = aws_s3_bucket.this["athena-results"].id
  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_glue_catalog_database" "layer" {
  for_each = toset(["bronze", "silver", "gold"])
  name     = "airlake_${each.key}"
}

# Workgroup có giới hạn 10 GB quét mỗi truy vấn (~$0,05) để tránh truy vấn lỡ tay
resource "aws_athena_workgroup" "main" {
  name = "airlake"
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 10737418240
    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena-results"].bucket}/"
    }
  }
}

# Terraform chỉ tạo vỏ tham số, giá trị nhập tay để không lọt vào state
resource "aws_ssm_parameter" "secret" {
  for_each = toset(["openaq_key", "telegram_token"])
  name     = "/airlake/${each.key}"
  type     = "SecureString"
  value    = "REPLACE_ME"
  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_sns_topic" "ops" {
  name = "airlake-ops-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.ops.arn
  protocol  = "email"
  endpoint  = each.value
}