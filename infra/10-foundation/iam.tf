data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  for_each           = toset(["kafka", "spark", "trino", "superset"])
  name               = "airlake-ec2-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  for_each   = aws_iam_role.ec2
  role       = each.value.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "cwagent" {
  for_each   = aws_iam_role.ec2
  role       = each.value.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_instance_profile" "ec2" {
  for_each = aws_iam_role.ec2
  name     = each.value.name
  role     = each.value.name
}

# Spark: ghi lake, đọc artifact, quản lý bảng Iceberg trong Glue, đẩy metric độ tươi
data "aws_iam_policy_document" "spark" {
  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.this["lake"].arn}/*",
      "${aws_s3_bucket.this["artifacts"].arn}/*",
    ]
  }
  statement {
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.this["lake"].arn,
      aws_s3_bucket.this["artifacts"].arn,
    ]
  }
  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${local.acct}:catalog",
      "arn:aws:glue:${var.region}:${local.acct}:database/airlake_*",
      "arn:aws:glue:${var.region}:${local.acct}:table/airlake_*/*",
    ]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AirLake"]
    }
  }
}

resource "aws_iam_role_policy" "spark" {
  name   = "spark-data"
  role   = aws_iam_role.ec2["spark"].id
  policy = data.aws_iam_policy_document.spark.json
}

# Trino: đọc lake S3 và metadata Iceberg từ AWS Glue Catalog
data "aws_iam_policy_document" "trino" {
  statement {
    actions = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.this["lake"].arn,
      "${aws_s3_bucket.this["lake"].arn}/*",
    ]
  }
  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${local.acct}:catalog",
      "arn:aws:glue:${var.region}:${local.acct}:database/airlake_*",
      "arn:aws:glue:${var.region}:${local.acct}:table/airlake_*/*",
    ]
  }
}

resource "aws_iam_role_policy" "trino" {
  name   = "trino-data-access"
  role   = aws_iam_role.ec2["trino"].id
  policy = data.aws_iam_policy_document.trino.json
}
