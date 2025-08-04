# Local values for consistent naming
locals {
  common_tags = {
    Environment  = var.environment
    Project      = var.project_name
    ManagedBy    = "terraform"
    Architecture = "medallion"
  }

  bronze_bucket_name = "${var.project_name}-bronze-${var.environment}-${random_id.bucket_suffix.hex}"
  silver_bucket_name = "${var.project_name}-silver-${var.environment}-${random_id.bucket_suffix.hex}"
  gold_bucket_name   = "${var.project_name}-gold-${var.environment}-${random_id.bucket_suffix.hex}"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------------------
# VPC Configuration
# -----------------------------------------------------------------------------------------
module "vpc" {
  source                = "./modules/vpc/vpc"
  vpc_name              = "vpc"
  vpc_cidr_block        = "10.0.0.0/16"
  enable_dns_hostnames  = true
  enable_dns_support    = true
  internet_gateway_name = "vpc_igw"
}

# Public Subnets
module "public_subnets" {
  source = "./modules/vpc/subnets"
  name   = "public subnet"
  subnets = [
    {
      subnet = "10.0.1.0/24"
      az     = "${var.aws_region}a"
    },
    {
      subnet = "10.0.2.0/24"
      az     = "${var.aws_region}b"
    },
    {
      subnet = "10.0.3.0/24"
      az     = "${var.aws_region}c"
    }
  ]
  vpc_id                  = module.vpc.vpc_id
  map_public_ip_on_launch = true
}

# Private Subnets
module "private_subnets" {
  source = "./modules/vpc/subnets"
  name   = "private subnet"
  subnets = [
    {
      subnet = "10.0.6.0/24"
      az     = "${var.aws_region}a"
    },
    {
      subnet = "10.0.5.0/24"
      az     = "${var.aws_region}b"
    },
    {
      subnet = "10.0.4.0/24"
      az     = "${var.aws_region}c"
    }
  ]
  vpc_id                  = module.vpc.vpc_id
  map_public_ip_on_launch = false
}

# Public Route Table
module "public_rt" {
  source  = "./modules/vpc/route_tables"
  name    = "public route table"
  subnets = module.public_subnets.subnets[*]
  routes = [
    {
      cidr_block     = "0.0.0.0/0"
      gateway_id     = module.vpc.igw_id
      nat_gateway_id = ""
    }
  ]
  vpc_id = module.vpc.vpc_id
}

# Private Route Table
module "private_rt" {
  source  = "./modules/vpc/route_tables"
  name    = "private route table"
  subnets = module.private_subnets.subnets[*]
  routes  = []
  vpc_id  = module.vpc.vpc_id
}

# Security Groups
resource "aws_security_group" "vpc_endpoints" {
  count       = var.enable_vpc_endpoints ? 1 : 0
  name        = "${var.project_name}-vpc-endpoints-sg"
  description = "Security group for VPC endpoints"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-vpc-endpoints-sg"
  })
}

# VPC Endpoints
resource "aws_vpc_endpoint" "s3" {
  count             = var.enable_vpc_endpoints ? 1 : 0
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = var.route_table_ids

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-s3-endpoint"
  })
}

resource "aws_vpc_endpoint" "glue" {
  count              = var.enable_vpc_endpoints ? 1 : 0
  vpc_id             = module.vpc.vpc_id
  service_name       = "com.amazonaws.${var.aws_region}.glue"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = var.subnet_ids
  security_group_ids = [aws_security_group.vpc_endpoints[0].id]

  private_dns_enabled = true

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-glue-endpoint"
  })
}

# -----------------------------------------------------------------------------------------
# KMS Configuration
# -----------------------------------------------------------------------------------------

resource "aws_kms_key" "data_lake_key" {
  description             = "KMS key for data lake encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "data_lake_key_alias" {
  name          = "alias/${var.project_name}-data-lake-key"
  target_key_id = aws_kms_key.data_lake_key.key_id
}

# -----------------------------------------------------------------------------------------
# Athena Configuration
# -----------------------------------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results-${var.environment}-${random_id.bucket_suffix.hex}"
  tags = merge(local.common_tags, {
    Name = "Athena Query Results"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "athena_results_lifecycle"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}

resource "aws_athena_workgroup" "medallion_workgroup" {
  name = "${var.project_name}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = local.common_tags
}

# -----------------------------------------------------------------------------------------
# Config Configuration
# -----------------------------------------------------------------------------------------

resource "aws_config_configuration_recorder" "data_lake_config" {
  name     = "${var.project_name}-config-recorder"
  role_arn = aws_iam_role.config_role.arn

  recording_group {
    all_supported                 = false
    include_global_resource_types = false
    resource_types = [
      "AWS::S3::Bucket",
      "AWS::Glue::Job",
      "AWS::Glue::Crawler",
      "AWS::StepFunctions::StateMachine",
      "AWS::KMS::Key"
    ]
  }
}

resource "aws_config_delivery_channel" "data_lake_config" {
  name           = "${var.project_name}-config-delivery-channel"
  s3_bucket_name = aws_s3_bucket.config_bucket.bucket
}

resource "aws_s3_bucket" "config_bucket" {
  bucket = "${var.project_name}-config-${var.environment}-${random_id.bucket_suffix.hex}"
  tags = merge(local.common_tags, {
    Name = "AWS Config Bucket"
  })
}

resource "aws_iam_role" "config_role" {
  name = "${var.project_name}-config-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "config.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "config_role_policy" {
  role       = aws_iam_role.config_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWS_ConfigServiceRolePolicy"
}

# -----------------------------------------------------------------------------------------
# Secrets Manager
# -----------------------------------------------------------------------------------------

module "data_lake_secrets" {
  source                  = "./modules/secrets-manager"
  name                    = "${var.project_name}-secrets"
  description             = "Secrets for data lake operations"
  kms_key_id              = aws_kms_key.data_lake_key.arn
  recovery_window_in_days = 7
  secret_string = jsonencode({
    database_connection = "placeholder-connection-string"
    api_keys            = "placeholder-api-keys"
  })
}

# -----------------------------------------------------------------------------------------
# SNS Configuration
# -----------------------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name              = "${var.project_name}-alerts"
  kms_master_key_id = aws_kms_key.data_lake_key.arn

  tags = local.common_tags
}

# -----------------------------------------------------------------------------------------
# Cloudwatch Configuration
# -----------------------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "medallion_dashboard" {
  dashboard_name = "${var.project_name}-medallion-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", module.medallion_pipeline.arn],
            [".", "ExecutionsFailed", ".", "."],
            [".", "ExecutionTime", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Step Functions Execution Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.bronze_to_silver.name, "JobRunId", "ALL"],
            [".", "glue.driver.aggregate.numFailedTasks", ".", ".", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Glue Job Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.bronze.bucket, "StorageType", "StandardStorage"],
            [".", ".", ".", aws_s3_bucket.silver.bucket, ".", "."],
            [".", ".", ".", aws_s3_bucket.gold.bucket, ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "S3 Bucket Sizes"
          period  = 86400
        }
      }
    ]
  })
}

resource "aws_cloudwatch_metric_alarm" "bronze_to_silver_glue_job_failures" {
  alarm_name          = "${var.project_name}-bronze-to-silver-glue-job-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors bronze_to_silver glue job failures"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    JobName = aws_glue_job.bronze_to_silver.name
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "silver_to_gold_glue_job_failures" {
  alarm_name          = "${var.project_name}-silver-to-gold-glue-job-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors silver_to_gold glue job failures"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    JobName = aws_glue_job.silver_to_gold.name
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "s3_storage_cost" {
  alarm_name          = "${var.project_name}-s3-storage-cost"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = "86400"
  statistic           = "Maximum"
  threshold           = "100"
  alarm_description   = "This metric monitors S3 storage costs"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    Currency    = "USD"
    ServiceName = "AmazonS3"
  }

  tags = local.common_tags
}

# -----------------------------------------------------------------------------------------
# S3 Configuration
# -----------------------------------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket = local.bronze_bucket_name
  tags = merge(local.common_tags, {
    Name  = "Bronze Layer"
    Layer = "bronze"
  })
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "bronze_lifecycle"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }

    expiration {
      days = var.data_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_intelligent_tiering_configuration" "bronze_tiering" {
  bucket = aws_s3_bucket.bronze.id
  name   = "bronze-intelligent-tiering"

  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90
  }

  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "silver" {
  bucket = local.silver_bucket_name
  tags = merge(local.common_tags, {
    Name  = "Silver Layer"
    Layer = "silver"
  })
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "silver_lifecycle"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }

    expiration {
      days = var.data_retention_days
    }
  }
}

resource "aws_s3_bucket_intelligent_tiering_configuration" "silver_tiering" {
  bucket = aws_s3_bucket.silver.id
  name   = "silver-intelligent-tiering"

  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 125
  }

  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 365
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "gold" {
  bucket = local.gold_bucket_name
  tags = merge(local.common_tags, {
    Name  = "Gold Layer"
    Layer = "gold"
  })
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    id     = "gold_lifecycle"
    status = "Enabled"

    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 730
      storage_class = "GLACIER"
    }

    expiration {
      days = var.data_retention_days
    }
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------------------
# Glue Configuration
# -----------------------------------------------------------------------------------------

resource "aws_glue_catalog_database" "medallion_db" {
  name        = "${replace(var.project_name, "-", "_")}_catalog"
  description = "Data catalog for medallion architecture"

  catalog_id = data.aws_caller_identity.current.account_id
}

resource "aws_glue_catalog_table" "data_lineage" {
  name          = "data_lineage_tracking"
  database_name = aws_glue_catalog_database.medallion_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"  = "parquet"
    "compressionType" = "snappy"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.gold.bucket}/lineage/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "source_table"
      type = "string"
    }

    columns {
      name = "target_table"
      type = "string"
    }

    columns {
      name = "transformation_type"
      type = "string"
    }

    columns {
      name = "execution_time"
      type = "timestamp"
    }

    columns {
      name = "job_name"
      type = "string"
    }
  }
}

resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = [aws_kms_key.data_lake_key.arn]
      }
    ]
  })
}

resource "aws_glue_crawler" "bronze_crawler" {
  database_name = aws_glue_catalog_database.medallion_db.name
  name          = "${var.project_name}-bronze-crawler"
  role          = aws_iam_role.glue_service_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.bronze.bucket}/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  schedule = "cron(0 6 * * ? *)"

  tags = merge(local.common_tags, {
    Name  = "Bronze Crawler"
    Layer = "bronze"
  })
}

# Data catalog partitions for better performance
resource "aws_glue_partition_index" "bronze_partition_index" {
  database_name = aws_glue_catalog_database.medallion_db.name
  table_name    = "bronze_table"

  partition_index {
    index_name = "year-month-day-index"
    keys       = ["year", "month", "day"]
  }

  depends_on = [aws_glue_crawler.bronze_crawler]
}

resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.medallion_db.name
  name          = "${var.project_name}-silver-crawler"
  role          = aws_iam_role.glue_service_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.silver.bucket}/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  schedule = "cron(0 7 * * ? *)"

  tags = merge(local.common_tags, {
    Name  = "Silver Crawler"
    Layer = "silver"
  })
}

resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.medallion_db.name
  name          = "${var.project_name}-gold-crawler"
  role          = aws_iam_role.glue_service_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.gold.bucket}/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  schedule = "cron(0 8 * * ? *)"

  tags = merge(local.common_tags, {
    Name  = "Gold Crawler"
    Layer = "gold"
  })
}

resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project_name}-bronze-to-silver"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.bronze.bucket}/logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.bronze.bucket}/temp/"
    "--bronze-bucket"                    = aws_s3_bucket.bronze.bucket
    "--silver-bucket"                    = aws_s3_bucket.silver.bucket
  }

  execution_property {
    max_concurrent_runs = 2
  }

  max_retries = 1
  timeout     = 60

  tags = merge(local.common_tags, {
    Name = "Bronze to Silver ETL"
  })
}

resource "aws_glue_job" "silver_to_gold" {
  name         = "${var.project_name}-silver-to-gold"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.silver.bucket}/scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.silver.bucket}/logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.silver.bucket}/temp/"
    "--silver-bucket"                    = aws_s3_bucket.silver.bucket
    "--gold-bucket"                      = aws_s3_bucket.gold.bucket
  }

  execution_property {
    max_concurrent_runs = 2
  }

  max_retries = 1
  timeout     = 60

  tags = merge(local.common_tags, {
    Name = "Silver to Gold ETL"
  })
}

# -----------------------------------------------------------------------------------------
# Step Function Configuration
# -----------------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/stepfunctions/${var.project_name}-medallion-pipeline"
  retention_in_days = 30

  tags = local.common_tags
}

module "step_function_role" {
  source             = "./modules/iam"
  role_name          = "step_function_role"
  role_description   = "step_function_role"
  policy_name        = "step_function_policy"
  policy_description = "step_function_policy"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Principal": {
                  "Service": "states.amazonaws.com"
                },
                "Effect": "Allow",
                "Sid": ""
            }
        ]
    }
    EOF
  policy             = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                  "glue:StartJobRun",
                  "glue:GetJobRun",
                  "glue:GetJobRuns",
                  "glue:BatchStopJobRun",
                  "glue:StartCrawler",
                  "glue:GetCrawler",
                  "glue:GetCrawlerMetrics"
                ],
                "Resource": "*",
                "Effect": "Allow"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    }
    EOF
}

module "medallion_pipeline" {
  source                     = "./modules/step-function"
  name                       = "${var.project_name}-medallion-pipeline"
  role_arn                   = module.step_functions_role.arn
  log_destination            = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  log_include_execution_data = true
  log_level                  = "ALL"
  definition = templatefile("${path.module}/files/step-function-definition.json", {
    table_sanity_check_function_arn = module.table_sanity_check_function.arn
    extract_table_data_function_arn = module.extract_table_data_function.arn
    invalid_invoice_error_topic_arn = module.invalid_invoice_error_topic.topic_arn
    data_storage_failure_topic_arn  = module.data_storage_failure_topic.topic_arn
  })
}

resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${var.project_name}"
  retention_in_days = 30

  tags = local.common_tags
}

# -----------------------------------------------------------------------------------------
# EventBridge Configuration
# -----------------------------------------------------------------------------------------

module "mediaconvert_eventbridge_rule" {
  source           = "./modules/eventbridge"
  rule_name        = "${var.project_name}-daily-pipeline"
  rule_description = "Trigger medallion pipeline daily"
  event_pattern = jsonencode({
    source = [
      "aws.mediaconvert"
    ]
    detail-type = [
      "MediaConvert Job State Change"
    ]
  })
  target_id  = "StepFunctionsTarget"
  target_arn = module.medallion_pipeline.arn
}

# EventBridge rule for scheduled execution
resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "${var.project_name}-daily-pipeline"
  description         = "Trigger medallion pipeline daily"
  schedule_expression = "cron(0 5 * * ? *)" # Daily at 5 AM UTC

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "step_functions_target" {
  rule      = aws_cloudwatch_event_rule.daily_pipeline.name
  target_id = "StepFunctionsTarget"
  arn       = module.medallion_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${var.project_name}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          module.medallion_pipeline.arn
        ]
      }
    ]
  })
}

# CloudWatch Alarms for monitoring
resource "aws_cloudwatch_metric_alarm" "pipeline_failures" {
  alarm_name          = "${var.project_name}-pipeline-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors step function failures"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    StateMachineArn = module.medallion_pipeline.arn
  }

  tags = local.common_tags
}

# -----------------------------------------------------------------------------------------
# Data Quality Configuration
# -----------------------------------------------------------------------------------------

resource "aws_glue_data_quality_ruleset" "bronze_quality_rules" {
  name        = "${var.project_name}-bronze-quality-rules"
  description = "Data quality rules for bronze layer"
  ruleset     = "Rules = [ColumnCount > 0, Completeness \"id\" > 0.95, Uniqueness \"id\" > 0.99]"

  target_table {
    database_name = aws_glue_catalog_database.medallion_db.name
    table_name    = "bronze_table"
  }

  tags = local.common_tags
}
resource "aws_glue_data_quality_ruleset" "silver_quality_rules" {
  name        = "${var.project_name}-silver-quality-rules"
  description = "Data quality rules for silver layer"
  ruleset     = "Rules = [ColumnCount > 0, Completeness \"id\" > 0.95, Uniqueness \"id\" > 0.99]"

  target_table {
    database_name = aws_glue_catalog_database.medallion_db.name
    table_name    = "silver_table"
  }

  tags = local.common_tags
}

resource "aws_glue_data_quality_ruleset" "gold_quality_rules" {
  name        = "${var.project_name}-gold-quality-rules"
  description = "Data quality rules for gold layer"
  ruleset     = "Rules = [ColumnCount > 0, Completeness \"id\" > 0.95, Uniqueness \"id\" > 0.99]"

  target_table {
    database_name = aws_glue_catalog_database.medallion_db.name
    table_name    = "gold_table"
  }

  tags = local.common_tags
}

# -----------------------------------------------------------------------------------------
# Lakeformation Configuration
# -----------------------------------------------------------------------------------------

resource "aws_lakeformation_data_lake_settings" "data_lake_settings" {
  admins = [data.aws_caller_identity.current.arn]

  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current.arn
  }

  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current.arn
  }
}

resource "aws_lakeformation_resource" "bronze_location" {
  arn = aws_s3_bucket.bronze.arn
}

resource "aws_lakeformation_resource" "silver_location" {
  arn = aws_s3_bucket.silver.arn
}

resource "aws_lakeformation_resource" "gold_location" {
  arn = aws_s3_bucket.gold.arn
}

# -----------------------------------------------------------------------------------------
# Cloudtrail Configuration
# -----------------------------------------------------------------------------------------

resource "aws_s3_bucket" "audit_logs" {
  bucket = "${var.project_name}-audit-logs-${var.environment}-${random_id.bucket_suffix.hex}"
  tags = merge(local.common_tags, {
    Name = "Audit Logs"
  })
}

resource "aws_s3_bucket_policy" "audit_logs_policy" {
  bucket = aws_s3_bucket.audit_logs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AWSCloudTrailAclCheck"
        Effect = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action   = "s3:GetBucketAcl"
        Resource = aws_s3_bucket.audit_logs.arn
      },
      {
        Sid    = "AWSCloudTrailWrite"
        Effect = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.audit_logs.arn}/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      }
    ]
  })
}

resource "aws_cloudtrail" "data_lake_audit" {
  name           = "${var.project_name}-audit-trail"
  s3_bucket_name = aws_s3_bucket.audit_logs.bucket

  event_selector {
    read_write_type           = "All"
    include_management_events = true

    data_resource {
      type = "AWS::S3::Object"
      values = [
        "${aws_s3_bucket.bronze.arn}/*",
        "${aws_s3_bucket.silver.arn}/*",
        "${aws_s3_bucket.gold.arn}/*"
      ]
    }
  }

  tags = local.common_tags
}