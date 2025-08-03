# Local values for consistent naming
locals {
  common_tags = {
    Environment   = var.environment
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

# KMS Keys for encryption
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

# Cost optimization with intelligent tiering
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

# VPC Endpoints for secure access (optional but recommended for production)
resource "aws_vpc_endpoint" "s3" {
  count           = var.enable_vpc_endpoints ? 1 : 0
  vpc_id          = var.vpc_id
  service_name    = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  
  route_table_ids = var.route_table_ids
  
  tags = merge(local.common_tags, {
    Name = "${var.project_name}-s3-endpoint"
  })
}

resource "aws_vpc_endpoint" "glue" {
  count              = var.enable_vpc_endpoints ? 1 : 0
  vpc_id             = var.vpc_id
  service_name       = "com.amazonaws.${var.aws_region}.glue"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = var.subnet_ids
  security_group_ids = [aws_security_group.vpc_endpoints[0].id]
  
  private_dns_enabled = true
  
  tags = merge(local.common_tags, {
    Name = "${var.project_name}-glue-endpoint"
  })
}

resource "aws_security_group" "vpc_endpoints" {
  count       = var.enable_vpc_endpoints ? 1 : 0
  name        = "${var.project_name}-vpc-endpoints-sg"
  description = "Security group for VPC endpoints"
  vpc_id      = var.vpc_id
  
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

# Additional variables for VPC configuration
variable "enable_vpc_endpoints" {
  description = "Enable VPC endpoints for secure access"
  type        = bool
  default     = false
}

variable "vpc_id" {
  description = "VPC ID for VPC endpoints"
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_ids" {
  description = "Subnet IDs for VPC endpoints"
  type        = list(string)
  default     = []
}

variable "route_table_ids" {
  description = "Route table IDs for S3 VPC endpoint"
  type        = list(string)
  default     = []
}

# Athena for querying data
resource "aws_athena_workgroup" "medallion_workgroup" {
  name = "${var.project_name}-workgroup"
  
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics         = true
    result_configuration_updates_enabled = true
    
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
      
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key       = aws_kms_key.data_lake_key.arn
      }
    }
    
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
  
  tags = local.common_tags
}

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

# Data catalog partitions for better performance
resource "aws_glue_partition_index" "bronze_partition_index" {
  database_name = aws_glue_catalog_database.medallion_db.name
  table_name    = "bronze_table" # This would be created by the crawler
  
  partition_index {
    index_name = "year-month-day-index"
    keys       = ["year", "month", "day"]
  }
  
  depends_on = [aws_glue_crawler.bronze_crawler]
}

# AWS Config for compliance monitoring
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

# AWS Secrets Manager for storing sensitive configurations
resource "aws_secretsmanager_secret" "data_lake_secrets" {
  name                    = "${var.project_name}-secrets"
  description            = "Secrets for data lake operations"
  kms_key_id             = aws_kms_key.data_lake_key.arn
  recovery_window_in_days = 7
  
  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "data_lake_secrets" {
  secret_id = aws_secretsmanager_secret.data_lake_secrets.id
  secret_string = jsonencode({
    database_connection = "placeholder-connection-string"
    api_keys           = "placeholder-api-keys"
  })
}

# CloudWatch Dashboard for monitoring
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
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.medallion_pipeline.arn],
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

# Additional CloudWatch Alarms
resource "aws_cloudwatch_metric_alarm" "glue_job_failures" {
  alarm_name          = "${var.project_name}-glue-job-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors glue job failures"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    JobName = aws_glue_job.bronze_to_silver.name
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
  threshold           = "100" # $100 threshold
  alarm_description   = "This metric monitors S3 storage costs"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    Currency    = "USD"
    ServiceName = "AmazonS3"
  }
  
  tags = local.common_tags
}

# Data lineage tracking with AWS Glue Data Catalog
resource "aws_glue_catalog_table" "data_lineage" {
  name          = "data_lineage_tracking"
  database_name = aws_glue_catalog_database.medallion_db.name
  
  table_type = "EXTERNAL_TABLE"
  
  parameters = {
    "classification" = "parquet"
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

resource "aws_kms_alias" "data_lake_key_alias" {
  name          = "alias/${var.project_name}-data-lake-key"
  target_key_id = aws_kms_key.data_lake_key.key_id
}

data "aws_caller_identity" "current" {}

# S3 Buckets for Medallion Architecture

# Bronze Layer - Raw Data
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

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Silver Layer - Cleaned and Validated Data
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

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Gold Layer - Business-Ready Aggregated Data
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

# IAM Roles and Policies

# Glue Service Role
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

# Glue Data Catalog Database
resource "aws_glue_catalog_database" "medallion_db" {
  name        = "${replace(var.project_name, "-", "_")}_catalog"
  description = "Data catalog for medallion architecture"
  
  catalog_id = data.aws_caller_identity.current.account_id
}

# Glue Crawlers for each layer
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
  
  schedule = "cron(0 6 * * ? *)" # Daily at 6 AM
  
  tags = merge(local.common_tags, {
    Name  = "Bronze Crawler"
    Layer = "bronze"
  })
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
  
  schedule = "cron(0 7 * * ? *)" # Daily at 7 AM
  
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
  
  schedule = "cron(0 8 * * ? *)" # Daily at 8 AM
  
  tags = merge(local.common_tags, {
    Name  = "Gold Crawler"
    Layer = "gold"
  })
}

# Step Functions for orchestration
resource "aws_iam_role" "step_functions_role" {
  name = "${var.project_name}-step-functions-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

resource "aws_iam_role_policy" "step_functions_policy" {
  name = "${var.project_name}-step-functions-policy"
  role = aws_iam_role.step_functions_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlerMetrics"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# Sample Glue ETL Jobs
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
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.bronze.bucket}/logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                         = "s3://${aws_s3_bucket.bronze.bucket}/temp/"
    "--bronze-bucket"                   = aws_s3_bucket.bronze.bucket
    "--silver-bucket"                   = aws_s3_bucket.silver.bucket
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
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.silver.bucket}/logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                         = "s3://${aws_s3_bucket.silver.bucket}/temp/"
    "--silver-bucket"                   = aws_s3_bucket.silver.bucket
    "--gold-bucket"                     = aws_s3_bucket.gold.bucket
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

# Step Functions State Machine for orchestration
resource "aws_sfn_state_machine" "medallion_pipeline" {
  name     = "${var.project_name}-medallion-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn
  
  definition = jsonencode({
    Comment = "Medallion Architecture Data Pipeline"
    StartAt = "StartBronzeCrawler"
    States = {
      StartBronzeCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.bronze_crawler.name
        }
        Next = "WaitForBronzeCrawler"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 3
            BackoffRate = 2.0
          }
        ]
      }
      WaitForBronzeCrawler = {
        Type = "Wait"
        Seconds = 60
        Next = "CheckBronzeCrawlerStatus"
      }
      CheckBronzeCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.bronze_crawler.name
        }
        Next = "IsBronzeCrawlerComplete"
      }
      IsBronzeCrawlerComplete = {
        Type = "Choice"
        Choices = [
          {
            Variable = "$.Crawler.State"
            StringEquals = "READY"
            Next = "StartBronzeToSilverJob"
          }
        ]
        Default = "WaitForBronzeCrawler"
      }
      StartBronzeToSilverJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_to_silver.name
        }
        Next = "StartSilverCrawler"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
      }
      StartSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_crawler.name
        }
        Next = "WaitForSilverCrawler"
      }
      WaitForSilverCrawler = {
        Type = "Wait"
        Seconds = 60
        Next = "CheckSilverCrawlerStatus"
      }
      CheckSilverCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_crawler.name
        }
        Next = "IsSilverCrawlerComplete"
      }
      IsSilverCrawlerComplete = {
        Type = "Choice"
        Choices = [
          {
            Variable = "$.Crawler.State"
            StringEquals = "READY"
            Next = "StartSilverToGoldJob"
          }
        ]
        Default = "WaitForSilverCrawler"
      }
      StartSilverToGoldJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_to_gold.name
        }
        Next = "StartGoldCrawler"
        Retry = [
          {
            ErrorEquals = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
      }
      StartGoldCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.gold_crawler.name
        }
        Next = "PipelineComplete"
      }
      PipelineComplete = {
        Type = "Succeed"
      }
    }
  })
  
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                 = "ALL"
  }
  
  tags = local.common_tags
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/stepfunctions/${var.project_name}-medallion-pipeline"
  retention_in_days = 30
  
  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${var.project_name}"
  retention_in_days = 30
  
  tags = local.common_tags
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
  arn       = aws_sfn_state_machine.medallion_pipeline.arn
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
          aws_sfn_state_machine.medallion_pipeline.arn
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
    StateMachineArn = aws_sfn_state_machine.medallion_pipeline.arn
  }
  
  tags = local.common_tags
}

# SNS Topic for alerts
resource "aws_sns_topic" "alerts" {
  name              = "${var.project_name}-alerts"
  kms_master_key_id = aws_kms_key.data_lake_key.arn
  
  tags = local.common_tags
}

# Data Quality with AWS Glue Data Quality
resource "aws_glue_data_quality_ruleset" "silver_quality_rules" {
  name        = "${var.project_name}-silver-quality-rules"
  description = "Data quality rules for silver layer"
  ruleset     = "Rules = [ColumnCount > 0, Completeness \"id\" > 0.95, Uniqueness \"id\" > 0.99]"
  
  target_table {
    database_name = aws_glue_catalog_database.medallion_db.name
    table_name    = "silver_table" # This would be dynamically created by crawlers
  }
  
  tags = local.common_tags
}

# Additional monitoring and governance resources

# AWS Lake Formation for data lake governance
resource "aws_lakeformation_data_lake_settings" "data_lake_settings" {
  admins = [data.aws_caller_identity.current.arn]
  
  default_database_permissions {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current.arn
  }
  
  default_table_permissions {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current.arn
  }
  
  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current.arn
  }
  
  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current.arn
  }
}

# Register S3 locations with Lake Formation
resource "aws_lakeformation_resource" "bronze_location" {
  arn = aws_s3_bucket.bronze.arn
}

resource "aws_lakeformation_resource" "silver_location" {
  arn = aws_s3_bucket.silver.arn
}

resource "aws_lakeformation_resource" "gold_location" {
  arn = aws_s3_bucket.gold.arn
}

# CloudTrail for audit logging
resource "aws_cloudtrail" "data_lake_audit" {
  name           = "${var.project_name}-audit-trail"
  s3_bucket_name = aws_s3_bucket.audit_logs.bucket
  
  event_selector {
    read_write_type                 = "All"
    include_management_events       = true
    
    data_resource {
      type   = "AWS::S3::Object"
      values = [
        "${aws_s3_bucket.bronze.arn}/*",
        "${aws_s3_bucket.silver.arn}/*",
        "${aws_s3_bucket.gold.arn}/*"
      ]
    }
  }
  
  tags = local.common_tags
}

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