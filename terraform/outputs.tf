# Outputs
output "bronze_bucket_name" {
  description = "Name of the Bronze layer S3 bucket"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  description = "Name of the Silver layer S3 bucket"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  description = "Name of the Gold layer S3 bucket"
  value       = aws_s3_bucket.gold.bucket
}

output "glue_database_name" {
  description = "Name of the Glue catalog database"
  value       = aws_glue_catalog_database.medallion_db.name
}

output "step_function_arn" {
  description = "ARN of the Step Functions state machine"
  value       = module.medallion_pipeline.arn
}

output "kms_key_arn" {
  description = "ARN of the KMS key used for encryption"
  value       = aws_kms_key.data_lake_key.arn
}

# Additional outputs for monitoring and operations
output "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.medallion_workgroup.name
}

output "cloudwatch_dashboard_url" {
  description = "URL to the CloudWatch dashboard"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.medallion_dashboard.dashboard_name}"
}

output "secrets_manager_secret_arn" {
  description = "ARN of the Secrets Manager secret"
  value       = module.data_lake_secrets.arn
  sensitive   = true
}

output "config_bucket_name" {
  description = "Name of the AWS Config bucket"
  value       = aws_s3_bucket.config_bucket.bucket
}

output "audit_trail_arn" {
  description = "ARN of the CloudTrail for audit logging"
  value       = aws_cloudtrail.data_lake_audit.arn
}