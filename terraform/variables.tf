# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "medallion-data-platform"
}

variable "data_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 2555 # ~7 years
}