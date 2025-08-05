variable "topic_name" {}
variable "subscriptions" {
  type = list(object({
    protocol = string
    endpoint = string
  }))
}
variable "kms_key_id" {
  description = "KMS Key ID for encrypting SNS topic"
  type        = string
}
