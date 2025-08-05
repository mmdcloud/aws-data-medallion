resource "aws_kms_key" "key" {
  description             = var.description
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = var.enable_key_rotation
  policy                  = var.policy
}

resource "aws_kms_alias" "alias" {
  name          = var.alias_name
  target_key_id = aws_kms_key.key.key_id
}
