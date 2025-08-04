resource "aws_sfn_state_machine" "state_machine" {
  name       = var.name
  role_arn   = var.role_arn
  definition = var.definition
  logging_configuration {
    level                  = var.log_level
    include_execution_data = var.log_include_execution_data
    log_destination        = var.log_destination
  }
}
