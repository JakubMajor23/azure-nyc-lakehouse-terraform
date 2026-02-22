variable "project_name" {
  type    = string
  default = "nyctaxi"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "location" {
  type    = string
  default = "polandcentral"
}

variable "sql_admin_login" {
  type    = string
  default = "sqladminuser"
}

variable "sql_admin_password" {
  type      = string
  sensitive = true
}

variable "tags" {
  type = map(string)
  default = {
    Project     = "NYC-Taxi-Data-Warehouse"
    Environment = "dev"
    ManagedBy   = "Terraform"
    CostCenter  = "StudentAccount"
  }
}
