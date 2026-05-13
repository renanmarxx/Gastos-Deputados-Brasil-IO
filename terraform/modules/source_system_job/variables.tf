#-----------------------
# CI Run
#-----------------------
variable "environment" {
  type = string
}

#-----------------------
# Job Cluster
#-----------------------
variable "policy_id" {
  type = number
}

variable "max_workers" {
  type    = number
  default = 5
}

variable "min_workers" {
  type    = number
  default = 1
}

variable "custom_tags" {
  type    = map(any)
  default = { "component" = "PENDING" }
}

variable "spark_configs" {
  type    = map(any)
  default = { "fs.s3a.server-side-encryption-algorithm" : "PENDING" }
}

variable "spark_env_vars" {
  type    = map(any)
  default = { "PYSPARK_PYTHON" : "/databricks/python3/bin/python3" }
}

variable "cluster_driver" {
  type    = string
  default = "PENDING"
}

variable "runtime_engine" {
  type    = string
  default = "STANDARD"
}

variable "num_job_clusters" {
  type    = number
  default = 1
}

variable "data_security_mode" {
  type    = string
  default = "SINGLE_USER"
}

#-----------------------
# AWS Settings
#-----------------------
variable "zone_id" {
  type    = string
  default = "us-east-1-d"
}

variable "availability" {
  type    = string
  default = "SPOT_WITH_FALLBACK"
}

variable "first_on_demand" {
  type    = number
  default = 1
}

variable "spot_bid_price_percent" {
  type    = number
  default = 100
}

#-----------------------
# Workflow tasks
#-----------------------
variable "workspace_path" {
  type = string
}

variable "pypi_libraries" {
  type    = list(any)
  default = ["pyyaml", "unidecode"]
}

variable "datalake_library_whl" {
  type    = string
  default = "PENDING"
}

variable "data_contracts_library_whl" {
  type    = string
  default = "PENDING"
}

variable "notebook_source" {
  type    = string
  default = "WORKSPACE"
}

#-----------------------
# Workflow
#-----------------------
variable "job_schedule" {
  type = string
}
variable "workflow_name" {
  type = string
}

variable "workflow_tasks" {
  type = list(any)
  default = [
    {
      name             = "MOVE_RAW_FILES",
      depends_on       = [],
      entrypoint_file  = "move_raw_files.py",
      extra_parameters = []
    },
    {
      name             = "CREATE_ENHANCE_TABLE",
      depends_on       = ["MOVE_RAW_FILES"],
      entrypoint_file  = "create_enhance_tables.py",
      extra_parameters = []
    }
  ]
}

variable "custodian_ad_groups" {
  type = map(any)
  default = {
    "prod" = "PENDING",
    "devl" = "PENDING",
    "qual" = "PENDING",
  }
}

variable "timezone_id" {
  type    = string
  default = "UTC"
}

variable "pause_status" {
  type = map(any)
  default = {
    devl = "PAUSED",
    qual = "PAUSED",
    prod = "PAUSED"
  }
}

variable "max_concurrent_runs" {
  type    = number
  default = 1
}

variable "fail_email_notifications" {
  type = map(any)
  default = {
    prod = ["renanmarx@icloud.com"]
  }
}

#-----------------------
# Source Variables
#-----------------------
variable "system" { type = string }
variable "country" { type = string }
variable "data_contract" { type = string }

variable "framework" {
  type    = string
  default = "PENDING"
}

variable "entrypoints_dir" {
  type    = string
  default = "PENDING"
}

variable "developers_email" {
  type    = list(string)
  default = ["renanmarx@icloud.com"]
}

locals {
  workflow_name        = title("${var.workflow_name} - ${var.environment}")
  workspace_folder     = "${var.workspace_path}/${var.country}/${var.framework}/${var.entrypoints_dir}"
  instance_profile_arn = "arn:aws:iam::ACCOUNT_NUMBER:instance_profile/${var.custodian_ad_groups[lower(var.environment)]}"
  base_parameters      = ["--environment", var.environment, "--data_contracts", var.data_contract]
}
