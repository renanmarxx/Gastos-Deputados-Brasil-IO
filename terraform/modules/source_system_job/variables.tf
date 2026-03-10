#-----------------------
# CI Run
#-----------------------
variable "environment" {}

#-----------------------
# Job Cluster
#-----------------------
variable "policy_id" {
    default = "PENDING"
}

variable "max_workers" {
    default = 5
}

variable "min_workers {
    default = 1
}

variable "custom_tags" {
    default = { "component" = "PENDING" }
}

variable "spark_configs" {
    default = { "fs.s3a.server-side-encryption-algorithm" : "PENDING" }
}

variable "spark_env_vars" {
    default = { "PYSPARK_PYTHON" : "/databricks/python3/bin/python3" }
}

variable "cluster_driver" {
    default = "PENDING"
}

variable "runtime_engine" {
    default = "STANDARD"
}

variable "num_job_clusters" {
    default = 1
}

variable "data_security_mode" {
    default = "SINGLE_USER"
}

#-----------------------
# AWS Settings
#-----------------------
variable "zone_id" {
    default = "us-east-1-d"
}

variable "availability" {
    default = "SPOT_WITH_FALLBACK"
}

variable "first_on_demand" {
    default = 1
}

variable "spot_bid_price_percent" {
    default = 100
}

#-----------------------
# Workflow tasks
#-----------------------
variable "workspace_path" {}

variable "pypi_libraries" {
    default = ["pyyaml", "unidecode"]
}

variable "datalake_library_whl" {
    default = "PENDING"
}

variable "data_contracts_library_whl" {
    default = "PENDING"
}

variable "notebook_source" {
    default = "WORKSPACE"
}

#-----------------------
# Workflow
#-----------------------
variable "job_schedule" {}
variable "workflow_name" {}

variable "extra_permissions" {
    default = []
}

variable "workflow_tasks" {
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
    default = {
        "prod" = "PENDING",
        "devl" = "PENDING",
        "qual" = "PENDING",
    }
}

variable "timezone_id" {
    default = "UTC"
}

variable "pause_status" {
    default = {
        devl = "PAUSED",
        qual = "PAUSED",
        prod = "PAUSED"
    }
}

variable "max_concurrent_runs" {
    default = 1
}

variable "fail_email_notifications" {
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
    default = "PENDING" #which folder?
}

variable "entrypoints_dir" {
    default = "PENDING" #which folder?
}

variable "developers_email" {
    type = list(string)
    default = ["renanmarx@icloud.com"]
}

locals {
    workflow_name = title("${var.workflow_name} - $(var.environment)")
    workspace_folder = "${var.workspace_path}/${var.country}/${var.framework}/${var.entrypoints_dir}"
    access_groups = concat(var.extra_permissions, [{permission =  "CAN_MANAGE_RUN", name = var.custodian_ad_groups[lower(var.environment)]}])
    instance_profile_arn = "arn:aws:iam::ACCOUNT_NUMBER:instance_profile/${var.custodian_ad_groups[lower(var.environment)]}"
    base_parameters = ["--environment", var.environment, "--data_contracts", var.data_contract]
}