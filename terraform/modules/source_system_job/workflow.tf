data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_job" "tables_creation_job" {
  name                = local.workflow_name
  max_concurrent_runs = var.max_concurrent_runs

  schedule {
    quartz_cron_expression = var.job_schedule
    timezone_id            = var.timezone_id
    pause_status           = var.pause_status[var.environment]
  }

  email_notifications {
    on_failure = lookup(
      var.fail_email_notifications,
      var.environment,
      var.developers_email
    )
  }

  dynamic "job_cluster" {
    for_each = range(var.num_job_clusters)
    iterator = num
    content {
      job_cluster_key = "job_cluster_${tostring(num.key)}"

      new_cluster {
        spark_version       = data.databricks_spark_version.latest_lts.id
        node_type_id        = var.cluster_driver
        data_security_mode  = var.data_security_mode
        policy_id           = var.policy_id
        runtime_engine      = var.runtime_engine
        spark_conf          = var.spark_configs
        custom_tags         = var.custom_tags
        spark_env_vars      = var.spark_env_vars
        enable_elastic_disk = true

        aws_attributes {
          availability           = var.availability
          zone_id                = var.zone_id
          first_on_demand        = var.first_on_demand
          instance_profile_arn   = local.instance_profile_arn
          spot_bid_price_percent = var.spot_bid_price_percent
        }

        autoscale {
          min_workers = var.min_workers
          max_workers = var.max_workers
        }
      }
    }
  }

  dynamic "task" {
    for_each = var.workflow_tasks
    iterator = item
    content {
      task_key        = upper(item.value.name)
      job_cluster_key = "job_cluster_${tostring(item.key % var.num_job_clusters)}"

      dynamic "depends_on" {
        for_each = toset(item.value.depends_on)
        iterator = dep
        content {
          task_key = upper(dep.value)
        }
      }

      spark_python_task {
        python_file = "${local.workspace_folder}/${item.value.entrypoint_file}"
        source      = var.notebook_source
        parameters  = concat(local.base_parameters, item.value.extra_parameters)
      }

      library {
        whl = var.datalake_library_whl
      }

      library {
        whl = var.data_contracts_library_whl
      }

      dynamic "library" {
        for_each = toset(var.pypi_libraries)
        iterator = lib
        content {
          pypi {
            package = lib.value
          }
        }
      }
    }
  }

  tags = {
    system      = lower(var.system)
    country     = lower(var.country)
    environment = lower(var.environment)
    phase       = "enhance"
    deployed_by = "terraform"
  }
}
