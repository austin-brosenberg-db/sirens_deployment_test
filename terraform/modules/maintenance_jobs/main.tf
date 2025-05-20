terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_spark_version" "latest" {}
data "databricks_node_type" "smallest" {
  local_disk = true
}
data "databricks_current_user" "me" {}

resource "databricks_job" "maintenance_jobs" {
  # if delta notebooks create this resource else do nothing
  count = var.maintenance_jobs[0].isDeltaStreaming ? 1 : 0

  name = var.maintenance_jobs[0].task_name
  tags = var.maintenance_jobs[0].tags
  description = var.maintenance_jobs[0].description

  job_cluster {

    job_cluster_key = var.maintenance_jobs[0].cluster.cluster_key

    new_cluster {

      node_type_id = data.databricks_node_type.smallest.id

      spark_conf     = var.maintenance_jobs[0].spark_conf
      spark_version  = data.databricks_spark_version.latest.id
      runtime_engine = var.maintenance_jobs[0].cluster.runtime_engine

      dynamic "aws_attributes" {
        for_each = length(var.maintenance_jobs[0].aws_attributes) > 0 ? [var.maintenance_jobs[0].aws_attributes] : []
        content {
          zone_id                = try(aws_attributes.value.zone_id, null)
          availability           = try(aws_attributes.value.availability, null)
          first_on_demand        = try(aws_attributes.value.first_on_demand, null)
          spot_bid_price_percent = try(aws_attributes.value.spot_bid_price_percent, null)
          instance_profile_arn   = try(aws_attributes.value.instance_profile_arn, null)
          ebs_volume_type        = try(aws_attributes.value.ebs_volume_type, null)
          ebs_volume_count       = try(aws_attributes.value.ebs_volume_count, null)
          ebs_volume_size        = try(aws_attributes.value.ebs_volume_size, null)
        }
      }

      dynamic "azure_attributes" {
        for_each = length(var.maintenance_jobs[0].azure_attributes) > 0 ? [var.maintenance_jobs[0].azure_attributes] : []
        content {
          first_on_demand    = try(azure_attributes.value.first_on_demand, null)
          availability       = try(azure_attributes.value.availability, null)
          spot_bid_max_price = try(azure_attributes.value.spot_bid_max_price, null)
        }
      }

      dynamic "gcp_attributes" {
        for_each = length(var.maintenance_jobs[0].gcp_attributes) > 0 ? [var.maintenance_jobs[0].gcp_attributes] : []
        content {
          google_service_account = try(gcp_attributes.value.google_service_account, null)
          availability           = try(gcp_attributes.value.availability, null)
          zone_id                = try(gcp_attributes.value.zone_id, null)
        }
      }
      autoscale {
        min_workers = var.maintenance_jobs[0].cluster.autoscale.min_workers
        max_workers = var.maintenance_jobs[0].cluster.autoscale.max_workers
      }
      custom_tags = {
        "cluster_usage" = "sirens"
      }
    }
  }
  schedule {
    quartz_cron_expression = var.maintenance_jobs[0].schedule.quartz_cron_expression
    timezone_id            = var.maintenance_jobs[0].schedule.timezone_id
  }

  dynamic "task" {
    for_each = length(var.maintenance_jobs[0].tasks) > 0 ? var.maintenance_jobs[0].tasks : []
    content {
      task_key        = task.value.task_key
      job_cluster_key = task.value.job_cluster_key
      library { whl = "/Workspace${var.sirens_repo}/${var.maintenance_jobs[0].whl_path}" }
      existing_cluster_id = task.value.existing_cluster_id
      notebook_task {
        notebook_path   = "${var.sirens_repo}/${task.value.notebook_path}"
        base_parameters = task.value.task_params
      }
    }
  }
}
