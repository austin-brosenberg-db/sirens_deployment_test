terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_current_user" "me" {}
locals {
  user_home = data.databricks_current_user.me.home
  user_repo = data.databricks_current_user.me.repos
}

resource "databricks_pipeline" "sirens_pipeline" {

  name    = var.dlt_jobs.task_name
  storage = var.dlt_jobs.storage_location

  dynamic "library" {
    for_each = var.dlt_jobs.notebook_libraries
    content {
      notebook {
        path = "${var.sirens_repo}/${library.value}"
      }
    }
  }
  configuration = {
    "sirens_wheel"                          = "/Workspace${var.sirens_repo}/${var.dlt_jobs.whl_path}"
    "sirens.home"                           = "/Workspace${var.sirens_repo}"
  }

  dynamic "cluster" {
    for_each = var.dlt_jobs.cluster != null ? var.dlt_jobs.cluster : []
    content {
      label = var.dlt_jobs.cluster[0].label

      spark_conf = var.dlt_jobs.spark_conf

      dynamic "aws_attributes" {
        for_each = length(var.dlt_jobs.aws_attributes) > 0 ? [var.dlt_jobs.aws_attributes] : []
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
        for_each = length(var.dlt_jobs.azure_attributes) > 0 ? [var.dlt_jobs.azure_attributes] : []
        content {
          first_on_demand    = try(azure_attributes.value.first_on_demand, null)
          availability       = try(azure_attributes.value.availability, null)
          spot_bid_max_price = try(azure_attributes.value.spot_bid_max_price, null)
        }
      }

      dynamic "gcp_attributes" {
        for_each = length(var.dlt_jobs.gcp_attributes) > 0 ? [var.dlt_jobs.gcp_attributes] : []
        content {
          google_service_account = try(gcp_attributes.value.google_service_account, null)
          availability           = try(gcp_attributes.value.availability, null)

          zone_id = try(gcp_attributes.value.zone_id, null)
        }
      }
      autoscale {
        min_workers = var.dlt_jobs.cluster[0].autoscale.min_workers
        max_workers = var.dlt_jobs.cluster[0].autoscale.max_workers
        mode        = var.dlt_jobs.cluster[0].autoscale.mode
      }
      custom_tags = var.dlt_jobs.cluster[0].custom_tags
    }
  }
  continuous = var.dlt_jobs.continuous
  photon     = var.dlt_jobs.photon
  target     = var.dlt_jobs.target_database
  edition    = var.dlt_jobs.edition
  channel    = var.dlt_jobs.channel
}

resource "databricks_job" "dlt" {
  depends_on = [
    databricks_pipeline.sirens_pipeline
  ]

  name = var.dlt_jobs.task_name
  tags = var.dlt_jobs.tags
  schedule {
    quartz_cron_expression = var.dlt_jobs.schedule.quartz_cron_expression
    timezone_id            = var.dlt_jobs.schedule.timezone_id
  }
  task {
    task_key = var.dlt_jobs.task_name
    pipeline_task {
      pipeline_id = databricks_pipeline.sirens_pipeline.id
    }
  }
}