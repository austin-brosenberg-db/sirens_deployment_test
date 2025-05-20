terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_spark_version" "latest" {}
data "databricks_node_type" "smallest" {
  for_each = toset(["STANDARD", "PHOTON"])
  local_disk = true
  photon_worker_capable = each.key == "PHOTON"
}
data "databricks_current_user" "me" {}

resource "databricks_job" "deltaStreaming" {
  # if delta notebooks create this resource else do nothing
  count = var.jobs[0].isDeltaStreaming ? 1 : 0

  name = var.jobs[0].task_name
  tags = var.jobs[0].tags
  description = var.jobs[0].description

  job_cluster {

    job_cluster_key = var.jobs[0].cluster.cluster_key

    new_cluster {
      # commented out 2023/21/03. Appears existing_cluster_id still needs a new cluster definition.. 
      #for_each = var.jobs[0].uses_existing_cluster ? [] : [1]

      #num_workers   = 2
      node_type_id = data.databricks_node_type.smallest[var.jobs[0].cluster.runtime_engine].id

      spark_conf     = var.jobs[0].spark_conf

      spark_version  = data.databricks_spark_version.latest.id
      runtime_engine = var.jobs[0].cluster.runtime_engine

      dynamic "aws_attributes" {
        for_each = length(var.jobs[0].aws_attributes) > 0 ? [var.jobs[0].aws_attributes] : []
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
        for_each = length(var.jobs[0].azure_attributes) > 0 ? [var.jobs[0].azure_attributes] : []
        content {
          first_on_demand    = try(azure_attributes.value.first_on_demand, null)
          availability       = try(azure_attributes.value.availability, null)
          spot_bid_max_price = try(azure_attributes.value.spot_bid_max_price, null)
        }
      }

      dynamic "gcp_attributes" {
        for_each = length(var.jobs[0].gcp_attributes) > 0 ? [var.jobs[0].gcp_attributes] : []
        content {
          google_service_account = try(gcp_attributes.value.google_service_account, null)
          availability           = try(gcp_attributes.value.availability, null)
          zone_id                = try(gcp_attributes.value.zone_id, null)
        }
      }
      autoscale {
        min_workers = var.jobs[0].cluster.autoscale.min_workers
        max_workers = var.jobs[0].cluster.autoscale.max_workers
      }
      custom_tags = {
        "cluster_usage" = "sirens"
      }
    }
  }
  dynamic "schedule" {
    for_each = try(var.jobs[0].schedule, null) != null ? [var.jobs[0].schedule] : []
    content {
      quartz_cron_expression = schedule.value.quartz_cron_expression
      timezone_id            = schedule.value.timezone_id
    }
  }

  dynamic "task" {
    for_each = var.jobs[0].tasks

    content {
      task_key        = task.value.task_key
      job_cluster_key = task.value.job_cluster_key

      dynamic "depends_on" {
        for_each = coalesce(task.value.depends, [])
        
        content {
          task_key = depends_on.value
        }
      }
      
      library { whl = "/Workspace${var.sirens_repo}/${var.jobs[0].whl_path}" }
      existing_cluster_id = task.value.existing_cluster_id
      notebook_task {
        notebook_path   = "${var.sirens_repo}/${task.value.notebook_path}"
        base_parameters = task.value.task_params
      }
    }
  }



/*
  task {
    task_key        = var.jobs[0].tasks[0].task_key
    job_cluster_key = var.jobs[0].tasks[0].job_cluster_key
    library { whl = "/Workspace${var.sirens_repo}/${var.jobs[0].whl_path}" }
    existing_cluster_id = var.jobs[0].tasks[0].existing_cluster_id
    notebook_task {
      notebook_path   = "${var.sirens_repo}/${var.jobs[0].tasks[0].notebook_path}"
      base_parameters = var.jobs[0].tasks[0].task_params
    }
  }



  task {
    task_key        = var.jobs[0].tasks[1].task_key
    job_cluster_key = var.jobs[0].tasks[1].job_cluster_key
    depends_on {
      task_key = var.jobs[0].tasks[0].task_key
    }
    library { whl = "/Workspace${var.sirens_repo}/${var.jobs[0].whl_path}" }
    existing_cluster_id = var.jobs[0].tasks[1].existing_cluster_id
    notebook_task {
      notebook_path   = "${var.sirens_repo}/${var.jobs[0].tasks[1].notebook_path}"
      base_parameters = var.jobs[0].tasks[1].task_params
    }
  }

  task {
    task_key        = var.jobs[0].tasks[2].task_key
    job_cluster_key = var.jobs[0].tasks[2].job_cluster_key
    depends_on {
      task_key = var.jobs[0].tasks[1].task_key
    }
    library { whl = "/Workspace${var.sirens_repo}/${var.jobs[0].whl_path}" }
    existing_cluster_id = var.jobs[0].tasks[2].existing_cluster_id
    notebook_task {
      notebook_path   = "${var.sirens_repo}/${var.jobs[0].tasks[2].notebook_path}"
      base_parameters = var.jobs[0].tasks[2].task_params
    }
  }

  dynamic task {
    for_each = length(var.jobs[0].tasks) > 3  ? [var.jobs[0].tasks[3]] : []
    content {
      task_key        = var.jobs[0].tasks[3].task_key
      job_cluster_key = var.jobs[0].tasks[3].job_cluster_key
      depends_on {
        task_key = var.jobs[0].tasks[2].task_key
      }
      library { whl = "/Workspace${var.sirens_repo}/${var.jobs[0].whl_path}" }
      existing_cluster_id = var.jobs[0].tasks[3].existing_cluster_id
      notebook_task {
        notebook_path   = "${var.sirens_repo}/${var.jobs[0].tasks[3].notebook_path}"
        base_parameters = var.jobs[0].tasks[3].task_params
      }
    }
  }
  */
}
