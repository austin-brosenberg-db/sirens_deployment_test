data "databricks_current_user" "me" {}
module "services" {
  source = "./modules/services"
}

module "databricks_repo" {
  source = "./modules/repos"

  git_pat_token = var.git_pat_token
  git_provider = var.git_provider
  git_url = var.git_url
  git_username = var.git_username
  git_repo_name = var.git_repo_name
  git_branch = var.git_branch
}

module "dlt_jobs" {
  source   = "./modules/dlt_jobs"

  sirens_repo = module.databricks_repo.sirens_repo

  for_each = { for datasource in var.dlt_jobs : datasource.datasource_name => datasource }
  dlt_jobs = {
    task_name          = each.value.task_name
    storage_location   = each.value.storage_location
    datasource_name    = each.value.datasource_name
    target_database    = each.value.target_database
    edition            = each.value.edition
    channel            = each.value.channel
    photon             = each.value.photon
    continuous          = each.value.continuous
    isDeltaStreaming   = each.value.isDeltaStreaming
    tags               = each.value.tags
    notebook_libraries = each.value.notebook_libraries
    aws_attributes     = each.value.aws_attributes
    azure_attributes   = each.value.azure_attributes
    gcp_attributes     = each.value.gcp_attributes
    whl_path           = each.value.whl_path
    spark_conf         = each.value.spark_conf
    configuration = {
      input            = each.value.configuration.input
      pipeline_refresh = each.value.configuration.pipeline_refresh
    }
    schedule = {
      quartz_cron_expression = each.value.schedule.quartz_cron_expression
      timezone_id            = each.value.schedule.timezone_id
    }

    cluster = [{
      label      = each.value.cluster[0].label
      spark_conf = each.value.spark_conf
      autoscale = {
        min_workers = each.value.cluster[0].autoscale.min_workers
        max_workers = each.value.cluster[0].autoscale.max_workers
        mode        = each.value.cluster[0].autoscale.mode
      }
      custom_tags = each.value.cluster[0].custom_tags
    }]
  }
}

module "jobs" {
  source = "./modules/jobs"

  sirens_repo = module.databricks_repo.sirens_repo

  for_each = { for datasource in var.jobs : datasource.datasource_name => datasource }

  jobs = [{
    task_name             = each.value.task_name
    datasource_name       = each.value.datasource_name
    description           = each.value.description
    language              = each.value.language
    uses_existing_cluster = each.value.uses_existing_cluster
    isDeltaStreaming      = each.value.isDeltaStreaming
    aws_attributes        = each.value.aws_attributes
    azure_attributes      = each.value.azure_attributes
    gcp_attributes        = each.value.gcp_attributes
    spark_conf            = each.value.spark_conf
    tags                  = each.value.tags
    whl_path              = each.value.whl_path
    tasks                 = each.value.tasks
    schedule = try(each.value.schedule, null)
    cluster = { cluster_key = each.value.cluster.cluster_key,
      runtime_engine = each.value.cluster.runtime_engine,
      autoscale = {
        min_workers = each.value.cluster.autoscale.min_workers
        max_workers = each.value.cluster.autoscale.max_workers
      }
    }
    }
  ]

}

module "intel_jobs" {
  source = "./modules/jobs"

  sirens_repo = module.databricks_repo.sirens_repo

  for_each = { for datasource in var.intel_jobs : datasource.datasource_name => datasource }

  jobs = [{
    task_name             = each.value.task_name
    datasource_name       = each.value.datasource_name
    description           = each.value.description
    language              = each.value.language
    uses_existing_cluster = each.value.uses_existing_cluster
    isDeltaStreaming      = each.value.isDeltaStreaming
    aws_attributes        = each.value.aws_attributes
    azure_attributes      = each.value.azure_attributes
    gcp_attributes        = each.value.gcp_attributes
    spark_conf            = each.value.spark_conf
    tags                  = each.value.tags
    whl_path              = each.value.whl_path
    tasks                 = each.value.tasks
    schedule = { quartz_cron_expression = each.value.schedule.quartz_cron_expression,
    timezone_id = each.value.schedule.timezone_id }
    cluster = { cluster_key = each.value.cluster.cluster_key,
      runtime_engine = each.value.cluster.runtime_engine,
      autoscale = {
        min_workers = each.value.cluster.autoscale.min_workers
        max_workers = each.value.cluster.autoscale.max_workers
      }
    }
    }
  ]
}

module "dlt_intel_normalize_jobs" {
  source   = "./modules/dlt_jobs"

  sirens_repo = module.databricks_repo.sirens_repo

  for_each = { for datasource in var.dlt_intel_jobs : datasource.datasource_name => datasource }
  dlt_jobs = {
    task_name          = each.value.task_name
    storage_location   = each.value.storage_location
    datasource_name    = each.value.datasource_name
    target_database    = each.value.target_database
    edition            = each.value.edition
    channel            = each.value.channel
    photon             = each.value.photon
    continuous          = each.value.continuous
    isDeltaStreaming   = each.value.isDeltaStreaming
    tags               = each.value.tags
    notebook_libraries = each.value.notebook_libraries
    aws_attributes     = each.value.aws_attributes
    azure_attributes   = each.value.azure_attributes
    gcp_attributes     = each.value.gcp_attributes
    whl_path           = each.value.whl_path
    spark_conf         = each.value.spark_conf
    configuration = {
      input            = each.value.configuration.input
      pipeline_refresh = each.value.configuration.pipeline_refresh
    }
    schedule = {
      quartz_cron_expression = each.value.schedule.quartz_cron_expression
      timezone_id            = each.value.schedule.timezone_id
    }

    cluster = [{
      label      = each.value.cluster[0].label
      spark_conf = each.value.spark_conf
      autoscale = {
        min_workers = each.value.cluster[0].autoscale.min_workers
        max_workers = each.value.cluster[0].autoscale.max_workers
        mode        = each.value.cluster[0].autoscale.mode
      }
      custom_tags = each.value.cluster[0].custom_tags
    }]
  }
}

module "maintenance_jobs" {
  source = "./modules/maintenance_jobs"

  sirens_repo = module.databricks_repo.sirens_repo

  for_each = { for datasource in var.maintenance_jobs : datasource.datasource_name => datasource }

  maintenance_jobs = [{
    task_name        = each.value.task_name
    datasource_name  = each.value.datasource_name
    description      = each.value.description
    language         = each.value.language
    isDeltaStreaming = each.value.isDeltaStreaming
    aws_attributes   = each.value.aws_attributes
    azure_attributes = each.value.azure_attributes
    gcp_attributes   = each.value.gcp_attributes
    spark_conf       = each.value.spark_conf
    tags             = each.value.tags
    whl_path         = each.value.whl_path
    tasks            = each.value.tasks
    schedule = { quartz_cron_expression = each.value.schedule.quartz_cron_expression,
    timezone_id = each.value.schedule.timezone_id }
    cluster = { cluster_key = each.value.cluster.cluster_key,
      runtime_engine = each.value.cluster.runtime_engine,
      autoscale = {
        min_workers = each.value.cluster.autoscale.min_workers
    max_workers = each.value.cluster.autoscale.max_workers } }
  }]
}

# Disabling Legacy SQL Dashboards
# No longer supported as of Apr 7, 2025
# https://docs.databricks.com/aws/en/dashboards/clone-legacy-to-aibi#clone-a-legacy-dashboard-to-an-aibi-dashboard

# module "sirens_dashboards" {
#   source = "./modules/sirens_dashboards"
#
#   sql_endpoint_id = var.sql_endpoint_id
#
#   target_database = var.target_database
#
#   sql_endpoint_config = var.sql_endpoint_config
# }
