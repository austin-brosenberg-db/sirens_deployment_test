variable "profile_name" {
  type    = string
}

variable "git_url" {
  description = "URL of the REPO to connect to"
  type        = string
}

variable "git_branch" {
  description = "Current git branch to checkout"
  default     = ""
  type        = string
}

variable "git_username" {
  description = "email or username to connect to the git repository"
  default     = ""
  type        = string
  sensitive   = true
}

variable "git_provider" {
  description = "literal string for one of the supported source code control proviers"
  default     = "gitHub"
  type        = string
}

variable "git_pat_token" {
  description = "PAT to use to authenticate against source code control provider"
  default     = ""
  type        = string
  sensitive   = true
}

variable "git_repo_name" {
  description = "Name of the repo in the workspace"
  type        = string
}

variable "jobs" {
  description = "used to pass the job parameters"
  type = list(object({
    task_name             = string
    datasource_name       = string
    description           = optional(string)
    language              = string
    isDeltaStreaming      = bool
    uses_existing_cluster = bool
    spark_conf            = optional(map(string))
    aws_attributes        = optional(map(string))
    azure_attributes      = optional(map(string))
    gcp_attributes        = optional(map(string))
    tags                  = optional(map(string))
    whl_path              = string
    tasks = list(object({
      task_key            = string
      job_cluster_key     = string
      depends             = optional(list(string))
      notebook_path       = string
      task_params         = map(string)
      existing_cluster_id = optional(string)
    }))
    schedule = optional(object({
      quartz_cron_expression = string
      timezone_id            = string
    }))
    cluster = object({
      cluster_key    = string
      runtime_engine = string
      autoscale = object({
        min_workers = number
        max_workers = number
      })
    })
  }))
}

variable "intel_jobs" {
  description = "used to pass the intel collection job parameters"
  type = list(object({
    task_name             = string
    datasource_name       = string
    description           = optional(string)
    language              = string
    isDeltaStreaming      = bool
    uses_existing_cluster = bool
    spark_conf            = optional(map(string))
    aws_attributes        = optional(map(string))
    azure_attributes      = optional(map(string))
    gcp_attributes        = optional(map(string))
    tags                  = optional(map(string))
    whl_path              = string
    tasks = list(object({
      task_key            = string
      job_cluster_key     = string
      depends             = optional(list(string))
      notebook_path       = string
      task_params         = optional(map(string))
      existing_cluster_id = optional(string)
    }))
    schedule = object({
      quartz_cron_expression = string
      timezone_id            = string
    })
    cluster = object({
      cluster_key    = string
      runtime_engine = string
      autoscale = object({
        min_workers = number
        max_workers = number
      })
    })
  }))
}

variable "dlt_jobs" {
  description = "used to pass the dlt job parameters"
  type = list(object({
    task_name          = string
    storage_location   = string
    datasource_name    = string
    target_database    = string
    edition            = string
    channel            = string
    photon             = bool
    continuous          = bool
    isDeltaStreaming   = bool
    tags               = optional(map(string))
    notebook_libraries = list(string)
    spark_conf         = optional(map(string))
    aws_attributes     = optional(map(string))
    azure_attributes   = optional(map(string))
    gcp_attributes     = optional(map(string))
    whl_path           = string
    configuration = object({
      input            = string
      pipeline_refresh = string
    })
    schedule = object({
      quartz_cron_expression = string
      timezone_id            = string
    }),
    cluster = optional(list(object({
      label = string
      autoscale = object({
        min_workers = number
        max_workers = number
        mode        = string
      })
      custom_tags = map(string)
    }))),
  }))
}

variable "dlt_intel_jobs" {
  description = "used to pass the dlt intel normalization job parameters"
  nullable = true
  default = []
  type = list(object({
    task_name          = string
    storage_location   = string
    datasource_name    = string
    target_database    = string
    edition            = string
    channel            = string
    photon             = bool
    continuous          = bool
    isDeltaStreaming   = bool
    tags               = optional(map(string))
    notebook_libraries = list(string)
    spark_conf         = optional(map(string))
    aws_attributes     = optional(map(string))
    azure_attributes   = optional(map(string))
    gcp_attributes     = optional(map(string))
    whl_path           = string
    configuration = object({
      input            = string
      pipeline_refresh = string
    })
    schedule = object({
      quartz_cron_expression = string
      timezone_id            = string
    }),
    cluster = optional(list(object({
      label = string
      autoscale = object({
        min_workers = number
        max_workers = number
        mode        = string
      })
      custom_tags = map(string)
    }))),
  }))
}

variable "maintenance_jobs" {
  description = "used to pass the maintenance job parameters"
  type = list(object({
    task_name        = string
    datasource_name  = string
    description      = optional(string)
    language         = string
    isDeltaStreaming = bool
    spark_conf       = optional(map(string))
    aws_attributes   = optional(map(string))
    azure_attributes = optional(map(string))
    gcp_attributes   = optional(map(string))
    tags             = optional(map(string))
    whl_path         = string
    tasks = list(object({
      task_key            = string
      job_cluster_key     = string
      depends             = optional(string)
      notebook_path       = string
      task_params         = map(string)
      existing_cluster_id = optional(string)
    }))
    schedule = object({
      quartz_cron_expression = string
      timezone_id            = string
    })
    cluster = object({
      cluster_key    = string
      runtime_engine = string
      autoscale = object({
        min_workers = number
        max_workers = number
      })
    })
  }))
}

variable "sql_endpoint_id" {
    type    = string
    default = ""
}

variable "target_database" {
    type = string
}

variable "sql_endpoint_config" {
    type = object({
        name: string
        cluster_size: string
        min_num_clusters: number
        max_num_clusters: number
        auto_stop_mins: number
        enable_serverless: bool
        channel: string
        warehouse_type: string
        enable_photon: bool
    })
    default = {
        name = "Sirens Endpoint"
        cluster_size = "2X-Small"
        min_num_clusters = 1
        max_num_clusters = 1
        auto_stop_mins = 120
        enable_serverless = true
        channel = "CHANNEL_NAME_CURRENT"
        warehouse_type = "PRO"
        enable_photon = true
    }
}