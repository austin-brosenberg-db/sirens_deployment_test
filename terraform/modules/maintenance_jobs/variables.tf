variable "sirens_repo" {
  type = string
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
