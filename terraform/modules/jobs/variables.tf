# variable "whl_path" {
#   type = string
# }

variable "sirens_repo" {
  type = string
}

variable "jobs" {
  description = "used to pass the job parameters"
  type = list(object({
    task_name             = string
    description           = optional(string)
    datasource_name       = string
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
