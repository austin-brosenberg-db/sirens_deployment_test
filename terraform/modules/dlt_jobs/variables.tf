variable "sirens_repo" {
  type = string
}

variable "deploy_aws" {
  type    = bool
  default = true
}

variable "aws_attributes" {
  type    = any
  default = {}
}

variable "gcp_attributes" {
  type = object({
    availability = optional(string)
    zone_id      = optional(string)
  })
  default = null
}

variable "dlt_jobs" {
  description = "used to pass the job parameters"
  type = object({
    task_name          = string
    storage_location   = string
    datasource_name    = string
    target_database    = string
    edition            = string
    channel            = string
    photon             = bool
    continuous         = bool
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
  })
}