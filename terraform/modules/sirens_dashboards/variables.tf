variable "sql_endpoint_id" {
    type     = string
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
}