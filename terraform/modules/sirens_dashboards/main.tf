terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_current_user" "me" {}

resource "databricks_sql_endpoint" "sirens_endpoint" {
    count                     = var.sql_endpoint_id == null ? 1 : 0
    name                      = var.sql_endpoint_config.name
    cluster_size              = var.sql_endpoint_config.cluster_size
    min_num_clusters          = var.sql_endpoint_config.min_num_clusters
    max_num_clusters          = var.sql_endpoint_config.max_num_clusters
    auto_stop_mins            = var.sql_endpoint_config.auto_stop_mins
    enable_serverless_compute = var.sql_endpoint_config.enable_serverless
    channel {
        name                  = var.sql_endpoint_config.channel
    }                   
    warehouse_type            = var.sql_endpoint_config.warehouse_type
    enable_photon             = var.sql_endpoint_config.enable_photon
}

data "databricks_sql_warehouse" "sirens_endpoint" {
    count = var.sql_endpoint_id != null ? 1 : 0
    id    = var.sql_endpoint_id
}

locals {
    sirens_endpoint = element(concat(databricks_sql_endpoint.sirens_endpoint[*].data_source_id,
     data.databricks_sql_warehouse.sirens_endpoint[*].data_source_id), 0)
}

resource "databricks_directory" "sirens_dashboards_dir" {
  path = "${data.databricks_current_user.me.home}/Sirens Dashboards"
}

resource "databricks_sql_query" "clone_authentication_count_last_hour_12327a38_bc47_49d4_b80f_8b56e83ecdea" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.authentication\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "authentication count last hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_detailed_list_a27ba25b_8a8e_43d2_a88f_6dd5222cfd56" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.authentication\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\n  order by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-07-01 00:00:00"
        end   = "2023-07-01 00:00:00"
      }
    }
  }
  name           = "authentication_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_event_result_b6a41fb5_2534_4af9_8995_295c2915bbb0" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.authentication\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-01-01 00:00:00"
        end   = "2023-07-11 23:59:59"
      }
    }
  }
  name           = "authentication_ event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_sourcetype_by_time_d9ed2296_6089_4510_bcc2_9cea70e3017a" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.authentication\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-07-01 00:00:00"
        end   = "2023-07-01 00:00:00"
      }
    }
  }
  name           = "authentication_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_sourcetype_count_cd351790_3c5c_4f13_a325_b9897f09952a" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.authentication\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-07-01 00:00:00"
        end   = "2023-07-01 00:00:00"
      }
    }
  }
  name           = "authentication_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_top_10_users_4a6f84c8_46dc_4a2f_b3d9_d92a08004770" {
  query  = "SELECT\n user,\n  count(user) as count\nFROM\n  ${var.target_database}.authentication\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  user"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-03-01 00:00:00"
        end   = "2023-05-31 00:00:00"
      }
    }
  }
  name           = "authentication_top_10_users"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_total_event_count_0ecba174_8c92_4c41_b459_da00319ed10b" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.authentication "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "authentication_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_authentication_unique_hosts_3a08b16f_0310_4c84_8045_4e4340f8599a" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.authentication"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "Authentication - Unique hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_count_last_hour_8958f20e_a76d_4b85_b9dd_599f2eb98597" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.dhcp\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "dhcp_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_detailed_list_ae0d4607_4b54_4747_8edd_bfe8fa18b409" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.dhcp\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dhcp_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_event_result_d6144d24_61c3_4d0d_aad9_22d7f1f58d4a" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.dhcp\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dhcp_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_sourcetype_by_time_06076c8d_a0ba_4f87_a2a9_899eb8a48833" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.dhcp\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dhcp_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_sourcetype_count_e1ea5f3d_6a90_432d_83c9_c44f7f348156" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.dhcp\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dhcp_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_top_10_hosts_7759471b_4fbe_41d8_afe1_18aae08da025" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.dhcp\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dhcp_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_total_event_count_44fb56b1_de41_44b0_ab92_f570ec64b75c" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.dhcp"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "dhcp_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dhcp_unique_hosts_3026fa32_8590_4b2f_a47b_67587b446456" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.dhcp"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "dhcp_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_count_last_hour_79d17105_7de6_4299_998f_039067cfda00" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.dns\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "dns count last hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_detailed_list_dfff1d53_d39c_4f1f_9e83_417970b19453" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.dns\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dns_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_event_result_9030d896_fe5a_4b74_9375_4420594800f8" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.dns\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dns_ event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_sourcetype_by_time_07748ded_ea55_4253_9919_4ace013263f4" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.dns\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dns_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_sourcetype_count_03d2aa78_927a_4409_b73a_aa5f4e445ce0" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.dns\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dns_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_top_10_hosts_9e7411ec_40d4_47dc_b723_7d2110a4a04a" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.dns\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "dns_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_total_event_count_fb29145d_47f9_47c6_a957_b1a981b2e4f2" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.dns "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "dns_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_dns_unique_hosts_a8d28bcf_bc2c_4a16_9c51_67292a38aae8" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.dns"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "dns - Unique hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_count_last_hour_123be039_85b3_4e49_8f91_2cef3d1f4178" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.file\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "file_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_detailed_list_6df1dbea_4e33_43da_ab00_b2d870f2eda5" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.file\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2018-03-01 00:00:00"
        end   = "2023-07-11 23:59:59"
      }
    }
  }
  name           = "file_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_event_result_0b354d30_b630_4c01_b448_043a1e294c82" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.file\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-01-01 00:00:00"
        end   = "2023-07-11 23:59:59"
      }
    }
  }
  name           = "file_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_sourcetype_by_time_5107347e_59da_497e_9cdc_ef5c5ef34cb4" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.file\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2018-03-01 00:00:00"
        end   = "2023-07-11 23:59:59"
      }
    }
  }
  name           = "file_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_sourcetype_count_73da9209_249d_4c82_8979_e4724d0d9b0f" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.file\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2018-01-01 00:00:00"
        end   = "2023-07-11 23:59:59"
      }
    }
  }
  name           = "file_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_top_10_hosts_eb740335_602a_43cb_bcfc_b90422c36f11" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.file\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      range {
        start = "2017-07-01 00:00:00"
        end   = "2023-07-11 23:59:59"
      }
    }
  }
  name           = "file_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_total_event_count_f1aafebb_e492_4e09_8873_5420cdafb69b" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.file"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "file_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_file_unique_hosts_04801c3f_9bf1_42a3_ad45_16ae0f78e976" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.file"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "file_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_count_last_hour_3c205459_30b5_4404_b1a7_93010e74dd26" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.network\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "network_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_detailed_list_5871ab2f_cac8_4b98_a9f5_77439c57e35f" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.network\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "network_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_event_result_3d924bdb_c86a_4643_98bd_0187c7b3bb4d" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.network\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "network_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_sourcetype_by_time_b5d0295b_bff9_497b_b33a_d12b4d970718" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.network\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "network_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_sourcetype_count_169c87d4_555a_4729_872f_00a66ff237d1" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.network\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "network_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_top_10_hosts_ef68f5fc_1bbd_413d_b7aa_52dbad4cd481" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.network\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "network_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_total_event_count_636a958e_9340_4241_861e_e3f72488f323" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.network"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "network_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_network_unique_hosts_3dd3efce_dce5_41ae_9e32_d38eb99e7241" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.network"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "network_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_count_last_hour_a9848142_e585_44b0_a601_db82d2b74d5a" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.powershell\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "powershell_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_detailed_list_d8fc4f93_8921_4849_889d_c9ee11c3325e" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.powershell\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "powershell_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_event_result_7e993c78_1469_49d9_ac0c_6c54ef0d1376" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.powershell\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "powershell_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_sourcetype_by_time_a798fddd_0c91_4904_a45c_1bcd98b2d326" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.powershell\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "powershell_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_sourcetype_count_e69180e7_3caf_4256_8156_e631101edb0e" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.powershell\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "powershell_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_top_10_hosts_aea7dd5b_6d43_4d1f_aafd_bc633b8d1d7a" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.powershell\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "powershell_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_total_event_count_2d265db2_dfe5_41df_b327_2db83c317550" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.powershell"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "powershell_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_powershell_unique_hosts_c99780f9_25ed_46b0_87ab_cce6923f6f9b" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.powershell"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "powershell_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_count_last_hour_059f4129_e183_4e4d_b664_3b710d2d25a1" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.process\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "process_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_detailed_list_b602bcd9_c744_4dbe_b268_eeee17323041" {
  query  = "SELECT\n  *\nFROM\n  ${var.target_database}.process\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "process_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_event_result_180bad07_991f_43a9_a985_c50b340196ee" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.process\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "process_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_sourcetype_by_time_da43979f_08a8_4c00_9fa1_8eb68833469f" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.process\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "process_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_sourcetype_count_4395a316_d119_477b_8f9a_83c7a7e0855e" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.process\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "process_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_top_10_hosts_36c09ff1_c08c_4941_ab1c_f7d3516dca92" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.process\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname\nlimit 10"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "process_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_total_event_count_a081110e_e36d_4607_8491_1e4d034f2d63" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.process"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "process_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_process_unique_hosts_64e6c7dd_a700_429a_b638_7f4d0c9bd7d7" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.process"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "process_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_count_last_hour_48f31d24_4c10_4c14_8946_4536dfa6262a" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.registry\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "registry_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_detailed_list_e2d28a6e_93d5_4d17_b188_4e775c35e55c" {
  query  = "SELECT\n  *\nFROM\n${var.target_database}.registry\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\norder by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "registry_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_event_result_e966f802_866d_412d_afa9_c7648c5e0a8d" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.registry\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "registry_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_sourcetype_by_time_98924f85_8488_47ec_910c_8232b2c7cf23" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.registry\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "registry_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_sourcetype_count_1743c8f7_a7be_4d35_b215_85533e32faa2" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n  ${var.target_database}.registry\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "registry_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_top_10_hosts_a3291f08_51a3_4f6a_bf19_7b3875f55b89" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.registry\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname\nlimit 10"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "registry_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_total_event_count_0e97bc99_841f_4d6b_ace0_f607c318c749" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.registry"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "registry_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_registry_unique_hosts_bbfc631c_02c0_419d_bea2_cb9fb8e88202" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.registry"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "registry_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_count_last_hour_7f4fc274_cd65_419e_a035_a6530fd99ba7" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.service\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "service_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_detailed_list_e91f71e9_cb2f_46d9_929c_11ef1aa9cc3d" {
  query  = "SELECT\n  *\nFROM\n ${var.target_database}.service\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\n  order by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "service_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_event_result_dd25dec7_0ce4_49f7_8726_48dbc1469936" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.service\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "service_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_sourcetype_by_time_9ace7fbb_ab9f_4473_a932_a518d613671a" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.service\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "service_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_sourcetype_count_e85b210d_0b3f_41ae_84a3_7b15c4222ccb" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n ${var.target_database}.service\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "service_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_top_10_hosts_31df8238_b24d_4966_b5b0_027a7f62a96a" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.service\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname\nlimit 10"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "service_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_total_event_count_0be57781_8e17_4729_b466_94dd863e37a6" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.service"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "service_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_service_unique_hosts_7c97e898_e2a6_4c72_9067_2c53b5d8be91" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.service"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "service_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_count_last_hour_51e345c4_496e_490a_ad50_c20ea90de287" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.user_management\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "user_management_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_detailed_list_87a27553_4bce_4564_ac67_2ed4c27ddd9d" {
  query  = "SELECT\n  *\nFROM\n ${var.target_database}.user_management\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\n  order by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "user_management_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_event_result_fda329cd_7e59_41ac_a440_8ea233fc0146" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.user_management\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "user_management_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_sourcetype_by_time_3a5584dd_d54b_409f_b20c_1b4d32b6f386" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.user_management\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "user_management_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_sourcetype_count_5084a1d7_f357_4501_9cff_d1db61920274" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n ${var.target_database}.user_management\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "user_management_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_top_10_hosts_16f1f2b0_c635_4230_a3f0_f4dae3ab221d" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.user_management\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname\nlimit 10"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "user_management_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_total_event_count_4f2f389f_8e34_433a_8f13_b4e7f80f72d7" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.user_management"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "user_management_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_user_management_unique_hosts_a36432d2_8bc8_4f65_bfdd_4ed4011da00d" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.user_management"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "user_management_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_count_last_hour_bf07209f_cfb0_46fa_b093_8cb009771289" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.web\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "web_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_detailed_list_610df980_4e6f_4a5a_b2a9_bc7fdf95940f" {
  query  = "SELECT\n  *\nFROM\n ${var.target_database}.web\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\n  order by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "web_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_event_result_611a9511_3ea5_4f92_aa5f_6eb1c90bdbad" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.web\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "web_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_sourcetype_by_time_d7928374_bc68_4f1f_8450_11959b282723" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.web\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "web_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_sourcetype_count_2a6787db_c0db_4855_b11b_2d79c7f4e4f7" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n ${var.target_database}.web\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "web_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_top_10_hosts_e7031aac_3399_42ad_a24d_1a4b4e54bcf2" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.web\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname\nlimit 10"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "web_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_total_event_count_24b50f02_db2b_499f_8670_420ad57f7568" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.web"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "web_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_web_unique_hosts_4d1c5f84_2dff_472f_ad1e_d1bdd4023f30" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.web"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "web_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_count_last_hour_950adad8_2f18_4bb3_9ca1_8ae7e5e0c867" {
  query          = "SELECT\n  count(*) as events_count\nfrom\n  ${var.target_database}.wmi\nwhere\n  `_event_time` >= (now() - interval 1 hour)  \n  "
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "wmi_count_last_hour"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_detailed_list_5c84419b_4c49_4df6_a8ef_2b4b60372f8f" {
  query  = "SELECT\n  *\nFROM\n ${var.target_database}.wmi\nwhere\n  `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\n  order by _event_time DESC"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "Select event time period"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "wmi_detailed_list"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_event_result_cceb855c_2d34_4f29_a379_ca77090df411" {
  query  = "SELECT\n  _event_date,\n  event_result,\n  count(*)\nFROM\n  ${var.target_database}.wmi\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  _event_date, event_result\n ORDER BY _event_date"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "wmi_event_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_sourcetype_by_time_6afc3d0e_8d13_44de_af96_64134e700b0f" {
  query  = "SELECT\n  _sourcetype as source, _event_time as time, count(_sourcetype) as count\nfrom\n  ${var.target_database}.wmi\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype, `_event_time`\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "wmi_sourcetype_by_time"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_sourcetype_count_f9a21f42_94be_4af9_b975_e59d61d76d18" {
  query  = "SELECT\n  _sourcetype as source, count(_sourcetype) as count\nfrom\n ${var.target_database}.wmi\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by _sourcetype\n"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "wmi_sourcetype_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_top_10_hosts_8320c98f_a501_4472_be8e_f72cfbb8e94a" {
  query  = "SELECT\n dvc_hostname,\n  count(dvc_hostname) as count\nFROM\n  ${var.target_database}.wmi\nwhere `_event_time` BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\ngroup by\n  dvc_hostname\nlimit 10"
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetimesec_range {
      value = "d_last_12_months"
    }
  }
  name           = "wmi_top_10_hosts"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_total_event_count_ccd7c70c_96ae_40c9_a103_8ad90621b14e" {
  query          = "SELECT count(*) as events_count from ${var.target_database}.wmi"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "wmi_total_event_count"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "clone_wmi_unique_hosts_3e6994f6_675d_45c1_99a5_f8a55ec7c6bb" {
  query          = "SELECT count(DISTINCT(dvc_hostname)) from ${var.target_database}.wmi"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "wmi_unique_hosts"
  description    = "Sirens query"
  data_source_id = local.sirens_endpoint
}

resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611ba17d88b8e_83e9_43ba_aa2a_e71747381fa2" {
  visualization_id = databricks_sql_visualization.table_8958f20e_a76d_4b85_b9dd_599f2eb98597403f0e3f_e789_40b0_a58b_8e218e34303b.visualization_id
  title            = "DHCP Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611ba18c5aa45_7af8_4d57_89c4_7e65481a95af" {
  visualization_id = databricks_sql_visualization.table_7759471b_4fbe_41d8_afe1_18aae08da02521ed904d_d7cb_4e97_9b3e_3b86c658c1c6.visualization_id
  title            = "DHCP top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611ba25f7f6b1_b04b_43ee_8e4b_68c52f34a86a" {
  visualization_id = databricks_sql_visualization.table_44fb56b1_de41_44b0_ab92_f570ec64b75c7fb63825_eabc_4fbc_ae96_21eeb0ccd7b8.visualization_id
  title            = "DHCP Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611ba3fae677a_cf06_4782_b2da_eb6bccfb1a3c" {
  visualization_id = databricks_sql_visualization.table_3026fa32_8590_4b2f_a47b_67587b446456d6f74bc7_2135_4f71_9839_8b86c9cf3b95.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611ba976ebde2_c8b5_4e4e_ba33_839c7eeae163" {
  visualization_id = databricks_sql_visualization.pie_2_e1ea5f3d_6a90_432d_83c9_c44f7f348156575b6f66_db76_41a3_b1ca_e255126356ae.visualization_id
  title            = "DHCP Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611ba9d16ef81_5f22_4a76_b338_df6cc2d94939" {
  visualization_id = databricks_sql_visualization.table_06076c8d_a0ba_4f87_a2a9_899eb8a488330c9cf2bc_542c_4f20_b2de_237fd54aeb35.visualization_id
  title            = "DHCP Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611bae0f645ea_9eac_425b_a7df_b7f2fe3d3eb1" {
  visualization_id = databricks_sql_visualization.table_d6144d24_61c3_4d0d_aad9_22d7f1f58d4a916dc6fa_5074_4511_98e7_021842cf447d.visualization_id
  title            = "DHCP event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "c117791e_44cc_451c_a703_92501a9611baf23db0c8_8e86_45a9_b314_6ce60c6b08bf" {
  visualization_id = databricks_sql_visualization.login_activities_ae0d4607_4b54_4747_8edd_bfe8fa18b4092b619f89_68d0_4ef8_b5e1_4e52df26e153.visualization_id
  title            = "Detailed DHCP activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0b36f65ed3_056d_481d_a244_669885045c22" {
  visualization_id = databricks_sql_visualization.table_a9848142_e585_44b0_a601_db82d2b74d5a8831f2d3_b73f_43a1_97e6_740e34d18193.visualization_id
  title            = "Powershell Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0b395718ce_0d08_4bdd_84fa_2cc0eb0cfc5d" {
  visualization_id = databricks_sql_visualization.table_a798fddd_0c91_4904_a45c_1bcd98b2d326c293d0d3_8cb9_400f_a2b9_36d8934abf12.visualization_id
  title            = "Powershell Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0b6bba2b95_478c_4b0d_a663_e88a6638f161" {
  visualization_id = databricks_sql_visualization.table_aea7dd5b_6d43_4d1f_aafd_bc633b8d1d7a1d5fbd29_8d1d_422e_9d6b_6812fcd2eb90.visualization_id
  title            = "Powershell top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0b9f654de1_91e4_4e55_8544_c78675f9e5eb" {
  visualization_id = databricks_sql_visualization.table_7e993c78_1469_49d9_ac0c_6c54ef0d13761144eb6b_636d_47d2_82d4_d6c1e29817cf.visualization_id
  title            = "Powershell event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0bbad74aa8_dd31_4617_bdee_0694f691ae74" {
  visualization_id = databricks_sql_visualization.login_activities_d8fc4f93_8921_4849_889d_c9ee11c3325e410686ee_63b6_4a94_8a83_35adfb6c6d1c.visualization_id
  title            = "Detailed Powershell activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0be50787e4_d6a8_433b_beb0_0ed9f85df0bf" {
  visualization_id = databricks_sql_visualization.pie_2_e69180e7_3caf_4256_8156_e631101edb0e58a1e194_804e_4d32_8182_1cee0566e843.visualization_id
  title            = "Powershell Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0bec72604a_9f4c_45a6_af3b_c469ae9a71f4" {
  visualization_id = databricks_sql_visualization.table_c99780f9_25ed_46b0_87ab_cce6923f6f9b870552d4_c284_462c_b820_bf3ba90e92a3.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "dd49c3d1_323d_43c7_8fb6_e16b38178a0bf3e385ef_c611_4671_8f01_9bc837cead06" {
  visualization_id = databricks_sql_visualization.table_2d265db2_dfe5_41df_b327_2db83c317550239f5e51_884c_430f_bd3f_92fc8a8843a9.visualization_id
  title            = "Powershell Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfed1c1aae82_1c58_4bad_b43d_5021a1dfeac5" {
  visualization_id = databricks_sql_visualization.table_b6a41fb5_2534_4af9_8995_295c2915bbb006810c33_1e37_479f_b05a_5c93dd062176.visualization_id
  title            = "Authentication event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfed3a445e16_8da9_4371_8e23_952b3786f0b9" {
  visualization_id = databricks_sql_visualization.table_d9ed2296_6089_4510_bcc2_9cea70e3017a659b3225_ffb5_46b4_8609_44ec64a6f9b8.visualization_id
  title            = "Authentication over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfed41f4655d_bf06_4686_91d8_ec14db2b99af" {
  visualization_id = databricks_sql_visualization.table_4a6f84c8_46dc_4a2f_b3d9_d92a08004770123dc0e5_a756_4e9d_88d3_783f8b8eb2fc.visualization_id
  title            = "Authentication top 10 users"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfed86277f38_9717_46e4_9907_9dfa7900a16e" {
  visualization_id = databricks_sql_visualization.login_activities_a27ba25b_8a8e_43d2_a88f_6dd5222cfd567f78e891_e4b6_4111_8dda_929dae10f98e.visualization_id
  title            = "Login activities "
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfed9f19b5b7_ac19_43f8_88d4_27240c42293f" {
  visualization_id = databricks_sql_visualization.table_12327a38_bc47_49d4_b80f_8b56e83ecdea27c335be_5603_4d13_9ffd_43bfdaab6cc8.visualization_id
  title            = "Authentications"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfedd15b2f14_5c79_4ab1_aa16_69cc034905d7" {
  visualization_id = databricks_sql_visualization.table_3a08b16f_0310_4c84_8045_4e4340f8599a34d7e23f_dc56_406b_b253_febd90ecb390.visualization_id
  title            = "Authentications"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfedf97a01c4_1558_489d_b2a1_98828f4fb8df" {
  visualization_id = databricks_sql_visualization.table_0ecba174_8c92_4c41_b459_da00319ed10b9dc1fda0_9013_486b_a7ca_3189235998a6.visualization_id
  title            = "Authentications"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "ea43b4fa_63c6_4533_99c7_6e63b584bfedffc2fdbd_da7d_4f0d_bc50_7c7a17e48ef4" {
  visualization_id = databricks_sql_visualization.pie_2_cd351790_3c5c_4f13_a325_b9897f09952abce429ae_10c2_44f1_a9fb_f97598067cc4.visualization_id
  title            = "Authentication by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a57197868138b469c_b782_4142_bc06_9cd020c9233e" {
  visualization_id = databricks_sql_visualization.table_4d1c5f84_2dff_472f_ad1e_d1bdd4023f3055d4df90_959f_4bc4_9635_ab09ca2875d6.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a571978686d610275_ecc3_4d44_920f_f528ad1ec7c5" {
  visualization_id = databricks_sql_visualization.table_d7928374_bc68_4f1f_8450_11959b282723df0faa0f_2d46_40a0_88e0_ce4aba317e33.visualization_id
  title            = "Web Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a5719786881e1521c_729d_4e90_82c7_23cbb43c781f" {
  visualization_id = databricks_sql_visualization.pie_2_2a6787db_c0db_4855_b11b_2d79c7f4e4f702612f30_2475_46ea_a1d2_bd85a1dd3752.visualization_id
  title            = "Web Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a571978689defd0a8_2d2e_4e46_b60f_dd647c7b3895" {
  visualization_id = databricks_sql_visualization.table_611a9511_3ea5_4f92_aa5f_6eb1c90bdbad4e9565b9_8358_42d9_973e_9355a1cda6a4.visualization_id
  title            = "Web event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a571978689e6ea857_4a97_4ea0_94bf_b39d77b6878c" {
  visualization_id = databricks_sql_visualization.table_e7031aac_3399_42ad_a24d_1a4b4e54bcf258333814_6e20_4020_8bd6_300dfa5a967d.visualization_id
  title            = "Web top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a57197868b42ead57_cd8c_44b6_8c2c_b43567b8c6f8" {
  visualization_id = databricks_sql_visualization.login_activities_610df980_4e6f_4a5a_b2a9_bc7fdf95940f76ea04f6_8da6_4704_8b87_a233304f71f5.visualization_id
  title            = "Detailed Web activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a57197868d862f686_132e_4949_83f9_7662e9278334" {
  visualization_id = databricks_sql_visualization.table_24b50f02_db2b_499f_8670_420ad57f75686436921c_503f_4ae5_b2ac_4ad8348d461c.visualization_id
  title            = "Web Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_widget" "f8d62b87_65dd_4f9c_a41d_b13a57197868e9ce3b88_f765_483d_8dd4_e6585d68853f" {
  visualization_id = databricks_sql_visualization.table_bf07209f_cfb0_46fa_b093_8cb00977128956d2c6d9_9c5f_47fa_bebf_23d69e5f7b2d.visualization_id
  title            = "Web Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868.id
}
resource "databricks_sql_visualization" "login_activities_5871ab2f_cac8_4b98_a9f5_77439c57e35f79e0c77a_daea_4e86_a65b_2b7b1a70f56a" {
  type     = "table"
  query_id = databricks_sql_query.clone_network_detailed_list_5871ab2f_cac8_4b98_a9f5_77439c57e35f.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_5c84419b_4c49_4df6_a8ef_2b4b60372f8ff11e6951_b11f_4bce_bc8f_eb28974ee7d0" {
  type     = "table"
  query_id = databricks_sql_query.clone_wmi_detailed_list_5c84419b_4c49_4df6_a8ef_2b4b60372f8f.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_610df980_4e6f_4a5a_b2a9_bc7fdf95940f76ea04f6_8da6_4704_8b87_a233304f71f5" {
  type     = "table"
  query_id = databricks_sql_query.clone_web_detailed_list_610df980_4e6f_4a5a_b2a9_bc7fdf95940f.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_6df1dbea_4e33_43da_ab00_b2d870f2eda527c28c14_9771_419d_8b4f_b4e3964941b7" {
  type     = "table"
  query_id = databricks_sql_query.clone_file_detailed_list_6df1dbea_4e33_43da_ab00_b2d870f2eda5.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_87a27553_4bce_4564_ac67_2ed4c27ddd9d9d684b62_96c5_4fb4_8d9e_b1ea19be8faf" {
  type     = "table"
  query_id = databricks_sql_query.clone_user_management_detailed_list_87a27553_4bce_4564_ac67_2ed4c27ddd9d.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_a27ba25b_8a8e_43d2_a88f_6dd5222cfd567f78e891_e4b6_4111_8dda_929dae10f98e" {
  type     = "table"
  query_id = databricks_sql_query.clone_authentication_detailed_list_a27ba25b_8a8e_43d2_a88f_6dd5222cfd56.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_ae0d4607_4b54_4747_8edd_bfe8fa18b4092b619f89_68d0_4ef8_b5e1_4e52df26e153" {
  type     = "table"
  query_id = databricks_sql_query.clone_dhcp_detailed_list_ae0d4607_4b54_4747_8edd_bfe8fa18b409.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_b602bcd9_c744_4dbe_b268_eeee17323041405c42c5_390d_4e21_aa95_eae363bd7b26" {
  type     = "table"
  query_id = databricks_sql_query.clone_process_detailed_list_b602bcd9_c744_4dbe_b268_eeee17323041.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_d8fc4f93_8921_4849_889d_c9ee11c3325e410686ee_63b6_4a94_8a83_35adfb6c6d1c" {
  type     = "table"
  query_id = databricks_sql_query.clone_powershell_detailed_list_d8fc4f93_8921_4849_889d_c9ee11c3325e.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_dfff1d53_d39c_4f1f_9e83_417970b19453208cf03c_ab63_4ecb_8075_2078cd0bb3a7" {
  type     = "table"
  query_id = databricks_sql_query.clone_dns_detailed_list_dfff1d53_d39c_4f1f_9e83_417970b19453.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_e2d28a6e_93d5_4d17_b188_4e775c35e55c47e4d422_4215_42b4_b806_a4b59c1fb990" {
  type     = "table"
  query_id = databricks_sql_query.clone_registry_detailed_list_e2d28a6e_93d5_4d17_b188_4e775c35e55c.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "login_activities_e91f71e9_cb2f_46d9_929c_11ef1aa9cc3db5d7430f_78bc_4898_80d8_0cb98214b7b1" {
  type     = "table"
  query_id = databricks_sql_query.clone_service_detailed_list_e91f71e9_cb2f_46d9_929c_11ef1aa9cc3d.id
  options  = "{\"columns\": [{\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"cellFormat\": {\"default\": {\"foregroundColor\": null}, \"rules\": []}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm:ss.SSS\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_time\", \"order\": 0, \"preserveWhitespace\": false, \"title\": \"Event time\", \"type\": \"datetime\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_sourcetype\", \"order\": 1, \"preserveWhitespace\": false, \"title\": \"Sourcetype\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"login_id\", \"order\": 2, \"preserveWhitespace\": false, \"title\": \"login_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"user\", \"order\": 3, \"preserveWhitespace\": false, \"title\": \"User\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"dateTimeFormat\": \"YYYY-MM-DD\", \"displayAs\": \"datetime\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_event_date\", \"order\": 4, \"preserveWhitespace\": false, \"title\": \"_event_date\", \"type\": \"date\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"_source\", \"order\": 5, \"preserveWhitespace\": false, \"title\": \"_source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src\", \"order\": 6, \"preserveWhitespace\": false, \"title\": \"Source\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dst\", \"order\": 7, \"preserveWhitespace\": false, \"title\": \"Destination\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": true, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_result\", \"order\": 8, \"preserveWhitespace\": false, \"title\": \"Event result\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_message\", \"order\": 9, \"preserveWhitespace\": false, \"title\": \"Event message\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc_hostname\", \"order\": 10, \"preserveWhitespace\": false, \"title\": \"dvc_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_original_severity\", \"order\": 11, \"preserveWhitespace\": false, \"title\": \"event_original_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_product\", \"order\": 12, \"preserveWhitespace\": false, \"title\": \"event_product\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_schema_file\", \"order\": 13, \"preserveWhitespace\": false, \"title\": \"event_schema_file\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_severity\", \"order\": 14, \"preserveWhitespace\": false, \"title\": \"event_severity\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_sub_type\", \"order\": 15, \"preserveWhitespace\": false, \"title\": \"event_sub_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_type\", \"order\": 16, \"preserveWhitespace\": false, \"title\": \"event_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"event_vendor\", \"order\": 17, \"preserveWhitespace\": false, \"title\": \"event_vendor\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"http_user_agent\", \"order\": 18, \"preserveWhitespace\": false, \"title\": \"http_user_agent\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_authentication_package_name\", \"order\": 19, \"preserveWhitespace\": false, \"title\": \"logon_authentication_package_name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_guid\", \"order\": 20, \"preserveWhitespace\": false, \"title\": \"logon_guid\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_method\", \"order\": 21, \"preserveWhitespace\": false, \"title\": \"logon_method\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_target\", \"order\": 22, \"preserveWhitespace\": false, \"title\": \"logon_target\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"logon_type\", \"order\": 23, \"preserveWhitespace\": false, \"title\": \"logon_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_ip_addr\", \"order\": 24, \"preserveWhitespace\": false, \"title\": \"Source IP Address\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_domain\", \"order\": 25, \"preserveWhitespace\": false, \"title\": \"Target domain\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_city\", \"order\": 26, \"preserveWhitespace\": false, \"title\": \"src_geo_city\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_country\", \"order\": 27, \"preserveWhitespace\": false, \"title\": \"src_geo_country\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lat\", \"numberFormat\": \"0.00\", \"order\": 28, \"preserveWhitespace\": false, \"title\": \"src_geo_lat\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"right\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"number\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_lon\", \"numberFormat\": \"0.00\", \"order\": 29, \"preserveWhitespace\": false, \"title\": \"src_geo_lon\", \"type\": \"float\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_geo_region\", \"order\": 30, \"preserveWhitespace\": false, \"title\": \"src_geo_region\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"src_isp\", \"order\": 31, \"preserveWhitespace\": false, \"title\": \"src_isp\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_id\", \"order\": 32, \"preserveWhitespace\": false, \"title\": \"target_app_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_name\", \"order\": 33, \"preserveWhitespace\": false, \"title\": \"Target App Name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_app_type\", \"order\": 34, \"preserveWhitespace\": false, \"title\": \"target_app_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_hostname\", \"order\": 35, \"preserveWhitespace\": false, \"title\": \"target_hostname\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_session_id\", \"order\": 36, \"preserveWhitespace\": false, \"title\": \"target_session_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_id\", \"order\": 37, \"preserveWhitespace\": false, \"title\": \"target_user_id\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_name\", \"order\": 38, \"preserveWhitespace\": false, \"title\": \"Target user name\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_user_type\", \"order\": 39, \"preserveWhitespace\": false, \"title\": \"target_user_type\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"dvc\", \"order\": 40, \"preserveWhitespace\": false, \"title\": \"Reporting host\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": true}, {\"alignContent\": \"left\", \"allowHTML\": false, \"allowSearch\": false, \"booleanValues\": [\"false\", \"true\"], \"displayAs\": \"string\", \"highlightLinks\": false, \"imageHeight\": \"\", \"imageTitleTemplate\": \"{{ @ }}\", \"imageUrlTemplate\": \"{{ @ }}\", \"imageWidth\": \"\", \"linkOpenInNewTab\": true, \"linkTextTemplate\": \"{{ @ }}\", \"linkTitleTemplate\": \"{{ @ }}\", \"linkUrlTemplate\": \"{{ @ }}\", \"name\": \"target_url\", \"order\": 41, \"preserveWhitespace\": false, \"title\": \"target_url\", \"type\": \"string\", \"useMonospaceFont\": false, \"visible\": false}], \"condensed\": true, \"itemsPerPage\": 25, \"version\": 2, \"withRowNumber\": true}"
  name     = "Login activities"
}
resource "databricks_sql_visualization" "pie_2_03d2aa78_927a_4409_b73a_aa5f4e445ce017e61ec8_e644_4875_b49f_f0188fdc2368" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_dns_sourcetype_count_03d2aa78_927a_4409_b73a_aa5f4e445ce0.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_169c87d4_555a_4729_872f_00a66ff237d137382e75_d5f0_4a8c_b67d_57ad6d79359a" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_network_sourcetype_count_169c87d4_555a_4729_872f_00a66ff237d1.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_1743c8f7_a7be_4d35_b215_85533e32faa22eacc652_3c08_4133_9110_873e0fe8b61c" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_registry_sourcetype_count_1743c8f7_a7be_4d35_b215_85533e32faa2.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_2a6787db_c0db_4855_b11b_2d79c7f4e4f702612f30_2475_46ea_a1d2_bd85a1dd3752" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_web_sourcetype_count_2a6787db_c0db_4855_b11b_2d79c7f4e4f7.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_4395a316_d119_477b_8f9a_83c7a7e0855ef7eb3507_9cf7_4e04_a3f1_faced3c03d48" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_process_sourcetype_count_4395a316_d119_477b_8f9a_83c7a7e0855e.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_5084a1d7_f357_4501_9cff_d1db61920274a849ab92_597d_4824_bda5_1b50364f399e" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_user_management_sourcetype_count_5084a1d7_f357_4501_9cff_d1db61920274.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_73da9209_249d_4c82_8979_e4724d0d9b0f67ebf884_bce6_4b33_bcdc_7f442d7eceec" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_file_sourcetype_count_73da9209_249d_4c82_8979_e4724d0d9b0f.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_cd351790_3c5c_4f13_a325_b9897f09952abce429ae_10c2_44f1_a9fb_f97598067cc4" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_authentication_sourcetype_count_cd351790_3c5c_4f13_a325_b9897f09952a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_e1ea5f3d_6a90_432d_83c9_c44f7f348156575b6f66_db76_41a3_b1ca_e255126356ae" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_dhcp_sourcetype_count_e1ea5f3d_6a90_432d_83c9_c44f7f348156.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_e69180e7_3caf_4256_8156_e631101edb0e58a1e194_804e_4d32_8182_1cee0566e843" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_powershell_sourcetype_count_e69180e7_3caf_4256_8156_e631101edb0e.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_e85b210d_0b3f_41ae_84a3_7b15c4222ccb41e2006d_b67e_40eb_b685_632bcc2e855a" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_service_sourcetype_count_e85b210d_0b3f_41ae_84a3_7b15c4222ccb.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_visualization" "pie_2_f9a21f42_94be_4af9_b975_e59d61d76d182a5f3672_e91b_46b4_8a35_93adadafa0d1" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a32\"}], \"groups\": [{\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_wmi_sourcetype_count_f9a21f42_94be_4af9_b975_e59d61d76d18.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"source\", \"id\": \"column_3912a8a31\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a32\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"pie\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a32\": {\"name\": \"Count\", \"type\": \"pie\", \"yAxis\": 0}}, \"showDataLabels\": true, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Pie 2"
}
resource "databricks_sql_widget" "r04cdcdfaf78" {
  visualization_id = databricks_sql_visualization.table_5107347e_59da_497e_9cdc_ef5c5ef34cb44f3401dd_cc56_45fd_a3bf_9b09da53a5d1.visualization_id
  title            = "File Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "r05220f5d503" {
  visualization_id = databricks_sql_visualization.pie_2_73da9209_249d_4c82_8979_e4724d0d9b0f67ebf884_bce6_4b33_bcdc_7f442d7eceec.visualization_id
  title            = "File Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "r0a10cdbbc92" {
  visualization_id = databricks_sql_visualization.table_8320c98f_a501_4472_be8e_f72cfbb8e94a46ac5d85_8d99_46e2_a225_3b42c14bfe98.visualization_id
  title            = "WMI top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "r0cbc676a3e1" {
  visualization_id = databricks_sql_visualization.table_fb29145d_47f9_47c6_a957_b1a981b2e4f2dac57ab1_1689_4756_ac2b_c720174551f4.visualization_id
  title            = "DNS Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r10978ee2564" {
  visualization_id = databricks_sql_visualization.table_07748ded_ea55_4253_9919_4ace013263f43c4cfe25_8403_445a_98d8_a77a491fdc9d.visualization_id
  title            = "DNS Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r171b4168d1d" {
  visualization_id = databricks_sql_visualization.table_4f2f389f_8e34_433a_8f13_b4e7f80f72d7b51b2bc8_a66d_4f01_b87e_7b4e47c4d7b5.visualization_id
  title            = "User Management Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "r1c66544ac4f" {
  visualization_id = databricks_sql_visualization.table_7c97e898_e2a6_4c72_9067_2c53b5d8be917eae30fe_bf7a_4f86_9da7_ae888e7db662.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r1e8c57147e9" {
  visualization_id = databricks_sql_visualization.table_da43979f_08a8_4c00_9fa1_8eb68833469f66d1e13a_5772_4cc0_b08c_f8ac10e50c41.visualization_id
  title            = "Process Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "r201ea52c53f" {
  visualization_id = databricks_sql_visualization.table_31df8238_b24d_4966_b5b0_027a7f62a96aeb2c853d_9c7e_4f43_8cf6_a37924239a30.visualization_id
  title            = "Service top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r23b45addd44" {
  visualization_id = databricks_sql_visualization.table_3c205459_30b5_4404_b1a7_93010e74dd2697ed6bb2_6522_4619_bf6e_0a94150c8643.visualization_id
  title            = "Network Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "r2ad4dd33eec" {
  visualization_id = databricks_sql_visualization.table_7f4fc274_cd65_419e_a035_a6530fd99ba7964d9833_c2a4_46ab_af41_1d773fd5eac8.visualization_id
  title            = "Service Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r2ef415d4a68" {
  visualization_id = databricks_sql_visualization.login_activities_e91f71e9_cb2f_46d9_929c_11ef1aa9cc3db5d7430f_78bc_4898_80d8_0cb98214b7b1.visualization_id
  title            = "Detailed Service activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r315b8d8c929" {
  visualization_id = databricks_sql_visualization.table_3d924bdb_c86a_4643_98bd_0187c7b3bb4d35baf743_0ece_4ed4_9dcd_6fef4b4d70a8.visualization_id
  title            = "Network event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "r3e3f514b328" {
  visualization_id = databricks_sql_visualization.table_123be039_85b3_4e49_8f91_2cef3d1f41783e3a7dec_b953_4d79_be53_b06cf331f51c.visualization_id
  title            = "File Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "r40ce1789d3a" {
  visualization_id = databricks_sql_visualization.pie_2_4395a316_d119_477b_8f9a_83c7a7e0855ef7eb3507_9cf7_4e04_a3f1_faced3c03d48.visualization_id
  title            = "Process Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "r4123d10714d" {
  visualization_id = databricks_sql_visualization.login_activities_dfff1d53_d39c_4f1f_9e83_417970b19453208cf03c_ab63_4ecb_8075_2078cd0bb3a7.visualization_id
  title            = "Detailed DNS activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r485450d1041" {
  visualization_id = databricks_sql_visualization.pie_2_1743c8f7_a7be_4d35_b215_85533e32faa22eacc652_3c08_4133_9110_873e0fe8b61c.visualization_id
  title            = "Registry Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r495ce4af45e" {
  visualization_id = databricks_sql_visualization.table_bbfc631c_02c0_419d_bea2_cb9fb8e882025f5346e1_b049_4cc1_9436_053ba895b373.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r4aa800472ce" {
  visualization_id = databricks_sql_visualization.table_ef68f5fc_1bbd_413d_b7aa_52dbad4cd481dc4f3079_169d_42be_bacb_7ab3442fb311.visualization_id
  title            = "Network top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "r4ae9e0b0257" {
  visualization_id = databricks_sql_visualization.table_f1aafebb_e492_4e09_8873_5420cdafb69b1cf73d31_b0a2_4563_b944_ab59a5eaa4f2.visualization_id
  title            = "File Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "r523376d1a99" {
  visualization_id = databricks_sql_visualization.table_ccd7c70c_96ae_40c9_a103_8ad90621b14ebead05e8_d638_408f_9496_dd9446342f6d.visualization_id
  title            = "WMI Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "r532063a6fa0" {
  visualization_id = databricks_sql_visualization.table_9e7411ec_40d4_47dc_b723_7d2110a4a04a116d7a3f_5ccf_4827_bdea_3492e292341e.visualization_id
  title            = "DNS top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r533b31a61d2" {
  visualization_id = databricks_sql_visualization.table_dd25dec7_0ce4_49f7_8726_48dbc1469936d4883145_ee77_49da_9b01_3c581512eb7a.visualization_id
  title            = "Service event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r55f848ded27" {
  visualization_id = databricks_sql_visualization.login_activities_b602bcd9_c744_4dbe_b268_eeee17323041405c42c5_390d_4e21_aa95_eae363bd7b26.visualization_id
  title            = "Detailed Process activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "r5a1259675a9" {
  visualization_id = databricks_sql_visualization.login_activities_e2d28a6e_93d5_4d17_b188_4e775c35e55c47e4d422_4215_42b4_b806_a4b59c1fb990.visualization_id
  title            = "Detailed Registry activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r5b84ee43e6d" {
  visualization_id = databricks_sql_visualization.table_fda329cd_7e59_41ac_a440_8ea233fc0146b91f087b_1b60_4096_a130_fb800ea7dff5.visualization_id
  title            = "User Management event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "r625a3b03c47" {
  visualization_id = databricks_sql_visualization.table_51e345c4_496e_490a_ad50_c20ea90de287170afb4e_78a7_4c55_ac5f_329ffda292d2.visualization_id
  title            = "User Management Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "r639ccfefd7d" {
  visualization_id = databricks_sql_visualization.table_a8d28bcf_bc2c_4a16_9c51_67292a38aae84e61a4a8_9db4_4b4d_88a2_0a07ce3b3406.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r64c68c41a5a" {
  visualization_id = databricks_sql_visualization.table_16f1f2b0_c635_4230_a3f0_f4dae3ab221d908af82a_0056_43f3_8c95_385051018a1c.visualization_id
  title            = "User Management top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "r6fbc611b261" {
  visualization_id = databricks_sql_visualization.login_activities_5c84419b_4c49_4df6_a8ef_2b4b60372f8ff11e6951_b11f_4bce_bc8f_eb28974ee7d0.visualization_id
  title            = "Detailed WMI activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "r7008c36b5ae" {
  visualization_id = databricks_sql_visualization.table_98924f85_8488_47ec_910c_8232b2c7cf232bd0e308_03e3_4766_a2bc_6bb156a6918c.visualization_id
  title            = "Registry Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r7170ab8d83a" {
  visualization_id = databricks_sql_visualization.pie_2_169c87d4_555a_4729_872f_00a66ff237d137382e75_d5f0_4a8c_b67d_57ad6d79359a.visualization_id
  title            = "Network Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "r735bbf8b55c" {
  visualization_id = databricks_sql_visualization.table_9030d896_fe5a_4b74_9375_4420594800f8d487899c_fd86_4970_9af7_3d3c87a72853.visualization_id
  title            = "DNS event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r747265451f0" {
  visualization_id = databricks_sql_visualization.table_a3291f08_51a3_4f6a_bf19_7b3875f55b89ddb1b4fd_9ec2_4075_b498_4b62c930668a.visualization_id
  title            = "Registry top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r76ca5ef85b4" {
  visualization_id = databricks_sql_visualization.table_9ace7fbb_ab9f_4473_a932_a518d613671a01a127ae_f7fe_4224_ad5f_06effaa0738b.visualization_id
  title            = "Service Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r7d9f3e0660d" {
  visualization_id = databricks_sql_visualization.table_3dd3efce_dce5_41ae_9e32_d38eb99e7241051ac851_92bc_4711_abc7_41c3cb954ce7.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "r8328bb07d63" {
  visualization_id = databricks_sql_visualization.table_e966f802_866d_412d_afa9_c7648c5e0a8d656b6d3f_8e8f_4d50_89a2_04a6b8184e52.visualization_id
  title            = "Registry event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r8b9c03eedec" {
  visualization_id = databricks_sql_visualization.table_a081110e_e36d_4607_8491_1e4d034f2d633ad4310e_b785_4a12_9e03_bc41f7916b42.visualization_id
  title            = "Process Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "r8c73ddbc371" {
  visualization_id = databricks_sql_visualization.table_79d17105_7de6_4299_998f_039067cfda007e690df7_e60f_473b_8eba_e24198552f6f.visualization_id
  title            = "DNS Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r8f0b18cd334" {
  visualization_id = databricks_sql_visualization.pie_2_5084a1d7_f357_4501_9cff_d1db61920274a849ab92_597d_4824_bda5_1b50364f399e.visualization_id
  title            = "User Management Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "r904ef02b13c" {
  visualization_id = databricks_sql_visualization.table_48f31d24_4c10_4c14_8946_4536dfa6262ac5962794_b782_4650_8195_f1a559ccb04b.visualization_id
  title            = "Registry Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "r90a8d3b9593" {
  visualization_id = databricks_sql_visualization.pie_2_e85b210d_0b3f_41ae_84a3_7b15c4222ccb41e2006d_b67e_40eb_b685_632bcc2e855a.visualization_id
  title            = "Service Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "r91c9fe0f289" {
  visualization_id = databricks_sql_visualization.table_cceb855c_2d34_4f29_a379_ca77090df411199605fb_17cc_4610_bf35_cfe604dea1cf.visualization_id
  title            = "WMI event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "r993ff0ec104" {
  visualization_id = databricks_sql_visualization.pie_2_03d2aa78_927a_4409_b73a_aa5f4e445ce017e61ec8_e644_4875_b49f_f0188fdc2368.visualization_id
  title            = "DNS Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12.id
}
resource "databricks_sql_widget" "r99a35129e7b" {
  visualization_id = databricks_sql_visualization.pie_2_f9a21f42_94be_4af9_b975_e59d61d76d182a5f3672_e91b_46b4_8a35_93adadafa0d1.visualization_id
  title            = "WMI Activity by source"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "r9edbf4e0b16" {
  visualization_id = databricks_sql_visualization.table_04801c3f_9bf1_42a3_ad45_16ae0f78e976859e2195_a67c_45bd_b3f9_cb752b46b8f5.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "ra13ce7895ca" {
  visualization_id = databricks_sql_visualization.table_0be57781_8e17_4729_b466_94dd863e37a69ed1350d_00d4_4673_a8b7_00658bcd626f.visualization_id
  title            = "Service Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89.id
}
resource "databricks_sql_widget" "rad9d394369c" {
  visualization_id = databricks_sql_visualization.table_36c09ff1_c08c_4941_ab1c_f7d3516dca92e3219da6_9414_44b6_a0fa_d4704424ba12.visualization_id
  title            = "Process top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "rb1ed863ee4e" {
  visualization_id = databricks_sql_visualization.table_3e6994f6_675d_45c1_99a5_f8a55ec7c6bb12612539_8927_4ff4_afd9_8da3f18e4b7c.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "rb3a17182bfa" {
  visualization_id = databricks_sql_visualization.table_180bad07_991f_43a9_a985_c50b340196eea87b1148_4c93_4443_9840_bb184f420572.visualization_id
  title            = "Process event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "rb3ee4d931e2" {
  visualization_id = databricks_sql_visualization.table_b5d0295b_bff9_497b_b33a_d12b4d9707184433538b_d51d_4472_8fa4_c0a16affcad7.visualization_id
  title            = "Network Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "rbb71afd8d98" {
  visualization_id = databricks_sql_visualization.table_0e97bc99_841f_4d6b_ace0_f607c318c749cb3c4fef_4c91_43b7_a991_075db31fe81a.visualization_id
  title            = "Registry Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6.id
}
resource "databricks_sql_widget" "rbd7e260d098" {
  visualization_id = databricks_sql_visualization.table_6afc3d0e_8d13_44de_af96_64134e700b0f266619b3_ffd8_48e4_8dbd_431caa2323bb.visualization_id
  title            = "WMI Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "rc9c95dc7222" {
  visualization_id = databricks_sql_visualization.table_eb740335_602a_43cb_bcfc_b90422c36f1154708fc5_e77f_47aa_8cd0_6e2a7265ca95.visualization_id
  title            = "File top 10 devices"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "rd0d63699ac0" {
  visualization_id = databricks_sql_visualization.login_activities_5871ab2f_cac8_4b98_a9f5_77439c57e35f79e0c77a_daea_4e86_a65b_2b7b1a70f56a.visualization_id
  title            = "Detailed Network activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "rd0d9edb8c67" {
  visualization_id = databricks_sql_visualization.table_059f4129_e183_4e4d_b664_3b710d2d25a1739cf9e4_9836_4b3a_bf49_fc9c7b43158b.visualization_id
  title            = "Process Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "re6ebc85378f" {
  visualization_id = databricks_sql_visualization.login_activities_6df1dbea_4e33_43da_ab00_b2d870f2eda527c28c14_9771_419d_8b4f_b4e3964941b7.visualization_id
  title            = "Detailed File activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "reca7b375d55" {
  visualization_id = databricks_sql_visualization.table_64e6c7dd_a700_429a_b638_7f4d0c9bd7d72a2e82c0_943c_448d_a5f8_6361cc691936.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753.id
}
resource "databricks_sql_widget" "rf027f23f8c3" {
  visualization_id = databricks_sql_visualization.table_3a5584dd_d54b_409f_b20c_1b4d32b6f386754e66fc_848d_401a_aa10_6701c1fb36b7.visualization_id
  title            = "User Management Activity over time by source type "
  position {
    size_y = 5
    size_x = 3
    pos_y  = 8
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "rf0aa41fee09" {
  visualization_id = databricks_sql_visualization.table_0b354d30_b630_4c01_b448_043a1e294c821f115779_ce16_42ac_b2cb_4c1c03429d6c.visualization_id
  title            = "File event results"
  position {
    size_y = 5
    size_x = 3
    pos_y  = 3
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8.id
}
resource "databricks_sql_widget" "rf68c1ff3da6" {
  visualization_id = databricks_sql_visualization.table_950adad8_2f18_4bb3_9ca1_8ae7e5e0c8675864b88f_2965_4275_b70d_9b7fecdca34e.visualization_id
  title            = "WMI Events"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2.id
}
resource "databricks_sql_widget" "rfad419f6691" {
  visualization_id = databricks_sql_visualization.login_activities_87a27553_4bce_4564_ac67_2ed4c27ddd9d9d684b62_96c5_4fb4_8d9e_b1ea19be8faf.visualization_id
  title            = "Detailed User Management activity"
  position {
    size_y = 14
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    title  = "Select event time period"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_widget" "rfc5087d8b2f" {
  visualization_id = databricks_sql_visualization.table_636a958e_9340_4241_861e_e3f72488f3234ecfbaa1_bd6a_4a9a_845b_3f9a4c3cbe12.visualization_id
  title            = "Network Events"
  position {
    size_y = 3
    size_x = 2
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86.id
}
resource "databricks_sql_widget" "rfebac222d3b" {
  visualization_id = databricks_sql_visualization.table_a36432d2_8bc8_4f65_bfdd_4ed4011da00d67d545e4_3c43_4964_91c4_88c6bf23c520.visualization_id
  title            = "Sending Hosts"
  position {
    size_y = 3
    size_x = 2
    pos_x  = 4
  }
  dashboard_id = databricks_sql_dashboard.silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137.id
}
resource "databricks_sql_dashboard" "silvio_sirens_authentication_ea43b4fa_63c6_4533_99c7_6e63b584bfed" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Authentication"
}
resource "databricks_sql_dashboard" "silvio_sirens_dhcp_c117791e_44cc_451c_a703_92501a9611ba" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - DHCP"
}
resource "databricks_sql_dashboard" "silvio_sirens_dns_6f5a4bd2_ecb8_4182_b207_e5a9fe6f5a12" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - DNS"
}
resource "databricks_sql_dashboard" "silvio_sirens_file_7b6f111e_d343_4b4c_9118_ff0b63bd49a8" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - File"
}
resource "databricks_sql_dashboard" "silvio_sirens_network_664a1735_a5f4_4520_b068_8a339e45af86" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Network"
}
resource "databricks_sql_dashboard" "silvio_sirens_powershell_dd49c3d1_323d_43c7_8fb6_e16b38178a0b" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Powershell"
}
resource "databricks_sql_dashboard" "silvio_sirens_process_643d44c5_8d95_4057_af7d_31d802100753" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Process"
}
resource "databricks_sql_dashboard" "silvio_sirens_registry_49908a0e_6d45_4584_b848_5d2538362fc6" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Registry"
}
resource "databricks_sql_dashboard" "silvio_sirens_service_8600591b_3f2b_464c_9724_bdb7e468cc89" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Service"
}
resource "databricks_sql_dashboard" "silvio_sirens_user_management_9a6ec999_e7f1_4d1b_a59c_eba24431b137" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - User Management"
}
resource "databricks_sql_dashboard" "silvio_sirens_web_f8d62b87_65dd_4f9c_a41d_b13a57197868" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Web"
}
resource "databricks_sql_dashboard" "silvio_sirens_wmi_095b0572_b16c_4235_b31f_b9a030dcc8c2" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - WMI"
}
resource "databricks_sql_visualization" "table_04801c3f_9bf1_42a3_ad45_16ae0f78e976859e2195_a67c_45bd_b3f9_cb752b46b8f5" {
  type     = "counter"
  query_id = databricks_sql_query.clone_file_unique_hosts_04801c3f_9bf1_42a3_ad45_16ae0f78e976.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_059f4129_e183_4e4d_b664_3b710d2d25a1739cf9e4_9836_4b3a_bf49_fc9c7b43158b" {
  type     = "counter"
  query_id = databricks_sql_query.clone_process_count_last_hour_059f4129_e183_4e4d_b664_3b710d2d25a1.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_06076c8d_a0ba_4f87_a2a9_899eb8a488330c9cf2bc_542c_4f20_b2de_237fd54aeb35" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_dhcp_sourcetype_by_time_06076c8d_a0ba_4f87_a2a9_899eb8a48833.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_07748ded_ea55_4253_9919_4ace013263f43c4cfe25_8403_445a_98d8_a77a491fdc9d" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_dns_sourcetype_by_time_07748ded_ea55_4253_9919_4ace013263f4.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_0b354d30_b630_4c01_b448_043a1e294c821f115779_ce16_42ac_b2cb_4c1c03429d6c" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_file_event_result_0b354d30_b630_4c01_b448_043a1e294c82.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_0be57781_8e17_4729_b466_94dd863e37a69ed1350d_00d4_4673_a8b7_00658bcd626f" {
  type     = "counter"
  query_id = databricks_sql_query.clone_service_total_event_count_0be57781_8e17_4729_b466_94dd863e37a6.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_0e97bc99_841f_4d6b_ace0_f607c318c749cb3c4fef_4c91_43b7_a991_075db31fe81a" {
  type     = "counter"
  query_id = databricks_sql_query.clone_registry_total_event_count_0e97bc99_841f_4d6b_ace0_f607c318c749.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_0ecba174_8c92_4c41_b459_da00319ed10b9dc1fda0_9013_486b_a7ca_3189235998a6" {
  type     = "counter"
  query_id = databricks_sql_query.clone_authentication_total_event_count_0ecba174_8c92_4c41_b459_da00319ed10b.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_12327a38_bc47_49d4_b80f_8b56e83ecdea27c335be_5603_4d13_9ffd_43bfdaab6cc8" {
  type     = "counter"
  query_id = databricks_sql_query.clone_authentication_count_last_hour_12327a38_bc47_49d4_b80f_8b56e83ecdea.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_123be039_85b3_4e49_8f91_2cef3d1f41783e3a7dec_b953_4d79_be53_b06cf331f51c" {
  type     = "counter"
  query_id = databricks_sql_query.clone_file_count_last_hour_123be039_85b3_4e49_8f91_2cef3d1f4178.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_16f1f2b0_c635_4230_a3f0_f4dae3ab221d908af82a_0056_43f3_8c95_385051018a1c" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_user_management_top_10_hosts_16f1f2b0_c635_4230_a3f0_f4dae3ab221d.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Host\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_180bad07_991f_43a9_a985_c50b340196eea87b1148_4c93_4443_9840_bb184f420572" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_process_event_result_180bad07_991f_43a9_a985_c50b340196ee.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_24b50f02_db2b_499f_8670_420ad57f75686436921c_503f_4ae5_b2ac_4ad8348d461c" {
  type     = "counter"
  query_id = databricks_sql_query.clone_web_total_event_count_24b50f02_db2b_499f_8670_420ad57f7568.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_2d265db2_dfe5_41df_b327_2db83c317550239f5e51_884c_430f_bd3f_92fc8a8843a9" {
  type     = "counter"
  query_id = databricks_sql_query.clone_powershell_total_event_count_2d265db2_dfe5_41df_b327_2db83c317550.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_3026fa32_8590_4b2f_a47b_67587b446456d6f74bc7_2135_4f71_9839_8b86c9cf3b95" {
  type     = "counter"
  query_id = databricks_sql_query.clone_dhcp_unique_hosts_3026fa32_8590_4b2f_a47b_67587b446456.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_31df8238_b24d_4966_b5b0_027a7f62a96aeb2c853d_9c7e_4f43_8cf6_a37924239a30" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_service_top_10_hosts_31df8238_b24d_4966_b5b0_027a7f62a96a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Host\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_36c09ff1_c08c_4941_ab1c_f7d3516dca92e3219da6_9414_44b6_a0fa_d4704424ba12" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_process_top_10_hosts_36c09ff1_c08c_4941_ab1c_f7d3516dca92.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_3a08b16f_0310_4c84_8045_4e4340f8599a34d7e23f_dc56_406b_b253_febd90ecb390" {
  type     = "counter"
  query_id = databricks_sql_query.clone_authentication_unique_hosts_3a08b16f_0310_4c84_8045_4e4340f8599a.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_3a5584dd_d54b_409f_b20c_1b4d32b6f386754e66fc_848d_401a_aa10_6701c1fb36b7" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_user_management_sourcetype_by_time_3a5584dd_d54b_409f_b20c_1b4d32b6f386.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_3c205459_30b5_4404_b1a7_93010e74dd2697ed6bb2_6522_4619_bf6e_0a94150c8643" {
  type     = "counter"
  query_id = databricks_sql_query.clone_network_count_last_hour_3c205459_30b5_4404_b1a7_93010e74dd26.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_3d924bdb_c86a_4643_98bd_0187c7b3bb4d35baf743_0ece_4ed4_9dcd_6fef4b4d70a8" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_network_event_result_3d924bdb_c86a_4643_98bd_0187c7b3bb4d.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_3dd3efce_dce5_41ae_9e32_d38eb99e7241051ac851_92bc_4711_abc7_41c3cb954ce7" {
  type     = "counter"
  query_id = databricks_sql_query.clone_network_unique_hosts_3dd3efce_dce5_41ae_9e32_d38eb99e7241.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_3e6994f6_675d_45c1_99a5_f8a55ec7c6bb12612539_8927_4ff4_afd9_8da3f18e4b7c" {
  type     = "counter"
  query_id = databricks_sql_query.clone_wmi_unique_hosts_3e6994f6_675d_45c1_99a5_f8a55ec7c6bb.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_44fb56b1_de41_44b0_ab92_f570ec64b75c7fb63825_eabc_4fbc_ae96_21eeb0ccd7b8" {
  type     = "counter"
  query_id = databricks_sql_query.clone_dhcp_total_event_count_44fb56b1_de41_44b0_ab92_f570ec64b75c.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_48f31d24_4c10_4c14_8946_4536dfa6262ac5962794_b782_4650_8195_f1a559ccb04b" {
  type     = "counter"
  query_id = databricks_sql_query.clone_registry_count_last_hour_48f31d24_4c10_4c14_8946_4536dfa6262a.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_4a6f84c8_46dc_4a2f_b3d9_d92a08004770123dc0e5_a756_4e9d_88d3_783f8b8eb2fc" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}, {\"column\": \"user\"}], \"groups\": [{\"column\": \"user\"}]}"
  query_id   = databricks_sql_query.clone_authentication_top_10_users_4a6f84c8_46dc_4a2f_b3d9_d92a08004770.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"user\", \"id\": \"column_a811bba81\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_4d1c5f84_2dff_472f_ad1e_d1bdd4023f3055d4df90_959f_4bc4_9635_ab09ca2875d6" {
  type     = "counter"
  query_id = databricks_sql_query.clone_web_unique_hosts_4d1c5f84_2dff_472f_ad1e_d1bdd4023f30.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_4f2f389f_8e34_433a_8f13_b4e7f80f72d7b51b2bc8_a66d_4f01_b87e_7b4e47c4d7b5" {
  type     = "counter"
  query_id = databricks_sql_query.clone_user_management_total_event_count_4f2f389f_8e34_433a_8f13_b4e7f80f72d7.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_5107347e_59da_497e_9cdc_ef5c5ef34cb44f3401dd_cc56_45fd_a3bf_9b09da53a5d1" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_file_sourcetype_by_time_5107347e_59da_497e_9cdc_ef5c5ef34cb4.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_51e345c4_496e_490a_ad50_c20ea90de287170afb4e_78a7_4c55_ac5f_329ffda292d2" {
  type     = "counter"
  query_id = databricks_sql_query.clone_user_management_count_last_hour_51e345c4_496e_490a_ad50_c20ea90de287.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_611a9511_3ea5_4f92_aa5f_6eb1c90bdbad4e9565b9_8358_42d9_973e_9355a1cda6a4" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_web_event_result_611a9511_3ea5_4f92_aa5f_6eb1c90bdbad.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_636a958e_9340_4241_861e_e3f72488f3234ecfbaa1_bd6a_4a9a_845b_3f9a4c3cbe12" {
  type     = "counter"
  query_id = databricks_sql_query.clone_network_total_event_count_636a958e_9340_4241_861e_e3f72488f323.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_64e6c7dd_a700_429a_b638_7f4d0c9bd7d72a2e82c0_943c_448d_a5f8_6361cc691936" {
  type     = "counter"
  query_id = databricks_sql_query.clone_process_unique_hosts_64e6c7dd_a700_429a_b638_7f4d0c9bd7d7.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_6afc3d0e_8d13_44de_af96_64134e700b0f266619b3_ffd8_48e4_8dbd_431caa2323bb" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_wmi_sourcetype_by_time_6afc3d0e_8d13_44de_af96_64134e700b0f.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_7759471b_4fbe_41d8_afe1_18aae08da02521ed904d_d7cb_4e97_9b3e_3b86c658c1c6" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}, {\"column\": \"user\"}], \"groups\": [{\"column\": \"user\"}]}"
  query_id   = databricks_sql_query.clone_dhcp_top_10_hosts_7759471b_4fbe_41d8_afe1_18aae08da025.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"user\", \"id\": \"column_a811bba81\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_79d17105_7de6_4299_998f_039067cfda007e690df7_e60f_473b_8eba_e24198552f6f" {
  type     = "counter"
  query_id = databricks_sql_query.clone_dns_count_last_hour_79d17105_7de6_4299_998f_039067cfda00.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_7c97e898_e2a6_4c72_9067_2c53b5d8be917eae30fe_bf7a_4f86_9da7_ae888e7db662" {
  type     = "counter"
  query_id = databricks_sql_query.clone_service_unique_hosts_7c97e898_e2a6_4c72_9067_2c53b5d8be91.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_7e993c78_1469_49d9_ac0c_6c54ef0d13761144eb6b_636d_47d2_82d4_d6c1e29817cf" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_powershell_event_result_7e993c78_1469_49d9_ac0c_6c54ef0d1376.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_7f4fc274_cd65_419e_a035_a6530fd99ba7964d9833_c2a4_46ab_af41_1d773fd5eac8" {
  type     = "counter"
  query_id = databricks_sql_query.clone_service_count_last_hour_7f4fc274_cd65_419e_a035_a6530fd99ba7.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_8320c98f_a501_4472_be8e_f72cfbb8e94a46ac5d85_8d99_46e2_a225_3b42c14bfe98" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_wmi_top_10_hosts_8320c98f_a501_4472_be8e_f72cfbb8e94a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Host\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_8958f20e_a76d_4b85_b9dd_599f2eb98597403f0e3f_e789_40b0_a58b_8e218e34303b" {
  type     = "counter"
  query_id = databricks_sql_query.clone_dhcp_count_last_hour_8958f20e_a76d_4b85_b9dd_599f2eb98597.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_9030d896_fe5a_4b74_9375_4420594800f8d487899c_fd86_4970_9af7_3d3c87a72853" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_dns_event_result_9030d896_fe5a_4b74_9375_4420594800f8.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_950adad8_2f18_4bb3_9ca1_8ae7e5e0c8675864b88f_2965_4275_b70d_9b7fecdca34e" {
  type     = "counter"
  query_id = databricks_sql_query.clone_wmi_count_last_hour_950adad8_2f18_4bb3_9ca1_8ae7e5e0c867.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_98924f85_8488_47ec_910c_8232b2c7cf232bd0e308_03e3_4766_a2bc_6bb156a6918c" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_registry_sourcetype_by_time_98924f85_8488_47ec_910c_8232b2c7cf23.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_9ace7fbb_ab9f_4473_a932_a518d613671a01a127ae_f7fe_4224_ad5f_06effaa0738b" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_service_sourcetype_by_time_9ace7fbb_ab9f_4473_a932_a518d613671a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_9e7411ec_40d4_47dc_b723_7d2110a4a04a116d7a3f_5ccf_4827_bdea_3492e292341e" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}, {\"column\": \"user\"}], \"groups\": [{\"column\": \"user\"}]}"
  query_id   = databricks_sql_query.clone_dns_top_10_hosts_9e7411ec_40d4_47dc_b723_7d2110a4a04a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"user\", \"id\": \"column_a811bba81\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_a081110e_e36d_4607_8491_1e4d034f2d633ad4310e_b785_4a12_9e03_bc41f7916b42" {
  type     = "counter"
  query_id = databricks_sql_query.clone_process_total_event_count_a081110e_e36d_4607_8491_1e4d034f2d63.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_a3291f08_51a3_4f6a_bf19_7b3875f55b89ddb1b4fd_9ec2_4075_b498_4b62c930668a" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_registry_top_10_hosts_a3291f08_51a3_4f6a_bf19_7b3875f55b89.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_a36432d2_8bc8_4f65_bfdd_4ed4011da00d67d545e4_3c43_4964_91c4_88c6bf23c520" {
  type     = "counter"
  query_id = databricks_sql_query.clone_user_management_unique_hosts_a36432d2_8bc8_4f65_bfdd_4ed4011da00d.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_a798fddd_0c91_4904_a45c_1bcd98b2d326c293d0d3_8cb9_400f_a2b9_36d8934abf12" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_powershell_sourcetype_by_time_a798fddd_0c91_4904_a45c_1bcd98b2d326.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_a8d28bcf_bc2c_4a16_9c51_67292a38aae84e61a4a8_9db4_4b4d_88a2_0a07ce3b3406" {
  type     = "counter"
  query_id = databricks_sql_query.clone_dns_unique_hosts_a8d28bcf_bc2c_4a16_9c51_67292a38aae8.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_a9848142_e585_44b0_a601_db82d2b74d5a8831f2d3_b73f_43a1_97e6_740e34d18193" {
  type     = "counter"
  query_id = databricks_sql_query.clone_powershell_count_last_hour_a9848142_e585_44b0_a601_db82d2b74d5a.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_aea7dd5b_6d43_4d1f_aafd_bc633b8d1d7a1d5fbd29_8d1d_422e_9d6b_6812fcd2eb90" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_powershell_top_10_hosts_aea7dd5b_6d43_4d1f_aafd_bc633b8d1d7a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_b5d0295b_bff9_497b_b33a_d12b4d9707184433538b_d51d_4472_8fa4_c0a16affcad7" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_network_sourcetype_by_time_b5d0295b_bff9_497b_b33a_d12b4d970718.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_b6a41fb5_2534_4af9_8995_295c2915bbb006810c33_1e37_479f_b05a_5c93dd062176" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_authentication_event_result_b6a41fb5_2534_4af9_8995_295c2915bbb0.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_bbfc631c_02c0_419d_bea2_cb9fb8e882025f5346e1_b049_4cc1_9436_053ba895b373" {
  type     = "counter"
  query_id = databricks_sql_query.clone_registry_unique_hosts_bbfc631c_02c0_419d_bea2_cb9fb8e88202.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_bf07209f_cfb0_46fa_b093_8cb00977128956d2c6d9_9c5f_47fa_bebf_23d69e5f7b2d" {
  type     = "counter"
  query_id = databricks_sql_query.clone_web_count_last_hour_bf07209f_cfb0_46fa_b093_8cb009771289.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Last hour \", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_c99780f9_25ed_46b0_87ab_cce6923f6f9b870552d4_c284_462c_b820_bf3ba90e92a3" {
  type     = "counter"
  query_id = databricks_sql_query.clone_powershell_unique_hosts_c99780f9_25ed_46b0_87ab_cce6923f6f9b.id
  options  = "{\"condensed\": true, \"counterColName\": \"count(DISTINCT dvc_hostname)\", \"counterLabel\": \"Unique hosts\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_ccd7c70c_96ae_40c9_a103_8ad90621b14ebead05e8_d638_408f_9496_dd9446342f6d" {
  type     = "counter"
  query_id = databricks_sql_query.clone_wmi_total_event_count_ccd7c70c_96ae_40c9_a103_8ad90621b14e.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_cceb855c_2d34_4f29_a379_ca77090df411199605fb_17cc_4610_bf35_cfe604dea1cf" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_wmi_event_result_cceb855c_2d34_4f29_a379_ca77090df411.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_d6144d24_61c3_4d0d_aad9_22d7f1f58d4a916dc6fa_5074_4511_98e7_021842cf447d" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_dhcp_event_result_d6144d24_61c3_4d0d_aad9_22d7f1f58d4a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_d7928374_bc68_4f1f_8450_11959b282723df0faa0f_2d46_40a0_88e0_ce4aba317e33" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_web_sourcetype_by_time_d7928374_bc68_4f1f_8450_11959b282723.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_d9ed2296_6089_4510_bcc2_9cea70e3017a659b3225_ffb5_46b4_8609_44ec64a6f9b8" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}, {\"column\": \"source\"}], \"groups\": [{\"column\": \"time\"}, {\"column\": \"source\"}]}"
  query_id   = databricks_sql_query.clone_authentication_sourcetype_by_time_d9ed2296_6089_4510_bcc2_9cea70e3017a.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_da43979f_08a8_4c00_9fa1_8eb68833469f66d1e13a_5772_4cc0_b08c_f8ac10e50c41" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"source\"}, {\"column\": \"time\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_3912a8a37\"}], \"groups\": [{\"column\": \"source\"}, {\"column\": \"time\"}]}"
  query_id   = databricks_sql_query.clone_process_sourcetype_by_time_da43979f_08a8_4c00_9fa1_8eb68833469f.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"source\", \"id\": \"column_3912a8a38\"}, \"x\": {\"column\": \"time\", \"id\": \"column_3912a8a35\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_3912a8a37\", \"transform\": \"SUM\"}]}, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"enabled\": true, \"placement\": \"below\", \"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_3912a8a37\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"xAxis\": {\"labels\": {\"enabled\": true}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_dd25dec7_0ce4_49f7_8726_48dbc1469936d4883145_ee77_49da_9b01_3c581512eb7a" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_service_event_result_dd25dec7_0ce4_49f7_8726_48dbc1469936.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_e7031aac_3399_42ad_a24d_1a4b4e54bcf258333814_6e20_4020_8bd6_300dfa5a967d" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_web_top_10_hosts_e7031aac_3399_42ad_a24d_1a4b4e54bcf2.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Host\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_e966f802_866d_412d_afa9_c7648c5e0a8d656b6d3f_8e8f_4d50_89a2_04a6b8184e52" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_registry_event_result_e966f802_866d_412d_afa9_c7648c5e0a8d.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_eb740335_602a_43cb_bcfc_b90422c36f1154708fc5_e77f_47aa_8cd0_6e2a7265ca95" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_file_top_10_hosts_eb740335_602a_43cb_bcfc_b90422c36f11.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_ef68f5fc_1bbd_413d_b7aa_52dbad4cd481dc4f3079_169d_42be_bacb_7ab3442fb311" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"dvc_hostname\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_29c7fba77\"}], \"groups\": [{\"column\": \"dvc_hostname\"}]}"
  query_id   = databricks_sql_query.clone_network_top_10_hosts_ef68f5fc_1bbd_413d_b7aa_52dbad4cd481.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"x\": {\"column\": \"dvc_hostname\", \"id\": \"column_9676829e26710\"}, \"y\": [{\"column\": \"count\", \"id\": \"column_29c7fba77\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"column\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_29c7fba77\": {\"type\": \"column\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"User\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}
resource "databricks_sql_visualization" "table_f1aafebb_e492_4e09_8873_5420cdafb69b1cf73d31_b0a2_4563_b944_ab59a5eaa4f2" {
  type     = "counter"
  query_id = databricks_sql_query.clone_file_total_event_count_f1aafebb_e492_4e09_8873_5420cdafb69b.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_fb29145d_47f9_47c6_a957_b1a981b2e4f2dac57ab1_1689_4756_ac2b_c720174551f4" {
  type     = "counter"
  query_id = databricks_sql_query.clone_dns_total_event_count_fb29145d_47f9_47c6_a957_b1a981b2e4f2.id
  options  = "{\"condensed\": true, \"counterColName\": \"events_count\", \"counterLabel\": \"Total events count\", \"rowNumber\": 1, \"stringDecChar\": \".\", \"stringDecimal\": 0, \"stringThouSep\": \",\", \"targetRowNumber\": 1, \"tooltipFormat\": \"0,0.000\", \"withRowNumber\": true}"
  name     = "Table"
}
resource "databricks_sql_visualization" "table_fda329cd_7e59_41ac_a440_8ea233fc0146b91f087b_1b60_4096_a130_fb800ea7dff5" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"_event_date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count(1)\"}], \"alias\": \"column_726d99943\"}, {\"column\": \"event_result\"}], \"groups\": [{\"column\": \"_event_date\"}, {\"column\": \"event_result\"}]}"
  query_id   = databricks_sql_query.clone_user_management_event_result_fda329cd_7e59_41ac_a440_8ea233fc0146.id
  options    = "{\"alignYAxesAtZero\": false, \"coefficient\": 1, \"columnConfigurationMap\": {\"series\": {\"column\": \"event_result\", \"id\": \"column_726d99946\"}, \"x\": {\"column\": \"_event_date\", \"id\": \"column_726d99941\"}, \"y\": [{\"column\": \"count(1)\", \"id\": \"column_726d99943\", \"transform\": \"SUM\"}]}, \"condensed\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"direction\": {\"type\": \"counterclockwise\"}, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"globalSeriesType\": \"line\", \"isAggregationOn\": true, \"legend\": {\"traceorder\": \"normal\"}, \"missingValuesAsZero\": true, \"numberFormat\": \"0,0[.]00000\", \"percentFormat\": \"0[.]00%\", \"series\": {\"error_y\": {\"type\": \"data\", \"visible\": true}, \"stacking\": null}, \"seriesOptions\": {\"column_726d99943\": {\"type\": \"line\", \"yAxis\": 0}, \"column_726d99945\": {\"type\": \"line\", \"yAxis\": 0}}, \"showDataLabels\": false, \"sizemode\": \"diameter\", \"sortX\": true, \"sortY\": true, \"swappedAxes\": false, \"textFormat\": \"\", \"useAggregationsUi\": true, \"valuesOptions\": {}, \"version\": 2, \"withRowNumber\": true, \"xAxis\": {\"labels\": {\"enabled\": true}, \"title\": {\"text\": \"Event date\"}, \"type\": \"-\"}, \"yAxis\": [{\"title\": {\"text\": \"Count\"}, \"type\": \"-\"}, {\"opposite\": true, \"type\": \"-\"}]}"
  name       = "Table"
}

# DK 22nd Jan 2023. Include exported resources for two dashboards. (Sirens - Threat Hunts and Sirens - Risk Metrics)
#
#
# Threat Hunts
resource "databricks_sql_widget" "b38d7292_6d00_414c_a67a_8870ce34169c03488dbc_0f63_4d2d_bd47_5edaf280ba27" {
  visualization_id = databricks_sql_visualization.bar_1_01bedb7f_c0fd_4b01_a784_5c5d321e8d845a4d723d_db14_45ab_8f1f_93c44673f12c.visualization_id
  title            = "Hunt Hits Per User"
  position {
    size_y = 8
    size_x = 3
    pos_y  = 5
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.sirens_threat_hunts_b38d7292_6d00_414c_a67a_8870ce34169c.id
}
resource "databricks_sql_widget" "b38d7292_6d00_414c_a67a_8870ce34169c875160ec_2912_407d_a570_5581c2069048" {
  visualization_id = databricks_sql_visualization.results_3998366d_82f8_49ae_a924_5746ae32095463b72281_0a55_4d13_a04a_3ade02c54b01.visualization_id
  title            = "Executed Hunts"
  position {
    size_y = 5
    size_x = 3
  }
  parameter {
    type   = "dashboard-level"
    name   = "date_time"
    map_to = "date_time"
  }
  description  = "Hunts executed within period"
  dashboard_id = databricks_sql_dashboard.sirens_threat_hunts_b38d7292_6d00_414c_a67a_8870ce34169c.id
}
resource "databricks_sql_widget" "b38d7292_6d00_414c_a67a_8870ce34169c9235126d_64f8_4a13_871a_5d3d2af84ead" {
  visualization_id = databricks_sql_visualization.pie_1_d98771b7_27b1_40fa_8706_f46d30669c9dd89d0b29_e180_4c23_a752_0d6898940819.visualization_id
  title            = "Hunt Status"
  position {
    size_y = 5
    size_x = 3
    pos_x  = 3
  }
  description  = "Shows the status of recent hunts"
  dashboard_id = databricks_sql_dashboard.sirens_threat_hunts_b38d7292_6d00_414c_a67a_8870ce34169c.id
}
resource "databricks_sql_widget" "b38d7292_6d00_414c_a67a_8870ce34169cd6f28892_bc34_4972_894c_4b0b5068aa85" {
  visualization_id = databricks_sql_visualization.results_a2d84e40_ec8a_4b3c_9192_a0172ca5b2c76298d474_ce5b_40f1_978e_cfbb3caad220.visualization_id
  title            = "Hunt Executions"
  position {
    size_y = 7
    size_x = 6
    pos_y  = 13
  }
  parameter {
    type   = "dashboard-level"
    name   = "date_time"
    map_to = "date_time"
  }
  description  = "Shows the last time a threat hunt executed and resulting status"
  dashboard_id = databricks_sql_dashboard.sirens_threat_hunts_b38d7292_6d00_414c_a67a_8870ce34169c.id
}
resource "databricks_sql_widget" "b38d7292_6d00_414c_a67a_8870ce34169cf6fa0f3e_fc59_469e_88c7_7d40ef43e7a8" {
  visualization_id = databricks_sql_visualization.bar_1_2d2ad029_1e15_4226_b8b0_b012ca96157d4893ab7f_82ea_4ef9_8ecb_5e44cbf0d808.visualization_id
  title            = "Results Per Hunt"
  position {
    size_y = 8
    size_x = 3
    pos_y  = 5
  }
  description  = "Shows the total result count across all analytics in an executed hunt"
  dashboard_id = databricks_sql_dashboard.sirens_threat_hunts_b38d7292_6d00_414c_a67a_8870ce34169c.id
}
resource "databricks_sql_visualization" "bar_1_01bedb7f_c0fd_4b01_a784_5c5d321e8d845a4d723d_db14_45ab_8f1f_93c44673f12c" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"risk_object\"}, {\"column\": \"hunt_name\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"hits\"}], \"alias\": \"column_a0a2807298773\"}], \"groups\": [{\"column\": \"risk_object\"}, {\"column\": \"hunt_name\"}]}"
  query_id   = databricks_sql_query.hunt_hits_per_user_01bedb7f_c0fd_4b01_a784_5c5d321e8d84.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"column\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\", \"title\": {\"text\": \"# of hits\"}}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": \"stack\", \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_a0a2807298773\": {\"name\": \"hits\", \"yAxis\": 0}}, \"valuesOptions\": {}, \"direction\": {\"type\": \"counterclockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"showDataLabels\": false, \"columnConfigurationMap\": {\"x\": {\"column\": \"risk_object\", \"id\": \"column_a0a2807298771\"}, \"series\": {\"column\": \"hunt_name\", \"id\": \"column_a0a2807298772\"}, \"y\": [{\"column\": \"hits\", \"transform\": \"SUM\", \"id\": \"column_a0a2807298773\"}]}, \"isAggregationOn\": true}"
  name       = "Bar 1"
}
resource "databricks_sql_visualization" "bar_1_2d2ad029_1e15_4226_b8b0_b012ca96157d4893ab7f_82ea_4ef9_8ecb_5e44cbf0d808" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"executed_time\"}, {\"column\": \"hunt_name\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"total_result_count\"}], \"alias\": \"column_7d64758d89148\"}], \"groups\": [{\"column\": \"executed_time\"}, {\"column\": \"hunt_name\"}]}"
  query_id   = databricks_sql_query.results_per_hunt_2d2ad029_1e15_4226_b8b0_b012ca96157d.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"column\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\", \"title\": {\"text\": \"Result Count\"}}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": null, \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_7d64758d88320\": {\"name\": \"total_result_count\", \"yAxis\": 0, \"type\": \"column\"}, \"column_7d64758d89148\": {\"yAxis\": 0, \"type\": \"column\"}, \"LSASS_Memory_Read_Access\": {\"color\": \"#FFAB00\"}}, \"valuesOptions\": {}, \"direction\": {\"type\": \"counterclockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": false, \"dateTimeFormat\": \"DD/MM/YYYY HH:mm\", \"showDataLabels\": false, \"columnConfigurationMap\": {\"x\": {\"column\": \"executed_time\", \"id\": \"column_7d64758d111636\"}, \"series\": {\"column\": \"hunt_name\", \"id\": \"column_7d64758d88319\"}, \"y\": [{\"id\": \"column_7d64758d89148\", \"column\": \"total_result_count\", \"transform\": \"SUM\"}]}, \"isAggregationOn\": true, \"hideXAxis\": false, \"condensed\": true, \"withRowNumber\": true}"
  name       = "Bar 1"
}
resource "databricks_sql_visualization" "pie_1_d98771b7_27b1_40fa_8706_f46d30669c9dd89d0b29_e180_4c23_a752_0d6898940819" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"result\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"count\"}], \"alias\": \"column_8e3f9a6420971\"}], \"groups\": [{\"column\": \"result\"}]}"
  query_id   = databricks_sql_query.hunts_by_result_d98771b7_27b1_40fa_8706_f46d30669c9d.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"pie\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\"}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": null, \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_8e3f9a6420971\": {\"name\": \"count\", \"yAxis\": 0, \"type\": \"pie\"}}, \"valuesOptions\": {\"success\": {\"color\": \"#00A972\"}}, \"direction\": {\"type\": \"clockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": false, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"showDataLabels\": true, \"columnConfigurationMap\": {\"x\": {\"column\": \"result\", \"id\": \"column_8e3f9a6420970\"}, \"y\": [{\"column\": \"count\", \"transform\": \"SUM\", \"id\": \"column_8e3f9a6420971\"}]}, \"isAggregationOn\": true}"
  name       = "Pie 1"
}
resource "databricks_sql_visualization" "results_3998366d_82f8_49ae_a924_5746ae32095463b72281_0a55_4d13_a04a_3ade02c54b01" {
  type     = "counter"
  query_id = databricks_sql_query.executed_hunts_timeframe_3998366d_82f8_49ae_a924_5746ae320954.id
  options  = "{\"counterLabel\": \"Executed Hunts\", \"counterColName\": \"total_record_count\", \"rowNumber\": 1, \"targetRowNumber\": 1, \"stringDecimal\": 0, \"stringDecChar\": \".\", \"stringThouSep\": \",\", \"tooltipFormat\": \"0,0.000\", \"targetColName\": \"\", \"formatTargetValue\": false, \"stringSuffix\": \"\", \"countRow\": false}"
  name     = "Results"
}
resource "databricks_sql_visualization" "results_a2d84e40_ec8a_4b3c_9192_a0172ca5b2c76298d474_ce5b_40f1_978e_cfbb3caad220" {
  type     = "table"
  query_id = databricks_sql_query.latest_hunt_executions_a2d84e40_ec8a_4b3c_9192_a0172ca5b2c7.id
  options  = "{\"version\": 2}"
  name     = "Results"
}
resource "databricks_sql_dashboard" "sirens_threat_hunts_b38d7292_6d00_414c_a67a_8870ce34169c" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Threat Hunts"
}

resource "databricks_sql_query" "executed_hunts_timeframe_3998366d_82f8_49ae_a924_5746ae320954" {
  run_as_role = "owner"
  query       = "SELECT COUNT(*) AS total_record_count\nFROM ${var.target_database}.threathunt_index\nWHERE start_time BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\""
  parent      = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetime_range {
      value = "d_today"
    }
  }
  name           = "executed_hunts_timeframe"
   data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "hunt_hits_per_user_01bedb7f_c0fd_4b01_a784_5c5d321e8d84" {
  run_as_role = "owner"
  query       = "SELECT \n        tr.`_risk_column`['risk_object'] AS risk_object,\n        th.hunt_name,\n        count(th.hunt_name) AS hits\nFROM ${var.target_database}.threathunt_results tr\nJOIN ${var.target_database}.threathunt_index th\nON tr.run_id = th.run_id\nWHERE tr.`_risk_column`['object_type'] = 'user'\nAND tr.start_time BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\nGROUP BY  tr.`_risk_column`['risk_object'], th.hunt_name;"
  parent      = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetime_range {
      value = "d_last_30_days"
    }
  }
  name           = "hunt_hits_per_user"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "hunts_by_result_d98771b7_27b1_40fa_8706_f46d30669c9d" {
  run_as_role    = "owner"
  query          = "SELECT result, COUNT(*) AS count\nFROM ${var.target_database}.threathunt_index\nGROUP BY result"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "hunts_by_result"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "latest_hunt_executions_a2d84e40_ec8a_4b3c_9192_a0172ca5b2c7" {
  run_as_role = "owner"
  query       = "SELECT start_time, end_time, hunt_name, result, status, state\nFROM threathunt_index\nWHERE start_time BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\nORDER BY start_time DESC\n\n"
  parent      = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetime_range {
      value = "d_today"
    }
  }
  name           = "latest_hunt_executions"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "results_per_hunt_2d2ad029_1e15_4226_b8b0_b012ca96157d" {
  run_as_role    = "owner"
  query          = "SELECT DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(th.start_time)), 'yyyy-MM-dd HH:mm:ss') AS executed_time, th.hunt_name, SUM(result_count) as total_result_count\nFROM ${var.target_database}.threathunt_results tr\nJOIN ${var.target_database}.threathunt_index th\nON tr.run_id = th.run_id\nGROUP BY executed_time, th.hunt_name"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "results_per_hunt"
  data_source_id = local.sirens_endpoint
}

# Risk Metrics
resource "databricks_sql_visualization" "area_1_b1048a1d_f1bd_4ffe_8626_25fe8490d5e57ae96802_5ea2_4d7a_b3dc_d77b1bf1e542" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"date\"}, {\"column\": \"user_name\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"risk_score\"}], \"alias\": \"column_eee19b9816760\"}], \"groups\": [{\"column\": \"date\"}, {\"column\": \"user_name\"}]}"
  query_id   = databricks_sql_query.risk_score_overtime_by_user_b1048a1d_f1bd_4ffe_8626_25fe8490d5e5.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"area\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\", \"title\": {\"text\": \"Risk Score\"}}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": null, \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_eee19b9816760\": {\"name\": \"risk_score\", \"yAxis\": 0, \"type\": \"area\"}}, \"valuesOptions\": {}, \"direction\": {\"type\": \"counterclockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": false, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"showDataLabels\": false, \"columnConfigurationMap\": {\"x\": {\"column\": \"date\", \"id\": \"column_eee19b9816758\"}, \"series\": {\"column\": \"user_name\", \"id\": \"column_eee19b9816759\"}, \"y\": [{\"column\": \"risk_score\", \"transform\": \"SUM\", \"id\": \"column_eee19b9816760\"}]}, \"isAggregationOn\": true}"
  name       = "Area 1"
}
resource "databricks_sql_visualization" "bar_1_e1bd6a68_ac70_4009_9486_48bcc47f53e52942307c_dde9_4c84_85e8_5d817142fc08" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"user_name\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"risk_score\"}], \"alias\": \"column_866f641f32142\"}, {\"column\": \"mitre_technique\"}], \"groups\": [{\"column\": \"user_name\"}, {\"column\": \"mitre_technique\"}]}"
  query_id   = databricks_sql_query.risk_score_by_user_e1bd6a68_ac70_4009_9486_48bcc47f53e5.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"column\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\", \"title\": {\"text\": \"Risk Score\"}}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": \"stack\", \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_866f641f32142\": {\"yAxis\": 0, \"type\": \"column\"}}, \"valuesOptions\": {}, \"direction\": {\"type\": \"counterclockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": true, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"showDataLabels\": false, \"columnConfigurationMap\": {\"x\": {\"column\": \"user_name\", \"id\": \"column_866f641f32144\"}, \"y\": [{\"id\": \"column_866f641f32142\", \"column\": \"risk_score\", \"transform\": \"SUM\"}], \"series\": {\"column\": \"mitre_technique\", \"id\": \"column_eee19b98172106\"}}, \"isAggregationOn\": true, \"condensed\": true, \"withRowNumber\": true}"
  name       = "Bar 1"
}
resource "databricks_sql_visualization" "bar_1_fc6a3999_b8e8_46c0_8b95_36c079ba0c02d6383204_fb7e_44b5_878e_3d97c6bcfe9c" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"device_name\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"risk_score\"}], \"alias\": \"column_866f641f20647\"}, {\"column\": \"mitre_technique\"}], \"groups\": [{\"column\": \"device_name\"}, {\"column\": \"mitre_technique\"}]}"
  query_id   = databricks_sql_query.risk_score_by_device_fc6a3999_b8e8_46c0_8b95_36c079ba0c02.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"column\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\", \"title\": {\"text\": \"Risk Score\"}}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": \"stack\", \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_866f641f20324\": {\"yAxis\": 0, \"type\": \"column\"}, \"column_866f641f20647\": {\"yAxis\": 0, \"type\": \"column\"}}, \"valuesOptions\": {}, \"direction\": {\"type\": \"counterclockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": true, \"dateTimeFormat\": \"DD/MM/YYYY HH:mm\", \"showDataLabels\": false, \"columnConfigurationMap\": {\"x\": {\"column\": \"device_name\", \"id\": \"column_866f641f20645\"}, \"y\": [{\"id\": \"column_866f641f20647\", \"column\": \"risk_score\", \"transform\": \"SUM\"}], \"series\": {\"column\": \"mitre_technique\", \"id\": \"column_eee19b98193158\"}}, \"isAggregationOn\": true, \"condensed\": true, \"withRowNumber\": true}"
  name       = "Bar 1"
}
resource "databricks_sql_widget" "c534f82a_fce3_4f59_b4e9_d64f81e6a857259d7e4b_bd69_4468_b204_b2e5fc4c2c9b" {
  visualization_id = databricks_sql_visualization.results_3fb3df57_7b6b_4dcf_95bb_cea897074c0f8146d72e_0ead_4b26_bcaf_a48c68c85e00.visualization_id
  title            = "Results - risk_events"
  position {
    size_y = 8
    size_x = 6
    pos_y  = 14
  }
  parameter {
    type   = "dashboard-level"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.sirens_risk_metrics_c534f82a_fce3_4f59_b4e9_d64f81e6a857.id
}
resource "databricks_sql_widget" "c534f82a_fce3_4f59_b4e9_d64f81e6a857439e49a2_81f8_46e9_b81a_0f12994691d7" {
  visualization_id = databricks_sql_visualization.bar_1_e1bd6a68_ac70_4009_9486_48bcc47f53e52942307c_dde9_4c84_85e8_5d817142fc08.visualization_id
  title            = "High Risk Users"
  position {
    size_y = 8
    size_x = 3
    pos_y  = 6
  }
  dashboard_id = databricks_sql_dashboard.sirens_risk_metrics_c534f82a_fce3_4f59_b4e9_d64f81e6a857.id
}
resource "databricks_sql_widget" "c534f82a_fce3_4f59_b4e9_d64f81e6a85747d0951e_cc3d_4d16_a5f1_2e65ab6c3cb2" {
  visualization_id = databricks_sql_visualization.results_69696ae6_e5c8_426f_aae9_752a5936e4e36a25fe22_1e80_4052_b652_a190a2e27623.visualization_id
  title            = "Risk Score Over Time by Device"
  position {
    size_y = 6
    size_x = 3
  }
  parameter {
    type   = "dashboard-level"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.sirens_risk_metrics_c534f82a_fce3_4f59_b4e9_d64f81e6a857.id
}
resource "databricks_sql_widget" "c534f82a_fce3_4f59_b4e9_d64f81e6a857710c1e31_d245_471e_ba03_071c9e6b3f91" {
  visualization_id = databricks_sql_visualization.bar_1_fc6a3999_b8e8_46c0_8b95_36c079ba0c02d6383204_fb7e_44b5_878e_3d97c6bcfe9c.visualization_id
  title            = "High Risk Devices"
  position {
    size_y = 8
    size_x = 3
    pos_y  = 6
    pos_x  = 3
  }
  dashboard_id = databricks_sql_dashboard.sirens_risk_metrics_c534f82a_fce3_4f59_b4e9_d64f81e6a857.id
}
resource "databricks_sql_widget" "c534f82a_fce3_4f59_b4e9_d64f81e6a857875956ff_596f_417d_a476_590bb5978809" {
  visualization_id = databricks_sql_visualization.area_1_b1048a1d_f1bd_4ffe_8626_25fe8490d5e57ae96802_5ea2_4d7a_b3dc_d77b1bf1e542.visualization_id
  title            = "Risk Score Over Time by User"
  position {
    size_y = 6
    size_x = 3
    pos_x  = 3
  }
  parameter {
    type   = "dashboard-level"
    name   = "date_time"
    map_to = "date_time"
  }
  dashboard_id = databricks_sql_dashboard.sirens_risk_metrics_c534f82a_fce3_4f59_b4e9_d64f81e6a857.id
}
resource "databricks_sql_visualization" "results_3fb3df57_7b6b_4dcf_95bb_cea897074c0f8146d72e_0ead_4b26_bcaf_a48c68c85e00" {
  type     = "table"
  query_id = databricks_sql_query.risk_events_3fb3df57_7b6b_4dcf_95bb_cea897074c0f.id
  options  = "{\"version\": 2}"
  name     = "Results"
}
resource "databricks_sql_visualization" "results_69696ae6_e5c8_426f_aae9_752a5936e4e36a25fe22_1e80_4052_b652_a190a2e27623" {
  type       = "chart"
  query_plan = "{\"selects\": [{\"column\": \"date\"}, {\"function\": \"SUM\", \"args\": [{\"column\": \"risk_score\"}], \"alias\": \"column_8e3f9a647649\"}, {\"column\": \"device_name\"}], \"groups\": [{\"column\": \"date\"}, {\"column\": \"device_name\"}]}"
  query_id   = databricks_sql_query.risk_score_overtime_by_device_69696ae6_e5c8_426f_aae9_752a5936e4e3.id
  options    = "{\"version\": 2, \"globalSeriesType\": \"area\", \"sortX\": true, \"sortY\": true, \"legend\": {\"traceorder\": \"normal\"}, \"xAxis\": {\"type\": \"-\", \"labels\": {\"enabled\": true}}, \"yAxis\": [{\"type\": \"-\", \"title\": {\"text\": \"Risk Score\"}}, {\"type\": \"-\", \"opposite\": true}], \"alignYAxesAtZero\": true, \"error_y\": {\"type\": \"data\", \"visible\": true}, \"series\": {\"stacking\": null, \"error_y\": {\"type\": \"data\", \"visible\": true}}, \"seriesOptions\": {\"column_8e3f9a647649\": {\"yAxis\": 0, \"type\": \"area\"}}, \"valuesOptions\": {}, \"direction\": {\"type\": \"counterclockwise\"}, \"sizemode\": \"diameter\", \"coefficient\": 1, \"numberFormat\": \"0,0.[00000]\", \"percentFormat\": \"0[.]00%\", \"textFormat\": \"\", \"missingValuesAsZero\": true, \"useAggregationsUi\": true, \"swappedAxes\": false, \"dateTimeFormat\": \"YYYY-MM-DD HH:mm\", \"showDataLabels\": false, \"columnConfigurationMap\": {\"x\": {\"column\": \"date\", \"id\": \"column_8e3f9a6410577\"}, \"y\": [{\"id\": \"column_8e3f9a647649\", \"column\": \"risk_score\", \"transform\": \"SUM\"}], \"series\": {\"column\": \"device_name\", \"id\": \"column_8e3f9a6410578\"}}, \"isAggregationOn\": true}"
  name       = "Results"
}
resource "databricks_sql_dashboard" "sirens_risk_metrics_c534f82a_fce3_4f59_b4e9_d64f81e6a857" {
  tags   = ["Sirens"]
  parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name   = "Sirens - Risk Metrics"
}

resource "databricks_sql_query" "risk_events_3fb3df57_7b6b_4dcf_95bb_cea897074c0f" {
  run_as_role = "owner"
  query       = "SELECT `_event_time`, risk_object, risk_score, source, annotation['mitre_tactic'] AS mitre_tactic,\nannotation['mitre_technique'] AS mitre_technique from ${var.target_database}.risk\nWHERE _event_time BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\nORDER BY `_event_time` DESC"
  parent      = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetime_range {
      value = "d_last_30_days"
    }
  }
  name           = "risk_events"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "risk_score_by_device_fc6a3999_b8e8_46c0_8b95_36c079ba0c02" {
  run_as_role    = "owner"
  query          = "SELECT risk_object AS device_name,\n      SUM(risk_score) AS risk_score,\n      annotation['mitre_technique'] as `mitre_technique`,\n      annotation['mitre_tactic'] as `mitre_tactic`\nFROM ${var.target_database}.risk\nWHERE object_type = 'system'\nGROUP BY risk_object, annotation['mitre_technique'], annotation['mitre_tactic']\nORDER BY risk_score DESC;"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "risk_score_by_device"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "risk_score_by_user_e1bd6a68_ac70_4009_9486_48bcc47f53e5" {
  run_as_role    = "owner"
  query          = "SELECT risk_object AS user_name,\n      SUM(risk_score) AS risk_score,\n      annotation['mitre_technique'] AS `mitre_technique`,\n      annotation['mitre_tactic'] AS `mitre_tactic`\nFROM ${var.target_database}.risk\nWHERE object_type = 'user'\nGROUP BY risk_object, annotation['mitre_technique'], annotation['mitre_tactic']\nORDER BY risk_score DESC;"
  parent         = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  name           = "risk_score_by_user"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "risk_score_overtime_by_device_69696ae6_e5c8_426f_aae9_752a5936e4e3" {
  run_as_role = "owner"
  query       = "SELECT \n  risk_object AS device_name,\n  DATE_TRUNC('DAY', _event_time) AS date,\n  SUM(risk_score) AS risk_score\nFROM ${var.target_database}.risk\nWHERE \n  object_type = 'system'\n  AND _event_time BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\nGROUP BY \n  risk_object, \n  DATE_TRUNC('DAY', _event_time)\nORDER BY \n  DATE_TRUNC('DAY', _event_time) ASC;"
  parent      = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetime_range {
      value = "d_last_30_days"
    }
  }
  name           = "Risk_score_overtime_by_device"
  data_source_id = local.sirens_endpoint
}
resource "databricks_sql_query" "risk_score_overtime_by_user_b1048a1d_f1bd_4ffe_8626_25fe8490d5e5" {
  run_as_role = "owner"
  query       = "SELECT \n  risk_object AS user_name,\n  DATE_TRUNC('DAY', _event_time) AS date,\n  SUM(risk_score) AS risk_score\nFROM ${var.target_database}.risk\nWHERE \n  object_type = 'user'\n  AND _event_time BETWEEN \"{{date_time.start}}\" and \"{{date_time.end}}\"\nGROUP BY \n  risk_object, \n  DATE_TRUNC('DAY', _event_time)\nORDER BY \n  DATE_TRUNC('DAY', _event_time) ASC;"
  parent      = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"
  parameter {
    title = "date_time"
    name  = "date_time"
    datetime_range {
      value = "d_last_30_days"
    }
  }
  name           = "risk_score_overtime_by_user"
  data_source_id = local.sirens_endpoint
}
