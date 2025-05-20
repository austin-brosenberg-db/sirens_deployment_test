terraform {
  required_version = ">= 1.3.7"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.47.0"
    }

  }
}

provider "databricks" {
  profile = var.profile_name == "" ? null : var.profile_name # only pass if set
}