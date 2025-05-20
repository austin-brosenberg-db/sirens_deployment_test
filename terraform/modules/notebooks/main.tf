terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}


resource "databricks_notebook" "this" {

  path     = "${var.user_home}/${var.notebook_subdirectory}/${var.notebook_name[0]}"
  language = var.notebook_language
  source   = var.notebook_filename
}
