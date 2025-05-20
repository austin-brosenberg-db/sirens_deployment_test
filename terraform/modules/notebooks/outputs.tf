output "notebook_url" {
  value = databricks_notebook.this.url
}

output "notebook_path" {
  value = databricks_notebook.this.path
}

output "notebook_filename" {
  value = var.notebook_name

}