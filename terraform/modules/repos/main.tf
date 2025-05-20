terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_current_user" "me" {}

resource "databricks_git_credential" "sirens_repo" {
  count                 = var.git_pat_token != "" ? 1 : 0
  git_username          = var.git_username
  git_provider          = var.git_provider
  personal_access_token = sensitive(var.git_pat_token)
  force                 = true
}

resource "databricks_repo" "sirens_repo" {
  depends_on = [databricks_git_credential.sirens_repo]
  url        = var.git_url
  path       = "${data.databricks_current_user.me.repos}/${var.git_repo_name}"
  branch     = var.git_branch
}
