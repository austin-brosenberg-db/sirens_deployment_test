variable "git_url" {
  description = "URL of the REPO to connect to"
  type        = string
}

variable "git_username" {
  description = "email or username to connect to the git repository"
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
  type        = string
  sensitive   = true
}

variable "git_repo_name" {
  description = "Name of the repo in the workspace"
  type        = string
}

variable "git_branch" {
  description = "Current git branch to checkout"
  default     = ""
  type        = string
}