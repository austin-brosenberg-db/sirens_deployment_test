variable "notebook_subdirectory" {
  description = "A name for the subdirectory to store the notebook."
  type        = string
  default     = "Terraform"
}

variable "notebook_filename" {
  description = "The notebook's filename as absolute path"
  type        = string
}

variable "notebook_language" {
  description = "The language of the notebook."
  type        = string
}

variable "user_home" {
  description = "The home directory of the current user"
  type        = string
}

variable "notebook_name" {
  description = "Regex'd notebook name only from absolute path"

}