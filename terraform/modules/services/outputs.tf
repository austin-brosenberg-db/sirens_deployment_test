output "current_user_home" {
  value = data.databricks_current_user.me.home
}

output "current_user_name" {
  value = data.databricks_current_user.me.user_name
}