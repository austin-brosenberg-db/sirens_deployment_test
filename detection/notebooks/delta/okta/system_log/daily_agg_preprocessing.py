# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import collect_set, count, col, expr, date_trunc, array_distinct, flatten, sum, size, max

# COMMAND ----------

def okta_system_log_daily_agg():
  return (
    spark.table('sirens.okta_system_log_hourly_agg')
    .filter("hour_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS")
    .groupBy('eventType', 'alternateId', 'oktaURL')
    .agg(
      max('_event_date').alias('_event_date'),
      array_distinct(flatten(collect_set('targetApps'))).alias('targetApps'),
      array_distinct(flatten(collect_set('targetUsers'))).alias('targetUsers'),
      array_distinct(flatten(collect_set('targetUserGroups'))).alias('targetUserGroups'),
      array_distinct(flatten(collect_set('ipAddresses'))).alias('ipAddresses'),
      array_distinct(flatten(collect_set('userAgents'))).alias('userAgents'),
      max('hour_timestamp').alias('max_hour_timestamp'),
      sum('count').alias('count')
    )
  )

# COMMAND ----------

okta_system_log_daily_agg().write.mode('overwrite').saveAsTable('sirens.okta_system_log_daily_agg')

# COMMAND ----------


