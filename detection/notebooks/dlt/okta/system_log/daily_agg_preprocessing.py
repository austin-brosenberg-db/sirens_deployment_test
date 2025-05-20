# Databricks notebook source
# DBTITLE 1,Set working directory to Sirens home
import os

sirens_home = spark.conf.get("sirens.home", None)

if sirens_home is not None:
  os.chdir(sirens_home)

# COMMAND ----------

import dlt

from pyspark.sql.types import *
from pyspark.sql.functions import collect_set, count, col, expr, date_trunc, array_distinct, flatten, sum, size, max

# COMMAND ----------

@dlt.table(spark_conf={"pipelines.trigger.interval": "1 hour"})
def okta_system_log_daily_agg():
  return (
    dlt.read('okta_system_log_hourly_agg')
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
