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

from databricks.sirens import config_reader
import yaml

# COMMAND ----------

source = spark.conf.get('source')
sourcetype = spark.conf.get('sourcetype')
silver_table_database = spark.conf.get('silver_table_database')
silver_table_name = config_reader.DataSource(spark, silver_table_database, source, sourcetype).silver_table_name

# COMMAND ----------

@dlt.table(spark_conf={"pipelines.trigger.interval": "1 hour"})
def okta_system_log_hourly_agg():
  return (
    spark.table(f"{silver_table_database}.{silver_table_name}")
    .filter("_event_date >= current_date() - 1")
    .filter("_event_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS")
    .withColumn("hour_timestamp", date_trunc('HOUR', '_event_time'))
    .groupBy('eventType', 'actor.alternateId', 'oktaURL', '_event_date', 'hour_timestamp')
    .agg(
      collect_set('targetApp').alias('targetApps'),
      collect_set('targetUser').alias('targetUsers'),
      collect_set('targetUserGroup').alias('targetUserGroups'),
      collect_set('client.ipAddress').alias('ipAddresses'),
      collect_set('client.userAgent.rawUserAgent').alias('userAgents'),
      count('*').alias('count')
    )
  )
