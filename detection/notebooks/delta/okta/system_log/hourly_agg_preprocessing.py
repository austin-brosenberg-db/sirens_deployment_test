# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

dbutils.widgets.text("source", '', label="source")
dbutils.widgets.text("sourcetype", '', label="sourcetype")
dbutils.widgets.text("silver_table_database", '', label="silver_table_database")

# COMMAND ----------

source=dbutils.widgets.get("source")
sourcetype=dbutils.widgets.get("sourcetype")
silver_table_database=dbutils.widgets.get("silver_table_database")

if source == '' or sourcetype == '' or silver_table_database == '':
    raise Exception('please pass parameters to multi-task job, or fill out notebook widget values')

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import collect_set, count, col, expr, date_trunc, array_distinct, flatten, sum, size, max

from databricks.sirens import config_reader
import yaml

# COMMAND ----------

silver_table_name = config_reader.DataSource(spark, silver_table_database, source, sourcetype).silver_table_name

# COMMAND ----------


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

# COMMAND ----------

okta_system_log_hourly_agg().write.mode('overwrite').saveAsTable('sirens.okta_system_log_hourly_agg')

# COMMAND ----------


