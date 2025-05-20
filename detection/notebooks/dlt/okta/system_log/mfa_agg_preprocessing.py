# Databricks notebook source
# DBTITLE 1,Set working directory to Sirens home
import os

sirens_home = spark.conf.get("sirens.home", None)

if sirens_home is not None:
  os.chdir(sirens_home)

# COMMAND ----------

import dlt

from pyspark.context import SparkContext
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import *
from pyspark.sql.functions import row_number, expr, when, col, date_trunc, count, collect_set, udf
from pyspark.sql.window import Window

from databricks.sirens import config_reader
import yaml

# COMMAND ----------

source = spark.conf.get('source')
sourcetype = spark.conf.get('sourcetype')
silver_table_database = spark.conf.get('silver_table_database')
silver_table_name = config_reader.DataSource(spark, silver_table_database, source, sourcetype).silver_table_name

# COMMAND ----------

@dlt.table(spark_conf={"pipelines.trigger.interval": "1 hour"})
def okta_system_log_mfa_agg():
  def lead(col, offset=1, default=None, ignoreNulls=False):
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.functions.lead(_to_java_column(col), offset, default, ignoreNulls))
  
  okta = (
    spark.read.table(f"{silver_table_database}.{silver_table_name}")
    .filter('_event_date >= current_date-90')
    .withColumn('ipAddress_occurrence_count', row_number().over(Window.partitionBy('actor.alternateId', 'client.ipAddress').orderBy('_event_time')))
    .filter("_event_date >= current_date() - 1")
    .filter("_event_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS")
    .withColumn(
      "mfa_action",
      when(expr('eventType = "system.push.send_factor_verify_push"'), "push")
      .when(expr('legacyEventType = "core.user.factor.attempt_success" AND debugContext.debugData.factor = "OKTA_VERIFY_PUSH"'), "approved push")
      .when(expr('eventType = "user.mfa.okta_verify.deny_push"'), "denied push")
    )
  )
  
  return (
    okta
    .withColumn("hour_timestamp", date_trunc('HOUR', '_event_time'))
    .withColumn(
      'reply_timestamp',
      when(expr('mfa_action IN ("approved push", "denied push")'), col('_event_time'))
    )
    .withColumn(
      'next_reply_timestamp',
      when(expr('mfa_action = "push"'), lead('reply_timestamp', ignoreNulls=True).over(Window.partitionBy('actor.alternateId').orderBy('_event_time')))
      .otherwise(col('_event_time'))
    )
    .withColumn(
      'hanging_push', 
      when(
        expr(
          'mfa_action = "push" AND (next_reply_timestamp IS NULL OR next_reply_timestamp >= _event_time + INTERVAL 20 MINUTES)'
        ), True)
      .otherwise(False)
    )
    .groupBy('actor.alternateId', 'oktaURL', '_event_date', 'hour_timestamp')
    .agg(
      count(when(expr('hanging_push IS TRUE'), "hanging_push")).alias('hanging_pushes'),
      count(when(expr('mfa_action = "denied push"'), "denied push")).alias('denied_pushes'),
      count(when(expr('mfa_action = "approved push"'), "approved push")).alias('approved_pushes'),
      count(when(expr('mfa_action = "push"'), "push")).alias('pushes'),
      count(when(expr('ipAddress_occurrence_count = 1 AND mfa_action = "push"'), "first_ip")).alias('first_ip_push_occurrences'),
      collect_set('client.ipAddress').alias('ipAddresses'),
      collect_set('client.userAgent.rawUserAgent').alias('userAgents'),
    )
  )
