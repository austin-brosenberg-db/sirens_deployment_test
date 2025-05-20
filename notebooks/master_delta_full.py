# Databricks notebook source
# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %pip install pyyaml inflection

# COMMAND ----------

# DBTITLE 1,Setup Widgets
dbutils.widgets.text("source", '', label="source")
dbutils.widgets.text("sourcetype", '', label="sourcetype")
dbutils.widgets.text("database", '', label="database")

# COMMAND ----------

# DBTITLE 1,Read Widgets
source = dbutils.widgets.get("source")
sourcetype = dbutils.widgets.get("sourcetype")
database = dbutils.widgets.get("database")
if source == '' or sourcetype == '' or database == '':
    raise Exception('please pass parameters to multi-task job, or fill out notebook widget values')
else:
    input_source = {'source': source, 'sourcetype': sourcetype, 'database': database}

# COMMAND ----------

# DBTITLE 1,Imports
import os
import databricks

databricks.__path__.append(os.path.abspath("../databricks"))

from databricks.sirens.datasource import DataSource
from databricks.sirens.logging import get_logger
from databricks.sirens import connectors
from databricks.sirens import parsers
from databricks.sirens import normalize
logger = get_logger(__name__)

# COMMAND ----------

# DBTITLE 1,Read data source config file (inputs.yaml)
data_source_obj = DataSource(spark, input_source['database'], input_source['source'], input_source['sourcetype'])
config = data_source_obj.read()
logger.info(f"working on: {config.get('input').get('sourcetype')} using connector: {data_source_obj.get_connector_name()} w/schema {config.get('input').get('rawSchemaFile')} in {data_source_obj.get_stream_mode()} mode")
logger.info(f"filepath: {config.get('input').get('rawPath')}")

# COMMAND ----------

# DBTITLE 1,create connector, parser and delta reader/writer objects
connector_mod = connectors.get(data_source_obj.get_connector_name())
log_parser = parsers.Parse(config.get("input").get("parser"), spark)
delta_reader = connectors.Reader('readDelta', spark, data_source_obj)
delta_writer = connectors.Writer('writeDelta', spark, data_source_obj)

# COMMAND ----------

# DBTITLE 1,Read raw data, parse to bronze and write bronze delta table
# read, parse and write
df = connectors.Reader(connector_mod, spark, data_source_obj).read()
df = log_parser.toBronze(df, data_source_obj)
delta_writer.write(df, data_source_obj.bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Read bronze delta table, transform to silver, and write delta table
df = delta_reader.read(data_source_obj.bronze_table_name)
df = log_parser.toSilver(df, data_source_obj)
delta_writer.write(df, data_source_obj.silver_table_name)

# COMMAND ----------

# DBTITLE 1,Read bronze delta table, and step through event_type transforms into silver cim tables
normalizer = normalize.Normalizer(spark, data_source_obj)

# Read bronze delta table
silver_df = delta_reader.read(data_source_obj.silver_table_name)

for et in config.get("transforms").get("silver").get("event_type"):
    target_table = et.get("target_table")
    tblName = data_source_obj.get_table_name(source, sourcetype, prefix=target_table, suffix='silver')

    # filter events as defined in event_type filter
    df = normalizer.filter_frame(df=silver_df, target_table=target_table)

    # transform events as defined in event_type transforms
    df = normalizer.transform_frame(df=df, target_table=target_table)

    delta_writer.write(df, target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset Checkpoint
# MAGIC
# MAGIC To reprocess the file, we need to remove the checkpoint direcotry. This is useful for testing / dev work - parallel in splunk is adding salt to reprocess same file

# COMMAND ----------

checkpoint_reset = False
if checkpoint_reset:
    dbutils.fs.rm('/tmp/delta/_checkpoints/', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema inference and partition of streaming DataFrames/Datasets
# MAGIC
# MAGIC By default, Structured Streaming from file based sources requires you to specify the schema, rather than rely on Spark to infer it automatically. This restriction ensures a consistent schema will be used for the streaming query, even in the case of failures. For ad-hoc use cases, you can reenable schema inference by setting spark.sql.streaming.schemaInference to true.

# COMMAND ----------

schema_infer = False
if schema_infer:
    spark.conf.set("spark.sql.streaming.schemaInference", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()
