# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.modules import get_modules
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
module = get_modules()
logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
logger.info("executing")

# COMMAND ----------

TABLE = 'staging'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

streams = []
intel_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {database} LIKE '*_normalized'").collect()]
for tbl in intel_tables:
    if BaseUtils._if_table_exists(spark, database, tbl):
        streams.append(tbl)

# COMMAND ----------

def create_intelligence_staging():
    df = None

    for stream in streams:
        if df is None:
            df = spark.readStream.table(f'{database}.{stream}')
        else:
            df = df.union(spark.readStream.table(f'{database}.{stream}'))
    return df.where('type IS NOT NULL')

if streams:
    staging_df = create_intelligence_staging()
    query = (staging_df.writeStream
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .toTable(f'{database}.{TABLE}')
    )
    query.awaitTermination()
logger.info("completed")