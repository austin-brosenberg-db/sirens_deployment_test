# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import

# COMMAND ----------

#import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.modules import get_modules
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
module = get_modules()
logger.info("executing")

# COMMAND ----------

TABLE = 'misc_normalized'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType

schema = (StructType()
          .add('indicator', StringType())
          .add('type', StringType())
          .add('indicator_type', StringType())
          .add('tags', ArrayType(StringType()))
          )

raw_df = spark.readStream.table(f'{database}.misc_raw').select(
    '*',
    from_json(col('_raw_record'), schema).alias('record')
).selectExpr(
    '_source', 'record.*', '_collection_ts', '_raw_record'
)

# COMMAND ----------

normalized_df = raw_df.select(
    "indicator",
    "type",
    "indicator_type",
    col("_source").alias("source"),
    lit("").alias("source_locator"),
    lit("TLP:CLEAR").alias("tlp"),
    "tags",
    from_json(lit("{}"), MapType(StringType(), StringType())).alias("flags"),
    from_json(lit("{}"), MapType(StringType(), StringType())).alias("context"),
    to_date("_collection_ts").alias("date_first"),
    to_date("_collection_ts").alias("date_last"),
    "_collection_ts",
    "_raw_record"
)

# COMMAND ----------

query = (normalized_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)
logger.info("completed")
