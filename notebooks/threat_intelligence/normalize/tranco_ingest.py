# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import

# COMMAND ----------

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

TABLE = 'tranco_bronze'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

tranco_raw_df = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.tranco_raw')

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

schema = StructType() \
    .add('domain', StringType()) \
    .add('rank', StringType())

tranco_bronze_df = tranco_raw_df \
    .select(
        '*',
        from_json(col('_raw_record'), schema).alias('record')
    ) \
    .select(
        col('_source'),
        col('record.domain'),
        col('record.rank').cast(IntegerType()).alias('rank'),
        col('_collection_ts'),
        col('_raw_record')
    )

query = (tranco_bronze_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)
query.awaitTermination()
logger.info("completed")
