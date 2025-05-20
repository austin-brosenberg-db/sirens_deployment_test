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

TABLE = 'misp_warninglists_bronze'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

raw_df = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.misp_warninglists_raw')

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType

schema = (StructType()
          .add('value', StringType())
          .add('type', StringType())
          .add('description', StringType())
          .add('list_name', StringType())
          .add('matching_attributes', ArrayType(StringType()))
          .add('version', StringType())
          )

normalized_df = raw_df.select(
    '*',
    from_json(col('_raw_record'), schema).alias('record')
).selectExpr(
    '_source', 'record.*', '_collection_ts', '_raw_record'
)

# COMMAND ----------

query = (normalized_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)
logger.info("completed")