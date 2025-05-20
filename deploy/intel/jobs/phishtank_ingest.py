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

TABLE = 'phishtank_normalized'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

phishtank_raw_df = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.phishtank_raw')

# COMMAND ----------

schema = (StructType()
    .add('url', StringType())
    .add('phish_id', StringType())
    .add('phish_detail_url', StringType())
    .add('online', StringType())
    .add('submission_time', TimestampType())
    .add('verification_time', TimestampType())
    .add('target', StringType())
    .add('verified', StringType())
    .add('details', ArrayType(MapType(StringType(), StringType())))
)

phishtank_bronze_df = (
    phishtank_raw_df
    .select(
        '*',
        from_json(col('_raw_record'), schema).alias('record')
    )
    .selectExpr(
        '_source', 'record.*', '_collection_ts', '_raw_record'
    )
)

# COMMAND ----------

from pyspark.sql.functions import col, lit, array, from_json, to_date, coalesce, expr
    
phishtank_normalized_df = phishtank_bronze_df.select(
    col('url').alias('indicator'),
    lit('url').alias('type'),
    lit('phishing_url').alias('indicator_type'),
    col('_source').alias('source'),
    col('phish_detail_url').alias('source_locator'),
    lit('TLP:CLEAR').alias('tlp'),
    array(col('target')).alias('tags'),
    from_json(lit('{}'), 'map<string,string>').alias('flags'),
    expr("map('online', online, 'verification_time', CAST(verification_time AS string), 'target', target, 'verified', verified)").alias('context'),
    to_date(col('submission_time')).alias('date_first'),
    to_date(coalesce(col('verification_time'), col('submission_time'))).alias('date_last'),
    col('_collection_ts'),
    col('_raw_record')
)

# COMMAND ----------

query = (phishtank_normalized_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)
query.awaitTermination()
logger.info("completed")