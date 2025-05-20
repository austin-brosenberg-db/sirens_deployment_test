# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)



# COMMAND ----------

import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

# COMMAND ----------

@dlt.view()
def phishtank_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.phishtank_raw')

# COMMAND ----------

@dlt.table()
def phishtank_bronze():
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

    return (
        dlt.readStream('phishtank_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )