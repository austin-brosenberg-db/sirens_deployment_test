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
def misp_warninglists_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.misp_warninglists_raw')

# COMMAND ----------

@dlt.table()
def misp_warninglists_bronze():
    schema = (StructType()
        .add('value', StringType())
        .add('type', StringType())
        .add('description', StringType())
        .add('list_name', StringType())
        .add('matching_attributes', ArrayType(StringType()))
        .add('version', StringType())
    )

    return (
        dlt.readStream('misp_warninglists_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )