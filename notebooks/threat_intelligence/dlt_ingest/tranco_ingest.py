# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import



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
def tranco_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.tranco_raw')

# COMMAND ----------

@dlt.table()
def tranco_bronze():
    schema = (StructType()
        .add('domain', StringType())
        .add('rank', StringType())
    )

    return (
        dlt.readStream('tranco_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.domain', 'CAST(record.rank AS int) as rank', '_collection_ts', '_raw_record'
        )
    )