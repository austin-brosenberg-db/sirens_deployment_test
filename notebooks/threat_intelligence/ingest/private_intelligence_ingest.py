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
def private_intelligence_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.private_intelligence_raw')

# COMMAND ----------

@dlt.table()
def private_intelligence_bronze():
    schema = (StructType()
        .add('indicator', StringType())
        .add('type', StringType())
        .add('itype', StringType())
        .add('comment', StringType())
        .add('tags', ArrayType(StringType()))
    )

    return (
        dlt.readStream('private_intelligence_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 
            'record.indicator',
            'record.type',
            'record.itype AS indicator_type',
            'record.tags', 
            'record.comment', 
            '_collection_ts', 
            '_raw_record'
        )
    )