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
def misc_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.misc_raw')

# COMMAND ----------

@dlt.table()
def misc_bronze():
    schema = (StructType()
        .add('indicator', StringType())
        .add('type', StringType())
        .add('indicator_type', StringType())
        .add('tags', ArrayType(StringType()))
    )

    return (
        dlt.readStream('misc_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )

# COMMAND ----------

@dlt.view()
def misc_normalized():

    dlt.readStream('misc_bronze').createOrReplaceTempView('misc')

    return spark.sql('''
        SELECT 
            indicator,
            type,
            indicator_type,
            _source AS source,
            '' AS source_locator,
            'TLP:CLEAR' AS tlp,
            tags as tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            FROM_JSON('{}', 'map<string,string>') AS context,
            to_date(_collection_ts) AS date_first,
            to_date(_collection_ts) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
            misc
    ''')
