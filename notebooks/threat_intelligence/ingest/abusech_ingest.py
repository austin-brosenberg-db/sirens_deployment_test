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
def abusech_threatfox_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_threatfox_raw')

@dlt.view()
def abusech_urlhaus_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_urlhaus_raw')

@dlt.view()
def abusech_feodotracker_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_feodotracker_raw')


@dlt.view()
def abusech_malwarebazaar_raw():
    return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_malwarebazaar_raw')


# COMMAND ----------

@dlt.table()
def abusech_threatfox_bronze():
    schema = (StructType()
        .add('ioc_type', StringType())
        .add('ioc_value', StringType())
        .add('first_seen_utc', TimestampType())
        .add('last_seen_utc', TimestampType())
        .add('anonymous', StringType())
        .add('confidence_level', StringType())
        .add('fk_malware', StringType())
        .add('ioc_id', StringType())
        .add('malware_alias', StringType())
        .add('malware_printable', StringType())
        .add('reference', StringType())
        .add('reporter', StringType())
        .add('tags', StringType())
        .add('threat_type', StringType())
    )

    return (
        dlt.readStream('abusech_threatfox_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )

# COMMAND ----------

@dlt.table()
def abusech_feodotracker_bronze():
    schema = (StructType()
        .add('dst_ip', StringType())
        .add('dst_port', StringType())
        .add('malware', StringType())
        .add('c2_status', StringType())
        .add('first_seen_utc', TimestampType())
        .add('last_online', TimestampType())
    )

    return (
        dlt.readStream('abusech_feodotracker_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )

# COMMAND ----------

@dlt.table()
def abusech_urlhaus_bronze():
    schema = (StructType()
        .add('id', StringType())
        .add('dateadded', TimestampType())
        .add('last_online', TimestampType())
        .add('reporter', StringType())
        .add('tags', StringType())
        .add('threat', StringType())
        .add('url', StringType())
        .add('url_status', StringType())
        .add('urlhaus_link', StringType())
    )

    return (
        dlt.readStream('abusech_urlhaus_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )

# COMMAND ----------

@dlt.table()
def abusech_malwarebazaar_bronze():
    schema = (StructType()
        .add('first_seen_utc', TimestampType())
        .add('sha1_hash', StringType())
        .add('sha256_hash', StringType())
        .add('md5_hash', StringType())
        .add('imphash', StringType())
        .add('ssdeep', StringType())
        .add('tlsh', StringType())
        .add('mime_type', StringType())
        .add('clamav', StringType())
        .add('reporter', StringType())
        .add('signature', StringType())
        .add('vtpercent', StringType())
        .add('file_name', StringType())
        .add('file_type_guess', StringType())
    )

    return (
        dlt.readStream('abusech_malwarebazaar_raw')
        .select(
            '*',
            from_json(col('_raw_record'), schema).alias('record')
        )
        .selectExpr(
            '_source', 'record.*', '_collection_ts', '_raw_record'
        )
    )