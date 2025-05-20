# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import



import dlt
from dataclasses import dataclass
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------

database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

# COMMAND ----------

@dataclass
class Table:
    accessible: bool = False
Table.accessible = BaseUtils._if_table_exists(spark, database, "itisac_raw")

# COMMAND ----------

if Table.accessible:
    @dlt.view()
    def itisac_raw():
        return spark.readStream.option('ignoreChanges', 'true').table(f'{database}.itisac_raw')

# COMMAND ----------

if Table.accessible:
    @dlt.table()
    def itisac_reports_bronze():
        schema = (StructType()
            .add('id', StringType())
            .add('title', StringType())
            .add('reportBody', StringType())
            .add('tags', ArrayType(StringType()))
            .add('timeBegan', LongType())
            .add('created', LongType())
            .add('updated', LongType())
            .add('distributionType', StringType())
            .add('externalTrackingId', StringType())
            .add('externalUrl', StringType())
            .add('enclaveIds', ArrayType(StringType()))
        )

        return (
            dlt.readStream('itisac_raw')
            .where('_type = "report"')
            .select(
                '*',
                from_json(col('_raw_record'), schema).alias('record')
            )
            .selectExpr(
                '_source', 'record.*', '_collection_ts', '_raw_record'
            )
        )

# COMMAND ----------

if Table.accessible:
    @dlt.table()
    def itisac_indicators_bronze():
        schema = (StructType()
            .add('indicatorType', StringType())
            .add('value', StringType())
            .add('firstSeen', LongType())
            .add('lastSeen', LongType())
            .add('tags', ArrayType(StringType()))
            .add('report_id', StringType())
            .add('report_title', StringType())
            .add('enclaveIds', ArrayType(StringType()))
            .add('notes', ArrayType(StringType()))
            .add('sightings', IntegerType())
            .add('source', StringType())
            .add('whitelisted', BooleanType())
        )

        return (
            dlt.readStream('itisac_raw')
            .where('_type = "indicator"')
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
def itisac_indicators_normalized():
    dlt.readStream('itisac_indicators_bronze').createOrReplaceTempView('itisac')
    
    df = spark.sql('''
        SELECT 
            CASE
                WHEN (indicatorType = 'URL' AND value RLIKE '/' AND value NOT RLIKE 'https?://')
                    THEN 'http://' || value 
                ELSE
                    value
            END AS indicator,
            CASE
                WHEN indicatorType = 'IP'
                    THEN 'ip'
                WHEN 
                    (indicatorType = 'URL' AND value RLIKE '/')
                    THEN 'url'
                WHEN 
                    (indicatorType = 'URL' AND value NOT RLIKE '/') OR
                    indicatorType = 'DOMAIN'
                    THEN 'domain'
                WHEN indicatorType = 'MD5'
                    THEN 'md5'
                WHEN indicatorType = 'SHA256'
                    THEN 'sha256'
                WHEN indicatorType = 'SHA1'
                    THEN 'sha1'
                WHEN indicatorType = 'EMAIL_ADDRESS'
                    THEN 'email'
                ELSE
                    NULL
            END AS type,
            CASE
                WHEN indicatorType = 'IP'
                    THEN 'c2_ip'
                WHEN 
                    (indicatorType = 'URL' AND value RLIKE '/')
                    THEN 'c2_url'
                WHEN 
                    (indicatorType = 'URL' AND value NOT RLIKE '/') OR
                    indicatorType = 'DOMAIN'
                    THEN 'c2_domain'
                WHEN indicatorType = 'MD5'
                    THEN 'mal_md5'
                WHEN indicatorType = 'SHA256'
                    THEN 'mal_sha256'
                WHEN indicatorType = 'SHA1'
                    THEN 'mal_sha1'
                WHEN indicatorType = 'EMAIL_ADDRESS'
                    THEN 'mal_email'
                ELSE
                    NULL
            END AS indicator_type,
            _source AS source,
            'https://station.trustar.co/constellation/reports/' || report_id AS source_locator,
            'TLP:AMBER' AS tlp,
            tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            map(
                'report_id', report_id,
                'report_title', report_title,
                'whitelisted', CAST(whitelisted as string),
                'source', source
            ) AS context,
            to_date(firstSeen) AS date_first,
            to_date(coalesce(lastSeen, firstSeen)) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
            itisac
    ''')

    return df
