# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import

from dataclasses import dataclass
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.modules import get_modules
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
module = get_modules()

# COMMAND ----------

TABLE = 'itisac_indicators_normalized'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

logger.info("executing")
raw_df = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.itisac_raw')

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, LongType, StringType, StructType

schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("reportBody", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("timeBegan", LongType(), True),
    StructField("created", LongType(), True),
    StructField("updated", LongType(), True),
    StructField("distributionType", StringType(), True),
    StructField("externalTrackingId", StringType(), True),
    StructField("externalUrl", StringType(), True),
    StructField("enclaveIds", ArrayType(StringType()), True)
])

itisac_reports_bronze_df = raw_df \
    .where("_type = 'report'") \
    .select('*', from_json(col('_raw_record'), schema).alias('record')) \
    .selectExpr('_source', 'record.*', '_collection_ts', '_raw_record')


# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType, ArrayType, IntegerType, BooleanType

schema = StructType() \
    .add('indicatorType', StringType()) \
    .add('value', StringType()) \
    .add('firstSeen', LongType()) \
    .add('lastSeen', LongType()) \
    .add('tags', ArrayType(StringType())) \
    .add('report_id', StringType()) \
    .add('report_title', StringType()) \
    .add('enclaveIds', ArrayType(StringType())) \
    .add('notes', ArrayType(StringType())) \
    .add('sightings', IntegerType()) \
    .add('source', StringType()) \
    .add('whitelisted', BooleanType())

itisac_indicators_bronze_df = raw_df \
    .where('_type = "indicator"') \
    .select('*', from_json(col('_raw_record'), schema).alias('record')) \
    .selectExpr('_source', 'record.*', '_collection_ts', '_raw_record')


checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", 'itisac_indicators_normalized')
query = (itisac_indicators_bronze_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.itisac_indicators_normalized')
)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, when, expr, to_date, coalesce, from_json, map_from_arrays

itisac_indicators_normalized_df = itisac_indicators_bronze_df \
    .withColumn('indicator', 
                when((col('indicatorType') == 'URL') & (col('value').rlike('/')) & (~col('value').rlike('https?://')), 
                     concat(lit('http://'), col('value')))
                .otherwise(col('value'))) \
    .withColumn('type', 
                when(col('indicatorType') == 'IP', lit('ip'))
                .when((col('indicatorType') == 'URL') & col('value').rlike('/'), lit('url'))
                .when(((col('indicatorType') == 'URL') & (~col('value').rlike('/'))) | (col('indicatorType') == 'DOMAIN'), lit('domain'))
                .when(col('indicatorType') == 'MD5', lit('md5'))
                .when(col('indicatorType') == 'SHA256', lit('sha256'))
                .when(col('indicatorType') == 'SHA1', lit('sha1'))
                .when(col('indicatorType') == 'EMAIL_ADDRESS', lit('email'))
                .otherwise(lit(None))) \
    .withColumn('indicator_type', 
                when(col('indicatorType') == 'IP', lit('c2_ip'))
                .when((col('indicatorType') == 'URL') & col('value').rlike('/'), lit('c2_url'))
                .when(((col('indicatorType') == 'URL') & (~col('value').rlike('/'))) | (col('indicatorType') == 'DOMAIN'), lit('c2_domain'))
                .when(col('indicatorType') == 'MD5', lit('mal_md5'))
                .when(col('indicatorType') == 'SHA256', lit('mal_sha256'))
                .when(col('indicatorType') == 'SHA1', lit('mal_sha1'))
                .when(col('indicatorType') == 'EMAIL_ADDRESS', lit('mal_email'))
                .otherwise(lit(None))) \
    .withColumn('source', col('_source')) \
    .withColumn('source_locator', concat(lit('https://station.trustar.co/constellation/reports/'), col('report_id'))) \
    .withColumn('tlp', lit('TLP:AMBER')) \
    .withColumn('flags', from_json(lit('{}'), 'map<string,string>')) \
    .withColumn('context', map_from_arrays(
        array(lit('report_id'), lit('report_title'), lit('whitelisted'), lit('source')),
        array(col('report_id'), col('report_title'), col('whitelisted').cast('string'), col('source'))
    )) \
    .withColumn('date_first', to_date(col('firstSeen'))) \
    .withColumn('date_last', to_date(coalesce(col('lastSeen'), col('firstSeen')))) \
    .select('indicator', 'type', 'indicator_type', 'source', 'source_locator', 'tlp', 'tags', 'flags', 'context', 'date_first', 'date_last', '_collection_ts', '_raw_record')

# COMMAND ----------

query = (normalized_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)
query.awaitTermination()
logger.info("completed")