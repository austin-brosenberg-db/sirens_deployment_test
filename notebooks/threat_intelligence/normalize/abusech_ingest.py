# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

# COMMAND ----------

import databricks_sirens.fix_package_import
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.modules import get_modules
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
module = get_modules()

# COMMAND ----------

logger.info("executing")
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

# COMMAND ----------

abusech_threatfox_raw = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_threatfox_raw')

abusech_feodotracker_raw = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_feodotracker_raw')

abusech_urlhaus_raw = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_urlhaus_raw')

abusech_malwarebazaar_raw = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.abusech_malwarebazaar_raw')

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

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

abusech_threatfox_bronze = (
    abusech_threatfox_raw
    .select(
        '*',
        from_json(col('_raw_record'), schema).alias('record')
    )
    .selectExpr(
        '_source', 'record.*', '_collection_ts', '_raw_record'
    )
)

# COMMAND ----------

from pyspark.sql.functions import expr, split, to_date, coalesce, lit, from_json, map_from_arrays

abusech_threatfox_normalized_df = abusech_threatfox_bronze.select(
    expr("CASE WHEN ioc_type = 'ip:port' THEN SPLIT(ioc_value, ':')[0] ELSE ioc_value END").alias("indicator"),
    expr("""
        CASE
            WHEN ioc_type = 'ip:port' THEN 'ip'
            WHEN ioc_type = 'domain' THEN 'domain'
            WHEN ioc_type = 'md5_hash' THEN 'md5'
            WHEN ioc_type = 'sha256_hash' THEN 'sha256'
            WHEN ioc_type = 'url' THEN 'url'
            ELSE NULL
        END
    """).alias("type"),
    expr("""
        CASE
            WHEN ioc_type = 'ip:port' AND threat_type = 'botnet_cc' THEN 'c2_ip'
            WHEN ioc_type = 'domain' AND threat_type = 'botnet_cc' THEN 'c2_domain'
            WHEN ioc_type = 'ip:port' AND threat_type = 'payload_delivery' THEN 'mal_ip'
            WHEN ioc_type = 'domain' AND threat_type IN ('payload_delivery', 'cc_skimming') THEN 'mal_domain'
            WHEN ioc_type = 'md5_hash' THEN 'mal_md5'
            WHEN ioc_type = 'sha256_hash' THEN 'mal_sha256'
            WHEN ioc_type = 'url' AND threat_type = 'payload_delivery' THEN 'mal_url'
            WHEN ioc_type = 'url' AND threat_type = 'botnet_cc' THEN 'c2_url'
            ELSE NULL
        END
    """).alias("indicator_type"),
    col("_source").alias("source"),
    col("ioc_id").alias("source_locator"),
    lit("TLP:CLEAR").alias("tlp"),
    split(expr("concat(tags, ',', fk_malware, ',', malware_alias, ',', malware_printable)"), ",").alias("tags"),
    from_json(lit("{}"), "map<string,string>").alias("flags"),
    expr("""
        CASE
            WHEN ioc_type = 'ip:port' THEN map_from_arrays(array('port', 'reference', 'reporter', 'threat_type'), array(SPLIT(ioc_value, ':')[1], reference, reporter, threat_type))
            ELSE map_from_arrays(array('reference', 'reporter', 'threat_type'), array(reference, reporter, threat_type))
        END
    """).alias("context"),
    to_date("first_seen_utc").alias("date_first"),
    to_date(coalesce("last_seen_utc", "first_seen_utc")).alias("date_last"),
    col("_collection_ts"),
    col("_raw_record")
)

checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", 'abuse_threatfox_normalized')

query = (abusech_threatfox_normalized_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.abuse_threatfox_normalized')
)

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col

schema = StructType() \
    .add('dst_ip', StringType()) \
    .add('dst_port', StringType()) \
    .add('malware', StringType()) \
    .add('c2_status', StringType()) \
    .add('first_seen_utc', TimestampType()) \
    .add('last_online', TimestampType())

abusech_feodotracker_bronze_df = abusech_feodotracker_raw \
    .select(
        '*',
        from_json(col('_raw_record'), schema).alias('record')
    ) \
    .selectExpr(
        '_source', 'record.*', '_collection_ts', '_raw_record'
    )

# COMMAND ----------

from pyspark.sql.functions import array, col, coalesce, from_json, lit, map_from_arrays, to_date

abusech_feodotracker_normalized_df = abusech_feodotracker_bronze_df.select(
    col("dst_ip").alias("indicator"),
    lit("ip").alias("type"),
    lit("c2_ip").alias("indicator_type"),
    col("_source").alias("source"),
    lit("").alias("source_locator"),
    lit("TLP:CLEAR").alias("tlp"),
    array(col("malware")).alias("tags"),
    from_json(lit("{}"), "map<string,string>").alias("flags"),
    map_from_arrays(array(lit("status"), lit("port")), array(col("c2_status"), col("dst_port"))).alias("context"),
    to_date(col("first_seen_utc")).alias("date_first"),
    to_date(coalesce(col("last_online"), col("first_seen_utc"))).alias("date_last"),
    col("_collection_ts"),
    col("_raw_record")
)

checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", 'abusech_feodotracker_normalized')

query = (abusech_feodotracker_normalized_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.abusech_feodotracker_normalized')
)

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, TimestampType

schema = StructType() \
    .add('id', StringType()) \
    .add('dateadded', TimestampType()) \
    .add('last_online', TimestampType()) \
    .add('reporter', StringType()) \
    .add('tags', StringType()) \
    .add('threat', StringType()) \
    .add('url', StringType()) \
    .add('url_status', StringType()) \
    .add('urlhaus_link', StringType())

abusech_urlhaus_bronze_df = abusech_urlhaus_raw \
    .select("*", 
            from_json(col('_raw_record'), schema).alias('record')) \
    .select("_source", "record.*", "_collection_ts", "_raw_record")

# COMMAND ----------

from pyspark.sql.functions import col, split, to_date, coalesce, lit, expr

abusech_urlhaus_normalized = abusech_urlhaus_bronze_df \
    .select(
        col('url').alias('indicator'),
        lit('url').alias('type'),
        lit('mal_url').alias('indicator_type'),
        col('_source').alias('source'),
        col('urlhaus_link').alias('source_locator'),
        lit('TLP:CLEAR').alias('tlp'),
        split(col('tags'), ',').alias('tags'),
        expr("FROM_JSON('{}', 'map<string,string>')").alias('flags'),
        expr("map('threat', threat, 'url_status', url_status)").alias('context'),
        to_date(col('dateadded')).alias('date_first'),
        to_date(coalesce(col('last_online'), col('dateadded'))).alias('date_last'),
        col('_collection_ts'),
        col('_raw_record')
    )
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", 'abusech_urlhaus_normalized')

query = (abusech_urlhaus_normalized.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.abusech_urlhaus_normalized')
)


# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

schema = StructType() \
    .add('first_seen_utc', TimestampType()) \
    .add('sha1_hash', StringType()) \
    .add('sha256_hash', StringType()) \
    .add('md5_hash', StringType()) \
    .add('imphash', StringType()) \
    .add('ssdeep', StringType()) \
    .add('tlsh', StringType()) \
    .add('mime_type', StringType()) \
    .add('clamav', StringType()) \
    .add('reporter', StringType()) \
    .add('signature', StringType()) \
    .add('vtpercent', StringType()) \
    .add('file_name', StringType()) \
    .add('file_type_guess', StringType())

abusech_malwarebazaar_bronze_df = abusech_malwarebazaar_raw \
    .select('*', from_json(col('_raw_record'), schema).alias('record')) \
    .select('_source', 'record.*', '_collection_ts', '_raw_record')

# COMMAND ----------

from pyspark.sql.functions import col, to_date, array, lit, from_json, map_from_arrays
from functools import reduce

malwarebazaar_df = abusech_malwarebazaar_bronze_df

def create_dataframe_for_hash_type(hash_type):
    return malwarebazaar_df.select(
        col(f"{hash_type}_hash").alias("indicator"),
        lit(hash_type).alias("type"),
        lit(f"mal_{hash_type}").alias("indicator_type"),
        col("_source").alias("source"),
        col("sha256_hash").alias("source_locator"),
        lit("TLP:CLEAR").alias("tlp"),
        array(col("mime_type"), col("file_type_guess"), col("signature")).alias("tags"),
        from_json(lit("{}"), "map<string,string>").alias("flags"),
        map_from_arrays(
            array(lit("signature"), lit("mime_type"), lit("reporter"), lit("file_name"), lit("clamav"), lit("file_type_guess"), lit("vtpercent")),
            array(col("signature"), col("mime_type"), col("reporter"), col("file_name"), col("clamav"), col("file_type_guess"), col("vtpercent"))
        ).alias("context"),
        to_date(col("first_seen_utc")).alias("date_first"),
        to_date(col("first_seen_utc")).alias("date_last"),
        col("_collection_ts"),
        col("_raw_record")
    )

hash_types = ['sha256', 'sha1', 'md5']
dfs = [create_dataframe_for_hash_type(hash_type) for hash_type in hash_types]

abusech_malwarebazaar_normalized_df = reduce(lambda df1, df2: df1.unionAll(df2), dfs)

checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", 'aabusech_malwarebazaar_normalized')

query = (abusech_urlhaus_normalized.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.abusech_malwarebazaar_normalized')
)
query.awaitTermination()
logger.info("completed")
