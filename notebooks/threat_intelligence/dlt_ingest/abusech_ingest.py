# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)



# COMMAND ----------

import databricks_sirens.fix_package_import
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

@dlt.view()
def abusech_threatfox_normalized():
    dlt.readStream('abusech_threatfox_bronze').createOrReplaceTempView('threatfox')
    
    df = spark.sql('''
        SELECT 
            CASE 
                WHEN ioc_type = 'ip:port' 
                    THEN SPLIT(ioc_value, ":")[0]
                ELSE 
                    ioc_value
            END AS indicator,
            CASE
                WHEN ioc_type = 'ip:port'
                    THEN 'ip'
                WHEN ioc_type = 'domain'
                    THEN 'domain'
                WHEN ioc_type = 'md5_hash'
                    THEN 'md5'
                WHEN ioc_type = 'sha256_hash'
                    THEN 'sha256'
                WHEN ioc_type = 'url'
                    THEN 'url'
                ELSE
                    NULL
            END AS type,
            CASE
                WHEN ioc_type = 'ip:port' AND threat_type = 'botnet_cc'
                    THEN 'c2_ip'
                WHEN ioc_type = 'domain' AND threat_type = 'botnet_cc'
                    THEN 'c2_domain'
                WHEN ioc_type = 'ip:port' AND threat_type = 'payload_delivery'
                    THEN 'mal_ip'
                WHEN ioc_type = 'domain' AND threat_type IN ('payload_delivery', 'cc_skimming')
                    THEN 'mal_domain'
                WHEN ioc_type = 'md5_hash'
                    THEN 'mal_md5'
                WHEN ioc_type = 'sha256_hash'
                    THEN 'mal_sha256'
                WHEN ioc_type = 'url' AND threat_type = 'payload_delivery'
                    THEN 'mal_url'
                WHEN ioc_type = 'url' AND threat_type = 'botnet_cc'
                    THEN 'c2_url'
                ELSE
                    NULL
            END AS indicator_type,
            _source AS source,
            ioc_id AS source_locator,
            'TLP:CLEAR' AS tlp,
            split(
                tags || ',' || fk_malware || ','|| malware_alias || ',' || malware_printable, 
                ','
            ) AS tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            CASE 
                WHEN ioc_type = 'ip:port' 
                    THEN map(
                        'port', SPLIT(ioc_value, ":")[0],
                        'reference', reference,
                        'reporter', reporter,
                        'threat_type', threat_type
                    )
                ELSE 
                    map(
                        'reference', reference, 
                        'reporter', reporter, 
                        'threat_type', threat_type
                    )
            END AS context,
            to_date(first_seen_utc) AS date_first,
            to_date(coalesce(last_seen_utc, first_seen_utc)) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
            threatfox
    ''')
    return df

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

@dlt.view()
def abusech_feodotracker_normalized():
    dlt.readStream('abusech_feodotracker_bronze').createOrReplaceTempView('feodotracker')
    
    df = spark.sql('''
        SELECT 
            dst_ip AS indicator,
            'ip' AS type,
            'c2_ip' AS indicator_type,
            _source AS source,
            '' AS source_locator,
            'TLP:CLEAR' AS tlp,
            array(malware) AS tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            map('status', c2_status, 'port', dst_port) AS context,
            to_date(first_seen_utc) AS date_first,
            to_date(coalesce(last_online, first_seen_utc)) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
           feodotracker
    ''')
    return df

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

@dlt.view()
def abusech_urlhaus_normalized():
    dlt.readStream('abusech_urlhaus_bronze').createOrReplaceTempView('urlhaus')
    
    df = spark.sql('''
        SELECT 
            url AS indicator,
            'url' AS type,
            'mal_url' AS indicator_type,
            _source AS source,
            urlhaus_link AS source_locator,
            'TLP:CLEAR' AS tlp,
            split(tags, ',') AS tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            map('threat', threat, 'url_status', url_status) AS context,
            to_date(dateadded) AS date_first,
            to_date(coalesce(last_online, dateadded)) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
            urlhaus
    ''')
    return df

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

# COMMAND ----------

@dlt.view()
def abusech_malwarebazaar_normalized():
    dlt.readStream('abusech_malwarebazaar_bronze').createOrReplaceTempView('malwarebazaar')
    
    dfs = [spark.sql(f'''
            SELECT 
                {hash_type}_hash AS indicator,
                '{hash_type}' AS type,
                'mal_{hash_type}' AS indicator_type,
                _source AS source,
                sha256_hash AS source_locator,
                'TLP:CLEAR' AS tlp,
                array(mime_type, file_type_guess, signature) AS tags,
                FROM_JSON('{{}}', 'map<string,string>') AS flags,
                map(
                    'signature', signature,
                    'mime_type', mime_type,
                    'reporter', reporter,
                    'file_name', file_name,
                    'clamav', clamav,
                    'file_type_guess', file_type_guess,
                    'vtpercent', vtpercent
                ) AS context,
                to_date(first_seen_utc) AS date_first,
                to_date(first_seen_utc) AS date_last,
                _collection_ts,
                _raw_record
            FROM 
            malwarebazaar
    ''') for hash_type in ['sha256', 'sha1', 'md5']]
    
    return reduce(DataFrame.unionAll, dfs)
