# Databricks notebook source

import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)



# COMMAND ----------
import dlt
from functools import reduce
from pyspark.sql import DataFrame
from databricks.sirens.utils.base_utils import BaseUtils

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
                FROM_JSON('{% raw %}{{}}{% endraw %}', 'map<string,string>') AS flags,
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

# COMMAND ----------

@dlt.view()
def phishtank_normalized():
    dlt.readStream('phishtank_bronze').createOrReplaceTempView('phishtank')
    
    df = spark.sql('''
        SELECT 
            url AS indicator,
            'url' AS type,
            'phishing_url' AS indicator_type,
            _source AS source,
            phish_detail_url AS source_locator,
            'TLP:CLEAR' AS tlp,
            array(target) AS tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            map(
                'online', online,
                'verification_time', CAST(verification_time AS string),
                'target', target,
                'verified', verified
            ) AS context,
            to_date(submission_time) AS date_first,
            to_date(coalesce(verification_time, submission_time)) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
            phishtank
    ''')

    return df

# COMMAND ----------

@dlt.view()
def eset_malware_normalized():
    dlt.readStream('eset_malware_bronze').createOrReplaceTempView('eset_malware')
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
            eset_malware
        ''')
            

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

# COMMAND ----------

@dlt.view()
def private_intelligence_normalized():

    dlt.readStream('private_intelligence_bronze').createOrReplaceTempView('private_intelligence')

    return spark.sql('''
        SELECT 
            indicator,
            type,
            indicator_type,
            _source AS source,
            '' AS source_locator,
            'TLP:AMBER' AS tlp,
            tags as tags,
            FROM_JSON('{}', 'map<string,string>') AS flags,
            map('comment', comment) AS context,
            to_date(_collection_ts) AS date_first,
            to_date(_collection_ts) AS date_last,
            _collection_ts,
            _raw_record
        FROM 
            private_intelligence
    ''')

# COMMAND ----------
streams = []
possible_tables = [
        'abusech_threatfox_normalized',
        'abusech_urlhaus_normalized',
        'abusech_feodotracker_normalized',
        'abusech_malwarebazaar_normalized',
        'itisac_indicators_normalized',
        'phishtank_normalized',
        'eset_malware_normalized',
        'misc_normalized',
        'private_intelligence_normalized',
    ]
for tbl in possible_tables:
    if BaseUtils.if_table_exists(spark, database, tbl):
        streams.append(tbl)

# COMMAND ----------
@dlt.view()
def intelligence_staging():
    df = None

    for stream in streams:
        if df is None:
            df = dlt.readStream(stream)
        else:
            df = df.union(dlt.readStream(stream))
    return df.where('type IS NOT NULL')
