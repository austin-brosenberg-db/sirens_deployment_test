# Databricks notebook source
# MAGIC %pip install pyyaml netaddr geoip2 tldextract pysubnettree {spark.conf.get('sirens_wheel', '')}

# COMMAND ----------

import os
from pathlib import Path

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)



DNS_RESOLVER_PARALLELISM = int(spark.conf.get('dns.resolver.parallelism', '50'))
DNS_RESOLVER_TIMEOUT_SEC = int(spark.conf.get('dns.resolver.timeout_sec', '3'))

# COMMAND ----------

import dlt

# COMMAND ----------

from databricks.sirens.enrichment.geoip import GeoIPEnrichment, ASNEnrichment
from databricks.sirens.enrichment.dns import DnsResolutionEnrichment, RDnsResolutionEnrichment
from databricks.sirens.enrichment.tldextract import TLDExtractEnrichment
from databricks.sirens.enrichment.cidr_enrichment import CidrEnrichment
from databricks.sirens.enrichment.cdn import CdnEnrichment
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.exceptions import SirensConfigException
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

maxmind_loc = ConfigReader.get_config_key(ConfigReader.read(), "schema:threat_intel", "maxmind_db_location")
scratch_dir = ConfigReader.get_config_key(ConfigReader.read(), "default", "scratch_dir")

if maxmind_loc:
    if not sirens_home:
        raise SirensConfigException("sirens_home spark config var not set - cannot determined the home of maxmind databases")
    
    asn_db = os.path.join(spark.conf.get('sirens.home'), maxmind_loc, "GeoLite2-ASN.mmdb")
    city_db = os.path.join(spark.conf.get('sirens.home'), maxmind_loc, "GeoLite2-City.mmdb")
    if not Path(asn_db).is_file() or not Path(city_db).is_file():
        raise SirensConfigException(f'cannot find maxmind database files: asb_db={asn_db}, city_db={city_db}, maxmind_loc={maxmind_loc}, sirens_home={sirens_home}')

    asn_db = "file:" + str(asn_db)
    city_db = "file:" + str(city_db)
    dbfs_scratch = "file:" + scratch_dir + "/maxmind_db"

    try:
        dbutils.fs.mkdirs(dbfs_scratch)
        dbutils.fs.cp(asn_db, dbfs_scratch)
        dbutils.fs.cp(city_db, dbfs_scratch)
        asn_db = "dbfs:" + scratch_dir + "/maxmind_db/GeoLite2-ASN.mmdb"
        city_db = "dbfs:" + scratch_dir + "/maxmind_db/GeoLite2-City.mmdb"
    except Exception as exc:
        raise SirensConfigException(f"Cannot locate maxmind database files: {exc}")
else:
    asn_db = 'dbfs:/maxmind/GeoLite2-ASN.mmdb'
    city_db = 'dbfs:/maxmind/GeoLite2-City.mmdb'

# COMMAND ----------

cloud_ranges_records = spark.sql(f'''
    WITH most_recent AS (
        SELECT
            provider, 
            MAX(date) as date
        FROM
            {database}.cloud_ranges
        GROUP BY 1
    )
    SELECT 
        r.ip_prefix,
        r.provider,
        r.region,
        r.service
    FROM
        {database}.cloud_ranges r 
            JOIN most_recent m 
                ON (r.provider=m.provider AND r.date=m.date)
    WHERE
        type = 4
    ''').collect()


# COMMAND ----------

asn_enricher = ASNEnrichment(
  spark.sparkContext, 
  asn_db, 
  ip_column_name_or_expr=None,
  use_dbfs_directly=True
)

geo_enricher = GeoIPEnrichment(
  spark.sparkContext, 
  city_db, 
  ip_column_name_or_expr=None,
  use_dbfs_directly=True
)

dns_enricher = DnsResolutionEnrichment(
  ip_column_name_or_expr=None,
  dest_column_name=None,
  parallelism=DNS_RESOLVER_PARALLELISM,
  timeout=DNS_RESOLVER_TIMEOUT_SEC,
)

rdns_enricher = RDnsResolutionEnrichment(
  ip_column_name_or_expr=None,
  dest_column_name=None,
  parallelism=DNS_RESOLVER_PARALLELISM,
  timeout=DNS_RESOLVER_TIMEOUT_SEC,
)

tldextract_enricher = TLDExtractEnrichment(
    src_column_name_or_expr=None,
    dest_column_name=None
)

cloud_ranges_enrichment = CidrEnrichment(
    cloud_ranges_records,
    None,
    None
)

cdn_enricher = CdnEnrichment(
    src_column_name_or_expr=None,
    dest_column_name=None
)

spark.udf.register('geo_info', geo_enricher.create_pandas_udf_function())
spark.udf.register('asn_info', asn_enricher.create_pandas_udf_function())
spark.udf.register('rdns_resolve', rdns_enricher.create_pandas_udf_function())
spark.udf.register('dns_resolve', dns_enricher.create_pandas_udf_function())
spark.udf.register('tld_extract', tldextract_enricher.create_pandas_udf_function())
spark.udf.register('cloud_ranges', cloud_ranges_enrichment.create_pandas_udf_function())
spark.udf.register('cdn_extract', cdn_enricher.create_pandas_udf_function())

# COMMAND ----------

def enrich(df):
    df.createOrReplaceTempView('intel')
    results = spark.sql('''
        WITH with_domain AS (
            SELECT 
                *,
                CASE 
                    WHEN `type` = 'domain'
                        THEN indicator
                    WHEN (`type` = 'url' AND indicator NOT RLIKE 'https?://\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}' AND indicator RLIKE 'https?://([^/]+)' )
                        THEN REGEXP_EXTRACT(indicator, 'https?://([^/]+)', 1)
                    ELSE
                        NULL
                END as _domain            
            FROM 
                intel
        ),
        with_ips AS (
            SELECT
                *,
                CASE 
                    WHEN _domain IS NOT NULL 
                        THEN dns_resolve(_domain)
                    WHEN `type` = 'ip' 
                        THEN array(indicator)
                    WHEN `type` = 'url' AND indicator RLIKE 'https?://\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}'
                        THEN array(REGEXP_EXTRACT(indicator, 'https?://(\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3})', 1))
                    ELSE
                        array()
                END as _ips
            FROM
                with_domain
        ),
        tld_cdn_extracted AS (
            SELECT
                *,
                tld_extract(_domain) as _domain_tld,
                cdn_extract(_domain) as _domain_cdn
            FROM
                with_ips
        ),
        domain_enriched AS (
            SELECT 
                *,
                named_struct(
                  'suffix', _domain_tld.suffix,
                  'registered_domain', _domain_tld.registered_domain,
                  'subdomain', _domain_tld.subdomain,
                  'cdn_name', _domain_cdn.cdn_name,
                  'cdn_pattern', _domain_cdn.cdn_pattern
                ) AS domain_enrichment
            FROM 
                tld_cdn_extracted
        ),
        exploded AS (
            SELECT 
                *,
                explode_outer(_ips) as _ip 
            FROM 
                domain_enriched
        ),
        ip_enriched AS (
            SELECT 
                *,
                geo_info(_ip) AS _geo,
                asn_info(_ip) AS _asn,
                rdns_resolve(_ip) as _rdns,
                cloud_ranges(_ip) as _cloud_ranges
            FROM 
                exploded
        ),
        cdn_enriched AS (
            SELECT
                *,
                cdn_extract(_rdns) as _rdns_cdn
            FROM
                ip_enriched
        ),
        formatted AS (
            SELECT 
                named_struct(
                    'ip', _ip,
                    'rdns', _rdns,
                    'asn', _asn.as_number,
                    'asname', _asn.as_org,
                    'country', _geo.country,
                    'city', _geo.city,
                    'latitude', _geo.latitude,
                    'longitude', _geo.longitude,
                    'cloud_prefix', _cloud_ranges.ip_prefix,
                    'cloud_provider', _cloud_ranges.provider,
                    'cloud_service', _cloud_ranges.service,
                    'cloud_region', _cloud_ranges.region,
                    'cdn_name', _rdns_cdn.cdn_name,
                    'cdn_pattern', _rdns_cdn.cdn_pattern
                ) as _ip_enrichment_item,
                *
            FROM 
               cdn_enriched
        )
        SELECT * 
        FROM 
            formatted
    ''')

    return results
    

# COMMAND ----------

@dlt.table()
def intelligence_enriched():
    df = dlt.readStream('intelligence_staging')
    return enrich(df)

# COMMAND ----------

@dlt.table(spark_conf={'pipelines.trigger.interval': '5 minutes'})
def intelligence():
    spark.conf.set("spark.sql.mapKeyDedupPolicy","LAST_WIN")
    return spark.sql('''
        with unexploded AS (
            SELECT
                indicator,
                type,
                indicator_type,
                source,
                source_locator,
                tlp,
                tags,
                TO_JSON(flags) as flags, 
                TO_JSON(context) as context, 
                date_first, 
                date_last, 
                _collection_ts,
                _raw_record,
                FIRST(domain_enrichment) as domain_enrichment,
                FILTER(collect_set(_ip_enrichment_item), e -> e.ip IS NOT NULL) as ip_enrichment
            FROM 
                LIVE.intelligence_enriched
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
        ),
        final AS (
            SELECT
                indicator,
                type,
                indicator_type,
                source,
                source_locator,
                tlp,
                tags,
                FROM_JSON(flags, 'map<string,string>') as flags,
                FROM_JSON(context, 'map<string,string>') as context,
                date_first,
                date_last,
                ip_enrichment,
                domain_enrichment,
                _collection_ts as collection_ts,
                _raw_record as raw_record
            FROM
                unexploded
        )
        SELECT 
            indicator,
            type,
            COUNT(1) as count,
            collect_set(indicator_type) as indicator_types,
            collect_set(source) as sources,
            collect_set(source || ':' || source_locator) as source_locator,
            collect_set(tlp) as tlps,
            array_distinct(flatten(collect_set(tags))) as tags,
            aggregate(collect_list(context), FROM_JSON('{}', 'map<string,string>'), (acc, context) -> map_concat(acc, context)) as context,
            MIN(date_first) as date_first,
            MAX(date_last) as date_last,
            array_distinct(flatten(collect_set(ip_enrichment))) as ip_enrichment,
            FIRST(domain_enrichment) as domain_enrichment,
            MIN(collection_ts) as min_collection_ts,
            MAX(collection_ts) as max_collection_ts
        FROM 
            final
        GROUP BY 1,2
    ''')
