# Databricks notebook source
# MAGIC %pip install pyyaml netaddr geoip2 tldextract pysubnettree {spark.conf.get('sirens_wheel', '')}

# COMMAND ----------

import os
from pathlib import Path

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import

DNS_RESOLVER_PARALLELISM = int(spark.conf.get('dns.resolver.parallelism', '50'))
DNS_RESOLVER_TIMEOUT_SEC = int(spark.conf.get('dns.resolver.timeout_sec', '3'))

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
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
module = get_modules()
logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
logger.info("executing")

# COMMAND ----------

sirens_home = "/Workspace/Repos/derek.king@databricks.com/databricks-sirens/"
spark.conf.set("sirens.home", '/Workspace/Repos/derek.king@databricks.com/databricks-sirens/')

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

def get_cloud_ranges():
    from pyspark.sql import functions as F

    # Create DataFrame for the cloud_ranges table
    cloud_ranges_df = spark.table(f"{database}.cloud_ranges")

    # Create the most_recent DataFrame
    most_recent_df = cloud_ranges_df.groupBy("provider").agg(F.max("date").alias("date"))

    # Join the cloud_ranges_df with most_recent_df
    joined_df = cloud_ranges_df.alias("r").join(
        most_recent_df.alias("m"),
        (cloud_ranges_df["provider"] == most_recent_df["provider"]) & (cloud_ranges_df["date"] == most_recent_df["date"])
    )

    # Filter by type and select required columns
    return joined_df.filter(joined_df["type"] == 4).select("r.ip_prefix", "r.provider", "r.region", "r.service").collect()

cloud_ranges_records = get_cloud_ranges()


# COMMAND ----------

geo_enricher = GeoIPEnrichment(
  city_db, 
  ip_column_name_or_expr='geo'
)
asn_enricher = ASNEnrichment(
    asn_db,
    ip_column_name_or_expr=None
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

geo_info = geo_enricher.create_pandas_udf_function()
asn_info = asn_enricher.create_pandas_udf_function()

rdns_resolve = rdns_enricher.create_pandas_udf_function()
dns_resolve = dns_enricher.create_pandas_udf_function()
tld_extract = tldextract_enricher.create_pandas_udf_function()
cloud_ranges = cloud_ranges_enrichment.create_pandas_udf_function()
cdn_extract = cdn_enricher.create_pandas_udf_function()

# COMMAND ----------

def enrich(df):
      from pyspark.sql.functions import expr, when, array, explode_outer, regexp_extract

      # Define the initial transformations
      with_domain = df.withColumn("_domain", 
                              when(df['type'] == 'domain', df.indicator)
                              .when((df['type'] == 'url') & (~df.indicator.rlike('https?://\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}')) & (df.indicator.rlike('https?://([^/]+)')), 
                                    regexp_extract(df.indicator, 'https?://([^/]+)', 1))
                              .otherwise(None))

      # Apply DNS resolution and handle IPs
      with_ips = with_domain.withColumn("_ips", 
                                    when(with_domain["_domain"].isNotNull(), dns_resolve("_domain"))
                                    .when(df['type'] == 'ip', array(df.indicator))
                                    .when((df['type'] == 'url') & (df.indicator.rlike('https?://\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}')), 
                                          array(regexp_extract(df.indicator, 'https?://(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})', 1)))
                                    .otherwise(array()))

      # Extract TLD and CDN information
      tld_cdn_extracted = with_ips.withColumn("_domain_tld", tld_extract("_domain"))\
                              .withColumn("_domain_cdn", cdn_extract("_domain"))

      # Enrich domain information
      domain_enriched = tld_cdn_extracted.withColumn("domain_enrichment", 
                                                expr("named_struct('suffix', _domain_tld.suffix, 'registered_domain', _domain_tld.registered_domain, 'subdomain', _domain_tld.subdomain, 'cdn_name', _domain_cdn.cdn_name, 'cdn_pattern', _domain_cdn.cdn_pattern)"))

      # Explode IPs for further enrichment
      exploded = domain_enriched.withColumn("_ip", explode_outer("_ips"))

      # Enrich IP information
      ip_enriched = exploded.withColumn("_geo", geo_info("_ip"))\
                        .withColumn("_asn", asn_info("_ip"))\
                        .withColumn("_rdns", rdns_resolve("_ip"))\
                        .withColumn("_cloud_ranges", cloud_ranges("_ip"))

      # Enrich CDN information based on rDNS
      cdn_enriched = ip_enriched.withColumn("_rdns_cdn", cdn_extract("_rdns"))

      # Format the final output
      formatted = cdn_enriched.withColumn("_ip_enrichment_item", 
                                          expr("named_struct('ip', _ip, 'rdns', _rdns, 'asn', _asn.as_number, 'asname', _asn.as_org, 'country', _geo.country, 'city', _geo.city, 'latitude', _geo.latitude, 'longitude', _geo.longitude, 'cloud_prefix', _cloud_ranges.ip_prefix, 'cloud_provider', _cloud_ranges.provider, 'cloud_service', _cloud_ranges.service, 'cloud_region', _cloud_ranges.region, 'cdn_name', _rdns_cdn.cdn_name, 'cdn_pattern', _rdns_cdn.cdn_pattern)"))

      return formatted

# COMMAND ----------

TABLE='intelligence_enriched'
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# Read the stream from the 'intelligence_staging' Delta table
enriched_df = enrich(spark.readStream.table(f"{database}.staging"))
query = (enriched_df.writeStream
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .toTable(f'{database}.{TABLE}')
    )
query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, MapType
spark.conf.set("spark.sql.mapKeyDedupPolicy","LAST_WIN")

# Read the enriched intelligence data
df = spark.read.table(f"{database}.intelligence_enriched")

# Unexploded DataFrame
unexploded = df.groupBy(
    "indicator", "type", "indicator_type", "source", "source_locator", "tlp", "tags",
    F.to_json("flags").alias("flags"), F.to_json("context").alias("context"),
    "date_first", "date_last", "_collection_ts", "_raw_record"
).agg(
    F.first("domain_enrichment").alias("domain_enrichment"),
    F.expr("filter(collect_set(_ip_enrichment_item), e -> e.ip IS NOT NULL)").alias("ip_enrichment")
)

# Final DataFrame
final = unexploded.select(
    "indicator", "type", "indicator_type", "source", "source_locator", "tlp", "tags",
    F.from_json("flags", MapType(StringType(), StringType())).alias("flags"),
    F.from_json("context", MapType(StringType(), StringType())).alias("context"),
    "date_first", "date_last", "ip_enrichment", "domain_enrichment",
    F.col("_collection_ts").alias("collection_ts"),
    F.col("_raw_record").alias("raw_record")
)

# Aggregated DataFrame
aggregated = final.groupBy("indicator", "type").agg(
    F.count("*").alias("count"),
    F.collect_set("indicator_type").alias("indicator_types"),
    F.collect_set(F.concat_ws(":", "source", "source_locator")).alias("source_locator"),
    F.collect_set("tlp").alias("tlps"),
    F.array_distinct(F.flatten(F.collect_set("tags"))).alias("tags"),
    F.expr("aggregate(collect_list(context), cast(map() as map<string,string>), (acc, context) -> map_concat(acc, context))").alias("context"),
    F.min("date_first").alias("date_first"),
    F.max("date_last").alias("date_last"),
    F.array_distinct(F.flatten(F.collect_set("ip_enrichment"))).alias("ip_enrichment"),
    F.first("domain_enrichment").alias("domain_enrichment"),
    F.min("collection_ts").alias("min_collection_ts"),
    F.max("collection_ts").alias("max_collection_ts")
)

# COMMAND ----------

aggregated.write.mode("overwrite").saveAsTable(f"{database}.intelligence")
