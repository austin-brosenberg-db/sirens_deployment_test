# Databricks notebook source

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
max_rank = spark.conf.get('threat.intelligence.popular_domains.max_rank', '100000')

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {database}.curated_c2_domains AS 
    WITH cti_domains AS (
        SELECT 
            indicator AS domain,
            array_distinct(flatten(collect_set(indicator_types))) as indicator_types,
            array_distinct(flatten(collect_set(sources))) as sources,
            array_distinct(flatten(collect_set(source_locator))) as source_locators,
            array_distinct(flatten(collect_set(tlps))) as tlps,
            array_distinct(flatten(collect_set(tags))) as tags,
            array_distinct(flatten(collect_set(ip_enrichment))) as ip_enrichment,
            MIN(min_collection_ts) as min_collection_ts,
            MAX(max_collection_ts) as max_collection_ts,
            FIRST(ip_enrichment)[0].ip AS ip,
            FIRST(ip_enrichment)[0].rdns AS rdns,
            FIRST(ip_enrichment)[0].asn AS asn,
            FIRST(ip_enrichment)[0].asname AS asname,
            FIRST(ip_enrichment)[0].country AS country,
            FIRST(ip_enrichment)[0].city AS city,
            FIRST(ip_enrichment)[0].cloud_prefix AS cloud_prefix,
            FIRST(ip_enrichment)[0].cloud_provider AS cloud_provider,
            FIRST(ip_enrichment)[0].cloud_service AS cloud_service,
            FIRST(ip_enrichment)[0].cloud_region AS cloud_region,
            FIRST(ip_enrichment)[0].cdn_name AS cdn_name,
            FIRST(ip_enrichment)[0].cdn_pattern AS cdn_pattern,
            FIRST(domain_enrichment).suffix as domain_suffix,
            FIRST(domain_enrichment).registered_domain as registered_domain,
            FIRST(domain_enrichment).cdn_name as domain_cdn_name,
            FIRST(domain_enrichment).cdn_pattern as domain_cdn_pattern
        FROM 
            {database}.intelligence
        WHERE
            type = 'domain' AND 
            SIZE(filter(indicator_types, itype -> itype IN ('c2_domain'))) > 0 AND 
            max_collection_ts > CURRENT_DATE - 30 AND 
            min_collection_ts > CURRENT_DATE - 90 AND 
            sources != array('threatfox.abuse.ch')
        group by 1
    ),
    popular_domains as (
        SELECT 
            domain AS domain
        FROM 
            {database}.tranco_bronze
        WHERE 
            _collection_ts = (SELECT MAX(_collection_ts) FROM {database}.tranco_bronze) and
            rank <= {max_rank}
    ),
    allowlist_domains AS (
        select indicator AS domain
        from {database}.private_allowlist_bronze 
        where type = 'domain'
    ),
    dyn_dns_domains AS (
        SELECT distinct indicator AS domain
        FROM threat_intelligence.intelligence
        WHERE 
            type = 'domain' AND 
            SIZE(FILTER(indicator_types, itype -> itype = 'dyndns_domain')) > 0
    ),
    etld_domains AS (
        SELECT distinct replace(indicator, '*.', '') AS domain
        FROM threat_intelligence.intelligence
        WHERE 
            type = 'domain' AND 
            SIZE(FILTER(indicator_types, itype -> itype = 'etld_domain')) > 0
    )
    SELECT *
    FROM 
        cti_domains
    WHERE 
        -- ignore allow listed domains
        domain NOT IN (SELECT domain FROM allowlist_domains) AND 
        -- ignore popular domains
        domain NOT IN (SELECT domain FROM popular_domains) AND 
        -- ignore domains that are effective TLDs.
        domain NOT IN (SELECT domain FROM etld_domains) AND 
        -- ignore domains whose 2TLD is popular, BUT not in dyndns or ETLD since these are user controlled
        NOT (
            registered_domain IN (SELECT domain FROM popular_domains) AND    
            (
                registered_domain NOT IN (SELECT domain FROM dyn_dns_domains) OR 
                registered_domain NOT IN (SELECT domain FROM etld_domains)
            )
        )
''')

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {database}.curated_c2_ips AS 
    WITH allowlist_ips AS (
        SELECT 
            indicator AS ip
        FROM 
            {database}.private_allowlist_bronze 
        WHERE 
            type = 'ip'
    )
    SELECT 
        indicator as ip,
        array_distinct(flatten(collect_set(indicator_types))) as indicator_types,
        array_distinct(flatten(collect_set(sources))) as sources,
        array_distinct(flatten(collect_set(source_locator))) as source_locators,
        array_distinct(flatten(collect_set(tlps))) as tlps,
        array_distinct(flatten(collect_set(tags))) as tags,
        array_distinct(flatten(collect_set(ip_enrichment))) as ip_enrichment,
        MIN(min_collection_ts) as min_collection_ts,
        MAX(max_collection_ts) as max_collection_ts,
        FIRST(ip_enrichment)[0].rdns AS rdns,
        FIRST(ip_enrichment)[0].asn AS asn,
        FIRST(ip_enrichment)[0].asname AS asname,
        FIRST(ip_enrichment)[0].country AS country,
        FIRST(ip_enrichment)[0].city AS city,
        FIRST(ip_enrichment)[0].cloud_prefix AS cloud_prefix,
        FIRST(ip_enrichment)[0].cloud_provider AS cloud_provider,
        FIRST(ip_enrichment)[0].cloud_service AS cloud_service,
        FIRST(ip_enrichment)[0].cloud_region AS cloud_region,
        FIRST(ip_enrichment)[0].cdn_name AS cdn_name,
        FIRST(ip_enrichment)[0].cdn_pattern AS cdn_pattern
    FROM 
        {database}.intelligence
    where
        type = 'ip' AND
        indicator NOT IN (select ip from allowlist_ips) AND
        max_collection_ts > CURRENT_DATE - 14 AND 
        min_collection_ts > CURRENT_DATE - 90 AND 
        SIZE(FILTER(indicator_types, itype -> itype LIKE 'c2_%')) > 0 AND
        -- ignore parking_ip and tor_ip
        SIZE(FILTER(indicator_types, itype -> itype IN ('tor_ip', 'parking_ip'))) = 0 AND 
        NOT (indicator LIKE ANY ('10.%', '192.168.%')) AND 
        SIZE(filter(ip_enrichment, ip_enrich -> 
          ip_enrich.asname IN (
            'CLOUDFLARENET',
            'EDGECAST',
            'FASTLY'
          )
          OR 
          ip_enrich.rdns LIKE ANY (
            '%.cloudfront.net.', 
            '%.cdn77.com.', 
            '%.1e100.net.',
            '%.akamaitechnologies.com.'
          )
          OR 
          ip_enrich.cloud_provider IN (
            'fastly',
            'google',
            'github',
            'cloudflare'
          ) 
          OR
          ip_enrich.cloud_service LIKE ANY (
            -- Azure
            'Azure AzureDevOps',
            'Azure AzureStorage',
            'Azure AzureAD', 
            'Azure AzureDatabricks',
            'Azure AzureEventHub', 
            'Azure AzureFrontDoor',
            'Azure AzureIdentity',
            -- AWS
            'CLOUDFRONT',
            'S3',
            'CDN',
            'GLOBALACCELERATOR',
            -- Oracle
            '%OBJECT_STORAGE%'
          )
        )) = 0
        AND NOT (
            ARRAY_CONTAINS(source_locator, 'it-isac:https://station.trustar.co/constellation/reports/1f5391fb-1d1d-4a7f-88db-56c3450a76c9') OR
            -- for EC2 and equivalent, we age out very fast
            max_collection_ts < CURRENT_DATE - 7 AND
            SIZE(filter(ip_enrichment, ip_enrich -> 
              ip_enrich.cloud_service IN (
                'EC2',
                'linode',
                'digitalocean',
                'Azure'
              )
            )) > 0 
        )
        AND (sources != array('threatfox.abuse.ch'))
    GROUP BY 1
''')

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {database}.curated_malicious_drivers AS 
    WITH allowlist_sha256 AS (
        SELECT 
            indicator AS sha256
        FROM 
            {database}.private_allowlist_bronze 
        WHERE 
            type = 'sha256'
    )
    SELECT 
        indicator AS sha256,
        array_distinct(flatten(collect_set(indicator_types))) as indicator_types,
        array_distinct(flatten(collect_set(sources))) as sources,
        array_distinct(flatten(collect_set(source_locator))) as source_locators,
        array_distinct(flatten(collect_set(tlps))) as tlps,
        array_distinct(flatten(collect_set(tags))) as tags,
        MIN(min_collection_ts) as min_collection_ts,
        MAX(max_collection_ts) as max_collection_ts
    FROM 
        {database}.intelligence
    WHERE
      indicator NOT IN (select sha256 from allowlist_sha256) AND
      SIZE(FILTER(sources, s ->s = 'magicsword-io/LOLDrivers')) > 0
    group by 1
''')

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {database}.curated_malicious_clients AS 
    WITH allowlist_ips AS (
        SELECT 
            indicator AS ip
        FROM 
            {database}.private_allowlist_bronze 
        WHERE 
            type = 'ip'
    )
    SELECT 
        indicator as ip,
        array_distinct(flatten(collect_set(indicator_types))) as indicator_types,
        array_distinct(flatten(collect_set(sources))) as sources,
        array_distinct(flatten(collect_set(source_locator))) as source_locators,
        array_distinct(flatten(collect_set(tlps))) as tlps,
        array_distinct(flatten(collect_set(tags))) as tags,
        array_distinct(flatten(collect_set(ip_enrichment))) as ip_enrichment,
        MIN(min_collection_ts) as min_collection_ts,
        MAX(max_collection_ts) as max_collection_ts,
        FIRST(ip_enrichment)[0].rdns AS rdns,
        FIRST(ip_enrichment)[0].asn AS asn,
        FIRST(ip_enrichment)[0].asname AS asname,
        FIRST(ip_enrichment)[0].country AS country,
        FIRST(ip_enrichment)[0].city AS city,
        FIRST(ip_enrichment)[0].cloud_prefix AS cloud_prefix,
        FIRST(ip_enrichment)[0].cloud_provider AS cloud_provider,
        FIRST(ip_enrichment)[0].cloud_service AS cloud_service,
        FIRST(ip_enrichment)[0].cloud_region AS cloud_region,
        FIRST(ip_enrichment)[0].cdn_name AS cdn_name,
        FIRST(ip_enrichment)[0].cdn_pattern AS cdn_pattern
    FROM 
        {database}.intelligence
    where
        type = 'ip' AND
        indicator NOT IN (select ip from allowlist_ips) AND
        max_collection_ts > CURRENT_DATE - 7 AND 
        min_collection_ts > CURRENT_DATE - 90 AND 
        SIZE(FILTER(indicator_types, itype -> itype IN ('bot_ip','brute_ip','scan_ip','ssh_ip','tor_ip','vpn_ip'))) > 0 AND
        NOT (indicator LIKE ANY ('10.%', '192.168.%')) 
    GROUP BY 1
''')
