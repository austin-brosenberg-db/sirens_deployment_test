# Databricks notebook source

import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)



# COMMAND ----------
import dlt
from pyspark.sql.functions import expr
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

# COMMAND ----------

interval_hours = int(spark.conf.get('dns.threat.intel.lookback.interval_hours', '4'))
interval_days  = int(spark.conf.get('dns.threat.intel.lookback.interval_days', '1'))

# COMMAND ----------

@dlt.view()
def dns_queries_summary():
    return spark.sql(f'''
          SELECT 
            date,
            region,
            accountId,
            vpcId,
            queryName,
            count(distinct srcAddr) as srcAddrCount,
            MIN(queryTimestamp) as minQueryTimestamp,
            MAX(queryTimestamp) as maxQueryTimestamp,
            collect_set(rcode) as rcodes,
            collect_set(queryType) as queryTypes,
            array_distinct(flatten(collect_set(answers))) as answers,
            array_distinct(flatten(collect_set(
                TRANSFORM(
                    FILTER(
                        answers, 
                        a -> a.Type = "A"
                    ),
                    a -> a.Rdata
                )
            ))) as answer_ips,
            array_distinct(flatten(collect_set(
                TRANSFORM(
                    FILTER(
                        answers, 
                        a -> a.Type IN ("CNAME", "NS")
                    ),
                    a -> a.Rdata
                )
            ))) as answer_domains,
            collect_set(srcAddr) as srcAddrs,
            collect_set(srcIds.instance) as srcId_instances,
            COUNT(1) as count
        FROM 
            LIVE.dns_query_logs_batch
        WHERE
                date >= current_date - {interval_days} AND 
                ingestTimestamp >= current_timestamp - INTERVAL {interval_hours} HOUR
        GROUP BY 1,2,3,4,5
    ''')

@dlt.view()
def domain_threat_intel():
    return spark.sql(f'''
        select 
            *,
            domain || '.' AS domain_dot
        from 
            {database}.curated_c2_domains
    ''')

@dlt.table(spark_conf={'pipelines.trigger.interval': '1 hour'})
def dns_query_threat_intel_events():
    cti = dlt.read('domain_threat_intel')
    dns = dlt.read('dns_queries_summary')
    return (
        dns.join(cti, on=(dns.queryName == cti.domain_dot)).drop('domain_dot')
    )