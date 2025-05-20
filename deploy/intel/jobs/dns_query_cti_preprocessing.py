# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import



# COMMAND ----------

import dlt
from pyspark.sql.functions import expr
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------

TABLE = 'dns_query_threat_intel_events'
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

# COMMAND ----------

interval_hours = int(spark.conf.get('dns.threat.intel.lookback.interval_hours', '4'))
interval_days  = int(spark.conf.get('dns.threat.intel.lookback.interval_days', '1'))

# COMMAND ----------

from pyspark.sql import functions as F


def dns_queries_summary():
    dns_query_logs_batch = spark.table("LIVE.dns_query_logs_batch")
    filtered_dns_query_logs_batch = dns_query_logs_batch.filter(
        (F.col("date") >= F.current_date() - F.expr(f"{interval_days}")) &
        (F.col("ingestTimestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {interval_hours} HOUR"))
    )
    return filtered_dns_query_logs_batch.groupBy("date", "region", "accountId", "vpcId", "queryName")\
        .agg(
            F.countDistinct("srcAddr").alias("srcAddrCount"),
            F.min("queryTimestamp").alias("minQueryTimestamp"),
            F.max("queryTimestamp").alias("maxQueryTimestamp"),
            F.collect_set("rcode").alias("rcodes"),
            F.collect_set("queryType").alias("queryTypes"),
            F.array_distinct(F.flatten(F.collect_set("answers"))).alias("answers"),
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.expr("TRANSFORM(FILTER(answers, a -> a.Type = 'A'), a -> a.Rdata)")
                    )
                )
            ).alias("answer_ips"),
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.expr("TRANSFORM(FILTER(answers, a -> a.Type IN ('CNAME', 'NS')), a -> a.Rdata)")
                    )
                )
            ).alias("answer_domains"),
            F.collect_set("srcAddr").alias("srcAddrs"),
            F.collect_set(F.col("srcIds.instance")).alias("srcId_instances"),
            F.count(F.lit(1)).alias("count")
        )

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