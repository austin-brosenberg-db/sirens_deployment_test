# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)



# COMMAND ----------
import dlt

# COMMAND ----------
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

# COMMAND ----------

interval_hours = int(spark.conf.get('vpcflow.threat.intel.lookback.interval_hours', '4'))
interval_days  = int(spark.conf.get('vpcflow.threat.intel.lookback.interval_days', '1'))

# COMMAND ----------

def flow_summary(tablename):
    return spark.sql(f'''
        SELECT 
            accountName,
            accountId,
            region,
            protocol,
            pktDstAddr,
            flowDirection,
            collect_set(action) as actions,
            collect_set(dstPort) as dstPorts,
            SUM(count) AS count,
            MIN(date) AS first_seen,
            MAX(date) AS last_seen,
            COUNT(DISTINCT date) AS num_days,
            collect_set(srcAddr) as srcAddrs,
            COLLECT_SET(pktDstAddr_asn.as_org) AS dstAsns,
            COLLECT_SET(pktDstAddr_geo.country_code) AS dstCountryCodes,
            COLLECT_SET(pktDstAddr_geo.city) AS dstCities,
            array_distinct(flatten(collect_set(transform(tcpFlags, flag -> cast(flag as int))))) AS tcpFlags,
            SUM(bytes) as bytes,
            SUM(packets) as packets
        FROM
            {tablename}
        WHERE
            date > current_date - {interval_days} AND 
            hour_ingestTimestamp > current_timestamp - INTERVAL {interval_hours} HOUR
        GROUP BY 1,2,3,4,5,6
    ''')

@dlt.view()
def ip_threat_intel():
    return spark.read.table(f'{database}.curated_c2_ips')

@dlt.view()
def metacluster_flows():
    return flow_summary(f'{database}.metacluster_flow_egress_summarized')

@dlt.view()
def nat_flows():
    return flow_summary(f'{database}.nat_flow_egress_summarized')

@dlt.view()
def vpc_flows():
    return flow_summary(f'{database}.vpc_flow_egress_summarized')

@dlt.table(spark_conf={'pipelines.trigger.interval': '1 hour'})
def metacluster_flow_egress_threat_intel_events():
    cti = dlt.read('ip_threat_intel')
    vpcflow = dlt.read('metacluster_flows')
    return (
        vpcflow.join(cti, on=(vpcflow.pktDstAddr == cti.ip))
    )

@dlt.table(spark_conf={'pipelines.trigger.interval': '1 hour'})
def nat_flow_egress_threat_intel_events():
    cti = dlt.read('ip_threat_intel')
    vpcflow = dlt.read('nat_flows')
    return (
        vpcflow.join(cti, on=(vpcflow.pktDstAddr == cti.ip))
    )

@dlt.table(spark_conf={'pipelines.trigger.interval': '1 hour'})
def vpc_flow_egress_threat_intel_events():
    cti = dlt.read('ip_threat_intel')
    vpcflow = dlt.read('vpc_flows')
    return (
        vpcflow.join(cti, on=(vpcflow.pktDstAddr == cti.ip))
    )