# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)
import databricks_sirens.fix_package_import

# COMMAND ----------

from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.modules import get_modules
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
module = get_modules()
logger.info("executing")

# COMMAND ----------

database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

# COMMAND ----------

interval_hours = int(spark.conf.get('vpcflow.threat.intel.lookback.interval_hours', '4'))
interval_days  = int(spark.conf.get('vpcflow.threat.intel.lookback.interval_days', '1'))

# COMMAND ----------

#@dlt.table(spark_conf={'pipelines.trigger.interval': '1 hour'})

# COMMAND ----------

from pyspark.sql import functions as F

def flow_summary(tablename):
    df = spark.table(tablename)
    current_date = F.current_date()
    current_timestamp = F.current_timestamp()
    return df.filter(
        (F.col("date") > current_date - F.expr(f"{interval_days}")) &
        (F.col("hour_ingestTimestamp") > current_timestamp - F.expr(f"INTERVAL {interval_hours} HOUR"))
    ).groupBy(
        "accountName", "accountId", "region", "protocol", "pktDstAddr", "flowDirection"
    ).agg(
        F.collect_set("action").alias("actions"),
        F.collect_set("dstPort").alias("dstPorts"),
        F.sum("count").alias("count"),
        F.min("date").alias("first_seen"),
        F.max("date").alias("last_seen"),
        F.countDistinct("date").alias("num_days"),
        F.collect_set("srcAddr").alias("srcAddrs"),
        F.collect_set("pktDstAddr_asn.as_org").alias("dstAsns"),
        F.collect_set("pktDstAddr_geo.country_code").alias("dstCountryCodes"),
        F.collect_set("pktDstAddr_geo.city").alias("dstCities"),
        F.array_distinct(F.flatten(F.collect_set(F.expr("transform(tcpFlags, flag -> cast(flag as int))")))).alias("tcpFlags"),
        F.sum("bytes").alias("bytes"),
        F.sum("packets").alias("packets")
    )

# COMMAND ----------

ip_threat_intel_df = spark.read.table(f'{database}.curated_c2_ips')
metacluster_flows_df = flow_summary(f'{database}.metacluster_flow_egress_summarized')
nat_flows_df = flow_summary(f'{database}.nat_flow_egress_summarized')
vpc_flows_df = flow_summary(f'{database}.vpc_flow_egress_summarized')

# COMMAND ----------

TABLE = 'metacluster_flow_egress_threat_intel_events'
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

metacluster_flow_egress_threat_intel_events_df = metacluster_flows_df.join(ip_threat_intel_df, on=(metacluster_flows_df.pktDstAddr == ip_threat_intel_df.ip))

query = (metacluster_flow_egress_threat_intel_events_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)

# COMMAND ----------

TABLE = 'nat_flow_egress_threat_intel_events'
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

nat_flow_egress_threat_intel_events_df = nat_flows_df.join(ip_threat_intel_df, on=(nat_flows_df.pktDstAddr == ip_threat_intel_df.ip))

query = (nat_flow_egress_threat_intel_events_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)

# COMMAND ----------

TABLE = 'vpc_flow_egress_threat_intel_events'
checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

vpc_flow_egress_threat_intel_events_df =  vpc_flows_df.join(ip_threat_intel_df, on=(vpc_flows_df.pktDstAddr == ip_threat_intel_df.ip))

query = (vpc_flow_egress_threat_intel_events_df.writeStream
    .option("checkpointLocation", checkpoint_dir)
    .trigger(once=True)
    .toTable(f'{database}.{TABLE}')
)
query.awaitTermination()
logger.info("completed")
