# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------

from databricks.sirens.intel.tranco import *
from databricks.sirens.intel.utils import write_replace_where
from pyspark.sql.functions import *
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
table = f'{database}.tranco_raw'

df = (TrancoCollector(spark)
  .collect()
  .withColumn('date', to_date(col('_collection_ts')))
)
date = str(df.select('date').distinct().collect()[0].date)
write_replace_where(spark, df, table, f"date = '{date}'")