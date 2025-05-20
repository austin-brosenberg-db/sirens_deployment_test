# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------

from databricks.sirens.intel.cloud_ranges import get_all_ranges
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_date
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

schema = StructType([
    StructField('ip_prefix', StringType(), True),
    StructField('region', StringType(), True),
    StructField('service', StringType(), True),
    StructField('type', IntegerType(), True),
    StructField('provider', StringType(), True),
])

# COMMAND ----------

(spark.createDataFrame(get_all_ranges(), schema=schema)
  .withColumn('date', current_date())
  .write.mode('append')
  .format('delta')
  .saveAsTable(f'{database}.cloud_ranges')
)