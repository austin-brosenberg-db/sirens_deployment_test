# Databricks notebook source
import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------

from databricks.sirens.intel.abusech import *
from databricks.sirens.intel.utils import write_ignore_duplicates
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

write_ignore_duplicates(
    spark,
    AbuseChThreatfoxCollector(spark).collect(),
    f'{database}.abusech_threatfox_raw'
)

write_ignore_duplicates(
    spark,
    AbuseChUrlhausCollector(spark).collect(),
    f'{database}.abusech_urlhaus_raw'
)
write_ignore_duplicates(
    spark,
    AbuseChFeodotrackerCollector(spark).collect(),
    f'{database}.abusech_feodotracker_raw'
)
write_ignore_duplicates(
    spark,
    AbuseChMalwareBazaarCollector(spark).collect(),
    f'{database}.abusech_malwarebazaar_raw'
)
