# Databricks notebook source
# MAGIC %md # itisac_collector
# MAGIC
# MAGIC queries the Trustar API for IT-ISAC, collects reports, collects indicators, writes to it-isac raw table
# MAGIC
# MAGIC **NOTE:** must downgrade `setuptools` in order to install `trustar`, ref: https://github.com/MISP/misp-modules/issues/523

# COMMAND ----------

# MAGIC %pip install setuptools==57.5.0 
# MAGIC %pip install trustar==0.3.34

# COMMAND ----------

import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

import ast
dbutils.widgets.text("options", '', label="options")

try:
    options = ast.literal_eval(dbutils.widgets.get("options"))
except Exception as exc:
    raise exc

feed = {**options}

# COMMAND ----------

from requests import HTTPError
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
lookback_hours = int(spark.conf.get('itisac.collector.lookback_hours', '24'))


# COMMAND ----------

from databricks.sirens.intel.itisac import *
from databricks.sirens.intel.utils import write_ignore_duplicates

# COMMAND ----------
scope = feed.get("token").get("scope") or "it-isac"
user_api_key = feed.get("token").get("api_key") or "user_api_key"
user_api_secret = feed.get("token").get("api_secret") or "user_api_secret"

try:
    user_api_key = dbutils.secrets.get(scope = scope, key = user_api_key)
    user_api_secret = dbutils.secrets.get(scope = scope, key = user_api_secret)
except Exception as e:
    print(e)

try:
  write_ignore_duplicates(
      spark,
      ItIsacCollector(spark, user_api_key, user_api_secret).collect(lookback_hours),
      f'{database}.itisac_raw'
  )
except HTTPError as e:
  print(e)
  pass