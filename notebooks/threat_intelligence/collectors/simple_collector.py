# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

import os

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
    os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------

dbutils.widgets.text("options", '', label="options")
dbutils.widgets.text("context", '', label="context")
dbutils.widgets.text("database", '', label="database")

# COMMAND ----------

import ast
try:
    options = ast.literal_eval(dbutils.widgets.get("options"))
    context = ast.literal_eval(dbutils.widgets.get("context"))
except Exception as exc:
    raise exc

database = dbutils.widgets.get("database")
feed = {**options, **context}

# COMMAND ----------

from databricks.sirens.intel.simple import SimpleCollector
from databricks.sirens.intel.utils import write_ignore_duplicates
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

# COMMAND ----------

df = None
try:
    if 'ignore_pattern' not in feed.keys():
        feed['ignore_pattern'] = SimpleCollector.get_default_ignore_pattern()
    
    # if database not passed as a yaml config
    # look in notebook options else look at config file
    if 'database' not in feed.keys():
        if not database:
            database = Schemas.get_schema(module=module.THREAT_INTEL).name
    else:
        database = feed.get("database")

    logger.info(f"Running collector for: {feed.get('source_name')}")
    df = SimpleCollector(spark, **feed).collect()

except Exception as e:
    logger.info(f"Failed to collect {feed['source_name']}: {e}")

write_ignore_duplicates(spark, df, f"{database}.misc_raw")
