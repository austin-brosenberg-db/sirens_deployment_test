# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import requests
import datetime
import requests
import json
import yaml

from pyspark.sql.types import *

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------

from databricks.sirens.intel.utils import write_ignore_duplicates
from databricks.sirens.intel.schema import raw_schema

# COMMAND ----------

dbutils.widgets.text('repo_path', '')
dbutils.widgets.text('repo_branch', '')
dbutils.widgets.text('intel.database', '')

# COMMAND ----------

repo_path = dbutils.widgets.get('repo_path').rstrip('/')
repo_branch = dbutils.widgets.get('repo_branch')
database = dbutils.widgets.get('intel.database')

threats_file = f'/Workspace{repo_path}/threats.yaml'
allowlist_file = f'/Workspace{repo_path}/allowlist.yaml'

# COMMAND ----------

print(f'database = {database}')
print(f'repo_path = {repo_path}')
print(f'repo_branch = {repo_branch}')
print(f'threats_file = {threats_file}')
print(f'allowlist_file = {allowlist_file}')

# COMMAND ----------

databricksURL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
myToken = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {
  "Authorization": f"Bearer {myToken}",
  "Content-Type": "application/javascript",
}

# COMMAND ----------

def get_repo_id(url, repo_path, headers):
  response = requests.get(f'{url}/api/2.0/repos?path_prefix={repo_path}',  headers=headers, timeout=300)
  if response.status_code == requests.codes.ok:
    git_repo = response.json()['repos'][0]
    return git_repo['id']
  else:
    response.raise_for_status()

def sync_repo(url, repo_id, branch, headers):
  data = json.dumps({'branch': branch})
  response = requests.patch(
    f"{url}/api/2.0/repos/{repo_id}", 
    headers=headers, 
    data=data
  )
  if response.status_code == requests.codes.ok:
      return response.json()
  else:
      {}

# COMMAND ----------

print(f'get repo ID for repo_path: {repo_path} ...')
repo_id = get_repo_id(databricksURL, repo_path, headers)

print(f'Syncing repo {repo_id} ...')
sync_resp = sync_repo(databricksURL, repo_id, repo_branch, headers)

print(f'Repo Sync Response: {sync_resp}')

# COMMAND ----------

collection_time = datetime.datetime.now()
print(f'Reading {threats_file} ...')
with open(threats_file, 'rt') as inf:
  threats = yaml.safe_load(inf.read())

# COMMAND ----------

def convert_data(data):
    if isinstance(data, (str, int, float, bool)):
        return data
    elif isinstance(data, list):
        return [convert_data(item) for item in data]
    elif isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
            new_data[key] = convert_data(value)
        return new_data
    else:
        # Convert unsupported types to strings
        return str(data)

threats = [convert_data(record) for record in threats]

# COMMAND ----------

records = [{
  '_source': 'databricks private intelligence',
  '_type': 'indicator',
  '_raw_record': json.dumps(record),
  '_collection_ts': collection_time
} for record in threats]

write_ignore_duplicates(
    spark,
    spark.createDataFrame(records, schema=raw_schema),
    f'{database}.private_intelligence_raw'
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from 
# MAGIC threat_intelligence.private_intelligence_raw
# MAGIC order by _collection_ts DESC 
# MAGIC limit 10

# COMMAND ----------

collection_time = datetime.datetime.now()
print(f'Reading {allowlist_file} ...')
with open(allowlist_file, 'rt') as inf:
  allowlist = yaml.safe_load(inf.read())

# COMMAND ----------

records = [{
  'source': 'databricks private allowlist',
  'type': record.get('type'),
  'indicator': record.get('indicator'),
  'label': record.get('label'),
  'comment': record.get('comment'),
  'raw_record': json.dumps(record),
  'collection_ts': collection_time
} for record in allowlist]

schema = (StructType()
    .add('source', StringType())
    .add('indicator', StringType())
    .add('type', StringType())
    .add('label', StringType())
    .add('comment', StringType())
    .add('raw_record', StringType())
    .add('collection_ts', TimestampType(), True)
)

(spark.createDataFrame(records, schema=schema)
.write.mode('overwrite')
.saveAsTable(f'{database}.private_allowlist_bronze'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from 
# MAGIC threat_intelligence.private_allowlist_bronze
# MAGIC order by collection_ts DESC 
# MAGIC limit 10

# COMMAND ----------


