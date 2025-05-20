# Databricks notebook source
import requests
import zipfile
import json
import os
import datetime
import fnmatch
from pyspark.sql.functions import *

sirens_home = spark.conf.get("sirens.home", None)
if sirens_home is not None:
  os.chdir(sirens_home)

import databricks
import os
databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------

from databricks.sirens.intel.cve import cve_record_schema
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules
module = get_modules()

# COMMAND ----------

def download_zip_file(url, save_path):
    tmp_file = save_path + ".tmp"
    try:
        response = requests.get(url, stream=True, timeout=300)
        with open(tmp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):  
                f.write(chunk)
    except:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        raise
    os.rename(tmp_file, save_path)


def delete_existing_zip_file(remove_path):
    try:
        files = os.listdir(remove_path)
        patterns = ["cves_*.json", "cvelistV5.zip", "cvelistV5.zip.tmp"]
        for file in files:
            file_path = os.path.join(remove_path, file)
            if os.path.isfile(file_path) and any(fnmatch.fnmatch(file, pattern) for pattern in patterns):
                print(f"Deleting {file} ...")
                os.remove(file_path)
        print(f"All files in {remove_path} deleted successfully.")
    except OSError:
        print("Error occurred while deleting files.")

def extract_cve_data(zip_file_url, output_file):
    # Download the ZIP file
    zip_file_path = "cvelistV5.zip"
    if os.path.exists(zip_file_path):
        delete_existing_zip_file(os.getcwd())
    print(f'Downloading {zip_file_url} to {zip_file_path} ...')
    download_zip_file(zip_file_url, zip_file_path)

    # Unzip and process the files
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        print(f'Writing to {output_file} ...')
        with open(output_file, "w") as output_json_file:
            for file_info in zip_ref.infolist():
                file_name = file_info.filename

                if file_name.endswith('/recent_activities.json'):
                  continue

                # Process only files starting with "cves/" and ending with ".json"
                if file_name.startswith("cvelistV5-main/cves/") and file_name.endswith(".json"):
                    with zip_ref.open(file_info) as json_file:
                        output_json_file.write(json.dumps(json.load(json_file)))
                        output_json_file.write("\n")  # Add a newline between each JSON object

# COMMAND ----------

def bronze_to_silver(df):
    # select most common columns
    df = df.select('cveId', 'state', 'datePublished', 'dateUpdated', 'assignerOrgId', 'assignerShortName', 'descriptions', 'problemTypes', 'affected', 'references', 'metrics', 'source', 'dataType', 'dataVersion', 'rawJson')

    # explode problemTypes array to problemTypesElement
    df = df.withColumn('problemTypesElement', explode_outer(df.problemTypes)).drop('problemTypes')
    # explode problemTypesElement array to problemTypesDescriptionElement
    df = df.withColumn('problemTypesDescriptionElement', explode_outer(df.problemTypesElement.descriptions)).drop('problemTypesElement')
    # coalesce CWE-ID and cweId field in problemTypesDescriptionElement
    df = df.withColumn('temp1', df.problemTypesDescriptionElement['CWE-ID']).withColumn('temp2', df.problemTypesDescriptionElement['cweId'])
    df = df.withColumn('cweId', coalesce(df.temp1, df.temp2)).drop('temp1', 'temp2')
    df = df.withColumn('problemTypesDescriptionElement', df.problemTypesDescriptionElement.dropFields('`CWE-ID`', 'cweId').withField('cweId', df['cweId']))
    # reformat problemTypes to contain an array of problemTypesDescriptionElements
    df = df.groupBy('cveId', 'state', 'datePublished', 'dateUpdated', 'assignerOrgId', 'assignerShortName', 'descriptions', 'affected', 'references', 'metrics', 'source', 'dataType', 'dataVersion', 'rawJson').agg(collect_list('problemTypesDescriptionElement').alias('problemTypes'))
    # reorder columns
    df = df.select('cveId', 'state', 'datePublished', 'dateUpdated', 'assignerOrgId', 'assignerShortName', 'descriptions', 'problemTypes', 'affected', 'references', 'metrics', 'source', 'dataType', 'dataVersion', 'rawJson')
    return df

def load_tables(tablename, local_file, dbfs_file):
    extract_cve_data(
        "https://github.com/CVEProject/cvelistV5/archive/refs/heads/main.zip", 
        local_file
    )

    print(f'Loading from {dbfs_file} ...')
    bronze_df = (spark.read.text(dbfs_file)
            .withColumn('record', from_json(col('value'), cve_record_schema))
            .selectExpr('record.cveMetadata.*', 'record.containers.cna.*', 'record.*', 'value as rawJson')
            .drop('cveMetadata', 'containers')
    )
    silver_df = bronze_to_silver(bronze_df)
    print(f'Writing to {tablename + "_bronze"} ...')
    bronze_df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename + "_bronze")
    print(f'Writing to {tablename + "_silver"} ...')
    silver_df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(tablename + "_silver")


# COMMAND ----------

catalog = spark.conf.get('intel.catalog', None)         # set this config to target catalog name in UC enabled workspaces
database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)
dbfs_path = spark.conf.get('intel.cve.dbfs_path', 'data/cves')
if catalog is not None:
    database = f'{catalog}.{database}'

today = str(datetime.date.today())
sec_ts = datetime.datetime.now().strftime('%s')
basedir = f'/dbfs/{dbfs_path}/latest_release'
basedir_dbfs = f'dbfs:/{dbfs_path}/latest_release'
filename = f"cves_{today}_{sec_ts}.json"

# COMMAND ----------

print(f'today={today}, sec_ts={sec_ts}, basedir={basedir}, basedir_dbfs={basedir_dbfs}')

os.makedirs(basedir, exist_ok=True)
os.chdir(basedir)

load_tables(f'{database}.cves', f'{basedir}/{filename}', f'{basedir_dbfs}/{filename}')
