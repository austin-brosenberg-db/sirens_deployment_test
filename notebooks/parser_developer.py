# Databricks notebook source
# MAGIC %md
# MAGIC # Guided Datasource parser notebook
# MAGIC
# MAGIC The Parser Developer notebook is designed to help guide you through building the YAML configuration and parser logic file required to process new datasources.
# MAGIC
# MAGIC The notebook is structured in six stages:
# MAGIC * **Stage 0**:  Notebook Setup
# MAGIC * **Stage 1**:  YAML Configuration Development
# MAGIC * **Stage 2**:  Read the Raw file
# MAGIC * **Stage 3**:  Extract metadata. (Bronze Transformation)
# MAGIC * **Stage 4**:  Flatten nested columns & extract silver features
# MAGIC * **Stage 5**:  Filter specific events and normalize data to CIM tables
# MAGIC * **Stage 6**:  Operationalize the working config
# MAGIC
# MAGIC The notebook is designed to be ran & re-ran to build out & correct the parsing logic. To do so,
# MAGIC iterate on the **'configItems'** yaml, **toBronze()**, and **toSilver()** methods.

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 0. Notebook Setup
# MAGIC
# MAGIC Enter the database, source and sourcetype this parser will operate on
# MAGIC - **database** no writes are done in developer mode, but specify one to keep the DataSource() class happy.
# MAGIC - **source** can be thought of as the originating vendor of the datasource (aws, apache, microsoft)
# MAGIC - **sourcetype** is the product or product component of the datasource (cloudtrail, vpc_logs, access_combined, winevent_security)
# MAGIC
# MAGIC Source and sourcetype make the unique datasource, and defines the directory you will place a working config in stage 5.

# COMMAND ----------

# DBTITLE 1,Ensure that pyyaml is installed
# MAGIC %pip install pyyaml inflection

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# DBTITLE 1,Create input widgets
dbutils.widgets.text("database", "", label="database")
dbutils.widgets.text("source", "", label="source")
dbutils.widgets.text("sourcetype", "", label="sourcetype")

# COMMAND ----------

# DBTITLE 1,Fill out widgets & run
source = dbutils.widgets.get("source")
sourcetype = dbutils.widgets.get("sourcetype")
database = dbutils.widgets.get("database")
if source == "" or sourcetype == "":
    raise Exception(
        "please pass the source and sourcetype parameters to multi-task job, or fill out notebook widget values"
    )
else:
    input_source = {"source": source, "sourcetype": sourcetype, "database": database}

print(input_source)

# COMMAND ----------

# DBTITLE 1,Required Imports
import os
import pprint

# depending on where this notebook runs from, the relative path could be ../databricks, ../../databricks, ../../../databricks, or traverse higher.
import databricks

databricks.__path__.append(os.path.abspath("../databricks"))

from databricks.sirens import connectors
from databricks.sirens import normalize
from databricks.sirens.datasource import DataSource
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.parsers.base_parser import BaseParser

# COMMAND ----------

# DBTITLE 1,Parser Support Functions
def bronze_parsing_issues(df):
    """
    bronze_parsing_issues verifies if the passed dataframe values are valid
    param df: A dataframe to verify its content
    Return: The list of errors found in the dataframe, or an empty array if no errors are found.
    """
    errors = []
    timestamp_errors = df.filter(
        col("_event_date").isNull() | col("_event_time").isNull()
    ).count()
    if timestamp_errors:
        errors.append(
            f"Looks like timestamp parsing is failing for {timestamp_errors} rows. - Please check the DataFrame above and correct as required."
        )

    invalid_source = df.filter(col("_source") == source).count()
    if invalid_source:
        errors.append(
            f"Looks like input->source key has not been changed from the default - Please check and set appropriately"
        )

    invalid_sourcetype = df.filter(col("_sourcetype") == sourcetype).count()
    if invalid_sourcetype:
        errors.append(
            f"Looks like input->sourcetype has not been changed from the default - Please check and set appropriately"
        )

    invalid_dvc_hostname = df.filter(col("dvc_hostname").isNull()).count()
    if invalid_dvc_hostname:
        errors.append(
            f"dvc_hostname appears to be failing - please check and correct as required."
        )

    default_dvc_hostname = df.filter(
        col("dvc_hostname") == data_source_obj.workspace_name
    ).count()
    if default_dvc_hostname:
        errors.append(
            f"Looks like dvc_hostname column is defaulting to the workspace_url (last resort) - check and correct if required"
        )

    return errors


def silver_flattening_issues(df):
    errors = []
    num_cols = df.columns
    if len(num_cols) <= 8:
        # Assume 7 metadata fields + min 1 bronze field. We should have more than 8 columns at this point.
        errors.append(
            "There appear to be 8 columns or less. toSilver() should extract fields, or regexp_extract fields from syslog type line based data"
        )

    return errors


# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 1. Develop the YAML keys to ingest & transform data
# MAGIC
# MAGIC The **'configItems'** yaml below will become your inputs.yaml file. It will define how to connect to raw data and process it.
# MAGIC
# MAGIC **Required Steps**
# MAGIC
# MAGIC 1) Enter appropriate information in the **'config'** key.
# MAGIC 2) Configure the **'input'** key.
# MAGIC     - configure source & sourcetype keys
# MAGIC     - configure rawPath to point to your data (make sure you have permissions to read it)
# MAGIC     - add any required options in the **options** key. (These are the [generic load & save](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html) options)
# MAGIC
# MAGIC **Recommendation:** While developing parsers set **'streamType'** to **'batch'**, then migrate to **'streaming'** once you have confidence.

# COMMAND ----------

# DBTITLE 1,Datasource configuration
config_file_def = f"""config:
  version: 1.0
  updated: 1234
  author: user
  description: f2213
input:
  source: {source}
  sourcetype: {sourcetype}
  parser: generic_text
  rawPath: /tmp/alexott-1/
  connector:
    name: autoloader  # json|kafka|csv|txt [required]
    options:
      cloudFiles.format: text
      cloudFiles.includeExistingFiles: true
  streamType: streaming # batch|streaming [required]
transforms:
  bronze:
    meta:
      host_column: host
      table_config:
        partition_cols:
          - _event_date
  silver:
    event_type:
      - target_table: <cim_table>
        filter: <SQL _Filter>
        fields:
          - <target_column>:
            action: alias
            value: <source_column>
"""

config_file_name = f"""{source}_{sourcetype}_config.yaml"""
with open(config_file_name, "w") as f:
    f.write(config_file_def)

# COMMAND ----------

# DBTITLE 1,Read the configuration
data_source_obj = DataSource(spark, database, source, sourcetype,
                             config_file_name=config_file_name)
config = data_source_obj.read()
print(f"INFO: Config Contents\n{pprint.pformat(config)}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 2: Read the raw file
# MAGIC
# MAGIC Test if the configuration in the **inputs** section is valid.

# COMMAND ----------

connector_mod = connectors.get(data_source_obj.get_connector_name())
df = connectors.Reader(connector_mod, spark, data_source_obj).read()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP! ##
# MAGIC You should now see a DataFrame with raw data. If not, go back to your configuration.
# MAGIC
# MAGIC Inspect the **connector->name**, and/or the **options** you are passing. (you may use any supported spark options for the connector type you define)

# COMMAND ----------

# DBTITLE 1,Print the json schema to use in stage 5.
print(df.schema.json())

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 3 Metadata Extraction
# MAGIC
# MAGIC Now its time to tell Sirens how to extract metadata.
# MAGIC
# MAGIC **Required Steps**
# MAGIC 1. Define how to extract the event timestamp.
# MAGIC 2. Define how to extract the sending hostname.
# MAGIC 3. Make any changes in the 'change me or delete me' section of the ``toBronze()`` method in the cell below.
# MAGIC
# MAGIC > #### *1. Timestamp extraction*
# MAGIC > * If your event timestamp is isolated to a single column use the **timestamp_column** and **timestamp_format** keys
# MAGIC > * If the event timestamp is not isolated, extract it with the **timestamp_regex** key in conjunction with the above
# MAGIC >
# MAGIC > ###### Example extraction from column name
# MAGIC > |ts|event|
# MAGIC > |-|-|
# MAGIC > |27/May/2022 19:12:38 +0000| My event data |
# MAGIC >
# MAGIC > ```
# MAGIC >  transforms:
# MAGIC >   bronze:
# MAGIC >     meta:
# MAGIC >       timestamp_column: ts
# MAGIC >       timestamp_format: "dd/MMM/yyyy HH:mm:ss Z"
# MAGIC > ```
# MAGIC >
# MAGIC > ###### Example extraction using regex
# MAGIC > |event|
# MAGIC > |-|
# MAGIC > |2.216.174.119 - - [27/May/2022:20:20:59 +0000] "GET /homepage.html HTTP/1.1" 200 1640 "http://81.43.45.12" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7|
# MAGIC >
# MAGIC > ```
# MAGIC >  transforms:
# MAGIC >   bronze:
# MAGIC >     meta:
# MAGIC >       timestamp_column: event
# MAGIC >       timestamp_format: 'dd/MMM/yyyy HH:mm:ss Z'
# MAGIC >       timestamp_regex:  '^(?<host>[^ ]*) [^ ]* ([^ ]*) \[(?<time>[^\]]*)\]'
# MAGIC > ```
# MAGIC >
# MAGIC > see [time modifiers](https://docs.databricks.com/sql/language-manual/sql-ref-datetime-pattern.html) for more information on valid timestamp_formats and the Sirens user guide for more examples.
# MAGIC
# MAGIC > #### *2. Host extraction*
# MAGIC > Extract the sending host from one of the following;
# MAGIC > * column name in the data
# MAGIC > * column name and optional regex
# MAGIC > * statically
# MAGIC > * from a segment of the raw file path
# MAGIC >
# MAGIC > ###### Example host column extraction
# MAGIC > |ts|host|src_ip|
# MAGIC > |-|-|-|
# MAGIC > |2022-02-27T10:12:32.43|fw-1|192.168.0.34|
# MAGIC >
# MAGIC > ```
# MAGIC >  transforms:
# MAGIC >   bronze:
# MAGIC >     meta:
# MAGIC >       timestamp_column: ts
# MAGIC >       timestamp_format: "yyyy-MM-dd'T'HH:mm:ss"
# MAGIC >       host_column: host
# MAGIC > ```
# MAGIC > ###### Example host column with regex extraction
# MAGIC >
# MAGIC > |event|
# MAGIC > |-|
# MAGIC > |2.216.174.119 - - [27/May/2022:20:20:59 +0000] "GET /homepage.html HTTP/1.1" 200 1640 "http://81.43.45.12" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7|
# MAGIC >
# MAGIC > ```
# MAGIC >  transforms:
# MAGIC >   bronze:
# MAGIC >     meta:
# MAGIC >       timestamp_column: ts
# MAGIC >       timestamp_format: 'dd/MMM/yyyy HH:mm:ss Z'
# MAGIC >       host_column: event
# MAGIC >       host_regex: '^(?<host>[^ ]*)'
# MAGIC > ```
# MAGIC > ###### Example statically assigned
# MAGIC > **Note:** This is specified higher up in the config under the **input** key
# MAGIC >
# MAGIC > ```
# MAGIC >  input:
# MAGIC >   host: linux_web_1
# MAGIC > ```
# MAGIC > ###### Example raw file path segment
# MAGIC > **Note:** This is specified higher up in the config under the **input** key
# MAGIC > |input_filename|
# MAGIC > |-|
# MAGIC > |s3:/cyberdata/syslog/firewalls/fw1/traffic.json|
# MAGIC > ```
# MAGIC >  input:
# MAGIC >   host_rawPath_segment: 4
# MAGIC > ```
# MAGIC
# MAGIC > #### *3. Amend ``toBronze()`` method*
# MAGIC >
# MAGIC > You may need to make minimal changes such as exploding records into single events, or converting json in Map<string,string> types etc.
# MAGIC >
# MAGIC > * Change the ``toBronze()`` method in the template parser below if required using pyspark commands.
# MAGIC >
# MAGIC > * Run (and rerun) the cell(s) below until the bronze checks pass successfully.

# COMMAND ----------

# DBTITLE 1,Configure the datasource parser methods
from pyspark.sql import DataFrame

from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.parsers import plugins
from databricks.sirens.utils.base_utils import *

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("Description", spark)

    def toBronze(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """extract an event timestamp and augment raw data with the required metadata

        :param df: Incoming DataFrame
        :type df: DataFrame
        :param data_source_obj: the DataSource object
        :type data_source_obj: DataSource
        :return: dataframe with event_timestamp, metadata and a partition column (_event_date)
        :rtype: DataFrame
        """
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=adding row metadata")

        #####
        # insert custom logic here
        #####

        try:
            # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param dataSourceObj: the datasource object
        :type dataSourceObj: object
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        self.df = df
        self.dataSourceObj = data_source_obj
        logger.info(
            f"pipeline_run_id={self.dataSourceObj.pipeline_run_id} message=silver transformation starting")


        try:
            #########################
            # enter custom logic here
            #########################


            # remove if nested colmns should NOT be flattened
            self.df = BaseUtils.flatten_frame(df)

            logger.debug(
                f"pipeline_run_id={self.dataSourceObj.pipeline_run_id} message=silver transformation completed")
            return self.df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc


# COMMAND ----------

# DBTITLE 1,Execute toBronze() - to add metadata
log_parser = Parse(spark)
bronze_df = log_parser.toBronze(df, data_source_obj)
display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Verify metadata has been correctly extracted
bronze_issues = bronze_parsing_issues(bronze_df)
if bronze_issues:
    print(
        "WARNING: The following errors are occuring during the inital raw metadata parsing."
    )
    [print(f"\t{x}") for x in bronze_issues]
else:
    print("INFO: Raw metadata parsing appears to be OK...")

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 4. Flatten structs and extract features
# MAGIC
# MAGIC The objective in this stage is to ensure columns are accessible using a single column name or dot notation.
# MAGIC
# MAGIC Now its time to flatten complex data structures and extract columns into a silver table.
# MAGIC
# MAGIC **Required Steps**
# MAGIC 1. Amend ``toSilver()`` method in the datasource parser
# MAGIC 2. Run & re-run the silver transformation as required
# MAGIC
# MAGIC > #### *1. Amend ``toSilver()`` method*
# MAGIC > ###### Some Example transformations
# MAGIC > `self.df = self.df.select(self.df.colRegex("`_.*`"), "dvc_hostname", *cols)`
# MAGIC > `self.df = self.df.select(self.df.colRegex("`_.*`"), "raw.*").drop("raw")`
# MAGIC > `self.df = (self.df.select(self.df.colRegex("`_.*`"), regexp_extract(col("_source_file"), r'workspaceId=(\d+)', 1).alias('workspace')`
# MAGIC
# MAGIC > #### *2. Run and re-run
# MAGIC > The following cell executes transformations defined. Re-run until all columns are accessible by name or dot notation
# MAGIC > ###### Example dot notation column access
# MAGIC > `responseElements.ConsoleLogin`
# MAGIC >
# MAGIC > ###### Example access by name
# MAGIC > `Parent_Process_Id`

# COMMAND ----------

# DBTITLE 1,Execute silver transformations
silver_df = log_parser.toSilver(bronze_df, data_source_obj)
display(silver_df)

# COMMAND ----------

# DBTITLE 1,Verify silver level parsing looks OK
################################################################################
# The bronze_flattening_issues method validates the bronze table integrity and #
# prints out issues that it detects.                                           #
#                                                                              #
# Developer Input Required: No                                                 #
################################################################################

flattening_issues = silver_flattening_issues(silver_df)
if flattening_issues:
    print("WARNING: There appears to be little column flattening happening")
    [print(f"\t{x}") for x in flattening_issues]

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 5. Silver Transformation for CIM tables
# MAGIC ## Define Event Filters, Destination CIM tables and Field Transformations
# MAGIC
# MAGIC The final stage is to create *event based* common information model tables.
# MAGIC
# MAGIC > ### Common Information Model (CIM) tables
# MAGIC > Sirens defines *event based* tables for the following event types;
# MAGIC >
# MAGIC > authentication, dns, dhcp, file, network, powershell, process, registry, scheduled_tasks, service, user_management, web, wmi
# MAGIC
# MAGIC **Required Steps**
# MAGIC 1. Identify what events exist in the silver table that will map to one or more CIM tables.
# MAGIC 2. Create an Excel/Gsheets doc with the CIM table fields in it (personal preference).
# MAGIC 3. Identify fields in the silver table that map directly, or will be transformed to map onto each CIM field and place them in the doc.
# MAGIC 4. Configure the `transforms->silver` key in the `configItems` from the mapping document.
# MAGIC 5. Run & re-run until correct.
# MAGIC
# MAGIC > ##### Important
# MAGIC > - Some fields have allowed values only and need to be mapped based on your data.
# MAGIC >     - example. *event_result* (allowed values "**Success**" or "**Failure**"). Be sure to follow the CIM guide for each event based table
# MAGIC >
# MAGIC > - Not all fields in the CIM table will be mappable by every datasource. Best practice is to set those columns to `None` or `null`
# MAGIC
# MAGIC > ####1. Identify events in the silver table
# MAGIC > This can be done in a few ways
# MAGIC > - For simple feeds there may be only a single event type.
# MAGIC > - Use the admin guide for the data feed being configured.
# MAGIC > - execute pyspark commands to see what events the data has.
# MAGIC > ###### Example pyspark command
# MAGIC > The following command uses the service column to decide which events are within a specific dataset.
# MAGIC > `df.select('service').distinct().show()`
# MAGIC >
# MAGIC > To filter events into each table define a SQL expression in the `filter:` key as shown in stage 4.
# MAGIC
# MAGIC > ####2 & 3. Create a mapping document
# MAGIC > ###### Example authentication cim table mapping (snipped)
# MAGIC > |cim field|data type|silver field|
# MAGIC > |-|-|-|
# MAGIC > |dest|string|hostname|
# MAGIC > |dest_ip_addr|string|destIp|
# MAGIC > |event_result|string|action|
# MAGIC
# MAGIC > ####4. Configure transformations
# MAGIC > The example shows how to map source and destination fields.
# MAGIC > You can use the following actions
# MAGIC > Each field has 3 possible keys (action, type, value) where;
# MAGIC > - action: <add | alias | rename>
# MAGIC > - type: <literal | expression> (used when action = add)
# MAGIC > - value: <existing column | literal string | SQL expression>
# MAGIC > ###### Example authentication mapping (snipped)
# MAGIC > ```
# MAGIC > transforms
# MAGIC >   silver:
# MAGIC >     event_type:
# MAGIC >       - target_table: authentication
# MAGIC >         name: login activity
# MAGIC >         filter: eventName == "ConsoleLogin" or "additionalEventData.MFAUsed" == "Yes"
# MAGIC >         fields:
# MAGIC >          - dest:
# MAGIC >             action: alias
# MAGIC >             value: hostname
# MAGIC >          - dest_ip_addr:
# MAGIC >             action: alias
# MAGIC >             value: destIp
# MAGIC >          - event_result:
# MAGIC >             action: add
# MAGIC >             type: expression
# MAGIC >             value: case when action == "ok" then "Success" else "Failure" end
# MAGIC >          - event_product:
# MAGIC >             action: add
# MAGIC >             type: literal
# MAGIC >             value: Okta_IAM
# MAGIC > ```
# MAGIC
# MAGIC You can define one or more **'target_table'** keys to filter and transform many tables.

# COMMAND ----------

# DBTITLE 1,Process each event_type/filter defined in configItems variable
normalizer = normalize.Normalizer(spark, data_source_obj)
try:
    table_tranformations = config.get("transforms").get("silver").get("event_type")

    for et in table_tranformations:
        target_table = et.get("target_table")

        # filter events as defined in event_type filter
        event_filtered_df = normalizer.filter_frame(df=silver_df, target_table=target_table)

        # transform events as defined in event_type transforms
        cim_df = normalizer.transform_frame(df=event_filtered_df, target_table=target_table)
        display(cim_df)
except AttributeError as exc:
    print("No silver transformations found")
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage 5. Operationalize
# MAGIC
# MAGIC **In addition to DataFrames that represent the data how you want them, you should now also have the following artifacts;**
# MAGIC
# MAGIC - a working **'configItems'** dict
# MAGIC - two working methods in the Parse class (**toBronze()** and **toSilver()**)
# MAGIC - optionally a .json representation of the input StructType
# MAGIC
# MAGIC **To operationalize this data source we need to;**
# MAGIC 1) update the **'input->parser'** key, to \<source_sourcetype\> (which will become the name of the parser file for this datasource)
# MAGIC 2) Copy **'configItems'** dict into an **'inputs.yaml'** file
# MAGIC 3) Copy/replace the **toBronze()** and **toSilver()** functions into the template parser.py file (find a copy in the templates directory), and name it **\<source\>_\<sourcetype\>.py'**. Make sure to include any imports you needed to add in this notebook.
# MAGIC 4) [Optional] Create a **'schema.json'** file from the input Struct
# MAGIC 5) Under 'log_sources' create a directory **'\<source\>/\<sourcetype\>/'** and move **'inputs.yaml'**, and **'schema.json'** (if created) into it
# MAGIC 6) Move the datasource parser (created in step 3) into the **'parsers/'** directory (ensuring you don't overwrite anything existing)
# MAGIC 7) Enable the new datasource in **'sirens.config'**
# MAGIC
# MAGIC     a) edit the file and add the input
# MAGIC
# MAGIC   [input:\<source\>:\<sourcetype\>]
# MAGIC
# MAGIC   enabled = True
# MAGIC
# MAGIC 8) Code Gen the notebooks (using sirens.py)
# MAGIC 9) Deploy the notebooks as required
