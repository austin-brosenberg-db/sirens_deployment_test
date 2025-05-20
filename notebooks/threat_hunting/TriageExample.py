# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

# DBTITLE 1,Import Libraries
# depending on where this notebook runs from, the relative path could be ../databricks, ../../databricks, ../../../databricks, or traverse higher.
import databricks
import os
databricks.__path__.append(os.path.abspath("../../databricks"))

import databricks.sirens.threathunting as TH
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC # List the Threat Hunt Library
# MAGIC Threat Hunts are defined in the conf/threathunting directory. This command lists the metadata about each defined hunt.
# MAGIC
# MAGIC Use the `OutputFormat` argument to return asJSON, asDataFrame or asObject

# COMMAND ----------

hunt_lib = TH.list_hunt_library(output_format=TH.OutputFormat.asDataFrame)
display(hunt_lib)

# COMMAND ----------

# MAGIC %md
# MAGIC # List information about a specific Threat Hunt from the Library
# MAGIC
# MAGIC Threat Hunts are defined in the conf/threathunting directory. This command lists the metadata about the specific hunt_name.
# MAGIC
# MAGIC Use the `OutputFormat` argument to return asJSON, asDataFrame or asObject

# COMMAND ----------

import json
hunt = TH.list_hunt_library_by_name(hunt_name="hnt_exec_T1047.000-100-WMIC-Usage", output_format=TH.OutputFormat.asJSON)
print(json.dumps(json.loads(hunt), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC # Search for executed threat hunts
# MAGIC
# MAGIC Search for and discover the status and state of scheduled/executed threat hunts.
# MAGIC
# MAGIC Specify one or more arguments to filter for a specific set of hunts.
# MAGIC
# MAGIC Use the `OutputFormat` argument to return asJSON, asDataFrame or asObject

# COMMAND ----------

import json
hunts = TH.list_executed_hunts(output_format=TH.OutputFormat.asDataFrame)
display(hunts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Hunt Workflow Status
# MAGIC
# MAGIC You can apply workflow status's to the hunt_index table.
# MAGIC
# MAGIC Use the workflow commands to edit and update the current status of the threat hunt run.
# MAGIC
# MAGIC Available commands:
# MAGIC   - `assign_to_me()`
# MAGIC   - `set_severity()`
# MAGIC   - `set_priority()`
# MAGIC   - `set_status()`
# MAGIC   - `set_assignee()`
# MAGIC   - `write_hunt()`

# COMMAND ----------

id = '<ENTER_ID>'

# COMMAND ----------


hunts = hunts.filter(hunts['run_id'] == id)

hunts = TH.assign_to_me(hunts, run_id=id)
hunts = TH.set_severity(df=hunts, severity=TH.WorkflowSeverity.INFORMATIONAL, run_id=id)
hunts = TH.set_priority(df=hunts, priority=TH.WorkflowPriority.HIGH, run_id=id)
hunts = TH.set_status(df=hunts, status=TH.WorkflowStatus.IN_PROGRESS, run_id=id)
hunts = TH.set_assignee(hunts, assignee='derek.king', run_id=id)

display(hunts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save the hunt index workflow update

# COMMAND ----------

TH.write_hunt_index(hunts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Display the results captured for a specific hunt
# MAGIC
# MAGIC Filter the results table to display the results from each captured analytic command.
# MAGIC
# MAGIC See `get_hunt_results()` to retrieve a list of reconstructed analytic DataFrame(s) to work with in the notebook.

# COMMAND ----------

for html_title, df in TH.show_hunt_results(run_id=id):
    displayHTML(html_title)
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve the results of a specific hunt to work with as DataFrames
# MAGIC
# MAGIC Use this command to return a list of DataFrames (one per captured analytic) that can be triaged. You can interact with these DataFrames as if running the analytics searches in real-time. 
# MAGIC
# MAGIC `get_hunt_results()` reconstructs the results captured within the threat hunt, and returns each analytic as a separate DataFrame
# MAGIC
# MAGIC **Some example workflows are:**
# MAGIC - add row numbers for subsequent interaction with them using `add_row_numbers()`
# MAGIC - mark lines as interesting using `mark()` for discovery or future actions
# MAGIC - annotate lines with hunter notes and observations using `annotate()`
# MAGIC - manually add a risk_score to individual lines using `add_risk_score()`

# COMMAND ----------

analytics_frames = TH.get_hunt_result(run_id=id)
if isinstance(analytics_frames, list):
    print(f"retrieved {len(analytics_frames)} DataFrames")
else:
    print("no results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triage analytic_1

# COMMAND ----------

analytic_1 = analytics_frames[0]
display(analytic_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add row numbers
# MAGIC Use the `add_row_numbers()` function to interact with entries in a DataFrame

# COMMAND ----------

analytic_1 = TH.add_row_numbers(analytic_1, time_col="`@timestamp`")
display(analytic_1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Toggle individual rows as interesting
# MAGIC Use the `mark()` function to identify rows of interest, so they are easily visible or to use as a filter later.
# MAGIC
# MAGIC rows argument can be specified as individual rows or ranges (examples: "1", "1,3", "2-5")

# COMMAND ----------

analytic_1 = TH.mark(df=analytic_1, rows="1")
display(analytic_1)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC # Filter for marked rows
# MAGIC
# MAGIC Display only rows marked as interesting.

# COMMAND ----------

analytic_1 = TH.filter_marked(analytic_1)
display(analytic_1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Annotate rows with hunter notes, observations or determinations
# MAGIC
# MAGIC Use the `annotate()` function to add an annotation map to a row. 
# MAGIC
# MAGIC Annotations are freeform key/value maps.

# COMMAND ----------

analytic_1 = TH.annotate(df=analytic_1, row_number=1, annotation={'determination': 'suspicious', 'analyst_notes': 'please follow up with a host scan'}, overwrite_existing=True)
display(analytic_1)

# COMMAND ----------

TH.write_hunt_results(analytic_1, "analytic_1")

# COMMAND ----------

# MAGIC %md
# MAGIC # Risk Framework
# MAGIC
# MAGIC Use the `add_risk_score()` and `update_risk_index_from_df()` functions to attribute results to a risk_object (typically a username column, or device_identifier column), either during a threat hunt or post hunt and during triage.
# MAGIC
# MAGIC How risk metrics work
# MAGIC ---------------------
# MAGIC Supply a confidence score between 1-100, and an impact score between 1-100. These metrics will be converted to an overall risk score when written to the `risk` table. 
# MAGIC
# MAGIC **Example:** a confidence of 20 and impact of 50, will produce a risk score of 10. (confidence * impact) / 100
# MAGIC
# MAGIC
# MAGIC
# MAGIC Sample workflow
# MAGIC ---------------
# MAGIC <b></br>
# MAGIC - use `add_row_numbers()` to a DataFrame that contains hunt results
# MAGIC - use `add_risk_score()` to assign a `confidence` and `impact` to a `risk_object`. Optionally provide annotations such as MITRE ATT&CK tactic and techniques
# MAGIC - To update the `risk` table use the `update_risk_index_from_df()` function. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a risk to Dataframe
# MAGIC
# MAGIC Add a risk column to the entity in 'SubjectUserName', setting the object type to user, and a appropriate annotation to group risks by
# MAGIC

# COMMAND ----------

analytic_1 = TH.add_risk_score(analytic_1, risk_object="SubjectUserName", object_type=TH.RiskObjectType.USER.value, impact=10, confidence=5, source='notebook', annotation={'tactic': 'credential_access'})
display(analytic_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update the risk table from the risk entities marked in the DataFrame
# MAGIC
# MAGIC This command will filter the DataFrame for risk rows and update the `risk` table, calculating the risk_score as it does so.

# COMMAND ----------

TH.write_risk_index_from_df(analytic_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View the Risk table
# MAGIC
# MAGIC Discover the risk table and entities with risks associatied. Track over time, and use to guide hunting & triage activities. 

# COMMAND ----------

risk_df = TH.list_risk_table()
display(risk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Send Data Externally
# MAGIC
# MAGIC Use the action framework to integrate with third party systems
# MAGIC

# COMMAND ----------

from databricks.sirens.actions.webhook import WebHookAPI
api_call = WebHookAPI(server="https://webhook.site/9e05c4e2-4a0a-4a5a-ba9d-de8c468b6ad5", action_name="webhook.site")
#api_call.post(analytic_1)
