# Databricks notebook source

#import databricks
#import os
#databricks.__path__.append(os.path.abspath("../../../databricks"))

# COMMAND ----------
"""
%md
# Notebook Examples

See the sirens user guide for more information on how to use actions and alerts, and how to configure them.
"""
# COMMAND ----------
"""
%md
# Example Sending Data to an external Webhook

Sirens can be used to send data to an external webhook, either ad-hoc (using a notebook) or as part of processing the alerts table.

It can be used in two ways:

1) By using the previously configured action (in conf/actions) and calling the action name.
2) By providing the server URL, headers and other arguments directly in the code.

You may also 'max and match' - meaning, if you call the action by name, Sirens will read the config, but if you also provide the server URL,
headers, etc., in the code, the code will take precedence.
"""
# COMMAND ----------
from databricks.sirens.actions.webhook import WebHookAPI
from pyspark.sql import SparkSession
# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("Alice", 34), ("Bob", 45)], ["name", "age"])
df.show()



# COMMAND ----------
"""
%md

In this example, we will send the data to a webhook.site endpoint, using the action_name parameter only. This will read the configuration below from the action.yaml file.

`
name: webhook.site
connection:
  server: webhook.site/54eba1de-e79a-4b07-8c8f-bc696b20430f
  auth:
    secret: "{{secrets/scope/key}}"
  headers:
    Content-Type: application/json
    user-agent: "MyApp/1.0"
  ratelimit:
    max_calls: 10
    period_seconds: 60
  max_timeout: 10
  retries: 3
  verify_ssl: False
`
"""
# COMMAND ----------
# Send the dataframe as a single event of multiple records
api = WebHookAPI(action_name='webhook.site')
api.post(data=df, row_per_event=False)

# COMMAND ----------
# Example - Overriding the server URL and headers from the already configured action
api = WebHookAPI(server="https://webhook.site/54eba1de-e79a-4b07-8c8f-bc696b20430f", headers = {'user-agent': 'databricks-sirens'}, action_name='webhook.site')
api.post(data=df, row_per_event=True)

# COMMAND ----------
# Example - Providing all details in the code directly - i.e no configured action.
api = WebHookAPI(server="https://webhook.site/54eba1de-e79a-4b07-8c8f-bc696b20430f", headers = {'user-agent': 'databricks-sirens'}, verify_ssl=False, auth_token='xxxx')
api.post(data=df, row_per_event=False)
