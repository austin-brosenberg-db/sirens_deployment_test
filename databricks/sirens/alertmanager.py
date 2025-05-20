"""Manages processing of alerts from alert tables defined in conf/alerts/alerts.yaml and sends
to the configured notification module.

Reads the table passed, attempts to send the configured notification and updates the notification_history
table with the result. Alerts can be traced back with a join on the alerts table using uuid column.

Currently we try the notification only once, and capture the action result.

# TODO
    - support for alert suppression
    - support for retries of failed notifications

Returns None

Authors:
    Derek King 1st December 2022

Classes:
    AlertHandler(database: str, tables: list, since: str)

Functions:
    process_alerts(AlertHandlerObj)
    get_alerts()

Example:
    alertHandlerObj = AlertHandler(database='sirens', tables=['alerts'], since='INTERVAL 1 HOUR')
    alertHandlerObj.process_alerts(alertHandlerObj)
"""

import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType

from databricks.sirens import action
from databricks.sirens import config_reader
from databricks.sirens import connectors
from databricks.sirens.exceptions import SirensAlertManagerException
from databricks.sirens.internal.baseaction import ActionResult
from databricks.sirens.logging import get_logger
from databricks.sirens.utils.base_utils import BaseUtils

logger = get_logger(__name__)


class AlertHandler:
    """Handler class for alerts

    :raises SirensAlertManagerException: _description_
    :return: Updates notification_history table
    :rtype: None
    """

    def __init__(self, database: str, tables: list, since: str = None, time_column: str = "alertedTime"):
        self.spark = SparkSession.getActiveSession()
        self.databaseName = database
        self.tables = tables
        self.time_modifier = since
        self.time_column = time_column
        self.alertsConf = config_reader.Alerts().read()
        self.pipeline_run_id = BaseUtils.get_uuid_str()
        self.globalConfig = config_reader.GlobalConfig.read()
        self.scratch_dir = self.globalConfig['default']['scratch_dir']
        if not isinstance(tables, list):
            raise SirensAlertManagerException("tables expected to be a list")
        if self.time_modifier:
            if not BaseUtils.is_time_interval_valid(self.time_modifier):
                logger.error(f"{self.time_modifier}: Invalid time interval. Specify 'INTERVAL  [int] [MODIFIER]'")
                raise SirensAlertManagerException(
                    f"{self.time_modifier}: Invalid time interval. Specify 'INTERVAL  [int] [MODIFIER]'")
        if not self.alertsConf:
            logger.error("Failure to read the alerts.yaml file")
            raise SirensAlertManagerException("Failure to read the alerts.yaml file")

        # Get tables from conf that matches the user request.
        self.table_configs = self._get_tables_from_conf()
        logger.debug(f'table config: {self.table_configs}')
        if not self.table_configs:
            logger.error(
                f"pipeline_run_id={self.pipeline_run_id} message = tables {self.tables} not defined in alerts.yaml - please correct")
            raise SirensAlertManagerException(
                f"Failure to match tables: {self.tables} to any definitions in alerts.yaml - please correct"
            )

    def _get_tables_from_conf(self) -> list:
        """Create a list of tables that match the self.tables - from alertsObj

        :return: list of tables defined in the alerts.yaml file
        :rtype: list
        """
        table_defs = []
        logger.debug(f"alerts_conf is: {self.alertsConf}")
        for alert_table_def in self.alertsConf:
            k, v = list(alert_table_def.items())[0]
            [table_defs.append({k: v}) for table in self.tables if table in k]

        return table_defs

    def _read_alert_table(self, table: str) -> DataFrame:
        """read the delta table provided

        :param table: alert table to read
        :type table: str
        :raises SirensAlertManagerException: on any exception
        :return: table as dataframe
        :rtype: DataFrame
        """
        try:
            delta_reader = connectors.Reader("readDelta", self.spark, self)
            df = delta_reader.read_alerts(table, self.pipeline_run_id)
            # Filter for time_mofifier
            if self.time_modifier:
                timeframe_interval = col(self.time_column).cast("timestamp") >= (
                        current_timestamp() - expr(self.time_modifier))
                df = BaseUtils.filter_dataframe(df, timeframe_interval)
        except Exception as exc:
            logger.error(
                f"pipeline_run_id={self.pipeline_run_id} message={exc}"
            )
            raise SirensAlertManagerException(f"{exc}") from exc

        return df

    def _micro_batch(self, microBatch, batchId):
        """process micro batch from readStream

        :param microBatch: batch passed by spark
        :type microBatch: dataframe
        :param batchId: unique batchId
        :type batchId: pass
        """
        microBatch.persist()

        # Call the notification
        resultsDF = Notification(handlerObj=self, module=self.module, action=self.request,
                                 params=self.options, timeout=self.timeout).send(microBatch)

        # Join alertDF with resultsDF
        if resultsDF:
            microBatch = microBatch.join(resultsDF, microBatch.uuid == resultsDF.action.uuid, "inner").drop(
                resultsDF.action.uuid)

        # Write the notification results to the notification_history table.
        microBatch.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(
            f"{self.databaseName}.notification_history")

        # write out the alert along with notification update to dest alert table if specified in config.
        # dest_alert_table = self.alertsConf.get("destination_table")
        if self.destination_table:
            microBatch.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(
                f"{self.databaseName}.{self.destination_table}")

        microBatch.unpersist()

    def _process_events(self, alert_table: str, df: DataFrame, condition: dict) -> None:
        """Filter the alerts table for conditions in alerts.yaml and call micro_batch to send them off to the action handler

        :param alert_table: alert table to read
        :type alert_table: str
        :param df: dataframe to process
        :type df: DataFrame
        :param condition: sql expression from alerts.yaml
        :type condition: dict
        """

        self.module = condition.get("condition").get("action").get("module")
        self.request = condition.get("condition").get("action").get("request")
        self.options = condition.get("condition").get("action").get("options")
        self.filter = condition.get("condition").get("filter")
        self.timeout = condition.get("condition").get("timeout")
        self.checkpoint_location = self.scratch_dir + "/checkpoints/" + alert_table
        logger.debug(f"checkpoint location: {self.checkpoint_location}")

        # filter frame condition
        if self.filter:
            df = BaseUtils.filter_dataframe(df, self.filter)
            logger.debug(f"filtered df for condition: {self.filter}")

        # Send alerts
        status = (df.writeStream
                  .trigger(once=True)
                  .option("checkpointLocation", self.checkpoint_location)
                  .foreachBatch(self._micro_batch)
                  .start()
                  )

        return

    def process_alerts(self) -> None:
        """Process the alert table(s) and send notifications as defined in the alerts.yaml config

        :raises SirensAlertManagerException: for any Exceptions
        :return: Updated records in notification_history table for attempts to send notifications
        :rtype: None
        """

        # Step through each requested alerts table
        for table in self.table_configs:
            alert_table, v = list(table.items())[0]
            logger.debug(f"processing alert table: {alert_table}, config: {v}")

            # Get all conditions for this alert table
            conditions = v.get("conditions", None)
            if not conditions:
                logger.warning(f"No conditions found for alert table: {alert_table}")
                return None

            # Get the latest alerts from this table
            df = self._read_alert_table(alert_table)

            # process the condition set(s)
            for condition in conditions:
                self.destination_table = condition.get("condition").get("destination_table")
                self._process_events(alert_table, df, condition)

        return

    def get_alerts(self):
        for table in self.tables:
            return self._read_alert_table(table)


class Notification:
    """Class to call the actionHandler for passing data to external systems
    """

    def __init__(self, handlerObj: object, module: str, action: str = None, params: dict = {},
                 timeout: int = 30) -> None:
        self.spark = SparkSession.getActiveSession()
        self.module = module
        self.timeout = timeout
        self.params = params
        self.action = action
        self.handlerObj = handlerObj
        logger.info(f"module={self.module}, action={self.action}, params={self.params}, timeout={self.timeout}")
        if not isinstance(self.timeout, int):
            raise SirensAlertManagerException(f"Integer required for timeout parameter: {self.timeout}")
        if not isinstance(self.action, str):
            raise SirensAlertManagerException("No action 'request' parameter in alerts.yaml - please correct")
        # if not isinstance(self.params, dict):
        #    raise SirensAlertManagerException("Invalid dict params passed - please correct")

    def _update_notification_results_dataframe(self, df: DataFrame, new_data: list, schema: StructType) -> DataFrame:
        """ Append last notification result to notification dataframe

        :param df: incoming dataframe
        :type df: DataFrame
        :param new_data: data to append
        :type new_data: list
        :param schema: schema to use
        :type schema: StructType
        :return: new dataframe
        :rtype: DataFrame
        """
        if not isinstance(new_data, list):
            new_data = list(new_data)

        new_record_df = BaseUtils.create_dataframe(spark=self.spark, schema=schema, data=new_data)
        df = df.union(new_record_df)
        return df

    def send(self, df: DataFrame) -> Optional[DataFrame]:
        """Main Entry point for sending a notification to an external system.

        :param df: alert table incoming dataframe
        :type df: DataFrame
        :return: Nothing
        :rtype: None
        """

        # Get each row as a list of dicts
        entries = BaseUtils.frame_to_dict(df)
        logger.debug("Notification.send()")

        # Get notification_history schema
        schema = _get_dataframe_schema()
        notification_history_df = BaseUtils.create_dataframe(self.spark, schema=schema)

        # Call the action handler
        for event in entries:
            # get the alert table uuid to tie notification_history w/ alert.
            unique_key = event.get("uuid")
            if not unique_key:
                raise SirensAlertManagerException("uuid column does not exist in alert table.")

            start_time = datetime.datetime.now()

            # Hand off the to action handler
            logger.debug("calling action handler")
            result = self._call_action_handler(event)
            logger.debug("action handler finished")

            finish_time = datetime.datetime.now()
            elapsed_time = finish_time - start_time
            elapsed_seconds = elapsed_time.seconds

            # update the history dataframe
            data = []
            time_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

            data = [((time_now, "alert_manager", "notifications", result.status, result.data, self.module,
                      self.handlerObj.pipeline_run_id, elapsed_seconds, unique_key),)]

            notification_history_df = self._update_notification_results_dataframe(notification_history_df, data, schema)

        if BaseUtils.is_spark_dataframe(notification_history_df):
            return notification_history_df
        else:
            return None

    def _call_action_handler(self, body: dict) -> ActionResult:
        """send a notification

        :param body: data to send
        :type body: dict
        :return: status, action_result
        :rtype: ActionResult
        """
        logger.debug(f"creating handler to: {self.module}")
        notifyObj = action.Handler(module=self.module, timeout=self.timeout)

        # Execute the third party action request.
        logger.debug(f"executing action: {self.action} against module: {self.module}")
        result = notifyObj.execute(
            action_request=self.action, params=self.params, body=body
        )
        if notifyObj.timed_out:
            status = False
            action_result = 'module timed out due to condition timeout setting'
            return ActionResult(status, action_result)

        return ActionResult(result.status, result.data)


def _get_dataframe_schema() -> StructType:
    """Return the alerts table schema

    :return: Alerts struct
    :rtype: StructType
    """
    schema = StructType([
        StructField('action', StructType([
            StructField("attempt_time", StringType(), False),
            StructField("_source", StringType(), False),
            StructField("_sourcetype", StringType(), False),
            StructField("status", StringType(), False),
            StructField("action_result", StringType(), False),
            StructField("module", StringType(), False),
            StructField("pipeline_run_id", StringType(), False),
            StructField("elapsed_seconds", StringType(), False),
            StructField("uuid", StringType(), False)]))
    ])
    return schema
