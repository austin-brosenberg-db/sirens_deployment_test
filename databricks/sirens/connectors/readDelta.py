import logging

from pyspark.sql import DataFrame, SparkSession

from databricks.sirens.exceptions import SirensConnectorException
from . import plugins
from databricks.sirens.logging import get_logger
from ..datasource import DataSource

logger = get_logger(__name__)

@plugins.register
class Reader:
    """read from delta tables
    """
    def __init__(self, spark: SparkSession, data_source_obj: DataSource) -> None:
        self.data_source_obj = data_source_obj
        self.spark = spark
        self.databaseName = self.data_source_obj.databaseName

    @plugins.register
    def read(self, table_name=None, database=None) -> DataFrame:
        """read a delta table

        :param table_name: table to read
        :type table_name: str
        :return: pyspark DataFrame
        :rtype: DataFrame
        """
        self.table_name = table_name
        self.database = database
        self.stream_mode = self.data_source_obj.get_stream_mode()
        # If no table name passed, this is being used as a connector
        if not self.table_name:
            self.connector_opts = self.data_source_obj.get_connector_opts()
            logger.info(self.connector_opts)
            self.table_name = self.connector_opts.get("table", None)
            if not self.table_name:
                logger.error("readDelta called in connector mode without a table option.")
                raise SirensConnectorException("readDelta called in connector mode without a table option.")

            # if database specified - then use it
            self.databaseName = self.connector_opts.get("database", None)

        if 'batch' in self.stream_mode:
            try:
                logger.info(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=reading {self.databaseName}.{self.table_name}")
                return self.spark.read.table(f"{self.databaseName}.{self.table_name}")
            except Exception as exc:
                logger.error(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=can't read delta table: {exc}")
                raise SirensConnectorException(f"ERROR: {exc}") from exc

        if 'streaming' in self.stream_mode:
            try:
                logger.info(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=reading {self.databaseName}.{self.table_name}")
                return self.spark.readStream.table(f"{self.databaseName}.{self.table_name}")
            except Exception as exc:
                logger.error(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=can't read delta table: {exc}")
                raise SirensConnectorException(f"ERROR: {exc}") from exc

    def read_alerts(self, table, pipeline_run_id) -> DataFrame:
        """Return the alerts table

        :param table: table to read
        :type table: str
        :return: DataFrame
        :rtype: DataFrame
        """
        self.table = table
        self.stream_mode = "streaming"
        try:
            logger.info(f"pipeline_run_id={pipeline_run_id} message=reading alerts table: {self.table}")
            return self.spark.readStream.table(f"{self.databaseName}.{self.table}")
        except Exception as exc:
            logger.error(f"pipeline_run_id={pipeline_run_id} message={exc}")
            raise SirensConnectorException(f"ERROR: {exc}") from exc

    def read_aggregates(self, aggregate_config: dict, table_name=None, database=None) -> DataFrame:
        """read a delta table (specifically for creating aggregate sinks)

        :param table_name: table to read
        :type table_name: str
        :return: pyspark DataFrame
        :rtype: DataFrame
        """
        self.table_name = table_name
        if not self.table_name:
            self.table_name = aggregate_config.get("source_table")

        self.database = database
        if not self.database:
            self.database = self.connector_opts.get("database", None)

        self.stream_mode = aggregate_config.get(aggregate_config.stream_type, self.data_source_obj.get_stream_mode())

        if 'batch' in self.stream_mode:
            try:
                logger.info(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=reading {self.database}.{self.table_name}")
                return self.spark.read.table(f"{self.database}.{self.table_name}")
            except Exception as exc:
                logger.error(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=can't read delta table: {exc}")
                raise SirensConnectorException(f"ERROR: {exc}") from exc

        if 'streaming' in self.stream_mode:
            try:
                logger.info(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=reading {self.database}.{self.table_name}")
                return self.spark.readStream.table(f"{self.database}.{self.table_name}")
            except Exception as exc:
                logger.error(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=can't read delta table: {exc}")
                raise SirensConnectorException(f"ERROR: {exc}") from exc
