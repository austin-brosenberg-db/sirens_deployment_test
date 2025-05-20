import logging
import os
from typing import Union

from pyspark.sql import DataFrameWriter, DataFrame, SparkSession

from databricks.sirens.connectors.common import Connector
from databricks.sirens.exceptions import SirensConnectorException
from databricks.sirens.sql import SqlQuery
from . import plugins
from databricks.sirens.logging import get_logger
from ..datasource import DataSource
from ..global_config import GlobalConfig

logger = get_logger(__name__)


@plugins.register
class Writer:
    """Class used to write pyspark DataFrames to Delta Tables.
    """

    def __init__(self, spark: SparkSession, data_source_obj: DataSource, db_layer: str=None) -> None:
        self.data_source_obj = data_source_obj
        self.spark = spark
        self.databaseName = self.data_source_obj.targetDatabase
        self.source, self.source_type = self.data_source_obj.get_source_sourcetype_info()
        if db_layer:
            self.db_checkpoint_location = data_source_obj.data_source_config.get("schemas").get(db_layer).get("checkpointLocation")
        else:
            self.db_checkpoint_location = None
            self.checkpoint_location = os.path.join(GlobalConfig.get_global_scratch_dir(GlobalConfig.get()), "checkpoints",
                                                self.source, self.source_type)

        # create and use the configured database
        try:
            SqlQuery.create_database(database_name=self.databaseName, spark=self.spark)
        except Exception as exc:
            raise SirensConnectorException(f"Database Error: {exc}") from exc

    @plugins.register
    def write(self, df: DataFrame, table_name: str, table_config=None, database: str = None, source: str = None, source_type: str = None) -> None:
        """write the DataFrame to Delta

        :return: pyspark DataFrameWriter Object
        :rtype: DataFrameWriter
        """
        if table_config is None:
            table_config = {}
        if not database:
            database = self.databaseName
        stream_mode = self.data_source_obj.get_stream_mode()
        job_schedule = self.data_source_obj.global_config[f"input:{self.data_source_obj.source}:{self.data_source_obj.sourcetype}"]['schedule']

        base_table_config = self.data_source_obj.get_table_config(table_name)

        full_table_config = base_table_config | table_config

        auto_optimize = str(full_table_config.get("optimize_write", ""))
        if auto_optimize in ["true", "false"]:
            logger.info(
                f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=Delta optimize write {auto_optimize}")
            self.spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", auto_optimize)

        auto_compact = str(full_table_config.get("auto_compact", ""))
        if auto_compact in ["true", "false"]:
            logger.info(
                f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=Delta auto compact {auto_compact}")
            self.spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", auto_compact)

        partition_cols: Union[list, str] = full_table_config.get("partition_cols", [])
        if isinstance(partition_cols, str):
            # global config just deals with strings
            partition_cols = partition_cols.split(",")
        if len(partition_cols) > 0:
            logging.info(
                f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=Partitioning by {','.join(partition_cols)}")

        if 'batch' == stream_mode:
            try:
                logger.info(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=writing to {database}.{table_name} delta table in batch")
                (df.write.format('delta')
                 .mode('append')
                 .option("mergeSchema", "true")
                 .saveAsTable(f"{database}.{table_name}", partitionBy=partition_cols)
                 )
            except Exception as exc:
                logger.error(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=error writing DataFrame: {exc}")
                raise SirensConnectorException(f"Error writing DataFrame: {exc}") from exc

        if 'streaming' == stream_mode:
            try:
                checkpoint_location = self.resolve_checkpoint_location(table_name=table_name, database=database, source=source, source_type=source_type)
                logger.info(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=writing to {database}.{table_name} \
                            delta table in streaming. checkpoint: {checkpoint_location}")

                if job_schedule == 'continuous':
                    processing_time = self.data_source_obj.global_config[
                        f"input:{self.data_source_obj.source}:{self.data_source_obj.sourcetype}"].get("processingtime", 1)

                    (df.writeStream
                        .format('delta')
                        .trigger(processingTime=f"{processing_time} second")
                        .option('checkpointLocation', f"{checkpoint_location}")
                        .option("mergeSchema", "true")
                        .toTable(f"{database}.{table_name}", partitionBy=partition_cols)
                    )

                else:
                    (df.writeStream
                        .format('delta')
                        .trigger(availableNow=True)
                        .option('checkpointLocation', f"{checkpoint_location}")
                        .option("mergeSchema", "true")
                        .toTable(f"{database}.{table_name}", partitionBy=partition_cols)
                    )
            except Exception as exc:
                logger.error(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=error writing DataFrame: {exc}")
                raise SirensConnectorException(f"Error writing DataFrame: {exc}") from exc

        # check the connector_status for the datasourceObj
        # Some connectors require we maintain state between runs.
        # Check here for those, and write to status file, now the data is safely stored.
        try:
            if self.data_source_obj.connector_status:
                if Connector(self.spark, self.data_source_obj).write(self.data_source_obj.connector_status):
                    logger.debug(
                        f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=connector status updated OK. {self.data_source_obj.connector_status}")
                    del self.data_source_obj.connector_status
                else:
                    logger.error(
                        f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=connector status update failed")
                    raise SirensConnectorException(
                        f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=failed to write status file. Check logs")
        except AttributeError:
            pass

        return

    def write_agg(self, df: DataFrame, aggregate_config: dict, table_name: str,
                  table_config=None, database: str = None, source: str = None, source_type: str = None) -> None:
        """write aggregated DataFrame to Delta

        :return: pyspark DataFrameWriter Object
        :rtype: DataFrameWriter
        """
        if table_config is None:
            table_config = {}
        if df.isStreaming:
            stream_mode = "streaming"
        else:
            stream_mode = "batch"

        if not table_name:
            table_name = aggregate_config.get("target_table")

        if not database:
            database = self.databaseName

        save_mode = aggregate_config.get("save_mode")
        if not save_mode:
            raise SirensConnectorException(
                f"required config (save_mode) not specified for aggregation table: {table_name}")

        base_table_config = self.data_source_obj.get_table_config(table_name)
        full_table_config = base_table_config | table_config

        partition_cols: Union[list, str] = full_table_config.get("partition_cols", [])
        if isinstance(partition_cols, str):
            # global config just deals with strings
            partition_cols = partition_cols.split(",")
        if len(partition_cols) > 0:
            logging.info(
                f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=Partitioning by {','.join(partition_cols)}")

        if 'batch' == stream_mode:
            try:
                logger.info(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=writing to {database}.{table_name} delta table in batch")
                (df.write
                 .format('delta')
                 .mode(save_mode)
                 .option("mergeSchema", "true")
                 .saveAsTable(f"{database}.{table_name}", partitionBy=partition_cols)
                 )
            except Exception as exc:
                logger.error(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=error writing DataFrame: {exc}")
                raise SirensConnectorException(f"Error writing DataFrame: {exc}") from exc

        if 'streaming' == stream_mode:
            try:
                checkpoint_location = self.resolve_checkpoint_location(table_name=table_name, database=database, source=source, source_type=source_type)
                logger.info(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=writing to {database}.{table_name} \
                            delta table in streaming. checkpoint: {checkpoint_location}")
                (df.writeStream
                 .format('delta')
                 .outputMode(save_mode)
                 .trigger(availableNow=True)
                 .option('checkpointLocation', f"{checkpoint_location}")
                 .option("mergeSchema", "true")
                 .toTable(f"{database}.{table_name}", partitionBy=partition_cols)
                 )
            except Exception as exc:
                logger.error(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=error writing DataFrame: {exc}")
                raise SirensConnectorException(f"Error writing DataFrame: {exc}") from exc

    def resolve_checkpoint_location(self, table_name: str, database: str=None, source: str=None, source_type: str=None) -> str:
        """acquire checkpoint location based upon the following logic:
        if db_checkpoint_location is defined for each database in schemas section of inputs.yaml, the checkpoint location
        will be <db_checkpoint_location>/<table_name>/_checkpoints/<optional: source>/<optional: source_type>.
        otherwise, the scratch dir defined in global sirens.config will be used as 
        <scratch_dir>/<checkpoints>/<source>/<source_type>/<database>/<table_name>.

        :return: checkpoint location
        :rtype: str
        """
        if self.db_checkpoint_location:
            checkpoint_path = os.path.join(self.db_checkpoint_location, table_name, "_checkpoints")

            if source and source_type:
                checkpoint_path = os.path.join(checkpoint_path, source, source_type)
        
        else:
            checkpoint_path = os.path.join(self.checkpoint_location, database, table_name)
        
        return checkpoint_path