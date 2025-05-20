##############################################################################################
#
# This parser is used to parse data recieved from winlogbeats endpoints, via a kafka topic.
#
##############################################################################################

from pyspark.sql import DataFrame
from pyspark.sql.functions import split, from_json

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.base_utils import *

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    def __init__(self, spark):
        super().__init__("winlogbeat_kafka_sysmon parser", spark)
        self.spark.conf.set("spark.sql.caseSensitive", "true")

    def toBronze(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """extract an event timestamp and augment raw data with the required metadata

        :param df: Incoming DataFrame
        :type df: DataFrame
        :param data_source_obj: the datasource config object
        :type data_source_obj: DataSource
        :return: dataframe with event_timestamp, metadata and a partition column (_event_date)
        :rtype: DataFrame
        """

        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=adding row metadata")

        schema_name = data_source_obj.get_schema_file_opt()
        if schema_name is None:  # TODO: should we provide a default schema?
            raise SirensParsingError("Schema file not found")

        schema = data_source_obj.read_schema_file(schema_name)

        df = df.select(from_json(df.value, schema).alias("json"))
        df = df.select("json.*")
        df = df.withColumnRenamed("@timestamp", "timestamp")
        df = df.withColumnRenamed("@metadata", "metadata")
        df = df.select("*", "host.*")

        # workarounds to resolve the parquet duplicates columns issue.
        df = df.withColumn("winlog", df['winlog'].dropFields(
            'user_data', 'event_data.ProcessId', 'event_data.LogonId', 'event_data.Id',
            'event_data.errorCode', 'event_data.`Default SD String:`'))
        try:
            # Add metadata to dataframe, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj=data_source_obj)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """

        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations started")
        # Flatten the DataFrame, or extract columns as needed.
        try:
            # -------------- CHANGE ME OR DELETE ME ----------------------------- #
            df = df.select("*", split(df.message, "\n").alias("message_split")).drop("message").withColumnRenamed(
                "message_split", "message")
            # ------------------------------------------------------------------- #

            # Attempt a generic frame flattening operation. - This will work in many cases.
            df = BaseUtils.flatten_frame(df)
            # return only sysmon records
            df = df.filter(col("winlog_channel") == "Microsoft-Windows-Sysmon/Operational")
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")

            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
