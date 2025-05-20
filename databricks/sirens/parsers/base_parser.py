from abc import abstractmethod, ABC
from typing import Tuple, Union, Optional

import pyspark.sql.functions as F
from dateutil.parser import parse
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import TimestampType

from databricks.sirens.datasource import DataSource, TimeStampInfo
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class BaseParser(ABC):
    """
    Base class for parsers
    """
    timestamp_column_name = "_raw_time"

    def __init__(self, name: str, spark: SparkSession):
        self.spark = spark
        self.name = name

    def _timestamp_recognition(self, df: DataFrame, timestamp_column: Union[str, Column]) -> Tuple[DataFrame, str]:
        """extract the event timestamp

        :param df: raw dataframe
        :type df: DataFrame
        :param timestamp_column: the column that contains the timestamp
        :type timestamp_column: Union[str, Column]
        """
        try:
            if isinstance(timestamp_column, str):
                timestamp_column = F.col(timestamp_column)
            return (df.withColumn(self.timestamp_column_name, timestamp_column),
                    self.timestamp_column_name)
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc

    @staticmethod
    def _add_metadata(df: DataFrame, data_source_obj: DataSource, timestamp_info: Optional[TimeStampInfo] = None,
                      host_col: Optional[Column] = None) -> DataFrame:
        """
        Add metadata to the dataframe, including _event_date (the partition column) needed to write to delta.
        :param df: DataFrame to add metadata to
        :type df: DataFrame
        :param data_source_obj: the DataSource object
        :type data_source_obj: DataSource
        :param timestamp_info: optional timestamp information that will be used to override the default timestamp
        :type timestamp_info: Optional[TimeStampInfo]
        :param host_col: explicit Column object representing host information - if not provided, will be inferred
        :type host_col: Optional[Column]
        :return:
        """
        # Extract event time information
        if timestamp_info is None:
            timestamp_info = data_source_obj.get_timestamp_info()

        # TODO: get rid of the UDF
        @F.udf(returnType=TimestampType())
        def parse_time(s):
            return parse(s)

        if timestamp_info.timestamp_regex:
            timestamp_col = F.regexp_extract(timestamp_info.timestamp_column, timestamp_info.timestamp_regex,
                                             timestamp_info.timestamp_regex_group)
        elif isinstance(timestamp_info.timestamp_column, str):
            timestamp_col = F.col(timestamp_info.timestamp_column)
        else:
            timestamp_col = timestamp_info.timestamp_column
        # TODO: add more timestamp types, like, long_seconds, long_microseconds, double_microseconds, etc. This will
        #  help to avoid the need to do conversion in the parsers, i.e., `ts` in zeek parser.
        if timestamp_info.timestamp_col_type == "timestamp":
            timestamp_col = timestamp_col.cast("timestamp")
        elif timestamp_info.timestamp_col_type == "long_milliseconds":
            timestamp_col = (timestamp_col.cast("long")/1000.0).cast("timestamp")
        elif (timestamp_info.timestamp_col_type == "double_milliseconds" or
              timestamp_info.timestamp_col_type == "long_seconds"):
            timestamp_col = timestamp_col.cast("double").cast("timestamp")
        else:
            if timestamp_info.timestamp_format:
                # TODO: do we really need to_timestamp here?
                timestamp_col = F.to_timestamp(
                    F.unix_timestamp(timestamp_col, timestamp_info.timestamp_format).cast("timestamp"),
                    "dd-MM-yyyy HH:mm:ss.SSSZ")
            else:
                timestamp_col = parse_time(timestamp_col.cast("string"))

        # Extract host data
        host = data_source_obj.get_input_host()
        if host in df.columns:
            host_col = F.col(host)

        if host_col is None:
            raw_path_segment = data_source_obj.get_raw_path_segment()
            if raw_path_segment is not None:
                host_col = F.lit(raw_path_segment)

        if host_col is None:
            regex_path = data_source_obj.get_regex_path()
            if regex_path is not None:
                if regex_path[1] is None:
                    host_col = F.col(regex_path[0])
                else:
                    host_col = F.regexp_extract(regex_path[0], regex_path[1], regex_path[2])

        if host_col is None:
            host_col = F.lit(host or data_source_obj.get_default_workspace())

        # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
        source, source_type = data_source_obj.get_source_sourcetype_info()
        ndf = df.select(
            F.current_timestamp().alias("_ingest_time"),
            timestamp_col.alias("_event_time"),
            F.lit(source).alias("_source"),
            F.lit(source_type).alias("_sourcetype"),
            host_col.alias("dvc_hostname"),
            F.col("_metadata.file_path").alias("_source_file"),
            "*").withColumn("_event_date", F.to_date(F.col("_event_time")))

        # Check the columns have been created. - further checks done in check_event_timestamp function.
        required_columns = ['_event_time', '_event_date', '_ingest_time', '_source', '_sourcetype', 'dvc_hostname']
        if not all(x in ndf.columns for x in required_columns):
            logger.error(f"failed to add fields for {source}:{source_type} - please check.")
            raise Exception(f"failed to add fields for {source}:{source_type} - please check.")

        ndf = ndf.select(*required_columns, *[col for col in ndf.columns if col not in required_columns])

        return ndf

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
        try:
            # Add metadata to the dataframe, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj=data_source_obj)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    @abstractmethod
    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        pass
