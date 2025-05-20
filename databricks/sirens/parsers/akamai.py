import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import MapType, DoubleType, LongType, StructField, StringType, StructType

from databricks.sirens.datasource import DataSource, TimeStampInfo
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils import parser_utils

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("Akamai WAF Cribl logs parser", spark)

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
            # Extract Timestamp column
            df = df.select("*", F.from_json("value", "_raw struct<start:string>, host string").alias("jsn")) \
                .select("*", "jsn.host",
                        F.col("jsn._raw.start").cast("double").cast("timestamp").alias(self.timestamp_column_name)) \
                .drop("jsn")

            # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj=data_source_obj,
                                    timestamp_info=TimeStampInfo(timestamp_column=self.timestamp_column_name,
                                                                 timestamp_col_type="timestamp")) \
                .drop(self.timestamp_column_name, 'host')
            logger.debug(
                f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    @staticmethod
    def add_url_decode_fields(col_name, fields: list):
        return dict([(f, F.expr(f"url_decode({col_name}.{f})")) for f in fields])

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        logger.info(
            f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation starting")

        # Define mappings for the data normalization
        geo_map = {
            "lat": "double",
            "long": "double",
        }

        network_map = {
            "asnum": "long",
        }

        netperf_map = {
            "asnum": "long",
            "cacheStatus": "int",
            "downloadTime": "int",  # or long?
            "firstByte": "int",
            "lastByte": "int",
            "lastMileRTT": "int",
            "midMileLatency": "int",
            "midMileRTT": "int",
            "netOriginLatency": "int",
        }

        message_map = {
            "bytes": "long",
            "respLen": "long",
            "reqPort": "int",
            "status": "int",
            **self.add_url_decode_fields("message", ["UA", "redirURL", "reqCT", "reqPath", "reqQuery", "respCT"])
        }

        akamai_raw_schema = StructType([
            StructField('_raw', StructType([
                StructField('content', StringType(), True),
                StructField('cp', StringType(), True),
                StructField('format', StringType(), True),
                StructField('geo', StructType([
                    StructField('city', StringType(), True),
                    StructField('country', StringType(), True),
                    StructField('lat', StringType(), True),
                    StructField('long', StringType(), True),
                    StructField('region', StringType(), True)]), True),
                StructField('id', StringType(), True),
                StructField('message', StructType([
                    StructField('UA', StringType(), True),
                    StructField('bytes', StringType(), True),
                    StructField('cliIP', StringType(), True),
                    StructField('fwdHost', StringType(), True),
                    StructField('proto', StringType(), True),
                    StructField('protoVer', StringType(), True),
                    StructField('redirURL', StringType(), True),
                    StructField('reqCT', StringType(), True),
                    StructField('reqHost', StringType(), True),
                    StructField('reqLen', LongType(), True),
                    StructField('reqMethod', StringType(), True),
                    StructField('reqPath', StringType(), True),
                    StructField('reqPort', StringType(), True),
                    StructField('reqQuery', StringType(), True),
                    StructField('respCT', StringType(), True),
                    StructField('respLen', StringType(), True),
                    StructField('sslVer', StringType(), True),
                    StructField('status', StringType(), True)]), True),
                StructField('nested_values', MapType(StringType(), StringType(), True), True),
                StructField('netPerf', StructType([
                    StructField('asnum', StringType(), True),
                    StructField('cacheStatus', StringType(), True),
                    StructField('downloadTime', StringType(), True),
                    StructField('edgeIP', StringType(), True),
                    StructField('firstByte', StringType(), True),
                    StructField('lastByte', StringType(), True),
                    StructField('lastMileRTT', StringType(), True),
                    StructField('midMileLatency', StringType(), True),
                    StructField('midMileRTT', StringType(), True),
                    StructField('netOriginLatency', StringType(), True)]), True),
                StructField('network', StructType([
                    StructField('asnum', StringType(), True),
                    StructField('edgeIP', StringType(), True),
                    StructField('network', StringType(), True),
                    StructField('networkType', StringType(), True)]), True),
                StructField('reqHdr', MapType(StringType(), StringType(), True), True),
                StructField('respHdr', MapType(StringType(), StringType(), True), True),
                StructField('start', StringType(), True),
                StructField('type', StringType(), True),
                StructField('version', StringType(), True),
                StructField('waf', MapType(StringType(), StringType(), True), True)]), True),
            StructField('_time', DoubleType(), True),
            StructField('cribl_pipe', StringType(), True),
            StructField('host', StringType(), True),
            StructField('index', StringType(), True),
            StructField('source', StringType(), True),
            StructField('sourcetype', StringType(), True)])

        try:
            df = df.select("*", F.from_json("value", akamai_raw_schema).alias("jsn")).drop("value") \
                .select("*", "jsn.*").drop("jsn") \
                .select("*", "_raw.*").drop("_raw") \
                .withColumn("nested_values", parser_utils.url_decode_map("nested_values")) \
                .withColumn("waf", parser_utils.url_decode_map("waf")) \
                .withColumn("reqHdr", parser_utils.url_decode_map("reqHdr")) \
                .withColumn("respHdr", parser_utils.url_decode_map("respHdr")) \
                .withColumn("message", parser_utils.transform_struct("message", message_map)) \
                .withColumn("geo", parser_utils.transform_struct("geo", geo_map)) \
                .withColumn("network", parser_utils.transform_struct("network", network_map)) \
                .withColumn("netPerf", parser_utils.transform_struct("netPerf", netperf_map)) \
                .withColumn("cp", F.col("cp").cast("long")) \
                .drop("_time", "cribl_pipe", "index", "sourcetype")
            logger.debug(
                f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
