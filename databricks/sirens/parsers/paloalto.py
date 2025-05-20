from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_csv, to_timestamp, lower, pandas_udf, lit
from pyspark.sql.types import StringType

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.parsers.internal.base import BaseCsvSource
from databricks.sirens.parsers.internal.paloalto_fields import get_paloalto_field_spark_type, \
    get_paloalto_field_type
from databricks.sirens.parsers.internal.paloalto_schemas import __paloalto_log_schemas__


class BasePaloAltoSyslogSource(BaseCsvSource):
    __partial_schema__ = "__col1__ string, log_time string, log_source_id long, log_type string"

    def __init__(self, log_type: str, timestamp_format: Optional[str] = None):
        super().__init__("paloalto_" + log_type)
        self.log_type = log_type
        scm = __paloalto_log_schemas__.get(log_type)
        if scm is None:
            raise RuntimeError(f"Log type '{log_type} isn't supported yet")
        self.timestamp_format = timestamp_format
        fields = [f.strip() for f in scm.strip().split(",")]
        fields_with_types = [f"`{f}` {get_paloalto_field_spark_type(f)}" for f in fields]
        self.schema = ", ".join(fields_with_types)
        self.partial_parsing_support = True

    @staticmethod
    def partial_parsing(df: DataFrame, data_col: str = "value", **kwargs) -> Optional[DataFrame]:
        ndf = df.select("*", from_csv(data_col, BasePaloAltoSyslogSource.__partial_schema__)
                        .alias(BasePaloAltoSyslogSource.__csv_col_name__)) \
            .select("*", f"{BasePaloAltoSyslogSource.__csv_col_name__}.*")
        if "timestamp_format" in kwargs:
            ndf = ndf.withColumn("log_time", to_timestamp(col("log_time"), kwargs["timestamp_format"]))
        else:
            ndf = ndf.withColumn("log_time", col("log_time").cast("timestamp"))

        ndf = ndf.select("*", col("log_time").cast("date").alias("log_date")) \
            .drop(BasePaloAltoSyslogSource.__csv_col_name__, "__col1__")
        return ndf

    def parse(self, df: DataFrame, data_col: str = "value", drop_data_col: bool = True) -> Optional[DataFrame]:
        tdf = df
        if "log_type" in df.columns:
            tdf = df.filter(f"log_type = '{self.log_type.upper()}'")
        tdf = super().parse(tdf.drop("log_type", "log_time", "log_source_id"))
        if tdf is None:
            return tdf
        tdf = tdf.drop("value")
        cols = []
        for cl in tdf.columns:
            if get_paloalto_field_type(cl) == 'pa_timestamp':
                if self.timestamp_format:
                    cols.append(to_timestamp(col(cl), self.timestamp_format).alias(cl))
                else:
                    cols.append(col(cl).cast("timestamp").alias(cl))
            else:
                cols.append(col(cl))
        ndf = tdf.select(*cols)
        if drop_data_col:
            ndf.drop(data_col)
        return ndf

    def normalize(self, df: DataFrame) -> DataFrame:
        return df


logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("palo alto parser", spark)

    def toBronze(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """extract an event timestamp and augment raw data with the required metadata

        :param df: Incoming DataFrame
        :type df: DataFrame
        :param data_source_obj: the datasource config object
        :type data_source_obj: DataSource
        :return: dataframe with event_timestamp, metadata and a partition column (_event_date)
        :rtype: DataFrame
        """

        source, source_type = data_source_obj.get_source_sourcetype_info()
        timestamp_info = data_source_obj.get_timestamp_info()
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=adding row metadata")

        partial_df = BasePaloAltoSyslogSource.partial_parsing(df, timestamp_format=timestamp_info.timestamp_format)
        partial_df = partial_df.filter(lower(col("log_type")) == source_type.lower())

        try:
            # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df=partial_df, data_source_obj=data_source_obj, host_col=lit("dvc_host"))
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
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
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation starting")

        # Define a function to check if an IP address is private or public
        @pandas_udf(returnType=StringType())
        def check_ip_address(ip_series):
            # Define a function to check if an IP address is private or public
            def is_private(ip):
                # Split the IP address into its components
                ip_parts = ip.split('.')
                first_octet = int(ip_parts[0])

                # Check if the IP address falls within any of the private address ranges
                if (first_octet == 10) or \
                    (first_octet == 172 and 16 <= int(ip_parts[1]) <= 31) or \
                    (first_octet == 192 and int(ip_parts[1]) == 168):
                    return 'Private'
                else:
                    return 'Public'

            # Apply the function element-wise to the input series
            return ip_series.apply(is_private)

        # Flatten the record, and userIdentity structs into individual columns
        try:
            _, sourcetype = data_source_obj.get_source_sourcetype_info()
            timestamp_info = data_source_obj.get_timestamp_info()
            pa_parser = BasePaloAltoSyslogSource(log_type=sourcetype, timestamp_format=timestamp_info.timestamp_format)
            df = pa_parser.parse(df)
            df = df.withColumn("dvc_hostname", col("log_source_name"))
            df = df.withColumn("source_ip_type", check_ip_address("source_ip"))
            df = df.withColumn("dest_ip_type", check_ip_address("dest_ip"))
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
