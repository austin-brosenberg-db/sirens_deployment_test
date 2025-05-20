from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.base_utils import BaseUtils

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("Zeek Log Parser", spark)

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
        try:  # TODO: use existing functions to rename...
            df = df.withColumn('ts', col('ts').cast(TimestampType()))
            new_cols = [column.replace('.', '_') for column in df.columns]
            df = df.toDF(*new_cols)
            # Add metadata to dataframe, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj=data_source_obj)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toBronze).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations started")

        # Flatten the record, and userIdentity structs into individual columns
        try:
            # Attempt a generic frame flattening operation. - This will work in many cases.
            df = BaseUtils.flatten_frame(df)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")

            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
