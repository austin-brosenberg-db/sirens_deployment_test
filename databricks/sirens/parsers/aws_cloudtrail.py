from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, from_json, to_json

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.features.cloudtrail import CloudTrailFeatures
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.base_utils import *

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("aws cloudtrail parser", spark)

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
            # cloud trail arrives as a single array record, which needs to be expanded into one row per event.
            # We need to explode the Record ARRAY into rows.
            df = df.select(explode("Records").alias("record"))
            # Add metadata to frame, including _event_date (the partition column) needed to write to delta
            df = self._add_metadata(df, data_source_obj=data_source_obj)
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

        # Flatten the record, and userIdentity structs into individual columns
        try:
            df = df.select("*", "record.*", "record.userIdentity.*").withColumnRenamed("userName", "username")
            df = df.withColumns({
                'responseElements': from_json(to_json('responseElements'), "map<string, string>"),
                'requestParameters': from_json(to_json('requestParameters'), "map<string, string>")
            })
            df = CloudTrailFeatures.add_all_features(df)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
