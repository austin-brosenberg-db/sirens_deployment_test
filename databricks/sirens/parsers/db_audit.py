from pyspark.sql import DataFrame
from pyspark.sql.functions import from_utc_timestamp, from_unixtime, regexp_extract

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
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
        super().__init__("Databricks Audit Logs", spark)

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
            df = df.withColumns({
                "timestamp": from_utc_timestamp(from_unixtime(col("timestamp") / 1000), "UTC"),
                "email": col("userIdentity.email"),
            }).drop("_rescued_data", "userIdentity")
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
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation starting")
        # Flatten the record, and userIdentity structs into individual columns
        try:
            # -------------- CHANGE ME OR DELETE ME ----------------------------- #
            df = (df.select("*",
                            regexp_extract(col("_source_file"), r'workspaceId=(\d+)', 1).alias('workspace'),
                            col("requestParams").getItem("instanceId"), col("requestParams").getItem("targetUserName"),
                            col("requestParams").getItem("endpoint"), col("requestParams").getItem("targetUserId"),
                            col("requestParams").getItem("shardName"), col("requestParams").getItem("targetGroupId"),
                            col("requestParams").getItem("targetGroupName"), col("requestParams").getItem("duration"),
                            col("requestParams").getItem("approver"), col("requestParams").getItem("reason"),
                            col("requestParams").getItem("authType"),
                            col("requestParams").getItem("user_name"), col("requestParams").getItem("group_name"),
                            col("requestParams").getItem("user"), col("requestParams").getItem("account"),
                            col("requestParams").getItem("userName")))
            # ------------------------------------------------------------------- #

            # Attempt a generic frame flattening operation. - This will work in many cases.
            df = BaseUtils.flatten_frame(df)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")

            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
