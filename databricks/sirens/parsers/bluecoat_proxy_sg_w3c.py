from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws

from databricks.sirens.datasource import DataSource, TimeStampInfo
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
        super().__init__("Symantec Blue coat ProxySG (W3C ELFF)", spark)

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
            # Add metadata to dataframe, including _event_date (the partition column) needed to write to delta.
            timestamp_info = TimeStampInfo(timestamp_column=concat_ws(' ', col("_c0"), col("_c1")),
                                           timestamp_format="yyyy-MM-dd HH:mm:ss")
            df = self._add_metadata(df, data_source_obj=data_source_obj,
                                    timestamp_info=timestamp_info)
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
            # -------------- CHANGE ME OR DELETE ME ----------------------------- #
            df = df.selectExpr("_event_date", "_event_time", "_source", "_sourcetype", "dvc_hostname", "_ingest_time",
                               "_source_file", "_c0 as date", "_c1 as time", "_c2 as time_taken",
                               "_c3 as c_ip", "_c4 as cs_username", "_c5 as cs_auth_group", "_c6 as x_exception_id",
                               "_c7 as sc_filter_result", "_c8 as cs_categories", "_c9 as cs_Referer",
                               "_c10 as sc_status", "_c11 as s_action", "_c12 as cs_method", "_c13 as rs_Content_Type",
                               "_c14 as cs_uri_scheme", "_c15 as cs_host", "INT(_c16) as cs_uri_port",
                               "_c17 as cs_uri_path", "_c18 as cs_uri_query", "_c19 as cs_uri_extension",
                               "_c20 as cs_User_Agent", "_c21 as s_ip", "INT(_c22) as sc_bytes",
                               "INT(_c23) as cs_bytes", "_c24 as x_virus_id", "_c25 as x_bluecoat_application_name",
                               "_c26 as x_bluecoat_application_operation")
            # ------------------------------------------------------------------- #

            # Attempt a generic frame flattening operation. - This will work in many cases.
            df = BaseUtils.flatten_frame(df)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")

            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
