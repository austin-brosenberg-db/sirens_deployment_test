from pyspark.sql.functions import explode, col
from pyspark.sql import DataFrame, SparkSession

from databricks.sirens.utils.base_utils import *
from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from  databricks.sirens.parsers.base_parser import BaseParser

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark: SparkSession):
        # -------- CHANGE ME TO RELEVANT DATASOURCE NAME ---------- #
        super().__init__("<Name of parser>", spark)

    def toBronze(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Extracts an event timestamp and augment raw data with the required metadata

        If this function only adds metadata, consider omitting it, so it will be using the BaseParser implementation.

        :param df: Incoming DataFrame
        :type df: DataFrame
        :param data_source_obj: the datasource config object
        :type data_source_obj: DataSource
        :return: dataframe with event_timestamp, metadata and a partition column (_event_date)
        :rtype: DataFrame
        """
        # ########################################################################################################
        # This is the most important function of the ingest process. Failure here likely results in dropped data.#
        # ########################################################################################################

        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=adding row metadata")

        #################################################################################################
        # * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * #
        #                                                                                               #
        # MAKE ANY MODIFICATIONS HERE TO ENSURE A SINGLE ROW PER EVENT                                  #
        # FOR EXAMPLE AS BELOW - CLOUDTRAIL Records BLOB MUST BE EXPANDED                               #
        # EITHER CHANGE THE BLOCK AS REQUIRED OR REMOVE IT IF YOUR DATASOURCE IS ALREADY ONE ROW/EVENT  #
        #                                                                                               #
        # * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * #
        #################################################################################################

        # ---------- CHANGE ME OR DELETE ME ------------------------------ #
        try:
            df = df.select(explode("Records").alias("record"))
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc

        # ----------------------------------------------------------------- #

        try:
            # Add metadata to dataframe, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj)
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

        ###################################################################################
        # * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * #
        #                                                                                 #
        # MAKE ANY MODIFICATIONS HERE TO ENSURE EITHER A FLATTENED DATAFRAME OR ONE       #
        # ACCESSIBLE USING DOT NOTATION                                                   #
        # EXAMPLE BELOW: EXPANDS STRUCTTYPE COLUMNS record and record.userIdentity        #
        #                                                                                 #
        # OTHER EXAMPLES INCLUDE EXTRACTING COLUMNS FROM SYSLOG RECORDS USING REGEXP      #
        # AN EXAMPLE OF THIS CAN BE FOUND IN THE APACHE ACCESS_COMBINED PARSER            #
        #                                                                                 #
        # * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * #
        ###################################################################################

        # Flatten the record, and userIdentity structs into individual columns
        try:
            # -------------- CHANGE ME OR DELETE ME ----------------------------- #
            df = df.select("*", "record.*", "record.userIdentity.*")
            # ------------------------------------------------------------------- #

            # Attempt a generic frame flattening operation. - This will work in many cases.
            df = BaseUtils.flatten_frame(df)
            logger.debug(
                f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")

            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
