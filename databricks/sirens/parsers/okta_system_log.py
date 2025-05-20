from pyspark.sql import DataFrame

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.features.okta import OktaFeatures
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("OKTA Log Parser", spark)

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
        try:
            # expand struct
            df = OktaFeatures.add_all_features(df)
            logger.debug(
                f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")

            return df
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc
