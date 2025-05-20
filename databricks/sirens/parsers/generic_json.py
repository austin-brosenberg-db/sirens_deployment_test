from pyspark.sql import DataFrame, SparkSession

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.parser_utils import maybe_normalize_column_names

logger = get_logger(__name__)

_default_timestamp_column_type = "timestamp"


@plugins.register
class Parse(BaseParser):
    """A parser for generic JSON logs. Responsible for event timestamp and other metadata extraction.
    """

    def __init__(self, spark: SparkSession):
        super().__init__("Generic JSON logs parser", spark)

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
        df = maybe_normalize_column_names(df, data_source_obj.data_source_config)
        logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
        return df
