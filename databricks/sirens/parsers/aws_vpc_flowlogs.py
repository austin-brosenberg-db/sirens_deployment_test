from pyspark.sql import DataFrame

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
        super().__init__("aws vpc_flowlogs parser", spark)

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

        # Flatten the DataFrame, or extract columns as needed.
        try:
            # Attempt a generic frame flattening operation. - This will work in many cases.
            df = BaseUtils.flatten_frame(df)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
