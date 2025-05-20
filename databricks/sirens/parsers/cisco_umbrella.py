from pyspark.sql import DataFrame
from pyspark.sql.functions import from_csv, col

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
        super().__init__("Cisco Umbrella Parser", spark)

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
        source, source_type = data_source_obj.get_source_sourcetype_info()
        logger.info(f"Cisco Umbrella source type {source_type}")
        
        try:
            if source_type == "umbrella_dns":
                ddl = """STRUCT<
                raw_timestamp: STRING,
                most_granular_identity: STRING,
                identities: STRING,
                internal_ip: STRING,
                external_ip: STRING,
                action: STRING,
                query_type: STRING,
                response_code: STRING,
                domain: STRING,
                categories: STRING,
                most_granular_identity_type: STRING,
                identity_type: STRING,
                blocked_categories: STRING
                >"""
            
            elif source_type == "umbrella_admin_audit":
                ddl = """STRUCT<
                id: STRING,
                timestamp: STRING,
                email: STRING,
                user: STRING,
                type: STRING,
                action: STRING,
                logged_in_from: STRING,
                before: STRING,
                after: STRING
                >"""
            
            else:
                raise Exception("unsupported type!")
            
            df = (df
                  .withColumn('csv', from_csv(col('value'), ddl,
                                              {'sep': ',', 'unescapedQuoteHandling': 'BACK_TO_DELIMITER'}))
                  .select('*', 'csv.*')
                  .drop('value', 'csv', '_raw_time')
                  )

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
