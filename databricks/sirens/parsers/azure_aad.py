from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, split, when, trim, col, expr, lit

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
        super().__init__("AzureAD Parser", spark)

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
        logger.info(f"AzureAD source type {source_type}")
        
        try:
            if source_type == "aad_riskyusers":
                
                df = (df
                      .withColumn("splitKV", split(col("UserDisplayName"), ", "))
                      .withColumn("LastName", when(col("UserDisplayName").contains(","),
                                                   trim(col("splitKV").getItem(0)))
                                  .otherwise(lit(None).cast("string")))
                      .withColumn("FirstName", when(col("UserDisplayName").contains(","),
                                                  trim(split(col("splitKV").getItem(1), " ").getItem(0)))
                                  .otherwise(lit(None).cast("string")))
                      .withColumn("Location", when(col("UserDisplayName").contains("("), 
                                                 trim(expr("substring_index(substring_index(UserDisplayName, '(', -1), ')', 1)")))
                              .otherwise(lit(None).cast("string")))
                      .drop('splitKV')
                      )
            
            if source_type == "aad_userriskevents":
                
                ddl = "ARRAY<STRUCT<Key: STRING, Value: STRING>>"
                
                df = (BaseUtils.flatten_frame(df)
                      .withColumn("additional_info_kv", from_json(col("additionalInfo"), ddl))
                      .drop("additionalInfo"))

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
