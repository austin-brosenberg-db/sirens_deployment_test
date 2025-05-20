from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, split, slice, expr, size, coalesce

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.base_utils import *
from databricks.sirens.parsers.internal.zscaler_schemas import casb_schema, webproxy_raw_data_schema, privateaccess_schema

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("Zscaler Parser", spark)

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
        logger.info(f"Zscaler source type {source_type}")
        try:
            # expand struct
            # df = df.select("*", "raw.*").drop("raw")
            if source_type == 'audit_logs':
                # Flatten the record, and auditOldValue and auditNewValue structs into individual columns
                df = df.withColumns({
                    'auditOldValue': from_json('auditOldValue', "map<string,string>"),
                    'auditNewValue': from_json('auditNewValue', "map<string,string>")
                })
            elif source_type.lower() in ("casb", "casb_crd"):
                df = df.withColumn("record", from_json("_raw", casb_schema))\
                    .selectExpr("*", "record.event.*") \
                    .drop("_raw", "_time", "record")
            elif source_type.lower() == "private_access":
                df = df.withColumn("record", from_json("_raw", privateaccess_schema))\
                .selectExpr("*", "record.*") \
                .drop("_raw", "_time", "record")
            elif source_type == 'webproxy':
                df = df.drop("ClientIP")
                silver_df = df.withColumn("fields", split("_raw", "\t"))\
                    .withColumn("keys", expr("transform(fields, x -> split(x, '=')[0])"))\
                    .withColumn("values", expr("transform(fields, x -> IF(size(split(x, '=')) > 1, split(x, '=')[1], null))"))\
                    .withColumn("keys", slice(col("keys"), 2, size(col("keys")) - 1))\
                    .withColumn("values", slice(col("values"), 2, size(col("values")) - 1))\
                    .withColumn("dct", expr("map_from_entries(arrays_zip(keys, values))"))

                raw_fields = [f"dct.{fn}" for fn in webproxy_raw_data_schema]
                df = silver_df.selectExpr("*", *raw_fields)\
                  .withColumn("dvc_hostname", coalesce(col("devicehostname"), col("dvc_hostname")))\
                  .drop("_raw", "_time", "dct", "fields", "keys", "values")
            elif source_type == "user_status":
                df = df.withColumns({
                    "SAMLAttributes": from_json("SAMLAttributes", "struct<myname: array<string>, myemail: array<string>>"),
                    'PosturesHit': split(col("PosturesHit"), ","),
                    'PosturesMiss': split(col("PosturesMiss"), ",")
                    })
            
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc
