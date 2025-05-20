from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as F

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.parser_utils import remap_column_values
from databricks.sirens.utils.base_utils import *

logger = get_logger(__name__)


def status_to_activity(col_name: str, mapping: dict) -> Column:
    """
    Map status codes to activity codes
    :param col_name:
    :param mapping:
    :return:
    """
    cl = F.col(col_name)
    init = F.when(cl.isNull(), 0)
    for k, v in mapping.items():
        init = init.when(cl == k, F.lit(v))

    return init.otherwise(F.lit(99))


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark: SparkSession):
        super().__init__("azure_nsg_flow_logs", spark)

    def toBronze(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Extract event timestamp to _raw_time, and flatten the record, and userIdentity structs into individual columns

        :param df: Dataframe with raw data
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: DataFrame with event timestamp extracted to _raw_time, and flattened columns
        :rtype: DataFrame
        """
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=bronze transformations started")
        try:
            partial_schema = "records array<string>"
            jsn_col_name = "__jsn__"
            record_schema = "category string, time timestamp, operationName string, resourceId string"
            df = df.select("*", F.from_json("value", partial_schema).alias(jsn_col_name)) \
                .select("*", f"{jsn_col_name}.*").drop(jsn_col_name, "value") \
                .select("*", F.explode("records").alias("record")).drop("records") \
                .select("*", F.from_json("record", record_schema).alias(jsn_col_name)) \
                .select("*", f"{jsn_col_name}.*").drop(jsn_col_name)
            df = self._add_metadata(df, data_source_obj) \
                .drop("time", "resourceId") # cleanup a bit

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=bronze transformations completed")
            return df
        except Exception as exc:
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
        try:
            nsg_flow_log_record_schema = ("category STRING, macAddress STRING, operationName STRING, properties "
                                          "STRUCT<Version: short, flows: ARRAY<STRUCT<flows: ARRAY<STRUCT<flowTuples: "
                                          "ARRAY<STRING>, mac: STRING>>, rule: STRING>>>, resourceId STRING, "
                                          "systemId STRING, time timestamp")
            flow_tuple_schema = ("ts long,src_ip string, dest_ip string, src_port int,dest_port int, proto string, "
                                 "tf_flow string, tf_decision string, flow_state string, packets_sent int, "
                                 "bytes_sent long, packets_received int, bytes_received long")
            json_col = "__json__"

            names_mapping = {
                "macAddress": "mac_address",
                "operationName": "operation_name",
                "systemId": "system_id",
                "Version": "version",
            }

            tf_decision_mapping = {
                "A": "Allowed",
                "D": "Denied",
            }

            proto_mapping = {
                "T": "TCP",
                "U": "UDP",
            }

            tf_flow_mapping = {
                "I": "Inbound",
                "O": "Outbound",
            }

            # TODO: Do we need this?
            flow_state_mapping = {
                "B": "Begin",
                "C": "Continue",
                "E": "End"
            }

            df = df.select("*", F.from_json("record", nsg_flow_log_record_schema).alias(json_col)) \
                .drop("record", "operationName", "category") \
                .select("*", f"{json_col}.*").drop(json_col) \
                .select("*", "properties.*").drop("properties") \
                .select("*", F.explode("flows")).drop("flows") \
                .select("*", "col.*").drop("col") \
                .select("*", F.explode("flows").alias("flow")).drop("flows") \
                .select("*", "flow.*").drop("flow") \
                .select("*", F.explode("flowTuples").alias("flow_tuple")).drop("flowTuples") \
                .select("*", F.from_csv("flow_tuple", flow_tuple_schema).alias("flow_tuple_decoded")).drop("flow_tuple") \
                .select("*", "flow_tuple_decoded.*").drop("flow_tuple_decoded") \
                .withColumn("flow_ts", F.col("ts").cast("timestamp")).drop("ts") \
                .withColumn("rid", F.split("resourceId", "/")) \
                .withColumn("resource_id", F.struct(F.element_at("rid", 3).alias("subscription"),
                                                    F.element_at("rid", 5).alias("resource_group"),
                                                    F.element_at("rid", 7).alias("provider"),
                                                    F.element_at("rid", 9).alias("nsg_name"),
                                                    F.col("resourceId").alias("complete"))).drop("rid", "resourceId")

            df2 = df.withColumnsRenamed(names_mapping)
            df2 = remap_column_values(df2, "proto", proto_mapping)
            df2 = remap_column_values(df2, "tf_decision", tf_decision_mapping)
            df2 = remap_column_values(df2, "tf_flow", tf_flow_mapping)
            df2 = remap_column_values(df2, "flow_state", flow_state_mapping) \
                .withColumn("mac_address", F.coalesce(F.col("mac_address"), F.col("mac"))).drop("mac")

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")
            return df2
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
