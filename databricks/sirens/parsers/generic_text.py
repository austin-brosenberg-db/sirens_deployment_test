from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser

logger = get_logger(__name__)

_default_timestamp_column_type = "timestamp"


@plugins.register
class Parse(BaseParser):
    """A parser for generic JSON logs. Responsible for event timestamp and other metadata extraction.
    """

    def __init__(self, spark: SparkSession):
        super().__init__("Generic regex-based text logs parser", spark)

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """

        config = data_source_obj.data_source_config
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations started")
        col_name = config.get("transforms", {}).get("silver", {}) \
            .get("meta", {}).get("source_column_name")
        if not col_name:
            raise SirensParsingError("Column name not provided in the configuration")
        regex = config.get("transforms", {}).get("silver", {}) \
            .get("meta", {}).get("source_column_regex")
        if not regex:
            raise SirensParsingError("Regex not provided in the configuration")
        regex = regex.strip()
        mappings = config.get("transforms", {}).get("silver", {}) \
            .get("meta", {}).get("mappings")
        if not mappings:
            raise SirensParsingError("Mappings not provided in the configuration")
        remove_original_column = config.get("transforms", {}).get("silver", {}) \
            .get("meta", {}).get("remove_original_column", True)

        post_transforms = config.get("transforms", {}).get("silver", {}) \
            .get("meta", {}).get("post_transforms", [])
        input_filter = config.get("transforms", {}).get("silver", {}) \
            .get("meta", {}).get("input_filter")

        try:
            if input_filter:
                df = df.filter(input_filter)
            # extract main data in one go
            cols = {}
            for v in mappings:
                name = v.get("name")
                group_index = v.get("index")
                if not name or group_index is None:
                    raise SirensParsingError("Name or index is not provided in the mappings")
                cl = F.regexp_extract(col_name, regex, group_index)
                val_type = v.get("type")
                if val_type is not None:
                    cl = cl.cast(val_type)
                cols[name] = cl

            df = df.withColumns(cols)

            # perform post transformations
            for t in post_transforms:
                action = t.get("action")
                if action is None:
                    raise SirensParsingError("Type is not provided in the post_transforms")
                if action == "add":
                    expr = t.get("expression")
                    name = t.get("name")
                    if not expr or not name:
                        raise SirensParsingError("Expression or name is not provided in the post_transforms")
                    cl = F.expr(expr.strip())
                    col_type = t.get("type")
                    if col_type:
                        cl = cl.cast(col_type)
                    df = df.withColumn(name, cl)
                elif action == "drop":
                    cols = t.get("columns")
                    if cols is None:
                        raise SirensParsingError("Columns are not provided in the post_transforms for drop action")
                    df = df.drop(*cols)
                else:
                    raise SirensParsingError(f"Unsupported post transformation action: '{action}'")

            # remove original column if needed
            if remove_original_column:
                df = df.drop(col_name)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc
