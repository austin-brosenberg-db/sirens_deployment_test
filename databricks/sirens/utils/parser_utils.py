from typing import Dict, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from databricks.sirens.parsers.internal.common import normalize_dataframe


def url_decode_map(col_name: str):
    """URL Decode keys & values of the map column.

    :param col_name: name of the map column
    :type col_name: str
    :return:
    """
    return F.expr(
        f"transform_values(transform_keys({col_name}, (k, v) -> url_decode(k)), (k, v) -> url_decode(v))")


def transform_struct(col_name: str, m: Dict[str, Union[str, Column]]):
    """Transforms fields inside the given struct column or not.

    :param col_name: name of the struct column to transform
    :type col_name: str
    :param m: mapping of individual fields to types/columns
    :type m: dictionary of mapping
    :return: transformed column
    """
    cl = F.col(col_name)
    for k, v in m.items():
        if isinstance(v, str):
            cl = cl.withField(k, cl[k].cast(v))
        else:
            cl = cl.withField(k, v)

    return cl


def maybe_normalize_column_names(df: DataFrame, data_source_config: dict) -> DataFrame:
    """
    Normalize column names in the dataframe according to the configuration
    :param df: dataframe to normalize
    :param data_source_config:
    :return:
    """
    use_snake_case = data_source_config.get("transforms", {}).get("silver", {}) \
    .get("meta", {}).get("use_snake_case", False)
    aliases = data_source_config.get("transforms", {}).get("silver", {}) \
        .get("meta", {}).get("aliases", {})
    mapping = {}
    for cl, alias in aliases.items():
        mapping[cl] = {"alias": alias}

    return normalize_dataframe(df, mapping=mapping, snake_case=use_snake_case)


def remap_column_values(df: DataFrame, col_name: str, mapping: dict) -> DataFrame:
    """
    Map old to new values in a specific column

    :param df: original dataframe
    :type df: DataFrame
    :param col_name: column name
    :type col_name: str
    :param mapping: mapping of old to new values
    :type mapping: dict
    """
    cl = F.col(col_name)
    dest_cl = None
    for k, v in mapping.items():
        if dest_cl is None:
            dest_cl = F.when(cl == k, F.lit(v))
        else:
            dest_cl = dest_cl.when(cl == k, F.lit(v))

    dest_cl = dest_cl.otherwise(cl)
    return df.withColumn(col_name, dest_cl)
