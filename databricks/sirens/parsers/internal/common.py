from typing import Optional, Union, Dict, Any
import re

import pandas as pd
import inflection

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

__column_name_normalization_regex__ = re.compile("[^a-zA-Z-0-9_]+")

# TODO: move to the internal package
__data_types_to_spark_types_mapping__ = {
    "ip": "string", # IPv4 or IPv6
    "domain": "string", # hostname/domain name
    "url": "string", # HTTP URL
    "uri": "string",
    "mac_address": "string",
    "http_user_agent": "string",
    # "": "string",
    # "": "string",
    # "": "string",
    # "": "string",
    # "": "string",
}

__spark_types__ = {
    "string": T.StringType(),
    "boolean": T.BooleanType(),
    "bool": T.BooleanType(),
    "long": T.LongType(),
    "int": T.IntegerType(),
    "integer": T.IntegerType(),
    "double": T.DoubleType(),
    "float": T.FloatType(),
    "timestamp": T.TimestampType(),
    "date": T.DateType(),
}


def get_spark_type(typ: Union[str, T.DataType]) -> T.DataType:
    if isinstance(typ, T.DataType):
        return typ

    t = __spark_types__.get(typ)
    if t is None:
        raise Exception(f"No mapping for type '{typ}'")
    return t


def get_spark_type_for_data_type(typ: str) -> str:
    return __data_types_to_spark_types_mapping__.get(typ, typ)


def normalize_name(nm: str, maybe_alias: Optional[str] = None, snake_case: bool = False) -> str:
    """Performs normalization of the name - remove not allowed characters, optionally convert to sname case.

    :param nm: current name
    :param maybe_alias: new name if it's defined
    :param snake_case: boolean flag specifying if the name should be transformed into snake case if it uses Camel case
    :return:
    """
    if maybe_alias is not None:
        return maybe_alias

    t = re.sub(__column_name_normalization_regex__, "_", nm)
    if snake_case:
        t = inflection.underscore(t)
    return t


def maybe_convert_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception as e:
        print(f"maybe_convert_int got exception for value ‘{v}': {e}")
    return None


def maybe_convert_int_ms_to_timestamp(v):
    i = maybe_convert_int(v)
    if i is None:
        return None
    try:
        return pd.to_datetime(int(v), unit='ms')
    except Exception as e:
        print(f"maybe_convert_int_ms_to_timestamp got exception for value ‘{v}': {e}")

    return None


def timestamp_col_from_long_string(col: Column) -> Column:
    return (col.cast("long") / 1000).cast("timestamp")


def timestamp_col_from_utc_long(col: Column) -> Column:
    return F.from_utc_timestamp(F.from_unixtime(col/1000), "UTC")


def timestamp_col_from_long_seconds(col: Column) -> Column:
    return col.cast("double").cast("timestamp")


def timestamp_col_from_long_milliseconds(col: Column) -> Column:
    return (col/1000.0).cast("timestamp")


def timestamp_col_from_double_string(col: Column) -> Column:
    return col.cast("double").cast("timestamp")


def parse_list_value(col_or_name: Union[str, Column], pattern: str = ",", value_type: Optional[str] = None,
                     remove_empty_values: bool = True):
    """
    Splits a given string column into a list, optionally casting its elements into a given type.

    :param col_or_name: Column name or Column object representing the source column
    :param pattern: string pattern to use for splitting the string column
    :param value_type: optional type to cast to
    :param remove_empty_values: boolean flag specifying if empty values should be removed as well
    :return: new column with a list of parsed values
    """
    if isinstance(col_or_name, str):
        col = F.col(col_or_name)
    else:
        col = col_or_name
    cl = F.split(col, pattern)
    if remove_empty_values:
        cl = F.filter(cl, lambda x: F.length(x) > 0)
    if value_type is not None:
        cl = F.transform(cl, lambda x: x.cast(str(value_type)))
    if isinstance(col_or_name, str):
        cl = cl.alias(col_or_name)
    return cl


def create_list_column_parser(pattern: str = ",", value_type: Optional[str] = None, remove_empty_values: bool = True):
    """
    Helper function to wrap ``parse_list_value`` for use in tables declarations.
    :param pattern: string pattern to use for splitting the string column
    :param value_type: optional type to cast to
    :param remove_empty_values: boolean flag specifying if empty values should be removed as well
    :return: a lambda function that accepts a column to parse.
    """
    return lambda name: parse_list_value(name, pattern=pattern, value_type=value_type,
                                         remove_empty_values=remove_empty_values)


def extract_json(df: DataFrame, schema: Union[str, T.StructType], json_col: str = "value",
                 only_json_data: bool = False, drop_json_col: bool = False) -> DataFrame:
    """Helper function to simplify application of the ``from_json`` to DataFrame

    :param df: source DataFrame
    :param schema: schema to use in ``from_json``. Could be specified as string or as ``StructType`` instance
    :param json_col: name of column with JSON data
    :param only_json_data: boolean flag that specifies if we shouldn't include any original columns and leave only
      columns with extracted data.
    :param drop_json_col: boolean flag that specifies if we should drop the column with JSON data.
    :return: transformed DataFrame
    """
    json_col_name = "__jsn__"
    if only_json_data:
        ndf = df.select(F.from_json(json_col, schema).alias(json_col_name)) \
            .select(f"{json_col_name}.*")
    else:
        ndf = df.select("*", F.from_json(json_col, schema).alias(json_col_name)) \
            .select("*", f"{json_col_name}.*")\
            .drop(json_col_name)
        if drop_json_col:
            ndf = ndf.drop(json_col)

    return ndf


def bulk_rename_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Performs bulk rename of columns using the provided mapping between old & new column names

    :param df: DataFrame instance where columns should be renamed
    :param mapping: dictionary that maps old column names into new names
    :return: DataFrame with renamed columns
    """
    cols = []
    for cl in df.columns:
        rn = mapping.get(cl)
        if rn is None:
            cols.append(F.col(f"`{cl}`"))
        else:
            cols.append(F.col(f"`{cl}`").alias(rn))
    return df.select(*cols)


# TODO: think how to normalize names in the nested structs, etc.
def normalize_dataframe(df: DataFrame, mapping: Optional[Dict[str, Any]] = None,
                        snake_case: bool = True) -> DataFrame:
    """
    Perform normalization of a DataFrame - rename columns, do casts, perform additional conversions,
    like, convert JSON string into column, etc.
    :param df: DataFrame to normalize
    :param mapping: dictionary of column name into a dict with fields
        (`alias` - new name, `type` - new type, `conversion` - custom conversion function)
    :param snake_case: boolean defining if we should convert column names to snake_case
    :return: modified DataFrame
    """

    if not mapping and not snake_case:
        return df

    if mapping is None:
        mapping = {}

    cols = []
    for f in df.schema.fields:
        c = f.name
        m = mapping.get(c, {})
        if "conversion" in m:
            conv = m["conversion"]
            if isinstance(conv, Column):
                cl = conv
            else:
                cl = conv(F.col(f"`{c}`"))
        elif "type" in m:
            orig_type = f.dataType.typeName()
            new_type = get_spark_type_for_data_type(m["type"])
            if orig_type != new_type:
                cl = F.col(f"`{c}`").cast(new_type)
            else:
                cl = F.col(f"`{c}`")
        else:
            cl = F.col(f"`{c}`")
        cols.append(cl.alias(normalize_name(c, m.get("alias"), snake_case)))

    tdf = df.select(*cols)

    return tdf
