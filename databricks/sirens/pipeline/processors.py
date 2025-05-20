import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, ArrayType


@pandas_udf(StringType())
def lookup_value_udf(current_values: pd.Series, lookuptable: pd.Series) -> pd.Series:
    """A Pandas UDF to map a value (e.g. error code) from the column of 'current_values' to the description defined in lookuptable column.

    :param current_values: a batch of values to be mapped.
    :type current_values: Pandas Series
    :param lookuptable: a batch of map objects
    :type lookuptable: Pandas Series
    :return: a batch of descriptions mapped from the lookup table.
    :rtype: Pandas Series
    """
    lookup_local = lookuptable[0]
    return current_values.apply(lambda x: lookup_local.get(x, None))

@pandas_udf(ArrayType(StringType()))
def lookup_value_list_udf(current_values: pd.Series, lookuptable: pd.Series) -> pd.Series:
    """A Pandas UDF to map a list of values (e.g. a list of privileges) from the column of 'current_values' to the description defined in lookuptable column.

    :param current_values: a batch of value lists to be mapped.
    :type current_values: Pandas Series
    :param lookuptable: a batch of map objects
    :type lookuptable: Pandas Series
    :return: a batch of description lists mapped from the lookup table.
    :rtype: Pandas Series
    """

    lookup_local = lookuptable[0]
    result = []
    for lst in current_values:
        row = [lookup_local.get(val, val) for val in lst]
        result.append(row)
    return pd.Series(result)
