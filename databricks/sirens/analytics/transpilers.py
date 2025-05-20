"""

Author: Derek King
Dated: 2024-10-03
Version: 1.0

This module provides a set of functions to perform various data transformations and aggregations on a PySpark DataFrame,
specifically designed for use with Databricks and Splunk.

Classes:
- Splunk: A class containing static methods for various data transformations and aggregations.

Functions:

Splunk.eval(df, col_name, condition) -> DataFrame:    
- Evaluates a SQL expression on a specified column of the DataFrame.

Splunk.stats(df: DataFrame, aggregate_functions: list[Column], group_by: list) -> DataFrame:  
- Computes statistical aggregations on a DataFrame using specified aggregate functions and group-by columns.

Splunk.eventstats(df: DataFrame, aggregation_functions: list, group_by: list = []) -> DataFrame:
- Performs event statistics aggregation on a DataFrame using specified aggregation functions and optional group-by columns.

Splunk.streamstats(df: DataFrame, aggregation_functions: list, group_by: list = [], time_column: str = "_event_time",
- Applies streaming statistics to a DataFrame using specified aggregation functions and windowing options.

Splunk.outputlookup(df: DataFrame, schema: str, append: bool = False, columns: Union[List, None] = None) -> bool:

Splunk.inputlookup(schema: str, prefilter: str = None, append: bool = True, df: DataFrame = None,

Splunk.bucket(df: DataFrame, span: str, time_column: str = '_event_time') -> DataFrame:
- Buckets the DataFrame based on a specified time span.

Splunk.sort(df, columns: list, direction: Union[Literal['asc', 'desc']] = "asc") -> DataFrame:
- Sorts a DataFrame by specified columns in ascending or descending order.

Splunk.where(df, expression: str) -> DataFrame:

Splunk.table(df, columns: list) -> DataFrame:
"""

import re
from typing import Union, List, Literal

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column, Window, SparkSession

from databricks.sirens.internal.baseaggregate import BaseAggregator
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)

spark = SparkSession.getActiveSession()

__all__ = ["Splunk"]


class Stats(BaseAggregator):
    # no timebased agregation
    type = "stats"

    def __init__(self, agg_functions: List[Column], groupby_cols: List = []):
        self.agg_functions = agg_functions
        self.groupby_cols = groupby_cols

    def collect_stats(self, df):
        return super().collect_stats(df=df, groupby_cols=self.groupby_cols, aggs=self.agg_functions)


class Utils:

    @staticmethod
    def _get_multiplier(value):
        """
        Convert a time specifier string into its equivalent value in minutes.
        The function takes a string representing a time duration and converts it into minutes.
        The input string should be in the format of a number followed by a single character
        indicating the time unit. Supported units are:
        - 's' for seconds
        - 'm' for minutes
        - 'h' for hours
        - 'd' for days
        - 'w' for weeks
        Examples:
            - '1s' -> 0 (rounded from 0.0167)
            - '1m' -> 1
            - '1h' -> 60
            - '1d' -> 1440
            - '1w' -> 10080
        :param value: A string representing the time duration (e.g., '1s', '1m', '1h', '1d', '1w').
        :type value: str
        :return: The equivalent time duration in minutes, or None if the input is invalid.
        :rtype: int or None
        """
        matches = re.match(r'(\d+)([sdmhw])', value)
        if not matches.group(1) or not matches.group(2):
            print("invalid time specifier [1s, 1m, 1h, 1d, 1w]")
            return None

        num = int(matches.group(1))
        period = matches.group(2)
        if period == 's':
            multiplier = (num / 60)
        if period == 'm':
            multiplier = num
        if period == 'h':
            multiplier = (num * 60)
        if period == 'd':
            multiplier = (num * 1440)
        if period == 'w':
            multiplier = (num * 10080)

        return int(round(multiplier, 0))

    @staticmethod
    def _cols_exist_in_df(df, cols):
        """
        Check if all specified columns exist in the DataFrame.

        :param df: The DataFrame to check.
        :type df: pandas.DataFrame
        :param cols: A list of column names to check for existence in the DataFrame.
        :type cols: list
        :return: True if all columns exist in the DataFrame, False otherwise.
        :rtype: bool
        """
        for col in cols:
            if col not in df.columns:
                logger.warning("group_by columns do not exist in the DataFrame")
                return False
        return True

    @staticmethod
    def _str_to_list(value: Union[List, str]) -> List:
        """
        Convert a comma-separated string to a list.

        If the input value is a string, it splits the string by commas and returns a list of substrings.
        If the input value is already a list, it returns the value unchanged.

        :param value: The input value, which can be either a list or a comma-separated string.
        :type value: Union[List, str]
        :return: A list of substrings if the input is a string, or the original list if the input is already a list.
        :rtype: List
        """
        if isinstance(value, str):
            return [v.strip() for v in value.split(',')]
        return value


class Splunk:

    @staticmethod
    def eval(df: DataFrame, col_name: str, condition: str) -> DataFrame:
        """
            :param df: The input DataFrame.
            :type df: DataFrame
            :param col_name: The name of the column to be evaluated.
            :type col_name: str
            :param condition: The condition to be evaluated, expressed as a SQL expression.
            :type condition: str
            :returns: A new DataFrame with the evaluated column. If an error occurs, the original DataFrame is returned.
            :rtype: DataFrame
        """
        try:
            df_eval = df.withColumn(col_name, F.expr(condition))
        except Exception as exc:
            logger.error(f"Error evaluating expression: {exc}")
            return df

        return df_eval

    @staticmethod
    def stats(df: DataFrame, aggregate_functions: List[Column], group_by: List) -> DataFrame:
        """
        Compute statistical aggregations on a DataFrame.

        This function applies the specified aggregate functions to the DataFrame,
        grouping by the specified columns.

        :param df: The input DataFrame to compute statistics on.
        :type df: DataFrame
        :param aggregate_functions: A list of aggregate functions to apply.
        :type aggregate_functions: list[Column]
        :param group_by: A list of columns to group by.
        :type group_by: list
        :return: A DataFrame with the computed statistics.
        :rtype: DataFrame
        :raises Exception: If an error occurs during the aggregation process.
        """
        group_by = Utils._str_to_list(group_by)
        if not Utils._cols_exist_in_df(df, group_by):
            return df

        try:
            aggregation_exprs = [F.expr(x) for x in aggregate_functions]
            v = Stats(agg_functions=aggregation_exprs, groupby_cols=group_by)
            output_df = v.collect_stats(df)
        except Exception as exc:
            logger.error(f"Error aggregating stats: {exc}")
            return df

        return output_df

    @staticmethod
    def eventstats(df: DataFrame, aggregation_functions: List, group_by: List = []) -> DataFrame:
        """
        Perform event statistics aggregation on a DataFrame.
        This function applies specified aggregation functions to the DataFrame,
        optionally partitioning the data by specified columns.
        :param df: The input DataFrame to be aggregated.
        :type df: DataFrame
        :param aggregation_functions: A list of aggregation functions to apply. Each function should be a string
        in the format 'function_name column_name'.
        :type aggregation_functions: list
        :param group_by: A list of columns to group by. If empty, the entire DataFrame is considered as a single group.
        :type group_by: list, optional
        :return: A DataFrame with the applied aggregations.
        :rtype: DataFrame
        :raises Exception: If there is an error during the aggregation process.
        """
        group_by = Utils._str_to_list(group_by)

        if not Utils._cols_exist_in_df(df, group_by):
            return df
        try:
            if group_by:
                window = Window.partitionBy(*[group_by])
            else:
                window = Window.partitionBy(F.lit(1))

            expression = []
            for agg in aggregation_functions:
                components = agg.split()
                func_call = components[0]
                if 'as' in components[1].lower():
                    col_name = components[2]
                else:
                    col_name = components[0]
                expression.append((func_call, col_name))

            if not expression:
                print('invalid SQL expression')
                return df
        except Exception as exc:
            logger.error(f"Error aggregating eventstats: {exc}")
            return df

        return df.select('*', *[F.expr(y[0]).over(window).alias(y[1]) for y in expression])

    @staticmethod
    def streamstats(df: DataFrame, aggregation_functions: List, group_by: List = [], time_column: str = "_event_time",
                    window_size: Union[int, None] = None, window_time: Union[str, None] = None,
                    current: bool = True) -> DataFrame:
        """
        Apply streaming statistics to a DataFrame using specified aggregation functions and windowing options.
        :param df: The input DataFrame to which the streaming statistics will be applied.
        :type df: DataFrame
        :param aggregation_functions: A list of SQL-like aggregation functions to apply, e.g., ['avg(bytes) AS avg_bytes'].
        :type aggregation_functions: list
        :param group_by: A list of columns to group by. Default is an empty list.
        :type group_by: list, optional
        :param time_column: The name of the time column to use for windowing. Default is "_event_time".
        :type time_column: str, optional
        :param window_size: The size of the fixed window in terms of rows. Cannot be used with window_time. Default is None.
        :type window_size: Union[int, None], optional
        :param window_time: The size of the time-based window as a string (e.g., '10 minutes'). Cannot be used with window_size. Default is None.
        :type window_time: Union[str, None], optional
        :param current: Whether to use the current event (True) or the previous event (-1) for statistics. Default is True.
        :type current: bool, optional
        :return: A DataFrame with the applied streaming statistics.
        :rtype: DataFrame
        """
        group_by = Utils._str_to_list(group_by)

        if not Utils._cols_exist_in_df(df, group_by):
            return df
        try:
            if window_time and window_size:
                print('cannot specify both window_size and window_time')
                return df

            # if to use the current or -1 event for stats
            if current:
                last_event = 0
            else:
                last_event = -1

            # default to tumbling window
            if not window_time and not window_size:
                window_size = Window.unboundedPreceding

            # convert the window_time str to mins
            if window_time:
                multiplier = Utils._get_multiplier(window_time)

            # Fixed Size tumbling Window(s)
            if window_size and group_by:
                window = (Window.partitionBy(*group_by).orderBy(
                    F.col(time_column).cast("timestamp").cast("long")).rowsBetween(window_size, last_event))
            if window_size and not group_by:
                window = (Window.orderBy(F.col(time_column).cast("timestamp").cast("long")).rowsBetween(window_size,
                                                                                                        last_event))
            # Timebased Window(s)
            if window_time and group_by:
                window = (Window.partitionBy(*group_by).orderBy(
                    F.col(time_column).cast("timestamp").cast("long")).rangeBetween(-(multiplier * 60), last_event))
            if window_time and not group_by:
                window = (
                    Window.orderBy(F.col(time_column).cast("timestamp").cast("long")).rangeBetween(-(multiplier * 60),
                                                                                                   last_event))

            # Create transformation expressions
            expression = []
            for agg in aggregation_functions:
                components = agg.split()
                func_call = components[0]
                if 'as' in components[1].lower():
                    col_name = components[2]
                else:
                    col_name = components[0]
                expression.append((func_call, col_name))

            if not expression:
                print("invalid SQL expressions ['avg(bytes) AS avg_bytes']")
                return df
        except Exception as exc:
            logger.error(f"Error aggregating streamstats: {exc}")
            return df

        return df.select('*', *[F.expr(y[0]).over(window).alias(y[1]) for y in expression]).na.fill(0)

    @staticmethod
    def outputlookup(df: DataFrame, schema: str, append: bool = False, columns: Union[List, None] = None) -> bool:
        """
        Writes a DataFrame to a specified schema in a database.
        :param df: The DataFrame to be written.
        :type df: DataFrame
        :param schema: The target schema where the DataFrame will be saved.
        :type schema: str
        :param append: If True, data will be appended to the existing table. If False, the table will be overwritten. Default is False.
        :type append: bool, optional
        :param columns: A list of column names to be selected from the DataFrame before writing. If None, all columns will be written. Default is None.
        :type columns: Union[List, None], optional
        :return: True if the DataFrame is successfully written, False otherwise.
        :rtype: bool
        """
        try:
            if append:
                mode = 'append'
            else:
                mode = 'overwrite'

            if columns:
                df = df.select(*[columns])
        except Exception as exc:
            logger.error(f"Error outputlookup: {exc}")
            return False

        return df.write.mode(mode).saveAsTable(schema)

    @staticmethod
    def inputlookup(schema: str, prefilter: str = None, append: bool = True, df: DataFrame = None,
                    columns: Union[List, None] = None) -> bool:
        """
        Reads a table from the given schema, optionally filters and selects columns, and appends it to an existing DataFrame.
        :param schema: The schema name of the table to read.
        :type schema: str
        :param prefilter: An optional filter condition to apply to the table.
        :type prefilter: str, optional
        :param append: Whether to append the read table to the existing DataFrame. Defaults to True.
        :type append: bool, optional
        :param df: The existing DataFrame to append to. If None, the function will return the read table.
        :type df: DataFrame, optional
        :param columns: A list of columns to select from the table. If None, all columns are selected.
        :type columns: Union[List, None], optional
        :return: The resulting DataFrame after reading, filtering, selecting, and appending.
        :rtype: DataFrame
        :raises Exception: If there is an error reading the table or performing operations.
        """
        try:
            lookup_df = spark.read.table(schema)

            if columns:
                lookup_df = lookup_df.select(*[columns])

            if prefilter:
                lookup_df = lookup_df.filter(prefilter)

            if append and df:
                df = df.union(lookup_df)
            else:
                # return only the lookup table
                df = lookup_df
        except Exception as exc:
            logger.error(f"Error inputlookup: {exc}")
            return False

        return df

    @staticmethod
    def bucket(df: DataFrame, span: str, time_column: str = '_event_time') -> DataFrame:
        """
        Bucket the DataFrame based on a specified time span.

        :param df: The input DataFrame to be bucketed.
        :type df: DataFrame
        :param span: The time span for bucketing, e.g., '10 minutes'.
        :type span: str
        :param time_column: The name of the time column to use for bucketing. Default is '_event_time'.
        :type time_column: str, optional
        :return: A DataFrame with an additional column for the time window.
        :rtype: DataFrame
        :raises Exception: If the specified time column does not exist in the DataFrame.
        """
        # TODO check span string is valid

        if time_column not in df.columns:
            raise Exception(f"Column {time_column} does not exist")

        return df.select("*", F.window(time_column, span))

    @staticmethod
    def sort(df: DataFrame, by: List, direction: Literal['asc', 'desc'] = "asc") -> DataFrame:
        """
        Sort a DataFrame by specified columns in ascending or descending order.
        :param df: The DataFrame to be sorted.
        :type df: DataFrame
        :param by: List of column names to sort by.
        :type by: list
        :param direction: The direction to sort the DataFrame, either 'asc' for ascending or 'desc' for descending. Default is 'asc'.
        :type direction: Literal['asc', 'desc']
        :return: The sorted DataFrame.
        :rtype: DataFrame
        """
        by = Utils._str_to_list(by)
        if not Utils._cols_exist_in_df(df, by):
            return df
        if direction == "asc":
            ascending = True
        else:
            ascending = False

        return df.sort(*[by], ascending=ascending)

    @staticmethod
    def where(df: DataFrame, expression: str) -> DataFrame:
        """
        Filters the DataFrame based on the given SQL expression.
        :param df: The DataFrame to filter.
        :type df: DataFrame
        :param expression: The SQL expression to filter the DataFrame.
        :type expression: str
        :return: A DataFrame filtered according to the given expression.
        :rtype: DataFrame
        """
        return df.filter(F.expr(expression))

    @staticmethod
    def table(df, columns: List) -> DataFrame:
        """
        Selects specified columns from a DataFrame.
        Args:
            df (DataFrame): The input DataFrame.
            columns (list): A list of column names to select from the DataFrame.
        Returns:
            DataFrame: A new DataFrame with only the specified columns.
        """
        columns = Utils._str_to_list(columns)
        if not Utils._cols_exist_in_df(df, columns):
            return df
        return df.select(*columns)
