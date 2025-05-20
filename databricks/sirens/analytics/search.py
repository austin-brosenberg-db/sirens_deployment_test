"""

Author: Derek King
Date: 2024-06-10
Version: 1.0

This module provides functions for performing various analytics on data using Spark DataFrames. The functions include:
- `ioc_hits`: Looks up IOC matches in the intelligence table by executing a Spark join operation.
- `ioc_extract`: Extracts IOC patterns from a DataFrame using built-in or custom REGEX patterns.
- `first_time_seen`: Analyzes the first time a specific event is seen in the given DataFrame.
- `time_series_spike`: Analyzes time series data for spikes by aggregating the data into buckets and applying specified aggregate functions.
Functions:
-----------
- `ioc_hits(df: DataFrame, indicator_column: Column, matches_only: bool = False, ioc_type: str = None, target_intel_column: Column = None, schema: str = ThreatIntel.DATABASE, intel_table: str = ThreatIntel.INTEL_TABLE, output_format: OutputFormat = OutputFormat.asDataFrame) -> DataFrame`
    Given a DataFrame, looks up IOC matches in the intelligence table by executing a Spark join operation.
- `ioc_extract(df: DataFrame, search_cols: List[Column] = None, search_ioc_types: List[IOCType] = None, add_ioc_types: List[IOCType] = None, output_format: OutputFormat = OutputFormat.asDataFrame) -> Union[DataFrame | None]`
    Extracts IOC patterns from a DataFrame using built-in REGEX(s) or by adding custom IOCTypes and REGEX patterns.
- `first_time_seen(df: DataFrame, group_by: list, baseline: str, time_column: str = "_event_time", allow_list: Union[DataFrame, str] = None, output_format: OutputFormat = OutputFormat.asDataFrame) -> AnalysisOutput`
    Analyzes the first time a specific event is seen in the given DataFrame, using the baseline lookup cache.
- `time_series_spike(df, bucket_span: str, aggregate_functions: list, group_by: list, time_column: str = "_event_time", allow_list: Union[DataFrame, str] = None, stddev_multiplier: int = 2, output_format: OutputFormat = OutputFormat.asDataFrame) -> AnalysisOutput`
    Analyzes time series data for spikes by aggregating the data into buckets and applying specified aggregate functions.
"""

from databricks.sirens.common.entities import OutputFormat, ThreatIntel
from databricks.sirens.common.outputs import sirens_analytics, AnalysisOutput
from pyspark.sql import DataFrame, Column
from typing import Union, List
from databricks.sirens.analytics import _search
from databricks.sirens.analytics import _ioc_extractor
from databricks.sirens.common.entities import IOCType

__all__ = ["ioc_hits", "ioc_extract", "first_time_seen", "time_series_spike"]


@sirens_analytics()
def ioc_hits(df: DataFrame,
             indicator_column: Column,
             matches_only: bool = False,
             ioc_type: str = None,
             target_intel_column: Column = None,
             schema: str = ThreatIntel.DATABASE,
             intel_table: str = ThreatIntel.INTEL_TABLE,
             output_format: OutputFormat = OutputFormat.asDataFrame) -> DataFrame:
    """Given a DataFrame lookup IOC matches in the intelligence table by executing a spark join operation.

    :param df: Dataframe used as the incoming reference
    :type df: DataFrame
    :param indicator_column: The column the function should join on (for intel matches)
    :type indicator_column: Column
    :param matches_only: Return ONLY rows with a successful IOC hit in the Intel table, defaults to False
    :type matches_only: bool, optional
    :param ioc_type: Optional filter to apply to the intel table for a specific IOC type if known (speeds up joins), defaults to None
    :type ioc_type: str, optional
    :param target_intel_column: Specify the target column in the intel table if different to the source, defaults to None
    :type target_intel_column: Column, optional
    :param schema: Override the 'database' key in sirens.config by providing a specific schema to find the intel table in, defaults to ThreatIntel.DATABASE
    :type schema: str, optional
    :param intel_table: The delta table to be used if required, defaults to ThreatIntel.INTEL_TABLE
    :type intel_table: str, optional
    :param output_format: Override the default OutputFormat style, defaults to OutputFormat.asDataFrame
    :type output_format: OutputFormat, optional
    :return: A dataset that has been joined with the threat intelligence table where matches have been found
    :rtype: DataFrame
    """
    return _search._ioc_hits(df=df, indicator_column=indicator_column, matches_only=matches_only,
                             ioc_type=ioc_type, target_intel_column=target_intel_column, schema=schema,
                             intel_table=intel_table, output_format=output_format)


@sirens_analytics()
def ioc_extract(df: DataFrame, search_cols: List[Column] = None,
                search_ioc_types: List[IOCType] = None,
                add_ioc_types: List[IOCType] = None,
                output_format: OutputFormat = OutputFormat.asDataFrame) -> Union[DataFrame, None]:
    """Extract IOC patterns from a DataFrame using built-in REGEX(s), or by adding custom IOCTypes and REGEX patterns.

    Has default regex patterns for the following IOC Types:
     - IPV4
     - URLs
     - DNS
     - EMAIL Addresses
     - HASHES (MD5, SHA1, SHA256)
     - Windows File Paths

     You can add new IOCTypes with REGEXs to search for by including the add_ioc_types key with a
     namedtuple of IOCType (imported from databricks.sirens.entities).

     Example: linux_fp_rex = IOCType("LinuxFps", "<regex>", "3")
              results = ioc_extract(df, add_ioc_types=linux_fp_rex, search_ioc_types=["LinuxFps"])

    :param df: Incoming DataFrame to search
    :type df: DataFrame
    :param search_cols: Limit the search to a column or column(s), defaults to ALL Columns in the DataFrame
    :type search_cols: List[Column], optional
    :param search_ioc_types: Narrow the search for only specific IOC Types, defaults to ALL types
    :type search_ioc_types: List[IOCType], optional
    :param add_ioc_types: Add new IOCType(s) to the class for searching, defaults to None
    :type add_ioc_types: List[IOCType], optional
    ::param output_format: Override the default OutputFormat style, defaults to OutputFormat.asDataFrame
    :type output_format: OutputFormat, optional
    :return: Returns a DataFrame of IOCs and IOC Types, the original Row, and Column they were found in
    :rtype: Union[DataFrame | None]
    """
    return _ioc_extractor._ioc_extract(df=df, search_cols=search_cols, search_ioc_types=search_ioc_types,
                                       add_ioc_types=add_ioc_types, output_format=output_format)


# TODO - baseline needs to be defined / better understood - an maybe optional - also str is not the right def
# inition - as its really a delta table?
@sirens_analytics()
def first_time_seen(df: DataFrame, group_by: list, baseline: str, time_column: str = "_event_time",
                    allow_list: Union[DataFrame, str] = None,
                    output_format: OutputFormat = OutputFormat.asDataFrame) -> AnalysisOutput:
    """
    Analyzes the first time a specific event is seen in the given DataFrame, using the baseline lookup cache
    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the event data.
    group_by : list
        A list of columns to group by when analyzing the events.
    baseline : str
        The baseline column to compare against.
    time_column : str, optional
        The name of the column containing the event time, by default "_event_time".
    allow_list : Union[DataFrame, str], optional
        An optional allow list to filter the events, by default None.
    output_format : OutputFormat, optional
        The format of the output, by default OutputFormat.asDataFrame.
    Returns
    -------
    AnalysisOutput
        The result of the analysis in the specified output format.
    """
    return _search._first_time_seen(df=df, group_by=group_by, baseline=baseline, time_column=time_column,
                                    allow_list=allow_list, output_format=output_format)


@sirens_analytics()
def time_series_spike(df, bucket_span: str, aggregate_functions: list, group_by: list,
                      time_column: str = "_event_time", allow_list: Union[DataFrame, str] = None,
                      stddev_multiplier: int = 2,
                      output_format: OutputFormat = OutputFormat.asDataFrame) -> AnalysisOutput:
    """
    Analyze time series data for spikes.

    This function detects spikes in time series data by aggregating the data into buckets and applying specified aggregate functions. It can group the data by specified columns and allows for filtering using an allow list. The output format can be customized.

    :param df: The input DataFrame containing the time series data.
    :type df: DataFrame
    :param bucket_span: The span of each bucket for aggregation (e.g., '1h' for one hour).
    :type bucket_span: str
    :param aggregate_functions: A list of aggregate functions to apply to the data (e.g., ['sum', 'mean']).
    :type aggregate_functions: list
    :param group_by: A list of columns to group the data by.
    :type group_by: list
    :param time_column: The name of the column containing the time information. Defaults to "_event_time".
    :type time_column: str, optional
    :param allow_list: A DataFrame or a string specifying the allow list for filtering the data. Defaults to None.
    :type allow_list: Union[DataFrame, str], optional
    :param stddev_multiplier: The multiplier for the standard deviation to determine spikes. Defaults to 2.
    :type stddev_multiplier: int, optional
    :param output_format: The format of the output. Defaults to OutputFormat.asDataFrame.
    :type output_format: OutputFormat, optional
    :return: The result of the spike analysis.
    :rtype: AnalysisOutput
    """
    return _search._time_series_spike(df=df, bucket_span=bucket_span, aggregate_functions=aggregate_functions,
                                      groupby=group_by, time_column=time_column, allow_list=allow_list,
                                      stddev_multiplier=stddev_multiplier, output_format=output_format)
