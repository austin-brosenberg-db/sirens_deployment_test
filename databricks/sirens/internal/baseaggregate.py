"""Internal base classes for building content off.

    Authors:
        Derek King 14th September 2023

    classes:
        BaseAggregator:
            Implements the logic used for creating Spark Streaming Aggregations.

    functions:
        collect_stats():
            basic grouping, aggregation of a DataFrame

        collect_stats_over_window_with_watermarking():
            aggregates over a window, and uses the .withWatermark() argument to capture/drop late data

        collect_stats_over_tumbling_window():
            aggregates over a user defined time window

        collect_stats_over_sliding_window():
            aggregates over a user defined time window, that includes a sliding window argument

"""
from typing import List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from databricks.sirens.exceptions import SirensAggregationException
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

class BaseAggregator:
    """Base class for aggregation operations
    """
    type = "base_aggregator"

    @staticmethod
    def collect_stats(df: DataFrame, groupby_cols: list, aggs: list,
                      agg_name: str = None) -> DataFrame:
        """Return an aggregated dataframe

        :param df: incoming dataframe
        :type df: DataFrame
        :param groupby_cols: list of columns in the dataframe to groupby
        :type groupby_cols: list
        :param aggs: list of aggregate functions to apply
        :type aggs: list
        :param name: aggregation collection name (i.e 'per_hour')
        :type name: str, optional
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        logger.debug('message=collecting stats - no time windows, no watermarking/state management')
        try:
            df = df.groupby(*groupby_cols).agg(*aggs)

            if bool(agg_name):
                df = df.withColumn("aggregation_name", F.lit(agg_name))

        except Exception as exc:
            logger.error(f"message={exc}")
            raise SirensAggregationException(exc) from exc

        return df

    @staticmethod
    def collect_stats_over_window_with_watermarking(df: DataFrame, groupby_cols: list, aggs: list,
                                                    window_duration: str, wait_for_late_data: str, slide_duration: str = None,
                                                    timeColumn: str = "_event_time",
                                                    start_time: str = None, agg_name: str = None) -> DataFrame:
        """return watermarked tumbling or sliding windowed aggregate.

        :param df: incoming dataframe
        :type df: DataFrame
        :param groupby_cols: list of columns in the dataframe to groupby
        :type groupby_cols: list
        :param aggs: list of aggregate functions to apply
        :type aggs: list
        :param window_duration: A string specifying the width of the window, e.g. 10 minutes, 1 second
        :type window_duration: str
        :param wait_for_late_data: .withWatermark - the minimum delay to wait to data to arrive late, relative to the latest record that has been processed in the form of an interval (e.g. “1 minute” or “5 hours”)
        :type wait_for_late_data: str
        :param slide_duration: A new window will be generated every slideDuration. Must be less than or equal to the windowDuration.
        :type slide_duration: str
        :param timeColumn: The column or the expression to use as the timestamp for windowing by time, defaults to "_event_time"
        :type timeColumn: Column, optional
        :param start_time: The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals, defaults to None
        :type start_time: _type_, optional
        :param name: aggregation collection name (i.e 'per_hour')
        :type name: str, optional
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        logger.debug('message=collecting stats window with watermarking/state management')
        try:
            windowing = F.window(timeColumn=timeColumn, windowDuration=window_duration, slideDuration=slide_duration, startTime=start_time)
            df = df.withWatermark(eventTime=timeColumn, delayThreshold=wait_for_late_data) \
                .groupBy(*groupby_cols, windowing) \
                .agg(*aggs) \
                .select("*", F.col("window.start").cast("timestamp").alias("window_start"), F.col("window.end").cast("timestamp").alias("window_end")) \
                .drop("window")

            if bool(agg_name):
                df = df.withColumn("aggregation_name", F.lit(agg_name))

        except Exception as exc:
            logger.error(f"message={exc}")
            raise SirensAggregationException(exc) from exc

        return df

    @staticmethod
    def collect_stats_over_tumbling_window(df: DataFrame, groupby_cols: list, aggs: list,
                                           window_duration: str, timeColumn: str = "_event_time",
                                           start_time=None, agg_name: str = None) -> DataFrame:
        """return aggregation over tumbling window

        :param df: incoming dataframe
        :type df: DataFrame
        :param groupby_cols: list of columns in the dataframe to groupby
        :type groupby_cols: list
        :param aggs: list of aggregate functions to apply
        :type aggs: list
        :param window_duration: A string specifying the width of the window, e.g. 10 minutes, 1 second
        :type window_duration: str
        :param timeColumn: The column or the expression to use as the timestamp for windowing by time, defaults to "_event_time"
        :type timeColumn: Column, optional
        :param start_time: The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals, defaults to None
        :type start_time: _type_, optional
        :param name: aggregation collection name (i.e 'per_hour')
        :type name: str, optional
        :return: aggregated dataframe
        :rtype: DataFrame
        """

        logger.debug('message=tumbling window - no watermarking/state management')
        try:
            windowing = F.window(timeColumn=timeColumn, windowDuration=window_duration, startTime=start_time)
            df = df.groupBy(*groupby_cols, windowing).agg(*aggs) \
                .select("*", F.col("window.start").cast("timestamp").alias("window_start"), F.col("window.end").cast("timestamp").alias("window_end")) \
                .drop("window")

            if bool(agg_name):
                df = df.withColumn("aggregation_name", F.lit(agg_name))

        except Exception as exc:
            logger.error(f"message={exc}")
            raise SirensAggregationException(exc) from exc

        return df

    @staticmethod
    def collect_stats_over_sliding_window(df: DataFrame, groupby_cols: list, aggs: list, window_duration: str,
                                          slide_duration: str, timeColumn: str = "_event_time",
                                          start_time=None, agg_name: str = None) -> DataFrame:
        """return aggregation over a sliding window

        :param df: incoming dataframe
        :type df: DataFrame
        :param groupby_cols: list of columns in the dataframe to groupby
        :type groupby_cols: list
        :param aggs: list of aggregate functions to apply
        :type aggs: list
        :param window_duration: A string specifying the width of the window, e.g. 10 minutes, 1 second
        :type window_duration: str
        :param slide_duration: A new window will be generated every slideDuration. Must be less than or equal to the windowDuration.
        :type slide_duration: str
        :param timeColumn: The column or the expression to use as the timestamp for windowing by time, defaults to "_event_time"
        :type timeColumn: Column, optional
        :param start_time: The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals, defaults to None
        :type start_time: _type_, optional
        :param name: aggregation collection name (i.e 'per_hour')
        :type name: str, optional
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        logger.debug('message=sliding window - no watermarking....')
        try:
            windowing = F.window(timeColumn=timeColumn, windowDuration=window_duration, slideDuration=slide_duration, startTime=start_time)
            df = df.groupBy(*groupby_cols, windowing).agg(*aggs) \
                .select("*", F.col("window.start").cast("timestamp").alias("window_start"), F.col("window.end").cast("timestamp").alias("window_end")) \
                .drop("window")

            if bool(agg_name):
                df = df.withColumn("aggregation_name", F.lit(agg_name))

        except Exception as exc:
            logger.error(f"message={exc}")
            raise SirensAggregationException(exc) from exc

        return df
