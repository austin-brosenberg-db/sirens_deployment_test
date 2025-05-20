"""
User facing API to Aggregate tables in the data pipeline

Authors:
    Derek King 14th September 2023

Classes:
    PipelineAggregator:
        Implements aggregations extending BaseAggregator for pipeline based aggregations.
    Aggregate:
        User facing API for aggregations (adhoc & ETL pipeline aggregations).

Functions:
    pipeline_aggregation():
        user facing function that accepts dataSourceObj to execute pipeline based aggregations.
        Depending on the configuration options passed, the DataFrame will be returned appropriately
        (i.e tumbling window, sliding window, stateful watermarked etc)

"""
from typing import Union, List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensAggregationException
from databricks.sirens.internal.baseaggregate import BaseAggregator
from databricks.sirens.logging import get_logger
from databricks.sirens.utils.base_utils import BaseUtils

logger = get_logger(__name__)


class PipelineAggregator(BaseAggregator):
    """aggregator class for data pipelines
    """

    type = "pipeline_aggregator"

    def __init__(self, groupby: list, aggs: list, window_duration=None, wait_for_late_data=None,
                 slide_duration=None, timeColumn: str = "_event_time", start_time: str = None,
                 agg_name: str = None):

        self.groupby = groupby
        self.aggs = aggs
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.timeColumn = timeColumn
        self.wait_for_late_data = wait_for_late_data
        self.start_time = start_time
        self.agg_name = agg_name

    def agg_by_window_with_state(self, df: DataFrame) -> DataFrame:
        """return watermarked tumbling or sliding windowed aggregate

        :param df: incoming dataframe
        :type df: DataFrame
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        try:
            return super().collect_stats_over_window_with_watermarking(
                df=df, groupby_cols=self.groupby, aggs=self.aggs,
                window_duration=self.window_duration, wait_for_late_data=self.wait_for_late_data,
                slide_duration=self.slide_duration, timeColumn=self.timeColumn,
                start_time=self.start_time, agg_name=self.agg_name)

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensAggregationException(exc) from exc

    def agg_by_sliding_window(self, df: DataFrame) -> DataFrame:
        """return sliding window aggregation

        :param df: incoming dataframe
        :type df: DataFrame
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        try:
            return super().collect_stats_over_sliding_window(
                df=df, groupby_cols=self.groupby, aggs=self.aggs,
                window_duration=self.window_duration, slide_duration=self.slide_duration,
                timeColumn=self.timeColumn, start_time=self.start_time, agg_name=self.agg_name)

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensAggregationException(exc) from exc

    def agg_by_tumbling_window(self, df: DataFrame) -> DataFrame:
        """return tumbling window aggregation

        :param df: incoming dataframe
        :type df: DataFrame
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        try:
            return super().collect_stats_over_tumbling_window(
                df=df, groupby_cols=self.groupby, aggs=self.aggs, window_duration=self.window_duration,
                timeColumn=self.timeColumn,
                start_time=self.start_time, agg_name=self.agg_name)

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensAggregationException(exc) from exc

    def agg_no_window(self, df: DataFrame) -> DataFrame:
        """return aggregated dataframe

        :param df: incoming dataframe
        :type df: DataFrame
        :return: aggregated dataframe
        :rtype: DataFrame
        """
        try:
            return super().collect_stats(df=df, groupby_cols=self.groupby, aggs=self.aggs,
                                         agg_name=self.agg_name)
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensAggregationException(exc) from exc


class Aggregate:
    SUPPORTED_AGGREGATION_KEYS = ['name', 'aggregation_name', 'target_table', 'source_table', 'stream_type',
                                  'save_mode',
                                  'filter', 'groupby', 'window_duration', 'wait_for_late_data', 'agg_columns',
                                  'sliding_duration', 'start_time']

    def __init__(self, data_source_obj: Optional[DataSource] = None):
        if data_source_obj:
            self.pipeline_run_id = data_source_obj.pipeline_run_id
            self.run_as_pipeline = True
        else:
            self.run_as_pipeline = False

    @staticmethod
    def _get_agg_cols_as_expr(agg_cols_as_str: str) -> List[Column]:
        if not bool(agg_cols_as_str):
            aggs = [F.expr("count('*') AS count")]
        else:
            agg_cols = agg_cols_as_str.split(",")
            agg_cols = [x.strip() for x in agg_cols]
            aggs = [F.expr(x) for x in agg_cols]

        return aggs

    @staticmethod
    def _clean_cols(cols_as_str: str) -> Union[str, list]:
        if not cols_as_str:
            return cols_as_str
        else:
            cols = cols_as_str.split(",")
            cols = [x.strip() for x in cols]
            return cols

    def _apply_filter(self, df: DataFrame, sql_filter: str) -> DataFrame:
        try:
            df = df.filter(sql_filter)
            return df
        except Exception as exc:
            if self.run_as_pipeline:
                logger.error(f"pipeline_run_id={self.pipeline_run_id} message=invalid sql filter: {exc}")
            else:
                logger.error(f"message={exc}")

            raise SirensAggregationException(exc)

    @staticmethod
    def _valid_configs(aggregate_config: dict, df_cols: list, is_streaming_df: bool) -> bool:
        error_list = []

        stream_type = aggregate_config.get('stream_type')
        save_mode = aggregate_config.get('save_mode')
        sql_filter = aggregate_config.get('filter')
        groupby_cols = aggregate_config.get('groupby')
        window_duration = aggregate_config.get('window_duration')
        sliding_duration = aggregate_config.get('sliding_duration')
        wait_for_late_data = aggregate_config.get('wait_for_late_data')
        target_table = aggregate_config.get('target_table')

        # check for valid keys
        config_keys = list(aggregate_config.keys())
        if not all(i in Aggregate.SUPPORTED_AGGREGATION_KEYS for i in config_keys):
            logger.info("Invalid keys detected - check config")
            error_list.append("Invalid Keys detected in aggregation config - check.")

        # grab streaming type from dataframe if not a configured option in agg config.
        # this means the aggregation is using the pipeline default key (streamType)
        if not bool(stream_type):
            if is_streaming_df:
                stream_type = 'streaming'
            else:
                stream_type = 'batch'

        # req'd save_mode key. (update or complete)
        if not bool(save_mode) and target_table:
            error_list.append('required key (save_mode) not set.')
        elif save_mode and not target_table:
            logger.warn("save_mode is set for a dataframe.")

        if bool(save_mode) and (save_mode != "append" and save_mode != "complete"):
            error_list.append("save_mode must be one of (append | complete)")

        # check valid args for stream_type.
        if bool(stream_type) and (stream_type != "streaming" and stream_type != "batch"):
            error_list.append('stream_type must be one of (batch | streaming)')

        # update mode can only be used with watermarking, and window function.
        if stream_type == 'streaming' and save_mode == 'append':
            if not bool(window_duration) and not bool(wait_for_late_data):
                error_list.append('streaming in append mode requires both (wait_for_late_data) and (window_duration) '
                                  'to be configured')

        # wait_for_late_data is a structured streaming operation ONLY. cannot be used for batch reads.
        if stream_type == 'batch' and bool(wait_for_late_data):
            error_list.append("watermarking defined by (wait_for_late_data) is a streaming operation only. correct "
                              "stream_type, or remove watermarking")

        # sql_filter
        if bool(sql_filter) and not BaseUtils.validate_sql_expression(sql_filter):
            error_list.append(f"invalid sql_filter: {sql_filter}")

        # groupby_cols
        if bool(groupby_cols) and not all(i in df_cols for i in Aggregate._clean_cols(groupby_cols)):
            error_list.append("group_by col(s) missing from incoming dataframe")

        # windowing bit on but required window_duration not specified.
        if bool(wait_for_late_data) and not bool(window_duration):
            error_list.append("wait_for_late_data set, bit window_duration is not")

        if error_list:
            logger.info("message=pipeline aggregation checks found the following errors")
            logger.info(error_list)
            return False

        return True

    def pipeline_aggregation(self, df: DataFrame, aggregate_config: dict) -> Tuple[DataFrame, bool]:
        """take a dataframe with yaml config and return an aggregated dataframe.

        :param df: incoming dataframe
        :type df: DataFrame
        :param aggregate_config: aggregation config from inputs.yaml specific to this aggregation
        :type aggregate_config: dict
        :return: aggregated dataframe or original dataframe is an error, with a bool for OK|Error
        :rtype: DataFrame
        """

        # pre-flight checks. Should already be done in validate routine
        # but check anyway to keep a pipeline from failing.
        # skip this aggregation - but do not abort the pipeline if invalid.
        if not Aggregate._valid_configs(aggregate_config, df.columns, df.isStreaming):
            logger.warning("Invalid Aggregate Config Found - Skipping.")
            return df, False

        groupby_cols, agg_cols = [], []

        # read vars
        sql_filter = aggregate_config.get('filter')
        groupby_cols = aggregate_config.get('groupby')
        agg_cols = aggregate_config.get('agg_columns')
        window_duration = aggregate_config.get('window_duration')
        sliding_duration = aggregate_config.get('sliding_duration')
        wait_for_late_data = aggregate_config.get('wait_for_late_data')
        start_time = aggregate_config.get('start_time')
        aggregation_name = aggregate_config.get("aggregation_name")

        # clean up groupby(s)
        groupby_cols = Aggregate._clean_cols(groupby_cols)

        # generate aggregate columns
        aggs = Aggregate._get_agg_cols_as_expr(agg_cols)

        # filter clause specified for this aggregation
        if bool(sql_filter):
            logger.debug(f"message=filtering dataframe. Filter: {sql_filter}")
            df = self._apply_filter(df, sql_filter)

        aggregated_df = None

        # using watermarking streaming state management
        if bool(wait_for_late_data):
            # watermarked is the highest priority
            if self.run_as_pipeline:
                logger.debug(f'pipeline_run_id={self.pipeline_run_id} message=creating watermarked agg, w/windowing - '
                             f'either tumbling or sliding.')
            else:
                logger.debug('message=creating watermarked agg, w/windowing - either tumbling or sliding.')

            aggregated_df = PipelineAggregator(groupby_cols, aggs, window_duration=window_duration,
                                               wait_for_late_data=wait_for_late_data, slide_duration=sliding_duration,
                                               start_time=start_time, agg_name=aggregation_name) \
                .agg_by_window_with_state(df)  # noqa
            return aggregated_df, True

        # using tumbling window aggregation.
        if bool(window_duration) and not bool(sliding_duration):
            if self.run_as_pipeline:
                logger.debug(f'pipeline_run_id={self.pipeline_run_id} message=creating tumbling window over '
                             f'_event_time WITHOUT watermarking')
            else:
                logger.debug('message=creating tumbling window over _event_time WITHOUT watermarking')
            aggregated_df = PipelineAggregator(groupby_cols, aggs, window_duration=window_duration,
                                               slide_duration=sliding_duration, start_time=start_time,
                                               agg_name=aggregation_name) \
                .agg_by_tumbling_window(df=df)  # noqa
            return aggregated_df, True

        # using sliding window aggregation
        if bool(window_duration) and bool(sliding_duration):
            if self.run_as_pipeline:
                logger.debug(f'pipeline_run_id={self.pipeline_run_id} message=creating sliding window over '
                             f'_event_time WITHOUT watermarking')
            else:
                logger.debug('message=creating sliding window over _event_time WITHOUT watermarking')
            aggregated_df = PipelineAggregator(groupby_cols, aggs, window_duration=window_duration,
                                               slide_duration=sliding_duration, start_time=start_time,
                                               agg_name=aggregation_name) \
                .agg_by_sliding_window(df=df)  # noqa
            return aggregated_df, True

        # using simple aggregation without time-based windowing
        if not aggregated_df:
            if self.run_as_pipeline:
                logger.debug(f'pipeline_run_id={self.pipeline_run_id} message=creating standard (non time bucketed '
                             f'window) expression')
            else:
                logger.debug('message=creating standard (non time bucketed window) expression')
            aggregated_df = PipelineAggregator(groupby_cols, aggs, window_duration=window_duration,
                                               slide_duration=sliding_duration, agg_name=aggregation_name) \
                .agg_no_window(df=df)  # noqa

            return aggregated_df, True
