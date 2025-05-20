import pytest

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import TimestampType

from databricks.sirens.aggregate import Aggregate, PipelineAggregator
from databricks.sirens.exceptions import SirensAggregationException


def test__get_agg_cols_as_expr_no_cols(spark_session):
    agg_cols = None
    result = Aggregate._get_agg_cols_as_expr(agg_cols)
    assert isinstance(result, list)
    assert isinstance(result[0], Column)

def test__get_agg_cols_as_expr_cols(spark_session):
    agg_cols = "min('field1'), avg('field1'), max('fields1')"
    result = Aggregate._get_agg_cols_as_expr(agg_cols)
    assert isinstance(result, list)
    assert isinstance(result[0], Column)

def test__clean_cols_w_cols():
    cols = " column1, column2 "
    result = Aggregate._clean_cols(cols)
    assert isinstance(result, list)
    assert len(result) == 2

def test__clean_cols_w_no_cols():
    cols = ""
    result = Aggregate._clean_cols(cols)
    assert result == ""

def test__apply_filter(spark_session, aggregate_df):
    filter = "dvc_hostname == '68.180.228.229'"
    result = Aggregate()._apply_filter(aggregate_df, filter)
    assert isinstance(result, DataFrame)
    assert result.count() == 1

def test__apply_filter_is_empty_error(spark_session, aggregate_df):
    filter = "dvc_hostname >>> '68.180'"
    with pytest.raises(SirensAggregationException):
        result = Aggregate()._apply_filter(aggregate_df, filter)

def test__valid_configs(spark_session):
    aggregate_config = {
        "source_table": "aws_cloudtrail_silver",
        "filter": "dvc_hostname == 'some_value'",
        "groupby": "_event_time, eventName",
        "agg_columns": ["count(*)"],
        "window_duration": "10 minutes",
        "sliding_duration": "5 minutes",
        "wait_for_late_data": "1 hour",
        "start_time": "15 minutes",
        "save_mode": "append",
        "stream_type": "streaming"
    }
    df_cols = ["_event_time", "eventName"]
    is_streaming_df = False
    result = Aggregate._valid_configs(aggregate_config, df_cols, is_streaming_df)
    assert result is True

def test__valid_configs_invalid_keys():
    aggregate_config = {
        'stream_type': 'streaming',
        'invalid_key': 'invalid_value'
    }
    df_cols = []
    is_streaming_df = False
    result = Aggregate._valid_configs(aggregate_config, df_cols, is_streaming_df)
    assert result is False


def test_pipeline_aggregation_window_with_state(spark_session, aggregate_df):
    aggregate_config = {
        "groupby": "dvc_hostname",
        "agg_columns": "min(_event_time) AS earliest, max(_event_time) AS latest, count(*) AS count",
        "window_duration": "10 minutes",
        "sliding_duration": "5 minutes",
        "wait_for_late_data": "1 minute",
        "aggregation_name": "num_events_by_hostname",
        "save_mode": "append",
        "stream_type": "streaming"
    }

    expected_data = [
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 10:10:00", "2023-01-13 10:20:00", "num_events_by_hostname"),
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 10:15:00", "2023-01-13 10:25:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 10:10:00", "2023-01-13 10:20:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 10:15:00", "2023-01-13 10:25:00", "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "earliest", "latest", "count", "window_start", "window_end", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("window_start", expected_df['window_start'].cast(TimestampType()))
    expected_df = expected_df.withColumn("window_end", expected_df['window_end'].cast(TimestampType()))

    result, status = Aggregate().pipeline_aggregation(aggregate_df, aggregate_config)
    assert result.collect() == expected_df.collect()

def test_pipeline_aggregation_tumbling_window(spark_session, aggregate_df):
    aggregate_config = {
        "groupby": "dvc_hostname",
        "agg_columns": "min(_event_time) AS earliest, max(_event_time) AS latest, count(*) AS count",
        "window_duration": "30 minutes",
        "aggregation_name": "num_events_by_hostname",
        "save_mode": "append",
        "stream_type": "streaming"
    }
    expected_data = [
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 10:00:00", "2023-01-13 10:30:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 10:00:00", "2023-01-13 10:30:00", "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "earliest", "latest", "count", "window_start", "window_end", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("window_start", expected_df['window_start'].cast(TimestampType()))
    expected_df = expected_df.withColumn("window_end", expected_df['window_end'].cast(TimestampType()))

    result, status = Aggregate().pipeline_aggregation(aggregate_df, aggregate_config)

    assert result.collect() == expected_df.collect()

def test_pipeline_aggregation_sliding_window(spark_session, aggregate_df):
    aggregate_config = {
        "groupby": "dvc_hostname",
        "agg_columns": "min(_event_time) AS earliest, max(_event_time) AS latest, count(*) AS count",
        "window_duration": "1 hour",
        "sliding_duration": "30 minutes",
        "aggregation_name": "num_events_by_hostname",
        "save_mode": "append",
        "stream_type": "streaming"
    }

    expected_data = [
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 09:30:00", "2023-01-13 10:30:00", "num_events_by_hostname"),
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 10:00:00", "2023-01-13 11:00:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 09:30:00", "2023-01-13 10:30:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 10:00:00", "2023-01-13 11:00:00", "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "earliest", "latest", "count", "window_start", "window_end", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("window_start", expected_df['window_start'].cast(TimestampType()))
    expected_df = expected_df.withColumn("window_end", expected_df['window_end'].cast(TimestampType()))
    result, status = Aggregate().pipeline_aggregation(aggregate_df, aggregate_config)

    assert result.collect() == expected_df.collect()

def test_pipeline_aggregation_no_window(spark_session, aggregate_df):
    aggregate_config = {
        "groupby": "dvc_hostname",
        "aggregation_name": "num_events_by_hostname",
        "save_mode": "complete",
        "stream_type": "batch"
    }
    expected_data = [
        ("14.139.187.130", 3, "num_events_by_hostname"),
        ("68.180.228.229", 1, "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "count", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    result, status = Aggregate().pipeline_aggregation(aggregate_df, aggregate_config)

    assert result.collect() == expected_df.collect()
