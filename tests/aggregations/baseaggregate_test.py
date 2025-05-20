from databricks.sirens.internal.baseaggregate import BaseAggregator
from pyspark.sql.functions import expr, col
from pyspark.sql.types import TimestampType
from typing import Any
import pytest

@pytest.mark.usefixtures("spark_session", "aggregate_df")
def test_collect_stats(spark_session: Any, aggregate_df: Any):
    groupby = ["dvc_hostname"]
    aggs = [expr("count(*) AS count")]
    name = "num_events_by_hostname"
    expected_data = [
        ("14.139.187.130", 3, "num_events_by_hostname"),
        ("68.180.228.229", 1, "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "count", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    result = BaseAggregator.collect_stats(aggregate_df, groupby, aggs, name)
    assert result.collect() == expected_df.collect()

@pytest.mark.usefixtures("spark_session", "aggregate_df")
def test_collect_stats_multi_aggs(spark_session: Any, aggregate_df: Any):
    groupby = ["dvc_hostname"]
    aggs = ["min(_event_time) AS earliest", "max(_event_time) AS latest", "count(*) AS count"]
    aggs = [expr(x) for x in aggs]
    name = "num_events_by_hostname"
    expected_data = [
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "earliest", "latest", "count", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    result = BaseAggregator.collect_stats(aggregate_df, groupby, aggs, name)
    assert result.collect() == expected_df.collect()

@pytest.mark.usefixtures("spark_session", "aggregate_df")
def test_collect_stats_over_window_with_watermarking(spark_session: Any, aggregate_df: Any):
    groupby = ["dvc_hostname"]
    aggs = ["min(_event_time) AS earliest", "max(_event_time) AS latest", "count(*) AS count"]
    aggs = [expr(x) for x in aggs]
    name = "num_events_by_hostname"
    window_duration = "1 hour"
    wait_for_late_data = "1 minute"

    expected_data = [
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 10:00:00", "2023-01-13 11:00:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 10:00:00", "2023-01-13 11:00:00", "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "earliest", "latest", "count", "window_start", "window_end", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("window_start", expected_df['window_start'].cast(TimestampType()))
    expected_df = expected_df.withColumn("window_end", expected_df['window_end'].cast(TimestampType()))

    result = BaseAggregator.collect_stats_over_window_with_watermarking(aggregate_df, groupby, aggs,
                                                                        window_duration, wait_for_late_data, agg_name=name)

    assert result.collect() == expected_df.collect()

@pytest.mark.usefixtures("spark_session", "aggregate_df")
def test_collect_stats_over_window_with_watermarking_w_slide_window(spark_session: Any, aggregate_df: Any):
    groupby = ["dvc_hostname"]
    aggs = ["min(_event_time) AS earliest", "max(_event_time) AS latest", "count(*) AS count"]
    aggs = [expr(x) for x in aggs]
    name = "num_events_by_hostname"
    window_duration = "1 hour"
    wait_for_late_data = "1 minute"
    sliding_window = "30 minutes"

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

    result = BaseAggregator.collect_stats_over_window_with_watermarking(aggregate_df, groupby, aggs,
                                                                        window_duration, wait_for_late_data,
                                                                        slide_duration=sliding_window, agg_name=name)

    assert result.collect() == expected_df.collect()

@pytest.mark.usefixtures("spark_session", "aggregate_df")
def test_collect_stats_over_tumbling_window(spark_session: Any, aggregate_df: Any):
    groupby = ["dvc_hostname"]
    aggs = ["min(_event_time) AS earliest", "max(_event_time) AS latest", "count(*) AS count"]
    aggs = [expr(x) for x in aggs]
    name = "num_events_by_hostname"
    window_duration = "30 minutes"

    expected_data = [
        ("14.139.187.130", "2023-01-13T10:16:51.000+0000", "2023-01-13T10:17:56.000+0000", 3, "2023-01-13 10:00:00", "2023-01-13 10:30:00", "num_events_by_hostname"),
        ("68.180.228.229", "2023-01-13T10:18:59.000+0000", "2023-01-13T10:18:59.000+0000", 1, "2023-01-13 10:00:00", "2023-01-13 10:30:00", "num_events_by_hostname")
    ]
    expected_schema = ["dvc_hostname", "earliest", "latest", "count", "window_start", "window_end", "aggregation_name"]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("window_start", expected_df['window_start'].cast(TimestampType()))
    expected_df = expected_df.withColumn("window_end", expected_df['window_end'].cast(TimestampType()))

    result = BaseAggregator.collect_stats_over_tumbling_window(aggregate_df, groupby, aggs,
                                                               window_duration, agg_name=name)
    assert result.collect() == expected_df.collect()

@pytest.mark.usefixtures("spark_session", "aggregate_df")
def test_collect_stats_over_sliding_window(spark_session, aggregate_df):
    groupby = ["dvc_hostname"]
    aggs = ["min(_event_time) AS earliest", "max(_event_time) AS latest", "count(*) AS count"]
    aggs = [expr(x) for x in aggs]
    name = "num_events_by_hostname"
    window_duration = "1 hour"
    slide_duration = "30 minutes"

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

    result = BaseAggregator.collect_stats_over_sliding_window(aggregate_df, groupby, aggs,
                                                              window_duration, slide_duration, agg_name=name)
    assert result.collect() == expected_df.collect()
