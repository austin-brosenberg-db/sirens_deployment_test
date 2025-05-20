from databricks.sirens.analytics.transpilers import Splunk
import pytest

def test_eval(sample_splunk_test_dataframe):
    df = Splunk.eval(sample_splunk_test_dataframe, "age_category", "CASE WHEN age > 40 THEN 'Senior' ELSE 'Junior' END")
    assert "age_category" in df.columns
    assert df.filter(df.age_category == "Senior").count() == 1
    assert df.filter(df.age_category == "Junior").count() == 2

def test_stats(sample_splunk_test_dataframe):
    df = Splunk.stats(sample_splunk_test_dataframe, ["avg(age) as avg_age"], ["name"])
    assert "avg_age" in df.columns

def test_eventstats(sample_splunk_test_dataframe):
    df = Splunk.eventstats(sample_splunk_test_dataframe, ["avg(age) as avg_age"], ["name"])
    assert "avg_age" in df.columns

def test_streamstats(sample_splunk_test_dataframe):
    df = Splunk.streamstats(sample_splunk_test_dataframe, ["avg(age) as avg_age"], ["name"], "_event_time")
    assert "avg_age" in df.columns

def test_outputlookup(sample_splunk_test_dataframe, ct):
    schema = 'test_schema_' + ct
    result = Splunk.outputlookup(sample_splunk_test_dataframe, schema, append=False)
    assert result is None

def test_inputlookup(spark_session, ct):
    schema = 'test_schema_' + ct
    df = Splunk.inputlookup(schema)
    assert df is not None

def test_bucket(sample_splunk_test_dataframe):
    df = Splunk.bucket(sample_splunk_test_dataframe, "10 minutes", "_event_time")
    assert "window" in df.columns

def test_sort(sample_splunk_test_dataframe):
    df = Splunk.sort(sample_splunk_test_dataframe, ["age"], "asc")
    assert df.collect()[0]["age"] == 29

def test_where(sample_splunk_test_dataframe):
    df = Splunk.where(sample_splunk_test_dataframe, "age > 30")
    assert df.count() == 2

def test_table(sample_splunk_test_dataframe):
    df = Splunk.table(sample_splunk_test_dataframe, ["name"])
    assert "name" in df.columns
    assert "age" not in df.columns