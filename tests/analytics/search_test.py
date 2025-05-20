from pyspark.sql.functions import col
from databricks.sirens.analytics.search import ioc_hits, ioc_extract, first_time_seen, time_series_spike

def test_ioc_hits(sample_ioc_dataframe):
    df = ioc_hits(sample_ioc_dataframe, col("dest_ip"))
    assert df is not None

def test_ioc_extract(sample_ioc_dataframe, ):
    df = ioc_extract(sample_ioc_dataframe, search_cols="dest_ip")
    assert not df.isEmpty()