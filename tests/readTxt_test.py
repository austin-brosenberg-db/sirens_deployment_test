import pytest
from textwrap import fill
from pyspark.sql import DataFrame

from databricks.sirens.connectors.readTxt import Reader
from databricks.sirens.datasource import DataSource


@pytest.mark.usefixtures("spark_session")
def test_read_txt(spark_session, mocker):
    #  mocker.patch('connectors.readTxt.Reader._get_connector_path', return_value='samples/access_combined.txt')
    #  mocker.patch('connectors.readTxt.Reader._get_connector_opts', return_value=None)
    spark_session.conf.set('spark.databricks.workspaceUrl', 'fakeURL')
    dsobject = DataSource(spark_session, 'sirens_test', 'apache', 'access_combined')
    obj = dsobject.read()
    df = Reader(spark_session, dsobject).read()
    assert "value" in df.columns
    assert isinstance(df, DataFrame)
