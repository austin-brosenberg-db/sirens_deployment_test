import pytest

from pyspark.sql import DataFrame

from databricks.sirens.datasource import DataSource
#from pytest_mock import mocker

from databricks.sirens.exceptions import SirensConfigException


def test_datasource_no_config_file(mocker, spark_session):
    spark_session.conf.set('spark.databricks.workspaceUrl', 'fakeURL')

    dsobject = DataSource(spark_session, 'sirens_test', 'apache', 'access_combined')
    assert dsobject.databaseName == 'sirens_test'
    assert dsobject.targetDatabase == 'sirens_test'
    assert dsobject.source == 'apache'
    assert dsobject.sourcetype == 'access_combined'


def test_datasource_with_target_database(mocker, spark_session):
    spark_session.conf.set('spark.databricks.workspaceUrl', 'fakeURL')
    dsobject = DataSource(spark_session, 'sirens_test', 'apache', 'access_combined', target_database="silver_db")
    assert dsobject.databaseName == "sirens_test"
    assert dsobject.targetDatabase == "silver_db"


def test_datasource_read_config_file(mocker, spark_session):
    spark_session.conf.set('spark.databricks.workspaceUrl', 'fakeURL')

    dsobject = DataSource(spark_session, 'sirens_test', 'apache', 'access_combined')
    obj = dsobject.read()
    assert obj.get("input").get("sourcetype") == "access_combined"
