import pytest
from databricks.sirens.internal.environment import Environment, EnvironmentCheck
from databricks.sirens.exceptions import SirensEnvironmentException

def test_Environment(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection")
    mocker.patch("databricks.sirens.internal.environment.Introspection._get_dbutils")
    result = Environment()
    assert isinstance(result, object)

def test_Environment_w_args_1(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection")
    mocker.patch("databricks.sirens.internal.environment.Introspection._get_dbutils")
    result = Environment(workspace_url='workspace.cloud.databricks.com')
    assert result.workspace_url == 'workspace.cloud.databricks.com'

def test_compute():
    in_dict = {"spark.app.name": "databricks-sirens"}
    result = EnvironmentCheck.compute(in_dict)
    assert result is True

def test_spark_version_pos():
    spark_version = "11.2.x-scala-3.1.1"
    result = EnvironmentCheck.spark_version(spark_version)
    assert result is True

def test_spark_version_neg():
    spark_version = "10.0.x-scala-3.1.1"
    with pytest.raises(SirensEnvironmentException):
        result = EnvironmentCheck.spark_version(spark_version)
        assert result is False

def test_dbfs_status_pos():
    in_dict = {"files": [{"is_dir": "true"}]}
    result = EnvironmentCheck.dbfs_status(in_dict)
    assert result is True

def test_dbfs_status_neg():
    in_dict = {"files": [{"dir": "true"}]}
    result = EnvironmentCheck.dbfs_status(in_dict)
    assert result is False
