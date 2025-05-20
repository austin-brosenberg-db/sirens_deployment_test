from databricks.sirens.internal.introspection import Introspection


def test_introspection_w_url(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    introspection_obj = Introspection(workspace_url='company.cloud.databricks.com')
    assert isinstance(introspection_obj, object)


def test_introspection_wo_url(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    introspection_obj = Introspection()
    assert isinstance(introspection_obj, object)


def test_introspection_w_token(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    introspection_obj = Introspection(token="xxxx")
    assert isinstance(introspection_obj, object)


def test_introspection_all(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    introspection_obj = Introspection(workspace_url="xxxx", token="xxxx")
    assert isinstance(introspection_obj, object)


def test_introspection(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    introspection_obj = Introspection()
    assert introspection_obj.app_name == 'databricks-sirens'


def test__make_workspace_url_pos(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    url = 'workspace.cloud.databricks.com'
    result = Introspection()._make_workspace_url(url)
    assert result == 'https://workspace.cloud.databricks.com'


def test__get_dbutils(spark_session, mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    result = Introspection()._get_dbutils(spark_session)
    assert isinstance(result, object)


def test__get_notebook_token(spark_session, mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    dbutils = Introspection()._get_dbutils(spark_session)
    result = Introspection()._get_notebook_token(dbutils)
    assert result is None


def test__get_spark_conf(mocker):
    mocker.patch("databricks.sirens.internal.introspection.Introspection._get_dbutils")
    result = Introspection()._get_spark_conf()
    assert 'spark.app.name' in result.keys()
