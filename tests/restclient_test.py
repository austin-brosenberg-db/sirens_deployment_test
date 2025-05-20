from databricks.sirens.internal.restclient import DatabricksAPI

def test_DatabricksAPI():
    result = DatabricksAPI(app_name='test-sirens', workspace_url='workspace.cloud.databricks.com', token='xxx')
    assert isinstance(result, object)
    assert result.app_name == 'test-sirens'

def test_clusters_list(mocker):
    mocker.patch("databricks.sirens.internal.restclient.DatabricksAPI.api_call", return_value=200)
    APIObj = DatabricksAPI(app_name='test-sirens', workspace_url='workspace.cloud.databricks.com', token='xxx')
    result = APIObj.clusters_list()
    assert result == 200

def test_dbfs_list(mocker):
    mocker.patch("databricks.sirens.internal.restclient.DatabricksAPI.api_call", return_value=200)
    APIObj = DatabricksAPI(app_name='test-sirens', workspace_url='workspace.cloud.databricks.com', token='xxx')
    result = APIObj.dbfs_list(path="/tmp")
    assert result == 200

def test_api_call_get_pos(requests_mock):
    requests_mock.get('https://workspace.cloud.databricks.com/api/2.0/clusters/list', json={'name': 'something'})
    APIObj = DatabricksAPI(app_name='test-sirens', workspace_url='https://workspace.cloud.databricks.com/api', token='xxx')
    result = APIObj.api_call('GET', '/2.0/clusters/list')
    assert result == {'name': 'something'}

def test_api_call_post_pos(requests_mock):
    requests_mock.post('https://workspace.cloud.databricks.com/api/2.0/clusters/list', json={'name': 'something'})
    APIObj = DatabricksAPI(app_name='test-sirens', workspace_url='https://workspace.cloud.databricks.com/api', token='xxx')
    result = APIObj.api_call('POST', '/2.0/clusters/list')
    assert result == {'name': 'something'}

