from databricks.sirens.internal import endpoints

def test__make_endpoint_1():
    api = 'cluster'
    endpoint = 'list'
    result = endpoints._make_endpoint(api, endpoint)
    assert result == '/api/2.0/cluster/list'

def test__make_endpoint_2():
    api = 'cluster'
    result = endpoints._make_endpoint(api)
    assert result == '/api/2.0/cluster'

def test_RestAPI():
    result = endpoints.RestAPI()
    assert result.api_version == '/2.0/'

def test_Account():
    result = endpoints.Account()
    assert isinstance(result, object)

def test_Clusters():
    result = endpoints.Clusters()
    assert result.list == '/api/2.0/clusters/list'
    assert result.list_node_types == '/api/2.0/clusters/list-node-types'
    assert result.events == '/api/2.0/clusters/events'

def test_DBFS():
    result = endpoints.DBFS()
    assert result.add_block == '/api/2.0/dbfs/add-block'
    assert result.close == '/api/2.0/dbfs/close'
    assert result.create == '/api/2.0/dbfs/create'
    assert result.delete == '/api/2.0/dbfs/delete'
    assert result.get_status == '/api/2.0/dbfs/get-status'
    assert result.list == '/api/2.0/dbfs/list'
    assert result.mkdirs == '/api/2.0/dbfs/mkdirs'
    assert result.move == '/api/2.0/dbfs/move'
    assert result.put == '/api/2.0/dbfs/put'
    assert result.read == '/api/2.0/dbfs/read'
