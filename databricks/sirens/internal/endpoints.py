"""
Creates the Databricks REST API endpoints.

Returns the endpoint as a string

Authors:
    Derek King (2023-01-11)

Classes:
    Clusters
    DBFS

Methods:
    _make_endpoint()

TODO: Add more endpoints for auto deploy.

"""
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

def _make_endpoint(api: str, endpoint: str = None) -> str:
    """create the restAPI endpoint url string

    :param api: API major
    :type api: str
    :param endpoint: endpoint string, defaults to None
    :type endpoint: str, optional
    :return: /api/2.0/clusters/list (example)
    :rtype: str
    """
    if endpoint and '/' not in api[-1]:
        api = ''.join((api, '/'))
    if endpoint:
        return(RestAPI.api_prefix + RestAPI.api_version + api + endpoint)
    else:
        return(RestAPI.api_prefix + RestAPI.api_version + api)

class RestAPI:
    api_prefix = '/api'
    api_version = '/2.0/'

class Account(RestAPI):
    pass

class Clusters(RestAPI):
    list = _make_endpoint('clusters', 'list')
    list_node_types = _make_endpoint('clusters', 'list-node-types')
    events = _make_endpoint('clusters', 'events')

class DBSQL(RestAPI):
    list = _make_endpoint('sql', 'warehouses')

class DBFS(RestAPI):
    add_block = _make_endpoint('dbfs', 'add-block')
    close = _make_endpoint('dbfs', 'close')
    create = _make_endpoint('dbfs', 'create')
    delete = _make_endpoint('dbfs', 'delete')
    get_status = _make_endpoint('dbfs', 'get-status')
    list = _make_endpoint('dbfs', 'list')
    mkdirs = _make_endpoint('dbfs', 'mkdirs')
    move = _make_endpoint('dbfs', 'move')
    put = _make_endpoint('dbfs', 'put')
    read = _make_endpoint('dbfs', 'read')

class InstanceProfile(RestAPI):
    list = _make_endpoint('instance-profiles', 'list')
