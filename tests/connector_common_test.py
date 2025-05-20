from databricks.sirens.connectors.common import DriverNode, Connector
import os

from databricks.sirens.datasource import DataSource


def test_get_temp_files():
    temp_dir, temp_file = DriverNode.get_temp_files('okta', 'oktaIM2_log', '12345-12345')
    assert temp_dir == '/tmp/okta/oktaIM2_log/12345-12345'
    assert temp_file == '/tmp/okta/oktaIM2_log/12345-12345/api_events.json'

def test_get_dbfs_files():
    dbfs_dir, dbfs_file = DriverNode.get_dbfs_files('FileStore/sirens', 'okta', 'oktaIM2_log', '2023-01-17T10:00:00.000')
    assert dbfs_dir == '/dbfs/FileStore/sirens/connectors/okta/oktaIM2_log/runs/2023-01-17T10:00:00.000'
    assert dbfs_file == '/dbfs/FileStore/sirens/connectors/okta/oktaIM2_log/runs/2023-01-17T10:00:00.000/api_events.json'

def test_get_dbfs_files_w_slash():
    t_now = '2023-01-17T10:00:00.000'
    source = 'okta'
    sourcetype = 'oktaIM2_log'
    scratch_dir = '/tmp/sirens/'
    dbfs_dir, dbfs_file = DriverNode.get_dbfs_files(scratch_dir, source, sourcetype, t_now)
    assert dbfs_dir == '/dbfs/tmp/sirens/connectors/okta/oktaIM2_log/runs/2023-01-17T10:00:00.000'
    assert dbfs_file == '/dbfs/tmp/sirens/connectors/okta/oktaIM2_log/runs/2023-01-17T10:00:00.000/api_events.json'

def test_Connector_get_dbfs_status_dir(mocker, spark_session):
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    dbfs_dir, dbfs_file = Connector(spark_session, dataSourceObj)._get_dbfs_status_dir('FileStore/sirens', 'okta', 'oktaIM2_log')
    assert dbfs_dir == '/dbfs/FileStore/sirens/connectors/okta/oktaIM2_log'
    assert dbfs_file == '/dbfs/FileStore/sirens/connectors/okta/oktaIM2_log/status.json'

def test__Connector_read_pos(mocker, spark_session, connector_status):
    mocker.patch("databricks.sirens.connectors.common.Connector._is_file", return_value=True)
    mocker.patch("databricks.sirens.connectors.common.Connector._read_file", return_value=connector_status)
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    ConnectorObj = Connector(spark_session, dataSourceObj)
    result = ConnectorObj.read()
    assert isinstance(result, dict)
    assert result.get("pipeline_run_id") == "12345-12345-12345"

def test__Connector_write_pos(spark_session, mocker):
    mocker.patch("databricks.sirens.connectors.common.Connector._write_file", return_value=True)
    mocker.patch("databricks.sirens.connectors.common.Connector._mkdir")
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    status_info = {
        "raw_cursor": "https://okta/info?since='2017-12-01T10:00:00'",
        "pipeline_run_id": "12345-12345-12345",
        "last_collection": "2017-12-01T10:00:00"
    }
    ConnectorObj = Connector(spark_session, dataSourceObj)
    result = ConnectorObj.write(status_info)
    assert result is True

def test__is_file_neg(mocker, spark_session):
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    fake_path = '/dbfs/non-existent/path/to_file.json'
    result = Connector(spark_session, dataSourceObj)._is_file(fake_path)
    assert result is False

def test__is_file_pos(mocker, spark_session):
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    path = 'databricks/sirens/config_reader.py'
    result = Connector(spark_session, dataSourceObj)._is_file(path)
    assert result is True

def test__mkdir(spark_session, mocker):
    mocker.patch("pathlib.Path.mkdir", return_value=True)
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    result = Connector(spark_session, dataSourceObj)._mkdir('tmp/something')
    assert result is True

def test_Connector__read_file(mocker, spark_session):
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    file = f"{os.curdir}/tests/samples/json_files/connector_status.json"
    result = Connector(spark_session, dataSourceObj)._read_file(file)
    assert isinstance(result, dict)
    assert result.get('pipeline_run_id') == "12345-12345-12345"

def test_ConnectorStatus_write(spark_session, mocker):
    mocker.patch("databricks.sirens.connectors.common.Connector._write_file", return_value=True)
    mocker.patch("databricks.sirens.connectors.common.Connector._mkdir")
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    status_info = {
        "raw_cursor": "https://okta/info?since='2017-12-01T10:00:00'",
        "pipeline_run_id": "12345-12345-12345",
        "last_collection": "2017-12-01T10:00:00"
    }
    result = Connector(spark_session, dataSourceObj).write(status_info)
    assert result is True

def test_ConnectorStatus_write_file_neg(spark_session, mocker):
    mocker.patch("databricks.sirens.connectors.common.Connector._write_file", return_value=False)
    mocker.patch("databricks.sirens.connectors.common.Connector._mkdir")
    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    dataSourceObj.read()
    status_info = {
        "raw_cursor": "https://okta/info?since='2017-12-01T10:00:00'",
        "pipeline_run_id": "12345-12345-12345",
        "last_collection": "2017-12-01T10:00:00"
    }
    result = Connector(spark_session, dataSourceObj).write(status_info)
    assert result is False
