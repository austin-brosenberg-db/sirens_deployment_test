import pytest
import os
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingException, SirensConfigException
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.utils.global_config import ConfigReader
from databricks.sirens.utils.path_utils import PathUtils
from databricks.sirens.utils.config_utils import config_utils


# TODO - fill out test cases

@pytest.mark.skip(reason='todo')
def test___get_fields_info___neg():
    assert True


@pytest.mark.skip(reason='todo')
def test___get_fields_info___pos():
    assert True


@pytest.mark.skip(reason='todo')
def test___normalise_fieldname___neg():
    assert True


@pytest.mark.skip(reason='todo')
def test___normalise_fieldname___pos():
    assert True


@pytest.mark.skip(reason='todo')
def test___rename_nested_field___neg():
    assert True


@pytest.mark.skip(reason='todo')
def test___rename_nested_field___pos():
    assert True


def test__get_default_workspace_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_default_workspace()
    assert isinstance(result, str)


def test__is_input_host_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_input_host()
    assert result is None


def test__is_rawpath_segment_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_raw_path_segment()
    assert result is None


def test__is_regex_path_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_regex_path()
    assert isinstance(result, tuple)
    assert result[0] == 'value', '^(?<host>[^ ]*)'


def test__make_schema_file_path_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.make_schema_file_path("inputs.yaml")
    assert 'log_sources/apache/access_combined/inputs.yaml' in result


@pytest.mark.skip(reason="Not testable today")
def test_normalise_fields_names_pos():
    assert True


@pytest.mark.skip(reason="Not testable today")
def test_parse_time_neg():
    assert True


@pytest.mark.skip(reason='wierd infer schema error')
def test__create_empty_dataframe_neg(spark_session, dataframe_simple_schema):
    df = BaseUtils._create_empty_dataframe(spark_session, dataframe_simple_schema)
    assert True


@pytest.mark.skip(reason='wierd infer schema error')
def test__create_empty_dataframe_pos(spark_session, dataframe_simple_schema):
    df = BaseUtils._create_empty_dataframe(spark_session, dataframe_simple_schema)
    assert type(df) is DataFrame


def test__create_table_neg(mocker, spark_session):
    mocker.patch("databricks.sirens.utils.base_utils.SqlQuery.create_table", return_value=False)
    assert BaseUtils._create_table(spark_session, 'sirens', 'web') is False


def test__create_table_pos(mocker, spark_session):
    mocker.patch("databricks.sirens.utils.base_utils.SqlQuery.create_table", return_value=True)
    assert BaseUtils._create_table(spark_session, 'sirens', 'web') is True


def test__get_configs_default_dir_neg(global_config):
    global_config["global"].pop("conf_dir")
    result = GlobalConfig.get_configs_default_dir(global_config)
    assert result == "conf"


def test__get_configs_default_dir_pos(global_config):
    old_value = GlobalConfig.get_configs_default_dir(global_config)
    global_config['global']['conf_dir'] = 'test'
    result = GlobalConfig.get_configs_default_dir(global_config)
    global_config['global']['conf_dir'] = old_value
    assert result == "test"


def test__get_deploy_dir_neg(global_config):
    global_config["deploy"].pop("deploy_dir")
    result = GlobalConfig.get_deploy_dir(global_config)
    assert result == "deploy"


def test__get_groupby_pos(global_config):
    old_value = GlobalConfig.get_groupby(global_config)
    global_config['deploy']['groupby'] = 'test'
    result = GlobalConfig.get_groupby(global_config)
    global_config['deploy']['groupby'] = old_value
    assert result == "test"


def test__get_groupby_neg(global_config):
    global_config["deploy"].pop("groupby")
    result = GlobalConfig.get_groupby(global_config)
    assert result == "source"


def test__get_notebook_language_pos(global_config):
    global_config['default']['notebook_language'] = 'test'
    result = GlobalConfig.get_notebook_language(global_config)
    assert result == "test"


def test__get_notebook_language_neg(global_config):
    global_config["default"].pop("notebook_language")
    result = GlobalConfig.get_notebook_language(global_config)
    assert result == "python"


def test__get_target_database_pos(global_config):
    old_value = GlobalConfig.get_target_database(global_config)
    global_config['default']['target_database'] = 'test'
    result = GlobalConfig.get_target_database(global_config)
    global_config['default']['target_database'] = old_value
    assert result == "test"


def test__get_target_database_neg(global_config):
    global_config["default"].pop("target_database")
    result = GlobalConfig.get_target_database(global_config)
    assert result == "sirens"


def test__get_dbfs_upload_dir_pos(global_config):
    old_value = GlobalConfig.get_dbfs_upload_dir(global_config)
    global_config['deploy']['dbfs_upload_dir'] = 'test'
    result = GlobalConfig.get_dbfs_upload_dir(global_config)
    global_config['deploy']['dbfs_upload_dir'] = old_value
    assert result == "test"


def test__get_dbfs_upload_dir_neg(global_config):
    global_config["deploy"].pop("dbfs_upload_dir")
    result = GlobalConfig.get_dbfs_upload_dir(global_config)
    assert result == "/FileStore/sirens"


def test__get_dashboards_dir_pos(global_config):
    old_value = GlobalConfig.get_dashboards_dir(global_config)
    global_config['deploy']['dashboard_dir'] = 'test'
    result = GlobalConfig.get_dashboards_dir(global_config)
    global_config['deploy']['dashboard_dir'] = old_value
    assert result == "test"


def test__get_dashboards_dir_neg(global_config):
    global_config["deploy"].pop("dashboard_dir")
    result = GlobalConfig.get_dashboards_dir(global_config)
    assert result == "sirens_dashboards"


def test__get_warehouse_id_pos(global_config):
    global_config['deploy']['sql_warehouse_id'] = 'test'
    result = GlobalConfig.get_warehouse_id(global_config)
    assert result == "test"


def test__get_warehouse_id_neg(global_config):
    global_config["deploy"].pop("sql_warehouse_id")
    result = GlobalConfig.get_warehouse_id(global_config)
    assert result is None


def test__get_deploy_dir_pos(global_config):
    old_value = GlobalConfig.get_deploy_dir(global_config)
    global_config['deploy']['deploy_dir'] = 'test'
    result = GlobalConfig.get_deploy_dir(global_config)
    global_config['deploy']['deploy_dir'] = old_value
    assert result == "test"


def test__get_connector_name_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"]["connector"]["name"] = "json"
    result = data_source_obj.get_connector_name()
    assert result == "json"


def test__get_connector_opts_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"]["connector"]["options"] = {"opt1": "val1", "opt2": "val2"}
    result = data_source_obj.get_connector_opts()
    assert isinstance(result, dict)
    assert result == {"opt1": "val1", "opt2": "val2"}


def test__get_connector_path_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"]["rawPath"] = "s3://file1"
    result = data_source_obj.get_connector_path()
    assert isinstance(result, str)
    assert result == "s3://file1"


def test__get_default_host_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config['input']['host'] = "failedhost"
    host = data_source_obj.get_default_host()
    assert host != "somehost"


def test__get_default_host_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_default_host()
    assert result is not None


def test__get_detection_default_dir_neg(global_config):
    global_config['global'].pop('detection_dir')
    result = GlobalConfig.get_detection_default_dir(global_config)
    assert result == 'detections'


def test__get_detection_default_dir_pos(global_config):
    old_value = GlobalConfig.get_detection_default_dir(global_config)
    global_config['global']['detection_dir'] = 'detections'
    result = GlobalConfig.get_detection_default_dir(global_config)
    global_config['global']['detection_dir'] = old_value
    assert result == "detections"


def test__get_detection_deploy_dir_neg(global_config):
    global_config['detections'].pop('deploy_dir')
    result = GlobalConfig.get_detection_deploy_dir(global_config)
    assert result == 'deploy/detections'


def test__get_detection_deploy_dir_pos(global_config):
    old_value = GlobalConfig.get_detection_deploy_dir(global_config)
    global_config['detections']['deploy_dir'] = 'detections'
    result = GlobalConfig.get_detection_deploy_dir(global_config)
    global_config['detections']['deploy_dir'] = old_value
    assert result == "detections"
    assert True


def test__get_global_database_neg(mocker, spark_session):
    result = GlobalConfig.get_global_database(GlobalConfig.get())
    assert result == 'sirens'


def test__get_input_config_dir_pos(global_config):
    old_value = GlobalConfig.get_input_config_dir(global_config)
    global_config['global']['input_config_dir'] = 'sirens'
    result = GlobalConfig.get_input_config_dir(global_config)
    global_config['global']['input_config_dir'] = old_value
    assert result == 'sirens'


def test__get_input_config_dir_neg(global_config):
    global_config['global'].pop('input_config_dir')
    result = GlobalConfig.get_input_config_dir(global_config)
    assert result == 'log_sources'


def test__get_global_database_pos(mocker, spark_session):
    gc = GlobalConfig.get()
    old_value = GlobalConfig.get_global_database(gc)
    gc['default']['target_database'] = 'unittest_sirens'
    result = GlobalConfig.get_global_database(gc)
    gc['default']['target_database'] = old_value
    assert result == 'unittest_sirens'


def test__get_global_schemas(mocker, spark_session):
    gc = GlobalConfig.get()
    schemas = GlobalConfig.get_global_schemas(gc)
    assert gc['schemas']['bronze'] == schemas['bronze']
    assert gc['schemas']['silver'] == schemas['silver']
    assert gc['schemas']['normalized'] == schemas['normalized']


def test__get_global_scratch_dir_neg(mocker, spark_session):
    gc = GlobalConfig.get()
    gc.remove_option('default', 'scratch_dir')
    result = GlobalConfig.get_global_scratch_dir(gc)
    assert result == '/FileStore/sirens'


def test__get_global_scratch_dir_pos(mocker, spark_session):
    gc = GlobalConfig.get()
    old_value = GlobalConfig.get_global_scratch_dir(gc)
    gc['default']['scratch_dir'] = 'FileStore/sirens'
    result = GlobalConfig.get_global_scratch_dir(gc)
    gc['default']['scratch_dir'] = old_value
    assert result == 'FileStore/sirens'


def test__get_global_sirens_lib_default(global_config):
    result = GlobalConfig.get_global_sirens_lib(global_config)
    assert result == 'latest'


def test__get_global_sirens_lib_pos(global_config):
    old_value = GlobalConfig.get_global_sirens_lib(global_config)
    global_config['default']['sirens_lib'] = "python.whl"
    result = GlobalConfig.get_global_sirens_lib(global_config)
    global_config['default']['sirens_lib'] = old_value
    assert result == 'python.whl'


def test__get_local_file_path_neg():
    with pytest.raises(SirensConfigException, match=r"rawPath is not 'LocalPath://' as expected.  is:.*$") as exc:
        path = "local://somepath"
        BaseUtils._get_local_file_path(path)
    assert exc.type is SirensConfigException

    with pytest.raises(SirensConfigException, match='Cannot find a directory.*$') as exc:
        path = "LocalPath://somepath"
        BaseUtils._get_local_file_path(path)
    assert exc.type is SirensConfigException


@pytest.mark.skip(reason='todo')
def test__get_local_file_path_pos():
    assert True


def test__get_notebook_type_neg():
    global_config_obj = {"default": {"non-existant": "doesntexist"}}
    GlobalConfig.get_notebook_type(global_config_obj)
    assert GlobalConfig.get_notebook_type(global_config_obj) is None


def test__get_notebook_type_pos(global_config_obj):
    assert GlobalConfig.get_notebook_type(global_config_obj) == "delta"


def test__get_relative_file_paths_neg():
    with pytest.raises(SirensParsingException) as exc:
        paths = PathUtils.get_relative_file_paths('log')
    assert exc.type is SirensParsingException


def test__get_relative_file_paths_pos():
    paths = PathUtils.get_relative_file_paths(['log_sources', 'aws'])
    assert isinstance(paths, list) is True
    assert '../log_sources/aws' in paths

def test__get_relative_file_paths_single_path():
    PathUtils.custom_dirs = ["log_sources", "conf"]
    paths = PathUtils.get_relative_file_paths(["log_sources/microsoft/wineventssecurity/inputs.yaml"])
    assert isinstance(paths, list) is True
    assert '../log_sources/microsoft/wineventssecurity/inputs.yaml' in paths
    assert '../log_sources/microsoft/wineventssecurity/custom/inputs.yaml' in paths
    assert '../log_sources/microsoft/wineventssecurity/default/inputs.yaml' in paths
    assert (paths.index('../log_sources/microsoft/wineventssecurity/custom/inputs.yaml') 
        < paths.index('../log_sources/microsoft/wineventssecurity/default/inputs.yaml')
        < paths.index('../log_sources/microsoft/wineventssecurity/inputs.yaml'))
    
def test__get_relative_file_paths_single_path_conf():
    PathUtils.custom_dirs = ["log_sources", "conf"]
    paths = PathUtils.get_relative_file_paths(["conf/enrichments/enrichments.yaml"])
    assert isinstance(paths, list) is True
    assert '../conf/enrichments/enrichments.yaml' in paths
    assert '../conf/enrichments/custom/enrichments.yaml' in paths
    assert '../conf/enrichments/default/enrichments.yaml' in paths

    paths = PathUtils.get_relative_file_paths(["conf/actions/slack/action.yaml"])
    assert isinstance(paths, list) is True
    assert '../conf/actions/slack/action.yaml' in paths
    assert '../conf/actions/slack/custom/action.yaml' in paths
    assert '../conf/actions/slack/default/action.yaml' in paths

def test__get_event_fields_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_event_fields('eb')
    assert result is None


def test__get_event_fields_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_event_fields('web')
    assert isinstance(result, list)
    assert len(result) > 0


def test__get_event_filter_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_event_filter('eb')
    assert result is None


def test__get_event_filter_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_event_filter('web')
    print(result)
    assert result == '_sourcetype == "access_combined"'


def test__get_schema_file_opt_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"].pop("rawSchemaFile", None)
    result = data_source_obj.get_schema_file_opt()
    assert result is None


def test__get_schema_file_opt_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"]["rawSchemaFile"] = "schema.json"
    result = data_source_obj.get_schema_file_opt()
    assert result == "schema.json"


def test__get_schema_hints_opt_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"].pop("rawSchemaHintsFile", None)
    result = data_source_obj.get_schema_hints_opt()
    assert result is None


def test__get_schema_hints_opt_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"]["rawSchemaHintsFile"] = "schemahints.json"
    result = data_source_obj.get_schema_hints_opt()
    assert result == "schemahints.json"


def test__get_source_sourcetype_info_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    result = data_source_obj.get_source_sourcetype_info()
    assert result is not None


def test__get_stream_mode_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"].pop("streamType", None)
    result = data_source_obj.get_stream_mode()
    assert result is None


def test__get_stream_mode_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["input"]["streamType"] = 'batch'
    result = data_source_obj.get_stream_mode()
    assert result == 'batch'


@pytest.mark.skip(reason="Not testable today")
def test__get_table_schema_pos(mocker, spark_session):
    # need to figure how to mock dataframe
    assert True


def test__get_template_dir_neg(global_config):
    global_config["global"].pop("template_dir", None)
    result = GlobalConfig.get_template_dir(global_config)
    assert result == 'templates'


def test__get_template_dir_pos(global_config):
    global_config["global"]["template_dir"] = 'templates'
    result = GlobalConfig.get_template_dir(global_config)
    assert result == 'templates'


def test__get_timestamp_info_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    with pytest.raises(SirensConfigException,
                       match="timestamp_column key not defined in transforms->bronze-meta - Please correct.") as exc:
        data_source_obj.data_source_config["transforms"]["bronze"]["meta"].pop("timestamp_column")
        result = data_source_obj.get_timestamp_info()
    assert exc.type is SirensConfigException


def test__get_timestamp_info_pos(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    data_source_obj.data_source_config["transforms"]["bronze"]["meta"]["timestamp_column"] = "timestamp"
    data_source_obj.data_source_config["transforms"]["bronze"]["meta"]["timestamp_format"] = None
    data_source_obj.data_source_config["transforms"]["bronze"]["meta"]["timestamp_regex"] = None
    timestamp_info = data_source_obj.get_timestamp_info()
    assert timestamp_info.timestamp_format is None
    assert timestamp_info.timestamp_column == "timestamp"
    assert timestamp_info.timestamp_regex is None


def test__get_uuid_pos():
    import uuid
    uid = BaseUtils.get_uuid_str()
    assert isinstance(uid, str) is True


def test__if_table_exists_neg(mocker, spark_session):
    mocker.patch("databricks.sirens.utils.base_utils.SqlQuery.describe_table", return_value=None)
    assert BaseUtils._if_table_exists(spark_session, 'sirens', 'table1') is False


def test__if_table_exists_pos(mocker, spark_session):
    mocker.patch("databricks.sirens.utils.base_utils.SqlQuery.describe_table", return_value=True)
    assert BaseUtils._if_table_exists(spark_session, 'sirens', 'table1') is True


def test__is_time_interval_valid_neg():
    interval = "INTERVAL 1x DAY"
    result = BaseUtils.is_time_interval_valid(interval)
    assert result is False


def test__is_time_interval_valid_pos():
    interval = "INTERVAL 1 DAY"
    result = BaseUtils.is_time_interval_valid(interval)
    assert result is True


def test__read_schema_file_neg(mocker, spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    file = "empty.yaml"
    result = data_source_obj.read_schema_file(file)
    assert result is None


@pytest.mark.skip(reason="Not testable today")
def test__read_schema_file_pos():
    assert True


@pytest.mark.skip(reason='todo')
def test_add_metadata_neg():
    assert True


@pytest.mark.skip(reason='todo')
def test_add_metadata_pos():
    assert True


@pytest.mark.skip(reason='todo')
def test_cast_columns_neg():
    assert True


@pytest.mark.skip(reason='todo')
def test_cast_columns_pos():
    assert True


def test_check_event_timestamp_pos(simple_dataframe):
    result = BaseUtils.check_event_timestamp(simple_dataframe, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    assert isinstance(result, DataFrame)


@pytest.mark.skip(reason='todo')
def test_flatten_frame_neg():
    assert True


@pytest.mark.skip(reason='todo')
def test_flatten_frame_pos():
    assert True


@pytest.mark.skip(reason='todo')
def test_get_latest_ts_neg():
    assert True


@pytest.mark.skip(reason='todo')
def test_get_latest_ts_pos():
    assert True


def test_get_table_name_pos():
    result = DataSource.get_table_name('aws', 'cloudtrail', 'aa', 'raw', '_')
    assert result == 'aa_aws_cloudtrail_raw'


def test_get_table_name_pos_no_prefix():
    result = DataSource.get_table_name(source='aws', source_type='cloudtrail', suffix='raw', sep='_')
    assert result == 'aws_cloudtrail_raw'


def test_get_table_name_pos_no_suffix():
    result = DataSource.get_table_name(source='aws', source_type='cloudtrail', sep='_')
    assert result == 'aws_cloudtrail'


def test__get_scratch_dir(global_config):
    global_config['default']['scratch_dir'] = "/tmp"
    result = GlobalConfig.get_global_scratch_dir(global_config)
    assert result == "/tmp"


def test_configreader_get_config_key(global_config_rec):
    result = ConfigReader._get_config_key(global_config_rec, "default", "target_database")
    assert result == 'sirens'


def test_configreader_get_config_key_non_existent_sec(global_config_rec):
    result = ConfigReader._get_config_key(global_config_rec, "not_exists", "target_database")
    assert result is None


def test_configreader_get_config_key_non_existent_key(global_config_rec):
    result = ConfigReader._get_config_key(global_config_rec, "default", "not_exists_key")
    assert result is None


def test_is_streaming():
    config = {"streamType": "streaming"}
    result = BaseUtils.is_streaming(config)
    assert result

    config = {"streamType": "Streaming"}
    assert BaseUtils.is_streaming(config)

    config = {"streamType": "batch"}
    result = BaseUtils.is_streaming(config)
    assert not result

    config = {}
    result = BaseUtils.is_streaming(config)
    assert not result


def test_get_read_method_spark(spark_session):
    expected_batch = "pyspark.sql.readwriter.DataFrameReader"
    expected_stream = "pyspark.sql.streaming.readwriter.DataStreamReader"

    config = {"dlt_read": False}
    result = BaseUtils.get_read_method(spark_session, config)
    assert expected_batch in str(result)

    config = {"dlt_read": "false"}
    result = BaseUtils.get_read_method(spark_session, config)
    assert expected_batch in str(result)

    config = {}
    result = BaseUtils.get_read_method(spark_session, config)
    assert expected_batch in str(result)

    config = {"dlt_read": False, "streamType": "streaming"}
    result = BaseUtils.get_read_method(spark_session, config)
    assert expected_stream in str(result)

def test_group_items():
    to_be_sorted = [
        {'key1':'1', 'another_key':'2'},
        {'key1':'3', 'another_key':'2'},
        {'key1':'1', 'another_key':'2'},
        {'key1':'2', 'another_key':'2'}
    ]
    groupby_key = ['key1']
    result = [list(g) for k, g in BaseUtils.group_items(to_be_sorted, groupby_key)]
    assert isinstance(result, list)
    assert len(result[0]) == 2

def test_is_file_accessible_true():
    f_path = "tests/utils_additional_test.py"
    result = config_utils.is_file_accesible(f_path)
    assert result is True

def test_is_file_accessible_false():
    f_path = "tests/anonfile.py"
    result = config_utils.is_file_accesible(f_path)
    assert result is False

def test_get_path_to_file_pos():
    f_path = "tests/utils_additional_test.py"
    result = config_utils.get_path_to_file(f_path)
    assert isinstance(result, str)

def test_get_path_to_file_neg():
    f_path = "tests/anonfile.py"
    result = config_utils.get_path_to_file(f_path)
    assert result is False

def test_get_path_to_file_abs_only():
    f_path = "tests/utils_additional_test.py"
    result = config_utils.get_path_to_file(f_path, absolute_only=True)
    assert os.path.isabs(str(result))

def test_get_intel_collection_nb_dir(global_config):
    result = GlobalConfig.get_intel_collection_nb_dir(global_config)
    assert isinstance(result, str)
    assert 'notebooks/threat_intelligence/collectors' in result

def test_get_intel_ingest_nb_dir(global_config):
    result = GlobalConfig.get_intel_ingest_nb_dir(global_config)
    assert isinstance(result, str)
    assert 'notebooks/threat_intelligence/normalize' in result

# @pytest.mark.skip(reason='need to run in DLT enabled environment')
def test_get_read_method_dlt(mock_dlt):
    import sys
    sys.modules['dlt'] = mock_dlt

    expected_batch = mock_dlt.read
    expected_stream = mock_dlt.readStream

    config = {"dlt_read": True}
    result = BaseUtils.get_read_method(None, config)
    assert result == expected_batch

    config = {"dlt_read": "true"}
    result = BaseUtils.get_read_method(None, config)
    assert result == expected_batch

    config = {"dlt_read": "TRUE"}
    result = BaseUtils.get_read_method(None, config)
    assert result == expected_batch

    config = {"dlt_read": True, "streamType": "streaming"}
    result = BaseUtils.get_read_method(None, config)
    assert result == expected_stream

@pytest.mark.skip(reason='todo')
def test_match_struct_fields_basic(spark_session):
    struct = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    table_col = StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True)

    result = BaseUtils.match_struct_fields(struct, table_col)
    assert result == struct

@pytest.mark.skip(reason='todo')
def test_match_struct_fields_nested(spark_session):
    struct = StructType([
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("zip", IntegerType(), True)
        ]), True)
    ])
    table_col = StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("zip", IntegerType(), True)
        ]), True)
    ]), True)

    result = BaseUtils.match_struct_fields(struct, table_col)
    assert result == struct

@pytest.mark.skip(reason='todo')
def test_match_struct_fields_array(spark_session):
    struct = StructType([
        StructField("name", StringType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ])
    table_col = StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ]), True)

    result = BaseUtils.match_struct_fields(struct, table_col)
    assert result == struct

@pytest.mark.skip(reason='todo')
def test_match_struct_fields_mismatch(spark_session):
    struct = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    table_col = StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("height", IntegerType(), True)
    ]), True)

    with pytest.raises(KeyError):
        BaseUtils.match_struct_fields(struct, table_col)


