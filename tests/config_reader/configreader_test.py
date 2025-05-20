from databricks.sirens.config_reader import (ActionReader, Alerts, CommonInformationModel,
                                             BaseConf, AggregateReader, OpenCyberSecurityFramework)
import configparser

from pyspark.sql.types import StructType

from databricks.sirens.datasource import DataSource
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.config_reader import ThreatIntelReader


def test_GlobalConfig_read_global_pos(global_config_rec):
    # result = GlobalConfig._read_global()
    # assert isinstance(result, object)
    # TODO - fix once off an airplane...
    result = global_config_rec
    # assert global_config_rec['default']['conf_dir'] == 'conf'
    assert True


def test_GlobalConfig_read():
    result = GlobalConfig.read()
    assert isinstance(result, configparser.ConfigParser)


def test_Actions_read_pos():
    # TODO patch original file for something in samples.
    result = ActionReader().read(action_module='slack')
    assert isinstance(result, dict)


def test_Alerts_read_pos():
    result = Alerts.read()
    assert isinstance(result, list)
    assert isinstance(result[0], dict)

def test_DataSource_read_config_pos(mocker, spark_session):
    spark_session.conf.set('spark.databricks.workspaceUrl', 'fakeURL')

    dsobject = DataSource(spark_session, 'sirens_test', 'apache', 'access_combined')
    obj = dsobject.read()
    assert obj.get("input").get("sourcetype") == "access_combined"


def test_printschema_pos(mocker, spark_session):
    obj = DataSource(spark_session, 'sirens_test', 'apache', 'access_combined')
    _ = obj.read()
    result = obj.print_schema()
    assert result is None


def test_get_named_conf():
    running_config = [
        {
            "name": "named_section",
            "config": "some_config"
        },
        {
            "name": "some_other_section",
            "config": "some_config"}
    ]
    result = BaseConf.get_named_conf(running_config, "named_section")
    assert isinstance(result, dict)
    assert result['name'] == 'named_section'


def test_get_aggregate():
    running_config = [
        {
            "name": "named_section",
            "config": "some_config"
        },
        {
            "name": "some_other_section",
            "config": "some_config"}
    ]
    result = AggregateReader.get_aggregate(running_config, "named_section")
    assert isinstance(result, dict)
    assert result['name'] == "named_section"


def test_get_aggregate_neg():
    running_config = [
        {
            "name": "named_section",
            "config": "some_config"
        },
        {
            "name": "some_other_section",
            "config": "some_config"}
    ]
    result = AggregateReader.get_aggregate(running_config, "missing_section")
    assert isinstance(result, dict)
    assert not result


def test_list_directory():
    dirs = ["tests/samples", "tests/data"]
    result = BaseConf.list_directory(directory=dirs, with_paths=False)
    assert isinstance(result, list)
    assert len(result) > 0
    assert '/' not in result[0]


def test_list_directory_no_files():
    dirs = ["non-existant"]
    result = BaseConf.list_directory(directory=dirs, with_paths=False)
    assert result is None


def test_list_directory_w_paths():
    dirs = ["tests/samples", "tests/data"]
    result = BaseConf.list_directory(directory=dirs, with_paths=True)
    assert isinstance(result, list)
    assert len(result) > 0
    assert '/' in result[0]

def test_threat_intel_reader_read_named_file_system_level(mocker, global_config_rec):
    mocker.patch('databricks.sirens.config_reader.PathUtils.get_relative_file_paths', return_value=['/fake/path'])
    mocker.patch('databricks.sirens.config_reader.BaseConf.read', return_value={'key': 'value'})
    mocker.patch('databricks.sirens.config_reader.GlobalConfig.get', return_value=global_config_rec)

    reader = ThreatIntelReader()
    result = reader.read_named_file('test_file')

    assert result == {'key': 'value'}


def test_threat_intel_reader_read_named_file_no_config(mocker, global_config_rec):
    mocker.patch('databricks.sirens.config_reader.PathUtils.get_relative_file_paths', return_value=['/fake/path'])
    mocker.patch('databricks.sirens.config_reader.BaseConf.read', return_value=None)
    mocker.patch('databricks.sirens.config_reader.GlobalConfig.get', return_value=global_config_rec)
    
    reader = ThreatIntelReader()
    result = reader.read_named_file('test_file')

    assert result == []

def test_ocsf_read():
    result = OpenCyberSecurityFramework().read("account")
    assert isinstance(result, StructType)

def test_cim_read():
    result = CommonInformationModel().read("authentication")
    assert isinstance(result, StructType)