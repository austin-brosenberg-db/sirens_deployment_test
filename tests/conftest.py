import pytest
from pytest_mock import mocker
from datetime import datetime
import os
import yaml
import json
from yaml.loader import SafeLoader
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType, Row, IntegerType
import configparser
from pathlib import Path

from databricks.sirens.controller import Controller
from databricks.sirens.datasource import DataSource
from databricks.sirens.global_config import GlobalConfig


@pytest.fixture
def sample_splunk_test_dataframe(spark_session):
    data = [("2024-10-01 10:31:12", "Alice", 34),
            ("2024-10-02 1:32:12", "Bob", 45),
            ("2024-10-20 10:00:12", "Cathy", 29)]
    schema = StructType([
        StructField("_event_time", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    return spark_session.createDataFrame(data, schema)
    
@pytest.fixture
def ct():
    return datetime.now().strftime("%H_%M_%S")

@pytest.fixture
def sample_ioc_dataframe(spark_session):
    data = [("2024-10-01 10:31:12", "Alice", "253.253.253.25"), 
            ("2024-10-01 10:31:12", "Bob", "2.2.2.2"),
            ("2024-10-01 10:31:12", "Cathy", "253.253.253.25")]
    
    schema = StructType([
        StructField("_event_time", StringType(), True),
        StructField("name", StringType(), True),
        StructField("dest_ip", StringType(), True)
    ])
    return spark_session.createDataFrame(data, schema)

def pytest_addoption(parser):
    parser.addoption("--rulename", action="store", default=None)
    parser.addoption("--parser_sourcetype", action="store", default=None)


@pytest.fixture(scope="session")
def rulename(request):
    return request.config.getoption("--rulename")


@pytest.fixture(scope="session")
def parser_sourcetype(request):
    return request.config.getoption("--parser_sourcetype")


@pytest.fixture(scope="session")
def dataSourceObj(spark_session):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    return data_source_obj


@pytest.fixture(scope='session')
def controller_obj():
    config_file = 'sirens.config'
    config = configparser.ConfigParser()
    config.read(config_file)
    controller_obj = Controller(config)
    return controller_obj


@pytest.fixture(scope="session")
def global_config():
    global_config = GlobalConfig.read()
    return global_config


@pytest.fixture(scope="session")
def metafield_expectations():
    expectations = {
        "_event_date_is_valid": "_event_date IS NOT NULL and _event_date > '2013-01-01'",
        "_event_time_is_valid": "_event_time IS NOT NULL and _event_time > '2013-01-01 00:00:00.01'",
        "_source_is_valid": "_source IS NOT NULL",
        "_sourcetype_is_valid": "_sourcetype IS NOT NULL",
        "_dvc_hostname_is_valid": "dvc_hostname IS NOT NULL"
    }
    return expectations


@pytest.fixture(scope="session")
def metafield_inverse_expectations():
    inverse_expectations = {
        "_event_date_is_valid": "NOT(_event_date IS NOT NULL and _event_date > '2013-01-01')",
        "_event_time_is_valid": "NOT(_event_time IS NOT NULL and _event_time > '2013-01-01 00:00:00.01')",
        "_source_is_valid": "NOT(_source IS NOT NULL)",
        "_sourcetype_is_valid": "NOT(_sourcetype IS NOT NULL)",
        "_dvc_hostname_is_valid": "NOT(dvc_hostname IS NOT NULL)"
    }
    return inverse_expectations


@pytest.fixture(scope="session")
def access_log_dataframe(spark_session):
    location = 'tests/samples/access_combined/access_combined.txt'
    with open(location, 'r') as f:
        data = f.read().splitlines()
    rows = [Row(value=x) for x in data]
    df = spark_session.createDataFrame(rows)

    TIMESTAMP_REGEX = r'^(?<host>[^ ]*) [^ ]* ([^ ]*) \[(?<time>[^\]]*)\]'
    df = df.select("*", regexp_extract("value", TIMESTAMP_REGEX, 3).alias('_time'))
    return df


@pytest.fixture(scope="session")
def bronze_access_log_dataframe(spark_session, access_log_dataframe):
    ex = r'^(?<host>[^ ]*) [^ ]* ([^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +((?:[^\"]|\\.)*?)(?: +\S*)?)?" ([^ ]*) ([^ ]*)(?: "((?:[^\"]|\\.)*)" "((?:[^\"]|\\.)*)")?$'
    df = access_log_dataframe.select('*',
                                     regexp_extract("value", ex, 1).alias('host'),
                                     regexp_extract("value", ex, 2).alias('user'),
                                     regexp_extract("value", ex, 4).alias('method'),
                                     regexp_extract("value", ex, 5).alias('path'),
                                     regexp_extract("value", ex, 6).alias('code'),
                                     regexp_extract("value", ex, 7).alias('size'),
                                     regexp_extract("value", ex, 8).alias('referer'),
                                     regexp_extract("value", ex, 9).alias('agent')
                                     )
    return df


@pytest.fixture(scope='session')
def access_log_config():
    d = DataSource(None, "sirens", "apache", "access_combined",
                   "tests/samples/access_combined/inputs.yaml",
                   enrichments = {'system': [{'name': 'iana_ports', 'table': 'iana', 'source_column': 'http_port'}],
                                            'content_pack': [{'name': 'iana_ports', 'table': 'iana', 'source_column': 'http_port'}]})
    d.read()
    return lambda: d


@pytest.fixture(scope='session')
def global_config_rec():
    location = f"{os.curdir}/tests/samples/global_config.conf"
    config_file = Path(location)
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    except Exception as exc:
        print(f"{exc}")


@pytest.fixture(scope='session')
def dataframe_simple_schema(spark_session):
    schema = ["col_name", "data_type", "comment"]
    return schema


@pytest.fixture(scope='session')
def datasource_obj():
    class DataSource:
        def __init__(self):
            self.dataSourceConfig = {"input": {"host": "somehost"}}
            self.workspace_name = 'workspace.databricks.com'

    return DataSource


@pytest.fixture(scope='session')
def global_config_obj():
    return {"default": {"notebook_type": "delta"}}


@pytest.fixture(scope='session')
def simple_dataframe(spark_session):
    # schema = ["col_name", "data_type", "comment"]
    schema = StructType([
        StructField("firstname", StringType()),
        StructField("givenname", StringType()),
        StructField("comment", StringType())
    ])
    data = [("derek", "king", "somenerd"), ("david", "veuve", "extrordinaire")]
    df = spark_session.createDataFrame(data=data, schema=schema)
    return df


@pytest.fixture(scope='session')
def connector_status():
    location = f"{os.curdir}/tests/samples/json_files/connector_status.json"
    with open(location) as f:
        data = json.load(f)
    return data


@pytest.fixture(scope='session')
def master_inventory():
    data = [{'task_type': 'ingest', 'task_name': 'aws_vpc_flowlogs', 'datasource_name': 'aws_vpc_flowlogs',
             'description':'a task description', 'app_name': 'sirens', 'language': 'python', 'isDeltaStreaming': 'true', 'source': 'aws',
             'sourcetype': 'vpc_flowlogs', 'schedule': '0 0 13 * * ?', 'timezone_id': 'GMT',
             "scratch_dir": "/FileStore/sirens", 'target_database': 'sirens', 'target_notebook_dir': 'sirens_notebooks',
             'dbfs_destination_dir': '/FileStore/sirens', 'dashboard_dir': '/FileStore/sirens/sirens_dashboards',
             'cluster': None, 'whl_path': 'lib/databricks-sirens.whl', 'clusters': [
            {'name': 'cluster:AlwaysOnCluster', 'default': 'true', 'cluster_name': 'AlwaysOnCluster',
             'cluster_id': '9999-999999-xxxxxxxx', 'workspace': 'sfe', "spark_options": {}, "compute_attributes": {},
             "autoscale": {"min_workers": 'null', "max_workers": 'null'}},
            {'name': 'cluster:sharedjobcluster', 'default': False, 'cluster_name': 'sharedjobcluster',
             'cluster_id': 'dddddddddd', 'workspace': 'sfe', "spark_options": {}, "compute_attributes": {},
             "autoscale": {"min_workers": "2", "max_workers": "6"}}], 'dashboards': [
            '/Users/derek.king/Documents/Dev_Work/databricks-sirens/deploy/dashboards/dashboard-a1f92afa-df82-401f-aa0c-f16fef4b9cf9.json',
            '/Users/derek.king/Documents/Dev_Work/databricks-sirens/deploy/dashboards/dashboard-1401ad36-81eb-4d83-8a6f-123bba9b42c1.json',
            '/Users/derek.king/Documents/Dev_Work/databricks-sirens/deploy/dashboards/dashboard-6c40953b-c09c-4f34-a741-8d5143ff702a.json'],
             'sql_warehouse_id': '39ad1b2373562575', "edition": "advanced", "channel": "current", "photon": "true",
             "continuous": "false",
             'notebook_paths': [{'ingest': '/deploy/delta/aws/vpc_flowlogs/01-aws-vpc_flowlogs-ingest.py'},
                                {'parse': '/deploy/delta/aws/vpc_flowlogs/02-aws-vpc_flowlogs-parse.py'},
                                {'normalize': '/deploy/delta/aws/vpc_flowlogs/03-aws-vpc_flowlogs-normalize.py'},
                                {'maintenance': '/deploy/delta/aws/vpc_flowlogs/06-aws-vpc_flowlogs-maintenance.py'}]}]
    return data


@pytest.fixture(scope='session')
def maintenance_inventory():
    data = [{"task_name": "okta_oktaIM2_log", "datasource_name": "okta_oktaIM2_log", "app_name": "sirens",
             'description':'a task description', "language": "python", "isDeltaStreaming": 'true', "source": "okta", "sourcetype": "oktaIM2_log",
             "schedule": "0 0 13 * * ?", "timezone_id": "BST", "scratch_dir": "/FileStore/sirens",
             "target_database": "sirens", "target_notebook_dir": "sirens_notebooks",
             "dbfs_destination_dir": "/FileStore/sirens", "dashboard_dir": "sirens_dashboards",
             "instance_profile": 'null', "cluster": 'null', "clusters": [
            {"name": "cluster:ExistingCluster", "default": "true", "cluster_name": 'null',
             "cluster_id": "9999-999999-xxxxxxxx", "cloud_provider": "aws", "workspace": "sfe", "spark_options": {},
             "compute_attributes": {}, "autoscale": {"min_workers": 'null', "max_workers": 'null'}},
            {"name": "jobcluster:jobcluster_1", "default": "true", "cluster_name": 'null', "cluster_id": "",
             "cloud_provider": "aws", "workspace": "sfe",
             "spark_options": {"sirens_wheel": "latest", "spark.databricks.io.cache.enabled": "true",
                               "spark.databricks.io.cache.maxdiskusage": "\"50g\"",
                               "spark.databricks.io.cache.maxmetadatacache": "\"1g\""}, "compute_attributes": {
                "instance_profile_arn": "arn:aws:iam::755921336062:instance-profile/veuve-cloud-databricks-com-primary-role"},
             "autoscale": {"min_workers": "2", "max_workers": "6"}}],
             "dashboards": ["/deploy/dashboards/dashboard-a1f92afa-df82-401f-aa0c-f16fef4b9cf9.json"],
             "sql_warehouse_id": "39ad1b2373562575", "edition": "advanced", "channel": "current", "photon": "true",
             "continuous": "false",
             "notebook_paths": [{"ingest": "/deploy/delta/okta/oktaIM2_log/01-okta-oktaIM2_log-ingest.py"},
                                {"parse": "/deploy/delta/okta/oktaIM2_log/02-okta-oktaIM2_log-parse.py"},
                                {"normalize": "/deploy/delta/okta/oktaIM2_log/03-okta-oktaIM2_log-normalize.py"}]},
            {"task_name": "Maintenance", "whl_path": "lib/databricks-sirens.whl", "language": "python", 'description':'a task description',
             "source": "okta", "sourcetype": "oktaIM2_log", "isDeltaStreaming": "true", "app_name": "sirens",
             "cluster": 'null', "clusters": [
                {"name": "cluster:ExistingCluster", "default": "true", "cluster_name": 'null',
                 "cluster_id": "9999-999999-xxxxxxxx", "cloud_provider": "aws", "workspace": "sfe", "spark_options": {},
                 "compute_attributes": {}, "autoscale": {"min_workers": 'null', "max_workers": 'null'}},
                {"name": "jobcluster:jobcluster_1", "default": "true", "cluster_name": 'null', "cluster_id": "",
                 "cloud_provider": "aws", "workspace": "sfe",
                 "spark_options": {"sirens_wheel": "latest", "spark.databricks.io.cache.enabled": "true",
                                   "spark.databricks.io.cache.maxdiskusage": "\"50g\"",
                                   "spark.databricks.io.cache.maxmetadatacache": "\"1g\""}, "compute_attributes": {
                    "instance_profile_arn": "arn:aws:iam::755921336062:instance-profile/veuve-cloud-databricks-com-primary-role"},
                 "autoscale": {"min_workers": "2", "max_workers": "6"}}], "schedule": "0 0 13 * * ?",
             "timezone_id": "BST", "target_database": "sirens", "target_notebook_dir": "sirens_notebooks",
             "dbfs_destination_dir": "/FileStore/sirens",
             "notebook_paths": [{"maintenance": "/deploy/delta/okta/oktaIM2_log/06-okta-oktaIM2_log-maintenance.py"}]},
            {"task_name": "Setup", "language": "python", "isDeltaStreaming": "true", "app_name": "sirens",
             "cluster": 'null', "clusters": [
                {"name": "cluster:ExistingCluster", "default": "true", "cluster_name": 'null',
                 "cluster_id": "9999-999999-xxxxxxxx", "cloud_provider": "aws", "workspace": "sfe", "spark_options": {},
                 "compute_attributes": {}, "autoscale": {"min_workers": 'null', "max_workers": 'null'}},
                {"name": "jobcluster:jobcluster_1", "default": "true", "cluster_name": 'null', "cluster_id": "",
                 "cloud_provider": "aws", "workspace": "sfe",
                 "spark_options": {"sirens_wheel": "latest", "spark.databricks.io.cache.enabled": "true",
                                   "spark.databricks.io.cache.maxdiskusage": "\"50g\"",
                                   "spark.databricks.io.cache.maxmetadatacache": "\"1g\""}, "compute_attributes": {
                    "instance_profile_arn": "arn:aws:iam::755921336062:instance-profile/veuve-cloud-databricks-com-primary-role"},
                 "autoscale": {"min_workers": "2", "max_workers": "6"}}], "schedule": "0 0 13 * * ?",
             "timezone_id": "BST", "target_database": "sirens", "target_notebook_dir": "sirens_notebooks",
             "dbfs_destination_dir": "/FileStore/sirens", "dashboard_dir": "sirens_dashboards", "dashboards": [
                "/Users/derek.king/Documents/Dev_Work/databricks-sirens/deploy/dashboards/dashboard-a1f92afa-df82-401f-aa0c-f16fef4b9cf9.json"],
             "sql_warehouse_id": "39ad1b2373562575", "setup_notebooks": [{"dashboard_import": "import_dashboards.py"}]}]
    return data


@pytest.fixture(scope='session')
def web_dataframe_for_enrichment(spark_session):
    schema = StructType([
        StructField("http_port", StringType()),
        StructField("http_method", StringType()),
        StructField("src_ip", StringType()),
        StructField("dest_ip", StringType())
    ])
    data = [("443", "GET", "1.1.1.1", "2.2.2.2"),
            ("80", "POST", "3.3.3.3", "4.4.4.4"),
            ("25", "POST", "5.5.5.5", "6.6.6.6")]
    df = spark_session.createDataFrame(data=data, schema=schema)
    return df


@pytest.fixture(scope='session')
def web_enrichment(spark_session):
    schema = StructType([
        StructField("http_port", StringType()),
        StructField("protocol", StringType()),
        StructField("service", StringType())

    ])
    data = [("443", "ssl", "tcp"),
            ("80", "http", "tcp"),
            ]
    df = spark_session.createDataFrame(data=data, schema=schema)
    return df


@pytest.fixture(scope='session')
def apache_df(spark_session):
    data = [("2023-01-13", "2023-01-13T10:16:51.000+0000", "apache", "access_combined", "12345", "derek.king"), ]
    schema = "_event_date string, _event_time string, source string, sourcetype string, run_id string, user string"
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture(scope="session")
def aggregate_df(spark_session):
    schema = StructType([
        StructField("_event_date", StringType()),
        StructField("_event_time", StringType()),
        StructField("_source", StringType()),
        StructField("_sourcetype", StringType()),
        StructField("dvc_hostname", StringType()),
        StructField("event_message", StringType()),
        StructField("event_message_result", StringType()),
        StructField("event_result", StringType()),
        StructField("event_schema_file", StringType()),

    ])
    data = [
        ("2023-01-13", "2023-01-13T10:16:51.000+0000", "apache", "access_combined", "14.139.187.130", "success",
         "success", "success", "access_log"),
        ("2023-01-13", "2023-01-13T10:16:55.000+0000", "apache", "access_combined", "14.139.187.130", "success",
         "success", "success", "access_log"),
        ("2023-01-13", "2023-01-13T10:17:56.000+0000", "apache", "access_combined", "14.139.187.130", "success",
         "success", "success", "access_log"),
        ("2023-01-13", "2023-01-13T10:18:59.000+0000", "apache", "access_combined", "68.180.228.229", "success",
         "success", "success", "access_log"),
    ]
    df = spark_session.createDataFrame(data=data, schema=schema)
    return df


@pytest.fixture(scope="session")
def soar_transforms():
    transforms = {'container': {'NAME': 'name', 'LABEL': "lit('events')", 'SOURCE_DATA_IDENTIFIER': 'uuid'},
                  'artifact': {'DESCRIPTION': 'summary', 'KILL_CHAIN': 'attacks.killchain.stage',
                               'TYPE': "lit('[]')", 'LABEL': "lit('event')", 'NAME': 'name',
                               'RUN_AUTOMATION': "lit('False')", 'SEVERITY': 'severity',
                               'TAGS': "lit('[]')", 'SOURCE_DATA_IDENTIFIER': 'uuid',
                               'START_TIME': '_event_time', 'DATA': 'context',
                               'CEF': {'fileHash': 'target.type', 'fileSize': 'target.filesize',
                                       'externalId': 'attacks.mitre.subtechnique'
                                       }
                               }
                  }

    return transforms


@pytest.fixture(scope="session")
def mock_dlt():
    from unittest.mock import MagicMock
    dlt = MagicMock()
    dlt.read = "read"
    dlt.readStream = "read_stream"
    return dlt


def _get_top_level_directory():
    import os
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
