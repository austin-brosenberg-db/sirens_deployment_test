import builtins
import json
import os

import pytest

from databricks.sirens.deployer import TF, Inventory, Jobs
from databricks.sirens.exceptions import SirensEnvironmentException, SirensConfigException


def test__get_nb_target_paths():
    notebooks = [{"ingest": "deploy/delta/databricks/db_audit/01-databricks-db_audit-ingest.py"},
                 {"parse": "deploy/delta/databricks/db_audit/02-databricks-db_audit-parse.py"},
                 {"normalize": "deploy/delta/databricks/db_audit/03-databricks-db_audit-normalize.py"},
                 {"aggregate": "deploy/delta/databricks/db_audit/03-databricks-db_audit-aggregate.py"}
                 ]
    ingest, parse, normalize, aggregate = Jobs()._get_nb_target_paths(notebooks)
    expect_ingest = "deploy/delta/databricks/db_audit/01-databricks-db_audit-ingest.py"
    expect_parse = "deploy/delta/databricks/db_audit/02-databricks-db_audit-parse.py"
    expect_normalize = "deploy/delta/databricks/db_audit/03-databricks-db_audit-normalize.py"
    expect_aggregate = "deploy/delta/databricks/db_audit/03-databricks-db_audit-aggregate.py"
    assert ingest == expect_ingest
    assert parse == expect_parse
    assert normalize == expect_normalize
    assert aggregate == expect_aggregate


def test__make_jobs_json(master_inventory):
    result = Jobs()._make_jobs_json(master_inventory)
    expected = {'jobs': [
        {'task_name': 'aws_vpc_flowlogs', 'description': 'a task description', 'datasource_name': 'aws_vpc_flowlogs',
         'language': 'python', 'isDeltaStreaming': 'true', 'aws_attributes': {}, 'azure_attributes': {},
         'gcp_attributes': {}, 'spark_conf': {'spark.databricks.isv.product': 'databricks-sirens'},
         'uses_existing_cluster': True,
         'tags': {'app': 'sirens', 'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'product': 'delta', 'type': 'ingest'},
         'whl_path': 'lib/databricks-sirens.whl',
         'cluster': {'cluster_key': 'cluster:AlwaysOnCluster', 'runtime_engine': 'PHOTON',
                     'autoscale': {'min_workers': '1', 'max_workers': '5'}}, 'tasks': [
            {'task_key': 'ingest', 'job_cluster_key': 'cluster:AlwaysOnCluster',
             'existing_cluster_id': '9999-999999-xxxxxxxx',
             'notebook_path': '/deploy/delta/aws/vpc_flowlogs/01-aws-vpc_flowlogs-ingest.py',
             'task_params': {'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'database': 'sirens'}},
            {'task_key': 'parse', 'job_cluster_key': 'cluster:AlwaysOnCluster',
             'existing_cluster_id': '9999-999999-xxxxxxxx',
             'notebook_path': '/deploy/delta/aws/vpc_flowlogs/02-aws-vpc_flowlogs-parse.py',
             'task_params': {'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'database': 'sirens'},
             'depends': ['ingest']}, {'task_key': 'normalize', 'job_cluster_key': 'cluster:AlwaysOnCluster',
                                      'existing_cluster_id': '9999-999999-xxxxxxxx',
                                      'notebook_path': '/deploy/delta/aws/vpc_flowlogs/03-aws-vpc_flowlogs-normalize.py',
                                      'task_params': {'source': 'aws', 'sourcetype': 'vpc_flowlogs',
                                                      'database': 'sirens'}, 'depends': ['parse']}],
         'schedule': {'quartz_cron_expression': '0 0 13 * * ?', 'timezone_id': 'GMT'}}]}
    assert result == expected


def test_create_job_tf(master_inventory):
    result = Jobs().create_job_tf(master_inventory)
    expected = {'jobs': [
        {'task_name': 'aws_vpc_flowlogs', 'description': 'a task description', 'datasource_name': 'aws_vpc_flowlogs',
         'language': 'python', 'isDeltaStreaming': 'true', 'aws_attributes': {}, 'azure_attributes': {},
         'gcp_attributes': {}, 'spark_conf': {'spark.databricks.isv.product': 'databricks-sirens'},
         'uses_existing_cluster': True,
         'tags': {'app': 'sirens', 'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'product': 'delta', 'type': 'ingest'},
         'whl_path': 'lib/databricks-sirens.whl',
         'cluster': {'cluster_key': 'cluster:AlwaysOnCluster', 'runtime_engine': 'PHOTON',
                     'autoscale': {'min_workers': '1', 'max_workers': '5'}}, 'tasks': [
            {'task_key': 'ingest', 'job_cluster_key': 'cluster:AlwaysOnCluster',
             'existing_cluster_id': '9999-999999-xxxxxxxx',
             'notebook_path': '/deploy/delta/aws/vpc_flowlogs/01-aws-vpc_flowlogs-ingest.py',
             'task_params': {'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'database': 'sirens'}},
            {'task_key': 'parse', 'job_cluster_key': 'cluster:AlwaysOnCluster',
             'existing_cluster_id': '9999-999999-xxxxxxxx',
             'notebook_path': '/deploy/delta/aws/vpc_flowlogs/02-aws-vpc_flowlogs-parse.py',
             'task_params': {'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'database': 'sirens'},
             'depends': ['ingest']}, {'task_key': 'normalize', 'job_cluster_key': 'cluster:AlwaysOnCluster',
                                      'existing_cluster_id': '9999-999999-xxxxxxxx',
                                      'notebook_path': '/deploy/delta/aws/vpc_flowlogs/03-aws-vpc_flowlogs-normalize.py',
                                      'task_params': {'source': 'aws', 'sourcetype': 'vpc_flowlogs',
                                                      'database': 'sirens'}, 'depends': ['parse']}],
         'schedule': {'quartz_cron_expression': '0 0 13 * * ?', 'timezone_id': 'GMT'}}]}
    assert result == expected


@pytest.mark.skip("fails on github")
def test_get_master_inventory(mocker, global_config_rec):
    mocker.patch("databricks.sirens.deployer.Inventory._get_env_vars", return_value=("one", "two"))
    result = Inventory.get_master_inventory(global_config_rec)
    print(result[0])
    assert result[0]['task_name'] == "aws_cloudtrail"
    assert result[0]['language'] == 'python'
    assert len(result) == 3


def test__make_url():
    result = Inventory._make_url("workspace.cloud.databricks.com")
    expected = "https://workspace.cloud.databricks.com"
    assert result == expected
    result = Inventory._make_url("https://workspace.cloud.databricks.com")
    assert result == expected


@pytest.mark.skip(reason='not working')
def test__get_env_vars(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "sfe.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "1234")
    host, token = Inventory._get_env_vars()
    assert host == "sfe.databricks.com"
    assert token == "1234"
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    with pytest.raises(SirensEnvironmentException):
        host, token = Inventory._get_env_vars()


def test__get_warehouse_user_option(monkeypatch):
    monkeypatch.setattr('builtins.input', lambda _: "1")
    wh = [{"name": "cluster1", "id": "1234"}]
    result = Inventory._get_warehouse_user_option(wh)
    assert result == "1234"


def test__get_warehouses_by_rest(mocker):
    mocker.patch("databricks.sirens.internal.restclient.DatabricksAPI")
    result = Inventory._get_warehouses_by_rest("https://workspace.cloud.databricks.com", "1234")
    assert len(result) == 0


def test__get_instance_profiles_by_rest(mocker):
    mocker.patch("databricks.sirens.internal.restclient.DatabricksAPI")
    result = Inventory._get_instance_profiles_by_rest("https://work.cloud.databricks.com", "1234")
    assert len(result) == 0


def test__get_dashboards():
    result = Inventory._get_dashboards("non-existant-dir")
    assert len(result) == 0


def test__get_clusters(global_config_rec):
    result = Inventory._get_clusters(global_config_rec)
    print(result)
    expected = [{'name': 'jobcluster:jobcluster_1', 'default': 'true', 'cluster_name': None, 'cluster_id': '',
                 'cloud_provider': 'aws', 'workspace': 'dummy_workspace',
                 'spark_options': {'sirens_wheel': 'latest', 'spark.databricks.io.cache.enabled': 'true',
                                   'spark.databricks.io.cache.maxdiskusage': '"50g"',
                                   'spark.databricks.io.cache.maxmetadatacache': '"1g"'}, 'compute_attributes': {
            'instance_profile_arn': 'arn:aws:iam::999999999999:instance-profile/some_profile'},
                 'autoscale': {'min_workers': '2', 'max_workers': '6'}},
                {'name': 'cluster:AlwaysOnCluster', 'default': 'true', 'cluster_name': 'AlwaysOnCluster',
                 'cluster_id': '1115-999999-9999999', 'cloud_provider': 'aws', 'workspace': 'dummy_workspace',
                 'spark_options': {}, 'compute_attributes': {},
                 'autoscale': {'min_workers': None, 'max_workers': None}}]
    assert result == expected


def test__get_compute_attrs(global_config_rec):
    result = Inventory._get_compute_attrs(global_config_rec, "jobcluster:jobcluster_1")
    assert result.get("instance_profile_arn") == "arn:aws:iam::999999999999:instance-profile/some_profile"


def test__get_spark_options(global_config_rec):
    result = Inventory._get_spark_options(global_config_rec, "jobcluster:jobcluster_1")
    assert result.get("spark.databricks.io.cache.enabled") == "true"


def test__get_cloud_provider(global_config_rec):
    result = Inventory._get_cloud_provider(global_config_rec, "dummy_workspace")
    assert result == "aws"


def test__get_user_option(monkeypatch):
    monkeypatch.setattr('builtins.input', lambda _: "1")
    result = Inventory._get_user_option(["one", "two"], "enter number")
    assert result == (1, "one")


def test__is_delta_nb():
    result = Inventory._is_delta_nb("delta")
    assert result is True


def test__get_notebook_paths():
    result = Inventory._get_notebook_paths("source", "deploy", "delta", "aws", "cloudtrail")
    assert result[0].endswith('deploy/delta/aws/cloudtrail/')
    result = Inventory._get_notebook_paths("stage", "deploy", "delta", "aws", "cloudtrail")
    assert result[0].endswith('deploy/delta/ingest/')


def test_get_notebook_files(monkeypatch):
    monkeypatch.setattr('os.listdir', lambda _: "notebook1.py")
    result = Inventory._get_notebook_files(['samples'])
    assert not result


def test__get_storage_location():
    result = Jobs()._get_storage_location('/scratch_dir')
    assert result == "/scratch_dir/dlt_runs"
    result = Jobs()._get_storage_location('scratch_dir')
    assert result == "/scratch_dir/dlt_runs"


def test__get_cluster_key(master_inventory):
    result = Jobs()._get_cluster_key(master_inventory[0])
    assert result == "cluster:AlwaysOnCluster"


def test__get_default_cluster(master_inventory):
    result = Jobs()._get_default_cluster("interactive", master_inventory[0]['clusters'])
    assert result.get("default") == "true"
    result = Jobs()._get_default_cluster("job", master_inventory[0]['clusters'])
    assert not result


def test__make_maintenance_task_json(maintenance_inventory):
    result = Jobs()._make_maintenance_task_json(maintenance_inventory)
    expected = {'maintenance_jobs': [
        {'task_name': 'Maintenance', 'datasource_name': 'Maintenance', 'description': 'a task description',
         'language': 'python', 'isDeltaStreaming': 'true', 'aws_attributes': {
            'instance_profile_arn': 'arn:aws:iam::755921336062:instance-profile/veuve-cloud-databricks-com-primary-role'},
         'azure_attributes': {}, 'gcp_attributes': {}, 'whl_path': 'lib/databricks-sirens.whl',
         'spark_conf': {'sirens_wheel': 'latest', 'spark.databricks.io.cache.enabled': 'true',
                        'spark.databricks.io.cache.maxdiskusage': '"50g"',
                        'spark.databricks.io.cache.maxmetadatacache': '"1g"',
                        'spark.databricks.isv.product': 'databricks-sirens'}, 'uses_existing_cluster': False,
         'tags': {'sirens': 'sirens'}, 'cluster': {'cluster_key': 'maintenance', 'runtime_engine': 'PHOTON',
                                                   'autoscale': {'min_workers': '2', 'max_workers': '6'}},
         'schedule': {'quartz_cron_expression': '0 0 13 * * ?', 'timezone_id': 'BST'}, 'tasks': [
            {'task_key': 'okta_oktaIM2_log_maintenance', 'job_cluster_key': 'maintenance',
             'notebook_path': '/deploy/delta/okta/oktaIM2_log/06-okta-oktaIM2_log-maintenance.py',
             'task_params': {'source': 'okta', 'sourcetype': 'oktaIM2_log', 'database': 'sirens'}}]}]}
    assert result == expected


def test__get_cluster_opts(master_inventory):
    result = Jobs()._get_cluster_opts(master_inventory[0])
    expected = {'cloud_provider': None, 'compute_attributes': {},
                'spark_options': {'spark.databricks.isv.product': 'databricks-sirens'},
                'cluster_id': '9999-999999-xxxxxxxx', 'autoscale': {'min_workers': '1', 'max_workers': '5'},
                'runtime_engine': 'PHOTON'}
    assert result == expected


def test__set_csp_attrs():
    compute_attributes = {"instance_profile": "xxx"}
    provider = "aws"
    aws_result, azure_result, gcp_result = Jobs()._set_csp_attrs(provider, compute_attributes)
    assert not azure_result
    assert not gcp_result
    assert aws_result.get("instance_profile") == "xxx"


def test__make_dlt_jobs_json(master_inventory):
    result = Jobs()._make_dlt_jobs_json(master_inventory)
    expected = {'dlt_jobs': [{'task_name': 'aws_vpc_flowlogs', 'storage_location': '/FileStore/sirens/dlt_runs',
                              'datasource_name': 'aws_vpc_flowlogs', 'target_database': 'sirens', 'edition': 'advanced',
                              'channel': 'current', 'photon': 'true', 'continuous': 'false', 'isDeltaStreaming': 'true',
                              'aws_attributes': {}, 'azure_attributes': {}, 'gcp_attributes': {},
                              'whl_path': 'lib/databricks-sirens.whl',
                              'spark_conf': {'spark.databricks.isv.product': 'databricks-sirens'},
                              'tags': {'app': 'sirens', 'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'product': 'dlt',
                                       'type': 'ingest'},
                              'notebook_libraries': ['/deploy/delta/aws/vpc_flowlogs/01-aws-vpc_flowlogs-ingest',
                                                     '/deploy/delta/aws/vpc_flowlogs/02-aws-vpc_flowlogs-parse',
                                                     '/deploy/delta/aws/vpc_flowlogs/03-aws-vpc_flowlogs-normalize',
                                                     '/deploy/delta/aws/vpc_flowlogs/06-aws-vpc_flowlogs-maintenance'],
                              'configuration': {
                                  'input': "'\\[{'database': sirens, 'source': aws,'sourcetype': vpc_flowlogs}\\]'",
                                  'pipeline_refresh': True},
                              'schedule': {'quartz_cron_expression': '0 0 13 * * ?', 'timezone_id': 'GMT'}, 'cluster': [
            {'label': 'default', 'custom_tags': {'cluster_type': 'sirens'},
             'autoscale': {'min_workers': '1', 'max_workers': '5', 'mode': 'ENHANCED'}}]}]}
    assert result == expected


def test_create_dlt_job_tf(master_inventory):
    result = Jobs().create_dlt_job_tf(master_inventory)
    expected = {'dlt_jobs': [{'task_name': 'aws_vpc_flowlogs', 'storage_location': '/FileStore/sirens/dlt_runs',
                              'datasource_name': 'aws_vpc_flowlogs', 'target_database': 'sirens', 'edition': 'advanced',
                              'channel': 'current', 'photon': 'true', 'continuous': 'false', 'isDeltaStreaming': 'true',
                              'aws_attributes': {}, 'azure_attributes': {}, 'gcp_attributes': {},
                              'whl_path': 'lib/databricks-sirens.whl',
                              'spark_conf': {'spark.databricks.isv.product': 'databricks-sirens'},
                              'tags': {'app': 'sirens', 'source': 'aws', 'sourcetype': 'vpc_flowlogs', 'product': 'dlt',
                                       'type': 'ingest'},
                              'notebook_libraries': ['/deploy/delta/aws/vpc_flowlogs/01-aws-vpc_flowlogs-ingest',
                                                     '/deploy/delta/aws/vpc_flowlogs/02-aws-vpc_flowlogs-parse',
                                                     '/deploy/delta/aws/vpc_flowlogs/03-aws-vpc_flowlogs-normalize',
                                                     '/deploy/delta/aws/vpc_flowlogs/06-aws-vpc_flowlogs-maintenance'],
                              'configuration': {
                                  'input': "'\\[{'database': sirens, 'source': aws,'sourcetype': vpc_flowlogs}\\]'",
                                  'pipeline_refresh': True},
                              'schedule': {'quartz_cron_expression': '0 0 13 * * ?', 'timezone_id': 'GMT'}, 'cluster': [
            {'label': 'default', 'custom_tags': {'cluster_type': 'sirens'},
             'autoscale': {'min_workers': '1', 'max_workers': '5', 'mode': 'ENHANCED'}}]}]}
    assert result == expected


def test_create_maintenance_tasks_tf(maintenance_inventory):
    result = Jobs().create_maintenance_tasks_tf(maintenance_inventory)
    expected = {'maintenance_jobs': [
        {'task_name': 'Maintenance', 'datasource_name': 'Maintenance', 'description': 'a task description',
         'language': 'python', 'isDeltaStreaming': 'true', 'aws_attributes': {
            'instance_profile_arn': 'arn:aws:iam::755921336062:instance-profile/veuve-cloud-databricks-com-primary-role'},
         'azure_attributes': {}, 'gcp_attributes': {}, 'whl_path': 'lib/databricks-sirens.whl',
         'spark_conf': {'sirens_wheel': 'latest', 'spark.databricks.io.cache.enabled': 'true',
                        'spark.databricks.io.cache.maxdiskusage': '"50g"',
                        'spark.databricks.io.cache.maxmetadatacache': '"1g"',
                        'spark.databricks.isv.product': 'databricks-sirens'}, 'uses_existing_cluster': False,
         'tags': {'sirens': 'sirens'}, 'cluster': {'cluster_key': 'maintenance', 'runtime_engine': 'PHOTON',
                                                   'autoscale': {'min_workers': '2', 'max_workers': '6'}},
         'schedule': {'quartz_cron_expression': '0 0 13 * * ?', 'timezone_id': 'BST'}, 'tasks': [
            {'task_key': 'okta_oktaIM2_log_maintenance', 'job_cluster_key': 'maintenance',
             'notebook_path': '/deploy/delta/okta/oktaIM2_log/06-okta-oktaIM2_log-maintenance.py',
             'task_params': {'source': 'okta', 'sourcetype': 'oktaIM2_log', 'database': 'sirens'}}]}]}
    assert result == expected


def test_read_yaml_file():
    result = Inventory._read_yaml_file("tests/data/yaml/rule_dict.yaml")
    assert isinstance(result, dict)


def test_read_yaml_file_non_existent():
    with pytest.raises(FileNotFoundError):
        result = Inventory._read_yaml_file("tests/data/yaml/rule_dict_non_existent.yaml")
        assert result


def test_create_intel_tf(master_inventory):
    result = Jobs().create_intel_tf(master_inventory)
    assert isinstance(result, dict)


def test_create_dlt_intel_tf(master_inventory):
    #with pytest.raises(SirensConfigException):
    result = Jobs().create_dlt_intel_tf(master_inventory)
    assert isinstance(result, dict)


def test__get_intel_notebook_paths():
    result = Inventory._get_intel_notebook_paths("notebook1", "deploy")
    assert 'deploy/intel/jobs/notebook1' in result
