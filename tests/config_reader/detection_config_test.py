import os
import pytest
from databricks.sirens.config_reader import Detection

detection_sources = []
for source in os.listdir('detection/pipelines'):
    for sourcetype in os.listdir(f'detection/pipelines/{source}'):
        detection_sources.append((source, sourcetype))

envs = [name for name in os.listdir('env_vars') if os.path.isdir(os.path.join('env_vars', name))] + ['']


def test_load_pipeline_yaml():
    for env in envs:
        for source, sourcetype in detection_sources:
            config = Detection(source=source, name=sourcetype, env=env)
            pipeline_list = config.list_pipelines()

            forced_pipeline_keys = ['name', 'input_table', 'output_table', 'rule_dir', 'enabled', 'streamType']
            forced_clustering_keys = ['name', 'input_table', 'grouping_keys', 'rule_dir', 'streamType']

            for pipeline in pipeline_list:
                raw_yaml = config._get_detection_yaml(pipeline)
                try:
                    pipeline_configs = config._load_pipeline_yaml(pipeline)
                except Exception as e:
                    print(f"Could not load pipeline config for env {env}: {e}")
                    if 'not found in config' in str(e):
                        continue
                    else:
                        raise
                pipeline_keys = list(pipeline_configs.keys())

                if source.lower() == 'correlation' and pipeline.lower() == 'clustering':
                    for key in forced_clustering_keys:
                        assert key in pipeline_keys
                else:
                    for key in forced_pipeline_keys:
                        assert key in pipeline_keys

def test_load_pipeline_with_transformation():
    config = Detection(source="apache", name="combined_access")
    pipeline = config._load_pipeline_yaml(pipeline_name="apache_transform_pipeline")
    assert "transformation_sql" in pipeline

def test_load_pipeline_with_transformation_sql():
    config = Detection(source="apache", name="combined_access")
    pipeline = config._load_pipeline_yaml(pipeline_name="apache_transformsql_pipeline")
    assert "transformation_sql" in pipeline and pipeline["transformation_sql"] == """SELECT * FROM sirens.apache_access_combined_silver WHERE _event_date > CURRENT_DATE - 1"""
    

def test_populate_transformation_no_input_table():
    detection_pipeline = {
        "name": "detection_pipeline",
        "output_table": "catalog.schema.alerts",
        "rule_dir": [],
        "enabled": True,
        "streamType": "batch",
        "dlt_read": False,
        "transformation": "join_network_with_processes",
        "rules": [{"name": "user_attaches_role_policy", "enabled": True}]
    }
    transform_sql = """WITH network_events AS (
        SELECT
            user_id,
            process_id,
            protocol,
            length,
            domain,
            hostname
        FROM lyu_catalog.nab.network_events
        WHERE protocol = 'TCP'
          AND length > 200
    ),
    process_events AS (
        SELECT
            process_id,
            event_type,
            hostname,
            user_id
        FROM lyu_catalog.nab.process_events
        WHERE event_type = 'CREATE_PROCESS'
    ),
    domain_threat_intel AS (
        SELECT
            domain,
            threat_level
        FROM lyu_catalog.nab.domain_threat_intel
    ),
    join_process_and_network AS (
        SELECT
            ne.user_id,
            ne.process_id,
            ne.hostname,
            ne.domain,
            pe.event_type
        FROM network_events ne
        JOIN process_events pe
            ON ne.process_id = pe.process_id
    ),
    combined_events AS (
        SELECT
            user_id,
            process_id,
            hostname,
            domain,
            COUNT(DISTINCT hostname) AS unique_hosts,
            COUNT(*) AS event_count
        FROM join_process_and_network
        GROUP BY user_id, process_id, hostname, domain
    ),
    final_join AS (
        SELECT
            ce.user_id,
            ce.process_id,
            ce.hostname,
            ce.domain,
            ce.unique_hosts,
            ce.event_count,
            dti.threat_level,
            CURRENT_DATE() AS _event_date
        FROM combined_events ce
        LEFT JOIN domain_threat_intel dti
            ON ce.domain = dti.domain
    )
    SELECT
        _event_date,
        user_id,
        COUNT(*) AS activity_count
    FROM final_join
    GROUP BY _event_date, user_id"""
    detection = Detection(source="source", name="pipeline_name")
    detection._populate_transform_sql(detection_pipeline, transform_sql)
    assert detection_pipeline["transformation_sql"] == transform_sql

def test_populate_transformation_input_table():
    detection_pipeline = {
        "name": "detection_pipeline",
        "output_table": "catalog.schema.alerts",
        "input_table": "catalog.schema.networks",
        "rule_dir": [],
        "enabled": True,
        "streamType": "batch",
        "dlt_read": False,
        "transformation": "join_network_with_processes",
        "rules": [{"name": "user_attaches_role_policy", "enabled": True}]
    }
    transform_sql = """SELECT user_id, process_id, protocol, length, domain, hostname FROM {{input_table}} WHERE protocol = 'TCP' AND length > 200"""
    detection = Detection(source="source", name="pipeline_name")
    detection._populate_transform_sql(detection_pipeline, transform_sql)
    assert detection_pipeline["transformation_sql"] == """SELECT user_id, process_id, protocol, length, domain, hostname FROM catalog.schema.networks WHERE protocol = 'TCP' AND length > 200"""

@pytest.mark.usefixtures("spark_session")
def test_load_pipeline_ruleset(spark_session):
    for env in envs:
        for source, sourcetype in detection_sources:
            config = Detection(source=source, name=sourcetype, env=env)
            pipeline_list = config.list_pipelines()

            for pipeline in pipeline_list:
                if source == 'correlation' and pipeline == 'clustering':
                    continue
                try:
                    pipeline_configs = config._load_pipeline_yaml(pipeline)
                except Exception as e:
                    print(f"Could not load pipeline config for env {env}: {e}")
                    if 'not found in config' in str(e):
                        continue
                    else:
                        raise

                try:
                    ruleset = config._load_pipeline_ruleset(pipeline_configs)
                except Exception as e:
                    print(f"Could not load ruleset config for env {env}: {e}")
                    if 'not found in config' in str(e):
                        continue
                    else:
                        raise

                assert len(ruleset.rules) > 0
