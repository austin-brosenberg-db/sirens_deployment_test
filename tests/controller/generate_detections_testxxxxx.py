import os
from pathlib import Path

import pytest

import shutil
import configparser

from databricks.sirens import config_reader
from databricks.sirens.config_reader import Detection
from databricks.sirens.controller import Controller
from databricks.sirens.config_reader import GlobalConfig


detection_sources = {}
for source in os.listdir('detection/pipelines'):
    detection_sources[source] = {}
    for sourcetype in os.listdir(f'detection/pipelines/{source}'):
        detection_sources[source][sourcetype] = {}

config = GlobalConfig.read()

config['detections']['deploy_dir'] = '/tmp/deploy_test'

for nb_type in ['dlt', 'delta']:
    config['default']['notebook_type'] = nb_type

    for source in detection_sources.keys():
        for sourcetype in detection_sources[source].keys():
            config[f'input:{source}:{sourcetype}']['enabled'] = 'true'
            detectionObj = Detection(source, sourcetype)
            pipeline_list = detectionObj.list_pipelines()

            pipeline_union = ""
            for idx, pipeline_yaml in enumerate(pipeline_list):
                pipeline_config = detectionObj._load_pipeline_yaml(pipeline_yaml)
                detection_sources[source][sourcetype][pipeline_yaml] = pipeline_config.get("enabled", True)

    controller_obj = Controller(config)
    controller_obj.generate_detections()


@pytest.mark.usefixtures("spark_session")
def test_enabled_preprocessing_generation(spark_session):
    for nb_type in ['dlt', 'delta']:
        for source in detection_sources.keys():
            for sourcetype in detection_sources[source].keys():
                for pipeline in detection_sources[source][sourcetype].keys():
                    if pipeline == 'clustering':
                        continue

                    detectionObj = config_reader.Detection(source, sourcetype)._load_pipeline_yaml(pipeline)

                    for dependency in detectionObj.get('depends_on', []):
                        preprocessing_source = f'detection/notebooks/{nb_type}/{source}/{sourcetype}/{dependency}.py'
                        if Path(preprocessing_source).is_file() and detection_sources[source][sourcetype][pipeline] is True:
                            base_dir = f"{config['detections']['deploy_dir']}/detection/{nb_type}"
                            assert Path(
                                f'{base_dir}/{source}/{sourcetype}/{dependency}.py'
                            ).is_file() is True


def test_enabled_pipeline_generation():
    for nb_type in ['dlt', 'delta']:
        for source in detection_sources.keys():
            for sourcetype in detection_sources[source].keys():
                for pipeline in detection_sources[source][sourcetype].keys():
                    if pipeline == 'clustering' and nb_type == 'delta':
                        continue

                    if detection_sources[source][sourcetype][pipeline] is True:
                        base_dir = f"{config['detections']['deploy_dir']}/detection/{nb_type}"

                        if pipeline.endswith('_detections') is False and pipeline not in ['clustering', 'alerts_writer', 'global_installations']:
                            pipeline += '_detections'

                        assert Path(
                            f"{base_dir}/{source}/{sourcetype}/{pipeline}.py"
                        ).is_file() is True



def test_tearDown():
    shutil.rmtree('/tmp/deploy_test')
