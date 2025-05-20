import os
import json
import yaml
from yaml import Loader
from pathlib import Path

from databricks.sirens.utils.config_utils import *
from tests.utils import *

from databricks.sirens.detection.detector import Detector

from databricks.sirens.detection import *
from databricks.sirens.utils.base_utils import *
from databricks.sirens.utils.path_utils import *

MODULE_NAME = "YAMLRules"


def create_detector(rule_dict):
    rules = DetectionRuleset(
        RuleEntity(
            name=rule_dict.get('name'),
            summary=rule_dict.get('summary'),
            severity=rule_dict.get('severity'),
            source=rule_dict.get('source'),
            sourceDetails=rule_dict.get('sourceDetails'),
            alertClass=rule_dict.get('alertClass'),
            ruleVersion=rule_dict.get('ruleVersion'),
            eventTime=rule_dict.get('eventTime'),
            alertedTime=rule_dict.get('alertedTime'),
            rawTime=rule_dict.get('rawTime'),
            actor=rule_dict.get('actor'),
            target=rule_dict.get('target'),
            attacks=rule_dict.get('attacks'),
            observables=rule_dict.get('observables'),
            context=rule_dict.get('context'),
            sourceUuids=rule_dict.get('sourceUuids'),
            risk=rule_dict.get('risk'),
            riskScale=rule_dict.get('riskScale'),
            filter=rule_dict.get('filter')
        )
    )

    return Detector(output_path="/", detection_rules=rules, module_name=MODULE_NAME)


def create_dataframe(spark_session, args):
    rdd = spark_session.sparkContext.parallelize([json.dumps(args)])
    df = spark_session.read.json(rdd)
    df = (df
          .withColumn('__ts', lit('2022-10-01T12:12:12.001Z'))
          .withColumn('__dt', lit('2022-10-01'))
          .withColumn('__uuid', lit('abcdef'))
          )
    return df


def validate_attack_format(attack):
    fields = {'mitre', 'killchain', 'other'}
    attack_fields = set(attack.keys())
    assert attack_fields.difference(fields) == set()

    valid_mitre_fields = {'taxonomy', 'tactic', 'techniqueId', 'technique', 'subtechniqueId', 'subtechnique'}
    mitre = attack.get('mitre', {})
    mitre_fields = set(mitre.keys())
    assert mitre_fields.difference(valid_mitre_fields) == set()

    valid_killchain_fields = {'stage'}
    killchain = attack.get('killchain', {})
    killchain_fields = set(killchain.keys())
    assert killchain_fields.difference(valid_killchain_fields) == set()


def validate_attacks_format(rule_dict):
    if 'attacks' in rule_dict:
        for attack in rule_dict['attacks']:
            validate_attack_format(attack)


def validate_observables_format(rule_dict):
    if 'observables' in rule_dict:
        fields = {'ipAddresses', 'domains', 'fileHashes', 'urls', 'filePaths', 'processes'}
        observable_fields = set(rule_dict['observables'].keys())
        assert observable_fields.difference(fields) == set()


def validate_actor_format(rule_dict):
    if 'actor' in rule_dict:
        fields = {'type', 'domain', 'id', 'role', 'beliefCompromised'}
        actor_fields = set(rule_dict['actor'].keys())
        assert actor_fields.difference(fields) == set()


def validate_target_format(rule_dict):
    if 'target' in rule_dict:
        fields = {'type', 'domain', 'id', 'role', 'beliefCompromised'}
        target_fields = set(rule_dict['target'].keys())
        assert target_fields.difference(fields) == set()


def validate_enhanced_rule_format(rule_dict):
    mandatory = [
        'name',
        'summary',
        'eventTime',
        'severity',
        'source',
        'alertClass',
        'ruleVersion',
        'filter',
    ]

    optional = [
        'actor',
        'eventTime',
        'target',
        'sourceDetails',
        'attacks',
        'observables',
        'context',
        'alertedTime',
        'rawTime',
        'test_cases',
        'uuid',
        'risk',
        'riskScale',
        'time_window',
        'compound_logic',
        'event_window',
        'collect'
    ]

    for field in rule_dict:
        assert field in rule_dict, f'Missing mandatory field {field}: rule_dict={rule_dict}'

    valid_fields = set(mandatory + optional)
    assert set(rule_dict.keys()).difference(valid_fields) == set()

    validate_actor_format(rule_dict)
    validate_target_format(rule_dict)
    validate_observables_format(rule_dict)
    validate_attacks_format(rule_dict)


def execute_test_cases(spark_session, rule_file, rule_dict, test_cases, env):
    rule_name = rule_dict['name']
    detector = create_detector(rule_dict)

    for test_case in test_cases:
        error_message = f"Rule: {rule_file}:'{rule_name}', Test case: {test_case['test_name']} failed, env: {env}"
        df = create_dataframe(spark_session, test_case['test_input'])

        try:
            alert_df = detector.detect(df)
        except Exception as e:
            pytest.fail(
                f"Failed running detector on rule: {rule_file}, test_input: {test_case['test_input']}, env: {env}, Exception: {e}")
            continue

        df_rdd = alert_df.collect()
        if test_case['expected_result'] == True:
            assert len(df_rdd) == 1, error_message

            if 'expected_context' in test_case and test_case['expected_context']:
                assert df_rdd[0].context == test_case['expected_context'], \
                    f'{error_message} - Context differs'

            if 'expected_summary' in test_case and test_case['expected_summary']:
                assert df_rdd[0].summary == test_case['expected_summary'], \
                    f'{error_message} - Summary differs'

            if 'expected_severity' in test_case and test_case['expected_severity']:
                assert df_rdd[0].severity == test_case['expected_severity'], \
                    f'{error_message} - Severity differs'

            assert df_rdd[0].eventTime is not None, \
                    f'{error_message} - Failed to extract timestamp (timestamp field may be missing or wrong)'

            assert df_rdd[0].eventDate is not None, \
                    f'{error_message} - Failed to extract date (timestamp field may be missing or wrong)'

        else:
            assert len(df_rdd) == 0, error_message

def _testing_yaml_file(file: str, spark_session):
    print(f'Testing YAML rules in {file:}')
    with open(file, 'rt') as yaml_file:
        base_filename = os.path.basename(file)
        try:
            rule_dicts = yaml.load(yaml_file, Loader=Loader)
        except Exception as e:
            pytest.fail(f'Failed to load YAML file: {file}. Exception: {e}')
            return

        if isinstance(rule_dicts, dict):
            rule_dicts = [rule_dicts]

        for rule_dict in rule_dicts:
            validate_enhanced_rule_format(rule_dict)

            test_cases = rule_dict.get('test_cases', [])
            if test_cases:
                execute_test_cases(spark_session, base_filename, rule_dict, test_cases)
            else:
                print(f"WARN: Rule: {base_filename}:'{rule_dict['name']}' has no test cases defined")

@pytest.mark.usefixtures("spark_session", "rulename")
def test_yaml_rules(spark_session, rulename):
    detection_prefix = None
    for path in PathUtils.get_relative_file_paths(["detection", "rules"]):
        if Path(path).is_dir():
            detection_prefix = path
            break
    if detection_prefix is None:
        raise SirensConfigException("rule directory not found")

    envs = [name for name in os.listdir('env_vars') if os.path.isdir(os.path.join('env_vars', name))] + ['']

    for env in envs:
        path = detection_prefix + '/**/*.yaml'
        for file in glob.glob(path, recursive=True):
            if rulename and rulename not in file:
                continue

            print(f'Testing YAML rules in {file} for env: {env}')
            with open(file, 'rt') as yaml_file:
                base_filename = os.path.basename(file)
                try:
                    rule_dicts = yaml.load(yaml_file, Loader=Loader)
                    rule_dicts = config_utils.sub_yaml_vars(rule_dicts, env)
                except Exception as e:
                    print(f"Could not load ruleset for env {env}: {e}")
                    if 'not found in config' in str(e):
                        continue
                    else:
                        raise

                if isinstance(rule_dicts, dict):
                    rule_dicts = [rule_dicts]

                for rule_dict in rule_dicts:
                    validate_enhanced_rule_format(rule_dict)

                    test_cases = rule_dict.get('test_cases', [])
                    if test_cases:
                        execute_test_cases(spark_session, base_filename, rule_dict, test_cases, env)
                    else:
                        print(f"WARN: Rule: {base_filename}:'{rule_dict['name']}' has no test cases defined for env {env}")
