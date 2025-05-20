import tempfile

from tests.conftest import _get_top_level_directory
from tests.utils import *

from databricks.sirens.detection import *


@pytest.mark.usefixtures("spark_session")
def test_from_yaml(spark_session):
    yaml_str = '''
    name: excessive_account_lockouts
    summary: <alternateId> had their account locked out >= <count> times within 1 hour on <oktaURL>
    severity: medium
    source: okta
    sourceDetails: <oktaURL>
    alertClass: ALERT
    ruleVersion: 1.0
    eventTime: <hour_timestamp>
    alertedTime: <hour_timestamp>
    rawTime: <hour_timestamp>
    actor:
      type: HOST
      domain: Public Internet
      id: <ipAddresses>
      beliefCompromised: 0.1
    target:
      type: USER
      domain: <oktaURL>
      id: <alternateId>
      beliefCompromised: 0.1
    attacks:
    - mitre:
        taxonomy: ENTERPRISE
        tactic: CREDENTIAL_ACCESS
        techniqueId: T1110
        technique: Brute Force
    observables:
      ipAddresses:
      - [ipAddresses]
      domains: []
      fileHashes: []
      urls: []
    context:
      Okta URL: <oktaURL>
      eventType: <eventType>
      ipAddresses: <ipAddresses>
      lockouts: <count>
      userAgents: <userAgents>
      who: <alternateId>
    filter: |-
      count >= 3 AND
      eventType = "user.account.lock"
    '''
    ruleset = DetectionRuleset.from_yaml(yaml_str)
    assert len(ruleset.rules) == 1
    rule = ruleset.rules['excessive_account_lockouts']
    assert rule.original_fields['severity'] == 'medium'
    assert rule.original_fields['context'] == {
        'Okta URL': '<oktaURL>',
        'eventType': '<eventType>',
        'ipAddresses': '<ipAddresses>',
        'lockouts': '<count>',
        'userAgents': '<userAgents>',
        'who': '<alternateId>',
    }
    assert rule.original_fields[
               'summary'] == '<alternateId> had their account locked out >= <count> times within 1 hour on <oktaURL>'
    assert rule.original_fields['filter'] == 'count >= 3 AND\neventType = "user.account.lock"'


@pytest.mark.usefixtures("spark_session")
def test_load_yaml(spark_session):
    path = _get_top_level_directory() + '/tests/data/yaml/rules_list.yaml'
    ruleset = DetectionRuleset.load_yaml(path)
    assert len(ruleset.rules) == 2

    assert ('excessive_account_lockouts' in ruleset.rules) == True
    assert ('signin_policy_modified' in ruleset.rules) == True

    path = _get_top_level_directory() + '/tests/data/yaml/rule_dict.yaml'
    ruleset = DetectionRuleset.load_yaml(path)
    assert len(ruleset.rules) == 1
    assert ('excessive_account_lockouts' in ruleset.rules) == True


@pytest.mark.usefixtures("spark_session")
def test_to_yaml(spark_session):
    ruleset = DetectionRuleset(
        RuleEntity(
            name='Excessive Account Lockouts',
            summary='<alternateId> had their account locked out >= <count> times within 1 hour on <oktaURL>',
            severity='medium',
            source='',
            sourceDetails='',
            alertClass="ALERT",
            ruleVersion='',
            eventTime='',
            alertedTime='',
            rawTime='',
            actor='',
            target='',
            attacks='',
            observables='',
            context={
                'Okta URL': '<oktaURL>',
                'who': '<alternateId>',
                'eventType': '<eventType>',
                'lockouts': '<count>',
            },
            filter='''
            count >= 3 AND
            eventType = "user.account.lock"
            '''
        )
    )
    yaml_str = DetectionRuleset.to_yaml(ruleset)
    rules = yaml.load(yaml_str, Loader=Loader)
    assert len(rules) == 1
    assert rules[0]['name'] == 'Excessive Account Lockouts'
    assert rules[0]['severity'] == 'medium'
    assert rules[0][
               'summary'] == '<alternateId> had their account locked out >= <count> times within 1 hour on <oktaURL>'
    assert rules[0]['context'] == {
        'Okta URL': '<oktaURL>',
        'who': '<alternateId>',
        'eventType': '<eventType>',
        'lockouts': '<count>',
    }
    assert normalize_string(rules[0]['filter']) == 'count >= 3 AND eventType = "user.account.lock"'


@pytest.mark.usefixtures("spark_session")
def test_dump_yaml(spark_session):
    ruleset = DetectionRuleset(
        RuleEntity(
            name='Excessive Account Lockouts',
            summary='<alternateId> had their account locked out >= <count> times within 1 hour on <oktaURL>',
            severity='medium',
            source='',
            sourceDetails='',
            alertClass="ALERT",
            ruleVersion='',
            eventTime='',
            alertedTime='',
            rawTime='',
            actor='',
            target='',
            attacks='',
            observables='',
            context={
                'Okta URL': '<oktaURL>',
                'who': '<alternateId>',
                'eventType': '<eventType>',
                'lockouts': '<count>',
            },
            filter='''
            count >= 3 AND
            eventType = "user.account.lock"
            '''
        )
    )
    with tempfile.NamedTemporaryFile() as outf:
        DetectionRuleset.dump_yaml(ruleset, outf.name)
        with open(outf.name, 'r') as inf:
            rules = yaml.load(inf, Loader=Loader)
            assert len(rules) == 1
            assert rules[0]['name'] == 'Excessive Account Lockouts'
            assert rules[0]['severity'] == 'medium'
            assert rules[0][
                       'summary'] == '<alternateId> had their account locked out >= <count> times within 1 hour on <oktaURL>'
            assert rules[0]['context'] == {
                'Okta URL': '<oktaURL>',
                'who': '<alternateId>',
                'eventType': '<eventType>',
                'lockouts': '<count>',
            }

            assert normalize_string(rules[0]['filter']) == 'count >= 3 AND eventType = "user.account.lock"'
