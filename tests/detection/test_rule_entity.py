import json
from datetime import datetime, timedelta

import pyspark
from pyspark.sql.functions import explode, lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, TimestampType

from databricks.sirens.exceptions import SirensDetectionException
from tests.utils import *
from databricks.sirens.detection import *


@pytest.mark.usefixtures("spark_session")
def test_alertedTime_empty(spark_session):
    rule = RuleEntity(
        name="rule1",
        summary="this is rule1",
        severity="medium",
        source="test",
        sourceDetails="",
        alertClass="UNDEFINED",
        ruleVersion="",
        eventTime="",
        alertedTime="",
        rawTime="",
        actor="",
        target="",
        sourceUuids=None,
        risk=None,
        riskScale=None,
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="1=1"
    )

    assert str(rule.pyspark_fields['alertedTime']) == str(expr("CURRENT_TIMESTAMP"))


def test_alertedTime_None(spark_session):
    rule = RuleEntity(
        name="rule1",
        summary="this is rule1",
        severity="medium",
        source="test",
        sourceDetails="",
        alertClass="UNDEFINED",
        ruleVersion="",
        eventTime="",
        alertedTime=None,
        rawTime="",
        actor="",
        target="",
        sourceUuids=None,
        risk=None,
        riskScale=None,
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="1=1"
    )

    assert str(rule.pyspark_fields['alertedTime']) == str(expr("CURRENT_TIMESTAMP"))


@pytest.mark.usefixtures("spark_session")
def test_alertedTime_filled(spark_session):
    rule = RuleEntity(
        name="rule1",
        summary="this is rule1",
        severity="medium",
        source="test",
        sourceDetails="",
        alertClass="UNDEFINED",
        ruleVersion="",
        eventTime="",
        alertedTime="<timestamp>",
        rawTime="",
        actor="",
        target="",
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="1=1"
    )

    assert str(rule.pyspark_fields['alertedTime']) == "Column<'timestamp'>"


@pytest.mark.usefixtures("spark_session")
def test_severity_lit(spark_session):
    rule = RuleEntity(
        name="rule1",
        summary="this is rule1",
        severity="medium",
        source="test",
        sourceDetails="",
        alertClass="UNDEFINED",
        ruleVersion="",
        eventTime="",
        alertedTime="<timestamp>",
        rawTime="",
        actor="",
        target="",
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="1=1"
    )

    assert str(rule.pyspark_fields['severity']) == "Column<'medium'>"


@pytest.mark.usefixtures("spark_session")
def test_severity_col(spark_session):
    rule = RuleEntity(
        name="rule1",
        summary="this is rule1",
        severity="<sev_column>",
        source="test",
        sourceDetails="",
        alertClass="UNDEFINED",
        ruleVersion="",
        eventTime="",
        alertedTime="<timestamp>",
        rawTime="",
        actor="",
        target="",
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="1=1"
    )

    assert str(rule.pyspark_fields['severity']) == "Column<'sev_column'>"


@pytest.mark.usefixtures("spark_session")
def rule_entity(spark_session):
    return RuleEntity(
        name="rule1",
        summary="this is rule1 triggered by <user>",
        severity="medium",
        source="test",
        sourceDetails="",
        alertClass="UNDEFINED",
        ruleVersion="",
        eventTime="<published>",
        alertedTime="",
        rawTime="",
        actor="",
        target="",
        sourceUuids=None,
        risk=None,
        riskScale=None,
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="user NOT RLIKE 'bad'"
    )


@pytest.mark.usefixtures("spark_session")
def test_field_expr_sub_normal(spark_session):
    _rule_entity = rule_entity(spark_session)

    string = 'source RLIKE "databricks" OR (source RLIKE "the source _" AND name RLIKE "db")'

    source_sub = _rule_entity._field_expr_sub('source', string)
    assert source_sub == 'x.source RLIKE "databricks" OR (x.source RLIKE "the source _" AND name RLIKE "db")'

    name_sub = _rule_entity._field_expr_sub('name', string)
    assert name_sub == 'source RLIKE "databricks" OR (source RLIKE "the source _" AND x.name RLIKE "db")'

    source_name_sub = _rule_entity._field_expr_sub('name', source_sub)
    assert source_name_sub == 'x.source RLIKE "databricks" OR (x.source RLIKE "the source _" AND x.name RLIKE "db")'


@pytest.mark.usefixtures("spark_session")
def test_field_expr_sub_array(spark_session):
    _rule_entity = rule_entity(spark_session)

    string = 'attacks[].mitre.technique RLIKE "T1 attacks[] _" AND observables.ipAddresses[] LIKE "255.%" AND attacks[].mitre.technique RLIKE "attacks[]"'

    attacks_sub = _rule_entity._field_expr_sub('attacks[]', string)
    assert attacks_sub == 'SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "T1 attacks[] _")) > 0 AND observables.ipAddresses[] LIKE "255.%" AND SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "attacks[]")) > 0'

    observables_sub = _rule_entity._field_expr_sub('observables.ipAddresses[]', string, '>= 1')
    assert observables_sub == 'attacks[].mitre.technique RLIKE "T1 attacks[] _" AND SIZE(FILTER(x.observables.ipAddresses, x2 -> x2 LIKE "255.%")) >= 1 AND attacks[].mitre.technique RLIKE "attacks[]"'

    attacks_observables_sub = _rule_entity._field_expr_sub('observables.ipAddresses[]', attacks_sub)
    assert attacks_observables_sub == 'SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "T1 attacks[] _")) > 0 AND SIZE(FILTER(x.observables.ipAddresses, x2 -> x2 LIKE "255.%")) > 0 AND SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "attacks[]")) > 0'


@pytest.mark.usefixtures("spark_session")
def cluster_df(spark_session):
    default_vals = {
        'eventTime': datetime.now() - timedelta(days=5),
        'eventDate': datetime.today() - timedelta(days=5),
        'alertClass': 'ALERT',
        'ruleVersion': '1',
        'filter': '1=1',
        'risk': 1.5,
        'observables': {
            'ipAddresses': ['255.0.0.0', '0.0.0.0']
        },
        'attacks': [
            {'mitre': {'technique': 'T01'}}
        ],
        'moduleName': 'testing'
    }
    rows = [
        {
            'key': 'user@databricks.com',
            'alerts': [
                {
                    'name': 'alert1',
                    'summary': 'alert1 for user1',
                    'severity': 'low',
                    'source': 'databricks',
                    'actor': {'id': 'user@databricks.com', 'beliefCompromised': 0.0},
                    **default_vals
                },
                {
                    'name': 'alert2',
                    'summary': 'alert2 for user1',
                    'severity': 'medium',
                    'source': 'databricks',
                    'actor': {'id': 'user@databricks.com', 'beliefCompromised': 0.0},
                    **default_vals
                },
                {
                    'name': 'alert3',
                    'summary': 'alert3 for user1',
                    'severity': 'low',
                    'source': 'aws',
                    'actor': {'id': 'user@databricks.com', 'beliefCompromised': 0.1},
                    **default_vals
                },
                {
                    'name': 'alert4',
                    'summary': 'alert4 for user1',
                    'severity': 'high',
                    'source': 'aws',
                    'actor': {'id': 'user@databricks.com', 'beliefCompromised': 0.2},
                    **default_vals
                },
                {
                    'name': 'alert1',
                    'summary': 'alert1 for user1',
                    'severity': 'low',
                    'source': 'databricks',
                    'actor': {'id': 'user@databricks.com', 'beliefCompromised': 0.3},
                    **default_vals
                }
            ]
        }
    ]
    schema = StructType([
        StructField('key', StringType()),
        StructField('alerts', ArrayType(AlertSchema))
    ])
    return spark_session.createDataFrame(rows, schema=schema)


@pytest.mark.usefixtures("spark_session")
def test_get_event_window(spark_session):
    def _window_query(df):
        return (
            df
            .selectExpr(
                'key',
                f'FILTER(alerts, x -> {_rule_entity._get_event_window(rule)}) AS alerts'
            )
        )

    df = cluster_df(spark_session)
    _rule_entity = rule_entity(spark_session)

    rule = {
        'time_window': 'eventTime >= CURRENT_TIMESTAMP-INTERVAL 60 DAYS'
    }
    assert _rule_entity._get_event_window(rule) == 'x.eventTime >= CURRENT_TIMESTAMP-INTERVAL 60 DAYS'
    assert _window_query(df).select(explode('alerts')).count() == 5

    rule['event_window'] = 'source IN ("aws", "gcp", "azure")'
    assert _rule_entity._get_event_window(
        rule) == 'x.eventTime >= CURRENT_TIMESTAMP-INTERVAL 60 DAYS AND x.source IN ("aws", "gcp", "azure")'
    assert _window_query(df).select(explode('alerts')).count() == 2

    rule['time_window'] = 'eventTime >= CURRENT_TIMESTAMP-INTERVAL 2 DAYS'
    assert _window_query(df).select(explode('alerts')).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_count_normal(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'count'
    condition = 'source IN ("aws", "databricks")'
    field = None
    threshold = None

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == 'SIZE( FILTER( alerts, x -> x.source IN ("aws", "databricks") ) ) > 0'
    assert df.filter(query1).count() == 1

    threshold = '>= 6'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           'SIZE( FILTER( alerts, x -> x.source IN ("aws", "databricks") ) ) >= 6'
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_count_array(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'count'
    condition = 'attacks[].mitre.technique RLIKE "T01"'
    field = None
    threshold = None

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           'SIZE( FILTER( alerts, x -> SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "T01")) > 0 ) ) > 0'
    assert df.filter(query1).count() == 1

    threshold = '>= 6'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           'SIZE( FILTER( alerts, x -> SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "T01")) > 0 ) ) >= 6'
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_sum(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'sum'
    condition = None
    field = 'risk'
    threshold = '>= 5.2'

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           'AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN TRUE THEN acc + DOUBLE(x.risk) ELSE acc END) ) >= 5.2'
    assert df.filter(query1).count() == 1

    condition = 'source IN ("aws", "gcp", "azure")'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           'AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN x.source IN ("aws", "gcp", "azure") THEN acc + DOUBLE(x.risk) ELSE acc END) ) >= 5.2'
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_difference(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'difference'
    condition = None
    field = 'risk'
    threshold = '<= -5'

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           'AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN TRUE THEN acc - DOUBLE(x.risk) ELSE acc END) ) <= -5'
    assert df.filter(query1).count() == 1

    condition = 'source IN ("aws", "gcp", "azure")'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           'AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN x.source IN ("aws", "gcp", "azure") THEN acc - DOUBLE(x.risk) ELSE acc END) ) <= -5'
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_product(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'product'
    condition = None
    field = 'risk'
    threshold = '>= 7'

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           'AGGREGATE( alerts, DOUBLE(1), (acc, x) -> (CASE WHEN TRUE THEN acc * DOUBLE(x.risk) ELSE acc END) ) >= 7'
    assert df.filter(query1).count() == 1

    condition = 'source IN ("aws", "gcp", "azure")'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           'AGGREGATE( alerts, DOUBLE(1), (acc, x) -> (CASE WHEN x.source IN ("aws", "gcp", "azure") THEN acc * DOUBLE(x.risk) ELSE acc END) ) >= 7'
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_belief(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'belief'
    condition = None
    field = 'actor.beliefCompromised'
    threshold = '>= 0.49'

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           'BROUND( AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN TRUE THEN IF( DOUBLE(x.actor.beliefCompromised) < 0, acc + acc * DOUBLE(x.actor.beliefCompromised), acc + DOUBLE(x.actor.beliefCompromised) - acc * DOUBLE(x.actor.beliefCompromised) ) ELSE acc END) ), 3 ) >= 0.49'
    assert df.filter(query1).count() == 1

    condition = 'source IN ("aws", "gcp", "azure")'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           'BROUND( AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN x.source IN ("aws", "gcp", "azure") THEN IF( DOUBLE(x.actor.beliefCompromised) < 0, acc + acc * DOUBLE(x.actor.beliefCompromised), acc + DOUBLE(x.actor.beliefCompromised) - acc * DOUBLE(x.actor.beliefCompromised) ) ELSE acc END) ), 3 ) >= 0.49'
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_cardinality_normal(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'cardinality'
    condition = None
    field = 'name'
    threshold = '>= 4'

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           """SIZE( ARRAY_REMOVE( AGGREGATE( alerts, ARRAY(''), (acc, x) -> ARRAY_UNION( acc, (CASE WHEN TRUE THEN ARRAY(IF(x.name IS NULL, '', STRING(x.name))) ELSE ARRAY('') END) ) ), '' ) ) >= 4"""
    assert df.filter(query1).count() == 1

    condition = 'source IN ("aws", "gcp", "azure")'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           """SIZE( ARRAY_REMOVE( AGGREGATE( alerts, ARRAY(''), (acc, x) -> ARRAY_UNION( acc, (CASE WHEN x.source IN ("aws", "gcp", "azure") THEN ARRAY(IF(x.name IS NULL, '', STRING(x.name))) ELSE ARRAY('') END) ) ), '' ) ) >= 4"""
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_get_clause_cardinality_array(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)

    type = 'cardinality'
    condition = None
    field = 'attacks[].mitre.technique'
    threshold = '>= 1'

    query1 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query1) == \
           """SIZE( ARRAY_REMOVE( AGGREGATE( alerts, ARRAY(''), (acc, x) -> ARRAY_UNION( acc, (CASE WHEN TRUE THEN AGGREGATE( x.attacks, ARRAY(''), (acc2, x2) -> ARRAY_UNION(acc2, IF(x2.mitre.technique IS NULL, ARRAY(''), ARRAY(STRING(x2.mitre.technique)))) ) ELSE ARRAY('') END) ) ), '' ) ) >= 1"""
    assert df.filter(query1).count() == 1

    condition = 'source IN ("gcp", "azure")'
    query2 = _rule_entity._get_clause(type=type, condition=condition, field=field, threshold=threshold)
    assert normalize_string(query2) == \
           """SIZE( ARRAY_REMOVE( AGGREGATE( alerts, ARRAY(''), (acc, x) -> ARRAY_UNION( acc, (CASE WHEN x.source IN ("gcp", "azure") THEN AGGREGATE( x.attacks, ARRAY(''), (acc2, x2) -> ARRAY_UNION(acc2, IF(x2.mitre.technique IS NULL, ARRAY(''), ARRAY(STRING(x2.mitre.technique)))) ) ELSE ARRAY('') END) ) ), '' ) ) >= 1"""
    assert df.filter(query2).count() == 0


@pytest.mark.usefixtures("spark_session")
def test_compound_clauses(spark_session):
    _rule_entity = rule_entity(spark_session)
    df = cluster_df(spark_session)
    rule = {
        'filter': [
            {
                'type': 'count',
                'condition': 'source = ("aws")',
                'event_id': 'my_custom_id_1'
            },
            {
                'type': 'count',
                'condition': 'attacks[].mitre.technique RLIKE "T"',
                'event_id': 'my_custom_id_2'
            },
            {
                'type': 'sum',
                'field': 'risk',
                'threshold': '>= 3',
                'event_id': 'my_custom_id_3'
            },
            {
                'type': 'cardinality',
                'field': 'source',
                'threshold': '>= 2',
                'event_id': 'my_custom_id_4'
            }
        ]
    }

    clauses = [
        '''SIZE( FILTER( alerts, x -> x.source = ("aws") ) ) > 0''',
        '''SIZE( FILTER( alerts, x -> SIZE(FILTER(x.attacks, x2 -> x2.mitre.technique RLIKE "T")) > 0 ) ) > 0''',
        '''AGGREGATE( alerts, DOUBLE(0), (acc, x) -> (CASE WHEN TRUE THEN acc + DOUBLE(x.risk) ELSE acc END) ) >= 3''',
        '''SIZE( ARRAY_REMOVE( AGGREGATE( alerts, ARRAY(''), (acc, x) -> ARRAY_UNION( acc, (CASE WHEN TRUE THEN ARRAY(IF(x.source IS NULL, '', STRING(x.source))) ELSE ARRAY('') END) ) ), '' ) ) >= 2'''
    ]

    query1 = _rule_entity._compound_clauses(rule)
    assert normalize_string(query1) == \
           f"""{clauses[0]} AND {clauses[1]} AND {clauses[2]} AND {clauses[3]}"""
    assert df.filter(query1).count() == 1

    rule['compound_logic'] = '(my_custom_id_1 AND NOT my_custom_id_2) OR NOT (my_custom_id_3 AND my_custom_id_4)'
    query2 = _rule_entity._compound_clauses(rule)
    assert normalize_string(query2) == \
           f"""( {clauses[0]} AND NOT {clauses[1]} ) OR NOT ( {clauses[2]} AND {clauses[3]} )"""
    assert df.filter(query2).count() == 0


def test_parse_sevent_sequence():
    # Arrange
    event_sequence = 'policy_script THeN WITHIN 2 SECONDS mitre attack THEN WITHIN 2 SECONDS script_in_use_updated'

    # Act
    result = RuleEntity.parse_sevent_sequence(event_sequence,
                                              {'policy_script', 'mitre attack', 'script_in_use_updated'})

    # Assert
    assert result == [{'type': 'event_id', 'value': 'policy_script'},
                      {'type': 'keyword', 'value': 'THEN WITHIN 2 SECONDS'},
                      {'type': 'event_id', 'value': 'mitre attack'},
                      {'type': 'keyword', 'value': 'THEN WITHIN 2 SECONDS'},
                      {'type': 'event_id', 'value': 'script_in_use_updated'}]


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)


# Define the schema for the DataFrame
sequences_schema = StructType([
    StructField('sequences', StructType([
        StructField('jamf_scripts', StructType([
            StructField('__event_sequence', StringType(), True),
            StructField('policy_script', StructType([
                StructField('condition', ArrayType(TimestampType()), True),
                StructField('reset_on', ArrayType(TimestampType()), True),
                StructField('event_id', StringType(), True)
            ]), True),
            StructField('mitre_attack', StructType([
                StructField('condition', ArrayType(TimestampType()), True),
                StructField('reset_on', ArrayType(TimestampType()), True),
                StructField('event_id', StringType(), True)
            ]), True),
            StructField('script_in_use_updated', StructType([
                StructField('condition', ArrayType(TimestampType()), True),
                StructField('reset_on', ArrayType(TimestampType()), True),
                StructField('event_id', StringType(), True)
            ]), True)
        ]), True)
    ]))
])


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_normal_case(spark_session):
    """
    This test case checks a normal condition where the events occur within the specified time limits.
    The event sequence 'policy_script THEN WITHIN 2 HOURS mitre_attack THEN script_in_use_updated' should pass as the events happen within 2 hours.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': 'policy_script THEN WITHIN 2 HOURS mitre_attack THEN script_in_use_updated',
                'policy_script': {
                    'condition': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                  datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'reset_on': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                 datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [datetime(2023, 4, 18, 22, 48, 29, 126000),
                                  datetime(2023, 4, 27, 23, 52, 36, 658000)],  # Within 2 hours
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [datetime(2023, 4, 18, 23, 48, 31, 126000),
                                  datetime(2023, 4, 28, 0, 52, 39, 658000)],  # After mitre_attack
                    'reset_on': None,
                    'event_id': 'script_in_use_updated'
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Act
    result = df.withColumn('sequential_filtering', RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
    result = result.select('sequential_filtering')

    # Assert
    assert result.collect()[0][0] is True


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_no_complete_sequence(spark_session):
    """
    This test case checks a condition where no complete sequence is formed.
    The event sequence 'policy_script THEN WITHIN 2 HOURS mitre_attack THEN script_in_use_updated' should fail as the 'mitre_attack' event happens after more than 2 hours from the 'policy_script' event.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': 'policy_script THEN WITHIN 2 HOURS mitre_attack THEN script_in_use_updated',
                'policy_script': {
                    'condition': [datetime(2023, 4, 18, 21, 48, 27, 126000), datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'reset_on': [datetime(2023, 4, 18, 21, 48, 27, 126000), datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [datetime(2023, 4, 19, 0, 48, 29, 126000), datetime(2023, 4, 28, 1, 52, 36, 658000)],
                    # After more than 2 hours
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [datetime(2023, 4, 19, 1, 48, 31, 126000), datetime(2023, 4, 28, 2, 52, 39, 658000)],
                    # After mitre_attack
                    'reset_on': None,
                    'event_id': 'script_in_use_updated'
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Act
    result = df.withColumn('sequential_filtering', RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
    result = result.select('sequential_filtering')

    # Assert
    assert result.collect()[0][0] is False


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_reset_interrupts_sequence(spark_session):
    """
    This test case checks a condition where a reset_on time interrupts a valid sequence.
    The event sequence 'policy_script THEN WITHIN 2 HOURS mitre_attack THEN script_in_use_updated' should fail as the 'reset_on' time of 'policy_script' interrupts the sequence.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': 'policy_script THEN WITHIN 2 HOURS mitre_attack THEN script_in_use_updated',
                'policy_script': {
                    'condition': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                  datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'reset_on': [datetime(2023, 4, 18, 22, 48, 27, 126000),
                                 datetime(2023, 4, 27, 23, 52, 34, 658000)],  # Interrupts the sequence
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [datetime(2023, 4, 18, 22, 48, 29, 126000),
                                  datetime(2023, 4, 27, 23, 52, 36, 658000)],  # Within 2 hours
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [datetime(2023, 4, 18, 23, 48, 31, 126000),
                                  datetime(2023, 4, 28, 0, 52, 39, 658000)],  # After mitre_attack
                    'reset_on': None,
                    'event_id': 'script_in_use_updated'
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Act
    result = df.withColumn('sequential_filtering', RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
    result = result.select('sequential_filtering')

    # Assert
    assert result.collect()[0][0] is False


@pytest.mark.usefixtures("spark_session")
def test_get_timestamps(spark_session):
    condition = "name = 'alert_0'"
    result = rule_entity(spark_session)._get_timestamps('condition', condition)

    expected_result = """TRANSFORM( FILTER( alerts, x -> x.name = 'alert_0' ), x -> x.eventTime) AS condition"""

    assert normalize_string(result) == expected_result


@pytest.mark.usefixtures("spark_session")
def test_get_event_sequences(spark_session):
    sequences = [
        {
            "event_sequence": "seq_1",
            "events": [
                {
                    "condition": "name = 'alert_0'",
                    "event_id": "event_1",
                },
                {
                    "condition": "name = 'alert_1'",
                    "reset_on": "name = 'alert_0'",
                    "event_id": "event_2",
                },
            ],
            "event_id": "seq_1",
        },
    ]

    result = rule_entity(spark_session)._get_event_sequences(sequences)

    expected_result = """Column<'struct(struct(seq_1 AS __event_sequence, struct(TRANSFORM(FILTER(alerts, lambdafunction((x.name = alert_0), x)), lambdafunction(x.eventTime, x)) AS condition, NULL AS reset_on, event_1 AS event_id) AS event_1, struct(TRANSFORM(FILTER(alerts, lambdafunction((x.name = alert_1), x)), lambdafunction(x.eventTime, x)) AS condition, TRANSFORM(FILTER(alerts, lambdafunction((x.name = alert_0), x)), lambdafunction(x.eventTime, x)) AS reset_on, event_2 AS event_id) AS event_2) AS seq_1)'>"""

    assert str(result) == expected_result


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_empty_sequence(spark_session):
    """
    This test case checks a condition where the event sequence is empty.
    The function should raise a SirensDetectionException in this case.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': '',
                'policy_script': {
                    'condition': [],
                    'reset_on': [],
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [],
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [],
                    'reset_on': None,
                    'event_id': 'script_in_use_updated'
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Assert
    with pytest.raises(pyspark.sql.utils.PythonException):
        # Act
        result = df.withColumn('sequential_filtering',
                               RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
        result = result.select('sequential_filtering')
        result.collect()[0][0]


def test_parse_sevent_sequence_invalid_keyword():
    # Arrange
    event_sequence = 'policy_script NOTVALID mitre_attack THEN script_in_use_updated'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack', 'script_in_use_updated'})


def test_parse_sevent_sequence_invalid_event_id():
    # Arrange
    event_sequence = 'invalid_event_id THEN mitre_attack THEN script_in_use_updated'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack', 'script_in_use_updated'})


def test_parse_sevent_sequence_invalid_time_constraint():
    # Arrange
    event_sequence = 'policy_script THEN WITHIN 2 INVALID_UNIT mitre_attack THEN script_in_use_updated'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack', 'script_in_use_updated'})


def test_parse_sevent_sequence_starts_with_keyword():
    # Arrange
    event_sequence = 'THEN policy_script THEN mitre_attack THEN script_in_use_updated'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack', 'script_in_use_updated'})


def test_parse_sevent_sequence_ends_with_keyword():
    # Arrange
    event_sequence = 'policy_script THEN mitre_attack THEN script_in_use_updated THEN'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack', 'script_in_use_updated'})


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_missing_event_id(spark_session):
    """
    This test case checks a condition where the sequential filtering is done with a missing event ID.
    The function should raise a SirensDetectionException in this case.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': 'policy_script THEN mitre_attack THEN script_in_use_updated',
                'policy_script': {
                    'condition': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                  datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'reset_on': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                 datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [datetime(2023, 4, 18, 22, 48, 29, 126000),
                                  datetime(2023, 4, 27, 23, 52, 36, 658000)],
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [datetime(2023, 4, 18, 23, 48, 31, 126000),
                                  datetime(2023, 4, 28, 0, 52, 39, 658000)],
                    'reset_on': None,
                    # event_id is missing
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Assert
    with pytest.raises(pyspark.sql.utils.PythonException):
        # Act
        result = df.withColumn('sequential_filtering',
                               RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
        result = result.select('sequential_filtering')
        result.collect()[0][0]


def test_parse_sevent_sequence_unrecognized_event_id():
    # Arrange
    event_sequence = 'policy_script THEN mitre_attack THEN script_in_use_updated'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack'})


def test_parse_sevent_sequence_invalid_keyword():
    # Arrange
    event_sequence = 'policy_script INVALID_KEYWORD mitre_attack THEN script_in_use_updated'

    # Assert
    with pytest.raises(SirensDetectionException):
        # Act
        result = RuleEntity.parse_sevent_sequence(event_sequence,
                                                  {'policy_script', 'mitre_attack', 'script_in_use_updated'})


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_invalid_time_unit(spark_session):
    """
    This test case checks a condition where an invalid time unit is present in the event sequence.
    The function should raise a SirensDetectionException in this case.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': 'policy_script THEN WITHIN 2 MONTHS mitre_attack THEN script_in_use_updated',
                # Invalid time unit 'MONTHS'
                'policy_script': {
                    'condition': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                  datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'reset_on': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                 datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [datetime(2023, 4, 18, 22, 48, 29, 126000),
                                  datetime(2023, 4, 27, 23, 52, 36, 658000)],
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [datetime(2023, 4, 18, 23, 48, 31, 126000),
                                  datetime(2023, 4, 28, 0, 52, 39, 658000)],
                    'reset_on': None,
                    'event_id': 'script_in_use_updated'
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Assert
    with pytest.raises(pyspark.sql.utils.PythonException) as e:
        # Act
        result = df.withColumn('sequential_filtering',
                               RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
        result = result.select('sequential_filtering')
        result.collect()[0][0]


@pytest.mark.usefixtures("spark_session")
def test_sequential_filtering_invalid_event_id(spark_session):
    """
    This test case checks a condition where an invalid event ID is present in the event sequence.
    The function should raise a SirensDetectionException in this case.
    """
    # Arrange
    sequences = {
        'sequences': {
            'jamf_scripts': {
                '__event_sequence': 'policy_script THEN mitre_attack THEN invalid_event_id',
                # Invalid event_id 'invalid_event_id'
                'policy_script': {
                    'condition': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                  datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'reset_on': [datetime(2023, 4, 18, 21, 48, 27, 126000),
                                 datetime(2023, 4, 27, 23, 22, 34, 658000)],
                    'event_id': 'policy_script'
                },
                'mitre_attack': {
                    'condition': [datetime(2023, 4, 18, 22, 48, 29, 126000),
                                  datetime(2023, 4, 27, 23, 52, 36, 658000)],
                    'reset_on': None,
                    'event_id': 'mitre_attack'
                },
                'script_in_use_updated': {
                    'condition': [datetime(2023, 4, 18, 23, 48, 31, 126000),
                                  datetime(2023, 4, 28, 0, 52, 39, 658000)],
                    'reset_on': None,
                    'event_id': 'script_in_use_updated'
                }
            }
        }
    }

    sequences_json = json.dumps(sequences, cls=DateTimeEncoder)

    # Convert the JSON string to a DataFrame
    df = spark_session.read.json(spark_session.sparkContext.parallelize([sequences_json]), schema=sequences_schema)

    # Assert
    with pytest.raises(pyspark.sql.utils.PythonException) as e:
        # Act
        result = df.withColumn('sequential_filtering',
                               RuleEntity.sequential_filtering('sequences', lit('jamf_scripts')))
        result = result.select('sequential_filtering')
        result.collect()[0][0]


@pytest.mark.usefixtures("spark_session")
def test_collect_events_with_empty_array(spark_session):
    field = "attacks[].mitre.technique"
    expected_output = """
        ARRAY_REMOVE(
            AGGREGATE(
                alerts, 
                ARRAY(''), 
                (acc, x) -> ARRAY_UNION(
                acc, (CASE WHEN TRUE THEN 
                AGGREGATE(
                    x.attacks,
                    ARRAY(''),
                    (acc2, x2) -> ARRAY_UNION(acc2, IF(x2.mitre.technique IS NULL, ARRAY(''), ARRAY(STRING(x2.mitre.technique))))
                    )
                 ELSE ARRAY('') END)
                )
            ), ''
            )
        """
    assert normalize_string(RuleEntity._collect_events(field)) == normalize_string(expected_output)


@pytest.mark.usefixtures("spark_session")
def test_collect_events_without_empty_array(spark_session):
    field = "name"
    expected_output = """
        ARRAY_REMOVE(
            AGGREGATE(
                alerts, 
                ARRAY(''), 
                (acc, x) -> ARRAY_UNION(
                acc, (CASE WHEN TRUE THEN ARRAY(IF(x.name IS NULL, '', STRING(x.name))) ELSE ARRAY('') END)
                )
            ), ''
            )
        """
    assert normalize_string(RuleEntity._collect_events(field)) == normalize_string(expected_output)


@pytest.mark.usefixtures("spark_session")
def test_collect_events_custom_condition_and_column(spark_session):
    field = "attacks[].mitre.technique"
    condition = "FALSE"
    array_column = "custom_column"
    expected_output = """
        ARRAY_REMOVE(
            AGGREGATE(
                custom_column, 
                ARRAY(''), 
                (acc, x) -> ARRAY_UNION(
                acc, (CASE WHEN FALSE THEN 
                AGGREGATE(
                    x.attacks,
                    ARRAY(''),
                    (acc2, x2) -> ARRAY_UNION(acc2, IF(x2.mitre.technique IS NULL, ARRAY(''), ARRAY(STRING(x2.mitre.technique))))
                    )
                 ELSE ARRAY('') END)
                )
            ), ''
        )
        """
    assert normalize_string(RuleEntity._collect_events(field, condition, array_column)) == normalize_string(expected_output)


@pytest.mark.usefixtures("spark_session")
def test_collect_fields_with_attacks_mitre(spark_session):
    _rule_entity = rule_entity(spark_session)

    # Define the rule with 'collect' fields
    rule = {
        'collect': {
            'field1': 'attacks[].mitre',
            'field2': 'summary'
        }
    }

    # Call the method
    _rule_entity.get_collect_fields(rule)

    # Check if the correct values are in the collect_fields attribute
    assert _rule_entity.collect_fields['field1'] == RuleEntity._collect_events('attacks[].mitre')
    assert _rule_entity.collect_fields['field2'] == RuleEntity._collect_events('summary')


@pytest.mark.usefixtures("spark_session")
def test_collect_fields_with_no_collect_key(spark_session):
    _rule_entity = rule_entity(spark_session)

    # Define the rule without 'collect' fields
    rule = {
        'name': 'something'
    }

    # Call the method
    _rule_entity.get_collect_fields(rule)

    # Check if the collect_fields attribute is not modified
    assert _rule_entity.collect_fields == {}


@pytest.mark.usefixtures("spark_session")
def test_collect_fields_with_empty_collect_key(spark_session):
    _rule_entity = rule_entity(spark_session)

    # Define the rule with empty 'collect' fields
    rule = {
    }

    # Call the method
    _rule_entity.get_collect_fields(rule)

    # Check if the collect_fields attribute is not modified
    assert _rule_entity.collect_fields == {}
