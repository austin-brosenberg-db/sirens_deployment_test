from datetime import datetime, date, timedelta

from pyspark.sql.functions import current_date
import pyspark.sql.functions as F

from tests.utils import *
from pyspark.sql.types import *
from databricks.sirens.detection import *

test_schema = (
    StructType()
    .add("user", StringType())
    .add("created_api_token", IntegerType())
    .add("ip_address", StringType())
    .add("privilegeGranted", ArrayType(StringType()))
    .add("eventType", StringType())
    .add("recordJson", StringType())
    .add("country", StringType())
    .add("published", TimestampType())
    .add(
        "requestParameters",
        StructType([StructField("roleArn", StringType(), True)]),
    )
    .add("date", DateType())
    .add("mfa_attempt_bypass", IntegerType())
    .add("detection_id", StringType())
)

test_args = {
    "user": "test@databricks.com",
    "ip_address": "10.0.0.1",
    "published": datetime.now(),
    "date": date.today(),
}

module_name = "test v1 pipeline"


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
def detection_rules(spark_session):
    return DetectionRuleset(rule_entity(spark_session))


@pytest.mark.usefixtures("spark_session")
def detector(spark_session):
    return Detector(output_path="", detection_rules=detection_rules(spark_session), module_name=module_name, spark=spark_session)


@pytest.mark.usefixtures("spark_session")
def test_detect(spark_session):
    """alert unit test"""

    test_args = {
        "user": "test@databricks.com",
        "ip_address": "10.0.0.1",
        "published": datetime.now(),
        "eventType": "TestAlertEventType",
        "date": datetime.today(),
        "alertClass": "UNDEFINED",
    }

    test_df = make_test_df(spark_session, test_schema, test_args)

    alert_df = detector(spark_session).detect(test_df)
    rows = alert_df.rdd.collect()
    assert rows[0].summary == "this is rule1 triggered by test@databricks.com"

    assert rows[0].source == "test"
    assert rows[0].context["key2"] == test_args["user"]
    assert rows[0].context["who"] == f"{test_args['user']} hello to {test_args['ip_address']}"

    assert rows[0].rawJson == test_df.select(F.to_json(F.struct(*test_df.schema.names)).alias('json')).rdd.collect()[
        0].json


@pytest.mark.usefixtures("spark_session")
def _verify_assertion(spark_session, detector, args):
    def _assert_columns(row, columns):
        for col in columns:
            assert row[col], args["test_args"][col]

    test_df = make_test_df(spark_session, test_schema, args["test_args"])
    alert_df = detector(spark_session).detect(test_df)
    rows = alert_df.rdd.collect()
    assert len(rows) == args["alert_size"]


@pytest.mark.usefixtures("spark_session")
def test_detector_negative(spark_session):
    """negtive detection unit test"""

    df_args = {
        "user": "bad@databricks.com",
        "ip_address": "10.0.0.0",
        "published": datetime.now(),
        "eventType": "TestAlertEventType",
        "date": datetime.today(),
        "alertClass": "UNDEFINED",
    }

    args = {
        "test_args": df_args,
        "alert_size": 0,
        "update_size": 0,
        "update_columns": ["user", "ip_address"],
    }

    _verify_assertion(spark_session, detector, args)


@pytest.mark.usefixtures("spark_session")
def test_detector_positive(spark_session):
    """positive detection unit test"""

    positive_df = {
        "user": "test@databricks.com",
        "ip_address": "10.0.0.1",
        "published": datetime.now(),
        "eventType": "TestAlertEventType",
        "privilegeGranted": ["admin1"],
        "date": datetime.today(),
        "alertClass": "UNDEFINED",
    }

    args = {
        "test_args": positive_df,
        "alert_size": 1,
        "update_size": 1,
        "update_columns": ["user", "ip_address"],
    }

    _verify_assertion(spark_session, detector, args)


@pytest.mark.usefixtures("spark_session")
def test_detector_uuid_inferred(spark_session):
    ruleset = DetectionRuleset(
        RuleEntity(
            name='rule1',
            summary='',
            source='test',
            alertClass='ALERT',
            eventTime="<current_timestamp>",
            ruleVersion=1,
            severity='low',
            filter='1=1'
        )
    )
    test_args = {
        "user": "test@databricks.com",
        "ip_address": "10.0.0.1",
        "published": datetime.now(),
        "eventType": "TestAlertEventType",
        "date": datetime.today(),
        "alertClass": "UNDEFINED",
    }

    test_df = make_test_df(spark_session, test_schema, test_args)

    alert_df = Detector(output_path="", detection_rules=ruleset, module_name=module_name).detect(test_df)
    assert re.match(".+-.+-.+-.+-.+", alert_df.collect()[0]['uuid'])


@pytest.mark.usefixtures("spark_session")
def test_detector_uuid_explicit(spark_session):
    ruleset = DetectionRuleset(
        RuleEntity(
            name='rule1',
            summary='',
            source='test',
            alertClass='ALERT',
            eventTime="<current_timestamp>",
            ruleVersion=1,
            severity='low',
            uuid='<detection_id>',
            filter='1=1'
        )
    )
    test_args = {
        "user": "test@databricks.com",
        "detection_id": "ltd:12345",
        "ip_address": "10.0.0.1",
        "published": datetime.now(),
        "eventType": "TestAlertEventType",
        "date": datetime.today(),
        "alertClass": "UNDEFINED",
    }

    test_df = make_test_df(spark_session, test_schema, test_args)

    alert_df = Detector(output_path="", detection_rules=ruleset, module_name=module_name).detect(test_df)
    assert alert_df.collect()[0]['uuid'] == 'ltd:12345'


@pytest.mark.usefixtures("spark_session")
def test_dedup_on_empty(spark_session):
    detector = Detector(detection_rules=detection_rules(spark_session))
    assert detector.dedup_on == None


@pytest.mark.usefixtures("spark_session")
def test_dedup_on_dict(spark_session):
    df = spark_session.createDataFrame([], StructType([]))

    detector = Detector(
        detection_rules=detection_rules(spark_session),
        dedup_on={'columns': ['col1', 'col2'], 'df': df},
    )
    assert detector.dedup_on == {'columns': ['col1', 'col2'], 'df': df, 'method': 'left_anti'}


@pytest.mark.usefixtures("spark_session")
def test_dedup_on_dict2(spark_session):
    df = spark_session.createDataFrame([], StructType([]))

    detector = Detector(
        detection_rules=detection_rules(spark_session),
        dedup_on={
            'columns': ['col1', 'col2'],
            'df': df,
            'method': 'inner'
        }
    )
    assert detector.dedup_on == {'columns': ['col1', 'col2'], 'df': df, 'method': 'inner'}


@pytest.mark.usefixtures("spark_session")
def test_dedup_functionality(spark_session):
    existing_rows = [
        {"user": "user1", "published": datetime.now() - timedelta(days=0)},
        {"user": "user2", "published": datetime.now() - timedelta(days=0)},
        {"user": "user1", "published": datetime.now() - timedelta(days=1)},
        {"user": "user3", "published": datetime.now() - timedelta(days=2)},
        {"user": "user4", "published": datetime.now() - timedelta(days=3)},

    ]
    existing_df = spark_session.createDataFrame(data=existing_rows, schema=test_schema)
    existing_alerts = Detector(
        df=existing_df,
        detection_rules=detection_rules(spark_session)
    ).detect()

    new_rows = [
        # Should dedup
        {"user": "user1", "published": datetime.now() - timedelta(days=0)},
        # Should dedup
        {"user": "user2", "published": datetime.now() - timedelta(days=0)},
        # Should dedup
        {"user": "user1", "published": datetime.now() - timedelta(days=0)},
        # Should dedup
        {"user": "user1", "published": datetime.now() - timedelta(days=1)},
        # Should not dedup
        {"user": "user2", "published": datetime.now() - timedelta(days=1)},
        # should dedupe (from above)
        {"user": "user2", "published": datetime.now() - timedelta(days=1)},
        # Should dedup
        {"user": "user3", "published": datetime.now() - timedelta(days=2)},
        # Should dedup
        {"user": "user4", "published": datetime.now() - timedelta(days=3)},
        # Should not dedup
        {"user": "user5", "published": datetime.now() - timedelta(days=4)},
        # Should not dedup
        {"user": "user6", "published": datetime.now() - timedelta(days=4)},
        # Should not dedup
        {"user": "user7", "published": datetime.now() - timedelta(days=4)},
        # Should not dedup
        {"user": "user8", "published": datetime.now() - timedelta(days=4)},
        # Should not dedup
        {"user": "user9", "published": datetime.now() - timedelta(days=5)}
    ]
    new_df = spark_session.createDataFrame(data=new_rows, schema=test_schema)

    detector = Detector(
        df=new_df,
        detection_rules=detection_rules(spark_session),
        dedup_on={'columns': ['name', 'summary', 'eventDate'], 'df': existing_alerts}
    )

    updated_alerts = detector.detect()

    assert updated_alerts.count() == 6
    assert updated_alerts.filter('eventDate = current_date').count() == 0
    assert updated_alerts.filter('eventDate = current_date-1').count() == 1
    assert updated_alerts.filter('eventDate = current_date-2').count() == 0
    assert updated_alerts.filter('eventDate = current_date-3').count() == 0
    assert updated_alerts.filter('eventDate = current_date-4').count() == 4
    assert updated_alerts.filter('eventDate = current_date-5').count() == 1


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
                    'actor': {'id': 'user@databricks.com'},
                    'eventTime': datetime.now() - timedelta(days=3),
                    'eventDate': datetime.today() - timedelta(days=3),
                    'alertClass': 'ALERT',
                    'ruleVersion': '1',
                    'filter': '1=1',
                    'risk': 1.5,
                    'uuid': '1',
                    'observables': {
                        'ipAddresses': ['1.0.0.0', '0.0.0.0']
                    },
                    'attacks': [
                        {'mitre': {'technique': 'T01'}}
                    ],
                    'moduleName': 'testing'
                },
                {
                    'name': 'alert2',
                    'summary': 'alert2 for user1',
                    'severity': 'medium',
                    'source': 'databricks',
                    'actor': {'id': 'user@databricks.com'},
                    'uuid': '2',
                    **default_vals,
                    'observables': {
                        'ipAddresses': ['2.0.0.0', '3.0.0.0'],
                        'domains': ['databricks.com']
                    },
                },
                {
                    'name': 'alert3',
                    'summary': 'alert3 for user1',
                    'severity': 'low',
                    'source': 'aws',
                    'actor': {'id': 'user@databricks.com'},
                    'uuid': '3',
                    **default_vals
                },
                {
                    'name': 'alert4',
                    'summary': 'alert4 for user1',
                    'severity': 'high',
                    'source': 'aws',
                    'actor': {'id': 'user@databricks.com'},
                    'uuid': '4',
                    **default_vals
                },
                {
                    'name': 'alert1',
                    'summary': 'alert1 for user1',
                    'severity': 'low',
                    'source': 'databricks',
                    'actor': {'id': 'user@databricks.com'},
                    'uuid': '5',
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
def test_correlation_detect_positive(spark_session):
    ruleset = DetectionRuleset(
        RuleEntity(
            name='rule1',
            summary='',
            source='test',
            alertClass='CORRELATION',
            eventTime="<current_timestamp>",
            ruleVersion=1,
            severity='low',
            time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
            filter=[
                {
                    'type': 'count',
                    'condition': 'source IN ("aws", "databricks")',
                    'event_id': 'event_1'
                }
            ]
        )
    )

    test_df = cluster_df(spark_session)

    alert_df = Detector(output_path="", detection_rules=ruleset, module_name=module_name, spark=spark_session).detect(test_df)
    assert alert_df.count() == 1


@pytest.mark.usefixtures("spark_session")
def test_correlation_detect_negative(spark_session):
    ruleset = DetectionRuleset(
        RuleEntity(
            name='rule1',
            summary='',
            source='test',
            alertClass='CORRELATION',
            eventTime="<current_timestamp>",
            ruleVersion=1,
            severity='low',
            time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
            filter=[
                {
                    'type': 'count',
                    'condition': 'source IN ("azure", "gcp")',
                    'event_id': 'event_1'
                }
            ]
        )
    )

    test_df = cluster_df(spark_session)

    alert_df = Detector(output_path="", detection_rules=ruleset, module_name=module_name, spark=spark_session).detect(test_df)
    assert alert_df.count() == 0


@pytest.mark.usefixtures("spark_session")
def test_correlation_detect_first_seen(spark_session):
    entity = RuleEntity(
        name='rule1',
        summary='',
        source='test',
        alertClass='CORRELATION',
        eventTime="<current_timestamp>",
        ruleVersion=1,
        severity='low',
        time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
        filter=[
            {
                'type': 'count',
                'condition': 'source IN ("aws", "databricks")',
                'event_id': 'event_1'
            }
        ]
    )
    test_df = cluster_df(spark_session)
    select_expr = detector(spark_session)._correlation_select(test_df, entity)

    first_seen = (
        select_expr
        .selectExpr('__first_seen')
        .rdd.collect()[0][0]
    )

    assert first_seen >= select_expr.selectExpr('CURRENT_TIMESTAMP - INTERVAL 6 DAYS').rdd.collect()[0][0]


@pytest.mark.usefixtures("spark_session")
def test_correlation_detect_last_seen(spark_session):
    entity = RuleEntity(
        name='rule1',
        summary='',
        source='test',
        alertClass='CORRELATION',
        eventTime="<current_timestamp>",
        ruleVersion=1,
        severity='low',
        time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
        filter=[
            {
                'type': 'count',
                'condition': 'source IN ("aws", "databricks")',
                'event_id': 'event_1'
            }
        ]
    )
    test_df = cluster_df(spark_session)
    select_expr = detector(spark_session)._correlation_select(test_df, entity)

    last_seen = (
        select_expr
        .selectExpr('__last_seen')
        .rdd.collect()[0][0]
    )

    assert last_seen >= select_expr.selectExpr('CURRENT_TIMESTAMP - INTERVAL 4 DAYS').rdd.collect()[0][0]


@pytest.mark.usefixtures("spark_session")
def test_correlation_detect_uuids(spark_session):
    entity = RuleEntity(
        name='rule1',
        summary='',
        source='test',
        alertClass='CORRELATION',
        eventTime="<current_timestamp>",
        ruleVersion=1,
        severity='low',
        time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
        filter=[
            {
                'type': 'count',
                'condition': 'source IN ("aws", "databricks")',
                'event_id': 'event_1'
            }
        ]
    )
    test_df = cluster_df(spark_session)
    select_expr = detector(spark_session)._correlation_select(test_df, entity)

    uuids = (
        select_expr
        .selectExpr('__uuids')
        .rdd.collect()[0][0]
    )

    assert uuids == ['1', '2', '3', '4', '5']


@pytest.mark.usefixtures("spark_session")
def test_correlation_detect_observables(spark_session):
    entity = RuleEntity(
        name='rule1',
        summary='',
        source='test',
        alertClass='CORRELATION',
        eventTime="<current_timestamp>",
        ruleVersion=1,
        severity='low',
        time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
        filter=[
            {
                'type': 'count',
                'condition': 'source IN ("aws", "databricks")',
                'event_id': 'event_1'
            }
        ]
    )
    test_df = cluster_df(spark_session)
    select_expr = detector(spark_session)._correlation_select(test_df, entity)

    observables = (
        select_expr
        .selectExpr('__observables')
        .rdd.collect()[0][0]
    )

    assert observables.ipAddresses == ['1.0.0.0', '0.0.0.0', '2.0.0.0', '3.0.0.0', '255.0.0.0']
    assert observables.domains == ['databricks.com']


@pytest.mark.usefixtures("spark_session")
def test_correlation_detect_uuid(spark_session):
    entity = RuleEntity(
        name='rule1',
        summary='',
        source='test',
        alertClass='CORRELATION',
        eventTime="<current_timestamp>",
        ruleVersion=1,
        severity='low',
        time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
        filter=[
            {
                'type': 'count',
                'condition': 'source IN ("aws", "databricks")',
                'event_id': 'event_1'
            }
        ]
    )
    ruleset = DetectionRuleset(entity)
    test_df = cluster_df(spark_session)
    _detector = Detector(output_path="", detection_rules=ruleset, module_name=module_name, spark=spark_session)
    select_expr = _detector._correlation_select(test_df, entity)
    alert_df = _detector.detect(test_df)

    uuid = alert_df.select('uuid').rdd.collect()[0][0]
    expected_uuid = (
        select_expr
        .select(F.expr("sha1(concat_ws(' ', key, __first_seen, __last_seen, __uuids, window, filter))"))
        .rdd.collect()[0][0]
    )

    assert expected_uuid == uuid


@pytest.mark.usefixtures("spark_session")
def test_correlation_select_with_collect_fields(spark_session):
    entity = RuleEntity(
        name='rule1',
        summary='',
        source='test',
        alertClass='CORRELATION',
        eventTime="<current_timestamp>",
        ruleVersion=1,
        severity='low',
        time_window='eventTime >= CURRENT_TIMESTAMP - INTERVAL 60 DAYS',
        filter=[
            {
                'type': 'count',
                'condition': 'source IN ("aws", "databricks")',
                'event_id': 'event_1'
            }
        ],
        collect={
            'mitre_techniques': 'attacks[].mitre.technique',
            'mitre_techniqueIds': 'attacks[].mitre.techniqueId',
            'summaries': 'summary'
        }
    )

    ruleset = DetectionRuleset(entity)
    test_df = cluster_df(spark_session)
    _detector = Detector(output_path="", detection_rules=ruleset, module_name=module_name, spark=spark_session)

    # Call the _correlation_select method
    result_df = _detector._correlation_select(test_df, entity)

    # Collect the result to a local object
    result = result_df.collect()[0]

    # Check if the new fields are added as expected
    assert result.mitre_techniqueIds == []
    assert result.mitre_techniques == ['T01']
    assert result.summaries == ['alert1 for user1', 'alert2 for user1', 'alert3 for user1', 'alert4 for user1']
