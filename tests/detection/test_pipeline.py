from datetime import datetime, date
from unittest.mock import MagicMock

from tests.utils import *
from pyspark.sql.types import *
from databricks.sirens.detection import *

test_schema = (
    StructType()
    .add("user", StringType())
    .add("date", DateType())
    .add("published", TimestampType())
    .add("ip_address", StringType())
    .add("data_int", IntegerType())
    .add("data_string", StringType())
)

module_name = "test rule"

test_args = {
    "user": "test@databricks.com",
    "ip_address": "10.0.0.1",
    "date": date.today(),
    "published": datetime.now(),
    "data_int": 1,
    "data_string": "string data"
}

groupby_columns = ["user"]


@pytest.mark.usefixtures("spark_session")
def test_df(spark_session):
    return make_test_df(spark_session=spark_session, args=test_args, schema=test_schema)


@pytest.mark.usefixtures("spark_session")
def detection_rules():
    rule_entity = RuleEntity(
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
        attacks=[],
        observables=[],
        context={
            "key2": "<user>",
            "who": "<user> hello to <ip_address>"
        },
        filter="1=0"
    )
    return DetectionRuleset(rule_entity)


@pytest.mark.usefixtures("spark_session")
def test_default(spark_session):
    """test pipeline with default configuration"""

    pipeline = Pipeline(module_name="test_v1", df=test_df(spark_session), spark=spark_session)
    pipeline.batch_proc = MagicMock()
    pipeline.start_streaming()
    pipeline.batch_proc.assert_called_once()


@pytest.mark.usefixtures("spark_session")
def test_send_alert(spark_session):
    """test batch_proc"""

    pipeline = Pipeline(module_name="test_v1", df=test_df(spark_session), spark=spark_session)
    detector = Detector(
        detection_rules=detection_rules(),
        module_name=module_name,
        output_path="/tmp",
    )
    pipeline.add_detector(detector)
    detector.detect = MagicMock()
    detector.send_alert = MagicMock()
    df = test_df(spark_session)
    detector.detect.return_value = df

    pipeline.start_streaming()
    detector.detect.assert_called_once()
    detector.send_alert.assert_called_once_with(df, 'append', None, 'delta', None, None)

@pytest.mark.usefixtures("spark_session")
def test_heartbeat(spark_session):
    """test pipeline with heartbeat configuration"""

    heartbeat_table_path = "/test_heartbeat"
    df = test_df(spark_session)
    pipeline = Pipeline(
        module_name="test_v1", df=df,
        spark=spark_session,
        heartbeat_table_path=heartbeat_table_path)
    pipeline.update_heartbeat = MagicMock()
    pipeline.start_streaming()
    pipeline.update_heartbeat.assert_called_once_with(df, "ingestTime", "test_v1", heartbeat_table_path)
