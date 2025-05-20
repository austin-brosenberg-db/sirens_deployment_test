import pytest
import uuid
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, StructField
from databricks.sirens import alertmanager
from databricks.sirens.internal.baseaction import ActionResult
from databricks.sirens.exceptions import SirensAlertManagerException

def test__call_action_handler_neg():
    with pytest.raises(SirensAlertManagerException) as exc:
        alert_obj = alertmanager.AlertHandler(database='sirens', tables='alerts')
    assert exc.type is SirensAlertManagerException

def test__call_action_handler_pos():
    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'])
    assert isinstance(alert_obj, object)

    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'], since="INTERVAL 1 DAY")
    assert alert_obj.time_modifier == "INTERVAL 1 DAY"

def test__get_dataframe_schema_pos():
    result = alertmanager._get_dataframe_schema()
    assert isinstance(result, StructType)

def test__get_tables_from_conf_pos():
    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'], since="INTERVAL 1 DAY")
    result = alert_obj._get_tables_from_conf()
    assert isinstance(result, list)
    assert isinstance(result[0], dict)

@pytest.mark.skip(reason="Not testable")
def test__micro_batch_pos():
    assert True

@pytest.mark.skip(reason="Not testable")
def test__process_events_pos():
    assert True

@pytest.mark.skip(reason="Not testable")
def test__read_alert_table_pos():
    assert True

def test__update_notification_results_dataframe_pos(simple_dataframe):
    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'], since="INTERVAL 1 DAY")
    notification_obj = alertmanager.Notification(alert_obj, 'slack', 'post_message')
    data = [("silvio", "fiorito", "spark_lord")]
    schema = StructType([
        StructField("firstname", StringType()),
        StructField("givenname", StringType()),
        StructField("comment", StringType())
    ])
    df = notification_obj._update_notification_results_dataframe(simple_dataframe, data, schema)
    assert isinstance(df, DataFrame)
    assert df.count() == 3

def test_process_alerts_pos(mocker):
    mocker.patch("databricks.sirens.alertmanager.AlertHandler._read_alert_table")
    mocker.patch("databricks.sirens.alertmanager.AlertHandler._process_events")
    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'], since="INTERVAL 1 DAY")
    process_handler = alert_obj.process_alerts()
    assert process_handler is None

def test_send_neg(mocker, simple_dataframe):
    mocker.patch("databricks.sirens.alertmanager.Notification._call_action_handler")
    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'], since="INTERVAL 1 DAY")
    notification_obj = alertmanager.Notification(alert_obj, 'slack', 'post_message')
    with pytest.raises(SirensAlertManagerException) as exc:
        notification_obj.send(simple_dataframe)
    assert str(exc.value) == "uuid column does not exist in alert table."

def test_send_pos(spark_session, mocker):
    data = [("silvio", "fiorito", uuid.uuid4())]
    schema = StructType([
        StructField("firstname", StringType()),
        StructField("givenname", StringType()),
        StructField("uuid", StringType())
    ])
    df = spark_session.createDataFrame(data, schema)
    mocker.patch("databricks.sirens.alertmanager.Notification._call_action_handler", return_value=ActionResult(True, 'something'))
    alert_obj = alertmanager.AlertHandler(database='sirens', tables=['alerts'], since="INTERVAL 1 DAY")
    notification_obj = alertmanager.Notification(alert_obj, 'slack', 'post_message')
    df = notification_obj.send(df)
    assert isinstance(df, DataFrame)
    assert df.count() == 1
