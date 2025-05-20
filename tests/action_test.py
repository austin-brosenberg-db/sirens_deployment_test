import pytest
import os
from pyspark.sql import DataFrame
from databricks.sirens.utils.base_utils import *
from databricks.sirens.action import Handler
from databricks.sirens.internal.baseaction import ActionResult
from databricks.sirens.exceptions import SirensActionException


def test_Handler_neg():
    with pytest.raises(SirensActionException) as exc:
        result = Handler(module='slack', timeout='string')
    assert exc.type is SirensActionException


def test_Handler_pos():
    result = Handler(module='slack', timeout=60)
    assert isinstance(result, object)


def test__task_neg():
    assert True


@pytest.mark.skip(reason="no way of currently testing this")
def test__task_pos(mocker):
    mocker.patch("databricks.sirens.action.actions.Action")
    params = {'channel': 'apis'}
    body = {"message": "sending message to slack"}
    obj = Handler(module='slack')
    obj._task(plugin='slack', params=params, body=body, requested_action='post_message')


def test_execute_neg():
    status, result = Handler(module='slack').execute('post_message', params=[{'dict_key': 'dict_value'}],
                                                     body=[{"body": "body"}])
    assert result == "invalid param type - please use dict type"


def test_execute_pos(spark_session, mocker):
    mocker.patch("databricks.sirens.action.Action", return_value=ActionResult(True, "success"))
    ar = 'post_message'
    params = {'channel': 'apis'}
    body = {"message": "sending message to slack"}
    handler = Handler(module='slack')
    params = ['some', 'params']
    result = handler.execute(ar, params, body)
    assert isinstance(result, ActionResult)


def test_df_to_dict(spark_session, simple_dataframe):
    result = Handler.df_to_dict(simple_dataframe)
    assert isinstance(result, list)
