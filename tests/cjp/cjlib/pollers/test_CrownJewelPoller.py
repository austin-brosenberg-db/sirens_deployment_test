#!/usr/bin/env python3

# This file implements tests for code within cj_poller.py
import boto3
from datetime import datetime, timedelta
import json
import os
import pytest
import re
import sys
from unittest.mock import patch, MagicMock

# src_dir = "../../..src"

# Import the application and libraries for testing
# sys.path.append(src_dir)
from databricks.sirens.cjp.cjlib.pollers.CrownJewelPoller import CrownJewelPoller as CJPoller
import databricks.sirens.cjp.cjlib.CrownJewelPollerConfig
import databricks.sirens.cjp.cjlib.CrownJewelSchema
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjException,
)
import databricks.sirens.cjp.cjlib.pollers.RestApiPoller

_poller_name = "fake_poller"
_aws_profile = "fake-aws-profile"
_test_data = [{"name": "fake1"}, {"name": "fake2"}, {"name": "fake3"}]
_test_cursor = "fake_cursor"

# Define a dummy class that
class FakePoller(CJPoller):
    pass


@patch(
    "databricks.sirens.cjp.cjlib.CrownJewelSchema.CrownJewelSchema", spec=databricks.sirens.cjp.cjlib.CrownJewelSchema.CrownJewelSchema
)
@patch(
    "databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig",
    spec=databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig,
)
def test_CrownJewelPoller(MockConfig, MockSchema):
    # Validate that an exception advising use of a derived class is raised
    params = {"bucket": "fake_bucket"}
    with pytest.raises(CjException, match="derived"):
        CJPoller(**params)

    # Validate that exceptions are raised when required parameters are missing or invalid
    with pytest.raises(CjMissingParameterException, match=r"name.*missing"):
        FakePoller(**params)

    params["name"] = _poller_name
    with pytest.raises(CjMissingParameterException, match=r"config.*missing"):
        FakePoller(**params)

    params["config"] = "{}"
    with pytest.raises(CjMissingParameterException, match=r"schema.*missing"):
        FakePoller(**params)

    # Validate that basic initialization occurs (config & schema objects added, parameters stored)
    params["schema"] = "{}"
    test_poller = FakePoller(**params)
    assert MockConfig.called
    assert MockSchema.called
    assert test_poller._profile == None
    assert test_poller._credential_schema == None

    params["profile"] = _aws_profile
    test_poller = FakePoller(**params)
    assert test_poller._profile == _aws_profile


# Validate that an exception advising use of a derived class is raised
def test_get_data():
    MockPoller = MagicMock(spec=CJPoller)
    with pytest.raises(CjException, match="derived"):
        CJPoller._get_data(MockPoller)


def test_get_time_range():
    MockPoller = MagicMock(spec=FakePoller)
    MockConfig = MagicMock(spec=databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig)
    MockPoller._config = MockConfig
    MockPoller._config.get_timestamp_format.return_value = "%Y-%m-%dT%H:%M:%S"
    MockPoller.start_timestamp = datetime.strptime(
        "2022-03-22T11:00:00", "%Y-%m-%dT%H:%M:%S"
    )
    MockPoller.end_timestamp = datetime.strptime(
        "2022-03-22T13:00:00", "%Y-%m-%dT%H:%M:%S"
    )
    result = CJPoller._get_time_range(MockPoller)
    assert result["start"] == "2022-03-22T11:00:00"
    assert result["end"] == "2022-03-22T13:00:00"

    # without end_timestamp
    MockPoller.end_timestamp = None
    result = CJPoller._get_time_range(MockPoller)
    assert result == {}


# Validate that a time between the start of test execution and an minute later is returned.
def test_now():
    MockPoller = MagicMock(spec=CJPoller)
    now = datetime.utcnow()
    later = now + timedelta(minutes=1)
    rval = CJPoller._now()
    assert rval >= int(now.timestamp()) * 1000 and rval <= int(later.timestamp()) * 1000


@patch("boto3.resource")
@patch("boto3.session.Session", spec=boto3.session.Session)
def test_write_data(MockSession, MockResource):
    MockConfig = MagicMock(spec=databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig)
    MockConfig.get_poller_type.return_value = _poller_name
    MockConfig.get_region.return_value = "us-west-2"
    MockConfig.get_bucket_prefix.return_value = "this_should_never_exist/"

    MockPoller = MagicMock(spec=CJPoller)
    MockPoller._config = MockConfig
    MockPoller._profile = None
    MockObject = MagicMock()

    # Test that it does nothing with no data or cursor to write
    CJPoller._write_data(MockPoller, {}, None)
    calls = MockConfig.method_calls
    assert "call.set_cursor" not in str(MockConfig.method_calls)

    # Test that cursor is written if it's passed
    MockConfig.reset_mock()
    CJPoller._write_data(MockPoller, {}, _test_cursor)
    calls = MockConfig.method_calls
    assert "call.set_cursor" in str(calls)
    assert calls[0].args[0] == _test_cursor
    assert not MockObject.method_calls

    # Test that data is written using prefix override parameter, if provided
    MockConfig.reset_mock()
    MockResource().Object.return_value = MockObject
    CJPoller._write_data(
        MockPoller, _test_data, cursor=None, prefix="overridden_prefix"
    )
    assert "overridden_prefix/" in str(MockResource().method_calls)
    assert "put" in str(MockObject.method_calls)

    # Test that data is written if it's passed and profile is not set
    MockConfig.reset_mock()
    MockResource().Object.return_value = MockObject
    CJPoller._write_data(MockPoller, _test_data, None)
    calls = MockObject.method_calls
    assert json.loads(calls[0].kwargs["Body"]) == _test_data
    MockResource().Object.return_value = None
    assert "call.set_cursor" not in str(MockConfig.method_calls)

    # Test that data is written if it's passed and profile is set
    MockConfig.reset_mock()
    MockObject.reset_mock()
    MockSession().resource().Object.return_value = MockObject
    MockPoller._profile = _aws_profile
    CJPoller._write_data(MockPoller, _test_data, _test_cursor)
    calls = MockObject.method_calls
    assert json.loads(calls[0].kwargs["Body"]) == _test_data
    MockSession().resource().Object.return_value = None
    calls = MockConfig.method_calls
    assert "call.set_cursor" in str(calls)
    assert _test_cursor in str(calls)

    MockConfig.reset_mock()
    MockObject.reset_mock()
    MockSession().resource().Object.return_value = MockObject
    MockConfig.is_stateless.return_value = False
    CJPoller._write_data(MockPoller, _test_data, _test_cursor)


def test_get_data():
    fake_data = [
        (_test_data, _test_cursor),
        (_test_data, _test_cursor),
        (_test_data, _test_cursor),
    ]

    MockConfig = MagicMock(spec=databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig)
    MockConfig.get_poller_type.return_value = _poller_name
    MockConfig.get_environment.return_value = "library"

    MockPoller = MagicMock(spec=FakePoller)
    MockPoller._get_data.return_value = fake_data
    MockPoller._config = MockConfig

    data_gen = CJPoller.get_data(MockPoller)
    for i in range(0, len(fake_data)):
        (data, cursor) = next(data_gen)
        assert data == _test_data
        assert cursor == _test_cursor
    with pytest.raises(StopIteration):
        (data, cursor) = next(data_gen)

    MockConfig.get_environment.return_value = "aws"
    with pytest.raises(CjException, match="aws environment"):
        data_gen = CJPoller.get_data(MockPoller)
        next(data_gen)


def test_get_data_error():
    MockPoller = MagicMock(spec=FakePoller)
    with pytest.raises(CjException, match="instead use derived class"):
        CJPoller._get_data(MockPoller)


@pytest.mark.filterwarnings("ignore:DeprecationWarning")
def test_process_data():
    fake_data = [
        (_test_data, _test_cursor),
        (_test_data, _test_cursor),
        (_test_data, _test_cursor),
    ]

    MockConfig = MagicMock(spec=databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig)
    MockConfig.get_poller_type.return_value = _poller_name

    MockPoller = MagicMock(spec=FakePoller)
    MockPoller._get_data.return_value = fake_data
    MockPoller._config = MockConfig

    MockConfig.get_environment.return_value = "aws"
    CJPoller.process_data(MockPoller)
    good_write = False
    parser = re.compile(r"^call.(_write_data)\((.*)\)$")
    for c in MockPoller.method_calls:
        match = parser.match(str(c))
        if match:
            print(match.group(2))
            if _test_cursor in match.group(2):
                complete = True
                for datum in _test_data:
                    for key, value in datum.items():
                        if str(key) not in match.group(2) or str(
                            value
                        ) not in match.group(2):
                            complete = False
                            break
                good_write = complete
    assert good_write
    assert "_write_data" in str(MockPoller.method_calls)

    MockConfig.get_environment.return_value = "library"
    with pytest.raises(CjException, match="library environment"):
        CJPoller.process_data(MockPoller)


# Note: Pytest does NOT require the code below, but it can be useful to run & terminate on error when debugging
if __name__ == "__main__":
    for token in dir():
        if token.find("test_") == 0:
            print(f"--- Running {token}()")
            locals()[token]()
