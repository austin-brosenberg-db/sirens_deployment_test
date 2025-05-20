#!/usr/bin/env python3

import boto3
from datetime import timedelta
import json
import os
import pytest
import sys
from unittest.mock import call, patch, MagicMock, Mock

src_dir = "../../src"  # path relative to execution

# Import the application and libraries for testing
sys.path.append(src_dir)
from databricks.sirens.cjp.cjlib.CrownJewelPollerConfig import CrownJewelPollerConfig as CJPConfig
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
    CjException,
)

_test_config = {
  "poller_type": "test",
  "url": "https://127.0.0.1",
  "custom_config":{
  },
  "credentials": {
    "location": ""
  },
  "authentication": {
    "type": "Basic"
  },
  "cursor": {
    "location": ""
  },
  "output": {
    "prefix": "test_valid"
  },
  "test_interval": 1,
  "timestamp_format": "%Y-%m-%d %H:%M:%S"
}

def test_CrownJewelPollerConfig():
    config_data = _test_config

    with patch.object(CJPConfig, "_store_config") as mock_store_config:
        config = CJPConfig(
            config=config_data,
            storage_location="fake_bucket",
            profile="google_workspace",
        )
    assert mock_store_config

    ## Negative cases
    with pytest.raises(
        TypeError, match=r"missing 1 required positional argument: 'config'"
    ):
        config = CJPConfig(storage_location="fake_bucket", profile="test")

    with patch.object(CJPConfig, "_store_config") as mock_store_config, pytest.raises(
        CjInvalidParameterException
    ):
        config = CJPConfig(
            config={},
            storage_location="fake_bucket",
            profile="google_workspace",
        )


@patch("boto3.session.Session", spec=boto3.session.Session)
def test_get_creds_from_secret_store(MockSession):
    mock_config = MagicMock(spec=CJPConfig)
    # mock_config._region = 'us-west-2'
    mock_config.get_region.return_value = "us-west-2"
    mock_config._profile = None
    fake_ssm_response = {"Parameter": {"Value": '{"fake_password" : "password123" }'}}
    MockSession().client().get_parameter.return_value = fake_ssm_response

    # Validate that errors are reported for invalid/incomplete parameters
    with pytest.raises(CjInvalidParameterException):
        CJPConfig._get_creds_from_secret_store(mock_config, None)
    fake_cred_info = {}
    with pytest.raises(CjInvalidParameterException):
        CJPConfig._get_creds_from_secret_store(mock_config, fake_cred_info)
    fake_cred_info = {"useless_param": "/security/not_a_real_path"}
    with pytest.raises(CjInvalidParameterException):
        CJPConfig._get_creds_from_secret_store(mock_config, fake_cred_info)

    # Confirm that credentials are requested with or without a profile set
    fake_cred_info = {"location": "/security/not_a_real_path"}
    rval = CJPConfig._get_creds_from_secret_store(mock_config, fake_cred_info)
    assert "fake_password" in rval
    assert rval["fake_password"] == "password123"
    mock_config._profile = "fake_aws_profile_name"
    rval = CJPConfig._get_creds_from_secret_store(mock_config, fake_cred_info)
    assert "fake_password" in rval
    assert rval["fake_password"] == "password123"

    # Confirm that FIPS-enabled endpoints are being used
    calls = MockSession().method_calls
    for c in calls:
        args = c[2]
        if len(args):
            assert "endpoint_url" in args and args["endpoint_url"].find("fips") > 0


def test_store_config():
    test_data = {
        "poller_type": "fake",
        "environment": "aws",
        "url": "https://127.0.0.1",
        "output": {"prefix": "/fake/dont_write"},
        "cursor": {"location": "/security/fake_secret"},
        "credentials": {"fake_creds"},
        "authentication": {"type": "Basic"},
        "test_interval": "1",
        "timestamp_format": "%Y-%m-%dT%H:%M:%S.%fZ",
    }
    mock_config = Mock()
    mock_config.get_environment.return_value = "aws"

    with pytest.raises(CjException, match="missing from config json"):
        CJPConfig._store_config(mock_config, data={}, storage_location="fake_bucket")
    with pytest.raises(CjMissingParameterException):
        CJPConfig._store_config(mock_config, data=test_data, storage_location=None)
    CJPConfig._store_config(mock_config, data=test_data, storage_location="fake_bucket")
    assert mock_config._type == test_data["poller_type"]
    assert mock_config._url == test_data["url"]
    assert mock_config._output_prefix == test_data["output"]["prefix"]
    assert mock_config._cursor_param == test_data["cursor"]["location"]
    # Validate that fields for which data wasn't provided are mock'd
    assert mock_config._custom_config is None
    assert mock_config._actions is None

    mock_config.reset_mock()
    test_data["custom_config"] = "fake_data"
    test_data["actions"] = "actions"
    test_data["junk"] = "superfluous field that should be tolerated/ignored"
    CJPConfig._store_config(mock_config, data=test_data, storage_location="fake_bucket")
    assert mock_config._custom_config == test_data["custom_config"]
    assert mock_config._actions == test_data["actions"]
    del test_data["junk"]

    # Confirm that "keep" is the default transform mode
    CJPConfig._store_config(mock_config, data=test_data, storage_location="fake_bucket")
    assert mock_config._transform_mode == "keep"

    # Confirm that alternate transform modes are detected
    mock_config.reset_mock()
    test_data["transform"] = "extra"
    CJPConfig._store_config(mock_config, data=test_data, storage_location="fake_bucket")
    assert mock_config._transform_mode == "extra"

    # Confirm that test interval absence raises an exception for non-library, stateful pollers
    mock_config.reset_mock()
    del test_data["test_interval"]
    with pytest.raises(CjException, match=r"test_interval.*missing"):
        CJPConfig._store_config(
            mock_config, data=test_data, storage_location="fake_bucket"
        )

    # Confirm that test interval absence raises an exception for non-library pollers
    mock_config.reset_mock()
    del test_data["output"]
    with pytest.raises(CjException, match=r"output.*missing"):
        CJPConfig._store_config(
            mock_config, data=test_data, storage_location="fake_bucket"
        )

    # Confirm that a bad cursor value for non-library poller raises an exception
    mock_config.reset_mock()
    fake_cursor = {"bad": "schema"}
    test_data["cursor"] = fake_cursor
    test_data["output"] = {}
    with pytest.raises(CjInvalidParameterException, match="valid dict"):
        CJPConfig._store_config(
            mock_config, data=test_data, storage_location="fake_bucket"
        )

    # Confirm that a cursor value of the correct type for library poller is accepted
    mock_config.reset_mock()
    fake_cursor = "fake value"
    del test_data["output"]
    test_data["cursor"] = fake_cursor
    test_data["environment"] = "library"
    mock_config.get_environment.return_value = "library"
    CJPConfig._store_config(mock_config, data=test_data, storage_location="fake_bucket")

    # Confirm that a cursor value of the correct type for library poller raises
    # and exception
    mock_config.reset_mock()
    test_data["cursor"] = {}
    test_data["environment"] = "library"
    mock_config.get_environment.return_value = "library"
    with pytest.raises(CjInvalidParameterException, match="valid dict"):
        CJPConfig._store_config(
            mock_config, data=test_data, storage_location="fake_bucket"
        )


def test_get_environment():
    mock_config = Mock()
    fake_env = "fake_environment"
    mock_config._env = fake_env
    assert CJPConfig.get_environment(mock_config) == fake_env


def test_set_credentials():
    key = "fake_password"
    value = "password123"

    mock_config = Mock()
    mock_config.get_environment.return_value = "aws"
    test_data = {}
    with pytest.raises(CjException, match=r"location"):
        CJPConfig._set_credentials(mock_config, test_data)

    test_data["location"] = ""
    with pytest.raises(CjException, match=r"location"):
        CJPConfig._set_credentials(mock_config, test_data)

    test_data["location"] = "/fake_credential_path"

    mock_config._get_creds_from_secret_store.return_value = {key: value}
    CJPConfig._set_credentials(mock_config, test_data)
    assert key in mock_config._credentials
    assert mock_config._credentials[key] == value

    mock_config._profile = "fake_aws_profile_name"
    CJPConfig._set_credentials(mock_config, test_data)
    assert "fake_password" in mock_config._credentials
    assert mock_config._credentials[key] == value

    mock_config._get_creds_from_secret_store.return_value = None
    with pytest.raises(CjException, match=r"No data retrieved"):
        CJPConfig._set_credentials(mock_config, test_data)

    mock_config.reset_mock()
    mock_config.get_environment.return_value = "unsupported"
    with pytest.raises(CjException, match="not currently supported"):
        CJPConfig._set_credentials(mock_config, test_data)

    mock_config.get_environment.return_value = "library"
    test_data = {key: value}
    CJPConfig._set_credentials(mock_config, test_data)
    assert mock_config._credentials[key] == value


def test_get_storage_location():
    mock_config = Mock()
    test_data = "test_bucket_name"
    mock_config._storage_location = test_data
    assert CJPConfig.get_storage_location(mock_config) == test_data


def test_bucket_prefix():
    mock_config = Mock()
    test_data = "/security/fake_security_path"
    mock_config._output_prefix = test_data
    assert CJPConfig.get_bucket_prefix(mock_config) == test_data


def test_get_credentials():
    mock_config = Mock()
    with pytest.raises(CjMissingParameterException, match=r"schema"):
        CJPConfig.get_credentials(mock_config)
    mock_config._credentials = {"login": "fake_username", "password": "fake_password"}
    test_schema = {
        "login": "str",
        "password": int,
    }
    with pytest.raises(CjException, match="was supposed to be"):
        CJPConfig.get_credentials(mock_config, test_schema)

    test_schema["password"] = "str"
    test_creds = CJPConfig.get_credentials(mock_config, test_schema)
    assert test_creds == mock_config._credentials

    mock_config._credentials = {"login": "fake_username"}
    with pytest.raises(CjException, match="password.*absent"):
        CJPConfig.get_credentials(mock_config, test_schema)


def test_get_transform_mode():
    mock_config = Mock()
    mock_config._transform_mode = "keep"
    assert CJPConfig.get_transform_mode(mock_config) == "keep"
    mock_config._transform_mode = "extra"
    assert CJPConfig.get_transform_mode(mock_config) == "extra"
    mock_config._transform_mode = "drop"
    assert CJPConfig.get_transform_mode(mock_config) == "drop"


def test_get_test_interval():
    mock_config = Mock()
    test_normal_offset_data = 20
    mock_config._test_interval = test_normal_offset_data
    assert CJPConfig.get_test_interval(mock_config) == timedelta(
        minutes=test_normal_offset_data
    )

    mock_config._test_interval = 0
    assert CJPConfig.get_test_interval(mock_config) == None

    test_negative_offset_data = -1
    mock_config._test_interval = test_negative_offset_data
    with pytest.raises(CjInvalidParameterException, match="invalid value"):
        CJPConfig.get_test_interval(mock_config)

    test_invalid_type_offset_data = "invalid"
    mock_config._test_interval = test_invalid_type_offset_data
    with pytest.raises(CjException):
        CJPConfig.get_test_interval(mock_config)


def test_get_timestamp_format():
    mock_config = Mock()
    test_valid_timestamp_format = "valid"
    mock_config._timestamp_format = test_valid_timestamp_format
    assert CJPConfig.get_timestamp_format(mock_config) == test_valid_timestamp_format

    test_none_timestamp_format = None
    mock_config._timestamp_format = test_none_timestamp_format
    with pytest.raises(CjInvalidParameterException):
        CJPConfig.get_timestamp_format(mock_config)

    test_invalid_timestamp_format = 1
    mock_config._test_timestamp_format = test_invalid_timestamp_format
    with pytest.raises(CjInvalidParameterException):
        CJPConfig.get_timestamp_format(mock_config)


def test_get_poller_type():
    mock_config = Mock()
    test_data = "test_poller"
    mock_config._type = test_data
    assert CJPConfig.get_poller_type(mock_config) == test_data


@patch("boto3.client")
@patch("boto3.session.Session", spec=boto3.session.Session)
def test_get_cursor(MockSession, MockClient):
    mock_config = MagicMock(spec=CJPConfig)
    mock_config._cursor_param = "/security/fake_cursor_would_be_here"
    mock_config._cursor_param_region = "fake_aws_region"
    mock_config.get_environment.return_value = "aws"
    mock_config.get_region.return_value = "us-west-2"
    test_data = "fake_cursor"
    fake_ssm_response = {"Parameter": {"Value": test_data}}

    mock_config._profile = None
    mock_config._region = "us-west-2"
    MockClient().get_parameter.return_value = fake_ssm_response
    assert CJPConfig.get_cursor(mock_config) == test_data

    mock_config._profile = "fake_aws_profile"
    MockSession().client().get_parameter.return_value = fake_ssm_response
    assert CJPConfig.get_cursor(mock_config) == test_data

    # Confirm that FIPS-enabled endpoints are being used
    calls = MockSession().method_calls
    for c in calls:
        args = c[2]
        if len(args):
            assert "endpoint_url" in args and args["endpoint_url"].find("fips") > 0

    mock_config._env = "library"
    fake_cursor = "fake cursor value"
    mock_config.get_environment.return_value = "library"
    mock_config._cursor_param = fake_cursor
    assert CJPConfig.get_cursor(mock_config) == fake_cursor

    mock_config.get_environment.return_value = "unsupported environment"
    with pytest.raises(CjException, match="not currently supported"):
        CJPConfig.get_cursor(mock_config)


def test_get_record_type():
    mock_config = Mock()
    test_data = "fake"
    mock_config._record_type = test_data
    assert CJPConfig.get_record_type(mock_config) == test_data


def test_get_region():
    mock_config = Mock()
    test_data = "us-fake-region"
    mock_config._region = test_data
    assert CJPConfig.get_region(mock_config) == test_data


def test_get_url():
    mock_config = Mock()
    test_data = "https://127.0.0.1"
    mock_config._url = test_data
    assert CJPConfig.get_url(mock_config) == test_data


def test_get_custom_config():
    mock_config = Mock()
    test_data = {"test_ran_correctly": True}
    mock_config._custom_config = test_data
    assert CJPConfig.get_custom_config(mock_config) == test_data


def test_is_stateless():
    mock_config = Mock()

    # Confirm that poller is deemed not stateless if a state_param is set
    mock_config._cursor_param = "Always a day away"
    assert CJPConfig.is_stateless(mock_config) is False

    # Confirm that poller is deemed stateless if a state_param is not set
    mock_config._cursor_param = None
    assert CJPConfig.is_stateless(mock_config) is True


@patch("boto3.client")
@patch("boto3.session.Session", spec=boto3.session.Session)
def test_set_cursor(MockSession, MockClient):
    mock_config = MagicMock(spec=CJPConfig)
    mock_config._cursor_param = "/security/fake_cursor_would_be_here"
    mock_config._cursor_param_region = "fake_aws_region"
    mock_config.get_environment.return_value = "aws"
    test_data = "fake_cursor"

    # Confirm no cursor is written if None value is passes
    mock_config._profile = None
    mock_config.get_region.return_value = "us-west-2"
    CJPConfig.set_cursor(mock_config, None)
    assert len(MockClient().method_calls) == 0
    MockClient().reset_mock()

    # Confirm cursor would be written if AWS profile is not set
    CJPConfig.set_cursor(mock_config, test_data)
    kwargs = MockClient().method_calls[0].kwargs
    assert kwargs["Name"] == mock_config._cursor_param
    assert kwargs["Value"] == test_data
    assert kwargs["Overwrite"]
    MockClient().reset_mock()

    # Confirm cursor would be written if AWS profile is set
    mock_config._profile = "fake_aws_profile"
    CJPConfig.set_cursor(mock_config, test_data)
    kwargs = MockSession().client().method_calls[0].kwargs
    assert kwargs["Name"] == mock_config._cursor_param
    assert kwargs["Value"] == test_data
    assert kwargs["Overwrite"]

    # Confirm that FIPS-enabled endpoints are being used
    calls = MockSession().method_calls
    for c in calls:
        args = c[2]
        if len(args):
            assert "endpoint_url" in args and args["endpoint_url"].find("fips") > 0

    mock_config.get_environment.return_value = "library"
    CJPConfig.set_cursor(mock_config, test_data)
    assert mock_config._cursor_param == test_data

    mock_config.get_environment.return_value = "unsupported environment"
    with pytest.raises(CjException, match="not currently supported"):
        CJPConfig.set_cursor(mock_config, test_data)


def test_get_authentication():
    mock_config = Mock()
    auth = {"mode": "secret knock"}

    # Confirm that poller is deemed not stateless if a state_param is set
    mock_config._authentication = "secret knock"
    with pytest.raises(CjInvalidParameterException, match=r"not a dictionary"):
        CJPConfig.get_authentication(mock_config)

    mock_config._authentication = auth
    assert CJPConfig.get_authentication(mock_config) == auth

    mock_config._authentication = False
    assert CJPConfig.get_authentication(mock_config) is None


def test_get_api_response():
    mock_config = Mock()
    response = {"msg": "success"}

    # Confirm that poller is deemed not stateless if a state_param is set
    mock_config._api_response = "this should fail"
    with pytest.raises(CjInvalidParameterException, match=r"not a dictionary"):
        CJPConfig.get_api_response(mock_config)

    mock_config._api_response = response
    assert CJPConfig.get_api_response(mock_config) == response

    mock_config._api_response = None
    assert CJPConfig.get_api_response(mock_config) is None


# Note: Pytest does NOT require the code below, but it can be useful to run & terminate on error when debugging
if __name__ == "__main__":
    for token in dir():
        if token.find("test_") == 0:
            print(f"--- Running {token}()")
            locals()[token]()
