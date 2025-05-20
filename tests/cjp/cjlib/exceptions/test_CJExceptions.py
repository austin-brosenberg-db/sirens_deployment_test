#!/usr/bin/env python3
import pytest
import sys

src_dir = "../../..src"
sys.path.append(src_dir)

from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
    CjException,
    CjInfrastructureException,
)


def test_CjException():
    test_msg = "test_exception"
    with pytest.raises(CjException):
        raise CjException()
    with pytest.raises(CjException, match=test_msg):
        raise CjException(msg=test_msg)


def test_CjInvalidParameterException():
    test_msg = "test_exception"
    test_param = "fake_parameter"
    with pytest.raises(
        CjInvalidParameterException, match=rf"Invalid.*{test_param}.*{test_msg}"
    ):
        raise CjInvalidParameterException(parameter=test_param, msg=test_msg)


def test_CjMissingParameterException():
    test_msg = "test_exception"
    test_param = "fake_parameter"
    with pytest.raises(
        CjMissingParameterException, match=rf"{test_param}.*missing.*{test_msg}"
    ):
        raise CjMissingParameterException(
            parameter="fake_parameter", msg="test_exception"
        )


def test_CjInfrastructureException():
    test_msg = "test_exception"
    test_signal = "fake_signal"
    with pytest.raises(
        CjInfrastructureException, match=rf"{test_signal}.*handled.*{test_msg}"
    ):
        raise CjInfrastructureException(msg="test_exception", signal=test_signal)


# Note: Pytest does NOT require the code below, but it can be useful to run & terminate on error when debugging
if __name__ == "__main__":
    test_CjException()
    test_CjInvalidParameterException()
    test_CjMissingParameterException()
    test_CjInfrastructureException()
