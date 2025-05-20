#!/usr/bin/env python3

# This file implements tests for code within cj_poller.py
import glob
import json
import os
import pytest
import sys
from unittest.mock import call, patch, ANY

src_dir = "../../src"

# Import the application and libraries for testing
sys.path.append(src_dir)

from databricks.sirens.cjp.cjlib.cjpcommon import create_poller, create_poller_from_files, get_poller_names
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import CjException, CjInvalidParameterException


# Check that each of the defined poller names trigger the appropriate CrownJewelPoller subclass to be instantiated
def test_create_poller():
    profiles = dict.fromkeys(get_poller_names())
    ## TODO/NOTE: please add a test dictionary for each poller class
    tests = [
    ]
    # Patch (only) the expected poller type, and confirm that a Mock object is returned
    for test in tests:
        with patch(test["class"]) as MockPoller:
            for profile in test["profiles"]:
                print(f"Checking that '{profile}' yields '{test['class']}'")
                poller = create_poller(
                    poller_name=profile, config={"poller_type": test["poller_type"]}
                )
                poller_name = poller._extract_mock_name().replace("()", "")
                assert poller_name and test["class"].find(poller_name) > 0
                del profiles[profile]
                print(f"removing {profile} from profiles")

    print(profiles)
    # Confirm that every profile was checked
    assert len(profiles) == 0

    # Confirm that a invalid poller name raised an exception
    with pytest.raises(CjInvalidParameterException, match="was not defined"):
        create_poller(poller_name="test_partial", config={})


_transform = "keep"
_cursor = '{"location" : "somewhere_safe"}'
_fake_schema = {"field1": {}}
_fake_schema_string = json.dumps(_fake_schema)
_return_schema = False


class ConfigFileFaker:
    def read(self):
        global _transform, _cursor
        return (
            '{"poller_type": "workday", "credentials": {}, "output": "my_bucket", '
            f'"cursor": {_cursor}, "transform": "{_transform}" }}'
        )


class SchemaFileFaker:
    def read(self):
        return _fake_schema_string


# Helper function to selectively simulate file errors
def prevent_schema_access(file_name, mode="r"):
    global _return_empty_schema
    if file_name.find("schema") >= 0:
        if _return_schema:
            return SchemaFileFaker()
        else:
            raise FileNotFoundError
    else:
        return ConfigFileFaker()


# Check that the expected exceptions occur when an invalid poller name is given or config file specifies invalid
# poller_type
def test_create_poller_errors():
    with pytest.raises(
        CjInvalidParameterException, match=r"poller_type.*is required"
    ) as e:
        create_poller(poller_name="fake_poller", config={})

    with pytest.raises(
        CjInvalidParameterException, match=r"poller type.*is not supported"
    ) as e:
        create_poller(poller_name="fake_poller", config={"poller_type": "fake"})

    with pytest.raises(CjException, match=r"credentials.*missing from config") as e:
        create_poller(
            poller_name="fake_poller",
            config={"poller_type": "workday", "environment": "aws"},
        )


# Note: Pytest does NOT require the code below, but it can be useful to run & terminate on error when debugging
if __name__ == "__main__":
    for token in dir():
        if token.find("test_") == 0:
            print(f"--- Running {token}()")
            locals()[token]()
