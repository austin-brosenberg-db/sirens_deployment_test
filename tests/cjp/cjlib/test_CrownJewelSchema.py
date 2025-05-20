#!/usr/bin/env python3
import copy
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
import json
import os
import pytest
import pytz
import re
import sys
from unittest.mock import patch, MagicMock

src_dir = "../../src"  # path relative to execution script

# Import the application and libraries for testing
sys.path.append(src_dir)
from databricks.sirens.cjp.cjlib.CrownJewelSchema import CrownJewelSchema as Schema
from databricks.sirens.cjp.cjlib.CrownJewelSchema import str_to_bool
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
    CjException,
)

_test_schema = OrderedDict({
  "date": {
    "field_in"  : "event_time",
    "type_in"   : "str",
    "type_out"  : "date"
  },
  "timestamp": {
    "field_in"  : "event_time",
    "type_in"   : "str",
    "type_out"  : "datetime",
    "cursor"    : True
  },
  "ts": {
    "field_in"  : "event_time",
    "type_in"   : "str",
    "type_out"  : "timestamp"
  },
  "test_int": {
    "field_in"  : "number",
    "type_in"   : "int",
    "type_out"  : "int"
  },
  "test_int2": {
    "field_in"  : "number2",
    "type_in"   : "str",
    "type_out"  : "int"
  },
  "test_float": {
    "field_in"  : "number3",
    "type_in"   : "int",
    "type_out"  : "float"
  },
  "test_str": {
    "field_in"  : "event_time"
  },
  "test_str2": {
    "field_in": "number",
    "type_in" : "int",
    "type_out": "str"
  },
  "test_bool": {
    "type_in"   : "str",
    "type_out"  : "bool"
  },
  "test_nested": {
    "field_in_complex" : True,
    "field_in" : "planet.country.name"
  }
})


@patch("logging.warning")
def test_CrownJewelSchema(MockLogger):

    # Verify exception if schema is not valid OrderedDict
    with pytest.raises(CjInvalidParameterException, match="must be OrderedDict"):
        schema = Schema(schema="cj_poller.py", transform="keep")

    # Verify that exception is raised for stateful poller w/o cursor field defined
    with pytest.raises(CjInvalidParameterException, match="lacks cursor"):
        temp_schema = OrderedDict({"some_field": {}})
        schema = Schema(schema=temp_schema, transform="keep", stateless=False)

    # Verify that an exception is raised if invalid transform value is set
    with pytest.raises(CjInvalidParameterException, match="must be one of"):
        schema = Schema(schema=_test_schema, transform="scramble")

    # Verify that a valid config can be loaded
    schema = Schema(schema=_test_schema, transform="keep")
    assert len(schema._schema.keys()) > 0
    assert schema._schema == _test_schema

    # Exercise debugging code
    MockLogger.reset_mock()
    os.environ["CJP_DEBUG"] = "true"
    schema = Schema(schema=_test_schema, transform="keep")
    assert len(schema._schema.keys()) > 0
    assert schema._schema == _test_schema
    assert MockLogger.called

    # Verify that a stateful poller requires a schema
    # Verify that a stateful poller has a cursor field defined in the schema


def test_field_value():
    temp_schema = deepcopy(_test_schema)
    schema = Schema(schema=temp_schema, transform="keep")
    dt = datetime.now(tz=timezone.utc)
    number = 0
    data_in = {
        "event_time": dt.isoformat(),
        "number": number,
        "number2": str(number),
        "number3": float(number * 1.0),
        "test_bool": "true",
    }

    assert schema._field_value("event_time", "date", data_in) == str(dt.date())
    assert schema._field_value("event_time", "timestamp", data_in) == str(dt)
    assert schema._field_value("event_time", "ts", data_in) == str(dt.timestamp())

    rval = schema._field_value("number", "test_int", data_in)
    assert rval == number and type(rval) == int

    rval = schema._field_value("number2", "test_int2", data_in)
    assert rval == number and type(rval) == int

    rval = schema._field_value("number3", "test_float", data_in)
    assert rval == float(number) and (type(rval) == float)

    rval = schema._field_value("event_time", "test_str", data_in)
    assert rval == str(dt.isoformat()) and type(rval) == str

    rval = schema._field_value("number", "test_str2", data_in)
    assert rval == str(number) and type(rval) == str

    # Test boolean interpretation of strings
    assert schema._field_value("test_bool", "test_bool", data_in)

    data_in["test_bool"] = "yes"
    assert schema._field_value("test_bool", "test_bool", data_in)

    data_in["test_bool"] = "false"
    assert not schema._field_value("test_bool", "test_bool", data_in)

    data_in["test_bool"] = "no"
    assert not schema._field_value("test_bool", "test_bool", data_in)

    # Add item in schema for which tehre is no data in entry
    schema._schema["test_empty"] = {}
    assert schema._field_value("test_empty", "test_empty", data_in) is None

    # Test whether missing item is inferred as boolean false
    schema._schema["test_empty"] = {"type_in": "bool", "type_out": "bool"}
    assert schema._field_value("test_empty", "test_empty", data_in) is False

    # Confirm unsupported conversions return error
    schema._schema["date"] = {
        "field_in": "number",
        "type_in": "int",
        "type_out": "date",
    }
    with pytest.raises(CjException, match="converting.*date.*not supported"):
        schema._field_value("number", "date", data_in)

    schema._schema["date"] = {
        "field_in": "number",
        "type_in": "int",
        "type_out": "timestamp",
    }
    with pytest.raises(CjException, match="converting.*timestamp.*not supported"):
        schema._field_value("number", "date", data_in)

    schema._schema["date"] = {
        "field_in": "number",
        "type_in": "int",
        "type_out": "datetime",
    }
    with pytest.raises(CjException, match="converting.*datetime.*not supported"):
        schema._field_value("number", "date", data_in)

    schema._schema["test_bool2"] = {
        "field_in": "event_time",
        "type_in": "datetime",
        "type_out": "bool",
    }
    with pytest.raises(CjException, match="converting.*bool.*not supported"):
        schema._field_value("event_time", "test_bool2", data_in)

    # Validate that a data point missing a required field generates an exception
    schema._schema["problem"] = {"required": True}
    with pytest.raises(CjException, match="was required from input data"):
        schema._field_value("problem", "problem", data_in)
    del schema._schema["problem"]

    # Validate that type_in field is required in schema with type_out is specified
    del schema._schema["date"]["type_in"]
    with pytest.raises(CjException, match="'type_in'.*required"):
        schema._field_value("event_time", "date", data_in)

    # Validate that an exception is raised for unsupported data type
    schema._schema = {"bad_field": {"type_in": "str", "type_out": "ball"}}
    with pytest.raises(CjException, match="schema is not supported"):
        schema._field_value(
            field_in="bad_field",
            field_out="bad_field",
            input_data={"bad_field": "yarn"},
        )


def test_transform():
    schema = Schema(schema=_test_schema, transform="keep")
    dt = datetime.now(tz=timezone.utc)
    number = 0
    data_in = {
        "event_time": dt.isoformat(),
        "number": number,
        "test_bool": "true",
        "planet": {"name": "Earth", "country": {"name": "mine", "shape": "irregular"}},
    }

    data_out = schema._transform(data_in)

    # Config that unnested values were transformed, as needed
    assert data_out["test_str2"] == str(number)

    # Confirm that the nested field was unpacked properly
    assert data_out["test_nested"] == data_in["planet"]["country"]["name"]

    # Confirm that the input for a field without field_in was inferred to be field_out
    assert data_out["test_bool"] is True

    # Validate that fields for which input was missing were empty
    for k in ["test_int2", "test_float"]:
        assert data_out[k] is None

    # Confirm that data column not in schema is kept by default
    extra_col = "all_your_base"
    data_in[extra_col] = "are belong to us"
    data_out = schema._transform(data_in)
    assert not "extra" in data_out
    assert extra_col in data_out

    # Test that extra fields are dropped when transform is drop
    schema = Schema(schema=_test_schema, transform="drop")
    data_out = schema._transform(data_in)
    assert not "extra" in data_out
    assert not extra_col in data_out

    # Test that extra fields are packed into "extra" columns when transform is extra
    schema = Schema(schema=_test_schema, transform="extra")
    data_out = schema._transform(data_in)
    assert "extra" in data_out
    assert extra_col in data_out["extra"]

    # Test exception for invalid input type
    with pytest.raises(CjInvalidParameterException, match="dict"):
        schema._transform([])

    # Confirm that setting field_in_complex to false doesn't impair non-complex processing
    schema = Schema(schema=_test_schema, transform="keep")
    schema._schema["test_bool"]["field_in_complex"] = False
    assert schema._transform(data_in)["test_bool"] is True

    # Test exception field_in_complex is true, but field in is absent
    del schema._schema["test_nested"]["field_in"]
    with pytest.raises(CjException, match="field_in.*required.*complex"):
        schema._transform(data_in)


def test_get_cursor_field():
    MockSchema = MagicMock(spec=Schema)
    MockSchema._schema = {"junk": {"cursor": True}, "timestamp": {"cursor": True}}

    with pytest.raises(CjException, match=r"Only.*timestamp.*junk"):
        Schema.get_cursor_field(MockSchema)

    MockSchema._schema = {
        "junk": {},
        "timestamp": {"cursor": True, "field_in": "event_timestamp"},
    }
    (cursor_in, cursor_out) = Schema.get_cursor_field(MockSchema)
    assert cursor_in == "event_timestamp"
    assert cursor_out == "timestamp"

    MockSchema._schema = {"junk": {}, "timestamp": {"cursor": True}}
    (cursor_in, cursor_out) = Schema.get_cursor_field(MockSchema)
    assert cursor_in == "timestamp"
    assert cursor_out == "timestamp"


def test_get_datetime():
    MockSchema = MagicMock(spec=Schema)
    MockSchema._schema = {
        "output_field": {"format_in": "%Y-%m-%d %H:%M:%S"},
        "output_field2": {},
    }

    test_data = "1999-12-31 23:59:59"
    dt = Schema.get_datetime(MockSchema, test_data, "output_field")
    assert dt == datetime.strptime(test_data, "%Y-%m-%d %H:%M:%S")
    dt = Schema.get_datetime(MockSchema, test_data, "output_field2")
    assert dt == datetime.strptime(test_data, "%Y-%m-%d %H:%M:%S")
    timezone = pytz.timezone("US/Eastern")
    dt = Schema.get_datetime(MockSchema, test_data, "output_field2", tz=timezone)
    assert dt == datetime.strptime(test_data, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone
    )

    # Validate that PDT/PST replacements work
    MockSchema._schema = {
        "output_field": {"format_in": "%Y-%m-%d %H:%M:%S%z"},
        "output_field2": {},
    }
    test_data = "1999-12-31 23:59:59PDT"  # UTC-7
    dt = Schema.get_datetime(MockSchema, test_data, "output_field")
    assert dt.time() == time(23, 59, 59)
    assert str(dt.tzinfo) == "UTC-07:00"
    test_data = "1999-12-31 23:59:59PST"  # UTC-8
    dt = Schema.get_datetime(MockSchema, test_data, "output_field")
    assert dt.time() == time(23, 59, 59)
    assert str(dt.tzinfo) == "UTC-08:00"

    # Validate that error is raised if incompatible data is provided
    MockSchema._schema = {
        "output_field": {"format_in": "%Y-%m-%d %H:%M:%S%z"},
        "output_field2": {},
    }
    test_data = "1999-12-31 23:59:59"
    with pytest.raises(ValueError, match="format"):
        dt = Schema.get_datetime(MockSchema, test_data, "output_field")


def test_get_str_from_datetime():
    MockSchema = MagicMock(spec=Schema)
    MockSchema._schema = {
        "output_field": {"format_in": "%Y-%m-%d %H:%M:%S"},
        "output_field2": {},
    }
    test_data = "1999-12-31 23:59:59"
    dt = datetime.strptime(test_data, "%Y-%m-%d %H:%M:%S")
    date_string = Schema.get_str_from_datetime(MockSchema, dt, "output_field")
    assert date_string == test_data
    date_string = Schema.get_str_from_datetime(MockSchema, dt, "output_field2")
    assert datetime.fromisoformat(date_string) == datetime.fromisoformat(test_data)


def test_str_to_bool():
    for value in [
        False,
        None,
        "0",
        "false",
        "False",
        "FALSE",
        "no",
        "No",
        "NO",
        "null",
        "Null",
        "NULL",
    ]:
        assert str_to_bool(value) is False
    for value in [True, "1", "true", "True", "TRUE", "of course"]:
        assert str_to_bool(value) is True
    with pytest.raises(CjInvalidParameterException, match="not supported"):
        str_to_bool(0)


def test_transform_list():
    MockSchema = MagicMock(spec=Schema)
    MockSchema._transform.return_value = "success"
    MockSchema._transform_mode = "keep"
    test_data = [1, 2, 3, 4, 5]
    data_out = Schema.transform_list(MockSchema, test_data)
    ct = re.findall("success", str(data_out))
    assert len(ct) == len(test_data)

    with pytest.raises(CjInvalidParameterException, match="entries"):
        data_out = Schema.transform_list(MockSchema, {})


def test_transform_one():
    MockSchema = MagicMock(spec=Schema)
    MockSchema._transform.return_value = "success"
    MockSchema._transform_mode = "keep"
    test_data = {"name": "test", "data": "not much"}
    data_out = Schema.transform_one(MockSchema, test_data)
    assert data_out == MockSchema._transform.return_value

    with pytest.raises(CjInvalidParameterException, match="entry"):
        data_out = Schema.transform_one(MockSchema, [])


# Note: Pytest does NOT require the code below, but it can be useful to run & terminate on error when debugging
if __name__ == "__main__":
    for token in dir():
        if token.find("test_") == 0:
            print(f"--- Running {token}()")
            locals()[token]()
