import pytest
import json
from pyspark.sql import DataFrame

from databricks.sirens.expectations import Expectations


def test_get_metafield_expectations(metafield_expectations, metafield_inverse_expectations):
    expectations, inverse_expectations = Expectations("authentication")._get_metafield_expectations()
    assert expectations == metafield_expectations, "Invalid Expectations"
    assert inverse_expectations == metafield_inverse_expectations, "Invalid InverseExpectations"


def test_get_file_content():
    content = json.loads(Expectations("authentication")._get_file_content("authentication"))
    assert "authentication" in content.values(), "invalid content received"


def test_get_expectations():
    expects, inverse = Expectations("authentication")._get_expectations("authentication", "mandatory")
    assert "_event_time_is_valid" in expects, "invalid content received"


def test_make_constraint():
    constraint, inverse = Expectations("authentication")._make_constraint("_event_time", "mandatory", "timestamp")
    assert "IS NOT NULL" and "'2013-01-01 00:00:00.01'" in constraint
    assert "NOT" in inverse

    # test for date datatype
    constraint, inverse = Expectations("authentication")._make_constraint("_event_time", "mandatory", "date")
    assert "IS NOT NULL" and "'2013-01-01'" in constraint
    assert "NOT" in inverse

    # test for non-mandatory type
    constraint, inverse = Expectations("authentication")._make_constraint("_event_time", "optional", "date")
    assert "'2013-01-01'" in constraint
    assert "IS NOT NULL" not in constraint
    assert "NOT" in inverse


def test_expectations_class():
    constraint, inverse = Expectations("authentication").get()
    assert "_event_time_is_valid" in constraint, "invalid content received"
    assert type(inverse) == dict
