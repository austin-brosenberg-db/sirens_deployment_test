import os
import shutil

import pytest

from databricks.sirens.utils.config_utils import *


def test_replace_var():
    configs = {"key1": "value1", "key2": "value2", "nested": {"key3": "value3"}}
    assert config_utils._replace_var("${key1}", configs) == "value1"
    assert config_utils._replace_var("${key2}", configs) == "value2"
    assert config_utils._replace_var("${nested.key3}", configs) == "value3"
    with pytest.raises(Exception):
        config_utils._replace_var("${nonexistent}", configs)


def test_check_env_var_dups():
    var = {"key1": {"subkey1": "value1"}, "key2": "value2"}
    seen = {"key1.subkey1"}
    with pytest.raises(Exception):
        config_utils._check_env_var_dups(var, seen)


def test_merge_dicts():
    d1 = {"key1": "value1", "key2": {"nested_key": "nested_value"}}
    d2 = {"key1": "new_value1", "key2": {"new_nested_key": "new_nested_value"}}
    merged_dict = config_utils.merge_dicts(d1, d2)
    assert merged_dict["key1"] == "new_value1"
    assert merged_dict["key2"]["nested_key"] == "nested_value"
    assert merged_dict["key2"]["new_nested_key"] == "new_nested_value"


def test_get_env_vars():
    directory = "env_vars/dummy_env"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(f"{directory}/dummy.yaml", "w") as f:
        f.write("dummy_var: dummy_value")

    env_vars = config_utils._get_env_vars("dummy_env")
    assert env_vars["dummy_var"] == "dummy_value"

    with pytest.raises(SirensConfigException):
        config_utils._get_env_vars("nonexistent_env")

    shutil.rmtree("env_vars/dummy_env")


def test_sub_yaml_vars():
    directory = "env_vars/dummy_env"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(f"{directory}/dummy.yaml", "w") as f:
        f.write("dummy_var: dummy_value")

    yaml_vars = {"key1": "${dummy_var}", "key2": {"nested_key": "${dummy_var}"}}
    subbed_yaml_vars = config_utils.sub_yaml_vars(yaml_vars, "dummy_env")
    assert subbed_yaml_vars["key1"] == "dummy_value"
    assert subbed_yaml_vars["key2"]["nested_key"] == "dummy_value"

    shutil.rmtree("env_vars/dummy_env")


def test__replace_var_nested_vars():
    configs = {"key1": "value1", "key2": "value2", "nested": {"key3": "value3", "nested2": {"key4": "value4"}}}
    assert config_utils._replace_var("${nested.nested2.key4}", configs) == "value4"


def test_check_env_var_dups_nested():
    var = {"key1": {"subkey1": "value1", "subkey2": {"subkey3": "value3"}}, "key2": "value2"}
    seen = {"key1.subkey1", "key1.subkey2.subkey3"}
    with pytest.raises(Exception):
        config_utils._check_env_var_dups(var, seen)


def test_merge_dicts_nested():
    d1 = {"key1": "value1", "key2": {"nested_key": "nested_value", "nested_key2": {"nested_key3": "nested_value3"}}}
    d2 = {"key1": "new_value1", "key2": {"new_nested_key": "new_nested_value", "nested_key2": {"nested_key3": "new_nested_value3"}}}
    merged_dict = config_utils.merge_dicts(d1, d2)
    assert merged_dict["key1"] == "new_value1"
    assert merged_dict["key2"]["nested_key"] == "nested_value"
    assert merged_dict["key2"]["new_nested_key"] == "new_nested_value"
    assert merged_dict["key2"]["nested_key2"]["nested_key3"] == "new_nested_value3"


def test_sub_yaml_vars_nested():
    directory = "env_vars/dummy_env"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(f"{directory}/dummy.yaml", "w") as f:
        f.write("dummy_var: dummy_value\nnested_dummy: {nested_key: nested_value}")

    yaml_vars = {"key1": "${dummy_var}", "key2": {"nested_key": "${nested_dummy.nested_key}"}}
    subbed_yaml_vars = config_utils.sub_yaml_vars(yaml_vars, "dummy_env")
    assert subbed_yaml_vars["key1"] == "dummy_value"
    assert subbed_yaml_vars["key2"]["nested_key"] == "nested_value"

    shutil.rmtree("env_vars/dummy_env")


def test__replace_var_non_string_input():
    configs = {"key1": 100, "key2": 200}
    assert config_utils._replace_var("${key1}", configs) == 100
    assert config_utils._replace_var("${key2}", configs) == 200
    with pytest.raises(Exception):
        config_utils._replace_var("${nonexistent}", configs)


def test__replace_var_mixed_input():
    configs = {"key1": "value1", "key2": "value2", "key3": 30}
    assert config_utils._replace_var("Text ${key1} more text ${key2}", configs) == "Text value1 more text value2"
    assert config_utils._replace_var("Number ${key3} and text ${key1}", configs) == "Number 30 and text value1"
    with pytest.raises(Exception):
        config_utils._replace_var("Text ${nonexistent} more text ${key2}", configs)

