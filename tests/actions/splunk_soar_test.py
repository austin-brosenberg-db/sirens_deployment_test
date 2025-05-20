from databricks.sirens.actions.splunk_soar import SoarAPI, SoarDefaults
from databricks.sirens.exceptions import SirensActionException
from databricks.sirens.internal.restadaptor import RestAdaptorResult
import pytest
import json


def test_SoarDefaults():
    assert isinstance(SoarDefaults.SUPPORTED_ACTIONS, list)


def test_container_defaults():
    assert isinstance(SoarDefaults.container_defaults, dict)


def test_artifact_defaults():
    assert isinstance(SoarDefaults.artifact_defaults, dict)


def test_snake_case_dict_values():
    in_dict = {"field1": "column.field", "CEF": {"fileHash": "context.filehash"}}
    result = SoarDefaults.snake_case_dict_values(in_dict)
    assert 'column_field' in result.values()
    assert 'context_filehash' in result['CEF'].values()


def test_snake_case_dict_values_neg():
    with pytest.raises(SirensActionException):
        input_list = ["failme"]
        result = SoarDefaults.snake_case_dict_values(input_list)


def test_get_literal_value():
    input_str = 'lit("somevalue")'
    result = SoarDefaults.get_literal_value(input_str)
    assert 'somevalue' in result


def test_get_literal_value_neg():
    input_str = 'Literal("somebadvalue")'
    result = SoarDefaults.get_literal_value(input_str)
    assert result is None


def test_SoarAPI():
    S = SoarAPI(server="10.10.10.10")
    assert isinstance(S, SoarAPI)


def test_SoarAPI_no_server():
    with pytest.raises(TypeError):
        S = SoarAPI()


def test_SoarAPI_all_args():
    S = SoarAPI(server="10.10.10.10", headers={}, auth_token="xxx", timeout=10,
                retries=1, verify_ssl=False)
    assert isinstance(S, SoarAPI)


def test_SoarAPI_some_args():
    S = SoarAPI(server="10.10.10.10", headers={}, auth_token="xxx",
                verify_ssl=False)
    assert isinstance(S, SoarAPI)


def test_list_actions():
    result = SoarAPI.list_actions()
    assert isinstance(result, list)
    assert len(result) > 0


def test_get_config_key_artifact():
    input_dict = {"artifact": {"art1": "value1"}, "container": {"con1": "value1"}}
    result = SoarAPI._get_config_key(input_dict, "artifacts")
    assert 'art1' in result.keys()


def test_get_config_key_container():
    input_dict = {"artifact": {"art1": "value1"}, "container": {"con1": "value1"}}
    result = SoarAPI._get_config_key(input_dict, "container")
    assert 'con1' in result.keys()


def test_get_config_key_bad_dict():
    input_dict = {"a": {"field1": "value1"}, "b": {"field1": "value1"}}
    with pytest.raises(SirensActionException):
        result = SoarAPI._get_config_key(input_dict, "container")


def test_df_to_dict(simple_dataframe):
    result = SoarAPI._df_to_dict(simple_dataframe)
    assert isinstance(result, list)


def test_make_dict():
    json_record = {"context": "somevalue", "observable": "obs_val"}
    transforms = {"data": "context", "cef": "observable"}
    result = json.loads(SoarAPI._make_dict(json_record, transforms))
    assert 'somevalue' in result.values()
    assert 'obs_val' in result.values()


def test_create_container_json():
    S = SoarAPI(server='10.10.10.10')
    result = S._create_container_json(source_data_identifier='xx', label='xx', name='name')
    result = json.loads(result)
    assert isinstance(result, dict)
    assert all(k in result.keys() for k in ('source_data_identifier', 'label', 'name'))


def test_create_artifacts_json(mocker, soar_transforms):
    mocker.patch("databricks.sirens.internal.restadaptor.RestAdaptor",
                 return_value=RestAdaptorResult(True, 200, 'success', 'somedata'))
    mocker.patch("databricks.sirens.actions.splunk_soar.SoarAPI")
    S = SoarAPI(server="10.10.10.10")
    S.soar_conf = soar_transforms
    df = [{'name': 'db_security_grp_modified',
           'summary': 'Modified security group in RDS from 811596193553 at 5.205.62.253', 'severity': 'high'}]
    result = S._create_artifacts_json(df, container_id=32)
    assert isinstance(result, list)
