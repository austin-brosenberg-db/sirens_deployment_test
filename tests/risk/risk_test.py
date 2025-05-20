import databricks.sirens.threathunting as th

from databricks.sirens.risk._entities import RiskObjectType, DefaultColumns, _get_config_defaults

from pyspark.sql import DataFrame

def test_add_risk_score(apache_df):
    result = th.add_risk_score(apache_df, risk_object="user", object_type=RiskObjectType.USER.value, impact=10, confidence=10)
    assert isinstance(result, DataFrame)
    assert "_risk" in result.columns
    assert "risk_object" in result.select(str(DefaultColumns.RISK.value + ".risk_object")).columns

def test_get_config_defaults(global_config_rec, mocker):
    mocker.patch("databricks.sirens.risk._entities.ConfigReader.read", return_value=global_config_rec)
    result = _get_config_defaults(section="schema:risk", key="risk_table")
    assert result == 'test_risk_table'

def test_get_config_defaults_non_existent(global_config_rec, mocker):
    mocker.patch("databricks.sirens.risk._entities.ConfigReader.read", return_value=global_config_rec)
    mocker.patch("databricks.sirens.risk._entities.ConfigReader._get_config_key", return_value=None)
    result = _get_config_defaults(section="schema:risk", key="risk_table")
    assert result == 'risk'
