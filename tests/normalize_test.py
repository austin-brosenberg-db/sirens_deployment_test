from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from databricks.sirens.datasource import DataSource, Column
from databricks.sirens.normalize import Normalizer, OCSFMapper

import pytest


def test__cast_to_target_table_schema_pos(mocker, spark_session, access_log_config, simple_dataframe):
    mocker.patch("databricks.sirens.utils.base_utils.SqlQuery.describe_table", return_value=False)
    mocker.patch("databricks.sirens.utils.base_utils.SqlQuery.create_table", return_value=True)
    mocker.patch("databricks.sirens.config_reader.CommonInformationModel.read")
    data_source_obj = access_log_config()
    data_source_obj.read()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    df = normalizer_obj._cast_to_target_table_schema(df=simple_dataframe, target_table='authentication', framework='cim', partition_cols=['_event_date'])
    assert type(df) == DataFrame


def test__create_missing_cols_pos(spark_session, access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    cols = [('new_col', 'value')]
    df = normalizer_obj._create_missing_cols(access_log_dataframe, cols)
    assert 'value' in df.columns


def test__create_missing_alias_source_cols(spark_session, access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    cols = [["some_duplicated_col", "some_duplicated_col"]]
    df = normalizer_obj._create_missing_alias_source_cols(access_log_dataframe, cols)
    assert 'x_some_duplicated_col' in df.columns


def test__gen_transform_cols_pos(spark_session, mocker):
    data_source_obj = DataSource(spark=spark_session, database="sirens", source="apache", sourcetype="access_combined")
    data_source_obj.read()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    transforms = data_source_obj.get_event_fields('web')
    result = normalizer_obj._gen_transform_cols(transforms)
    assert isinstance(result, tuple)
    assert len(result[0]) == 19
    assert result[0][0]['new'][0] == 'event_message'


def test__transform_to_cim_pos(spark_session, bronze_access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    transforms = data_source_obj.get_event_fields('web') or []
    flat_transform, struct_transforms = normalizer_obj._gen_transform_cols(transforms)
    result = normalizer_obj._transform_to_cim(df=bronze_access_log_dataframe, flat_transforms=flat_transform)
    assert isinstance(result, DataFrame)
    assert 'http_status_code' in result.columns
    assert len(result.columns) == 18

def test_filter_frame_pos(spark_session, bronze_access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    df = bronze_access_log_dataframe.select("*", lit("access_combined").alias("_sourcetype"))
    df = normalizer_obj.filter_frame(df, 'web')
    assert df.count() == 10


def test_transform_frame_pos(mocker, spark_session, bronze_access_log_dataframe, access_log_config):
    mocker.patch("databricks.sirens.datasource.DataSource.get_table_config")
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    df = normalizer_obj.transform_frame(bronze_access_log_dataframe, 'web')
    cols = df.columns
    assert len(cols) == 18
    assert 'url_path' in cols


def test__fix_bad_transforms():
    transform = [{'_event_date': {'action': 'alias', 'value': '_event_date'}},
                 {'_event_time': {'action': 'rename', 'value': '_event_time'}}]
    expected = [{'_event_date': {'action': 'rename', 'value': '_event_date'}},
                {'_event_time': {'action': 'rename', 'value': '_event_time'}}]
    result = Normalizer._fix_bad_transforms(transform)
    assert result == expected

def test_add_col_literal(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([("value1",)], ["existing_col"])
    new_col = ocsf_mapper.add_col("new_col", {"type": "literal", "value": "literal_value"})
    assert isinstance(new_col, Column)


def test_add_col_expression(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1,)], ["existing_col"])
    new_col = ocsf_mapper.add_col("new_col", {"type": "expression", "value": "existing_col + 1"})
    assert isinstance(new_col, Column)


def test_add_col_struct(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1, "value1")], ["col1", "col2"])
    new_col = ocsf_mapper.add_col("new_struct", {"type": "struct", "value": ["col1", "col2"]})
    assert isinstance(new_col, Column)

def test_append_alias_basic(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    col_name = "existing_col"
    alias_name = "alias_col"
    df = spark_session.createDataFrame([(1,)], [col_name])
    aliased_col = ocsf_mapper.append_alias(col_name, {"value": alias_name})
    assert isinstance(aliased_col, Column)


def test_append_alias_with_dot(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    col_name = "prefix.existing_col"
    alias_name = "alias_col"
    df = spark_session.createDataFrame([(1,)], [col_name.split('.')[-1]])
    aliased_col = ocsf_mapper.append_alias(col_name, {"value": alias_name})
    assert isinstance(aliased_col, Column)


def test_append_alias_with_empty_value(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    col_name = "existing_col"
    alias_name = ""
    df = spark_session.createDataFrame([(1,)], [col_name])
    aliased_col = ocsf_mapper.append_alias(col_name, {"value": alias_name})
    assert isinstance(aliased_col, Column)

def test_transform_nested_cols_add_literal(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1, "value1")], ["col1", "col2"])
    transforms = [{"new_struct": {"action": "add", "type": "struct", "value": ["col1", "col2"]}},
                    {"new_struct.new_col": {"action": "add", "type": "literal", "value": "literal_value"}}]
    transformed_df, top_level_structs = ocsf_mapper.transform_nested_cols(df, transforms)
    assert "new_struct" in transformed_df.columns
    assert "new_col" in transformed_df.select("new_struct.*").columns
    assert top_level_structs == ["new_struct"]

def test_transform_nested_cols_add_expression(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1, 2)], ["col1", "col2"])
    transforms = [{"new_struct": {"action": "add", "type": "struct", "value": ["col1", "col2"]}},
                    {"new_struct.new_col": {"action": "add", "type": "expression", "value": "col1 + col2"}}]
    transformed_df, top_level_structs = ocsf_mapper.transform_nested_cols(df, transforms)
    assert "new_struct" in transformed_df.columns
    assert "new_col" in transformed_df.select("new_struct.*").columns
    assert top_level_structs == ["new_struct"]

@pytest.mark.skip(reason="Not implemented")
def test_transform_nested_cols_add_struct(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1, "value1")], ["col1", "col2"])
    transforms = [{"new_struct": {"action": "add", "type": "struct", "value": ["col1", "col2"]}},
                    {"new_struct.sub_struct": {"action": "add", "type": "struct", "value": ["col1"]}}]
    transformed_df, top_level_structs = ocsf_mapper.transform_nested_cols(df, transforms)
    assert "new_struct" in transformed_df.columns
    assert "sub_struct" in transformed_df.select("new_struct.*").columns
    assert top_level_structs == ["new_struct"]

def test_transform_nested_cols_alias(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1, "value1")], ["col1", "col2"])
    transforms = [{"new_struct": {"action": "add", "type": "struct", "value": ["col1", "col2"]}},
                    {"new_struct.new_col": {"action": "alias", "value": "col1"}}]
    transformed_df, top_level_structs = ocsf_mapper.transform_nested_cols(df, transforms)
    assert "new_struct" in transformed_df.columns
    assert "new_col" in transformed_df.select("new_struct.*").columns
    assert top_level_structs == ["new_struct"]

def test_transform_nested_cols_multiple_structs(spark_session, access_log_config):
    data_source_obj = access_log_config()
    ocsf_mapper = OCSFMapper(spark_session, data_source_obj)
    df = spark_session.createDataFrame([(1, "value1")], ["col1", "col2"])
    transforms = [{"struct1": {"action": "add", "type": "struct", "value": ["col1"]}},
                    {"struct2": {"action": "add", "type": "struct", "value": ["col2"]}},
                    {"struct1.new_col": {"action": "add", "type": "literal", "value": "literal_value"}},
                    {"struct2.new_col": {"action": "alias", "value": "col2"}}]
    transformed_df, top_level_structs = ocsf_mapper.transform_nested_cols(df, transforms)
    assert "struct1" in transformed_df.columns
    assert "struct2" in transformed_df.columns
    assert "new_col" in transformed_df.select("struct1.*").columns
    assert "new_col" in transformed_df.select("struct2.*").columns
    assert top_level_structs == ["struct1", "struct2"]


def test__transform_to_cim_no_structs(spark_session, bronze_access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    flat_transforms = [
        {"new": ["new_col", "LITERAL=literal_value"]},
        {"rename": ["agent_renamed", "agent"]},
        {"alias": ["alias_col", "existing_col"]}
    ]
    struct_transforms = []
    result = normalizer_obj._transform_to_cim(bronze_access_log_dataframe, flat_transforms, struct_transforms)
    assert isinstance(result, DataFrame)
    assert "new_col" in result.columns
    assert "alias_col" in result.columns


def test__transform_to_cim_with_structs(spark_session, bronze_access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    flat_transforms = [
        {"new": ["new_col", "LITERAL=literal_value"]},
        {"rename": ["existing_col", "existing_col_renamed"]},
        {"alias": ["alias_col", "existing_col"]}
    ]
    struct_transforms = [
        {"new_struct": {"action": "add", "type": "struct", "value": ["existing_col"]}},
        {"new_struct.new_col": {"action": "add", "type": "literal", "value": "literal_value"}}
    ]
    result = normalizer_obj._transform_to_cim(bronze_access_log_dataframe, flat_transforms, struct_transforms)
    assert isinstance(result, DataFrame)
    assert "new_col" in result.columns
    #assert "existing_col_renamed" in result.columns
    assert "alias_col" in result.columns
    assert "new_struct" in result.columns
    assert "new_col" in result.select("new_struct.*").columns


def test__transform_to_cim_only_structs(spark_session, bronze_access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    flat_transforms = []
    struct_transforms = [
        {"new_struct": {"action": "add", "type": "struct", "value": ["existing_col"]}},
        {"new_struct.new_col": {"action": "add", "type": "literal", "value": "literal_value"}}
    ]
    result = normalizer_obj._transform_to_cim(bronze_access_log_dataframe, flat_transforms, struct_transforms)
    assert isinstance(result, DataFrame)
    assert "new_struct" in result.columns
    assert "new_col" in result.select("new_struct.*").columns


def test__transform_to_cim_empty_transforms(spark_session, bronze_access_log_dataframe, access_log_config):
    data_source_obj = access_log_config()
    normalizer_obj = Normalizer(spark_session, data_source_obj)
    flat_transforms = []
    struct_transforms = []
    result = normalizer_obj._transform_to_cim(bronze_access_log_dataframe, flat_transforms, struct_transforms)
    assert isinstance(result, DataFrame)
    assert result.columns == bronze_access_log_dataframe.columns
