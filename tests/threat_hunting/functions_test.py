import databricks.sirens.threathunting as SF
from databricks.sirens.threathunting._entities import ActiveHunt
from pyspark.sql import DataFrame
import pytest


def test_list_hunt_library():
    result = SF.list_hunt_library()
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], SF._entities.HuntConfigMeta)


def test_list_hunt_library_by_name():
    result = SF.list_hunt_library_by_name("LSASS_Memory_Read_Access")
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], SF._entities.HuntConfigMeta)


def test_add_row_numbers(spark_session, apache_df):
    result = SF.add_row_numbers(apache_df)
    assert isinstance(result, DataFrame)
    assert "_row" in result.columns


def test_add_row_numbers_no_time_col(spark_session, apache_df):
    result = SF.add_row_numbers(apache_df, time_col='doesnotexist')
    assert result == apache_df


def test_annotate(apache_df):
    result = SF.annotate(apache_df, row_number=1, annotation={'test_key': 'test_value'})
    assert isinstance(result, DataFrame)
    assert "_annotation" in result.columns


def test_mark_no_rows(apache_df):
    result = SF.mark(apache_df, rows=1)
    assert isinstance(result, DataFrame)
    assert "_row" in result.columns
    assert "_marked" in result.columns


def test_mark_rows(apache_df):
    apache_df = SF.add_row_numbers(apache_df)
    result = SF.mark(apache_df, rows=1)
    assert isinstance(result, DataFrame)
    assert "_marked" in result.columns


def test_filter_marked(apache_df):
    apache_df = SF.mark(apache_df, rows=1)
    result = SF.filter_marked(apache_df)
    assert isinstance(result, DataFrame)
    assert result.count() == 1


def test_list_executed_hunts(spark_session, mocker):
    hr = ActiveHunt(database="sirens", hunt_name="test", hunt_table="table", notebook="nb",
                    run_id='xxx', status='started', state='paused')
    df = spark_session.createDataFrame([hr], schema=ActiveHunt.schema)

    mocker.patch("databricks.sirens.threathunting._functions.Utils._filter_results", return_value=df)
    result = SF.list_executed_hunts()
    assert isinstance(result, DataFrame)


def test_list_executed_hunts_json_output(spark_session, mocker):
    hr = ActiveHunt(database="sirens", hunt_name="test", hunt_table="table", notebook="nb",
                    run_id='xxx', status='started', state='paused')
    df = spark_session.createDataFrame([hr], schema=ActiveHunt.schema)

    mocker.patch("databricks.sirens.threathunting._functions.Utils._filter_results", return_value=df)
    result = SF.list_executed_hunts(output_format=SF.OutputFormat.asJSON)
    assert isinstance(result, str)


@pytest.mark.skip("WIP")
def test_show_hunt_results(spark_session, mocker):
    df = spark_session.createDataFrame([("2024-01-30 10:10.00", "2024-01-30 10:12.30", "analytic_1",
                                         "analytic_description", "12345"), ],
                                       schema="index_start_time string, index_end_time string, command_name string,\
                                               description string, run_id string")
    mocker.patch("databricks.sirens.threathunting._functions.Utils._filter_results", return_value=df)
    html, results = SF.show_hunt_results()
    assert True


@pytest.mark.skip("WIP")
def test_get_hunt_result():
    result = SF.get_hunt_result()
    assert True


@pytest.mark.skip("WIP")
def test_write_hunt_index():
    result = SF.write_hunt_index()
    assert True


@pytest.mark.skip("WIP")
def test_write_hunt_results():
    result = SF.write_hunt_results()
    assert True
