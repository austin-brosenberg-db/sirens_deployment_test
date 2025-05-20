import pytest

from pyspark.sql.functions import col
from pyspark.testing import assertDataFrameEqual

from databricks.sirens.enrichments import DataFrameEnrichment, Enrichment


def test_filter_output_cols(simple_dataframe):
    result = DataFrameEnrichment.filter_output_cols(simple_dataframe, ['firstname', 'givenname'])
    assert len(result.columns) == 2
    assert 'firstname' in result.columns
    assert 'givenname' in result.columns

    result = DataFrameEnrichment.filter_output_cols(simple_dataframe, ['firstname'], ['givenname'], ['comment'])
    assert len(result.columns) == 3


def test_filter_rows(simple_dataframe):
    result = DataFrameEnrichment.filter_rows(simple_dataframe, "givenname == 'king'")
    assert result.filter(result.givenname == "king").count() == 1
    # Assert that the row with givenname equal to 'king' has the value 'king' in the givenname column
    assert result.filter(result.givenname == "king").first().givenname == "king"


def test_left_join_with_output_cols(spark_session, web_dataframe_for_enrichment, web_enrichment):
    # Perform the left join with specified output columns
    output_cols = ["protocol", "service"]
    result_df = DataFrameEnrichment.join(web_dataframe_for_enrichment, web_enrichment, source_col=["http_port"],
                                         output_cols=output_cols)

    expected_data = [("443", "GET", "1.1.1.1", "2.2.2.2", "ssl", "tcp"),
                     ("80", "POST", "3.3.3.3", "4.4.4.4", "http", "tcp"),
                     ("25", "POST", "5.5.5.5", "6.6.6.6", None, None)]
    expected_df = spark_session.createDataFrame(expected_data,
                                                ["http_port", "http_method", "src_ip", "dest_ip", "protocol",
                                                 "service"])

    assert result_df.sort(result_df.http_port.desc()).collect() == expected_df.sort(
        expected_df.http_port.desc()).collect()


def test_join_with_multiple_cols(spark_session):

    # Perform the left join with specified output columns
    output_cols = ["protocol", "service"]
    src_df = spark_session.createDataFrame([("category1", "subcategory1" , "prop1"), ("category2", "subcategory2" , "prop2")],
                                           ["category", "subcategory", "prop"])
    enrich_df = spark_session.createDataFrame([("category1", "subcategory1" , 10), ("category2", "subcategory2" , 20)],
                                           ["item_cat", "item_subcat", "enriched_prop"])
    result_df = DataFrameEnrichment.join(src_df, enrich_df, source_col=["category", "subcategory"], target_col=["item_cat", "item_subcat"])

    expected_df = spark_session.createDataFrame([("category1", "subcategory1" , "prop1", "category1", "subcategory1" , 10),
                     ("category2", "subcategory2" , "prop2", "category2", "subcategory2" , 20)],
                     ["category", "subcategory", "prop", "item_cat", "item_subcat", "enriched_prop"])

    assertDataFrameEqual(result_df, expected_df)

def test_join_with_multiple_cols(spark_session):
    
    # Perform the left join with specified output columns
    output_cols = ["protocol", "service"]
    src_df = spark_session.createDataFrame([("category1", "subcategory1" , "prop1"), ("category2", "subcategory2" , "prop2")],
                                           ["category", "subcategory", "prop"])
    enrich_df = spark_session.createDataFrame([("category1", "subcategory1" , 10), ("category2", "subcategory2" , 20)],
                                           ["item_cat", "item_subcat", "enriched_prop"])
    result_df = DataFrameEnrichment.join(src_df, enrich_df, source_col=["category", "subcategory"], target_col=["item_cat", "item_subcat"])

    expected_df = spark_session.createDataFrame([("category1", "subcategory1" , "prop1", "category1", "subcategory1" , 10),
                     ("category2", "subcategory2" , "prop2", "category2", "subcategory2" , 20)],
                     ["category", "subcategory", "prop", "item_cat", "item_subcat", "enriched_prop"])

    assertDataFrameEqual(result_df, expected_df)

def test_invalid_join_type(spark_session, web_dataframe_for_enrichment, web_enrichment):
    left_df = web_dataframe_for_enrichment

    # Perform a join with an invalid join type
    result_df = DataFrameEnrichment.join(web_dataframe_for_enrichment, web_enrichment, source_col=["http_port"],
                                         join_type='invalid')

    assert result_df.collect() == left_df.collect()


def test_join_with_post_filter_expression(spark_session, web_dataframe_for_enrichment, web_enrichment):
    result_df = DataFrameEnrichment.join(web_dataframe_for_enrichment, web_enrichment, source_col=["http_port"],
                                         post_filter_expression="http_port == 443")

    expected_data = [("443", "GET", "1.1.1.1", "2.2.2.2", "ssl", "tcp")]
    expected_df = spark_session.createDataFrame(expected_data,
                                                ["http_port", "http_method", "src_ip", "dest_ip", "protocol",
                                                 "service"])

    assert result_df.collect() == expected_df.collect()


def test_join_with_pre_filter_expression(spark_session, web_dataframe_for_enrichment, web_enrichment):
    result_df = DataFrameEnrichment.join(web_dataframe_for_enrichment, web_enrichment, source_col=["http_port"],
                                         target_col=["http_port"])

    expected_data = [("443", "GET", "1.1.1.1", "2.2.2.2", "ssl", "tcp"),
                     ("80", "POST", "3.3.3.3", "4.4.4.4", "http", "tcp"),
                     ("25", "POST", "5.5.5.5", "6.6.6.6", None, None)]
    expected_df = spark_session.createDataFrame(expected_data,
                                                ["http_port", "http_method", "src_ip", "dest_ip", "protocol",
                                                 "service"])

    assert result_df.sort(result_df.http_port.desc()).collect() == expected_df.sort(
        expected_df.http_port.desc()).collect()


def test_get_enrichment_config_all_params(mocker, access_log_config):
    mocker.patch('databricks.sirens.global_config.GlobalConfig.get_input_config_dir')
    mocker.patch('databricks.sirens.global_config.GlobalConfig.get_configs_default_dir')
    dataSourceObj = access_log_config()

    # Test case: All parameters provided
    running_config = {
        "table": "my_table",
        "file": "my_file",
        "source_column": "col1,col2",
        "target_column": "target",
        "join_type": "left",
        "broadcast": True,
        "delimiter": "|",
        "filters": {
            "output_columns": ["col1", "col2"],
            "prefilter": "pre_filter_expression",
            "postfilter": "post_filter_expression"
        },
        "defined_at": "location"
    }
    result = Enrichment(dataSourceObj)._get_enrichment_config(running_config)

    assert result['table'] == "my_table"
    assert result['file'] == "my_file"
    assert result['source_column'] == ["col1", "col2"]
    assert result['target_column'] == ["target"]
    assert result['join_type'] == "left"
    assert result['broadcast_df'] is True
    assert result['delimiter'] == "|"
    assert result['filters'] == {
        "output_columns": ["col1", "col2"],
        "prefilter": "pre_filter_expression",
        "postfilter": "post_filter_expression"
    }
    assert result['defined_at'] == "location"
    assert result['output_columns'] == ["col1", "col2"]
    assert result['pre_filter'] == "pre_filter_expression"
    assert result['post_filter'] == "post_filter_expression"


def test_get_enrichment_config_only_req(mocker, access_log_config):
    mocker.patch('databricks.sirens.global_config.GlobalConfig.get_input_config_dir')
    mocker.patch('databricks.sirens.global_config.GlobalConfig.get_configs_default_dir')
    dataSourceObj = access_log_config()
    # Test case: Only required parameters provided
    running_config = {
        "table": "my_table",
        "file": "my_file",
    }

    result = Enrichment(dataSourceObj)._get_enrichment_config(running_config)

    assert result['table'] == "my_table"
    assert result['file'] == "my_file"
    assert result['source_column'] is None
    assert result['target_column'] is None
    assert result['join_type'] == "left"
    assert result['broadcast_df'] is False
    assert result['delimiter'] == ","
    assert result['filters'] is None
    assert result['defined_at'] is None
    assert result['output_columns'] is None
    assert result['pre_filter'] is None
    assert result['post_filter'] is None


def test_get_enrichment_config_only_source(mocker, access_log_config):
    mocker.patch('databricks.sirens.global_config.GlobalConfig.get_input_config_dir')
    mocker.patch('databricks.sirens.global_config.GlobalConfig.get_configs_default_dir')
    dataSourceObj = access_log_config()
    # Test case: Only source_column parameter provided
    running_config = {
        "source_column": "col1,col2",
    }

    result = Enrichment(dataSourceObj)._get_enrichment_config(running_config)

    assert result['table'] is None
    assert result['file'] is None
    assert result['source_column'] == ["col1", "col2"]
    assert result['target_column'] is None
    assert result['join_type'] == "left"
    assert result['broadcast_df'] is False
    assert result['delimiter'] == ","
    assert result['filters'] is None
    assert result['defined_at'] is None
    assert result['output_columns'] is None
    assert result['pre_filter'] is None
    assert result['post_filter'] is None


def test_is_valid_enrichment_config(caplog, access_log_config):
    # Test case: Valid table configuration
    enrichment = "my_enrichment"
    running_config = {"table": "my_table"}
    dataSourceObj = access_log_config()
    result = Enrichment(dataSourceObj)._is_valid_enrichment_config(enrichment, running_config)
    assert result is True
    assert "bad config" not in caplog.text

    # Test case: Valid file configuration
    running_config = {"file": "my_file"}
    result = Enrichment(dataSourceObj)._is_valid_enrichment_config(enrichment, running_config)
    assert result is True
    assert "bad config" not in caplog.text

    # Test case: Invalid configuration with both file and table
    running_config = {"file": "my_file", "table": "my_table"}
    result = Enrichment(dataSourceObj)._is_valid_enrichment_config(enrichment, running_config)
    assert result is False
    assert "bad config" in caplog.text

    # Test case: Invalid configuration with neither file nor table
    running_config = {}
    result = Enrichment(dataSourceObj)._is_valid_enrichment_config(enrichment, running_config)
    assert result is False
    assert "bad config" in caplog.text


def test_select_enrichment_definition_system(access_log_config):
    dataSourceObj = access_log_config()

    # Test case: Matching enrichment definition in system level
    enrich_config = "iana"
    level = "system"
    expected_result = {
        "name": "iana_ports",
        'table': 'iana',
        'source_column': 'http_port',
        "defined_at": "system"
    }
    result = Enrichment(dataSourceObj)._select_enrichment_definition(enrich_config, level)
    assert result == expected_result


def test_select_enrichment_definition_content(access_log_config):
    dataSourceObj = access_log_config()

    # Test case: Matching enrichment definition in content pack level
    enrich_config = "iana"
    level = "content_pack"
    expected_result = {
        "name": "iana_ports",
        'table': 'iana',
        'source_column': 'http_port',
        "defined_at": "content_pack"
    }
    result = Enrichment(dataSourceObj)._select_enrichment_definition(enrich_config, level)
    assert result == expected_result


def test_select_nonexistent_definition(access_log_config):
    dataSourceObj = access_log_config()

    # Test case: No matching enrichment definition found
    enrich_config = "nonexistent_enrichment"
    expected_result = {}
    level = "system"
    result = Enrichment(dataSourceObj)._select_enrichment_definition(enrich_config, level)
    assert result == expected_result


def test_by_file_not_readable(mocker, access_log_config, spark_session, web_dataframe_for_enrichment):
    mocker.patch('os.path.join', return_value='mocked_path')
    dataSourceObj = access_log_config()

    # Mock pd.read_csv
    mocker.patch('pandas.read_csv')

    # Test case: File is not readable
    running_config = {
        "table": "my_table",
        "file": "my_file",
        "source_column": "col1,col2",
        "target_column": "target",
        "join_type": "left",
        "broadcast": True,
        "delimiter": "|",
        "filters": {
            "output_columns": ["col1", "col2"],
            "prefilter": "pre_filter_expression",
            "postfilter": "post_filter_expression"
        },
        "defined_at": "location"
    }
    os_path_isfile = mocker.patch('os.path.isfile', return_value=False)
    os_path_access = mocker.patch('os.access', return_value=False)
    result = Enrichment(dataSourceObj)._by_file(web_dataframe_for_enrichment, running_config)
    assert os_path_isfile.call_args[0][0] == 'mocked_path'
    assert os_path_access.call_args[0][0] == 'mocked_path'
    assert result.columns == ['http_port', 'http_method', 'src_ip', 'dest_ip']


@pytest.mark.skip("WIP")
def test_by_file_readable(mocker, access_log_config, spark_session, web_dataframe_for_enrichment):
    # Mock pd.read_csv
    mocker.patch('pandas.read_csv')

    mocker.patch('os.path.join', return_value='mocked_path')
    os_path_isfile = mocker.patch('os.path.isfile', return_value=False)
    os_path_access = mocker.patch('os.access', return_value=False)

    dataSourceObj = access_log_config()
    running_config = {
        "table": "my_table",
        "file": "my_file",
        "source_column": "col1,col2",
        "target_column": "target",
        "join_type": "left",
        "broadcast": True,
        "delimiter": "|",
        "filters": {
            "output_columns": ["col1", "col2"],
            "prefilter": "pre_filter_expression",
            "postfilter": "post_filter_expression"
        },
        "defined_at": "location"
    }

    # Test case: File is readable
    os_path_isfile.return_value = True
    os_path_access.return_value = True
    csv_dataframe = mocker.patch('pandas.read_csv',
                                 return_value=spark_session.createDataFrame([(1, 2), (3, 4)], ['col1', 'col2']))
    # spark_createDataFrame = mocker.patch.object(spark_session, 'createDataFrame', return_value=MockDataFrame(['col1', 'col2']))
    running_config['defined_at'] = 'system'
    result = Enrichment(dataSourceObj)._by_file(web_dataframe_for_enrichment, running_config)
    assert os_path_isfile.call_args[0][0] == 'mocked_path'
    assert os_path_access.call_args[0][0] == 'mocked_path'
    assert csv_dataframe.call_args[1]['sep'] == '|'
    # assert spark_createDataFrame.call_args[0][0].equals(pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]}))
    assert result.columns == ['source', 'col1', 'col2']


def test_sort_columns_alpha(spark_session, access_log_config, web_dataframe_for_enrichment):
    dataSourceObj = access_log_config()
    result = Enrichment.sort_cols_alpha(web_dataframe_for_enrichment)

    cols = ['dest_ip', 'http_port', 'http_method', 'src_ip']
    data = [("2.2.2.2", "GET", "443", "1.1.1.1"),
            ("4.4.4.4", "POST", "80", "3.3.3.3"),
            ("6.6.6.6", "POST", "25", "5.5.5.5")]
    expected = spark_session.createDataFrame(data=data, schema=cols)
    assert result.collect() == expected.collect()


def test__get_configured_stage_enrichments_w_tt(access_log_config):
    dataSourceObj = access_log_config()
    result = Enrichment._get_configured_stage_enrichments(dataSourceObj, 'silver', 'web')
    assert 'http_codes' in result[0]


def test__get_configured_stage_enrichments_no_tt(access_log_config):
    dataSourceObj = access_log_config()
    result = Enrichment._get_configured_stage_enrichments(dataSourceObj, 'bronze')
    assert 'iana' in result[0]
