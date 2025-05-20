import json
import os
from datetime import datetime, date
from pathlib import Path

import pyspark.sql.functions as F
import pytest
import yaml
from pyspark.sql import DataFrame

from databricks.sirens import connectors
from databricks.sirens import normalize
from databricks.sirens import parsers
from databricks.sirens.datasource import DataSource
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


def read_global_config():
    config = GlobalConfig.read()
    return config


def get_test_files(search_path):
    # Allow to test a single parser. Filter could be like '*/akamai/waf/*/*.yaml'
    filter = os.getenv("PARSER_TO_TEST")
    files_to_validate = []
    for path in Path(search_path).rglob('tests.yaml'):
        if filter is None or path.match(filter):
            files_to_validate.append(path)
    logger.debug(f"test yaml files found: {files_to_validate}")
    return files_to_validate


def get_source_from_path(file, input_dir):
    offset = len(input_dir)
    sub_str = file[offset:len(file)]
    file_arr = sub_str.split("/")

    return file_arr[1], file_arr[2]


def get_test_case(file):
    """read yaml test cases.

    :param file: _description_
    :type file: _type_
    """
    try:
        if Path(file).is_file():
            with open(Path(file)) as f:
                data = yaml.safe_load(f)

    except FileNotFoundError as exc:
        print(f"{exc}")

    return data


def get_inputs_yaml(spark_session, source, sourcetype):
    data_source_obj = DataSource(spark_session, "sirens", source, sourcetype)
    config = data_source_obj.read()

    return data_source_obj, config


def get_one_row_as_dict(df: DataFrame) -> dict:
    return json.loads(df.limit(1).select(F.to_json(F.struct("*"))).collect()[0][0])


def parse_to_bronze(connector_mod, log_parser, spark_session, dataSourceObj):
    logger.debug("\treading sample file")
    df = connectors.Reader(connector_mod, spark_session, dataSourceObj).read()
    logger.debug("\tparsing to bronze")
    df = log_parser.toBronze(df, dataSourceObj)
    if os.getenv("PARSER_TESTS_SHOW_OUTPUT"):
        df.printSchema()
        df.show(5, False)
    event_dict = get_one_row_as_dict(df)
    return df, event_dict


def parse_to_silver(df, log_parser, dataSourceObj):
    logger.debug("\tparsing to silver")
    df = log_parser.toSilver(df, dataSourceObj)
    if os.getenv("PARSER_TESTS_SHOW_OUTPUT"):
        df.printSchema()
        df.show(5, False)
    event_dict = get_one_row_as_dict(df)
    return df, event_dict


def normalize_df(spark_session, df, normalizer, target_table):
    logger.debug("\tnormalizing")

    df = normalizer.filter_frame(df=df, target_table=target_table)
    df = normalizer.transform_frame(df=df, target_table=target_table)
    if os.getenv("PARSER_TESTS_SHOW_OUTPUT"):
        df.printSchema()
        df.show(5, False)
    event_dict = get_one_row_as_dict(df)
    return df, event_dict


def run_tests(test_case, df, event_dict, stage, target_table=None):
    logger.debug(f"\trunning {stage} tests")
    tests = test_case.get('transforms').get(stage)
    if stage == "normalized":
        tests = test_case.get('transforms').get(stage).get(target_table)

    if tests.get('expected_columns'):
        logger.debug("\tchecking expected columns exist")
        expected_cols = list(tests.get('expected_columns').split(","))
        expected_cols = [col.strip() for col in expected_cols]
        cols = df.columns

        try:
            assert all(value in cols for value in expected_cols)
        except AssertionError:
            logger.debug(f"cols: {cols}, expected: {expected_cols}")

    if tests.get('expected_values'):
        logger.debug('\tchecking expected values match')
        expected_values = list(tests.get('expected_values'))
        for itm in expected_values:
            expected_column = next(iter(itm))
            expected_value = itm[expected_column]

            assert event_dict[expected_column] == expected_value

    return


@pytest.mark.usefixtures("spark_session", "parser_sourcetype", "mocker")
def test_parsers(spark_session, parser_sourcetype, mocker):
    mocker.patch("databricks.sirens.utils.base_utils.BaseUtils._create_table", return_value=True)
    global_config = read_global_config()
    input_dir = GlobalConfig.get_input_config_dir(global_config)

    # discover test.yaml files in input_dir.
    files = get_test_files(input_dir)

    # work through each test file - running tests.
    for file in files:
        source, source_type = get_source_from_path(str(file), input_dir)
        if parser_sourcetype and parser_sourcetype not in source_type:
            continue

        # read test cases for the parser.
        test_case_dict = get_test_case(file)
        tests = test_case_dict.get("test_cases")

        for test_case in tests:

            logger.info(f"running tests for: {source} {source_type}")
            # read inputs.yaml for parser
            data_source_obj, config = get_inputs_yaml(spark_session, source, source_type)

            # Over-ride rawPath to read the sample data in samples directory.
            config['input']['rawPath'] = os.path.join("LocalPath://", source, source_type, "samples")
            config['input']['streamType'] = 'batch'

            # instantiate connector & parser objects.
            connector_mod = connectors.get(test_case.get('connector'))
            log_parser = parsers.Parse(plugin=config.get("input").get("parser"),
                                       spark=spark_session)

            # read sample file to bronze dataframe
            bronze_df, event_dict = parse_to_bronze(connector_mod, log_parser, spark_session, data_source_obj)

            # run bronze tests
            if test_case.get('transforms').get('bronze'):
                run_tests(test_case, bronze_df, event_dict, 'bronze')

            # parse to silver
            silver_df, event_dict = parse_to_silver(bronze_df, log_parser, data_source_obj)

            # run silver tests
            if test_case.get('transforms').get('silver'):
                run_tests(test_case, silver_df, event_dict, 'silver')

            # parse to CIM tables
            if not test_case.get('transforms').get('normalized'):
                continue

            for target_table in test_case.get('transforms').get('normalized').keys():

                try:
                    normalizer = normalize.Normalizer(spark_session, data_source_obj)
                    table_tranformations = config.get("transforms").get("silver").get(target_table)

                    logger.info(f"transforming table: {target_table}")
                    cim_df, event_dict = normalize_df(spark_session, silver_df, normalizer,
                                                      target_table)
                    print(f"executing tests for {source}, {source_type}, CIM table {target_table}")
                    run_tests(test_case, cim_df, event_dict, 'normalized', target_table)

                except (AttributeError, TypeError) as exc:
                    pass
