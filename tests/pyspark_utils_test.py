from pyspark.sql.types import StructField, StringType

from databricks.sirens.utils.base_utils import *


def test_create_dataframe_pos_no_data(spark_session):
    schema = StructType([
        StructField("firstname", StringType()),
        StructField("givenname", StringType()),
        StructField("comment", StringType())
    ])
    result = BaseUtils.create_dataframe(spark=spark_session, schema=schema)
    assert isinstance(result, DataFrame) is True


def test_create_dataframe_pos_with_data(spark_session):
    schema = StructType([
        StructField("firstname", StringType()),
        StructField("givenname", StringType()),
        StructField("comment", StringType())
    ])
    data = [("derek", "king", "somenerd"), ("david", "veuve", "extraordinary")]
    result = BaseUtils.create_dataframe(spark=spark_session, schema=schema, data=data)
    assert isinstance(result, DataFrame) is True
    assert result.count() == 2


def test_frame_to_dict_pos(simple_dataframe):
    result = BaseUtils.frame_to_dict(simple_dataframe)
    assert isinstance(result, list)
    assert isinstance(result[0], dict)


def test_filter_dataframe_pos(simple_dataframe):
    filter = "firstname == 'derek'"
    result = BaseUtils.filter_dataframe(simple_dataframe, filter)
    assert result.count() == 1


def test_filter_dataframe_neg(simple_dataframe):
    filter = "name == derek"
    result = BaseUtils.filter_dataframe(simple_dataframe, filter)
    assert result.count() == 2
