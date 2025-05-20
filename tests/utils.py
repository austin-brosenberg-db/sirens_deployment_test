import logging

import pytest

from databricks.sirens.detection import *


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = SparkSession.builder.master("local[*]").appName("DBDF").enableHiveSupport().getOrCreate()
    logger = logging.getLogger('py4j')  # Suppress py4j logging
    logger.setLevel(logging.WARN)
    spark.conf.set("spark.sql.debug.maxToStringFields", 2048)
    return spark


@pytest.mark.usefixtures("spark_session")
def make_test_df(spark_session: SparkSession,
                 schema: StructType,
                 args: Dict) -> DataFrame:
    return spark_session.createDataFrame(
        Row(rec={**args}), schema=schema)


@pytest.mark.usefixtures("spark_session")
def detection_test(spark_session: SparkSession,
                   detector: Detector,
                   schema: StructType,
                   args: Dict) -> List[Row]:
    df = make_test_df(spark_session, schema, args)
    alert_df = detector.detect(df)
    return alert_df.collect()


def normalize_string(string):
    return re.sub(' {2,}', ' ', string.replace('\n', ' ')).strip()
