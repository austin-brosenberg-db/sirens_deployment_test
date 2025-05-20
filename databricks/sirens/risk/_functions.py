import datetime

from typing import List, Union
from pyspark.sql import SparkSession, DataFrame, Column
import pyspark.sql.functions as F

from ._entities import RiskObject, BackendDefaults, DefaultColumns, RiskObjectType
from databricks.sirens.sql import SqlQuery
from databricks.sirens.exceptions import SirensException
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

spark = SparkSession.builder.getOrCreate()

def _create_schema(schema):
    return SqlQuery.create_database(database_name=schema)

def _write_risk_index(risk_record: Union[RiskObject, List[RiskObject]],
                      database: BackendDefaults.DATABASE, risk_table: BackendDefaults.RISK_TABLE) -> bool:
    try:
        if not isinstance(risk_record, list):
            risk_record = [risk_record]

        for r in risk_record:
            r.confidence = int(r.confidence)
            r.impact = int(r.impact)

        for r in risk_record:
            r.risk_score = int(round((r.confidence * r.impact) / 100, 0))
            r.risk_score = 1 if r.risk_score < 1 else r.risk_score

        logger.debug(f"initializing backend database: {database}")
        _create_schema(database)

        index_df = spark.createDataFrame(risk_record, RiskObject.schema)
        index_df = index_df.withColumn("_event_time", F.col("_event_time").cast('timestamp'))

        logger.debug(f"updating risk index table: {database}.{risk_table}")

        index_df.write.mode('append').saveAsTable(f"{database}.{risk_table}")

    except Exception as exc:
        logger.error(exc)
        raise SirensException(exc)

    return True

def _filter_column(df: DataFrame, src_col: Column) -> Union[List, bool]:
    if src_col not in df.columns:
        logger.error(f"{src_col} does not exist in the DataFrame")
        return False

    df = df.select(F.col(src_col)).filter(F.col(src_col).isNotNull())

    r = [row.asDict() for row in df.collect()]
    return [row.get(src_col).asDict() for row in r]


def _write_risk_index_from_df(df: DataFrame, database: BackendDefaults.DATABASE,
                              risk_table: BackendDefaults.RISK_TABLE,
                              risk_column: Column = DefaultColumns.RISK.value) -> bool:

    # get dataframe rows as python dict.
    entries = _filter_column(df, risk_column)
    if not entries:
        logger.error(f"risk column: {risk_column} is not in the DataFrame")
        return False
    else:
        # add current time to each risk entry.
        [x.update({'_event_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}) for x in entries]

    risks = []
    try:
        [risks.append(RiskObject(**risk)) for risk in entries]
    except Exception as exc:
        logger.error(f'not a valid risk record: {exc}')
        return False

    return _write_risk_index(risks, database=database, risk_table=risk_table)

def _add_risk_score(df: DataFrame, risk_object: Column, object_type: RiskObjectType,
                    impact: int = 10, confidence: int = 10, source: str = None, annotation: dict = None) -> DataFrame:

    # validate confidence & impact values.
    if impact < 0:
        logger.warning(f"impact less than 0: ({impact}). resetting to 0")
        impact = 0
    if impact > 100:
        logger.warning(f"impact greater than 100: ({impact}). resetting to 100")
        impact = 100
    if confidence < 0:
        logger.warning(f"confidence less than 0: ({confidence}). resetting to 0")
        confidence = 0
    if confidence > 100:
        logger.warning(f"confidence greater than 100: ({confidence}). resetting to 100")
        confidence = 100

    if annotation:
        dict_as_cols = [item for line in [[F.lit(k), F.lit(v)] for k, v in annotation.items()] for item in line]
    else:
        dict_as_cols = []

    df = df.withColumn("_risk", F.struct(
        F.col(risk_object).alias("risk_object"),
        F.lit(object_type).alias("object_type"),
        F.lit(impact).cast('int').alias("impact"),
        F.lit(confidence).cast('int').alias("confidence"),
        F.lit(source).alias("source"),
        F.create_map(*dict_as_cols).alias("annotation")
    ))
    return df

def _list_risk_table(database: BackendDefaults.DATABASE, table: BackendDefaults.RISK_TABLE,
                     time_start: str = None, time_end: str = None,
                     risk_object: str = None, object_type: RiskObjectType = None,
                     source: str = None) -> Union[DataFrame, None]:

    try:
        risk_df = (spark.read.table('.'.join([database, table])))
    except Exception as exc:
        logger.error(f"failed to read {database}.{table}: {exc}")
        return None

    if time_start and not time_end:
        time_end = datetime.now()

    if time_start:
        risk_df = risk_df.filter((F.col("_event_time") >= time_start) & (F.col("_event_time") <= time_end))

    if risk_object:
        risk_df = risk_df.filter(F.col("risk_object") == risk_object)

    if object_type:
        risk_df = risk_df.filter(F.col("object_type") == object_type)

    if source:
        risk_df = risk_df.filter(F.col("source") == source)

    return risk_df
