from typing import Union

from pyspark.sql import DataFrame, Column

from . import _functions
from ._entities import BackendDefaults, DefaultColumns, RiskObjectType

__all__ = ["write_risk_index_from_df", "add_risk_score", "list_risk_table"]


def write_risk_index_from_df(df: DataFrame, risk_column: Column = DefaultColumns.RISK.value,
                             database: str = BackendDefaults.DATABASE,
                             risk_table: str = BackendDefaults.RISK_TABLE) -> bool:
    """update the risk index using the current DataFrame

    :param df: DataFrame containing the _risk column
    :type df: DataFrame
    :param risk_column: _risk column if ad-hoc, defaults to DefaultColumns.RISK.value
    :type risk_column: Column, optional
    :param database: database if ad-hoc, defaults to BackendDefaults.DATABASE
    :type database: str, optional
    :param risk_table: table if ad-hoc, defaults to BackendDefaults.RISK_TABLE
    :type risk_table: str, optional
    :return: True|False
    :rtype: bool
    """

    return _functions._write_risk_index_from_df(df=df, risk_column=risk_column,
                                                database=database, risk_table=risk_table)


def add_risk_score(df: DataFrame, risk_object: Column, object_type: RiskObjectType,
                   impact: int = 10, confidence: int = 10, source: str = None, annotation: dict = None) -> DataFrame:
    """add a new risk column to the dataframe

    :param df: incoming dataframe
    :type df: DataFrame
    :param risk_object: risk scores apply to either a user, or a device
    :type risk_object: Literal[&quot;user&quot;, &quot;host&quot;]
    :param risk_score: a score between 0 and 100, defaults to 10
    :type risk_score: int, optional
    :param risk_entity_column: column to apply the risk score to. Defaults to the CIM columns (user or dvc_hostname), defaults to None
    :type risk_entity_column: str, optional
    :return: augmented DataFrame
    :rtype: DataFrame
    """
    return _functions._add_risk_score(df=df, risk_object=risk_object, object_type=object_type, impact=impact,
                                      confidence=confidence, source=source, annotation=annotation)


def list_risk_table(database: str = BackendDefaults.DATABASE, table: str = BackendDefaults.RISK_TABLE,
                    time_start: str = None, time_end: str = None,
                    risk_object: str = None, object_type: RiskObjectType = None,
                    source: str = None) -> Union[DataFrame, None]:
    """search the risk table for risk modifiers, or return all of it as a dataframe

    :param database: _description_, defaults to None
    :type database: str, optional
    :param table: _description_, defaults to None
    :type table: str, optional
    :param time_start: _description_, defaults to None
    :type time_start: str, optional
    :param time_end: _description_, defaults to None
    :type time_end: str, optional
    :param risk_object: _description_, defaults to None
    :type risk_object: str, optional
    :param object_type: _description_, defaults to None
    :type object_type: RiskObjectType, optional
    :param source: _description_, defaults to None
    :type source: str, optional
    :return: _description_
    :rtype: Union[DataFrame, None]
    """
    return _functions._list_risk_table(database=database, table=table, time_start=time_start, time_end=time_end,
                                       risk_object=risk_object, object_type=object_type, source=source)
