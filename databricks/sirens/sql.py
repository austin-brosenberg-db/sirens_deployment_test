"""Implements Spark SQL commands used throughout Sirens.

Author:
    Derek King (September 2022)

Classes:
    SQL_Query()

Functions:
    _create_database()
    _create_table()
    _describe_table()
    _max_value()


"""
from typing import Union, Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from databricks.sirens.exceptions import SirensSQLException
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class SqlQuery:

    @staticmethod
    def create_database(database_name: str, spark: SparkSession = None) -> bool:
        """Create a Managed Database

        :param spark: active session
        :type spark: SparkSession
        :param database_name: Name to create
        :type database_name: str
        :raises SirensSQLException: _description_
        :return: _description_
        :rtype: bool
        """
        try:
            if not spark:
                spark = SparkSession.getActiveSession()

            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            spark.sql(f"USE {database_name}")
        except Exception as exc:
            raise SirensSQLException(f"unable to create database: {database_name}: {exc}") from exc

        return True

    @staticmethod
    def create_table(spark: SparkSession, database: str, table: str, partition_cols: List[str]) -> bool:
        """Create Managed Delta Table

        :param spark: active session
        :type spark: SparkSession
        :param database: enclosing database name
        :type database: str
        :param table: table to create
        :type table: str
        :param partition_cols: list of partition columns, if any
        :type partition_cols: list
        :return: True|False
        :rtype: bool
        """
        sql_str = f"CREATE table {database}.{table}"
        if len(partition_cols) > 0:
            sql_str += f" PARTITIONED BY ({','.join(partition_cols)})"
        try:
            spark.sql(sql_str)
            return True
        except Exception:
            return False

    @staticmethod
    def describe_table(spark: SparkSession, database: str, table: str) -> Optional[DataFrame]:
        """Describe Delta Table

        :param spark: active session
        :type spark: SparkSession
        :param database: enclosing database to use
        :type database: str
        :param table: table name
        :type table: str
        :return: Spark Information or False
        :rtype: Union[list, bool]
        """
        sql_str = f"DESCRIBE table {database}.{table}"
        try:
            return spark.sql(sql_str)
        except AnalysisException:
            return None
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensSQLException(f"{exc}") from exc

    @staticmethod
    def get_max_value(spark: SparkSession, database: str, table: str, column: str) -> Optional[DataFrame]:
        """given database, table and column, return the latest/max value

        :param spark: Spark session
        :type spark: SparkSession
        :param database: database name
        :type database: str
        :param table: table name
        :type table: str
        :param column: column for max
        :type column: str
        :return: latest/max row
        :rtype: DataFrame
        """
        sql_str = f"select max({column}) as max_value FROM {database}.{table}"
        try:
            return spark.sql(sql_str)
        except Exception:
            pass

        return None
