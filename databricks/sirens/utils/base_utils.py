"""Implements shared code/functions for Sirens

Author:
    Derek King (4th July 2022)
"""
import json
import os
import re
import uuid
import itertools

from pathlib import Path
from typing import Union, List, Type, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import TimestampType, DataType, StructType, ArrayType, StructField, StringType

from databricks.sirens.exceptions import SirensException, SirensConfigException
from databricks.sirens.logging import get_logger
from databricks.sirens.sql import SqlQuery
from databricks.sirens.utils.path_utils import PathUtils
from databricks.sirens.utils.schema_utils import DataFrameSchema

logger = get_logger(__name__)


class BaseUtils:
    _is_remote_only = None

    @staticmethod
    def is_time_interval_valid(interval_string):
        """takes a sql time interval in form 'INTERVAL 1 HOUR' and decides its validity

        :param interval_string: The SQL time interval (ie 'INTERVAL 1 DAY')
        :type interval_string: str
        :return: True or False
        :rtype: bool
        """
        valid_literals = ["YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND",
                          "YEARS", "MONTHS", "WEEKS", "DAYS", "HOURS", "MINUTES", "SECONDS", "MILLISECONDS",
                          "MICROSECONDS"]
        strs = interval_string.split()

        if 'INTERVAL' != strs[0].strip().upper() or strs[2].strip().upper() not in valid_literals:
            return False
        try:
            _ = int(strs[1].strip())
        except ValueError:
            return False

        return True

    @staticmethod
    def _create_table(spark: SparkSession, database: str, table_name: str, partition_cols: List[str] = None) -> bool:
        if partition_cols is None:
            partition_cols = []
        return SqlQuery.create_table(spark, database, table_name, partition_cols)

    @staticmethod
    def _if_table_exists(spark: SparkSession, database: str, table_name: str) -> bool:
        return SqlQuery.describe_table(spark, database, table_name) is not None

    @staticmethod
    def _get_table_schema(spark: SparkSession, database: str, table_name: str) -> Union[StructType, bool]:
        table_schema = SqlQuery.describe_table(spark, database, table_name)
        if table_schema:
            # Convert the schema rows into StructFields using DataFrameSchema
            fields = []
            for row in table_schema.collect():
                try:
                    field = DataFrameSchema.translate_type(row.col_name, row.data_type.lower())
                    fields.append(field)
                except KeyError:
                    logger.warning(f"Unsupported data type {row.data_type} for column {row.col_name}")
                    # Fall back to StringType for unsupported types
                    fields.append(StructField(row.col_name, StringType(), True))
            return DataFrameSchema.as_struct(fields)

        return False

    @staticmethod
    def _create_empty_dataframe(spark: SparkSession, schema: Union[StructType, str]) -> DataFrame:
        logger.debug('creating empty dataframe')
        return BaseUtils.create_dataframe(spark, schema)

    @staticmethod
    def cast_columns(col_schema: StructType, df: DataFrame) -> DataFrame:
        for col in col_schema:
            if col.name in df.columns:
                try:
                    #if it's a nested struct, check that all of the substructures match what is in the dataframe
                    if isinstance(col.dataType, StructType):
                        matched_schema = BaseUtils.match_struct_fields(col, df.schema[col.name])
                        df = df.withColumn(col.name, df[col.name].cast(matched_schema))

                    else:
                        df = df.withColumn(col.name, df[col.name].cast(col.dataType))
                except Exception as e:
                    print(f"Warning: Failed to cast column: {col.name} to data_type: {col.dataType}")
                    logger.warning(f"Warning: Failed to cast column: {col.name} to data_type: {col.dataType}")
                    pass
        return df

    @staticmethod
    def match_struct_fields(struct, table_col):
        """
        Matches the fields of a given struct with the fields of a table column, ensuring that the order
        of the input columns is maintained and that the columns in the table match the schema definition fields.
        :param struct: The input struct whose fields need to be matched.
        :type struct: pyspark.sql.types.StructType
        :param table_col: The table column whose fields need to be matched with the struct.
        :type table_col: pyspark.sql.types.StructField
        :return: A new StructType with fields matched to the table column.
        :rtype: pyspark.sql.types.StructType
        """
        # keep order dependence of input cols
        fields = table_col.dataType.fields

        output_fields = []

        #columns in table *need* to match up with schema definition fields
        for f in fields:
            if isinstance(f.dataType, StructType):
                sub_fields = BaseUtils.match_struct_fields(struct.dataType[f.name], f)
                output_fields.append(StructField(f.name, sub_fields, True))
            else:
                output_fields.append(struct.dataType[f.name])
        return StructType(output_fields)

    @staticmethod
    def get_latest_ts(spark: SparkSession, database: str, table: str, column: str) -> TimestampType:
        """given the datasource object, returns the last/latest timestamp currently in the given table.
        :param spark: spark object
        :type spark: _type_
        :param database: databasename
        :type database: str
        :param table: table name
        :type table: str
        :param column: max column
        :type column: str
        :return: max timestamp
        :rtype: TimestampType
        """

        max_row = SqlQuery.get_max_value(spark, database, table, column)
        if max_row is not None:
            try:
                latest_timestamp = max_row.first()["max_value"]
                if not isinstance(latest_timestamp, type(None)):
                    latest_timestamp = latest_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

            except Exception as exc:
                latest_timestamp = None
        else:
            latest_timestamp = None

        return latest_timestamp

    @staticmethod
    def get_uuid_str() -> str:
        """Create a new UUID

        :return: uuid
        :rtype: str
        """
        return str(uuid.uuid4())

    @staticmethod
    def maybe_get_local_file_path(path: str) -> str:
        """given a LocalPath predicate, return the local directory path for sample data
        """
        if path.startswith('LocalPath://'):
            return BaseUtils._get_local_file_path(path)
        return path

    @staticmethod
    def _get_local_file_path(path: str) -> str:
        """given a LocalPath predicate, return the local directory path for sample data

        :param path: LocalPath path from rawFilePath
        :type path: str
        :return: Path to the data
        :rtype: Path
        """
        if path.startswith('LocalPath://'):
            path = path[12:]
        else:
            logger.error(f"rawPath is not 'LocalPath://' as expected.  is: {path}")
            raise SirensConfigException(f"rawPath is not 'LocalPath://' as expected.  is: {path}")

        file_names = PathUtils.get_relative_file_paths(["log_sources", f"{path}"])

        for file in file_names:
            try:
                path_name = Path(file)
                if path_name.is_dir():
                    for root, dirs, files in os.walk(os.path.abspath(path_name)):
                        path_name = "file://" + root
                    return str(path_name)

            except Exception as exc:
                logger.error(f"Cannot find a directory path: {path}. Error: {exc}")
                raise SirensConfigException(exc) from exc

        raise SirensConfigException(f"Cannot find a directory path: {path}")

    @staticmethod
    def check_event_timestamp(df: DataFrame, timestamp_format: Union[TimestampType, str]) -> DataFrame:
        """all checks to try and ensure we have a reliable _event_time, and therefore _event_date column
        :param df: DataFrame to be processed
        :type df: DataFrame
        :param timestamp_format: the expected timestamp format of the raw events (defined in inputs.yaml)
        :type timestamp_format: DateFormat
        :return: DataFrame with _invalid_timestamp col added, or unchanged
        :rtype: DataFrame
        """
        # See. projects - "_invalid_timestamp BUG needs fixing".
        # the .collect() causes a structured streaming error. Need to find a workaround for this.
        return df

    # Check if the _raw_time column was added correctly, from the events timestamp.
    # if not, default to now.
    # if yes, we need to drop the added column before returning.

    # df = df.select("*", when(to_timestamp(col("_raw_time"), timestamp_format).cast("timestamp").isNull(),
    #               date_format(current_timestamp(), timestamp_format)).alias("_invalid_timestamp"))

    #  remove the _invalid_timestamp column if all values are correct (ie null).
    #  ideally - I would prefer when to not create the column at all if the timestamp is valid. maybe revisit.
    # null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in ["_invalid_timestamp"]]).collect()[0].asDict()
    # if null_counts.get("_invalid_timestamp") > 0:
    #    df = df.drop("_invalid_timestamp")

    # return(df)

    # return(df)

    def __normalise_fieldname__(raw: str):
        return re.sub('[^A-Za-z0-9_]+', '_', raw.strip())

    def flatten_frame(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):

        def __normalise_fieldname__(raw: str):
            return re.sub('[^A-Za-z0-9_]+', '_', raw.strip())

        def __rename_nested_field__(in_field: DataType, fieldname_normaliser):
            if isinstance(in_field, ArrayType):
                dtype = ArrayType(__rename_nested_field__(in_field.elementType, fieldname_normaliser),
                                  in_field.containsNull)
            elif isinstance(in_field, StructType):
                dtype = StructType()
                for field in in_field.fields:
                    dtype.add(fieldname_normaliser(field.name),
                              __rename_nested_field__(field.dataType, fieldname_normaliser))
            else:
                dtype = in_field
            return dtype

        def __get_fields_info__(dtype: DataType, name: str = ""):
            ret = []
            if isinstance(dtype, StructType):
                for field in dtype.fields:
                    for child in __get_fields_info__(field.dataType, field.name):
                        wrapped_child = ["{prefix}{suffix}".format(
                            prefix=("" if name == "" else "`{}`.".format(name)), suffix=child[0])] + child[1:]
                        ret.append(wrapped_child)
            elif isinstance(dtype, ArrayType) and (
                isinstance(dtype.elementType, ArrayType) or isinstance(dtype.elementType, StructType)):
                for child in __get_fields_info__(dtype.elementType):
                    wrapped_child = ["`{}`".format(name)] + child
                    ret.append(wrapped_child)
            else:
                return [["`{}`".format(name)]]
            return ret

        def normalise_fields_names(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):
            return df.select([
                col("`{}`".format(field.name)).cast(__rename_nested_field__(field.dataType, fieldname_normaliser))
                .alias(fieldname_normaliser(field.name)) for field in df.schema.fields])

        cols = []

        for child in __get_fields_info__(df.schema):
            if len(child) > 2:
                ex = "x.{}".format(child[-1])
                for seg in child[-2:0:-1]:
                    if seg != '``':
                        ex = "transform(x.{outer}, x -> {inner})".format(outer=seg, inner=ex)
                        ex = "transform({outer}, x -> {inner})".format(outer=child[0], inner=ex)
            else:
                ex = ".".join(child)
                cols.append(expr(ex).alias(fieldname_normaliser("_".join(child).replace('`', ''))))
        return df.select(cols)

    @staticmethod
    def _get_dbutils(spark):
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)

    @staticmethod
    def get_read_method(spark: SparkSession, config: dict):
        is_dlt = config.get("dlt_read", False)
        if not isinstance(is_dlt, bool):
            is_dlt = is_dlt.lower() == "true" if isinstance(is_dlt, str) else False
        is_streaming = BaseUtils.is_streaming(config)
        if is_dlt:
            import dlt
            return dlt.readStream if is_streaming else dlt.read
        else:
            return spark.readStream.table if is_streaming else spark.read.table

    @staticmethod
    def is_streaming(config: dict) -> bool:
        return str(config.get('streamType', 'batch')).lower() == "streaming"

    @staticmethod
    def dict_to_json_file(data: dict, target: str):
        """write python dict to json file

        :param data: dict to write
        :type data: dict
        :param target: full path name inc filename
        :type target: str
        :raises Exception: _description_
        """
        with open(target, 'w') as fp:
            json.dump(data, fp, indent=4)

        return True

    @staticmethod
    def frame_to_dict(df: DataFrame) -> List[dict]:
        if BaseUtils.is_spark_dataframe(df):
            try:
                return [row.asDict() for row in df.collect()]
            except Exception as exc:
                logger.error(f"{exc}")
                raise SirensException(f"{exc}")
        else:
            logger.error(f'expected Dataframe got {type(df)}')
            raise SirensException(f'expected Dataframe got {type(df)}')

    @staticmethod
    def create_dataframe(spark: SparkSession, schema: Union[StructType, str], data: list = None) -> DataFrame:
        """create a dataframe with or without data with a given schema

        :param spark: Spark session object
        :type spark: SparkSession
        :param schema: schema
        :type schema: StructType
        :param data: any input data, defaults to None
        :type data: list, optional
        :raises SirensException: _description_
        :return: created frame
        :rtype: DataFrame
        """
        if not data:
            data = []
        if isinstance(schema, StructType) or isinstance(schema, str):
            try:
                return spark.createDataFrame(data, schema)
            except Exception as exc:
                logger.error(f"{exc}")
                raise SirensException(f"{exc}")
        else:
            logger.error(f"expected StructType got {type(schema)}")
            raise SirensException(f"expected StructType got {type(schema)}")

    @staticmethod
    def filter_dataframe(df: DataFrame, filter: str) -> DataFrame:
        """filter dataframe for filter condition

        :param df: incoming dataframe
        :type df: DataFrame
        :param filter: SQL filter
        :type filter: str
        :return: filtered DataFrame or original if error
        :rtype: DataFrame
        """
        try:
            return df.filter(filter)
        except Exception as exc:
            logger.warning(f"{exc}")
            return df

    @staticmethod
    def query_table_version(table_name: str, version, spark: SparkSession):
        return spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {version}")

    @staticmethod
    def validate_sql_expression(sql_expression) -> bool:
        """take a sql expression and validate if its syntactically correct or not

        :param sql_expression: expression to be validated
        :type sql_expression: _type_
        :return: true or false
        :rtype: bool
        """
        from pyspark.sql.utils import ParseException
        try:
            spark_session = SparkSession.getActiveSession()
            # Corrects ISS-772.
            # parser = spark_session._jsparkSession.sessionState().sqlParser()
            # parser.parseExpression(sql_expression)
            query = f"SELECT {sql_expression}"
            spark_session.sql(f"EXPLAIN {query}")
        except ParseException as exc:
            logger.info(f"invalid sql filter: {sql_expression}, error: {exc}")
            return False
        except Exception as exc:
            logger.error(f"validate_sql_expression failed: {exc}, sql_expression: {sql_expression}")
            return False

        return True

    @staticmethod
    def _notebook_entry_point(dbutils):
        try:
            return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        except Exception:
            return None

    @staticmethod
    def _get_db_context():
        try:
            from dbruntime.databricks_repl_context import get_context
            context = get_context()
            return context
        except ModuleNotFoundError:
            return None

    @staticmethod
    def get_notebook_name():
        spark = SparkSession.getActiveSession()
        notebook_name = None
        try:
            dbutils = BaseUtils._get_dbutils(spark)
            notebook_name = BaseUtils._notebook_entry_point(dbutils)
        except Exception:
            pass
        try:
            if not notebook_name:
                context = BaseUtils._get_db_context()
                if context:
                    notebook_name = context.notebookPath
            if notebook_name:
                return os.path.basename(notebook_name)
            else:
                return None
        except Exception:
            return None

    @staticmethod
    def get_notebook_path():
        spark = SparkSession.getActiveSession()
        notebook_path = None
        try:
            dbutils = BaseUtils._get_dbutils(spark)
            notebook_path = BaseUtils._notebook_entry_point(dbutils)
            if notebook_path:
                return notebook_path
        except Exception:
            pass
        try:
            if not notebook_path:
                context = BaseUtils._get_db_context()
                if context:
                    return context.notebookPath
                else:
                    return None
        except Exception:
            return None

    @classmethod
    def is_remote_only(cls) -> bool:

        if cls._is_remote_only is not None:
            return cls._is_remote_only
        try:
            from pyspark import core  # noqa: F401

            cls._is_remote_only = False
            return cls._is_remote_only
        except ImportError:
            cls._is_remote_only = True
            return cls._is_remote_only

    @classmethod
    def _is_remote(cls) -> bool:
        """
        Returns if the current running environment is for Spark Connect.
        """
        return ("SPARK_CONNECT_MODE_ENABLED" in os.environ) or cls.is_remote_only()

    @classmethod
    def _get_dataframe_class(cls) -> Type["DataFrame"]:
        from pyspark.sql.dataframe import DataFrame as PySparkDataFrame

        if cls._is_remote():
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            return ConnectDataFrame  # type: ignore[return-value]
        else:
            return PySparkDataFrame

    @staticmethod
    def is_spark_dataframe(df) -> bool:
        return isinstance(df, (DataFrame, BaseUtils._get_dataframe_class()))

    @staticmethod
    def group_items(collection: List[Dict], grouping_keys: List) -> List[Dict]:
        if isinstance(grouping_keys, str):
            grouping_keys = [grouping_keys]
        if isinstance(collection, dict):
            collection = [collection]
        return itertools.groupby(sorted(collection, key=lambda i: [(i[x]) for x in grouping_keys]),
                                 lambda i: [i[x] for x in grouping_keys])
