import json
import re
from datetime import datetime
from typing import List, Union, Literal, Tuple, Iterator, Optional, Dict, Type
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession
from pyspark.sql import Column, Window, DataFrame
from pyspark.sql import functions as F
from pyspark.errors import AnalysisException

from databricks.sirens.config_reader import ThreatHuntReader
from ._entities import HuntConfigMeta, OutputFormat, BackendDefaults, ActiveHunt, DefaultColumns, HuntRecord, \
    HuntMetaCols
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)

spark = SparkSession.builder.getOrCreate()


class Output:
    def __init__(self, data: List[dataclass], format):
        self.format = format
        self.data = data

    def write(self):
        if self.format.value == "dataframe":
            return self.as_dataframe()
        if self.format.value == "json":
            return self.as_json()
        if self.format.value == "py_dataclass":
            return self.as_object()

    @classmethod
    def as_dict(self, data):
        data_as_dict = []
        [data_as_dict.append(asdict(line)) for line in data]
        return data_as_dict

    @classmethod
    def dataframe_as_dict(self, data: DataFrame) -> List[Dict]:
        return list(map(lambda row: row.asDict(), data.collect()))

    def as_object(self):
        if BaseUtils.is_spark_dataframe(self.data):
            items = []
            df_as_list = self.dataframe_as_dict(self.data)
            [items.append(ActiveHunt(**row)) for row in df_as_list]
            return items
        else:
            return self.data

    def as_dataframe(self):
        if BaseUtils.is_spark_dataframe(self.data):
            return self.data
        else:
            global spark
            new_data = self.as_dict(self.data)
            return spark.createDataFrame(new_data, self.data[0].schema)

    def as_json(self):
        if BaseUtils.is_spark_dataframe(self.data):
            new_data = self.dataframe_as_dict(self.data)
            return json.dumps(new_data)
        else:
            new_data = self.as_dict(self.data)
            return json.dumps(new_data)


class Utils:

    @staticmethod
    def _filter_results(database: str, index_table: str, hunt_table: str,
                        hunt_name: str = None, time_start: str = None, time_end: str = None,
                        run_id: str = None, command_name: str = None, status: str = None, state: str = None,
                        result: str = None, index_only: bool = False,
                        ) -> DataFrame:

        logger.debug(f"reading table: {'.'.join([database, index_table])}")
        indexDF = (spark.read.table('.'.join([database, index_table])))
        if not index_only:
            indexDF = (indexDF.withColumnsRenamed({"start_time": "index_start_time",
                                                   "end_time": "index_end_time",
                                                   "result": "index_result"}))
        # lookup records in the results table
        if not index_only:
            dataDF = spark.read.table('.'.join([database, hunt_table]))

        # set time_end if time_start spec;d and not end time
        if time_start and not time_end:
            time_end = datetime.now()

        # dependant filters based on index_only lookups
        if time_start and not index_only:
            indexDF = indexDF.filter((F.col("index_start_time") >= time_start) & (F.col("index_end_time") <= time_end))
        elif time_start and index_only:
            indexDF = indexDF.filter((F.col("start_time") >= time_start) & (F.col("end_time") <= time_end))

        if run_id:
            indexDF = indexDF.filter(F.col("run_id") == run_id)
            if not index_only:
                dataDF = dataDF.filter(F.col("run_id") == run_id)

        if hunt_name:
            indexDF = indexDF.filter(F.col("hunt_name") == hunt_name)

        if status:
            indexDF = indexDF.filter(F.col("status") == status)

        if state:
            indexDF = indexDF.filter(F.col("state") == state)

        if result and not index_only:
            indexDF = indexDF.filter(F.col("index_result") == result)
        elif result and index_only:
            indexDF = indexDF.filter(F.col("result") == result)

        # apply more specific filter if required.
        if command_name and not index_only:
            dataDF = dataDF.filter(F.col("command_name") == command_name)

        if not index_only:
            df = indexDF.join(dataDF, on="run_id", how="left")
        else:
            df = indexDF

        if df.isEmpty():
            logger.warning("No records found matching search filters")
            return

        return df

    @staticmethod
    def _write_hunt_index(database: BackendDefaults.DATABASE, table: BackendDefaults.INDEX_TABLE,
                          update_df: DataFrame) -> bool:
        from delta.tables import DeltaTable as delta

        if "start_time" in update_df.columns:
            update_df = update_df.withColumn("start_time", F.col("start_time").cast("timestamp"))
        if "end_time" in update_df.columns:
            update_df = update_df.withColumn("end_time", F.col("end_time").cast("timestamp"))

        # check this dataframe is an instance of ActiveHunt (i.e fits the table schema - somehow)
        try:
            update_df.to(ActiveHunt.schema)
        except Exception as exc:
            logger.error(f'not a valid hunt record: {exc}')
            return False

        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        index_table = delta.forName(spark, f"{database}.{table}")

        index_table.alias('index') \
            .merge(
            update_df.alias('updates'),
            'index.run_id = updates.run_id'
        ) \
            .whenMatchedUpdate(set={
            "start_time": 'updates.start_time',
            "end_time": 'updates.end_time',
            "state": 'updates.state',
            "status": 'updates.status',
            "result": 'updates.result',
            "_workflow_status": 'updates._workflow_status'
        }).execute()

        return True

    @staticmethod
    def _write_hunt_record(database: BackendDefaults.DATABASE, table: BackendDefaults.INDEX_TABLE,
                           update_df: DataFrame, command_name: str) -> bool:

        from delta.tables import DeltaTable as delta
        # check this dataframe is an instance of ActiveHunt (i.e fits the table schema - somehow)
        try:
            update_df.to(HuntRecord.schema)

        except Exception as exc:
            logger.error(f'not a valid hunt record: {exc}')
            return False

        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        meta_cols = [column for column in update_df.columns if column.startswith("_")]
        update_set = {i.name: str('updates.' + i.value) for i in HuntMetaCols if i.value in meta_cols}

        hunt_table = delta.forName(spark, f"{database}.{table}")

        hunt_table.alias('hunt') \
            .merge(
            update_df.alias('updates'),
            'hunt.run_id = updates._orig_run_id and hunt.command_name = updates._orig_command_name and hunt._command_seq = updates._command_seq'
        ).whenMatchedUpdate(set=update_set).execute()

        return True

    @staticmethod
    def _get_analytics_commands(df: DataFrame) -> Tuple[List[str], List[str], List[str], List[str], List[str]]:
        # get an ordered list of analytic commands that were run in the hunt
        commands_df = (df.select("index_start_time", "index_end_time", "command_name", "description", "run_id")
                       .select(F.concat_ws("~~~", "index_start_time", "index_end_time", "command_name",
                                           "description", "run_id").alias("new")))
        unique_list = sorted([i['new'] for i in commands_df.select('new').distinct().collect()])

        try:
            start_time, end_time, commands, description, run_id = zip(
                *map(lambda a: (a.split("~~~")[0], a.split("~~~")[1],
                                a.split("~~~")[2], a.split("~~~")[3], a.split("~~~")[4]), unique_list))
        except IndexError as exc:
            logger.info("results not available. Is the hunt status finished?")
            return [None, None, None, None, None]

        return start_time, end_time, commands, description, run_id

    @staticmethod
    def _get_elapsed_time(start_time, end_time):
        time_string = str(
            datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S.%f') - datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f'))
        time_obj = datetime.strptime(time_string, '%H:%M:%S.%f')
        total_seconds = str(
            round(time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second + time_obj.microsecond / 1000000, 2))
        return total_seconds

    @staticmethod
    def _get_analytic_results(df, analytic):
        df2 = df.filter(df.command_name == analytic)
        keys_df = df2.select(F.explode(F.map_keys(F.col("result")))).distinct()
        keys = list(map(lambda row: row[0], keys_df.collect()))
        key_cols = list(map(lambda f: F.col("result").getItem(f).alias(str(f)), keys))

        meta_cols = [column for column in df2.columns if column.startswith("_")]
        [key_cols.append(x) for x in meta_cols]

        df3 = df2.select(*key_cols, F.col("run_id").alias("_orig_run_id"),
                         F.col("command_name").alias("_orig_command_name"))

        if '_risk_column' in df3.columns:
            df3 = df3.withColumnRenamed('_risk_column', DefaultColumns.RISK.value)

        return df3

    @staticmethod
    def _dataframe_as_dict(df) -> dict:
        df = df.drop("_orig_run_id", "_orig_command_name")
        r = [row.asDict() for row in df.collect()]
        return r
        #return [row.get(src_col).asDict() for row in r]


def _list_hunt_library(output_format: OutputFormat = OutputFormat.asObject,
                       hunt_name: str = None) -> Optional[OutputFormat]:
    logger.info("looking up hunt library")
    hunts = []
    hunt_configs = ThreatHuntReader().list_hunts()
    if not hunt_configs:
        logger.warning("No threat hunts found.")
        return None

    # return only one item if a specific hunt searched for
    if hunt_name:
        configs = [h for h in hunt_configs if hunt_name in h.get("hunt").get("name")]
    # all hunts
    else:
        configs = hunt_configs

    [hunts.append(HuntConfigMeta.from_hunt_dict(hunt)) for hunt in configs]
    return Output(hunts, output_format).write()


def _add_row_numbers(df: DataFrame, time_col: Column = None):
    if not time_col:
        time_col = "_event_time"

    # check column exists
    if isinstance(time_col, str):
        time_col_normalized = re.sub('`', '', time_col)
        if time_col_normalized not in df.columns:
            logger.info(f'column: {time_col} does not exist')
            return df

    # remove column if it exists already.
    if DefaultColumns.ROW.value in df.columns:
        logger.debug(f"row column already exists. dropping.")
        df = df.drop(F.col(DefaultColumns.ROW.value))

    # add column
    w = Window.partitionBy(F.lit(1)).orderBy(F.col(time_col))
    return df.select(F.row_number().over(w).alias(DefaultColumns.ROW.value), "*")


def _annotate(df: DataFrame, row_number: int, annotation: dict, overwrite_existing: bool = False) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()

    if DefaultColumns.ROW.value not in df.columns:
        logger.warning("row_numbers not in DataFrame - attempting to add. - use `add_row_numbers()` before annotation")
        try:
            df = _add_row_numbers(df)
        except Exception as exc:
            logger.info(f'unable to add row numbers. Add manually.: {exc}')
            return df

    if DefaultColumns.ROW.value not in df.columns:
        logger.info('unable to add row numbers. Add manually')
        return df

    # create map column if it doesn't exist
    # if DefaultColumns.ANNOTATION.value not in df.columns:
    # TODO - have an error - each read/write / overwrites. started when you created the column on write.
    # seems the F.when NEVER triggers.
    df = df.withColumn(DefaultColumns.ANNOTATION.value, F.create_map())

    # set overwrite deuplicate keys
    if overwrite_existing:
        current_setting = spark.conf.get("spark.sql.mapKeyDedupPolicy")
        spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

    dict_as_cols = [item for line in [[F.lit(k), F.lit(v)] for k, v in annotation.items()] for item in line]
    df = (df.withColumn(DefaultColumns.ANNOTATION.value,
                        F.when(F.col(DefaultColumns.ROW.value).cast('int') == int(row_number),
                               F.map_concat(F.col(DefaultColumns.ANNOTATION.value), F.create_map(*dict_as_cols)))
                        .otherwise(F.col(DefaultColumns.ANNOTATION.value))))

    # restore original setting
    if overwrite_existing:
        spark.conf.set("spark.sql.mapKeyDedupPolicy", current_setting)

    return df


def _mark(df: DataFrame, rows: str, toggle: bool = True):
    # check and attempt to add row_numbers if missinf
    if DefaultColumns.ROW.value not in df.columns:
        logger.warning("row_numbers not in DataFrame - attempting to add. - use `add_row_numbers()` before annotation")
        try:
            df = _add_row_numbers(df)
        except Exception as exc:
            logger.info(f'unable to add row numbers. Add manually.: {exc}')
            return df

    if DefaultColumns.ROW.value not in df.columns:
        logger.info('unable to add row numbers. Add manually')
        return df

    if not isinstance(rows, str):
        rows = str(rows)

    if DefaultColumns.MARKED.value not in df.columns:
        df = df.withColumn(DefaultColumns.MARKED.value, F.lit(''))
    if toggle:
        val = '*'
    else:
        val = ''

    # break row_numbers into a list
    row_numbers = sum((i if len(i) == 1 else list(range(i[0], i[1] + 1))
                       for i in ([int(j) for j in i if j] for i in
                                 re.findall(r'(\d+),?(?:-(\d+))?', rows))), [])

    df = (df.withColumn(DefaultColumns.MARKED.value,
                        F.when(F.col(DefaultColumns.ROW.value).isin(row_numbers), F.lit(val))
                        .otherwise(F.col(DefaultColumns.MARKED.value))))

    # reorder columns (_marked left justified)
    cols = df.columns
    cols.remove(DefaultColumns.MARKED.value)
    df = df.select(F.col(DefaultColumns.MARKED.value).alias(DefaultColumns.MARKED.value), *cols)

    return df


def _filter_marked(df: DataFrame) -> DataFrame:
    if DefaultColumns.MARKED.value not in df.columns:
        logger.warning(f"Column {DefaultColumns.MARKED.value} dies not exist. run mark() function first.")
        return df

    return df.filter(F.col(DefaultColumns.MARKED.value) == "*")


def _list_executed_hunts(hunt_name: str = None, time_start: str = None, time_end: str = None,
                         run_id: str = None,
                         status: Optional[Literal['initializing', 'running', 'finished']] = None,
                         state: Optional[Literal['initializing', 'active', 'paused', 'finished']] = None,
                         result: Optional[Literal['success', 'failed']] = None,
                         database: str = BackendDefaults.DATABASE,
                         hunt_index: str = BackendDefaults.INDEX_TABLE,
                         hunt_table: str = BackendDefaults.HUNT_TABLE,
                         output_format: OutputFormat = OutputFormat.asDataFrame) -> OutputFormat:
    df = Utils._filter_results(hunt_name=hunt_name, time_start=time_start, time_end=time_end, run_id=run_id,
                               status=status, state=state, result=result, index_only=True,
                               database=database, index_table=hunt_index, hunt_table=hunt_table)
    df = df.sort(df.start_time.desc())
    return Output(df, output_format).write()


def _get_hunt_result(hunt_name: str = None, time_start: str = None, time_end: str = None,
                     run_id: str = None, command_name: str = None,
                     database: str = BackendDefaults.DATABASE,
                     hunt_index: str = BackendDefaults.INDEX_TABLE,
                     hunt_table: str = BackendDefaults.HUNT_TABLE,
                     output_format: OutputFormat = OutputFormat.asDataFrame) -> Tuple[List[OutputFormat], None]:
    df = Utils._filter_results(hunt_name=hunt_name, time_start=time_start, time_end=time_end,
                               run_id=run_id, command_name=command_name,
                               database=database, index_table=hunt_index, hunt_table=hunt_table)

    if BaseUtils.is_spark_dataframe(df):
        _, _, commands, _, run_id = Utils._get_analytics_commands(df)
        if not commands:
            return None

        dfs = []
        for _, analytic in enumerate(commands):
            df3 = Utils._get_analytic_results(df, analytic)
            dfs.append(df3)
        return dfs
    else:
        return


def _show_hunt_results(hunt_name: str = None, time_start: str = None, time_end: str = None,
                       run_id: str = None, command_name: str = None,
                       database: str = BackendDefaults.DATABASE,
                       hunt_index: str = BackendDefaults.INDEX_TABLE,
                       hunt_table: str = BackendDefaults.HUNT_TABLE,
                       output_format: OutputFormat = OutputFormat.asDataFrame) -> Union[
    Iterator[Tuple[str, DataFrame]], None]:
    df = Utils._filter_results(hunt_name=hunt_name, time_start=time_start, time_end=time_end, run_id=run_id,
                               command_name=command_name, database=database,
                               index_table=hunt_index, hunt_table=hunt_table)
    if BaseUtils.is_spark_dataframe(df):
        start_time, end_time, commands, description, run_id = Utils._get_analytics_commands(df)
        if not commands:
            yield None, None
        else:
            for num, analytic in enumerate(commands):
                results_df = Utils._get_analytic_results(df, analytic)
                total_seconds = Utils._get_elapsed_time(start_time[num], end_time[num])

                html_title = "<h1>" + analytic + "</h1><p>" \
                             + description[num] \
                             + "</p><p><b>Run_Id:</b> " + run_id[num] + "</p><p><b>Start Time:</b> " + start_time[
                                 num] + " <b>End Time:</b> " + end_time[num] \
                             + " <b>Elapsed Time:</b> " + total_seconds + "</p>"

                yield html_title, results_df


def _is_hunt_record(df) -> bool:
    try:
        if len(df.select("_orig_command_name", "_orig_run_id").columns) == 2:
            return True
    except TypeError as exc:
        print(f"not hunt rec: {exc}")
        return False

    print("something went wrong")
    return False


def _is_hunt_index(df) -> bool:
    try:
        d = Utils._dataframe_as_dict(df)
        if d:
            ActiveHunt(**d[0])
        return True
    except TypeError:
        return False


def _write_hunt(df: DataFrame, command_name: str = None,
                database: str = BackendDefaults.DATABASE,
                hunt_index: str = BackendDefaults.INDEX_TABLE,
                hunt_table: str = BackendDefaults.HUNT_TABLE) -> bool:
    # check what type of DataFrame has been passed.
    if _is_hunt_index(df):
        return Utils._write_hunt_index(database=database,
                                       table=hunt_index, update_df=df)
    if _is_hunt_record(df):
        if not command_name:
            logger.error("command_name must be specified for writing hunt records.")
            return False
        return Utils._write_hunt_record(database=database,
                                        table=hunt_table, update_df=df, command_name=command_name)

    logger.error('dataframe does not contain hunt data')
    return False
