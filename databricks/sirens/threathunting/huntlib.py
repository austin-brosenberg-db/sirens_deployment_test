"""Threat Hunting Library

    Author: Derek King
    Date: 2023-11-22
    version: 0.1
"""
import uuid
from datetime import datetime
from itertools import chain
from typing import Literal

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from databricks.sirens.config_reader import ThreatHuntReader
from databricks.sirens.exceptions import SirensException
from databricks.sirens.logging import get_logger
from databricks.sirens.sql import SqlQuery
from databricks.sirens.utils.base_utils import BaseUtils
from ._entities import (BackendDefaults, ActiveHunt, HuntRecord, HuntResult, HuntBackend, HuntState,
                        HuntStatus, DefaultColumns)
from ._functions import Utils

logger = get_logger(__name__)

spark = SparkSession.getActiveSession()

_active_hunt_stack = []
_active_backend_stack = []


def get_uuid():
    return str(uuid.uuid4())


def get_notebook_name():
    try:
        return BaseUtils.get_notebook_name()
    except ImportError:
        return None


class ThreatHunt:
    def __init__(self, hunt_name: str,
                 database: str = BackendDefaults.DATABASE,
                 index_table: str = BackendDefaults.INDEX_TABLE,
                 hunt_table: str = BackendDefaults.HUNT_TABLE):
        global _active_hunt_stack
        global _active_backend_stack

        attacks, tags = None, None
        try:
            # read hunt.yaml - get attacks/tags keys if present.
            self.hunt_config = ThreatHuntReader().read(hunt_name=hunt_name)
            attacks = self.hunt_config.get("hunt").get("attacks")
            tags = self.hunt_config.get("hunt").get("tags")
        except Exception:
            logger.warning(f'failed to find hunt config for hunt: {hunt_name}')

        self.hunt_name = hunt_name

        active_hunt_obj = ThreatHunt._get_active_obj(hunt_name, type='hunt')
        active_backend_obj = ThreatHunt._get_active_obj(hunt_name, type='backend')
        if not active_hunt_obj:
            self.active_hunt_obj = ActiveHunt(run_id=get_uuid(), hunt_name=self.hunt_name, hunt_table=hunt_table,
                                              database=database, notebook=get_notebook_name(),
                                              status=HuntStatus.INITIALIZING, state=HuntState.INITIALIZING
                                              )
            if attacks:
                self.active_hunt_obj.attacks = attacks
            if tags:
                self.active_hunt_obj.tags = tags

            logger.debug(f'initialized ActiveHunt as: {self.active_hunt_obj}')

            _active_hunt_stack.append({self.hunt_name: self.active_hunt_obj})

            self.active_backend_obj = HuntBackend(hunt_name=hunt_name, database=database,
                                                  index_table=index_table, hunt_table=hunt_table)
            _active_backend_stack.append({self.hunt_name: self.active_backend_obj})
            ThreatHunt._initialize_backend(hunt=self.active_hunt_obj, backend=self.active_backend_obj)
            logger.info(f"Hunt: {self.active_hunt_obj.hunt_name} activated. Run_Id: {self.active_hunt_obj.run_id}")

        else:
            self.active_hunt_obj = active_hunt_obj
            self.active_backend_obj = active_backend_obj
            logger.info(f"Hunt: {self.active_hunt_obj.hunt_name} resumed. Run_Id: {self.active_hunt_obj.run_id}")

    def _get_state(self) -> str:
        return self.state

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self._cell_ended()
            logger.info(f"Hunt {self.hunt_name} paused.")
        else:
            logger.info(f"Hunt {self.hunt_name} failed: {exc_value}")

    @property
    def get_active_hunt(self):
        global _active_hunt_stack
        active_obj = ThreatHunt._get_active_obj(self.hunt_name, type='hunt')
        return active_obj

    @property
    def get_run_id(self):
        global _active_hunt_stack
        active_obj = ThreatHunt._get_active_obj(self.hunt_name, type='hunt')
        return active_obj.run_id

    @property
    def __repr__(self):
        global _active_hunt_stack
        active_obj = ThreatHunt._get_active_obj(self.hunt_name, type='hunt')
        return f"{active_obj}"

    @property
    def __str__(self):
        global _active_hunt_stack
        active_obj = ThreatHunt._get_active_obj(self.hunt_name, type='hunt')
        return f"{active_obj}"

    @staticmethod
    def _create_schema(schema):
        return SqlQuery.create_database(database_name=schema)

    @staticmethod
    def _initialize_backend(hunt: ActiveHunt, backend: HuntBackend):
        try:
            logger.debug(f"initializing backend database: {backend.database}")
            ThreatHunt._create_schema(backend.database)
            index_df = spark.createDataFrame([hunt], ActiveHunt.schema)
            index_df.write.mode('append').saveAsTable(f"{backend.database}.{backend.index_table}")
        except Exception as exc:
            logger.error(exc)
            raise SirensException(exc)
        return None

    @staticmethod
    def _get_active_obj(hunt_name, type: Literal["hunt", "backend"]):
        if "hunt" not in type and "backend" not in type:
            logger.error("Hunt Object has been corrupted. Please Report Error.")
            raise SirensException("Hunt Object has been corrupted. Please Report Error.")

        global _active_hunt_stack
        global _active_backend_stack

        if 'hunt' in type:
            obj = _active_hunt_stack
        else:
            obj = _active_backend_stack

        if len(obj) > 0:
            for itm in obj:
                for k, v in itm.items():
                    if k == hunt_name:
                        return v
        else:
            return None

    @staticmethod
    def _get_active_hunt_index(hunt_name):
        global _active_hunt_stack
        if not len(_active_hunt_stack) > 0:
            return None

        for itm, val in enumerate(_active_hunt_stack):
            hunt = val.get(hunt_name)
            if hunt:
                if hunt.hunt_name == hunt_name:
                    return itm
        return None

    @staticmethod
    def _update_active_hunt(hunt: ActiveHunt, start_time: str = None, end_time: str = None,
                            state: str = None, status: bool = None,
                            result: str = None):
        if start_time:
            hunt.start_time = start_time
        if end_time:
            hunt.end_time = end_time
        if state:
            hunt.state = state
        if status is not None:
            hunt.status = status
        if result:
            hunt.result = result

        return hunt

    @staticmethod
    def _update_index_table(hunt: ActiveHunt, backend: HuntBackend):
        update_df = spark.createDataFrame([hunt], ActiveHunt.schema)
        return Utils._write_hunt_index(database=backend.database, table=backend.index_table, update_df=update_df)

    def _cell_ended(self):
        global _active_hunt_stack
        global _active_backend_stack

        active_hunt_obj = ThreatHunt._get_active_obj(self.hunt_name, type='hunt')
        active_backend_obj = ThreatHunt._get_active_obj(self.hunt_name, type='backend')
        if active_hunt_obj:
            active_hunt_obj = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, state=HuntState.PAUSED)
            ThreatHunt._update_index_table(hunt=active_hunt_obj, backend=active_backend_obj)

        return active_hunt_obj

    @staticmethod
    def _flush(df: DataFrame, backend: HuntBackend):
        try:
            if "start_time" in df.columns:
                df = df.withColumn("start_time", F.col("start_time").cast("timestamp"))
            if "end_time" in df.columns:
                df = df.withColumn("end_time", F.col("end_time").cast("timestamp"))

            # rename of _risk column.
            if DefaultColumns.RISK.value in df.columns:
                df = df.withColumnRenamed(DefaultColumns.RISK.value, "_risk_column")

            df = df.to(HuntRecord.schema)
            df.write.mode('append').saveAsTable(f"{backend.database}.{backend.hunt_table}")
            logger.debug(f"active_hunt_obj flushed to disk: {backend.database}.{backend.hunt_table}")

        except Exception as exc:
            logger.error(exc)
            raise SirensException(exc)

        return True

    def _start(self, hunt_name: str = None):
        global _active_hunt_stack
        global _active_backend_stack
        if not hunt_name:
            hunt_name = self.hunt_name

        active_hunt_obj = ThreatHunt._get_active_obj(hunt_name, type='hunt')
        active_backend_obj = ThreatHunt._get_active_obj(hunt_name, type='backend')

        if active_hunt_obj:
            ThreatHunt._update_active_hunt(hunt=active_hunt_obj, status=HuntStatus.RUNNING, state=HuntState.ACTIVE,
                                           start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

        else:
            active_hunt_obj = ActiveHunt(run_id=get_uuid(), hunt_name=hunt_name, database=BackendDefaults.DATABASE,
                                         start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                                         status=HuntStatus.RUNNING, state=HuntState.ACTIVE)
            _active_hunt_stack.append({hunt_name: active_hunt_obj})

        ThreatHunt._update_index_table(hunt=active_hunt_obj, backend=active_backend_obj)

        return active_hunt_obj

    @staticmethod
    def _end(hunt_name: str, status: HuntStatus = HuntStatus.FINISHED,
             result: HuntResult = HuntResult.SUCCESS) -> None:
        global _active_hunt_stack
        active_hunt_obj = ThreatHunt._get_active_obj(hunt_name, type='hunt')
        active_backend_obj = ThreatHunt._get_active_obj(hunt_name, type='backend')
        if active_hunt_obj:
            active_hunt_obj = ThreatHunt._update_active_hunt(hunt=active_hunt_obj, state=HuntState.FINISHED,
                                                             status=HuntStatus.FINISHED,
                                                             end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                                                             result=result)

            ThreatHunt._update_index_table(hunt=active_hunt_obj, backend=active_backend_obj)

            # remove the active hunt from the hunt stack
            pop_index = ThreatHunt._get_active_hunt_index(hunt_name)
            if isinstance(pop_index, int):
                _active_hunt_stack.pop(pop_index)
            else:
                logger.error("No valid hunt obj. Something went wrong.")
                raise SirensException("No valid hunt obj. Something went wrong.")

            logger.info(f"Hunt {active_hunt_obj.hunt_name} finished. Run_Id: {active_hunt_obj.run_id}")

        return

    def start(self, hunt_name: str = None) -> ActiveHunt:
        """_summary_

        :param hunt_name: _description_, defaults to None
        :type hunt_name: str, optional
        :return: _description_
        :rtype: ActiveHunt
        """
        return self._start(hunt_name=hunt_name)

    @staticmethod
    def end(hunt_name: str, status: HuntStatus = HuntStatus.FINISHED, result: HuntResult = HuntResult.SUCCESS):
        return ThreatHunt._end(hunt_name=hunt_name, status=status, result=result)

    def capture_output(self, command_name: str = None, description: str = None):
        def wrapper(func):
            def wrapped(*args, **kwargs):
                global _active_hunt_stack
                global _active_backend_stack

                active_hunt_obj = ThreatHunt._get_active_obj(self.hunt_name, type='hunt')
                active_backend_obj = ThreatHunt._get_active_obj(self.hunt_name, type='backend')

                if HuntStatus.RUNNING not in active_hunt_obj.status:
                    logger.info(f"Hunt status is currently in '{active_hunt_obj.status}' status, please ensure it is in 's' \
                           state before attempting to capture any output.")

                    return None

                start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                # execute function
                df = func(*args, **kwargs)
                end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                # add result_count info to captured dataframe
                result_count = df.select(F.col(df.columns[0])).summary("count").collect()[0][1]

                if not BaseUtils.is_spark_dataframe(df):
                    logger.info(f"Result is not a DataFrame: {type(df)} - skipping collection")

                # If not risk capture was defined against the search
                if DefaultColumns.RISK.value not in df.columns:
                    result_df = df.select(F.lit(start_time).alias("start_time"), F.lit(end_time).alias("end_time"),
                                          F.lit(active_hunt_obj.run_id).alias("run_id"),
                                          F.lit(command_name).alias("command_name"),
                                          F.lit(description).alias("description"),
                                          F.create_map(list(chain(
                                              *((F.lit(name).cast("string"), F.col(name).cast("string")) for name in
                                                df.columns)))).alias("result"),
                                          F.lit(result_count).cast("int").alias("result_count"),
                                          F.monotonically_increasing_id().alias("_increasing_seq")
                                          )
                # else there is one, and we need to work around it.
                else:
                    # separate risk column before collating the results (fixes loosing the map on reconstruction)
                    cols = df.columns
                    cols.remove(DefaultColumns.RISK.value)
                    result_df = (
                        df.select(F.col(DefaultColumns.RISK.value).alias(DefaultColumns.RISK.value + '_x'), "*")
                        .drop(DefaultColumns.RISK.value)
                        .select(F.col(DefaultColumns.RISK.value + '_x').alias(DefaultColumns.RISK.value),
                                F.lit(start_time).alias("start_time"), F.lit(end_time).alias("end_time"),
                                F.lit(active_hunt_obj.run_id).alias("run_id"),
                                F.lit(command_name).alias("command_name"),
                                F.lit(description).alias("description"),
                                F.create_map(list(chain(
                                    *((F.lit(name).cast("string"), F.col(name).cast("string")) for name in
                                      cols)))).alias("result"),
                                F.lit(result_count).cast("int").alias("result_count"),
                                F.monotonically_increasing_id().alias("_increasing_seq")))

                window = Window.partitionBy(F.lit(1)).orderBy(F.col('_increasing_seq'))
                result_df = result_df.withColumn('_command_seq', F.row_number().over(window)).drop("_increasing_seq")

                ThreatHunt._flush(result_df, active_backend_obj)

                return df

            return wrapped

        return wrapper
