"""common methods for connectors that rely on API connectivity.

Collecting remote API data happens on a driver node, but spark.read does not support reading from here.
Sirens writes data locally to a temp file then atomically moves the temp file to cloud storage.
Either a batch or streaming dataframe can then be generated from there.

Author:
    Derek King (2023-01-17)

Classes:
    DriverNode:
    Connector():
"""

import json
import os
import shutil
from pathlib import Path
from typing import Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from databricks.sirens.exceptions import SirensConnectorException
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class DriverNode:
    """operations that occur on or from the driver node

    :raises SirensConnectorException: on any exception
    :return: _description_
    :rtype: _type_
    """

    @staticmethod
    def get_temp_files(source: str, sourcetype: str, pipeline_run_id: str) -> Tuple[str, str]:
        """generate temp path names for files on the driver node. Recursively makes the temp_dir directory too

        :param source: source
        :type source: str
        :param sourcetype: sourcetype
        :type sourcetype: str
        :param pipeline_run_id: this pipeline id
        :type pipeline_run_id: str
        :return: temp_dir, temp_file
        :rtype: Tuple[str, str]
        """

        temp_dir = f'/tmp/{source}/{sourcetype}/{pipeline_run_id}'
        temp_file = f'/tmp/{source}/{sourcetype}/{pipeline_run_id}/api_events.json'
        Path(temp_dir).mkdir(parents=True, exist_ok=True)

        return temp_dir, temp_file

    @staticmethod
    def get_dbfs_files(scratch_dir: str, source: str, sourcetype: str, time: str) -> Tuple[str, str]:
        """generate destination path names on dbfs to move temp files to

        :param scratch_dir: scratch_dir
        :type scratch_dir: str
        :param time: current time
        :type time: str
        :return: dbfs_dir, dbfs_file
        :rtype: Tuple[str, str]
        """
        # issue-729. scratch_dir has leading slash - os.path.join gets confused and drops the /dbfs prefix.
        scratch_dir = scratch_dir.strip("/")
        # end_fix

        dbfs_dir = os.path.join('/dbfs', scratch_dir, 'connectors', source, sourcetype, 'runs', time)
        dbfs_file = os.path.join(dbfs_dir, 'api_events.json')
        logger.debug(f"get_dbfs_files: returning dir: {dbfs_dir}, dbfs_file: {dbfs_file}")

        return dbfs_dir, dbfs_file

    @staticmethod
    def move_files_to_dbfs(dbfs_dir: str, src: str, dest: str, pipeline_run_id: str) -> bool:
        """move temp files from driver node to dbfs

        :param dbfs_dir: directory to move to (used to recursively create)
        :type dbfs_dir: str
        :param src: file on driver node to move
        :type src: str
        :param dest: absolute path to move to dbfs (/dbfs/....)
        :type dest: str
        :param pipeline_run_id: current pipeline_id
        :type pipeline_run_id: str
        :raises SirensConnectorException: _description_
        :return: _description_
        :rtype: bool
        """
        try:
            logger.debug(f"creating dbfs dir: {dbfs_dir} if not exists")
            Path(dbfs_dir).mkdir(parents=True, exist_ok=True)
            logger.debug(f"moving: {src} to {dest}")
            shutil.move(src, dest)
        except Exception as exc:
            logger.error(f"pipeline_run_id={pipeline_run_id} message={exc}")
            raise SirensConnectorException({exc}) from exc
        return True

    @staticmethod
    def remove_temp_files(temp_dir: str, pipeline_run_id: str) -> bool:
        """recursively remove driver temp file

        :param temp_dir: temp directory on driver api stored files to
        :type temp_dir: str
        :param pipeline_run_id: current pipeline_id
        :type pipeline_run_id: str
        :return: True
        :rtype: bool
        """
        try:
            logger.debug(f"removing directories: {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as exc:
            logger.warning(f"pipeline_run_id={pipeline_run_id} message=failed to remove driver file: {temp_dir}: {exc}")

        return True


class Connector:
    """shared connector operations
    """

    def __init__(self, spark: SparkSession, data_source_obj):
        self.spark = spark
        self.data_source_obj = data_source_obj
        self.scratch_dir = GlobalConfig.get_global_scratch_dir(GlobalConfig.get())
        self.databaseName = GlobalConfig.get_global_database(GlobalConfig.get())
        self.module = self.data_source_obj.get_connector_name()
        self.source, self.sourcetype = self.data_source_obj.get_source_sourcetype_info()
        self.status_dir, self.file_name = self._get_dbfs_status_dir(self.scratch_dir, self.source, self.sourcetype)
        self.pipeline_run_id = str(self.data_source_obj.pipeline_run_id)

    def create_dataframe_from_json(self, path: str, streaming_mode: str,
                                   schema: Union[StructType, str, None] = None) -> DataFrame:
        """read json file into dataframe

        :param path: path to read
        :type path: str
        :param streaming_mode: streaming|batch
        :type streaming_mode: str
        :param schema: schema string
        :type schema: str or StructType
        :return: streaming or batch DataFrame
        :rtype: DataFrame
        """
        try:
            if streaming_mode == "streaming":
                rdr = self.spark.readStream
            elif streaming_mode == "batch":
                rdr = self.spark.read
            else:
                raise SirensConnectorException(f"unsupported streaming_mode: {streaming_mode}")

            df = rdr.json(path, schema=schema)
        except Exception as exc:
            logger.error({exc})
            raise SirensConnectorException({exc}) from exc

        return df

    @staticmethod
    def _get_dbfs_status_dir(scratch_dir: str, source: str, sourcetype: str) -> Tuple[str, str]:
        """generate destination path names on dbfs for connector status file

        :param scratch_dir: self.scratch_dir
        :type scratch_dir: str
        :param source: source
        :type source: str
        :param sourcetype: sourcetype
        :type sourcetype: str
        :return: dbfs_dir, dbfs_file
        :rtype: Tuple[str, str]
        """
        # issue-729. scratch_dir has leading slash - os.path.join gets confused and drops the /dbfs prefix.
        scratch_dir = scratch_dir.strip("/")
        # end_fix

        dbfs_dir = os.path.join('/dbfs', scratch_dir, 'connectors', source, sourcetype)
        dbfs_file = os.path.join(dbfs_dir, 'status.json')

        return dbfs_dir, dbfs_file

    @staticmethod
    def _is_file(f_name):
        return Path(f_name).is_file()

    @staticmethod
    def _mkdir(directory):
        return Path(directory).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _read_file(file_name):
        try:
            with open(file_name) as f:
                return json.load(f)

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensConnectorException({exc})

    def _write_file(self, content) -> bool:
        try:
            with open(self.file_name, 'w') as f:
                json.dump(content, f)
            return True
        except Exception as exc:
            logger.error({exc})
            raise SirensConnectorException({exc})

    def read(self) -> dict:
        """read the connector status file

        :return: Either empty dict for no status, or file as dict if previously written
        :rtype: dict
        """
        # No status file currently exists.
        if not self._is_file(self.file_name):
            return {}

        return self._read_file(self.file_name)

    def write(self, status_info) -> bool:
        """write the connector status file

        :param status_info: any information to be held. Commonly used for last cursor
        :type status_info: dict
        :return: true|false
        :rtype: bool
        """
        try:
            self._mkdir(self.status_dir)
        except Exception as exc:
            logger.error({exc})
            raise SirensConnectorException(f"write connector status failed: {exc}")

        return self._write_file(status_info)
