import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Optional, Tuple, Union

import yaml
from pyspark.sql.types import StructType
from pyspark.sql import Column
from yaml._yaml import ConstructorError

from databricks.sirens import _version
from databricks.sirens.exceptions import SirensConfigException
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.internal.secrets import SecretsManager
from databricks.sirens.logging import get_logger
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.utils.path_utils import PathUtils

logger = get_logger(__name__)
ENGINE_VERSION = _version.__version__


@dataclass
class TimeStampInfo:
    timestamp_column: Union[str, Column]
    timestamp_col_type: str = "string"
    timestamp_format: Optional[str] = None
    timestamp_regex: Optional[str] = None
    timestamp_regex_group: Optional[int] = None


class DataSource:
    """Class for reading config related to a input source.
    """

    def __init__(self, spark, database: str, source: str, sourcetype: str, config_file_name: Optional[str] = None,
                 enrichments: Optional[dict] = None, target_database: str = None, db_layer: str = None):
        """Read the datasource config files.

        :param spark: _description_
        :type spark: _type_
        :param database: _description_
        :type database: str
        :param source: _description_
        :type source: str
        :param sourcetype: _description_
        :type sourcetype: str
        """
        self.spark = spark
        self.source = source
        self.sourcetype = sourcetype
        self.config_file = None
        self._database_name = database
        self._target_datbase = target_database
        self._db_layer = db_layer
        self.DEFAULT_KEYS = {"databaseName": "sirens"}
        self.bronze_table_name = self.get_table_name(self.source, self.sourcetype, suffix='bronze')
        self.silver_table_name = self.get_table_name(self.source, self.sourcetype, suffix='silver')
        self.gold_table_name = self.get_table_name(self.source, self.sourcetype, suffix='gold')
        self.pipeline_run_id = BaseUtils.get_uuid_str()
        self.global_config = GlobalConfig.get()
        self.input_config_dir = GlobalConfig.get_input_config_dir(self.global_config)
        self.data_source_config: dict = {}
        self.config_file_name = config_file_name
        self.enrichments = enrichments
        PathUtils.custom_dirs = [self.input_config_dir, GlobalConfig.get_configs_default_dir(self.global_config)]
        try:
            self.workspace_name = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception:
            self.workspace_name = "unknown"

        # Keys that can be supported in the config file. Used to validate the user inputs into the yaml file.
        # If a key is not supported it will be ignored.
        # If it is supported, it will be validated for proper input.
        self.SUPPORTED_KEYS = {"config", "input", "transforms", "schemas"}.union(self.DEFAULT_KEYS.keys())
        logger.info(
            f"pipeline_run_id={self.pipeline_run_id} engine_version={ENGINE_VERSION} message=Starting run of {self.source} {self.sourcetype}.")

    def __str__(self):
        return self.source

    @staticmethod
    def get_table_name(source: str, source_type: str, prefix: str = None, suffix: str = None, sep: str = "_") -> str:
        table_name = ""
        if prefix is not None:
            table_name = prefix + sep
        table_name += source + sep + source_type
        if suffix is not None:
            table_name += sep + suffix

        return table_name

    @property
    def databaseName(self):
        return self._database_name
    
    @property
    def targetDatabase(self):
        if self._target_datbase:
            return self._target_datbase 
        elif self._db_layer :
            tgt_db = self.data_source_config.get("schemas", {}).get(self._db_layer)
            if tgt_db: return tgt_db
            g_schemas = GlobalConfig.get_global_schemas(self.global_config)
            g_tgt_db = g_schemas.get(self._db_layer, '')
            if g_tgt_db: return g_tgt_db
        return self._database_name


    def read(self) -> dict:
        """Reads the specific data source inputs.yaml file for configuration and loads into the dataSource Object (dataSourceObj)

        :raises Exception: For uncaught exceptions
        :raises Exception: FileNotFound for a missing/inacessible config file
        :return: Key/Value pairs of the configuration file
        :rtype: Dict
        """

        # Setup Default Keys - Over-write from inputs.yaml if it exists.
        default = self.DEFAULT_KEYS
        if self.config_file_name:
            inputs_yaml = self.config_file_name
        else:
            inputs_yaml = f"{self.input_config_dir}/{self.source}/{self.sourcetype}/inputs.yaml"
        file_names = PathUtils.get_relative_file_paths([inputs_yaml])

        data = {}
        for file in file_names:
            try:
                with open(file) as f:
                    logger.debug(f"reading config: {file}")
                    try:
                        for data in yaml.load_all(f, Loader=yaml.SafeLoader):
                            # Override any defaults.
                            default.update(data)
                            logger.debug(f"{data}")
                    except ConstructorError as exc:
                        logger.error("yaml error: either the file os badly formatted, or maybe you configured a {{secret}} without enclosing in quotes?")
                    except yaml.YAMLError as exc:
                        raise SirensConfigException(exc) from exc
            except FileNotFoundError:
                pass
        if not data:
            logger.error("unable to locate config file")
            raise SirensConfigException(
                f"Unable to locate config file: {self.source}/{self.sourcetype}/inputs.yaml")

        # Sanitize the input - ensure we only work with supported key/value pairs
        config_items = {k: default[k] for k in self.SUPPORTED_KEYS if k in default}

        # substitute any <dbsecret:scope:key> or {{secrets/scope/key}} values in the inputs.yaml
        config_items = SecretsManager().substitute_secrets(config_items)
        self.data_source_config = config_items
        return config_items

    def print_schema(self) -> Optional[str]:
        """print the contents of the rawSchemaFile defined in inputs.yaml

        :return: json string of schema
        :rtype: str
        """
        schema_name = self.get_schema_file_opt()
        if schema_name:
            return self.read_schema_file(schema_name)

        return None

    def get_source_sourcetype_info(self) -> Tuple[str, str]:
        """get source and source type, including overrides.
        :return: values for source and source type in inputs.yaml
        :rtype: Tuple[str, str]
        """

        source = self.data_source_config.get("input").get("source")
        source_type = self.data_source_config.get("input").get("sourcetype")
        # Overwrite if specified otherwise default to input keys.
        source = self.data_source_config.get("transforms").get("bronze").get("source", source)
        source_type = self.data_source_config.get("transforms").get("bronze").get(
            "sourcetype", source_type)

        return source, source_type

    def get_table_config(self, table_name: str) -> dict:
        if table_name == self.bronze_table_name:
            transform_type = "bronze"
        elif table_name == self.silver_table_name:
            transform_type = "silver"
        else:
            # assume it's an event table at this point
            table_config = {}

            if self.global_config.has_section(f"event_table:{table_name}"):
                for key, value in self.global_config.items(f"event_table:{table_name}"):
                    table_config[key] = value

            return table_config

        return self.data_source_config.get("transforms").get(transform_type).get("table_config", {})

    def get_connector_name(self) -> str:
        """read the input.yaml key input connector name, return the name of the format to be used
        :return: connector_name
        :rtype: str
        """
        return self.data_source_config.get("input").get("connector").get("name")

    def get_connector_path(self) -> str:
        """read the input.yaml key rawPath, return the location of the data to be read
        :return: location of the data to be read
        :rtype: str
        """
        return self.data_source_config.get("input").get("rawPath")

    def get_connector_opts(self) -> dict:
        """Discover any options configured for the connector type
        :return: dict of options, to be passed to spark read options
        :rtype: dict
        """
        return self.data_source_config.get("input").get("connector").get("options", {})

    def get_event_filter(self, target_table: str) -> Optional[str]:
        """get the sql filter declared for this event_type

        :return: sql expression to filter table by
        :rtype: str
        """
        try:
            evts = self.data_source_config.get("transforms").get("silver").get("event_type")
            for target in evts:
                if target_table == target.get('target_table'):
                    return target.get('filter')  # / maybe None (no filter defined))
            return None
        except Exception as exc:
            raise SirensConfigException(f"{exc}") from exc

    def get_event_fields(self, target_table: str) -> Optional[dict]:
        """get the fields and transformations declared for this event type

        :return: dict of fields and related transformations
        :rtype: Dict
        """
        try:
            evts = self.data_source_config.get("transforms").get("silver").get("event_type")
            for target in evts:
                if target_table == target.get('target_table'):
                    return target.get('fields')
            return None
        except Exception as exc:
            raise SirensConfigException(f"{exc}") from exc
        
    def get_framework(self, target_table: str) -> str:
        try:
            evts = self.data_source_config.get("transforms").get("silver").get("event_type")
            for target in evts:
                if target_table == target.get('target_table'):
                    return target.get('framework', 'cim')
            return 'cim'
        except Exception as exc:
            raise SirensConfigException(f"{exc}") from exc
            

    def get_schema_file_opt(self) -> str:
        """Read the name of the schema file defined for the sourcetype
        :return: name of the file with schema
        :rtype: str
        """
        return self.data_source_config.get("input").get("rawSchemaFile")

    def get_schema_hints_opt(self) -> str:
        """read the name of the schema hints file for the sourcetype
        :return: name of schema_hints
        :rtype: str
        """
        return self.data_source_config.get("input").get("rawSchemaHintsFile")

    def get_stream_mode(self) -> str:
        """read the streamMode for datasource
        :return: batch|streaming
        :rtype: str
        """
        return self.data_source_config.get("input").get("streamType")

    def get_timestamp_info(self) -> TimeStampInfo:
        """get the bronze metadata timestamp info from dataSource objects dict.

        :raises Exception: when timestamp_column is not defined.
        :return: values for timestamp_format, timestamp_column, timestamp_regex
        :rtype: Tuple[str, str, str]
        """
        timestamp_column = self.data_source_config.get("transforms").get("bronze").get(
            "meta").get("timestamp_column")
        if timestamp_column is None:
            raise SirensConfigException(
                "timestamp_column key not defined in transforms->bronze-meta - Please correct.")
        timestamp_format = self.data_source_config.get("transforms").get("bronze").get(
            "meta").get("timestamp_format")
        timestamp_regex = self.data_source_config.get("transforms").get("bronze").get(
            "meta").get("timestamp_regex")
        timestamp_column_type = self.data_source_config.get("transforms").get("bronze").get(
            "meta").get("timestamp_column_type", "string")
        timestamp_regex_group = None
        if timestamp_regex:
            timestamp_regex_group = self.data_source_config.get("transforms").get("bronze").get(
                "meta").get("timestamp_regex_group")
            if timestamp_regex_group is None:
                raise SirensConfigException(
                    "timestamp_regex_group key not defined in transforms->bronze-meta - Please correct.")

        return TimeStampInfo(timestamp_column=timestamp_column,
                             timestamp_col_type=timestamp_column_type,
                             timestamp_format=timestamp_format,
                             timestamp_regex=timestamp_regex,
                             timestamp_regex_group=timestamp_regex_group)

    # TODO: remove it - it's not a property of the data source, but more of a global config/environment classes
    def get_default_workspace(self):
        """return current workspace name from datasource Object
        """
        return self.workspace_name or "unknown"

    def get_input_host(self) -> Optional[str]:
        """return host if defined in config key statically
        """
        return self.data_source_config.get("input").get("host")

    def get_raw_path_segment(self) -> Optional[str]:
        """return host from rawPath string using the host_rawpath_segment key
        """
        host_segment = self.data_source_config.get("input").get("host_rawpath_segment")
        if host_segment is not None:
            raw_path = self.data_source_config.get("input").get("rawPath")
            try:
                return re.split(r"[\\|/]", raw_path)[host_segment]
            except IndexError:
                logger.warning(f"failed to extract segment {host_segment} from rawpath: {raw_path}")
                pass

        return None

    def get_regex_path(self) -> Optional[Tuple[str, str, int]]:
        """get the host_column and host_regex keys if they exist in config
        :return: keys or None
        :rtype: string,None
        """
        host_col = self.data_source_config.get("transforms").get("bronze").get("meta").get("host_column")
        if host_col:
            host_regex = self.data_source_config.get("transforms").get("bronze").get("meta").get("host_regex")
            host_regex_group = self.data_source_config.get("transforms").get("bronze").get("meta").get("host_regex_group", 1)
            return host_col, host_regex, host_regex_group

        return None

    # Extract default host
    def get_default_host(self):
        """get the default host to be assigned to metadata

        :return: default dvc_hostname
        :rtype: string
        """
        return (self.get_input_host() or self.get_raw_path_segment() or
                self.get_regex_path() or self.get_default_workspace())

    def make_schema_file_path(self, file: str) -> str:
        """create the directory/file path of the schema file to be used

        :param file: name of the schema file located in the sourcetype  directory
        :type file: str
        :return: absolute file path
        :rtype: str
        """
        if self.config_file is None:
            file_name = f"log_sources/{self.source}/{self.sourcetype}/{file}"
            file_list = PathUtils.get_relative_file_paths([file_name])
            for filename in file_list:
                if os.path.isfile(filename):
                    break
        else:
            dir = os.path.dirname(self.config_file)
            filename = f"{dir}/{file}"

        return filename

    def read_schema_file(self, file: str) -> Optional[StructType]:
        """Read the schema file configured for this sourcetype
        :param file: file to read
        :type file: str
        :return: pyspark sql structtype string
        :rtype: StructType
        """
        file_path = self.make_schema_file_path(file)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'r') as f:
                    schema_json = f.read()

                return StructType.fromJson(json.loads(schema_json))

            except Exception as exc:
                logging.error(f"failed to read schema file: {file_path} - trying with no schema: {exc}")
                return None
        else:
            logging.error(f"failed to read schema file: {file_path} - trying with no schema - path doesnt exist")
            return None
