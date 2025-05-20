import configparser
import os
from pathlib import Path
from typing import List, Optional, Any, Dict

from databricks.sirens.exceptions import SirensGlobalConfigException
from databricks.sirens.logging import get_logger, colours
from databricks.sirens.utils.path_utils import PathUtils

logger = get_logger(__name__)


class GlobalConfig(object):
    """Class to read the sirens.config file

    :raises Exception: _description_
    :return: _description_
    :rtype: _type_
    """

    __config__: configparser.ConfigParser = None

    @staticmethod
    def _read_config(file_names: List[str]) -> Optional[configparser.ConfigParser]:
        for file in file_names:
            try:
                config_file = Path(file)
                if config_file.is_file():
                    logger.debug(f"found: {config_file}")
                    config = configparser.ConfigParser()
                    read_files = config.read(config_file)
                    if read_files:  # only return config if we read a file
                        return config

            except Exception as exc:
                raise SirensGlobalConfigException(exc) from exc
        return None

    @staticmethod
    def _read_global() -> configparser.ConfigParser:
        """attempt read on global sirens.config
        """
        if "SIRENS_CONFIG" in os.environ:
            file_names = [os.path.join(os.environ["SIRENS_CONFIG"], "sirens.config")]
        else:
            file_names = PathUtils.get_relative_file_paths(["sirens.config"])
        config = GlobalConfig._read_config(file_names)
        if config:
            return config

        logger.info("Config file sirens.config not found, trying sirens.config.default")

        if "SIRENS_CONFIG" in os.environ:
            file_names = [os.path.join(os.environ["SIRENS_CONFIG"], "sirens.config.default")]
        else:
            file_names = PathUtils.get_relative_file_paths(["sirens.config.default"])
        config = GlobalConfig._read_config(file_names)
        if config:
            return config

        raise SirensGlobalConfigException('Global sirens.config not found.')

    @classmethod
    def read(cls, ) -> configparser.ConfigParser:  # TODO: remove it after switched to get everywhere
        if cls.__config__ is None:
            cls.__config__ = GlobalConfig._read_global()
        return cls.__config__

    @classmethod
    def get(cls) -> configparser.ConfigParser:
        if cls.__config__ is None:
            cls.__config__ = GlobalConfig._read_global()
        return cls.__config__

    @staticmethod
    def _get_coordinate_or_default(global_config: dict, coordinates: List[str], default: Any) -> Any:
        try:
            d = global_config
            for coord in coordinates:
                d = d[coord]
            return d
        except KeyError:
            coord_str = " -> ".join(coordinates)
            warn_str = f"{coord_str} not set -> defaulting to {default}"
            print(f"{colours.WARN}WARNING: {warn_str}{colours.ENDC}")
            logger.warning(warn_str)
            return default

    @staticmethod
    def get_intel_collection_nb_dir(globalConfig: dict) -> str:
        """get the default notebook directory for threat intelligence collection notebooks

        :param globalConfig: config object
        :type globalConfig: dict
        :return: default notebook directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(globalConfig, ['global', 'threat_intel_coll_notebooks_dir'],
                                                     "notebooks/threat_intelligence/collectors")

    @staticmethod
    def get_intel_ingest_nb_dir(globalConfig: dict) -> str:
        """get the default notebook directory for threat intelligence ingest notebooks

        :param globalConfig: config object
        :type globalConfig: dict
        :return: default notebook directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(globalConfig, ['global', 'threat_intel_ingest_notebooks_dir'],
                                                     "notebooks/threat_intelligence/ingest")

    @staticmethod
    def get_intel_normalize_nb_dir(globalConfig: dict) -> str:
        """get the default notebook directory for threat intelligence ingest notebooks

        :param globalConfig: config object
        :type globalConfig: dict
        :return: default notebook directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(globalConfig, ['global', 'threat_intel_ingest_notebooks_dir'],
                                                       "notebooks/threat_intelligence/normalize")

    @staticmethod
    def get_template_dir(global_config: dict) -> str:
        """get default template directory from global config
        :param global_config: config object
        :type global_config: dict
        :return: configured directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['global', 'template_dir'], "templates")

    @staticmethod
    def get_detection_deploy_dir(global_config: dict) -> str:
        """get detection deployment directory from global config
        :param global_config: config object
        :type global_config: dict
        :return: configured directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['detections', 'deploy_dir'], "deploy/detections")

    @staticmethod
    def get_deploy_dir(global_config: dict) -> str:
        """get deploy directory from global config
        :param global_config: config object
        :type global_config: dict
        :return: configured directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['deploy', 'deploy_dir'], "deploy")

    @staticmethod
    def get_groupby(global_config: dict) -> str:
        """get groupby key from global config
        :param global_config: config object
        :type global_config: dict
        :return: configured directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['deploy', 'groupby'], "source")

    @staticmethod
    def get_notebook_language(global_config: dict) -> str:
        """get notebook_language key from global config
        :param global_config: config object
        :type global_config: dict
        :return: configured language
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'notebook_language'], "python")

    @staticmethod
    def get_target_database(global_config: dict) -> str:
        """get target_database key from global config
        :param global_config: config object
        :type global_config: dict
        :return: target_databse key
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'target_database'], "sirens")

    @staticmethod
    def get_global_scratch_dir(global_config: dict) -> str:
        """get scratch directory key from global config
        :param global_config: config object
        :type global_config: dict
        :return: target_databse key
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'scratch_dir'], "/FileStore/sirens")

    @staticmethod
    def get_scratch_dir(global_config: dict) -> str:
        """return default -> notebook_type key
        :param global_config: global config dict
        :type global_config: dict
        :return: key value
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'scratch_dir'], None)
    
    @staticmethod
    def get_dbfs_upload_dir(global_config: dict) -> str:
        """get target directory for notebook deployment from global config
        :param global_config: config object
        :type global_config: dict
        :return: notebook_dir
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['deploy', 'dbfs_upload_dir'], "/FileStore/sirens")

    @staticmethod
    def get_dashboards_dir(global_config: dict) -> str:
        """get dashboard directory for deployment from global config
        :param global_config: config object
        :type global_config: dict
        :return: dashboard_dir
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['deploy', 'dashboard_dir'], "sirens_dashboards")

    @staticmethod
    def get_warehouse_id(global_config: dict) -> str:
        """get sql warehouse_id from global config
        :param global_config: config object
        :type global_config: dict
        :return: warehouse_id
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['deploy', 'sql_warehouse_id'], None)

    @staticmethod
    def get_detection_default_dir(global_config: dict) -> str:
        """get the default directory for detection yaml files.
        :param global_config: config object
        :type global_config: dict
        :return: default detection yaml directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['global', 'detection_dir'], "detections")

    @staticmethod
    def get_configs_default_dir(global_config: dict) -> str:
        """get the default directory for configuration files.
        :param global_config: config object
        :type global_config: dict
        :return: default configs directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['global', 'conf_dir'], "conf")

    @staticmethod
    def get_hunt_notebooks_dir(global_config: dict) -> str:
        """get the default directory for threat hunting notebook files.
        :param global_config: config object
        :type global_config: dict
        :return: default configs directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['global', 'hunting_notebooks_dir'], "notebooks/threat_hunting")

    @staticmethod
    def get_playbooks_dir(global_config: dict) -> str:
        """get the default directory for response playbook files.
        :param global_config: config object
        :type global_config: dict
        :return: default configs directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['global', 'response_notebooks_dir'], "notebooks/playbooks")

    @staticmethod
    def get_input_config_dir(global_config: dict) -> str:
        """get the default directory for input configuration files.
        :param global_config: config object
        :type global_config: dict
        :return: default configs directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['global', 'input_config_dir'], "log_sources")

    @staticmethod
    def get_global_database(global_config: dict) -> str:
        """return the default database
        :return: database name
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'target_database'], 'sirens')
    
    @staticmethod
    def get_global_schemas(global_config: dict) -> Dict:
        """return the default databases for bronze, silver and normalized layers
        :return: Dictionary of bronze, silver, and normalized database names
        :rtype: Dict
        """
        bronze_db = GlobalConfig._get_coordinate_or_default(global_config, ['schemas', 'bronze'], '')
        silver_db = GlobalConfig._get_coordinate_or_default(global_config, ['schemas', 'silver'], '')
        normalized_db = GlobalConfig._get_coordinate_or_default(global_config, ['schemas', 'normalized'], '')
        return {
            "bronze": bronze_db,
            "silver": silver_db,
            "normalized": normalized_db
        }


    @staticmethod
    def _get_global_scratch_dir(global_config: dict) -> str:
        """return the temporary scratch directory
        :return: scratch space directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'scratch_dir'], 'FileStore/sirens')

    @staticmethod
    def get_global_sirens_lib(global_config: dict) -> str:
        """return the temporary scratch directory
        :return: scratch space directory
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'sirens_lib'], 'latest')

    @staticmethod
    def get_notebook_type(global_config: dict) -> str:
        """return default -> notebook_type key

        :param global_config: global config dict
        :type global_config: dict
        :return: key value
        :rtype: str
        """
        return GlobalConfig._get_coordinate_or_default(global_config, ['default', 'notebook_type'], None)

    @staticmethod
    def get_top_level_directory():
        # handles the case where the code is checked out into databricks-sirens/databricks-sirens
        # as is done in github actions for some reason
        baseDir = str(Path('..').parent.absolute())
        return baseDir[:baseDir.rindex('databricks-sirens')] + 'databricks-sirens'



