from databricks.sirens.utils.path_utils import PathUtils
from databricks.sirens import _version
from databricks.sirens.logging import get_logger
from pathlib import Path
from typing import Union
import configparser
from databricks.sirens.exceptions import SirensGlobalConfigException

logger = get_logger(__name__)
ENGINE_VERSION = _version.__version__


class ConfigReader:
    """Class to read the sirens.config file

    :raises Exception: _description_
    :return: _description_
    :rtype: _type_
    """

    @staticmethod
    def _read_config(file_names):
        for file in file_names:
            try:
                config_file = Path(file)

                if config_file.is_file():
                    logger.debug(f"found: {config_file}")
                    config = configparser.ConfigParser()
                    config.read(config_file)
                    return (config)

            except Exception as exc:
                raise SirensGlobalConfigException(exc) from exc
        return None

    @staticmethod
    def _read_global():
        """attempt read on global sirens.config
        """
        file_names = PathUtils.get_relative_file_paths(["sirens.config"])
        config = ConfigReader._read_config(file_names)
        if config:
            return config

        logger.info("Config file sirens.config not found, trying sirens.config.default")

        file_names = PathUtils.get_relative_file_paths(["sirens.config.default"])
        config = ConfigReader._read_config(file_names)
        if config:
            return config

        raise SirensGlobalConfigException('Global sirens.config not found.')

    @staticmethod
    def read():
      return ConfigReader._read_global()
    
    @staticmethod
    def get_config_or_default(section: str, key: str, defaults: dict = {}) -> str:
        config = ConfigReader.read()
        returned_val = ConfigReader._get_config_key(config, section, key)
        return returned_val or defaults.get(key)

    @staticmethod
    def _get_config_key(config, section: str, key: str):
        """given a stanza name, get the given key/value from sirens.config

        :param config: config object
        :type config: config object
        :param section: stanza name
        :type section: str
        :param key: key to get
        :type key: str
        :return: key value
        :rtype: str
        """
        try:

            if not config.has_section(section):
                logger.warning(f"sirens.config does not have section: {section}")
                return None
            if not config.has_option(section, key):
                logger.warning(f"sirens.config does not have key: {key}")
                return None
            else:
                return config.get(section, key)
        except Exception as exc:
            logger.warning(f'failed to key global config stanza: {section}, key: {key} - defaulting. {exc}')
            return None
    
    @staticmethod
    def get_config_key(config, section: str, key: str) -> Union[str, None]:
        return ConfigReader._get_config_key(config, section, key)
