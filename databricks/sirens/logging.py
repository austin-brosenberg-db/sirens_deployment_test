"""Implements the system wide logging for Sirens.

Author:
    Derek King (July 2022)

Classes:
    colours()

Functions:
    get_logger()

"""

import os
import logging
import logging.handlers as handlers
import sys
import configparser
from pathlib import Path
from databricks.sirens.utils.path_utils import PathUtils


class colours:
    INFO = '\033[92m'
    WARN = '\033[93m'
    ERROR = '\033[91m'
    CONF = '\033[95m'
    ENDC = '\033[0m'


logLevels = {
    'INFO': logging.INFO,
    'WARN': logging.WARN,
    'ERROR': logging.ERROR,
    'DEBUG': logging.DEBUG,
    'CRITICAL': logging.CRITICAL
}


def _read_log_level_file(file_names):
    for file in file_names:
        try:
            config_file = Path(file)
            if config_file.is_file():
                config = configparser.ConfigParser()
                config.read(config_file)
                return config['global']['logging_level']

        except Exception as exc:
            raise Exception(exc) from exc
    return None


def _read_log_level():
    """attempt read on global sirens.config
    """
    file_names = PathUtils.get_relative_file_paths(["sirens.config"])
    log_level = _read_log_level_file(file_names)
    if log_level: return log_level

    file_names = PathUtils.get_relative_file_paths(["sirens.config.default"])
    log_level = _read_log_level_file(file_names)
    if log_level: return log_level

    raise Exception('Global sirens.config not found.')


def get_logger(mod_name):
    logger = logging.getLogger(mod_name)

    # LOG_DIR = "/tmp/sirens_logs"
    # if not os.path.exists(LOG_DIR):
    #     try:
    #         os.makedirs(LOG_DIR)
    #     except FileExistsError:
    #         pass

    # log_file = LOG_DIR + "/sirens_application.log"

    # # fh = logging.FileHandler(log_file, mode='a')
    # fh = handlers.TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=10)
    # fh.suffix = "%Y%m%d"

    log_level_str = _read_log_level()

    log_level = logLevels.get(log_level_str.upper())
    logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - function: %(funcName)s - lineNo: %(lineno)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s: %(name)s: %(levelname)s: %(message)s')
    # fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    if logger.hasHandlers():
        logger.handlers.clear()

    # logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
