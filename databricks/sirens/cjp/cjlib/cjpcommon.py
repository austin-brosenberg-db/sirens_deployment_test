from collections import OrderedDict
import json
import logging
import re
import os

from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjInvalidParameterException,
    CjException,
)

"""
This file contains a few routines needed commonly by both the cj_poller.py execution and test scripts.
"""

_config_dir = "./configs"
_poller_names = None


def create_poller_from_files(
    poller_name: str = None,
    bucket: str = None,
    start_timestamp: str = None,
    end_timestamp: str = None,
) -> object:
    """
    Load config and (optionally) schema data from files and call poller create

    Args:
        poller_name: name of the poller (used to reference config and schema files)
        bucket: (optional) location to which data should be stored, e.g. S3 bucket name; None implies stdout
        start_timestamp: (optional) start of time window (UTC) from which data should be retrieved
        end_timestamp: (optional) end of time window (UTC) from which data should be retrieved
    Returns:
        poller object returned from invocation of create_poller()
    Raises:
        CjException if poller config file is missing or schema files is required and missing
    """

    if not poller_name:
        raise CjInvalidParameterException(
            parameter="poller_name", msg=f"was {poller_name}"
        )

    # Read poller configuration from local file based on poller name
    config_file = f"{_config_dir}/cjp_{poller_name}_config.json"
    try:
        f = open(config_file)
        config = json.load(f)
    except Exception as e:
        msg = f"An error occurred reading {config_file} for the '{poller_name}' poller"
        raise CjException(poller_name, msg, e)

    config = config if config else {}
    config["transform"] = config.get("transform", "keep")

    schema = None
    schema_file = f"{_config_dir}/cjp_{poller_name}_schema.json"
    try:
        with open(schema_file, "r") as f:
            schema = json.load(f, object_pairs_hook=OrderedDict)
    except FileNotFoundError:
        if config.get("cursor"):
            raise CjException(
                f"a cursor was requested via config, but {schema_file} was missing"
            )
        elif config.get("transform") in ["drop", "extra"]:
            raise CjException(
                f"transform specified ({config['transform']}, but {schema_file} was missing"
            )
        else:
            # A config file is not required for "keep" transform mode without cursor support
            pass

    return create_poller(
        poller_name=poller_name,
        config=config,
        schema=schema,
        bucket=bucket,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )


def create_poller(
    poller_name: str = None,
    config: dict = {},
    schema: dict = None,
    bucket: str = None,
    start_timestamp: str = None,
    end_timestamp: str = None,
):
    """
    Create a poller object of the appropriate subclass

    Args:
        poller_name: name of the poller (used to reference config and schema files)
        config: dictionary of configuration options
        schema: (optional) dictionary of transformations for input->output data mapping
        bucket: (optional) location to which data should be stored, e.g. S3 bucket name; None implies stdout
        start_timestamp: (str or None) start of time window (UTC) from which data should be retrieved
        end_timestamp: (str or None) end of time window (UTC) from which data should be retrieved
    Returns:
        poller object for the subclass appropriate for the poller_type specified in config
    Raises:
        CjInvalidParameterException: if valid poller_type is absent from config
    """

    profile = os.environ["CJP_PROFILE"] if "CJP_PROFILE" in os.environ else None
    logging.debug(f"Using '{profile}' profile for authentication")

    config["transform"] = config.get("transform", "keep")

    poller_params = {
        "config": config,
        "name": poller_name,
        "profile": profile,
        "schema": schema,
        "bucket": bucket,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }

    for k in ["poller_type"]:
        if k not in config or not config[k]:
            raise CjInvalidParameterException(
                poller_name,
                None,
                k,
                f"['{k}'] is required in config, but was not defined",
            )

    poller_type = config["poller_type"]
    if poller_type == "workday":
        from databricks.sirens.cjp.cjlib.pollers.RestApiPoller import RestApiPoller

        return RestApiPoller(**poller_params)
    elif poller_type == "okta":
        from databricks.sirens.cjp.cjlib.pollers.OktaPoller import OktaPoller

        return OktaPoller(**poller_params)
    else:
        raise CjInvalidParameterException(
            poller_name,
            None,
            "poller_type",
            f"poller type {poller_type} is not supported, please add required configurations for the poller: {poller_type}",
        )


def get_poller_names() -> str:
    """
    Return a list of names of pollers for which config files are present (excluding those beginning with 'cjp_test')

    Returns:
         list of strings containing poller names (e.g. ['workday_login'])
    """
    global _poller_names
    if _poller_names is None:
        poller_name_re = re.compile("cjp_(.*)_config.json")
        poller_names = []
        try:
            for file_name in os.listdir(_config_dir):
                m = poller_name_re.match(file_name)
                if m:
                    if m.group(1).find("test") != 0:
                        poller_names.append(m.group(1))
            _poller_names = sorted(poller_names)
        except FileNotFoundError:
            _poller_names = {}
    return _poller_names
