import datetime

import boto3
from copy import deepcopy
from datetime import datetime, timedelta
import json
import logging
from databricks.sirens.cjp.cjlib.botohelper import boto_endpoints
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
    CjException,
)

"""
CrownJewelPollerConfig provides read (and selective write) access to values affecting poller execution
"""

# Definitions for reference by class

_required_config_keys = [
    "credentials",  # Information on credentials to be used for data retrieval
    "poller_type",  # Name of CrownJewelPoller subclass to be used for data source
    "url",  # Url of data source (to be used by CrownJewelPoller.get_data())
]

_cloud_config_keys = [
    "authentication",  # Authentication type and credentials format information
    "output",  # Information on where to write data
]

_stateful_config_keys = [
    "cursor",  # storage location of cursor (AWS) or value of cursor (library)
]

_test_support_keys = [
    "test_interval",  # Time offset (in minutes) to which cursor should be set (in dev) for testing purposes
    "timestamp_format",  # Format string passed to datetime.datetime.strftime() to store test cursor
]

_optional_config_keys = [
    "actions",  # list of actions/events/streams for which data is sought
    "api_response",  # information regarding unpacking data from responses
    "custom_config",  # optional poller-specific parameters
    "environment",  # running as a "library" or within "aws" currently supported
    "record_type",  # type/format of record
    "transform",  # strategy for columns not specified in schema
]


class CrownJewelPollerConfig:
    """
    Class to represent configuration of poller
    """

    _actions = None
    _api_response = None
    _authentication = None
    _credentials = None
    _cursor_param = None
    _custom_config = None
    _env = None
    _output_prefix = None
    _profile = None
    _record_type = None
    _region = None
    _storage_location = None
    _transform_mode = None
    _type = None
    _url = None

    def __init__(
        self: object,
        config: dict,
        storage_location: str = None,
        profile: str = None,
        region: str = "us-west-2",
    ) -> None:
        """
        initialize CrownJewelPollerConfig object

        Args:
            self: CrownJewelPollerConfig object
            config: configuration parameters for this poller
            storage_location: storage_location to which data should be written, or None for stdout
            profile: AWS profile to be used
            region: AWS region in which API endpoints should be accessed
        Raises:
            CjInvalidParameterException if configuration is invalid
        """
        self._profile = profile
        self._region = region

        if not config or type(config) is not dict:
            raise CjInvalidParameterException(parameter="config")
        self._store_config(config, storage_location)

    def _get_creds_from_secret_store(self: object, cred_info: dict) -> dict:
        """
        Retrieve credentials needed to access data source

         Args:
            self: CrownJewelPollerConfig object
            cred_info: parameters for accessing credential storage
        Returns:
            credentials for accessing remote data source
        Raises:
            CjInvalidParameterException if credential location is unspecified
        """
        if cred_info is None or type(cred_info) is not dict:
            raise CjInvalidParameterException(parameter="cred_info")
        for key in ["location"]:
            if key not in cred_info or not cred_info[key]:
                raise CjInvalidParameterException(
                    parameter="cred_info",
                    msg=f"cred_info['{key}'] is required, but not present",
                )

        if self._profile:
            session = boto3.session.Session(profile_name=self._profile)
        else:
            session = boto3.session.Session()

        region = self.get_region()
        client = session.client(
            service_name="ssm",
            region_name=region,
            endpoint_url=boto_endpoints["ssm"][region],
        )
        logging.debug(
            f"Attempting to retrieve credentials from AWS Parameter Store, '{cred_info['location']}' in {region}"
        )
        parameter = client.get_parameter(
            Name=cred_info["location"], WithDecryption=True
        )
        login_creds = parameter["Parameter"]["Value"]
        return json.loads(login_creds)

    def _store_config(self: object, data: dict, storage_location: str = None) -> None:
        """
        store configuration information within object

        Args:
            self: CrownJewelPollerConfig object
            data: configuration information for poller
        Raises:
            CjException if data is missing required fields
            CjMissingParameterException if invalid storage location is specified
        """
        supported_keys = deepcopy(_required_config_keys)
        for k in _required_config_keys:
            if k not in data:
                raise CjException(msg=f"'{k}' parameter was missing from config json")

        self._type = data["poller_type"]
        self._env = data.get("environment", "aws").lower()
        self._url = data["url"]
        logging.debug(f"running poller in '{self._env}' environment mode")

        if "cursor" in data:
            if self._env == "library" and isinstance(data["cursor"], str):
                self._cursor_param = data["cursor"]
            elif isinstance(data["cursor"], dict) and "location" in data["cursor"]:
                self._cursor_param = data["cursor"]["location"]
            else:
                raise CjInvalidParameterException(
                    parameter="cursor",
                    msg=f"{type(data['cursor'])} was neither str or valid dict",
                )
        cursor_info = (
            f"poller with cursor {self.get_cursor()}"
            if self._cursor_param
            else "stateless poller"
        )

        logging.debug(
            f"initializing config for {self._type} {cursor_info} accessing {self._url}"
        )

        if self.get_environment() != "library":
            supported_keys += _cloud_config_keys
            for k in _cloud_config_keys:
                if k not in data:
                    raise CjException(
                        msg=f"'{k}' parameter was missing from config json"
                    )
            if storage_location is None or not storage_location:
                raise CjMissingParameterException(parameter="storage_location")
            self._storage_location = storage_location
            self._output_prefix = data["output"]["prefix"]

        if self._cursor_param is not None:
            supported_keys += _stateful_config_keys
            for k in _stateful_config_keys:
                if k not in data:
                    raise CjException(
                        msg=f"'{k}' parameter was missing from config json"
                    )

            if self.get_environment() != "library":
                supported_keys += _test_support_keys
                for k in _test_support_keys:
                    if k not in data:
                        raise CjException(
                            msg=f"'{k}' parameter was missing from config json"
                        )
                self._test_interval = data["test_interval"]
                self._timestamp_format = data["timestamp_format"]

        self._actions = data.get("actions")
        self._api_response = data.get("api_response")
        self._authentication = data.get("authentication")
        self._custom_config = data.get("custom_config")
        self._record_type = data.get("record_type")
        self._transform_mode = data.get("transform", "keep")

        self._set_credentials(cred_info=data["credentials"])
        for k in data.keys():
            if k not in supported_keys + _optional_config_keys:
                logging.warning(f"['{k}'] in config will be ignored")

    def _set_credentials(self: object, cred_info: dict) -> None:
        """
        load credentials into object for later reference

        Args:
            self: CrownJewelPollerConfig object
            cred_info: parameters for accessing credential storage
        Raises:
             CjException if cred_info lacks required information or credential read fails
        """

        env = self.get_environment()
        if env == "library":
            self._credentials = cred_info
        else:

            for k in ["location"]:
                if k not in cred_info:
                    raise CjException(
                        msg=f"['credentials']['{k}'] is a required configuration parameter"
                    )
                if not cred_info[k] or type(cred_info[k]) is not str:
                    raise CjException(msg=f"['credentials']['{k}'] must have a value")
            if env == "aws":
                self._credentials = self._get_creds_from_secret_store(cred_info)
            else:
                raise CjException(f"'{env}' environment is not currently supported")

        if self._credentials is None:
            raise CjException(
                f"No data retrieved from AWS Parameter Store {self.get_region()} '{cred_info['location']}'"
            )

    def get_storage_location(self: object) -> str:
        """
        Return the storage location of the data

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            name of the storage location file
        """
        return self._storage_location

    def get_bucket_prefix(self: object) -> str:
        """
        Return the prefix for output files

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            prefix for bucket path
        """
        return self._output_prefix

    def get_credentials(self: object, schema: dict = None) -> dict:
        """
        return credentials loaded from store

        Args:
            self: CrownJewelPollerConfig object
            schema: expected keys and types of credentials
        Returns:
            credentials loaded from store
        Raises:
            CjException if credentials do not match schema
            CjMissingParameterException if schema was empty or not provided
        """
        if schema:
            for key in schema:
                if key not in self._credentials:
                    raise CjException(
                        msg=f"['{key}'] is required in credentials, but absent"
                    )
                value = self._credentials[key]
                if not type(value).__name__ == schema[key]:
                    raise CjException(
                        msg=f"['{key}'] was supposed to be {schema[key]}, but was {type(value)}"
                    )
        else:
            raise CjMissingParameterException(
                msg="No schema was provided for credential validation"
            )

        return self._credentials

    def get_custom_config(self: object) -> dict:
        """
        Return the contents of the custom_config portion of the poller configuration

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            poller-specific configuration options
        """
        return self._custom_config

    def get_environment(self: object) -> str:
        """
        Return the type of hosting environment

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            type of environment (e.g. 'aws', 'azure', 'gcp' or 'library')
        """
        return self._env

    def get_poller_type(self: object) -> str:
        """
        Return the type of poller

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            type of poller (e.g. 'workday' or 'slack')
        """
        return self._type

    def get_record_type(self: object) -> str:
        """
        Return the value for record_type from config file.

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            type of record to be parsed
        """
        return self._record_type

    def get_region(self: object) -> str:
        """
        Return the region in which a poller should be run

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            name of the aws-region
        """
        return self._region

    def get_cursor(self: object) -> str:
        """
        retrieve cursor from persistent storage

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            previously stored value, or None if no value previously stored
        """
        if self._cursor_param:
            env = self.get_environment()
            if env == "library":
                return self._cursor_param
            elif env == "aws":
                region = self.get_region()
                if self._profile:
                    session = boto3.session.Session(profile_name=self._profile)
                    ssm = session.client(
                        service_name="ssm",
                        region_name=region,
                        endpoint_url=boto_endpoints["ssm"][region],
                    )
                else:
                    ssm = boto3.client(
                        service_name="ssm",
                        region_name=region,
                        endpoint_url=boto_endpoints["ssm"][region],
                    )
                try:
                    parameter_response = ssm.get_parameter(Name=self._cursor_param)
                    return parameter_response["Parameter"]["Value"].strip()
                # It's ok for this to be missing for the first execution of a new poller
                except ssm.exceptions.ParameterNotFound as e:
                    return None
            else:
                raise CjException(f"'{env}' environment is not currently supported")

    def get_test_interval(self: object) -> timedelta:
        """
        Return an offset representing the intended period for functional testing of a given poller

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            offset to be used to define a test period or None (if cursor should not be reset)
        Raises:
            CjInvalidParameterException if invalid offset valid was specified
        """
        # For now, assume all offsets are an integer number of minutes
        try:
            offset = int(self._test_interval)
        except Exception as e:
            raise CjException(msg=str(e))
        if offset < 0:
            raise CjInvalidParameterException(
                parameter="['test_interval']",
                msg="invalid value ({self._test_interval})",
            )
        elif offset == 0:
            return None
        return timedelta(minutes=offset)

    def get_timestamp_format(self: object) -> str:
        """
        Return the cursor format from poller config file

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            strftime-compatible format for timestamp generation
        Raises:
            CjInvalidParameterException if timestamp_format was not previously provided in config
        """
        if self._timestamp_format and type(self._timestamp_format) is str:
            return self._timestamp_format
        raise CjInvalidParameterException(parameter="['timestamp_format']")

    def get_url(self) -> str:
        """
        Return the URL to which the Poller should connect

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            URL to which the poller should connect
        """
        return self._url

    def get_transform_mode(self: object) -> str:
        """
        Return how the specified schema should be used for transforms

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            action to take with extra columns or (None) if no transform is desired
        """
        logging.debug(f"Transform mode is {self._transform_mode}")
        return self._transform_mode

    def is_stateless(self: object) -> bool:
        """
        Return whether this poller should maintain state between invocations
        Args:
            self: CrownJewelPollerConfig object

        Returns:
            False if poller should maintain cursor, True if stateless
        """
        return self._cursor_param is None

    def set_cursor(self: object, cursor: str) -> None:
        """
        Record cursor value in persistent storage for access upon subsequent executions

        Args:
            self: CrownJewelPollerConfig object
            cursor: value to be stored
        """

        region = self.get_region()
        if cursor is None:
            logging.info("NOT Writing cursor as it was None")
        else:
            env = self.get_environment()
            if env == "library":
                self._cursor_param = cursor
            elif env == "aws":
                logging.info(f"Writing cursor ({cursor}) to {self._cursor_param}")
                if self._profile:
                    session = boto3.session.Session(profile_name=self._profile)
                    ssm = session.client(
                        service_name="ssm",
                        region_name=region,
                        endpoint_url=boto_endpoints["ssm"][region],
                    )
                else:
                    ssm = boto3.client(
                        service_name="ssm",
                        region_name=region,
                        endpoint_url=boto_endpoints["ssm"][region],
                    )
                ssm.put_parameter(
                    Description="Data cursor specific to poller",
                    Name=self._cursor_param,
                    Overwrite=True,
                    Type="String",
                    Value=str(cursor),
                )
            else:
                raise CjException(f"'{env}' environment is not currently supported")

    def get_authentication(self: object) -> dict:
        """
        Return the Authentication details for Poller to authenticate with Source.

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            Authentication details for Poller to use
        Raises:
            CjInvalidParameterException if value was specified but was not a dictionary
        """
        if self._authentication:
            if isinstance(self._authentication, dict):
                return self._authentication
            else:
                raise CjInvalidParameterException(
                    parameter="authentication",
                    msg="authentication was not a dictionary",
                )
        else:
            return None

    def get_api_response(self: object) -> dict:
        """
        Return the API Response Format and details for Poller to process response.

        Args:
            self: CrownJewelPollerConfig object
        Returns:
            API Response for Poller to process the response
        Raises:
            CjInvalidParameterException if value was specified but was not a dictionary
        """
        if self._api_response:
            if isinstance(self._api_response, dict):
                return self._api_response
            else:
                raise CjInvalidParameterException(
                    parameter="api_response", msg="api_response was not a dictionary"
                )
        return self._api_response
