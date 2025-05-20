"""Splunk SOAR API Module.

Author:
    Derek King. 01-Nov-23

Version: 1.0

the alert_handler reads alerts/records from a delta table and sends them to a phantom / Splunk SOAR instance using
the alerts.yaml & splunk_soar/action.yaml.

map your alert schema to the Phantom RESTAPI fields using action.yaml.

With the action.yaml and alerts.yaml configured, process them using
.. code-block:: python3

    from databricks.sirens.alertmanager import AlertHandler
    alert_processor = AlertHandler(database="<alert_database>", tables=["<list of tables to process>"], since="INTERVAL 1 day")
    alert_processor.process_alerts()

You can interact directly with the module by importing SoarAPI
.. code-block:: python3

    from databricks.sirens.actions.splunk_soar import SoarAPI
    SoarObj = SoarAPI(server=<server>, auth_token: str = None, verify_ssl=[True|False])
    # optionally create a container
    result = SoarObj.create_container(name"test_event", **kwargs)
    if result.status:
        container_id = result.data['id']

    # add artifacts (either specify a container_id or containers will be created)
    result = SoarObj.add_artifacts(df)

Classes:
    Action
    SoarAPI

Functions:
    create_container()
    add_artifacts()
    custom_rest()
    list()
    do_action(action, params, body)

Actions:
    send_alerts

"""
import inspect
import json
import re
from typing import Optional, Literal, List, Dict, Union

try:
    from pydantic.dataclasses import dataclass
except ImportError:
    from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from databricks.sirens.actions import plugins
from databricks.sirens.internal.baseapi import API
from databricks.sirens.internal.restadaptor import RestAdaptor
from databricks.sirens.internal.baseaction import BaseAction, ActionResult
from databricks.sirens.config_reader import ActionReader
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.exceptions import SirensActionException, SirensConfigException

from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class SoarDefaults:
    SUPPORTED_ACTIONS = ["send_alerts", "create_container", "add_artifacts"]

    container_defaults = {
        "name": "databricks_event",
        "label": "events"
    }

    artifact_defaults = {
        "name": "artifact_name",
        "label": "databricks_event"
    }

    cef_fields = [
        "ApplicationProtocol",
        "act",
        "app",
        "baseEventCount",
        "bytesIn",
        "bytesOut",
        "cat",
        "destinationAddress",
        "destinationDnsDomain",
        "destinationHostName",
        "destinationMacAddress",
        "destinationNtDomain",
        "destinationPort",
        "destinationProcessName",
        "destinationServiceName",
        "destinationTranslatedAddress",
        "destinationTranslatedPort",
        "destinationUserId",
        "destinationUserName",
        "destinationUserPrivileges",
        "deviceAction",
        "deviceAddress",
        "deviceDirection",
        "deviceDnsDomain",
        "deviceEventCategory",
        "deviceExternalId",
        "deviceFacility",
        "deviceHostname",
        "deviceInboundInterface",
        "deviceMacAddress",
        "deviceOutboundInterface",
        "deviceProcessName",
        "deviceTranslatedAddress",
        "dhost",
        "dmac",
        "dntdom",
        "dpriv",
        "dproc",
        "dpt",
        "dst",
        "duid",
        "duser",
        "dvc",
        "dvchost",
        "end",
        "endTime",
        "externalId",
        "fileCreateTime",
        "fileHash",
        "fileId",
        "fileModificationTime",
        "fileName",
        "filePath",
        "filePermission",
        "fileSize",
        "fileType",
        "fname",
        "fsize",
        "in",
        "message",
        "method",
        "msg",
        "oldfileCreateTime",
        "oldfileHash",
        "oldfileId",
        "oldfileModificationTime",
        "oldfileName",
        "oldfilePath",
        "oldfilePermission",
        "oldfileType",
        "oldfsize",
        "out",
        "proto",
        "receiptTime",
        "request",
        "requestClientApplication",
        "requestCookies",
        "requestMethod",
        "requestURL",
        "rt",
        "shost",
        "smac",
        "sntdom",
        "sourceAddress",
        "sourceDnsDomain",
        "sourceHostName",
        "sourceMacAddress",
        "sourceNtDomain",
        "sourcePort",
        "sourceServiceName",
        "sourceTranslatedAddress",
        "sourceTranslatedPort",
        "sourceUserId",
        "sourceUserName",
        "sourceUserPrivileges",
        "spriv",
        "spt",
        "src",
        "start",
        "startTime",
        "suid",
        "suser",
        "transportProtocol"
    ]

    @staticmethod
    def snake_case_dict_values(mapping: dict) -> dict:
        """return the dict with . substituted for _

        :param mapping: the content of action.yaml. contains dot notation - we need to swap for _ for when a DF is flattened
        :type mapping: dict
        :return: _description_
        :rtype: dict
        """
        if not isinstance(mapping, dict):
            raise SirensActionException("mapping must be a dict from action.yaml")

        new_dict = {}
        for k, v in mapping.items():

            # if value is a dict, traverse and substitute sub value(s)
            if isinstance(v, dict):
                temp_dict = dict(v.items())
                sub_dict = {}
                for sub_k, sub_v in temp_dict.items():
                    if re.search(r"\.", sub_v):
                        sub_dict[sub_k] = re.sub(r"\.", "_", sub_v)
                    else:
                        sub_dict[sub_k] = sub_v

                new_dict[k] = sub_dict
                continue

            # check values and sub for '_'
            if isinstance(v, str):
                if re.search(r"\.", v):
                    new_dict[k] = re.sub("\.", "_", v)
                else:
                    new_dict[k] = v
                continue

        return new_dict

    def get_literal_value(input_str: str) -> Union[str, None]:
        """extract the literal value from its wrapper as it was from action.yaml

        :param input_str: example: lit('False')
        :type input_str: str
        :return: example: False
        :rtype: Union[str, None]
        """
        matched = re.match(r'lit\((\"|\')*(\w+|\[\]|{})(\"|\')*\)', input_str)
        if matched and '[]' not in matched[2]:
            return matched[2]
        else:
            return None


class APICommon:
    @staticmethod
    def to_dict(container_obj):
        return {k: v for k, v in container_obj.__dict__.items() if v is not None and k != '__pydantic_initialised__'}


@dataclass
class CreateContainer(APICommon):
    artifacts: List[str] = None
    label: str = SoarDefaults.container_defaults.get("label")
    name: str = SoarDefaults.container_defaults.get("name")
    asset_id: int = None
    close_time: str = None
    custom_fields: List[str] = None
    data: str = None
    description: str = None
    due_time: str = None
    end_time: str = None
    ingest_app_id: int = None
    kill_chain: Literal[
        "Reconnaissance", "Weaponization", "Delivery", "Exploitation", "Installation", "Command & Control", "Actions on Objectives"] = None
    owner_id: Union[int, str] = None
    role_id: Union[int, str] = None
    run_automation: bool = None
    sensitivity: str = None
    severity: Literal["Low", "Medium", "High"] = None
    source_data_identifier: str = None
    start_time: str = None
    open_time: str = None
    status: Literal["New", "Open", "Closed"] = None
    tags: List[str] = None
    tenant_id: Union[int, str] = None
    container_type: str = None
    template_id: int = None
    authorized_users: List[int] = None


@dataclass
class AddArtifacts(APICommon):
    container_id: int = None
    cef: List[str] = None
    cef_types: List[str] = None
    data: List[str] = None
    description: str = None
    end_time: str = None
    ingest_app_id = None
    kill_chain: Literal[
        "Reconnaissance", "Weaponization", "Delivery", "Exploitation", "Installation", "Command & Control", "Actions on Objectives"] = None
    label: Literal["event", "net flow"] = None
    name: str = None
    owner_id: Union[int, str] = None
    run_automation: bool = None
    severity: str = None
    source_data_identifier: str = None
    start_time: str = None
    tags: List[str] = None
    type: str = None


class SoarAPI(API):
    """class used to implement the connectivity to a splunk SOAR instance.
       can be used from notebooks directly, or the action framework as part of the pipeline definition using the Action class.
    """

    def __init__(self, server: str, headers: dict = {}, auth_token: str = None, timeout: int = None,
                 retries: int = None, verify_ssl: bool = None, use_sirens_config: bool = True):
        """Soar API

        :param server: IP address, or resolvable hostname
        :type server: str
        :param headers: any headers to be passed, defaults to {}
        :type headers: dict, optional
        :param auth_token: auth_token if not relying on db_secrets, defaults to None
        :type auth_token: str, optional
        :param timeout: rest_api timeout in secs, defaults to None
        :type timeout: int, optional
        :param retries: number of times to retry the endpoint on failure, defaults to None
        :type retries: int, optional
        :param verify_ssl: <True|False> will default to True if not overridden, defaults to None
        :type verify_ssl: bool, optional

        """
        super().__init__(server, headers, auth_token, timeout, retries, verify_ssl, use_sirens_config)
        self.headers = headers
        self.server = server
        self.auth_token = auth_token
        self.timeout = timeout
        self.retries = retries
        self.verify_ssl = verify_ssl
        self.BASE_URL = f'https://{self.server}'
        spark = SparkSession.getActiveSession()

        # need an auth token if its not going to come from config.
        if not self.auth_token and not use_sirens_config:
            logger.error("auth token must be provided when not using sirens config")
            return

        # default to verify ssl auth if not explicitly set.
        if verify_ssl is None and not use_sirens_config:
            self.verify_ssl = True

        # set args using a config file (default for the action framework).
        if use_sirens_config:
            try:
                self.soar_conf = ActionReader().read('splunk_soar')
            except Exception as exc:
                logger.error(exc)
                raise SirensConfigException(exc)

        # get the auth_token from db secrets.
        if not self.auth_token and use_sirens_config:
            token_scope = self.soar_conf.get("connection").get("token", None)
            if token_scope:
                try:
                    self.auth_token = BaseUtils._get_dbutils(spark).secrets.get(token_scope['scope'],
                                                                                token_scope['key'])
                except Exception as exc:
                    self.auth_token = None
                    logger.error(f"failed to fetch token: {token_scope}. {exc}")
                    return
            else:
                raise SirensActionException("despite best efforts we do not have an auth token")

        # set a minimal header if not explicitly provided.
        if not self.headers:
            self.headers = {}
            self.headers['ph-auth-token'] = self.auth_token

        # set the rest api timeout is specified in config.
        if not self.timeout and use_sirens_config:
            self.timeout = self.soar_conf.get("connection").get("max_timeout", 30)
            if not isinstance(self.timeout, int):
                raise SirensActionException("config for timeout must be integer")

        # set the retry strategy.
        if not self.retries and use_sirens_config:
            self.retries = self.soar_conf.get("connection").get("retries", 3)
            if not isinstance(self.retries, int):
                raise SirensActionException("config for retries must be integer")

        # get the verify_ssl arg from config if using.
        if self.verify_ssl is None and use_sirens_config:
            self.verify_ssl = self.soar_conf.get("connection").get("verify_ssl", True)
            if not isinstance(self.verify_ssl, bool):
                raise SirensActionException("config for verify_ssl must be True or False")

        logger.debug(
            f'set server: {self.server}, with auth_token: {bool(self.auth_token)}, verify_ssl: {self.verify_ssl}')

    @staticmethod
    def list_actions() -> List:
        """print the methods/actions available in this API

        :return: list of methods that can be called
        :rtype: List
        """
        return [func for func in dir(SoarAPI) if callable(getattr(SoarAPI, func)) and not func.startswith("_")]

    @staticmethod
    def _get_config_key(conf: dict, key: Literal['container', 'artifacts']) -> dict:
        """return the portion of the config, in a format we can use.

        :param conf: action.yaml dict
        :type conf: dict
        :param key: either container or artifacts
        :type key: Literal[container, artifacts]
        :return: subsection of config, in a usable state
        :rtype: dict
        """
        try:
            artifact_mapping = conf['artifact']
            container_mapping = conf['container']
        except KeyError as exc:
            raise SirensActionException('key does not exist in action.yaml')

        if 'artifacts' in key:
            artifact_mapping = {k.lower(): v for k, v in artifact_mapping.items()}
            artifact_mapping = SoarDefaults.snake_case_dict_values(artifact_mapping)
            return artifact_mapping

        if 'container' in key:
            container_mapping = {k.lower(): v for k, v in container_mapping.items()}
            container_mapping = SoarDefaults.snake_case_dict_values(container_mapping)
            return container_mapping

    # Validate arguments passed to the API.
    def _validate_kwargs(action: str):
        def do_val(func):
            def validate(*args, **kwargs):
                _, _, _, values = inspect.getargvalues(inspect.currentframe())
                passed_args = values["kwargs"]

                flat_args = {k: v for k, v in passed_args.items() if k not in {'kwargs'}}
                if 'kwargs' in passed_args.keys():
                    flat_args.update(passed_args['kwargs'])

                # Pass kwargs to the container for pydantic validation
                if action == "create_container":
                    created = CreateContainer(**flat_args)

                # Pass kwargs to the artifact object for pydantic validation
                if action == "add_artifacts":
                    created = AddArtifacts(**flat_args)

                return func(*args, **kwargs)

            return validate

        return do_val

    @staticmethod
    def _df_to_dict(df):
        return [row.asDict() for row in df.collect()]

    @staticmethod
    def _make_dict(json_record: dict, transforms: dict, container_id: int = None) -> dict:
        """create the json record for phantom, using column values, or literals as define in the config

        :param json_record: line of json data from the dataframe
        :type json_record: dict
        :param transforms: the transforms dict from action.yaml
        :type transforms: dict
        :param container_id: a container_id if known/needed, defaults to None
        :type container_id: int, optional
        :return: json record that can be passed to phantom
        :rtype: dict
        """
        record_dict = {}
        for k, v in transforms.items():

            # process literal values first
            if isinstance(v, str) and v.startswith('lit('):
                val = SoarDefaults.get_literal_value(v)
                if val:
                    record_dict[k] = val
                    continue

            # get the column value
            if isinstance(v, str) and json_record.get(v):
                record_dict[k] = json_record.get(v)

            # if sub dictionary - process one level
            if isinstance(v, dict):
                temp_dict = dict(v.items())
                sub_dict = {}
                for sub_k, sub_v in temp_dict.items():
                    if json_record.get(sub_v):
                        sub_dict[sub_k] = json_record.get(sub_v)

                record_dict[k] = sub_dict
        if container_id:
            record_dict['container_id'] = container_id

        return json.dumps(record_dict)

    def _create_container_json(self, source_data_identifier: str, label: str, name: str, json_data: dict = None,
                               **kwargs) -> dict:
        """create the json record that can be sent to phantom

        :param source_data_identifier: unique identifier to tie back to the source system
        :type source_data_identifier: str
        :param label: container label
        :type label: str
        :param name: container name
        :type name: str
        :param json_data: data that can be used to create dynamic containers, defaults to None
        :type json_data: dict, optional
        :return: json record ready to be sent to phantom
        :rtype: dict
        """
        data = {}
        # if passed in some data, we can substitute in column values.
        if json_data:
            transforms = SoarAPI._get_config_key(self.soar_conf, 'container')
            data = json.loads(SoarAPI._make_dict(json_data, transforms))

        # link to the original event/record
        if source_data_identifier:
            data['source_data_identifier'] = source_data_identifier
        # override default container label
        if label:
            data['label'] = label
        # override default container name
        if name:
            data['name'] = name

        # apply hardcoded defaults if not set already.
        if not data.get("label"):
            data['label'] = SoarDefaults.container_defaults.get("label")

        if not data.get("name"):
            data['name'] = SoarDefaults.container_defaults.get("name")

        # add any kwargs passed
        data.update(kwargs)
        data = json.dumps(data)
        return data

    def _create_artifacts_json(self, df: Union[DataFrame, List[dict]], container_id: int = None,
                               container_per_row: bool = False) -> List[Dict]:
        """create the json records that can be sent to phantom as artifacts

        :param df: dataframe or a dataframe.toJson construct
        :type df: Union[DataFrame, List[dict]]
        :param container_id: container these artifacts should be added to, defaults to None
        :type container_id: int, optional
        :param container_per_row: if adding multiple artifacts to a single container or one artifact per container, defaults to False
        :type container_per_row: bool, optional
        :return: list of json rows that can be sent directly to phantom
        :rtype: List[Dict]
        """
        artifacts = []

        # ActionHandler already passes a json list
        if BaseUtils.is_spark_dataframe(df):
            # flatten dataframe
            df = BaseUtils.flatten_frame(df)

            # decompose dataframe to python dictionary
            json_lines = SoarAPI._df_to_dict(df)
        else:
            json_lines = df

        # get artifact mappings from action.yaml
        artifact_mapping = SoarAPI._get_config_key(self.soar_conf, 'artifacts')
        container_mapping = SoarAPI._get_config_key(self.soar_conf, 'container')

        # create a container for artifacts if no container_id passed.
        # add ALL artifacts in the DataFrame to a single container.
        if not container_id and not container_per_row:
            logger.debug("artifacts passed without existing container - creating one.")
            result = self.create_container()
            if result.status:
                container_id = result.data['id']
            else:
                raise SirensActionException(f"Failed to create container: {result.data}")

        # create artifact for each row in the dataframe/json records.
        for line in json_lines:

            # if a container per row in the DataFrame is needed, create it.
            if not container_id and container_per_row:
                print(f"executing - container_id: {container_id}, per_row: {container_per_row}")
                sub_dict = {}
                sub_dict['name'] = container_mapping.get("name")
                sub_dict['label'] = container_mapping.get("label")
                container_dict = json.loads(SoarAPI._make_dict(line, sub_dict))

                result = self.create_container(name=container_dict.get("name"), label=container_dict.get("label"),
                                               json_data=line)

                if result.status:
                    container_id = result.data['id']
                else:
                    logger.warning(f"Failed to create container: {result.data}")
                    continue

            # create the json record that will get passed to splunk soar
            artifacts.append(SoarAPI._make_dict(line, artifact_mapping, container_id))

            # reset for next row
            if container_per_row:
                container_id = None

        return artifacts

    @_validate_kwargs(action="add_artifacts")
    def add_artifacts(self, df: DataFrame, container_id: int = None, container_per_row: bool = True) -> ActionResult:
        """send artifacts to a phantom instance

        :param df: dataframe of records to be sent. requires action.yaml to define the mapping between dataframe schema and the rest API fields
        :type df: DataFrame
        :param container_id: either specify the container, or automatic creation, defaults to None
        :type container_id: int, optional
        :param container_per_row: False if the dataframe content should be captured in a single container, defaults to True
        :type container_per_row: bool, optional
        :return: ActionResult Object
        :rtype: ActionResult
        """
        endpoint = '/rest/artifact'

        # override as this would not make sense.
        if container_id:
            container_per_row = False

        artifacts = self._create_artifacts_json(df, container_id, container_per_row)
        if not artifacts:
            return ActionResult(False, {"failed": True, "message": 'no artifacts can be sent - check logs'})

        # Send each artifact to SOAR
        results = []
        for data in artifacts:
            result = RestAdaptor(hostname=self.BASE_URL, headers=self.headers, ssl_verify=self.verify_ssl,
                                 timeout=self.timeout, retries=self.retries).post(endpoint=endpoint, data=data)

            results.append(result.data)

        if not result.success:
            logger.debug(f"failed: {results}")
            return ActionResult(False, results)
        else:
            return ActionResult(True, results)

    @_validate_kwargs(action="create_container")
    def create_container(self, source_data_identifier: str = None, label: str = None, name: str = None,
                         json_data: dict = None, **kwargs) -> ActionResult:
        """Create a phantom container

        :param source_data_identifier: unique identifier from the source alert that can be used to track back, defaults to None
        :type source_data_identifier: str, optional
        :param label: a user identified label to be used, defaults to None
        :type label: str, optional
        :param name: the container name if specified, defaults to None
        :type name: str, optional
        :param json_data: json of a dataframe that can be used for dynamic mapping (from alerts.yaml), defaults to None
        :type json_data: dict, optional
        :return: ActionResult object
        :rtype: ActionResult
        """
        endpoint = '/rest/container'

        data = self._create_container_json(source_data_identifier, label, name, json_data, **kwargs)
        logger.debug(f"creating container as: {data} to endpoint: {endpoint}")

        result = RestAdaptor(hostname=self.BASE_URL, headers=self.headers, ssl_verify=self.verify_ssl,
                             timeout=self.timeout, retries=self.retries).post(endpoint=endpoint, data=data)

        if not result.success:
            logger.debug(f"failed: {result.data}")
            return ActionResult(False, result.data)
        else:
            logger.debug('success')
            return ActionResult(True, result.data)

    def custom_rest(self, method: Literal["GET", "POST"], endpoint: str, data: dict = None,
                    params: dict = None) -> ActionResult:
        """custom rest call for advanced usage. NOTE: you need to format the message as required for the rest call.

        :param method: rest get or post
        :type method: Literal[GET, POST]
        :param endpoint: example: /rest/container/2
        :type endpoint: str
        :param data: python dict of data to be sent. method will json.dumps(dict) for you, defaults to None
        :type data: dict, optional
        :param params: example: {"page": 2}, defaults to None
        :type params: dict, optional
        :return: ActionResult object
        :rtype: ActionResult
        """
        if data:
            data = json.dumps(data)

        if method == "GET":
            result = RestAdaptor(hostname=self.BASE_URL, headers=self.headers, ssl_verify=self.verify_ssl,
                                 timeout=self.timeout, retries=self.retries).get(endpoint=endpoint, params=params)
        if method == "POST":
            result = RestAdaptor(hostname=self.BASE_URL, headers=self.headers, ssl_verify=self.verify_ssl,
                                 timeout=self.timeout, retries=self.retries).post(endpoint=endpoint, data=data,
                                                                                  params=params)

        if not result.success:
            logger.debug(f"failed: {result.data}")
            return ActionResult(False, result.data)
        else:
            return ActionResult(True, result.data)


@plugins.register
class Action(BaseAction):

    def __init__(self, actionConfigObj: dict):
        super().__init__(actionConfigObj)

        self.artifact_label = actionConfigObj.get("artifact_label", SoarDefaults.artifact_defaults.get("label"))
        self.container_name = actionConfigObj.get("container_name", SoarDefaults.container_defaults.get("name"))
        self.container_label = actionConfigObj.get("container_label", SoarDefaults.container_defaults.get("label"))

    @plugins.register
    def do_action(self, requested_action: str, params: Optional[Dict],
                  body: Optional[Union[List[Dict], DataFrame]]) -> ActionResult:

        """Entry point for supported actions

        :param requested_action: name of a function that implements the action (ex. post_message)
        :type requested_action: str
        :param params: any params required to setup the action
        :type params: Optional[Dict]
        :param body: data to send using the action
        :type body: Optional[Union[List[Dict], DataFrame]]
        :raises SirensActionException: _description_
        :return: _description_
        :rtype: List
        """
        if not Action.validate_requested_action(requested_action, SoarDefaults.SUPPORTED_ACTIONS):
            logger.debug("requested action is not supported")
            return ActionResult(False, "requested action is not supported")

        phantom_conn = SoarAPI(self.server, self.headers, self.auth_token, use_sirens_config=True)

        if requested_action == "send_alerts":
            logger.debug(f"sending notification to Phantom. Server: {self.server}")
            return phantom_conn.add_artifacts(body)
