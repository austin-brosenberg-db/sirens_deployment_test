import json
from typing import List, Optional, Union, Dict

from pyspark.sql import DataFrame

from databricks.sirens.actions import plugins

from databricks.sirens.internal.baseapi import API

from databricks.sirens.internal.baseaction import BaseAction, ActionResult
from databricks.sirens.config_reader import ActionReader
from databricks.sirens.exceptions import SirensActionException, SirensConfigException

from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class WebHookDefaults:
    SUPPORTED_ACTIONS = ["post"]


class WebHookAPI(API):
    def __init__(self, server: str = None, action_name: str = None, headers: dict = {}, auth_token: str = None,
                 username: str = None, password: str = None, timeout: int = None, retries: int = None,
                 verify_ssl: bool = None, use_sirens_config: bool = True):
        # read the action.yaml config if passed, or cmd line args otherwise.
        config = {'connection': {'auth': {'empty': 'config'}, 'empty': 'config'}}
        if action_name:
            config = self.read_action_config(action_name)

        self.server = server or config.get('connection').get('server', None)
        self.headers = headers or config.get('connection').get('headers', {})
        self.auth_type = config.get('connection').get('auth').get('auth_type', None)
        self.auth_token = auth_token or config.get('connection').get('auth').get('secret', None)
        self.username = username or config.get('connection').get('auth').get('username', None)
        self.password = password or config.get('connection').get('auth').get('password', None)
        self.timeout = timeout or config.get('max_timeout_seconds', None)
        self.retries = retries or config.get('retries', None)
        self.verify_ssl = verify_ssl or config.get('verify_ssl', None)
        self.use_sirens_config = use_sirens_config

        super().__init__(self.server, self.headers, self.auth_token, self.auth_type, self.username, self.password,
                         self.timeout, self.retries, self.verify_ssl, self.use_sirens_config)

    def list_actions(self) -> List:
        """print the methods/actions available in this API

        :return: list of methods that can be called
        :rtype: List
        """
        return [func for func in dir(WebHookAPI) if callable(getattr(WebHookAPI, func)) and not func.startswith("_")]

    @staticmethod
    def read_action_config(action_name: str) -> Dict:
        try:
            action_config = ActionReader().read(action_name)
            #action_config = Actions.get_action_config(action_name)
        except SirensConfigException as exc:
            logger.error(f"failed to get action config for {action_name}")
            raise SirensActionException(f"failed to get action config for {action_name}") from exc

        return action_config


@plugins.register
class Action(BaseAction):
    def __init__(self, actionConfigObj: dict):
        super().__init__(actionConfigObj)

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
        if not Action.validate_requested_action(requested_action, WebHookDefaults.SUPPORTED_ACTIONS):
            logger.debug("requested action is not supported")
            return ActionResult(False, "requested action is not supported")

        conn = WebHookAPI(self.server, self.headers, self.auth_token, use_sirens_config=True)
        if requested_action == "post":
            logger.debug(f"sending webhook to server: {self.server}")
            return conn.post(body)
