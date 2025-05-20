from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional, Dict, Union, NamedTuple, Any

from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.exceptions import SirensActionException
from databricks.sirens.internal.secrets import SecretsManager
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class ActionResult(NamedTuple):
    status: bool
    data: Any


class BaseAction(ABC):
    def __init__(self, actionConfigObj: dict):
        spark = BaseAction._get_spark_session()

        self.actionConfigObj = actionConfigObj
        self.headers = actionConfigObj.get("connection").get("headers")
        self.server = actionConfigObj.get("connection").get("server")
        self.auth_type = actionConfigObj.get("connection").get("auth").get("auth_type", None)
        self.auth_token = actionConfigObj.get('connection').get('auth').get('secret', None)
        self.username = actionConfigObj.get('connection').get('auth').get('username', None)
        self.password = actionConfigObj.get('connection').get('auth').get('password', None)
        token_scope = actionConfigObj.get("connection").get("token", None)

        # auth token should be stored in db secrets
        if self.auth_token:
            self.auth_token = BaseAction._dbutils_get_auth_token(spark, self.auth_token)

        # last resort. check for token in a connection->auth_token key.
        if not self.auth_token:
            self.auth_token = actionConfigObj.get("connection").get("auth_token", None)

        # no choice but to error out on auth_token
        if not self.auth_token:
            logger.error("failed to get auth token from secrets and connection->auth_token")
            raise SirensActionException("failed to get auth token from secrets and connection->auth_token")

        # no choice but to error out on server
        if not self.server:
            logger.error("failed to get server identity from 'connection->server'")
            raise SirensActionException("failed to get server identity from 'connection->server'")

        logger.debug(
            f"creating connection to server: {self.server} headers: {self.headers}, with auth_token: {bool(self.auth_token)}")

    @staticmethod
    def _get_spark_session():
        return SparkSession.getActiveSession()

    @staticmethod
    def _dbutils_get_auth_token(spark, token_scope):
        try:
            if isinstance(token_scope, dict):
                auth_token = BaseUtils._get_dbutils(spark).secrets.get(token_scope['scope'], token_scope['key'])
            if isinstance(token_scope, str):
                if token_scope.startswith('{{'):
                    token_scope =  SecretsManager().substitute_secrets({'token': token_scope})

        except Exception as exc:
            auth_token = None

        return auth_token

    @abstractmethod
    def do_action(self, requested_action: str, params: Optional[Dict],
                  body: Optional[Union[DataFrame, List[Dict]]]) -> List:
        """Entry point for supported actions

        :param requested_action: name of a function that implements the action (ex. post_message)
        :type requested_action: str
        :param actionConfigObj: dict of the action.yaml file
        :type actionConfigObj: dict
        :param params: any params required to setup the action
        :type params: Optional[Dict]
        :param body: data to send using the action
        :type body: Optional[List[Dict]]
        :raises SirensActionException: _description_
        :return: _description_
        :rtype: List
        """
        pass

    @staticmethod
    def validate_requested_action(requested_action: str, supported_actions: list) -> bool:
        if requested_action not in supported_actions:
            logger.warning(f'requested action: {requested_action}, not in supported actions')
            return False
        else:
            return True
