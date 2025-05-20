from abc import ABC, abstractmethod
from typing import List, Optional, Union, Dict
import json
import base64
from databricks.sirens.internal.restadaptor import RestAdaptor
from pyspark.sql import DataFrame
from databricks.sirens.internal.baseaction import ActionResult
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)


class API(ABC):
    def __init__(self, server: str, headers: dict = {}, auth_token: str = None, auth_type: str = None, username: str = None, password: str = None,
                 timeout: int = None, retries: int = None, verify_ssl: bool = None, use_sirens_config: bool = True):
        self.server = server
        self.headers = headers
        self.auth_token = auth_token
        self.auth_type = auth_type
        self.username = username
        self.password = password
        self.timeout = timeout
        self.retries = retries
        self.verify_ssl = verify_ssl
        self.use_sirens_config = use_sirens_config

        if self.username and self.password:
            user_pass = f'{self.username}:{self.password}'.encode('utf-8')
            self.headers['Authorization'] = f"Basic {base64.b64encode(user_pass)}"

        if self.auth_token and self.auth_type == 'Bearer':
            # prefer auth_token over username/password
            self.headers['Authorization'] = f"Bearer {self.auth_token}"

        if not self.headers:
            self.headers = {'Content-Type': 'application/json'}

        if not self.timeout:
            self.timeout = 10

        if not self.retries:
            self.retries = 3

        if not self.verify_ssl:
            self.verify_ssl = False

        logger.debug(f"creating connection to server: {self.server} with headers: {self.headers}, with auth_token: {bool(self.auth_token)}")

    @abstractmethod
    def list_actions(self) -> List:
        """print the methods/actions available in this API

        :return: list of methods that can be called in this API.
        :rtype: List
        """
        # import inspect
        # return [func for func in dir(<MODULE>) if callable(getattr(<MODULE>, func)) and not func.startswith("_")]
        pass

    def post(self, data: Union[DataFrame, List[Dict]], row_per_event=False) -> ActionResult:

        if isinstance(data, DataFrame):
            data = BaseUtils.frame_to_dict(data)

        # post entire data to endpoint as a single record/blob
        if not row_per_event:
            if data:
                logger.debug(f"sending datatype: {type(data)}, data: {data}")
                result = (RestAdaptor(hostname=self.server, headers=self.headers, ssl_verify=self.verify_ssl,
                                     timeout=self.timeout, retries=self.retries)
                                     .post(data=json.dumps(data)))
            else:
                logger.warning('no records passed, sending nothing')
                return ActionResult(False, {"message": "no records passed to send"})

        # post individual records to the endpoint. (care here for API limits...)
        if row_per_event:
            results = []
            for itm in data:
                result = (RestAdaptor(hostname=self.server, headers=self.headers, ssl_verify=self.verify_ssl,
                                     timeout=self.timeout, retries=self.retries)
                                     .post(data=json.dumps(itm)))
                results.append(result.data)

        # return results
        if not result.success:
            logger.error(f"failed: {result.status_code}, {result.message}")
            return ActionResult(False, f"status: {result.status_code}, message: {result.message}, error: {result.data}")
        else:
            if not row_per_event:
                return ActionResult(True, result.data)
            else:
                return ActionResult(True, results)

