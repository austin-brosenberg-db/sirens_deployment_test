"""Rest Adaptor Module.

Author:
    Derek King. 01-Nov-23

Version: 1.0

Classes:
    RestAdaptor:
    RestAdaptorResult:

Functions:
    get()
    post()
"""

import requests
import requests.packages
import time
import json
from json import JSONDecodeError
from typing import Dict

from databricks.sirens.exceptions import SirensActionException
from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

def rate_limit(max_calls, period):
    def decorator(func):
        calls = 0
        last_reset = time.time()

        def wrapper(*args, **kwargs):
            nonlocal calls, last_reset

            # Calculate time elapsed since last reset
            elapsed = time.time() - last_reset

            # If elapsed time is greater than the period, reset the call count
            if elapsed > period:
                calls = 0
                last_reset = time.time()

            # Check if the call count has reached the maximum limit
            if calls >= max_calls:
                return RestAdaptorResult(False, 429, "Rate limit exceeded..")

            # Increment the call count
            calls += 1

            # Call the original function
            return func(*args, **kwargs)

        return wrapper
    return decorator

class RestAdaptorResult:
    def __init__(self, success: bool, status_code: int, message: str = '', data: Dict = None):
        self.success = bool(success)
        self.status_code = status_code
        self.message = str(message)
        self.data = data if data else {}


class RestAdaptor:
    def __init__(self, hostname: str, headers: dict, ssl_verify: bool = True, timeout: int = None,
                 retries: int = 3, max_calls: int = 10000, period: int = 86400):
        self.hostname = hostname
        self.headers = headers
        self.ssl_verify = ssl_verify
        self.timeout = timeout
        self.retries = retries
        if not self.retries:
            self.retries = 3
        self.max_calls = max_calls
        self.period = period
        if not self.ssl_verify:
            requests.packages.urllib3.disable_warnings()

    @rate_limit(max_calls=500, period=60)
    def _do(self, http_method: str, endpoint: str = None, params: Dict = None, data: Dict = None):
        
        if not self.hostname.startswith('http'):
            self.hostname = f"https://{self.hostname}"

        if endpoint:
            url = self.hostname + endpoint
        else:
            url = self.hostname

        # convert python dict to json.
        if isinstance(data, dict):
            data = json.dumps(data)

        count = 1
        while count <= self.retries:
            try:
                response = requests.request(method=http_method, url=url, data=data, headers=self.headers, params=params,
                                            verify=self.ssl_verify, timeout=self.timeout)
                break

            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as exc:
                if count == self.retries:
                    raise SirensActionException(f"Request failed. Reason: {exc}")

                logger.info(f'{exc}: retry {count}')
                count += 1
        
        if response.status_code != 204 and response.headers.get('content-type').strip().startswith('application/json'):
            try:
                data_out = response.json()

            except (ValueError, JSONDecodeError) as exc:
                logger.warning(f"bad json response from: {url}, error:{exc}")
                return RestAdaptorResult(False, response.status_code, message=response.reason, data={})
        else:
            data_out = {'response_text': response.text}

        if response.status_code >= 200 and response.status_code <= 299:
            return RestAdaptorResult(True, response.status_code, message=response.reason, data=data_out)

        if response.status_code >= 400 and response.status_code <= 499:
            return RestAdaptorResult(False, response.status_code, message=response.reason, data=data_out)

        raise SirensActionException(f"{response.status_code}: {response.reason}, {data_out}")

    def post(self, endpoint: str = None, data: dict = {}, params: dict = None) -> RestAdaptorResult:
        """post data to a rest endpoint

        :param endpoint: example: /rest/container/1
        :type endpoint: str
        :param data: data to be sent in dict/json formats, defaults to {}
        :type data: dict, optional
        :param params: params needed, defaults to None
        :type params: dict, optional
        :return: RestAdaptorResult
        :rtype: RestAdaptorResult
        """
        return self._do(http_method="POST", endpoint=endpoint, data=data, params=params)

    def get(self, endpoint: str, params: Dict = None) -> RestAdaptorResult:
        """get data from a rest endpoint

        :param endpoint: example: /rest/container/1
        :type endpoint: str
        :param params: params needed, defaults to None
        :type params: dict, optional
        :return: RestAdaptorResult
        :rtype: RestAdaptorResult
        """
        return self._do(http_method="GET", endpoint=endpoint, params=params)
