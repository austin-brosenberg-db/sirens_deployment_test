"""Implements REST API calls to the Databricks Rest API service.

Author:
    Derek King (2023-01-12)

Classes:
    DatabricksAPI

Methods:
    clusters_list()
    clusters_list_node_types()
    dbfs_list(path)

TODO - add more endpoints as the need arises
"""
import requests
import random
import time
from typing import Union, Optional

from databricks.sirens.internal.endpoints import Clusters, InstanceProfile, DBFS, DBSQL
from databricks.sirens._version import __version__
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class DatabricksAPI:
    def __init__(self, app_name: str, token: str, workspace_url: str):
        self.app_name = app_name
        self.token = token
        self.workspace_url = workspace_url
        self.bearer = f"Bearer {self.token}"
        self.headers = {"Authorization": self.bearer,
                        "User-Agent": self.app_name + "/" + __version__}

    def api_call(self, method: str, endpoint: str, body=None, retries=3) -> Optional[dict]:
        """create http request to databricks API

        :param method: GET|POST
        :type method: str
        :param endpoint: http url
        :type endpoint: str
        :param body: data to send, defaults to None
        :type body: dict, optional
        :param retries: retry attempts, defaults to 3
        :type retries: int, optional
        :return: json() response or None
        :rtype: Union[str, None]
        """
        # Should always get from the running cluster config, or be passed in.
        if not self.workspace_url:
            return None

        url = self.workspace_url + endpoint
        for retry in range(0, retries):
            try:
                logger.debug(f"{url}: {body}")
                if method == 'GET':
                    response = requests.get(url, headers=self.headers, data=body)
                if method == 'POST':
                    response = requests.post(url, headers=self.headers, data=body)
                if response.status_code == 200:
                    return response.json()

                # Retry or quit.
                if response.status_code == 500 or response.status_code == 429 or response.status_code == 503:
                    if retry == retries:
                        return None
                    else:
                        logger.debug(f'{response.status_code}: {url}')
                        backoff_delay = random.randrange(1, 10, 1)
                        time.sleep(backoff_delay)
                        continue
                else:
                    # No point retrying for any other errors.
                    logger.debug(f'{response.status_code}: {url}')
                    return None

            except Exception as exc:
                logger.debug(f'{exc}')
                if retry == retries:
                    return None
                else:
                    backoff_delay = random.randrange(1, 10, 1)
                    time.sleep(backoff_delay)
                    continue

        return None

    def clusters_list(self) -> Optional[dict]:
        return self.api_call('GET', endpoint=Clusters.list)

    def clusters_list_node_types(self) -> Optional[dict]:
        return self.api_call('GET', endpoint=Clusters.list_node_types)

    def dbfs_list(self, path) -> Optional[dict]:
        body = '{"path": "' + path + '"}'
        return self.api_call('GET', endpoint=DBFS.list, body=body)

    def instance_profiles_list(self) -> Optional[dict]:
        return self.api_call('GET', endpoint=InstanceProfile.list)

    def warehouses_list(self) -> Optional[dict]:
        return self.api_call('GET', endpoint=DBSQL.list)
