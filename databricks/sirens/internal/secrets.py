"""Handle Secrets in Sirens

    Authors: Derek King
    Date: 2024-04-03
    Version: 1.0

    classes:
        SecretsManager:
            Base class initially for reading databricks secrets abstracted in yaml files

    methods:
        substitute_secrets() - Parse a yaml (dict) looking for {{secrets/scope/key}} pattern and sub with values

"""

import re
from typing import Dict, List, Union

from pyspark.sql import SparkSession

from databricks.sirens.logging import get_logger
from databricks.sirens.utils.base_utils import BaseUtils

# from databricks.sdk import WorkspaceClient

logger = get_logger(__name__)


class SecretsManager:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        # TODO use databricks.sdk (not working with native auth in notebooks on shared cluster for some reason)
        # self.w = WorkspaceClient(self.kwargs)

    @staticmethod
    def substitute_secrets(d: Dict) -> Dict:
        """parse dict values looking for secret substitution in form '{{secrets/scope/key}}' and sub with the secret

        :param d: input_dict
        :type d: dict
        :return: dictionary with substituted values or original if can't be found/got.
        :rtype: dict
        """
        if not isinstance(d, dict):
            logger.error(f"Invalid Input type: {type(d)} - should be a dict")
            return d

        return SecretsManager._dict_replace_value(d)

    @staticmethod
    def get_secret(scope: str, key: str) -> Union[str, None]:
        try:
            return BaseUtils._get_dbutils(SparkSession.getActiveSession()).secrets.get(scope, key)
            # return self.w.secrets.get_secret(scope=scope, key=key)
        except Exception as exc:
            logger.error(f'failed to get key for scope: {scope}, reason: {exc}')
            return None

    @staticmethod
    def _replace_secret(v: str) -> str:
        try:
            secrets_re = re.compile(r"\{\{(secrets)\/([^:]+)\/(.+?)\}\}$")
            is_secret = re.search(secrets_re, v)

            if not is_secret:
                return v

            if len(is_secret.groups()) != 3:
                logger.warning(f"invalid secret expression: {v}" + ", should be in form {{secrets/scope/key}}")
                return v
            else:
                secret_type, scope, key = is_secret.groups()

        except ValueError as exc:
            logger.error(f"Invalid spec for secret replacement: {exc}")
            return v
        except Exception as exc:
            logger.error(f"{exc}")
            return v

        if secret_type == 'secrets':
            return SecretsManager.get_secret(scope=scope, key=key) or v
        else:
            logger.error(f"Supported secret type(s): 'secrets' - passed {secret_type} - please correct.")
            return v

    @staticmethod
    def _dict_replace_value(d: dict) -> dict:
        x = {}
        for k, v in d.items():
            if isinstance(v, dict):
                v = SecretsManager._dict_replace_value(v)
            elif isinstance(v, list):
                v = SecretsManager._list_replace_value(v)
            elif isinstance(v, str):
                v = SecretsManager._replace_secret(v)
            x[k] = v
        return x

    @staticmethod
    def _list_replace_value(l: List) -> List:
        x = []
        for e in l:
            if isinstance(e, list):
                e = SecretsManager._list_replace_value(e, )
            elif isinstance(e, dict):
                e = SecretsManager._dict_replace_value(e)
            elif isinstance(e, str):
                e = SecretsManager._replace_secret(e)
            x.append(e)
        return x
