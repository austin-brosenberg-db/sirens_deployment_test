"""Implements environment checks that can be done pre-flight.

Author:
    Derek King (2023-01-12)

Classes:
    Environment()
    EnvironmentChecks()

Methods:
    get_environment(global_config)

Returns:
    dict: w/ environment vars
"""
import re

from databricks.sirens.internal.restclient import DatabricksAPI
from databricks.sirens.internal.introspection import Introspection
from databricks.sirens.logging import get_logger
from databricks.sirens.exceptions import SirensEnvironmentException

logger = get_logger(__name__)


# TODO: make a singleton out of it
class Environment:
    def __init__(self, workspace_url: str = None, token: str = None):
        self.workspace_url = workspace_url
        self.token = token
        self.app_name = 'databricks-sirens'
        self.aboutMe = Introspection(workspace_url=workspace_url, token=token)
        if not self.workspace_url:
            self.workspace_url = self.aboutMe.workspace_url
        if not self.token:
            self.token = self.aboutMe.token

    def get_environment(self, global_config: dict) -> object:
        """get environment variables and do pre-flight checks

        :param global_config: sirens.config file
        :type global_config: dict
        :raises SirensEnvironmentException: _description_
        :return: environment object
        :rtype: object
        """
        APIObj = DatabricksAPI(app_name=self.app_name, workspace_url=self.workspace_url, token=self.token)

        # Create API Call that passes isv info.
        response = APIObj.clusters_list_node_types()
        self.aboutMe.cluster_list_node_types = response

        # Get Status for DBFS Dirs
        try:
            path = global_config['default']['scratch_dir']
        except KeyError as exc:
            raise SirensEnvironmentException("default->scratch_dir does not exist in sirens.config - please correct")

        if path[:1] != "/":
            path = "/" + path

        response = APIObj.dbfs_list(path=path)
        EnvironmentCheck.dbfs_status(response)
        self.aboutMe.dbfs_list = response

        # Check spark_version
        EnvironmentCheck.spark_version(
            self.aboutMe.pyspark_settings.get('spark.databricks.clusterUsageTags.sparkVersion', None))

        # Check worker environment
        EnvironmentCheck.compute(self.aboutMe.pyspark_settings)

        return self


class EnvironmentCheck:

    def compute(pyspark_settings: dict) -> bool:
        # TODO add list of recommended cluster types from baseline tests.
        logger.debug(f"pyspark_settings: \n {pyspark_settings}")
        return True

    def spark_version(version: str) -> bool:
        """check version of spark running

        :param version: version from pyspark_settings
        :type version: str
        :raises SirensEnvironmentException: _description_
        :return: true|false
        :rtype: bool
        """
        minimum_spark_version = float(11.1)
        if version is None:
            return False

        version = re.match(r"\d+\.\d+", version)
        if not version:
            logger.warning("spark_version: mimimum DBR check failed.")
            return False

        current_version = float(version.group(0))

        if current_version < minimum_spark_version:
            raise SirensEnvironmentException(
                f"Cluster requires spark version >= {minimum_spark_version} - currently {current_version}")

        return True

    def dbfs_status(response: dict) -> bool:
        """check dbfs scratch dir file path is_dir

        :param response: http response json dict
        :type response: dict
        :return: true or false
        :rtype: bool
        """
        try:
            path = response.get("files")
            is_dir = path[0].get("is_dir")
            if is_dir:
                return True
            else:
                return False
        except Exception as exc:
            return False
