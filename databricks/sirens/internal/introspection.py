"""Gets the current environment variables and running context

Author:
    Derek King (2023-01-12)

Classes:
    Introspection

"""
import json
from typing import Union

from pyspark.sql import SparkSession

from databricks.sirens.logging import get_logger

USED_CONF_KEYS = [
    'spark.driver.tempDirectory', 'spark.executor.tempDirectory',
    'spark.databricks.cloudProvider', 'spark.databricks.workspaceUrl',
    'spark.databricks.clusterUsageTags.clusterName',
    'spark.databricks.clusterUsageTags.clusterId',
    'spark.databricks.clusterUsageTags.sparkVersion',
    'spark.databricks.clusterUsageTags.clusterNodeType',
    'spark.databricks.clusterUsageTags.clusterMinWorkers', 'spark.app.name',
    'spark.databricks.clusterUsageTags.clusterMaxWorkers',
    'spark.databricks.clusterUsageTags.clusterScalingType',
    'spark.databricks.driverNodeTypeId',
    'spark.driver.maxResultSize', 'spark.databricks.workerNodeTypeId',
    'spark.executor.memory',
    'spark.databricks.clusterUsageTags.clusterUnityCatalogMode',
    'spark.databricks.clusterUsageTags.clusterLogDestination',
    'spark.databricks.clusterUsageTags.instanceProfileUsed',
    'spark.databricks.clusterUsageTags.instanceProfileArn',
    'spark.databricks.clusterUsageTags.enableCredentialPassthrough',
    'spark.scheduler.mode',
    'spark.databricks.isv.product'
]

logger = get_logger(__name__)


class Introspection:
    def __init__(self, workspace_url: str = None, token: str = None) -> None:
        self.spark = SparkSession.getActiveSession()
        self.pyspark_settings = self._get_spark_conf()
        dbutils = self._get_dbutils(self.spark)
        if workspace_url:
            self.workspace_url = workspace_url
        else:
            self.workspace_url = self.pyspark_settings.get("spark.databricks.workspaceUrl", None)
        # add prefix & suffix
        if self.workspace_url:
            self.workspace_url = self._make_workspace_url(self.workspace_url)

        if token:
            self.token = token
        else:
            self.token = self._get_notebook_token(dbutils)

        self.app_name = "databricks-sirens"

    @staticmethod
    def _make_workspace_url(workspace_url: str) -> str:
        """prefix workspace url with https://

        :param workspace_url: url
        :type workspace_url: str
        :return: url
        :rtype: str
        """
        # Add https:// if not already there.
        if workspace_url[:8] != 'https://':
            workspace_url = 'https://' + workspace_url

        return workspace_url

    @staticmethod
    def _get_notebook_token(dbutils) -> Union[str, None]:
        """get notebook context

        :param dbutils: dbutils object
        :type dbutils: object
        :return: notebook context
        :rtype: Union[str, None]
        """
        try:
            notebook_context = json.loads(
                dbutils.notebook().entry_point.getDbutils().notebook().getContext().safeToJson())
            token = notebook_context.get("attributes").get("api_token")
            return token
        except Exception as exc:
            logger.warning(f"{exc}")
            return None

    def _get_spark_conf(self) -> dict:
        """get spark context settings

        :return: specific spark settings of interest
        :rtype: dict
        """
        pyspark_settings = {}
        non_existing_value = "______non_existing_value______"
        try:
            conf_keys = USED_CONF_KEYS
            for key in conf_keys:
                value = self.spark.conf.get(key, non_existing_value)
                if value and value != non_existing_value:
                    pyspark_settings[key] = value
        except Exception as exc:
            logger.warning(exc)
        return pyspark_settings

    @staticmethod
    def _get_dbutils(spark) -> object:
        """get dbutils

        :param spark: spark
        :type spark: SparkSession
        :return: DBUtils
        :rtype: object
        """
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
