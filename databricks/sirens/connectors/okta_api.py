import datetime
import json
import os
from typing import Dict, Optional
from collections import OrderedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import IllegalArgumentException

from databricks.sirens.connectors.common import Connector
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.connectors.common import DriverNode
from databricks.sirens.exceptions import SirensConnectorException
from databricks.sirens.logging import get_logger
from . import plugins
from databricks.sirens.cjp.cjlib.cjpcommon import create_poller
from ..datasource import DataSource
from ..global_config import GlobalConfig

logger = get_logger(__name__)


@plugins.register
class Reader:
    """Class used to read OKTA api data
    """

    def __init__(self, spark: SparkSession, data_source_obj: DataSource, opts: Optional[Dict] = None) -> None:
        self.data_source_obj = data_source_obj
        self.spark = spark
        self.source, self.sourcetype = self.data_source_obj.get_source_sourcetype_info()
        self.path = BaseUtils.maybe_get_local_file_path(self.data_source_obj.get_connector_path())
        self.connector_opts = self.data_source_obj.get_connector_opts()
        self.base_url = self.connector_opts.get("url")
        self.schema_name = self.data_source_obj.get_schema_file_opt()
        self.scratch_dir = GlobalConfig.get_global_scratch_dir()
        self.streaming_mode = self.data_source_obj.get_stream_mode()
        self.target_database = self.data_source_obj.databaseName
        self.pipeline_run_id = str(self.data_source_obj.pipeline_run_id)
        # Get the schema definition if exists
        if self.schema_name is not None:
            self.schema = self.data_source_obj.read_schema_file(self.schema_name)
        else:
            self.schema = None
        # Get the authentication info for the API
        self.token = {
            'scope': self.connector_opts.get("token").get('scope'),
            'key': self.connector_opts.get("token").get('key')
        }
        # Get the last known status/cursor for this API.
        self.connector_status = Connector(self.spark, data_source_obj).read()
        if self.connector_status:
            self.cursor = self.connector_status.get("cursor", None)
            logger.debug(f"pipeline_run_id={self.pipeline_run_id} message=cursor exists. Set to: {self.cursor}")
        else:
            self.cursor = None
            logger.debug(f"pipeline_run_id={self.pipeline_run_id} message=cursor does not exist")

        # self.cursor = "https://dev-74006068.okta.com/api/v1/logs?since=2017-10-01T00:00:00.000Z"

        # verify we know where to connect to, and how to authenticate
        if self.token['scope'] is None or self.token['key'] is None or self.base_url is None:
            logger.error(
                f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=secret scope, token or url not set in connector options - please correct")
            raise SirensConnectorException("secret scope, token or url not set in connector options - please correct")

        # get start and end times
        self.start, self.end, self.backfill = None, None, None
        # Override if included as part of the options.
        self.start = self.connector_opts.get("start", None)
        self.end = self.connector_opts.get("end", None)
        self.backfill = self.connector_opts.get("backfill", None)

        # If initial collection and backfill specified
        if not self.cursor and self.backfill:
            try:
                backfill = int(self.backfill)
                if isinstance(backfill, int) and backfill > 0 and backfill <= 1095:
                    logger.debug(
                        f"pipleline_run_id={self.pipeline_run_id} message=setting collection to backfill {backfill} days")
                    daysAgo = datetime.date.today() - datetime.timedelta(days=backfill)
                    since = daysAgo.strftime("%Y-%m-%dT%H:%M:%S")
                    self.cursor = self.base_url + '?since=' + since
            except ValueError:
                logger.warning(
                    f"pipeline_run_id={self.pipeline_run_id} message=Incorrect value for backfill_days: ({self.backfill}) must be int between 1 and 1095 (max 3 years)")
                pass

        # If no backfill and no cursor set initial collection to 1 year of history
        if not self.cursor and not self.backfill:
            try:
                logger.debug(
                    f"pipleline_run_id={self.pipeline_run_id} message=defaulting collection to backfill 365 days")
                daysAgo = datetime.date.today() - datetime.timedelta(days=365)
                since = daysAgo.strftime("%Y-%m-%dT%H:%M:%S")
                self.cursor = self.base_url + '?since=' + since
            except Exception as exc:
                logger.warning(
                    f"pipeline_run_id={self.pipeline_run_id} message=failed to set inital collection to 365 days. {exc}")
                pass

    @plugins.register
    def read(self) -> DataFrame:
        """Reads the data using spark read or readStream passing any options defined in the options key in inputs.yaml

        :raises Exception: for any spark errors reading the file(s)
        :return: pyspark DataFrame
        :rtype: DataFrame
        """
        logger.info(
            f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=starting connection to okta API"
        )
        logger.debug(
            f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=fetching secret from databricks secrets"
        )
        try:
            token = BaseUtils._get_dbutils(self.spark).secrets.get(self.token['scope'], self.token['key'])
        except IllegalArgumentException as exc:
            raise SirensConnectorException(f"{exc}") from exc

        # Compile configuration options for the API client ("poller")
        poller_config = {
            "credentials": {"token": token},
            "environment": "library",
            "poller_type": "okta",
            "url": self.base_url,
        }
        if self.cursor:
            poller_config["cursor"] = self.cursor

        # Define which field contains a value to be used for cursor and how to interpret it
        poller_schema = {
            "published": {
                "cursor": True,
                "format_in": "%Y-%m-%dT%H:%M:%S.%fZ",
                "type_in": "datetime",
            }
        }

        poller = create_poller(poller_name="okta", config=poller_config, schema=OrderedDict(poller_schema))

        total_records = 0
        temp_dir, temp_file = DriverNode.get_temp_files(self.source, self.sourcetype, self.pipeline_run_id)
        with open(temp_file, 'a') as f:

            # execute poller to fetch remote data
            for data, cursor in poller.get_data():
                logger.info(
                    f"pipeline_run_id={self.data_source_obj.pipeline_run_id} " +
                    f"message=collected {len(data)} records from API, cursor is {cursor}"
                )
                total_records += len(data)

                # no data recieved
                if not data:
                    continue

                # write content to local driver node
                try:
                    f.write(json.dumps(data))
                except Exception as exc:
                    raise Exception({exc}) from exc

        # log if no records received
        t_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        if not total_records:
            logger.debug(
                f"pipeline_run_id={self.pipeline_run_id} message=no records collected using cursor: {self.cursor}")

        # move to object storage so we can create DataFrame
        dbfs_dir, dest_file = DriverNode.get_dbfs_files(self.scratch_dir, self.source, self.sourcetype, t_now)
        DriverNode.move_files_to_dbfs(dbfs_dir, temp_file, dest_file, self.pipeline_run_id)

        # create the dataframe
        spark_dir = os.path.join('dbfs:/', self.scratch_dir, 'connectors', self.source, self.sourcetype, 'runs', t_now)
        df = Connector(self.spark, self.data_source_obj).create_dataframe_from_json(spark_dir, self.streaming_mode,
                                                                                    self.schema)

        # remove temp file & directory from driver
        DriverNode.remove_temp_files(temp_dir, self.pipeline_run_id)

        # set the datasourceObj with last cursor information
        self.data_source_obj.connector_status = {
            "pipeline_run_id": self.pipeline_run_id,
            "last_fetched": t_now,
            "fetched_records": total_records,
            "cursor": cursor
        }

        return df
