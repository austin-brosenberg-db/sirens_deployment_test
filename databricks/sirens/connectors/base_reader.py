from pyspark.sql import DataFrame

from databricks.sirens.datasource import DataSource
from databricks.sirens.utils.base_utils import *
from databricks.sirens.exceptions import SirensConnectorException
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class BaseReader:
    """Class used to read data from file sources
    """
    def __init__(self, spark: SparkSession, data_source_obj: DataSource, fmt: str,
                 check_stream_type: bool = True) -> None:
        self.data_source_obj = data_source_obj
        self.spark = spark
        self.connector_opts = self.data_source_obj.get_connector_opts()
        self.stream_mode = self.data_source_obj.data_source_config.get("input").get("streamType").lower()
        if check_stream_type and self.stream_mode not in ['batch', 'streaming']:
            raise SirensConfigException(f"unsupported stream_mode: {self.stream_mode}")
        self.schema = None
        self.schema_name = self.data_source_obj.get_schema_file_opt()
        if self.schema_name:
            self.schema = self.data_source_obj.read_schema_file(self.schema_name)

        self.format = fmt
        self.path: Optional[str] = None

    def _read(self) -> DataFrame:
        """Reads the data using spark read or readStream passing any options defined in the options key in inputs.yaml

        :raises Exception: for any spark errors reading the file(s)
        :return: pyspark DataFrame
        :rtype: DataFrame
        """
        if self.stream_mode == 'batch':
            rdr = self.spark.read
        else:
            rdr = self.spark.readStream
        logger.info(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message=Starting collection run")
        logger.debug(f"pipeline_run_id={self.data_source_obj.pipeline_run_id} message={self.path} with "
                     f"opts: {self.connector_opts}")
        if self.connector_opts:
            rdr = rdr.options(**self.connector_opts)

        return rdr.load(path=self.path, schema=self.schema, format=self.format)


class BaseFileReader(BaseReader):
    """Class used to read data from file sources
    """
    def __init__(self, spark: SparkSession, data_source_obj: DataSource, fmt: str,
                 check_stream_type: bool = True) -> None:
        super().__init__(spark, data_source_obj, fmt, check_stream_type)
        self.path = BaseUtils.maybe_get_local_file_path(self.data_source_obj.get_connector_path())
        if not self.path:
            raise SirensConnectorException(f"path isn't provided in {self.data_source_obj.pipeline_run_id}")
