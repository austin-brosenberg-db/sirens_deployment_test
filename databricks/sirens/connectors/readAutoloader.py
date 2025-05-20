from pyspark.sql import DataFrame

from databricks.sirens.utils.base_utils import *
from . import plugins
from .base_reader import BaseFileReader
from ..datasource import DataSource


@plugins.register
class Reader(BaseFileReader):
    """Class used to read data using autoloader capability
    """
    def __init__(self, spark: SparkSession, data_source_obj: DataSource) -> None:
        super().__init__(spark, data_source_obj, "cloudFiles", check_stream_type=False)
        self.stream_mode = "streaming"
        if self.connector_opts is None:
            self.connector_opts = {}
        self.schema_hints_file = self.data_source_obj.get_schema_hints_opt()
        if self.schema_hints_file is not None:
            self.connector_opts['cloudFiles.schemaHints'] = self.data_source_obj.read_schema_file(self.schema_hints_file)
        self.connector_name = self.data_source_obj.get_connector_name()
        if 'cloudFiles.format' not in self.connector_opts.keys():
            self.connector_opts['cloudFiles.format'] = self.connector_name

    @plugins.register
    def read(self) -> DataFrame:
        """Reads the data using spark read or readStream passing any options defined in the options key in inputs.yaml

        :raises Exception: for any spark errors reading the file(s)
        :return: pyspark DataFrame
        :rtype: DataFrame
        """
        return super()._read()
