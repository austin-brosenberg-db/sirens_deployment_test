from pyspark.sql import DataFrame, SparkSession

from . import plugins
from .base_reader import BaseFileReader
from ..datasource import DataSource


@plugins.register
class Reader(BaseFileReader):
    """Class used to read JSON data
    """

    def __init__(self, spark: SparkSession, data_source_obj: DataSource) -> None:
        super().__init__(spark, data_source_obj, "json")

    @plugins.register
    def read(self) -> DataFrame:
        """Reads the data using spark read or readStream passing any options defined in the options key in inputs.yaml

        :raises Exception: for any spark errors reading the file(s)
        :return: pyspark DataFrame
        :rtype: DataFrame
        """
        return super()._read()
