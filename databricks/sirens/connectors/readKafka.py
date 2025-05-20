from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr

from . import plugins
from .base_reader import BaseReader
from databricks.sirens.exceptions import SirensConfigException
from ..datasource import DataSource


@plugins.register
class Reader(BaseReader):
    """class used to read from Kafka sources
    """

    def __init__(self, spark: SparkSession, data_source_obj: DataSource):
        super().__init__(spark, data_source_obj, "kafka")
        if self.connector_opts is None or \
                "subscribe" not in self.connector_opts or \
                "kafka.bootstrap.servers" not in self.connector_opts:
            raise SirensConfigException(
                "Kafka options (subscribe and kafka.bootstrap.servers) as a minimum need to be set.")

    @plugins.register
    def read(self) -> DataFrame:
        """read from a kafka topic

        :return: raw DataFrame
        :rtype: DataFrame
        """
        return (self._read().withColumn("value", expr("string(value)"))
                .filter(col("value").isNotNull()))
