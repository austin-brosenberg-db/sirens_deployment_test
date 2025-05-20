from typing import Optional, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .enrichment import EnrichmentBase

IANA_PORTS_TABLE = "iana_ports"

PORT_NUMBER_COLUMN = "port_number"

SERVICE_NAME_COLUMN = "service_name"

PORTS_SOURCE_URL = "https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.csv"


class IanaPortEnrichment(EnrichmentBase):
    """
    Enriches dataframe using the IANA port classification obtained from
    https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
    """

    def __init__(self, port_column: str, dest_name_column: Optional[str] = None,
                 iana_ports_df: Optional[DataFrame] = None, table_name: str = IANA_PORTS_TABLE,
                 cache: bool = True, protocol: str = "tcp", additional_columns: Optional[List[str]] = None):
        """
        Creates an object that will perform enrichment of a DataFrame with IANA ports information
        :param port_column: name of the port column in the table to enrich
        :param dest_name_column: optional column name for service name
        :param iana_ports_df: DataFrame with IANA ports information (primarily used for testing or sharing the same
            DataFrame between multiple enrichers).  Could be obtained with `get_iana_ports_df` or by direct read
        :param table_name: optional table name to read IANA ports information from
        :param cache: if we should cache read data
        :param protocol: what protocol should be used for enrichment. Default: `tcp`
        :param additional_columns: optional list of columns to add to the resulting DataFrame
        """
        super().__init__("iana_ports")
        if additional_columns is None:
            additional_columns = []
        if iana_ports_df:
            self._ports_df = iana_ports_df
        elif table_name:
            self._ports_df = SparkSession.getActiveSession().read.table(table_name)
        else:
            raise Exception("Please provide either iana_ports_df or table_name parameters")
        self._ports_df = self._ports_df.filter(f"protocol = '{protocol}'") \
            .select(SERVICE_NAME_COLUMN, PORT_NUMBER_COLUMN, *additional_columns)

        if cache:
            self._ports_df = self._ports_df.cache()
        self._port_column = port_column
        self._dest_name_column = dest_name_column

    def enrich(self, df: DataFrame) -> DataFrame:
        joined = df.join(F.broadcast(self._ports_df),
                         df[self._port_column] == self._ports_df[PORT_NUMBER_COLUMN], 'left')
        joined = joined.drop(self._ports_df[PORT_NUMBER_COLUMN])
        if self._dest_name_column:
            joined = joined.withColumnRenamed(SERVICE_NAME_COLUMN, self._dest_name_column)
        return joined

    @staticmethod
    def get_iana_ports_df(source_file: str):
        f"""
        Reads CSV data with IANA ports assignment from a given file
        :param source_file: path to file downloaded from {PORTS_SOURCE_URL}
        :return:
        """
        csv_schema = (f"{SERVICE_NAME_COLUMN} string, {PORT_NUMBER_COLUMN} int, protocol string, description string, "
                      "assignee string, contact string, registered date, modified date, "
                      "reference string, service_code long, unauth_use_reported string, notes string"
                      )
        df = SparkSession.getActiveSession().read.csv(source_file, schema=csv_schema, header=True,
                                                      enforceSchema=True)
        df = df.filter(f"{PORT_NUMBER_COLUMN} is not null and {SERVICE_NAME_COLUMN} <> 'www' "
                       f"and {SERVICE_NAME_COLUMN} <> 'www-http'")
        return df

    @staticmethod
    def import_port_definitions(source_file: str, table_name: str = IANA_PORTS_TABLE,
                                table_format: str = "delta", table_path: Optional[str] = None):
        f"""
        Imports IANA Ports definition from a given CSV file into a table
        :param table_name: name of the table
        :param source_file: path to file downloaded from {PORTS_SOURCE_URL}
        :param table_format: which file format to use for a table (default: `delta`)
        :param table_path: optional path to store the table with IANA ports information
        :return: noting
        """
        df = IanaPortEnrichment.get_iana_ports_df(source_file)
        writer = df.write.format(table_format)
        if table_path:
            writer = writer.option("path", table_path)
        writer.mode("overwrite").saveAsTable(table_name)
