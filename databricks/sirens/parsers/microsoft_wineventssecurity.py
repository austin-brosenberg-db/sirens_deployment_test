import logging

from pyspark.sql.functions import explode, col, split, slice, expr, size, coalesce, lower, when, regexp_extract, element_at, concat, lit, array_remove, transform, trim
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import *
from pyspark.sql.functions import map_from_entries, pandas_udf, StringType, expr
import pandas as pd
from typing import Optional

from databricks.sirens.utils.base_utils import *
from databricks.sirens.datasource import DataSource, TimeStampInfo
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.parsers.base_parser import BaseParser

from .internal.microsoft_wineventssecurity_schemas import *
from .internal.microsoft_wineventssecurity_splunk_props import get_winevent_props
from . import plugins
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)

@plugins.register
class WinServerCommonParse(BaseParser):
    def __init__(self, spark):
        self.name = "Winserver Parser"
        self.spark = spark
        self.timestamp_col = "_raw_time"


    def toSilver(self, df: DataFrame, dataSourceObj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream
        :param df: Dataframe already processed for metadata (toBronze).
        :type df: DataFrame
        :param dataSourceObj: the datasource object
        :type dataSourceObj: object
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        from pyspark.sql.functions import from_xml

        self.df = df
        logger.info(f"pipeline_run_id={dataSourceObj.pipeline_run_id} message=silver transformations started")

        self.source, self.sourcetype = dataSourceObj.get_source_sourcetype_info()
        logger.info(f"source type {self.sourcetype}")

        try:
            df = self.df
            self.df = df.withColumn("parsed", from_xml(col("_raw"), EventType))\
                .selectExpr("*", "parsed.System.*", "parsed.EventData AS EventData")
            
            
            self.df = self.df.withColumn(
                "EventDataDct", 
                when(
                    col('EventData.Data').isNull(),
                    expr("map()")
                ).otherwise(
                    expr("""
                        map_from_entries(
                            transform(
                                filter(EventData.Data, x -> x['_Name'] IS NOT NULL),
                                x -> struct(x['_Name'] as key, x['_VALUE'] as value)
                            )
                        )
                    """)
                )
            )
            self.df = self.df.withColumns(get_winevent_props())
            #LOOKUP enrichment
            empty_array = lit([]).cast(ArrayType(StringType()))
            df_with_parsed_privileges = self.df.withColumn(
                "privilege_id", 
                when(
                    col('EventDataDct.PrivilegeList').isNull(),
                    empty_array
                ).otherwise(
                    array_remove(
                        transform(
                            split(col('EventDataDct.PrivilegeList'), '\\s+'),
                            lambda x: trim(x)
                        ),
                        ''
                    )
                )
            )
            # Explode the array column to get individual privileges
            # df_exploded = df_with_parsed_privileges.selectExpr("*", "explode_outer(privilege_id) as individual_privilege")
            # If you want to drop the intermediate array column and rename the exploded column
            # self.df = df_exploded.drop("privilege_id").withColumnRenamed("individual_privilege", "privilege_id")

            # self.df = self.df.withColumn("EventType",col("parsed.System.Level"))
            

            self.df = df_with_parsed_privileges.drop("_raw")
            logger.debug(f"pipeline_run_id={dataSourceObj.pipeline_run_id} message=silver transformation completed")
            return self.df
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc