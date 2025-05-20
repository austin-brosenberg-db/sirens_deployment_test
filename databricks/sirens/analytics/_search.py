""" Private module to the public version search.py - that implements the logic for all search / analytics functions
    for Sirens.

    Author: Derek King
    Dated: June 2024
    Version: 1.0
    """
from databricks.sirens.common.entities import OutputFormat, ThreatIntel
from databricks.sirens.common.outputs import AnalysisOutput
from pyspark.sql import DataFrame, Column, SparkSession
from databricks.sirens.enrichments import DataFrameEnrichment
from databricks.sirens.analytics.transpilers import Splunk
from databricks.sirens.logging import get_logger
from typing import Union
import pyspark.sql.functions as F
logger = get_logger(__name__)

spark = SparkSession.getActiveSession()

def _ioc_hits(df: DataFrame,
            indicator_column: Column,
            matches_only: bool = False,
            ioc_type: str = None,
            target_intel_column: Column = None,
            schema: str = ThreatIntel.DATABASE, 
            intel_table: str=ThreatIntel.INTEL_TABLE,
            output_format: OutputFormat = OutputFormat.asDataFrame):
    try:
        catalog_schema = schema + "." + intel_table
        target_col = target_intel_column if target_intel_column else indicator_column
        join_type = "inner" if matches_only else "left"

        enrichment_df = spark.read.table(catalog_schema)
        if ioc_type:
            match_filter = f"type == '{ioc_type.value}'"
            enrichment_df = DataFrameEnrichment.filter_rows(df=enrichment_df, expression=match_filter)

        df = DataFrameEnrichment.join(left_df=df, right_df=enrichment_df, 
                                    source_col=indicator_column, target_col=target_col,
                                    join_type=join_type
                                    )
        return df
    except Exception as exc:
        logger.warning(f"message={exc}")
        return df


def _first_time_seen(df: DataFrame, group_by: list, baseline: str, time_column: str = "_event_time",
                        allow_list: Union[DataFrame, str] = None,
                        output_format: OutputFormat = OutputFormat.asDataFrame) -> AnalysisOutput:

        # create a baseline for the given dataset
        df = Splunk.stats(df, aggregate_functions=[f"min({time_column}) AS earliest", f"max({time_column}) AS latest"],
                  group_by=[*group_by])

        # input the lookup cache
        df = Splunk.inputlookup(schema=baseline, df=df)

        # recalculate the baseline
        df = Splunk.stats(df, aggregate_functions=["min(earliest) AS earliest", "max(latest) AS latest"], 
                   group_by=[*group_by])

        # update the cached baseline
        Splunk.outputlookup(df=df, schema=baseline)

        # add outliers
        df = Splunk.eval(df, "isOutlier", 'CASE WHEN earliest > now() THEN 1 ELSE 0 end')

        # remove allow_list or exclusion list
        # TODO - remove DataFrame isinstance test for utils.is_spark_dataframe()
        if allow_list:
            if isinstance(allow_list, DataFrame):
                print("Removing allow_list entries using DataFrame - NOT IMPLEMENTED YET")
            else:
                print("Removing allow_list entries using LOOKUP - NOT IMPLEMENTED YET")

        # return outliers
        df = df.filter(F.expr("isOutlier == 1"))

        return df


def _time_series_spike(df, bucket_span: str, aggregate_functions: list, group_by: list,
                        time_column: str = "_event_time", allow_list: Union[DataFrame, str] = None,
                        stddev_multiplier: int = 2,
                        output_format: OutputFormat = OutputFormat.asDataFrame) -> AnalysisOutput:
    
    # TODO - aggregate functions should always be equiv to dc(<FIELD>) - should the arg just be a field name, 
    # or are there other cases like, "dc(<field>) as count, avg(<field>) as avg" for timeseries spike analysis... ?!?!

    # bucket into time period
    df = Splunk.bucket(df, bucket_span, time_column)
    
    # add the bucket_span period into groupBy arg
    group_by.append(bucket_span) # ---- shouldn't this be time_column (not 'day' -- need to check - think there are variations)
    # may need to make changes here - different analysis techniques may / may not use this new column..... 

    # create baseline
    df = Splunk.stats(df, aggregate_functions=aggregate_functions, group_by=group_by)

    # create statistics
    df = Splunk.stats(df, 
        aggregate_functions=[f"max(case when {bucket_span} >= (now() + INTERVAL 1 {bucket_span}) then count else null end) AS latest",
                                f"avg(case when {bucket_span} < (now() + INTERVAL - 1 {bucket_span}) then count else null end) AS average",
                                f"stddev(case when {bucket_span} < (now() + INTERVAL - 1 {bucket_span}) then count else null end) AS stddev"],
        group_by=group_by)
    
    # remove allow_list or exclusion list
    if allow_list:
        if isinstance(allow_list, DataFrame):
            print("Removing allow_list entries using DataFrame - NOT IMPLEMENTED YET")
        else:
            print("Removing allow_list entries using LOOKUP - NOT IMPLEMENTED YET")
    
    # return outliers
    df = df.filter(F.expr("latest > ((stddev_multiplier * stddev) + average)"))

    return df