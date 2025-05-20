"""
User facing API functions for Sirens

Authors:
    Derek King August 2023


Functions:
    add_enrichment()
        augment a DataFrame using an existing enrichment configuration
    saved_aggregate():
        aggregate a DataFrame using an existing aggregation configuration

"""

from typing import Optional

from pyspark.sql import DataFrame

from databricks.sirens.aggregate import Aggregate
from databricks.sirens.config_reader import AggregateReader
from databricks.sirens.datasource import DataSource
from databricks.sirens.enrichments import Enrichment
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


def add_enrichment(left_df: DataFrame, enrichment: str, data_source_obj: DataSource,
                   output_cols: Optional[list] = None, pre_filter: Optional[str] = None,
                   post_filter: Optional[str] = None) -> DataFrame:
    """returns an enriched dataframe using an existing enrichment config -
    defined either in system enrichments or log_source enrichments

    :param left_df: DataFrame to be augmented
    :type left_df: DataFrame
    :param enrichment: name of enrichment defined in the yaml file
    :type enrichment: str
    :param data_source_obj: dataSource object
    :type data_source_obj: object
    :param output_cols: required cols from the enrichment table, defaults to None
    :type output_cols: List, optional
    :param pre_filter: sql expression filtering down rows in large enrichment tables before a join, defaults to None
    :type pre_filter: str, optional
    :param post_filter: sql expression to filter rows after join, defaults to None
    :type post_filter: str, optional
    :return: augmented DataFrame
    :rtype: DataFrame
    """
    logger.debug('executing add_enrichment function')
    df = Enrichment(data_source_obj).add(enrichment=enrichment, left_df=left_df,
                                         output_cols=output_cols,
                                         pre_filter=pre_filter,
                                         post_filter=post_filter)

    return df


def saved_aggregate(df: DataFrame, name: str, source: Optional[str] = None,
                    sourcetype: Optional[str] = None) -> DataFrame:
    """return an aggregated dataframe using an existing aggregation config -
    defined either in system aggregations or log_source aggregations

    :param df: incoming DataFrame
    :type df: DataFrame
    :param name: name of the configured aggregation. (denoted with '- name: xxx' in the aggregations.yaml file)
    :type name: str
    :param source: source of the log_source - i.e where to find a log_source defined aggregation, defaults to None
    :type source: str, optional
    :param sourcetype: sourcetype of the log_source - i.e where to find a log_source defined aggregation, defaults to None
    :type sourcetype: str, optional
    :return: aggregated DataFrame
    :rtype: DataFrame
    """
    aggregate_yaml_configs = AggregateReader().read(source, sourcetype)
    if not aggregate_yaml_configs:
        logger.error("aggregate configuration not found")
        return df

    aggregate_item = AggregateReader.get_aggregate(aggregate_yaml_configs, name)
    if not aggregate_item:
        logger.error("named aggregate item not found")
        return df

    df, status = Aggregate().pipeline_aggregation(df, aggregate_item)
    if not status:
        logger.info("aggregation failed - check logs")

    return df
