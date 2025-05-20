import inspect
import os
from itertools import chain
from typing import Literal, List, Dict, Optional

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import lit, expr, broadcast, create_map
from pyspark.sql.utils import AnalysisException

import databricks.sirens.pipeline.processors as processors
from databricks.sirens._version import ROOT_DIR
from databricks.sirens.config_reader import EnrichmentReader
from databricks.sirens.datasource import DataSource
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class DataFrameEnrichment:

    @staticmethod
    def filter_output_cols(df: DataFrame, output_cols: list, source_col: Optional[List[Column]] = None,
                           target_col: Optional[Column] = None) -> DataFrame:
        """given a list of output columns, return a dataframe that includes only those listed

        :param df: _description_
        :type df: DataFrame
        :param output_cols: _description_
        :type output_cols: list
        :param source_col: _description_, defaults to None
        :type source_col: Column, optional
        :param target_col: _description_, defaults to None
        :type target_col: Column, optional
        :return: _description_
        :rtype: DataFrame
        """
        if source_col is not None:
            [output_cols.append(col) for col in source_col]
        if target_col is not None:
            [output_cols.append(col) for col in target_col]

        # remove items in output_cols not in the dataframe
        # [output_cols.remove(x) for x in output_cols if x not in cols_in_df]
        cols_in_df = df.columns
        [x.lower() for x in cols_in_df]
        for x in output_cols:
            if x.lower() not in cols_in_df:
                logger.warning(
                    f"removing column ({x}) as it does not appear in the enrichment dataframe - check enrichments.yaml config.")
                output_cols.remove(x)

        df = df.select(*output_cols)

        return df

    @staticmethod
    def filter_rows(df: DataFrame, expression: str) -> DataFrame:
        """filter dataframe rows using the given expression

        :param df: dataframe to be filtered
        :type df: DataFrame
        :param expression: sql expression
        :type expression: str
        :return: filtered dataframe
        :rtype: DataFrame
        """
        try:
            logger.info("pre-filtering enrichment dataframe")
            logger.debug(f"filter expression: {expression}")
            df = df.filter(expression)
        except Exception as exc:
            logger.warning({exc}, exc_info=True)

        return df

    @staticmethod
    def join(left_df: DataFrame, right_df: DataFrame, source_col: list, target_col: Optional[Column] = None,
             join_type: str = "left", join_expr: Optional[str] = None,
             output_cols: Optional[list] = None, post_filter_expression: Optional[str] = None,
             broadcast_df: bool = False):

        # Limitation -> currently only handles a single column join criteria.
        # Need to figure how to pass a sql expression, with or without the user having to specify the left & right dataframes.

        SUPPORTED_JOINS = ["left", "inner", "right", "outer"]

        if join_type not in SUPPORTED_JOINS:
            logger.warning(f"{join_type} join not supported configure one of: {SUPPORTED_JOINS}")
            return left_df

        # TODO - how do we identify columns if advanced join_type is specified???
        # need to write parser
        # Handle join_expr - here.
        # needs to override source & target column.
        if join_expr is not None:
            logger.warning(f"join expression: {join_expr} - not yet coded.")

        # If output columns specified, select only the right side columns
        # Include any columns needed to perform the join
        if output_cols is not None:
            logger.debug("filtering enrichment output fields")
            right_df = DataFrameEnrichment.filter_output_cols(df=right_df, output_cols=output_cols,
                                                              source_col=source_col, target_col=target_col)

        # create the join expression
        def normalize_cols(cols):
            if not cols: return []
            if not isinstance(cols, List):
                return [cols]
            return cols

        def create_join_condition(left_cols, right_cols, df_left, df_right):
            if len(left_cols) != len(right_cols):
                raise ValueError("The number of columns in left_cols and right_cols must be the same")

            conditions = []
            for l_col, r_col in zip(left_cols, right_cols):
                conditions.append(df_left[l_col] == df_right[r_col])

            # Combine all conditions with & operator
            final_condition = conditions[0]
            for condition in conditions[1:]:
                final_condition = final_condition & condition
            return final_condition

        target_col = normalize_cols(target_col)
        source_col = normalize_cols(source_col)
        if target_col and source_col != target_col:
            exp = create_join_condition(source_col, target_col, left_df,
                                        right_df)  #left_df[source_col] == right_df[target_col]
        else:
            exp = source_col

        # Execute the Join Operation...
        if post_filter_expression is not None:
            logger.debug(f'applying post_filter: {post_filter_expression}')
            if not broadcast_df:
                logger.debug('broadcast = false')
                result_df = left_df.join(right_df, exp, join_type).filter(expr(post_filter_expression))
            else:
                logger.debug('broadcast = true')
                logger.warning('spark generally handles broadcasts automatically. Care should be taken not to '
                               'generate OOM errors using broadcast joins.')
                result_df = left_df.join(broadcast(right_df), exp, join_type).filter(expr(post_filter_expression))
        else:
            if not broadcast_df:
                logger.debug('broadcast = false')
                result_df = left_df.join(right_df, exp, join_type)
            else:
                logger.debug('broadcast = true')
                logger.warning('spark generally handles broadcasts automatically. Care should be taken not to '
                               'generate OOM errors using broadcasts joins.')
                result_df = left_df.join(broadcast(right_df), exp, join_type)

        return result_df


class Enrichment:
    """API into enrichments
    """

    def __init__(self, data_source_obj: DataSource):
        self.data_source_obj = data_source_obj
        self.config_dir = GlobalConfig.get_input_config_dir(GlobalConfig.get())
        self.conf_dir = GlobalConfig.get_configs_default_dir(GlobalConfig.get())

    @staticmethod
    def _get_enrichment_config(running_config):
        defs = {'table': running_config.get("table"), 'file': running_config.get("file"),
                'source_column': running_config.get("source_column"),
                'target_column': running_config.get("target_column"),
                'join_type': running_config.get("join_type", "left"),
                'broadcast_df': running_config.get("broadcast", False),
                'delimiter': running_config.get("delimiter", ","), 'filters': running_config.get('filters'),
                'defined_at': running_config.get("defined_at"), 'output_columns': None, 'pre_filter': None,
                'post_filter': None
                }
        if defs.get('filters'):
            defs['output_columns'] = defs['filters'].get("output_columns")
            defs['pre_filter'] = defs['filters'].get("prefilter")
            defs['post_filter'] = defs['filters'].get("postfilter")

        if defs.get('source_column') and isinstance(defs['source_column'], str):
            defs['source_column'] = defs.get("source_column").replace(' ', '').split(',')

        if defs.get('target_column') and isinstance(defs['target_column'], str):
            defs['target_column'] = defs.get('target_column').replace(' ', '').split(',')

        return defs

    @staticmethod
    def _get_matching_enrichment(enrichment_list, enrichment_name):
        try:
            for x in enrichment_list:
                if x['name'] == enrichment_name:
                    return x
        except Exception as exc:
            logger.info(f"Cannot find enrichment named: {enrichment_name}: {exc}")

        return None

    def _get_running_config(self, enrichment: str) -> dict:
        """return the most specific enrichment config.

        :param enrichment: name or enrichment
        :type enrichment: str
        :return: config from either system or content pack. (content pack is more specific)
        :rtype: list[dict]
        """
        defs, running_config = {}, {}

        # look for enrichment in system definition in conf/enrichments directory
        logger.debug(f"looking for enrichment: {enrichment} in system files")
        defs = Enrichment._select_enrichment_definition(self, enrichment, 'system')
        if defs:
            running_config = defs

        # be more specific if not found in system - look at content pack directory
        logger.debug(f"looking for enrichment: {enrichment} in the content pack - and overwriting definition if found.")
        defs = Enrichment._select_enrichment_definition(self, enrichment, 'content_pack')
        if defs:
            running_config = defs
        # Read enrichments from enrichments.yaml
        if not defs:
            running_config = Enrichment._find_enrichment_config(enrichment_name=enrichment,
                                                                source=self.data_source_obj.source,
                                                                sourcetype=self.data_source_obj.sourcetype)

        if running_config:
            running_config = Enrichment._get_enrichment_config(running_config)

        return running_config

    @staticmethod
    def _find_enrichment_config(enrichment_name: str, source, sourcetype) -> dict:
        """return the most specific enrichment config.

        :param enrichment_name: name or enrichment
        :type enrichment_name: str
        :return: config from either system or content pack. (content pack is more specific)
        :rtype: list[dict]
        """
        running_config = {}

        print(f"looking for enr: {enrichment_name}")
        enrichments = EnrichmentReader().read(source=source, sourcetype=sourcetype)
        logger.info(f'conf is: {enrichments}')

        if enrichments:
            enrichment_dict = Enrichment._get_matching_enrichment(enrichments, enrichment_name)
            if not enrichment_dict:
                logger.info(f"enrichment: {enrichment_name} not found in enrichments.yaml defs")
            else:
                running_config = Enrichment._get_enrichment_config(enrichment_dict)
        else:
            logger.info(f"enrichment: {enrichment_name} not found in enrichments.yaml defs")
            return {}

        return running_config

    @staticmethod
    def _is_valid_enrichment_config(enrichment, running_config):
        """check for valid enrichment config keys

        :param enrichment: enrichment name
        :type enrichment: _type_
        :param running_config: active running config for enrichment
        :type running_config: dict
        :return: valid or not
        :rtype: bool
        """

        is_valid = True

        # verify the definition type is either 'table' or 'file'
        if not running_config.get("file", None) and not running_config.get("table", None):
            logger.warning(f"bad config: {enrichment}. Should be defined as 'table' or 'file' - ignoring.")
            is_valid = False

        # bail out if both were configured for some reason.
        if running_config.get("file") and running_config.get("table"):
            logger.warning("bad config - file and table lookups defined - check config - ignoring.")
            is_valid = False

        return is_valid

    def _select_enrichment_definition(self, enrich_config: str, level: Literal["system", "content_pack"]) -> Dict:
        """looks for the matching enrichment definition in either system or content pack directory - marks in the dict
        if found in system or pack.

        :param enrich_config: _description_
        :type enrich_config: List
        :param level: _description_
        :type level: Literal[&quot;system&quot;, &quot;pack&quot;]
        :return: _description
        :rtype: Dict
        """
        enrichment_defs = {}
        # if cannot find the enrichment config on system level, read it from the content pack.
        if self.data_source_obj.enrichments:
            for i in self.data_source_obj.enrichments.get(level):
                if enrich_config in i['name']:
                    i['defined_at'] = level
                    enrichment_defs = i

        return enrichment_defs

    def _get_file_path(self, running_config: Dict) -> str:
        if running_config.get('defined_at') == "system":
            if "SIRENS_CONFIG" in os.environ:
                file_path = os.path.join(os.environ["SIRENS_CONFIG"], self.conf_dir,
                                         "enrichments", "data", running_config['file'] + '.csv')
            else:
                file_path = os.path.join(ROOT_DIR, self.conf_dir, "enrichments", "data",
                                         running_config['file'] + '.csv')
        else:
            if "SIRENS_CONFIG" in os.environ:
                file_path = os.path.join(os.environ["SIRENS_CONFIG"], self.config_dir, self.data_source_obj.source,
                                         self.data_source_obj.sourcetype, "data", running_config['file'] + '.csv')
            else:
                file_path = os.path.join(ROOT_DIR, self.config_dir, self.data_source_obj.source,
                                         self.data_source_obj.sourcetype, "data", running_config['file'] + '.csv')

        return file_path

    def _read_lookup_table(self, running_config: Dict) -> Dict:
        """loads the lookup table into spark session
        :param running_config: prioritized enrichment definition
        :rtype: None
        """
        result_dict = {}
        file_path = self._get_file_path(running_config)
        # Validate file location and readabilty
        logger.debug(f"looking for file in : {file_path}")
        if not os.path.isfile(file_path) and not os.access(file_path, os.R_OK):
            logger.warning(f"file: {file_path} is not readable. - ignoring enrichment: {running_config['file']}")
        else:
            logger.debug("reading file into dataframe")
            df = pd.read_csv(file_path, sep=running_config['delimiter'])
            key_col = running_config["source_column"][0]
            val_col = running_config["target_column"][0]
            result_dict = df.set_index(key_col)[val_col].to_dict()
        logger.debug(f"returning {result_dict}")
        return result_dict

    def _by_file(self, df: DataFrame, running_config: Dict) -> DataFrame:
        """performs the join using a csv file as the enrichment source

        :param df: left dataframe
        :type df: DataFrame
        :param running_config: prioritized enrichment definition
        :type running_config: Dict
        :return: enriched dataframe
        :rtype: DataFrame
        """

        logger.debug(f"csv enrichment file name: {running_config['file']}")

        # Set file path for either system or config_dir
        file_path = self._get_file_path(running_config)

        # Validate file location and readabilty
        logger.debug(f"looking for file in : {file_path}")
        if not os.path.isfile(file_path) and not os.access(file_path, os.R_OK):
            logger.warning(f"file: {file_path} is not readable. - ignoring enrichment: {running_config['file']}")
        else:
            logger.debug("reading file into dataframe")
            csv_dataframe = pd.read_csv(file_path, sep=running_config['delimiter'])
            cols = list(csv_dataframe.columns)
            enrichment_df = SparkSession.getActiveSession().createDataFrame(csv_dataframe, cols)

            # filter out unwanted rows if specified.
            if running_config.get('pre_filter'):
                logger.debug(f"filtering rows using filter: {running_config['pre_filter']}")
                enrichment_df = DataFrameEnrichment.filter_rows(enrichment_df, running_config['pre_filter'])

            # Perform the JOIN operation
            df = DataFrameEnrichment.join(left_df=df, right_df=enrichment_df,
                                          source_col=running_config['source_column'],
                                          target_col=running_config['target_column'],
                                          join_type=running_config['join_type'],
                                          output_cols=running_config['output_columns'],
                                          post_filter_expression=running_config['post_filter'],
                                          broadcast_df=running_config['broadcast_df'])

        return df

    @staticmethod
    def _by_delta_table(df: DataFrame, running_config: Dict) -> DataFrame:
        """performs the join using a delta table as the source

        :param df: left dataframe
        :type df: DataFrame
        :param running_config: prioritized enrichment definition
        :type running_config: Dict
        :return: enriched dataframe
        :rtype: DataFrame
        """

        logger.debug(f"enriching with delta table: {running_config['table']}")

        # read delta table - ignore if not exists error
        try:
            enrichment_df = SparkSession.getActiveSession().read.table(running_config['table'])
        except AnalysisException:
            logger.warning(f": {running_config['table']} does not exist. - ignoring..", exc_info=True)
            return df

        # If the enrichment table should be pre-filtered
        if running_config['pre_filter']:
            enrichment_df = DataFrameEnrichment.filter_rows(enrichment_df, running_config['pre_filter'])

        # perform the JOIN operation
        df = DataFrameEnrichment.join(left_df=df, right_df=enrichment_df, source_col=running_config['source_column'],
                                      target_col=running_config['target_column'],
                                      join_type=running_config['join_type'],
                                      output_cols=running_config['output_columns'],
                                      post_filter_expression=running_config['post_filter'],
                                      broadcast_df=running_config['broadcast_df'])

        return df

    @staticmethod
    def sort_cols_alpha(df: DataFrame) -> DataFrame:
        """return a dataframe sorted by column name alphabetically

        :param df: dataframe to sort
        :type df: DataFrame
        :return: sorted dataframe
        :rtype: DataFrame
        """
        cols = df.columns
        cols.sort()
        return df.select(*cols)

    @staticmethod
    def _get_configured_stage_enrichments(data_source_obj: DataSource, stage: str,
                                          target_table: Optional[str] = None) -> Dict:
        """return the dataSourceObj transforms/stage enrichments key as a dict (from inputs.yaml)

        :param data_source_obj: dataSourceObj
        :type data_source_obj: object
        :param stage: bronze|silver|gold
        :type stage: str
        :return: the names of any enrichments to be executed at the called stage of ETL pipeline
        :rtype: Dict
        """
        enrichments = {}
        try:
            if not target_table:
                enrichments = data_source_obj.data_source_config.get("transforms").get(stage).get("enrichments")
            else:
                event_types = data_source_obj.data_source_config.get("transforms").get(stage).get("event_type")
                for et in event_types:
                    if et.get('target_table') == target_table:
                        enrichments = et.get('enrichments')

        except Exception:
            logger.info(f"no enrichments configured for stage: {stage}")

        return enrichments

    def _enrich_by_stage(self, df: DataFrame, stage: str, target_table: Optional[str] = None) -> DataFrame:
        """ETL enrichment function that processes enrichments defined in inputs.yaml transformations/stage section

        :param df: _description_
        :type df: DataFrame
        :param stage: _description_
        :type stage: str
        :return: _description_
        :rtype: DataFrame
        """

        # Get the stage config (if there is one configured)
        enrich_config = Enrichment._get_configured_stage_enrichments(self.data_source_obj, stage, target_table)
        if not enrich_config:
            logger.warning(f"no enrichment config in inputs.yaml for stage: {stage}")
            return df

        # Process each named enrichment defined at the stage.(bronze,silver etc).
        for enrichment in enrich_config:
            if not target_table:
                logger.info(f"processing enrichment: {enrichment}, stage: {stage}")
            else:
                logger.info(f"processing enrichment: {enrichment}, stage: {stage}, target_table: {target_table}")

            # get the enrichment from system or current pack (by precedence level) as the running config
            running_config = Enrichment._get_running_config(self, enrichment)
            if not running_config:
                # if a definition can't be found in either system or log source dir, then ignore/don't process anything.
                logger.warning(f"enrichment: {enrichment} not found in system or content_pack - ignoring.")
                continue

            logger.debug(f"resulting config to use: {running_config}")

            # validate the keys for the enrichment (file or table)
            valid_config = Enrichment._is_valid_enrichment_config(enrichment, running_config)
            if not valid_config:
                continue

            if running_config.get('join_type') == "udf":
                continue

            if running_config.get("file"):  # csv style lookup.
                logger.debug("is file based enrichment")
                df = self._by_file(df, running_config)
            elif running_config.get("table"):  # delta table lookup
                logger.debug("is deltaTable enrichment")
                df = self._by_delta_table(df, running_config)

        df = Enrichment.sort_cols_alpha(df)
        return df

    def _enrich_by_name(self, enrichment: str, left_df: DataFrame, output_cols: Optional[list] = None,
                        pre_filter: Optional[str] = None, post_filter: Optional[str] = None) -> DataFrame:
        """enrich a dataframe with a named enrichment config defined in either in the content pack or system level

        :param enrichment: name of enrichment in enrichments.yaml
        :type enrichment: str
        :param left_df: main dataframe to enrich
        :type left_df: DataFrame
        :param output_cols: columns from the enrichment table, defaults to None
        :type output_cols: List, optional
        :return: enriched dataframe
        :rtype: DataFrame
        """
        df = left_df

        logger.info(f"processing enrichment: {enrichment}")

        # get the enrichment from system or current pack (by precedence level) as the running config
        running_config = Enrichment._find_enrichment_config(enrichment_name=enrichment,
                                                            source=self.data_source_obj.source,
                                                            sourcetype=self.data_source_obj.sourcetype)
        if not running_config:
            # if a definition can't be found in either system or log source dir, then ignore/don't process anything.
            logger.warning(f"enrichment: {enrichment} not found in system or content_pack - ignoring.")
            return left_df

        # over-write running config if specified as function input
        if output_cols:
            running_config['output_columns'] = output_cols

        if pre_filter:
            running_config['pre_filter'] = pre_filter

        if post_filter:
            running_config['post_filter'] = post_filter

        logger.debug(f"resulting config to use: {running_config}")

        # validate the keys for the enrichment (file or table)
        valid_config = Enrichment._is_valid_enrichment_config(enrichment, running_config)
        if not valid_config:
            return left_df

        # csv style lookup.
        if running_config.get("file"):
            logger.debug("is file based enrichment")
            df = self._by_file(df, running_config)

        # delta table lookup.
        elif running_config.get("table"):
            logger.debug("is deltaTable enrichment")
            df = self._by_delta_table(df, running_config)

        df = Enrichment.sort_cols_alpha(df)

        return df

    def add(self, enrichment: str, left_df: DataFrame, output_cols: Optional[list] = None,
            pre_filter: Optional[str] = None, post_filter: Optional[str] = None) -> DataFrame:
        """enrich a dataframe with a named enrichment config defined in either in the content pack or system level

        :param enrichment: name of enrichment in enrichments.yaml
        :type enrichment: str
        :param left_df: main dataframe to enrich
        :type left_df: DataFrame
        :param output_cols: columns from the enrichment table, defaults to None
        :type output_cols: List, optional
        :param pre_filter: pre-filter condition
        :param post_filter: post filter condition
        :return: enriched dataframe
        :rtype: DataFrame
        """

        return self._enrich_by_name(enrichment, left_df, output_cols, pre_filter, post_filter)

    def add_stage(self, df: DataFrame, stage: str, target_table: Optional[str] = None) -> DataFrame:
        """ETL enrichment function that processes enrichments defined in inputs.yaml transformations/stage section

        :param df: _description_
        :type df: DataFrame
        :param stage: _description_
        :type stage: str
        :param target_table: target table name, defaults to None
        :return: _description_
        :rtype: DataFrame
        """
        if stage not in ['bronze', 'silver']:
            logger.warning("invalid stage (bronze|silver) passed - ignoring.")
            return df

        if stage == 'bronze' and target_table:
            logger.warning("invalid options. bronze transforms do not have target tables (silver only) - ignoring.")
            return df

        return self._enrich_by_stage(df, stage, target_table)

    @staticmethod
    def _dict_to_map(d: dict):
        return create_map([lit(x) for x in chain(*d.items())])

    def register_udfs(self, stage: str, spark: SparkSession, df: DataFrame) -> None:
        """register udfs to spark session and populate df with columns of maps
        :param stage: _description_
        :type stage: str
        :param spark: spark session
        :type spark: SparkSession
        :param df: _description_
        :type df: DataFrame
        :return: the dataframe populated with map objects for lookup
        :rtype: DataFrame
        """
        lookup_dict = self.load_lookup(stage)
        for k, v in lookup_dict.items():
            df = df.withColumn(k, self._dict_to_map(v))
        for name, func in inspect.getmembers(processors, inspect.isfunction):
            spark.udf.register(name, func)

        return df

    def load_lookup(self, stage: str):
        """load lookup data as a python dictionary
        :param stage: _description_
        :type stage: str
        :return: the dictionary containing the mapping for enrichment
        :rtype: Dict
        """

        # Get the stage config (if there is one configured)
        enrich_config = Enrichment._get_configured_stage_enrichments(self.data_source_obj, stage)
        if not enrich_config:
            logger.warning(f"no enrichment config in inputs.yaml for stage: {stage}")
            return None
        lookup_table = dict()
        # Process each named enrichment defined at the stage.(bronze,silver etc).
        for enrichment in enrich_config:
            logger.info(f"processing enrichment: {enrichment}, stage: {stage}")
            
            # get the enrichment from system or current pack (by precedence level) as the running config
            running_config = Enrichment._get_running_config(self, enrichment)
            if not running_config:
                # if a definition can't be found in either system or log source dir, then ignore/don't process anything.
                logger.warning(f"enrichment: {enrichment} not found in system or content_pack - ignoring.")
                continue

            logger.debug(f"resulting config to use: {running_config}")

            # validate the keys for the enrichment (file or table)
            valid_config = Enrichment._is_valid_enrichment_config(enrichment, running_config)
            if not valid_config:
                continue

            # csv style lookup.
            if running_config.get("file") and running_config.get("join_type") == "udf" \
                and running_config.get("source_column") and running_config.get("target_column"):
                logger.debug("loading file based enrichment for pandas udf")
                lookup_config = self._read_lookup_table(running_config)
                if lookup_config:
                    lookup_table[running_config["file"]] = lookup_config

        self.lookup_dict = lookup_table
        return self.lookup_dict
