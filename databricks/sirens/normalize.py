"""Implements the code responsible for normalizing a DataFrame
into into the format specified in inputs.yaml transforms key(s).

Transforms should follow the common information model for the target table
but may define any SQL expression for a target column/field name.

Author:
    Derek King (4th July 2022)

Classes:
    normalizer()

Functions:
    filter_frame()
    transform_frame()
"""

import re
from enum import Enum
import numpy as np
from typing import Dict, List, Tuple, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import lit, expr, col, struct

from databricks.sirens.datasource import DataSource
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.config_reader import CommonInformationModel, OpenCyberSecurityFramework
from databricks.sirens.exceptions import SirensNormalizeException
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class NormalizerAction(Enum):
    """Enum for the types of actions that can be performed by the Normalizer class
    """
    rename = "rename"
    new_expressions = "new_expressions"
    new_literals = "new_literals"
    alias = "alias"

class OCSFMapper:

    def __init__(self, spark, dataSourceObj: DataSource) -> None:

        self.spark = spark
        self.data_source = dataSourceObj

    def add_col(self, name: str, keys: dict):
        """
        Adds a column to a DataFrame based on the provided name and keys.

        :param name: The name of the column to be added. If the name contains dots, only the part after the last dot is used.
        :type name: str
        :param keys: A dictionary containing the type and value of the column to be added.
                     The dictionary should have the following structure:

                     - If 'type' is 'literal', 'value' should be the literal value to be added.
                     - If 'type' is 'expression', 'value' should be the expression to be evaluated.
                     - If 'type' is 'struct', 'value' should be a list of values to be included in the struct.
        :type keys: dict

        :return: A column expression that can be used to add the column to a DataFrame.
        :rtype: pyspark.sql.Column
        """
        name = name.split(".")[-1]

        if keys['type'] == 'literal':

            return lit(keys['value']).alias(name)
        elif keys['type'] == 'expression':

            return expr(keys['value']).alias(name)
        elif keys['type'] == 'struct':

            return struct(*keys['value']).alias(name)

    def append_alias(self, name: str, keys: dict) -> Column:
        """
        Appends an alias to a column name.
        This method takes a column name and a dictionary of keys, splits the column name
        to get the last part after a dot (if any), and then returns the column with an alias
        using the value from the keys dictionary.
        :param name: The name of the column, potentially including a dot-separated prefix.
        :type name: str
        :param keys: A dictionary containing the key 'value' which holds the alias name.
        :type keys: dict
        :return: A column with the alias applied.
        :rtype: pyspark.sql.Column
        """
        name = name.split('.')[-1]

        return col(keys['value']).alias(name)


    def transform_nested_cols(self, df: DataFrame, transforms: list[dict]) -> Tuple[DataFrame, List[str]]:
        """
        Transforms nested columns in a DataFrame based on a list of transformation instructions.
        This method processes nested structures within a DataFrame, applying specified transformations
        such as adding new columns or aliasing existing ones. It handles nested fields within structs
        and organizes them by their level of depth to ensure proper transformation order.
        :param df: The input DataFrame to be transformed.
        :type df: DataFrame
        :param transforms: A list of dictionaries where each dictionary specifies a transformation action
        and its corresponding details. The keys in the dictionary represent the column
        names, and the values are dictionaries containing the action ('add' or 'alias')
        and additional parameters such as 'type' and 'value'.
        :type transforms: list[dict]
        :return: A tuple containing the transformed DataFrame and a list of top-level struct column names.
        :rtype: Tuple[DataFrame, List[str]]
        """
        transform_functions = {"add": self.add_col, 'alias': self.append_alias}

        #all structs
        struct_fields = [av for av in [f for f in transforms if list(f.values())[0]['action'] == 'add'] if list(av.values())[0]['type'] == 'struct']

        #get all fields that are nested within structs
        nested_fields = [f for f in transforms if '.' in list(f.keys())[0]]

        top_level_structs = [list(s.keys())[0] for s in struct_fields if '.' not in list(s.keys())[0]]

        #get top level structs
        for top in top_level_structs:

            #all of the sub-structs
            nested_structs = [list(sf.keys())[0] for sf in struct_fields if list(sf.keys())[0].startswith(f"{top}.")]

            #everything that is a nested thing
            sub_fields = {list(n.keys())[0]:list(n.values())[0] for n in nested_fields if list(n.keys())[0].startswith(top)}

            #organize structs by their level depth
            level_depth = [len(nest.split('.')) for nest in nested_structs]
            level_map = {l:[] for l in np.unique(level_depth)}

            for l, x in zip(level_depth, nested_structs):
                level_map[l].append(x)

            #more than just top level fields
            if len(level_map) > 0:

                addressed_fields = []

                #start from deepest level and work up
                for l in range(max(list(level_map.keys())), 1, -1):

                    current_structures = {}
                    for structure in level_map[l]:

                        #all entries in current structure
                        struct_entries = {k:v for k, v in sub_fields.items() if f"{structure}." in k and k not in addressed_fields}

                        #make sure we don't repeat fields
                        addressed_fields += list(struct_entries.keys())

                        #for current substructure, process fields
                        fields = []
                        for k, v in struct_entries.items():

                            #assign substructure fields to value of struct
                            if "struct" in v.values() and k in substructures.keys():
                                v['value'] = substructures[k]

                            fields.append(transform_functions[v['action']](k, v))

                        current_structures[structure] = fields

                    #yesterday's structs are today's sub-structs
                    substructures = current_structures.copy()

                #take care of everything at the top level
                top_level_fields = list(set(list(sub_fields.keys())) - set(addressed_fields))
                top_level_entries = {k:v for k, v in sub_fields.items() if k in top_level_fields}

                fields = []
                for k, v in top_level_entries.items():

                    #assign substructure fields to value of struct
                    if "struct" in v.values() and k in substructures.keys():
                        v['value'] = substructures[k]

                    fields.append(transform_functions[v['action']](k, v))

                df = df.withColumn(top, struct(*fields))

            #no structs within structs
            else:

                fields = []
                for k, v in sub_fields.items():

                    fields.append(transform_functions[v['action']](k, v))

                df = df.withColumn(top, struct(*fields))

        return df, top_level_structs

class Normalizer:
    """Class to extract specific events from a bronze table, and transform columns to the databricks common information model
    """

    default_metafields = ['_event_date', '_event_time', '_source', '_sourcetype', 'dvc_hostname']

    def __init__(self, spark, dataSourceObj: DataSource) -> None:
        """class instance

        :param dataSourceObj: the data source object
        :type dataSourceObj: DataSource
        """
        self.spark = spark
        self.data_source = dataSourceObj
        self.database_name = self.data_source.databaseName
        self.ocsf_mapper = OCSFMapper(spark, dataSourceObj)


    @staticmethod
    def _fix_bad_transforms(transforms: List[Dict]):
        """transform the action from alias to rename if source and target cols are the same. Prevents duplicate cols
        errors.

        :param transforms: column transformations list
        :type transforms: List[Dict]
        :return: new altered list
        :rtype: _type_
        """
        for entry in transforms:
            for target_col in entry:
                action = entry[target_col].get('action')
                source_col = entry[target_col].get('value')
                if action == "alias" and (source_col == target_col):
                    logger.warning(f"incorrect transform action found. Should be rename not alias for col: "
                                   f"{target_col}. updating.")
                    entry[target_col]['action'] = 'rename'

        return transforms

    @staticmethod
    def _gen_transform_cols(transform_fields: List[Dict]) -> Tuple[List, List]:
        """
        Generate transformation columns for flat and structured fields.
        This function processes a list of transformation field dictionaries and categorizes them into flat and structured
        transformation columns. It ensures that default metafields are included if not specified, fixes any bad transforms,
        and categorizes each transformation based on its action.
        :param transform_fields: List of dictionaries where each dictionary represents a transformation field with its
                                 corresponding action and value.
        :type transform_fields: List[Dict]
        :return: A tuple containing two lists - the first list contains flat transformation columns and the second list
                 contains structured transformation columns.
        :rtype: Tuple[List, List]
        :raises SirensNormalizeException: If any error occurs during the processing of transformation fields.
        """

        transforms = transform_fields

        flat_transform_cols = []
        struct_transform_cols = []

        # Check the default metafields have been specified in transforms, and include them if not.
        try:
            configured_transforms = []
            [configured_transforms.extend(x.keys()) for x in transforms]
            missing = set(list(Normalizer.default_metafields)) - set(list(configured_transforms))
            if len(missing) > 0:
                logger.debug(f"Adding missing metafields: {missing} to field transformation list")
                [transforms.append({x: {'action': 'rename', 'value': x}}) for x in missing]
        except Exception as exc:
            raise SirensNormalizeException(f"{exc}") from exc

        # Fix any bad transforms likely to cause issues.
        transforms = Normalizer._fix_bad_transforms(transforms)

        try:
            for f in transforms:
                for k in f:
                    action = f[k].get('action')
                    if not action:
                        raise SirensNormalizeException(f"Error: action not defined for column: {k}")

                    # specifically for nested fields
                    if "." in k or "struct" in f[k].values():
                        struct_transform_cols.append(f)

                    elif action == "add":  # if adding a new column to the dataframe
                        if f[k].get('type') == "literal":
                            flat_transform_cols.append({"new": [k, f"LITERAL={f[k].get('value')}"]})

                        if f[k].get('type') == "expression":
                            flat_transform_cols.append({"new": [k, f"EXPR={f[k].get('value')}"]})

                    elif action == "rename":  # if renaming a column
                        flat_transform_cols.append({"rename": [k, f[k].get('value')]})

                    elif action == "alias":  # if creating a new aliased column from existing
                        flat_transform_cols.append({"alias": [k, f[k].get('value')]})

                    elif action == "copy":
                        flat_transform_cols.append({"rename": [k, k]})

                    else:
                        raise SirensNormalizeException(f"Error: unknown action is defined for column: {k}: '{action}'")
        except Exception as exc:
            raise SirensNormalizeException(f"{exc}") from exc

        return flat_transform_cols, struct_transform_cols

    def _cim_cols(self, df: DataFrame, transforms: List[list],
                  action: NormalizerAction) -> Tuple[DataFrame, List[str]]:
        """Make the requested transformations, and return a common information model formatted DataFrame

        :param df: bronze DataFrame
        :type df: DataFrame
        :param transforms: list of columns and transformations
        :type transforms: list
        :param action: rename, new_literal, new_expressions, alias
        :type action: str
        :return: _description_
        :rtype: DataFrame
        """
        if len(transforms) == 0:
            return df, []

        cols = []
        try:
            for column in transforms:
                if action == NormalizerAction.rename:
                    df = df.withColumnRenamed(column[1], column[0])
                elif action == NormalizerAction.new_literals:
                    df = df.withColumn(column[0], lit(column[1]))
                cols.append(column[0])

            if action == NormalizerAction.new_expressions:
                df = df.select("*", *[expr(x[1]).alias(x[0]) for x in transforms])
            elif action == NormalizerAction.alias:
                df = df.select("*", *[col(x[1]).alias(x[0]) for x in transforms])
        except Exception as exc:
            raise SirensNormalizeException(f"Error transforming frame: {exc}") from exc

        return df, cols

    @staticmethod
    def _create_missing_cols(df: DataFrame, columns: List[Union[list,tuple]]) -> DataFrame:
        """Create any missing columns, before applying transformations - stop Spark errors.

        :param df: DataFrame
        :type df: DataFrame
        :param columns: list of columns in the transformations list
        :type columns: list
        :return: extended DataFrame with missing cols added
        :rtype: DataFrame
        """
        try:
            # TODO: use actual null value here.
            cols_to_add = [lit('null').alias(col[1]) for col in columns if col[1] not in df.columns]
            if cols_to_add:
                df = df.select("*", *cols_to_add)
        except Exception as exc:
            raise SirensNormalizeException(f"{exc}") from exc

        return df

    @staticmethod
    def _create_missing_alias_source_cols(df: DataFrame, columns: list) -> DataFrame:
        """generate any missing source columns used for alias operations to prevent spark erroring

        :param df: incoming dataframe
        :type df: DataFrame
        :param columns: alias column list
        :type columns: list
        :return: transformed dataframe
        :rtype: DataFrame
        """
        df = df
        source_columns = []

        for x in columns:
            # if source & target are the same, and source doesn't exist - create a dummy col.
            # result should be the target col is null. (without erroring out).
            if x[0] == x[1]:
                x[1] = "x_" + x[1]

            source_columns.append(x[1])
        source_columns = list(set(source_columns))

        try:
            cols_to_add = [lit(None).cast("string").alias(x) for x in source_columns if x not in df.columns]
            if cols_to_add:
                df = df.select("*", *cols_to_add)
        except Exception as exc:
            raise SirensNormalizeException(f"{exc}") from exc

        return df

    def _transform_to_cim(self, df: DataFrame, flat_transforms: list[dict], struct_transforms: list[dict] = []) -> DataFrame:
        """main workhorse for transform_frame. Returns the creates and returns the common information model compliant DataFrame

        :param df: DataFrame to be processed
        :type df: DataFrame
        :param transforms: list of columns and transformations
        :type transforms: list
        :return: processed DataFrame
        :rtype: DataFrame
        """
        if not flat_transforms and not struct_transforms:
            return df

        cols_cimd = []
        new_expressions, new_literals, renames, alias = [], [], [], []

        try:
            # ocsf for transform in transforms:
            for transform in flat_transforms:
                for k, v in transform.items():
                    column, value = v[0], v[1]
                    if k == 'new':
                        if re.search(r"^EXPR(\s)?=", value):
                            new_expressions.append([column, value[5:]])
                        elif re.search(r"^LITERAL(\s)?=", value):
                            new_literals.append([column, value[8:]])
                    elif k == 'rename':
                        renames.append([column, value])
                    elif k == 'alias':
                        alias.append([column, value])
                    else:
                        raise SirensNormalizeException(f"ERROR: unknown action is defined for column: {column}: '{k}'")
        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensNormalizeException(f"ERROR: {exc}") from exc

        # Create any missing columns
        df = self._create_missing_cols(df, renames)

        # Add literal value columns
        df, cols_added = self._cim_cols(df, new_literals, NormalizerAction.new_literals)
        cols_cimd.extend(cols_added)

        # Rename columns
        df, renamed_cols = self._cim_cols(df, renames, NormalizerAction.rename)
        cols_cimd.extend(renamed_cols)

        # Create new columns by SQL expressions
        df, derived_cols = self._cim_cols(df, new_expressions, NormalizerAction.new_expressions)
        cols_cimd.extend(derived_cols)

        # Create field aliases
        df = self._create_missing_alias_source_cols(df, alias)
        df, aliased_cols = self._cim_cols(df, alias, NormalizerAction.alias)
        cols_cimd.extend(aliased_cols)

        # may be the case that there are no structs(cim)
        if len(struct_transforms) != 0:
            df, struct_cols = self.ocsf_mapper.transform_nested_cols(df, struct_transforms)
            cols_cimd.extend(struct_cols)

        cols_cimd.sort()

        return df.select(*cols_cimd)

    def filter_frame(self, df: DataFrame, target_table: str) -> DataFrame:
        """Returns a DataFrame filtered for events defined in the target_table (inputs.yaml)

        :param df: DataFrame to be processed
        :type df: DataFrame
        :param target_table: target common information model table (defined in inputs.yaml)
        :type target_table: str
        :return: filtered DataFrame
        :rtype: DataFrame
        """
        event_filter = self.data_source.get_event_filter(target_table)
        logger.info(f"pipeline_run_id={self.data_source.pipeline_run_id} message=starting filter for table {target_table}")

        # ensure there are no _invalid_timestamp records. these should not pollute a normalized table.
        # should be reprocessed from bronze.
        if '_invalid_timestamp' in df.columns:
            df = df.where(col("_invalid_timestamp").isNull())

        # No event_type filters to apply
        if event_filter:
            df = df.filter(expr(event_filter))
        return df

    def _cast_to_target_table_schema(self, df: DataFrame, target_table: str, framework: str, partition_cols: List[str]) -> DataFrame:
        """
        Casts the columns of the given DataFrame to match the schema of the target table.
        This method checks if the target table exists in the database. If it does, it retrieves the schema of the table.
        If the table does not exist, it reads the default schema for the specified framework and creates the table with
        that schema. The DataFrame columns are then cast to match the target schema to ensure compatibility for writing
        to a Delta table.
        :param df: The input DataFrame to be cast.
        :type df: DataFrame
        :param target_table: The name of the target table.
        :type target_table: str
        :param framework: The framework to use for schema reading, either "ocsf" or "cim".
        :type framework: str
        :param partition_cols: The columns to partition the table by.
        :type partition_cols: list
        :return: The DataFrame with columns cast to the target schema.
        :rtype: DataFrame
        """

        config_class = {"ocsf": OpenCyberSecurityFramework, "cim": CommonInformationModel}
        table_name = f"{target_table}_{framework}"

        # If the table already exists, then use the table to get the schema
        if BaseUtils._if_table_exists(self.spark, self.database_name, table_name):

            # _get_table_schema
            target_schema = BaseUtils._get_table_schema(self.spark, self.database_name, target_table)

            # check we got a schema - or fail.
            if not target_schema:
                # return the original frame without casting any columns. Possibly it works/possibly it does not..
                logger.warning(f"pipeline_run_id={self.data_source.pipeline_run_id} message=Getting target table "
                               "schema failed - possible issues writing delta tables.")
                return df
        else:
            # Table does not exist already - so read the default CIM table and create the table.
            target_schema = config_class[framework]().read(target_table)

            # if the table does not exist and we fail to read a default CIM file, then drop through and
            # allow the current dataframe column types to dictate the table.
            # Next read, should detect the table existence and use that as the schema.
            if target_schema is None:
                logger.warning(f"pipeline_run_id={self.data_source.pipeline_run_id} message=failed to read common inf "
                               f"table: {target_table}, {self.data_source.source}, {self.data_source.sourcetype} - possible write issues coming up")
                return df

            # create table with the default schema
            created_ok = BaseUtils._create_table(self.spark, self.database_name, table_name, partition_cols)
            if not created_ok:
                logger.warning(f"pipeline_run_id={self.data_source.pipeline_run_id} message=Failed to create table: "
                               f"{target_table} {self.data_source.source}, {self.data_source.sourcetype} with default CIM schema")

        # cast columns to target schema to try and ensure delta write compatibility
        casted_df = BaseUtils.cast_columns(target_schema, df)

        return casted_df

    # ocsf def transform_frame(self, df: DataFrame, target_table: str) -> DataFrame:
    def transform_frame(self, df: DataFrame, target_table: str, framework: str = None) -> DataFrame:
        """returns a transformed DataFrame, designed to be common information model compliant using transforms in inputs.yaml

        :param df: DataFrame to be processed
        :type df: DataFrame
        :param target_table: target common information model or ocsf table (defined in inputs.yaml)
        :type target_table: str
        :param framework: ocsf or cim schema (defined in inputs.yaml)
        :type framework: str
        :return: transformed silver cim based DataFrame
        :rtype: DataFrame
        """
        try:
            logger.info(f"pipeline_run_id={self.data_source.pipeline_run_id} "
                        f"message=starting frame transformation into {target_table}")

            if not framework:
                framework = self.data_source.get_framework(target_table=target_table)

            transform_fields = self.data_source.get_event_fields(target_table) or []

            flat_transform_cols, struct_transform_cols = self._gen_transform_cols(transform_fields)

            # add/transform cols necessary for common information model (CIM) table
            transformed_df = self._transform_to_cim(df, flat_transform_cols, struct_transform_cols)
            table_config = self.data_source.get_table_config(target_table)

            # here we need to get either the current target table, or the default from CIM
            # then cast column types to avoid delta write issues.
            transformed_df = self._cast_to_target_table_schema(transformed_df, target_table,
                                                                framework, table_config.get("partition_cols", []))

        except Exception as exc:
            logger.error(f"pipeline_run_id={self.data_source.pipeline_run_id} message={exc}")
            raise SirensNormalizeException(f"Error transforming frame: {exc}") from exc

        return transformed_df
