from typing import Literal, Optional, Union, List, Iterator, Tuple
from pyspark.sql import DataFrame, Column

from databricks.sirens.threathunting import _functions
from ._entities import OutputFormat, BackendDefaults

__all__ = ["list_hunt_library", "list_hunt_library_by_name", "add_row_numbers", "annotate",
           "mark", "filter_marked", "list_executed_hunts", "show_hunt_results", "get_hunt_result",
           "write_hunt_index", "write_hunt_results"]


def list_hunt_library(output_format: OutputFormat = OutputFormat.asObject) -> OutputFormat:
    """list all hunts configured threat hunts
        OutputFormat can be imported from databricks.sirens.threathunting.entities

    :param output_format: specify the output format as .asObject, asJSON, asDataFrame, defaults to OutputFormat.asObject
    :type output_format: OutputFormat, optional
    :return: a list of configured hunts
    :rtype: OutputFormat
    """
    return _functions._list_hunt_library(output_format=output_format)

def list_hunt_library_by_name(hunt_name: str, output_format: OutputFormat = OutputFormat.asObject) -> OutputFormat:
    """given a specific hunt name, return high level information about it. 
        OutputFormat can be imported from databricks.sirens.threathunting.entities

    :param hunt_name: name of hunt to return (i.e HNT_EXEC_T1047.000-WMIC-Usage)
    :type hunt_name: str
    :param output_format: specify the output format as .asObject, asJSON, asDataFrame, defaults to OutputFormat.asObject
    :type output_format: OutputFormat, optional
    :return: single hunt information
    :rtype: OutputFormat
    """
    return _functions._list_hunt_library(hunt_name=hunt_name, output_format=output_format)

def add_row_numbers(df: DataFrame, time_col: Column = '_event_time') -> DataFrame:
    """add a row number column to the dataframe for future selection/annotations

    :param df: incoming dataframe
    :type df: DataFrame
    :param time_col: column with the event_time, defaults to _event_time
    :type time_col: Column, optional
    :return: DataFrame with row column numbers included
    :rtype: DataFrame
    """
    return _functions._add_row_numbers(df=df, time_col=time_col)

def annotate(df: DataFrame, row_number: int, annotation: dict, overwrite_existing: bool = False) -> DataFrame:
    """add annotations to dataframe rows. (run `add_row_numbers` function to get row numbers to annotate)

    :param df: DataFrame to annotate
    :type df: DataFrame
    :param row_number: row number to be annotated
    :type row_number: int
    :param annotation: a key/value list of annotations (i.e {'key1':'value1', 'key2': 'value2'})
    :type annotation: dict
    :param overwrite_existing: when supplying a duplicate key, control whether to EXCEPTION or Overwrite, defaults to EXCEPTION
    :type overwrite_existing: bool, optional
    :return: Augmented DataFrame
    :rtype: DataFrame
    """
    return _functions._annotate(df=df, row_number=row_number, annotation=annotation,
                                overwrite_existing=overwrite_existing)

def mark(df: DataFrame, rows: str, toggle: bool = True) -> DataFrame:
    """mark a row as interesting. Useful when identifying potentially suspicious entries. (run `add_row_numbers` first)

    :param df: incoming DataFrame
    :type df: DataFrame
    :param rows: row numbers to mark, either as single integer, a range, or comma list. (example '1, 3,4 7-10')
    :type rows: str
    :param toggle: mark or unmark the row with '*', defaults to True
    :type toggle: bool, optional
    :return: augmented DataFrame
    :rtype: DataFrame
    """
    return _functions._mark(df=df, rows=rows, toggle=toggle)

def filter_marked(df: DataFrame) -> DataFrame:
    """filter the DataFrame for marked entries only

    :param df: incoming DataFrame
    :type df: DataFrame
    :return: Subset of DataFrame
    :rtype: DataFrame
    """

    return _functions._filter_marked(df=df)

def list_executed_hunts(hunt_name: str = None, time_start: str = None, time_end: str = None,
                        run_id: str = None,
                        status: Optional[Literal['initializing', 'running', 'finished']] = None,
                        state: Optional[Literal['initializing', 'active', 'paused', 'finished']] = None,
                        result: Optional[Literal['success', 'failed']] = None,
                        database: str = BackendDefaults.DATABASE,
                        hunt_index: str = BackendDefaults.INDEX_TABLE,
                        hunt_table: str = BackendDefaults.HUNT_TABLE,
                        output_format: OutputFormat = OutputFormat.asDataFrame) -> OutputFormat:
    """list all executed threat hunts that match the filter criteria in the index table.

    :param hunt_name: name of hunt to filter for, defaults to None
    :type hunt_name: str, optional
    :param time_start: date/time of the hunt start, defaults to None (ex: 01-13-2024 13:00:00)
    :type time_start: str, optional
    :param time_end: date/time of the hunt end, defaults to None (ex: 01-13-2024 14:00:00)
    :type time_end: str, optional
    :param run_id: run_id of the hunt, defaults to None
    :type run_id: str, optional
    :param status: a status string ('initializing', 'running', 'finished'), defaults to None
    :type status: Optional[Literal[&#39;initializing&#39;, &#39;running&#39;, &#39;finished&#39;]], optional
    :param state: when a threat hunt is executing the state will be updated as each cell executes, defaults to None
    :type state: Optional[Literal[&#39;initializing&#39;, &#39;active&#39;, &#39;paused&#39;, &#39;finished&#39;]], optional
    :param result: a hunt in finished state has its execution result updated, defaults to None
    :type result: Optional[Literal[&#39;success&#39;, &#39;failed&#39;]], optional
    :param database: _description_, defaults to BackendDefaults.DATABASE
    :type database: str, optional
    :param hunt_index: _description_, defaults to BackendDefaults.INDEX_TABLE
    :type hunt_index: str, optional
    :param hunt_table: _description_, defaults to BackendDefaults.HUNT_TABLE
    :type hunt_table: str, optional
    :return: OutputFormat??
    :rtype: DataFrame
    """

    return _functions._list_executed_hunts(hunt_name=hunt_name, time_start=time_start, time_end=time_end,
                                           run_id=run_id, status=status, state=state, result=result,
                                           database=database, hunt_index=hunt_index, hunt_table=hunt_table,
                                           output_format=output_format)

def show_hunt_results(hunt_name: str = None, time_start: str = None, time_end: str = None,
                      run_id: str = None, command_name: str = None,
                      database: str = BackendDefaults.DATABASE,
                      hunt_index: str = BackendDefaults.INDEX_TABLE,
                      hunt_table: str = BackendDefaults.HUNT_TABLE) -> Union[Iterator[Tuple[str, DataFrame]], None]:
    """filter the threat hunt index and display the results back as cell outputs.
       Note: Although all args are optional, you have to supply something!

       See get_hunt_results() if you want to subsequently work with the results as DataFrames

    :param hunt_name: name of hunt to filter for, defaults to None
    :type hunt_name: str, optional
    :param time_start: date/time of the hunt start, defaults to None (ex: 01-13-2024 13:00:00)
    :type time_start: str, optional
    :param time_end: date/time of the hunt end, defaults to None (ex: 01-13-2024 14:00:00)
    :type time_end: str, optional
    :param run_id: run_id of the hunt, defaults to None
    :type run_id: str, optional
    :param command_name: a specific analytic command that is captured, defaults to None
    :type command_name: str, optional
    :param database: _description_, defaults to BackendDefaults.DATABASE
    :type database: str, optional
    :param hunt_index: _description_, defaults to BackendDefaults.INDEX_TABLE
    :type hunt_index: str, optional
    :param hunt_table: _description_, defaults to BackendDefaults.HUNT_TABLE
    :type hunt_table: str, optional
    :return: a cell output listing the commands, execution times, and DataFrame(s) as they appeared at job run time
    :rtype: Union[Iterator[Tuple[str, DataFrame]], None]
    """

    return _functions._show_hunt_results(hunt_name=hunt_name, time_start=time_start, time_end=time_end,
                                         run_id=run_id, command_name=command_name, 
                                         database=database, hunt_index=hunt_index, hunt_table=hunt_table)

def get_hunt_result(hunt_name: str = None, time_start: str = None, time_end: str = None,
                    run_id: str = None, command_name: str = None,
                    database: str = BackendDefaults.DATABASE,
                    hunt_index: str = BackendDefaults.INDEX_TABLE,
                    hunt_table: str = BackendDefaults.HUNT_TABLE
                    ) -> Union[List[DataFrame], None]:
    """filter the threat hunt index and get the original results back as a list of DataFrame(s).
       One DataFrame per captured analytic command is returned as they appeared in the original job.

       You can then assign a new variable to each array index to triage & subsequently work on the original
       captured data.

       Note: Although all args are optional, you have to supply something!

       See show_hunt_results() if you want to see the original captured output only.

    :param hunt_name: name of hunt to filter for, defaults to None
    :type hunt_name: str, optional
    :param time_start: date/time of the hunt start, defaults to None (ex: 01-13-2024 13:00:00)
    :type time_start: str, optional
    :param time_end: date/time of the hunt end, defaults to None (ex: 01-13-2024 14:00:00)
    :type time_end: str, optional
    :param run_id: run_id of the hunt, defaults to None
    :type run_id: str, optional
    :param command_name: a specific analytic command that is captured, defaults to None
    :type command_name: str, optional
    :param database: _description_, defaults to BackendDefaults.DATABASE
    :type database: str, optional
    :param hunt_index: _description_, defaults to BackendDefaults.INDEX_TABLE
    :type hunt_index: str, optional
    :param hunt_table: _description_, defaults to BackendDefaults.HUNT_TABLE
    :type hunt_table: str, optional
    :param output_format: specify the output format as .asObject, asJSON, asDataFrame, defaults to OutputFormat.asDataFrame
    :type output_format: OutputFormat, optional
    :return: a list of original Dataframes that can be assigned to a new variable, and subsequently worked.
    :rtype: Union[List[DataFrame], None]
    """

    return _functions._get_hunt_result(hunt_name=hunt_name, time_start=time_start, time_end=time_end,
                                       run_id=run_id, command_name=command_name,
                                       database=database, hunt_index=hunt_index, hunt_table=hunt_table)

def write_hunt_index(df: DataFrame,
                     database: str = BackendDefaults.DATABASE,
                     hunt_index: str = BackendDefaults.INDEX_TABLE,
                     hunt_table: str = BackendDefaults.HUNT_TABLE) -> bool:
    """write back the dataframe to the hunt index

    :param df: dataframe derived from the threat hunt index (typically hunt_index)
    :type df: DataFrame
    :param database: _description_, defaults to BackendDefaults.DATABASE
    :type database: str, optional
    :param hunt_index: _description_, defaults to BackendDefaults.INDEX_TABLE
    :type hunt_index: str, optional
    :param hunt_table: _description_, defaults to BackendDefaults.HUNT_TABLE
    :type hunt_table: str, optional
    :return: invalid dataframe if the dataframe does not match the hunt_index schema
    :rtype: bool
    """
    return _functions._write_hunt(df=df, database=database, hunt_index=hunt_index, hunt_table=hunt_table)

def write_hunt_results(df: DataFrame, analytic_command_name: str,
                       database: str = BackendDefaults.DATABASE,
                       hunt_index: str = BackendDefaults.INDEX_TABLE,
                       hunt_table: str = BackendDefaults.HUNT_TABLE) -> bool:
    """save added metafields (_annotation, _risk, _row, _marked) back to the results DataFrame

    :param df: _description_
    :type df: DataFrame
    :param analytic_command_name: _description_
    :type analytic_command_name: str
    :param database: _description_, defaults to BackendDefaults.DATABASE
    :type database: str, optional
    :param hunt_index: _description_, defaults to BackendDefaults.INDEX_TABLE
    :type hunt_index: str, optional
    :param hunt_table: _description_, defaults to BackendDefaults.HUNT_TABLE
    :type hunt_table: str, optional
    :return: _description_
    :rtype: bool
    """
    return _functions._write_hunt(df=df, command_name=analytic_command_name,
                                  database=database, hunt_index=hunt_index, hunt_table=hunt_table)

def filter_hunt(df: DataFrame, run_id: str):
    return df

def get_artifacts(df, column) -> list:
    # TODO
    # given a dataframe grab the rows of column into a python list
    # this can then be used to pass to other things, like censys etc.
    # look at medium app saved blogs 'threat intel-pivoting using censys
    return
