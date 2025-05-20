from pyspark.sql import DataFrame
from pyspark.sql.functions import current_user
from ._entities import WorkflowStatus, WorkflowPriority, WorkflowSeverity, DefaultColumns
from . import _functions

__all__ = ["set_severity", "set_priority", "set_status", "set_assignee", "assign_to_me"]


def set_severity(df: DataFrame, severity: WorkflowSeverity, run_id: str) -> DataFrame:
    """set the severity column for the dataframe with the given run_id

    :param df: _description_
    :type df: DataFrame
    :param severity: _description_
    :type severity: Severity
    :param run_id: _description_
    :type run_id: str
    """
    return _functions.set_workflow(df=df, column_name=DefaultColumns.SEVERITY.value, value=severity, run_id=run_id)


def set_priority(df: DataFrame, priority: WorkflowPriority, run_id: str) -> DataFrame:
    """set the priority column for the dataframe with the given run_id

    :param df: _description_
    :type df: DataFrame
    :param priority: _description_
    :type priority: Priority
    :param run_id: _description_
    :type run_id: _type_
    :return: _description_
    :rtype: DataFrame
    """
    return _functions.set_workflow(df=df, column_name=DefaultColumns.PRIORITY.value, value=priority, run_id=run_id)


def set_status(df: DataFrame, status: WorkflowStatus, run_id: str) -> DataFrame:
    """set the status column of the dataframe with the given run_id

    :param df: _description_
    :type df: DataFrame
    :param status: _description_
    :type status: Status
    :param run_id: _description_
    :type run_id: str
    :return: _description_
    :rtype: DataFrame
    """
    return _functions.set_workflow(df=df, column_name=DefaultColumns.STATUS.value, value=status, run_id=run_id)


def set_assignee(df: DataFrame, assignee: str, run_id: str) -> DataFrame:
    """set the assigned column of the dataframe with the given run_id

    :param df: _description_
    :type df: DataFrame
    :param assignee: _description_
    :type assignee: str
    :param run_id: _description_
    :type run_id: str
    :return: _description_
    :rtype: DataFrame
    """
    return _functions.set_workflow(df=df, column_name=DefaultColumns.ASSIGNEE.value, value=assignee, run_id=run_id)


def assign_to_me(df: DataFrame, run_id: str) -> DataFrame:
    """set the assigned column of the dataframe to the current user with the given run_id

    :param df: _description_
    :type df: DataFrame
    :param run_id: _description_
    :type run_id: str
    :return: _description_
    :rtype: DataFrame
    """
    return _functions.set_workflow(df=df, column_name=DefaultColumns.ASSIGNEE.value, value=current_user(),
                                   run_id=run_id)
