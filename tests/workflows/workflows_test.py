from pyspark.sql import DataFrame

import databricks.sirens.threathunting as th

from databricks.sirens.workflows._entities import DefaultColumns, WorkflowPriority, WorkflowSeverity, WorkflowStatus

def test_set_severity(apache_df):
    result = th.set_severity(apache_df, severity=WorkflowSeverity.CRITICAL, run_id="12345")
    assert isinstance(result, DataFrame)    
    assert DefaultColumns.WORKFLOW_STATUS.value in result.columns
    assert "severity" in result.select(str(DefaultColumns.WORKFLOW_STATUS.value + ".severity")).columns

def test_set_priority(apache_df):
    result = th.set_priority(apache_df, priority=WorkflowPriority.HIGH, run_id="12345")
    assert isinstance(result, DataFrame)
    assert DefaultColumns.WORKFLOW_STATUS.value in result.columns
    assert "priority" in result.select(str(DefaultColumns.WORKFLOW_STATUS.value + ".priority")).columns

def test_set_status(apache_df):
    result = th.set_status(apache_df, status=WorkflowStatus.NEW, run_id="12345")
    assert isinstance(result, DataFrame)
    assert DefaultColumns.WORKFLOW_STATUS.value in result.columns
    assert "status" in result.select(str(DefaultColumns.WORKFLOW_STATUS.value + ".status")).columns

def test_set_assignee(apache_df):
    result = th.set_assignee(apache_df, assignee='derek.king', run_id="12345")
    assert isinstance(result, DataFrame)
    assert DefaultColumns.WORKFLOW_STATUS.value in result.columns
    assert "assignee" in result.select(str(DefaultColumns.WORKFLOW_STATUS.value + ".assignee")).columns

