from pyspark.sql import DataFrame

from ._entities import WorkflowStatus, WorkflowPriority, WorkflowSeverity, WorkflowAssignee, DefaultColumns
import pyspark.sql.functions as F

from databricks.sirens.logging import get_logger
logger = get_logger(__name__)

def create_default_workflow(df: DataFrame) -> DataFrame:

    df = df.withColumn(DefaultColumns.WORKFLOW_STATUS.value, F.struct(
        F.lit(WorkflowStatus.NEW.value).alias(DefaultColumns.STATUS.value),
        F.lit(WorkflowSeverity.UNKNOWN.value).alias(DefaultColumns.SEVERITY.value),
        F.lit(WorkflowPriority.LOW.value).alias(DefaultColumns.PRIORITY.value),
        F.lit(WorkflowAssignee.UNASSIGNED.value).alias(DefaultColumns.ASSIGNEE.value)))

    return df

def set_workflow(df: DataFrame, column_name: str, value: str, run_id: str) -> DataFrame:

    # assignee is a freeform string value, not an enum.
    if column_name == DefaultColumns.ASSIGNEE.value:
        col_value = value
    else:
        # check valid instance has been passed in function.
        if type(value) not in (WorkflowPriority, WorkflowSeverity, WorkflowStatus, WorkflowAssignee):
            logger.info("invalid args: must be instance of (WorkflowPriority, WorkflowSeverity, WorkflowStatus, WorkflowAssignee)")
            return df
        else:
            col_value = value.value

    # Create the default column if it doesn't already exist.
    if DefaultColumns.WORKFLOW_STATUS.value not in df.columns:
        df = create_default_workflow(df)

    # add a workflow entry to matching row(s)
    df = (df.withColumn(DefaultColumns.WORKFLOW_STATUS.value, F.when(F.col(DefaultColumns.RUN_ID.value) == run_id,
                        F.col(DefaultColumns.WORKFLOW_STATUS.value).withField(column_name, F.lit(col_value)))
                        .otherwise(F.col(DefaultColumns.WORKFLOW_STATUS.value))))

    return df
