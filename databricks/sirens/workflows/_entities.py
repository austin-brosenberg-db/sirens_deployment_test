from enum import Enum

class DefaultColumns(Enum):
    WORKFLOW_STATUS = "_workflow_status"
    RUN_ID = "run_id"
    SEVERITY = 'severity'
    STATUS = 'status'
    PRIORITY = 'priority'
    ASSIGNEE = 'assignee'

class WorkflowSeverity(Enum):
    INFORMATIONAL = 'Informational'
    UNKNOWN = 'Unknown'
    LOW = 'Low'
    MEDIUM = 'Medium'
    HIGH = 'High'
    CRITICAL = 'Critical'

class WorkflowPriority(Enum):
    UNKNOWN = 'Unknown'
    LOW = 'Low'
    MEDIUM = 'Medium'
    HIGH = 'High'

class WorkflowStatus(Enum):
    NEW = 'New'
    IN_PROGRESS = 'In_Progress'
    PENDING = 'Pending'
    RESOLVED = 'Resolved'
    CLOSED = 'Closed'

class WorkflowAssignee(Enum):
    UNASSIGNED = 'Unassigned'
