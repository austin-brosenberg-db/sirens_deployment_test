from dataclasses import dataclass, asdict, field
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType
from databricks.sirens.utils.global_config import ConfigReader
from enum import Enum
from databricks.sirens.workflows import _entities as wf
    
def _get_config_defaults(section: str, key: str):
    config = ConfigReader.read()
    defaults = {'schema': 'sirens', 'index_table': 'threathunt_index', 'result_table': 'threathunt_results'}
    returned_val = ConfigReader._get_config_key(config, section, key)
    if returned_val:
        return returned_val
    else:
        return defaults.get(key)

class DefaultColumns(Enum):
    MARKED = "_marked"
    ANNOTATION = "_annotation"
    ROW = "_row"
    RISK = "_risk"

class HuntMetaCols(Enum):
    _annotation = "_annotation"
    _marked = "_marked"
    _row = "_row"
    _risk_column = "_risk"

@dataclass
class ActiveHunt:
    database: str
    hunt_name: str
    hunt_table: str
    notebook: str
    run_id: str
    status: str
    state: str
    _workflow_status: dict = field(default_factory=lambda: { 
        wf.DefaultColumns.STATUS.value: wf.WorkflowStatus.NEW.value,
        wf.DefaultColumns.SEVERITY.value: wf.WorkflowSeverity.UNKNOWN.value,
        wf.DefaultColumns.PRIORITY.value: wf.WorkflowPriority.LOW.value,
        wf.DefaultColumns.ASSIGNEE.value: wf.WorkflowAssignee.UNASSIGNED.value
        })
    start_time: str = None
    end_time: str = None
    result: str = None
    tags: dict = None
    attacks: list = None

    schema = StructType([
        StructField('start_time', StringType(), True),
        StructField('end_time', StringType(), True),
        StructField('hunt_name', StringType(), True),
        StructField('run_id', StringType(), True),
        StructField('status', StringType(), True),
        StructField('state', StringType(), True),
        StructField('database', StringType(), True),
        StructField('notebook', StringType(), True),
        StructField('hunt_table', StringType(), True),
        StructField('result', StringType(), True),
        StructField('tags', MapType(StringType(), StringType()), True),
        StructField("attacks", ArrayType(MapType(StringType(), MapType(StringType(), StringType()))), True),
        StructField('_workflow_status', StructType([
                    StructField('status', StringType(), True),
                    StructField('severity', StringType(), True),
                    StructField('priority', StringType(), True),
                    StructField('assignee', StringType(), True),
                    ])
                    )
    ])

@dataclass
class HuntRecord:
    run_id: str
    start_time: str
    end_time: str
    command_name: str
    description: str
    result: dict
    result_count: int = None
    _command_seq: int = None
    _risk_column: dict = None
    _row: int = None
    _marked: str = None
    _annotation: dict = None

    schema = StructType([
        StructField('start_time', StringType(), True),
        StructField('end_time', StringType(), True),
        StructField('run_id', StringType(), True),
        StructField('command_name', StringType(), True),
        StructField('description', StringType(), True),
        StructField('result', MapType(StringType(), StringType()), True),
        StructField('result_count', IntegerType(), True),
        StructField('_command_seq', IntegerType(), True),
        StructField('_risk_column', StructType([
                    StructField('risk_object', StringType(), True),
                    StructField('object_type', StringType(), True),
                    StructField('impact', StringType(), True),
                    StructField('confidence', StringType(), True),
                    StructField('source', StringType(), True),
                    StructField('annotation', MapType(StringType(), StringType()), True)
                    ])
                    ),
        StructField('_row', IntegerType(), True),
        StructField('_marked', StringType(), True),
        StructField('_annotation', MapType(StringType(), StringType()), True)
    ])

@dataclass
class HuntStatus:
    INITIALIZING = "initializing"
    RUNNING = "running"
    FINISHED = "finished"

@dataclass
class HuntState:
    INITIALIZING = "initializing"
    ACTIVE = "active"
    PAUSED = "paused"
    FINISHED = "finished"

@dataclass
class HuntResult:
    SUCCESS = "success"
    FAILED = "failed"

@dataclass
class BackendDefaults:
    STORE: str = "delta"
    INDEX_TABLE: str = _get_config_defaults('schema:threat_hunt', key='index_table')
    HUNT_TABLE: str = _get_config_defaults('schema:threat_hunt', key='results_table')
    DATABASE: str = _get_config_defaults('schema:threat_hunt', key='schema')

@dataclass
class HuntBackend:
    hunt_table: str = None
    hunt_name: str = None
    database: str = BackendDefaults.DATABASE
    store: str = BackendDefaults.STORE
    index_table: str = BackendDefaults.INDEX_TABLE

@dataclass
class HuntConfigMeta:
    name: str
    description: str = None
    author: str = None
    version: str = None
    created: str = None
    last_updated: str = None
    notebooks: list = None
    playbooks: list = None

    schema = StructType([
        StructField('name', StringType(), True),
        StructField('description', StringType(), True),
        StructField('author', StringType(), True),
        StructField('version', StringType(), True),
        StructField('created', StringType(), True),
        StructField('last_updated', StringType(), True),
        StructField('notebooks', ArrayType(StringType()), True),
        StructField('playbooks', ArrayType(StringType()), True)
    ])

    def from_hunt_dict(hunt_dict):
        config = {}
        config['name'] = hunt_dict.get("hunt").get("name")
        config['description'] = hunt_dict.get("hunt").get("description")
        config['author'] = hunt_dict.get("meta").get("author")
        config['version'] = hunt_dict.get("meta").get("version")
        config['created'] = hunt_dict.get("meta").get("created")
        config['last_updated'] = hunt_dict.get("meta").get("last_updated")

        config['notebooks'] = []
        notebooks = hunt_dict.get("notebooks")
        if notebooks:
            for nb in notebooks:
                config['notebooks'].append(nb.get("name"))

        config['playbooks'] = []
        playbooks = hunt_dict.get("playbooks")
        if playbooks:
            for pb in playbooks:
                config['playbooks'].append(pb.get("name"))
        else:
            config['playbooks'] = None

        return HuntConfigMeta(**config)

class OutputFormat(Enum):
    asObject = "py_dataclass"
    asDataFrame = "dataframe"
    asJSON: bool = "json"

