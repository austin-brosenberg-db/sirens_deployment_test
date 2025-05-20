
from pyspark.sql.types import StructType, StringType, StructField, MapType, IntegerType
from databricks.sirens.utils.global_config import ConfigReader
from dataclasses import dataclass
from enum import Enum


def _get_config_defaults(section: str, key: str):
    config = ConfigReader.read()
    defaults = {'schema': 'sirens', 'risk_table': 'risk'}
    returned_val = ConfigReader._get_config_key(config, section, key)
    return returned_val or defaults.get(key)

@dataclass
class BackendDefaults:
    STORE: str = "delta"
    RISK_TABLE: str = _get_config_defaults(section="schema:risk", key="risk_table")
    DATABASE: str = _get_config_defaults(section="schema:risk", key="schema")

class DefaultColumns(Enum):
    RISK = "_risk"

class RiskObjectType(Enum):
    USER = "user"
    SYSTEM = "system"

@dataclass
class RiskObject:
    _event_time: str
    risk_object: str
    object_type: str
    impact: int
    confidence: int
    source: str
    source_id: str = None
    package_name: str = None
    annotation: dict = None

    schema = StructType([
        StructField('_event_time', StringType(), True),
        StructField('risk_object', StringType(), True),
        StructField('object_type', StringType(), True),
        StructField('impact', IntegerType(), True),
        StructField('confidence', IntegerType(), True),
        StructField('risk_score', IntegerType(), True),
        StructField('source', StringType(), True),
        StructField("source_id", StringType(), True),
        StructField('package_name', StringType(), True),
        StructField('annotation', MapType(StringType(), StringType()), True)
    ])
