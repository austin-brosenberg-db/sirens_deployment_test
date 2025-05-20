"""A module for housing the common data structures used by Sirens

    Author: Derek King
    Date: 2024-06-01
    version: 0.1
"""
from enum import Enum
from collections import namedtuple
from dataclasses import dataclass
from databricks.sirens.utils.global_config import ConfigReader

# # # # # # #
# ENUMS
# # # # # # #
class OutputFormat(Enum):
    """For methods the accept an OutputFormat arg.
    """
    asObject = "py_dataclass"
    asDataFrame = "dataframe"
    asJSON = "json"
    asHTML = "html"  

class SchemaDefaults(Enum):
    """Backstop Schemas for when keys are not found / configured in sirens.config

    :param Enum: _description_
    :type Enum: _type_
    """
    INTEL_DATABASE = "threat_intelligence"

class TableDefaults(Enum):
    """Backstop Table names for when keys are not found / configured in sirens.config
    """
    INTEL_TABLE = "intelligence"


# # # # # # #
# Classes
# # # # # # #
class OutputRecord:
    def __init__(self, entry):
        self._make(entry)
    
    def _make(self, entry):
        for k, v in entry.items():
            setattr(self, k,  v)
    
    @property
    def record(self):
        return vars(self)

class AnalysisItem(OutputRecord):
    """Return class for search / analysis functions / commands
    """
    pass

# # # # # # # #
# Named Tuples
# # # # # # # #
IOCType = namedtuple("IOCType", ["value", "regex", "weight"])


# # # # # # # #
# dataclasses
# # # # # # # #
@dataclass
class ThreatIntel:
    DATABASE: str = ConfigReader.get_config_or_default(section="threat_intel", 
                                                       key='database',
                                                       defaults={'database': SchemaDefaults.INTEL_DATABASE.value})
    INTEL_TABLE: str = ConfigReader.get_config_or_default(section='threat_intel',
                                                          key='intel_table',
                                                          defaults={'intel_table': TableDefaults.INTEL_TABLE.value})


