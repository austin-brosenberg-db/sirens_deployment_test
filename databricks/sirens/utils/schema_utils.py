from databricks.sirens.utils.global_config import ConfigReader
from typing import Optional, Type
from databricks.sirens.internal._modules import SirensModule
from databricks.sirens.global_config import GlobalConfig
from collections import namedtuple

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, LongType, FloatType, DoubleType, BooleanType, ArrayType

Schema = namedtuple('schema', ['name', 'origin'])

class Schemas:
    """class to resolve the schema(s) required for specific inputs or modules available in sirens."""

    _DEFAULT_SCHEMA = "sirens"

    @classmethod
    def default_schema(cls):
        return cls._DEFAULT_SCHEMA
    
    @staticmethod
    def _from_named_stanza(config, stanza):
        # named stanza input
        if config.has_section(stanza):
            schema = config.get(stanza, "schema", fallback=None)
        else:
            return None
        if schema is not None:
            return Schema(name=schema, origin = 'stanza')
        else:
            return None
    
    @staticmethod
    def _from_system_stanza(config, module):
        if config.has_section(module):
            schema = config.get(module, "schema", fallback=None)
        else:
            return None
        if schema is not None:
            return Schema(name=schema, origin = 'system_stanza')
        else:
            return None
    
    @staticmethod
    def _from_default_stanza(config):
        schema = GlobalConfig.get_target_database(config)
        if schema is not None:
            return Schema(name=schema, origin = 'default_stanza')
        else:
            return None

    @staticmethod
    def get_schema(stanza: Optional[str] = None, module: Optional[Type[SirensModule]] = None) -> str:
        schema = None
        # precedence order (most specific -> least)
        # specific stanza input -> system stanza -> default database
        
        # read global config file.
        config = ConfigReader.read()

        # named stanza input
        schema = Schemas._from_named_stanza(config, stanza)
        if schema is not None:
            return schema
        
        # system stanza
        if module:
            if 'input' not in module:
                module = "schema:" + module
            if config.has_section(module):
                schema = Schemas._from_system_stanza(config, module)
                if schema is not None:
                    return schema

        # default database
        schema = Schemas._from_default_stanza(config)
        if schema is not None:
            return schema

        # should not be hit.
        if not schema:
            schema = Schemas._DEFAULT_SCHEMA
            return Schema(name=schema, origin='class_default')

        return schema
    
# TODO ocsf - tests.
class DataFrameSchema:

    type_match = {"string": StringType, 
                "model": StructType, 
                "integer": IntegerType, 
                "date": DateType, 
                "timestamp": TimestampType,
                "long": LongType,
                "bigint": LongType,
                "float": FloatType,
                "double": DoubleType,
                "boolean": BooleanType,
                "array<string>": lambda: ArrayType(StringType())}
    
    @staticmethod
    def translate_type(name: str, datatype: str) -> StructField:
        """
        Translates a given name and datatype into a Spark StructField.
        :param name: The name of the field.
        :type name: str
        :param datatype: The datatype of the field as a string.
        :type datatype: str
        :return: A StructField with the specified name and datatype, nullable by default.
        :rtype: StructField
        """
        #nullable by default
        schema_entry = StructField(name, DataFrameSchema.type_match[datatype](), True)

        return schema_entry

    @staticmethod
    def as_field(name, entries):
        """
        Create a StructField with the given name and entries.
        :param name: The name of the field.
        :type name: str
        :param entries: A list of StructField objects that define the schema of the field.
        :type entries: list
        :return: A StructField object with the specified name and schema.
        :rtype: pyspark.sql.types.StructField
        """


        return StructField(name, StructType(entries), True)

    @staticmethod
    def as_struct(entries):
        """
        Convert a list of entries into a StructType.
        :param entries: List of entries to be converted into a StructType.
        :type entries: list
        :return: A StructType object containing the provided entries.
        :rtype: StructType
        """

        return StructType(entries)
