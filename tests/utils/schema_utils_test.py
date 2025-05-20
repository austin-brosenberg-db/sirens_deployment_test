from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, TimestampType, LongType
from databricks.sirens.utils.schema_utils import DataFrameSchema, Schemas

def test_translate_type_string():
    field = DataFrameSchema.translate_type("name", "string")
    assert isinstance(field, StructField)
    assert field.name == "name"
    assert isinstance(field.dataType, StringType)
    assert field.nullable is True

def test_translate_type_integer():
    field = DataFrameSchema.translate_type("age", "integer")
    assert isinstance(field, StructField)
    assert field.name == "age"
    assert isinstance(field.dataType, IntegerType)
    assert field.nullable is True

def test_translate_type_date():
    field = DataFrameSchema.translate_type("birthdate", "date")
    assert isinstance(field, StructField)
    assert field.name == "birthdate"
    assert isinstance(field.dataType, DateType)
    assert field.nullable is True

def test_translate_type_timestamp():
    field = DataFrameSchema.translate_type("event_time", "timestamp")
    assert isinstance(field, StructField)
    assert field.name == "event_time"
    assert isinstance(field.dataType, TimestampType)
    assert field.nullable is True

def test_translate_type_long():
    field = DataFrameSchema.translate_type("id", "long")
    assert isinstance(field, StructField)
    assert field.name == "id"
    assert isinstance(field.dataType, LongType)
    assert field.nullable is True

def test_as_field():
    entries = [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
    field = DataFrameSchema.as_field("person", entries)
    assert isinstance(field, StructField)
    assert field.name == "person"
    assert isinstance(field.dataType, StructType)
    assert field.nullable is True
    assert field.dataType.fields == entries

def test_as_struct():
    entries = [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
    struct = DataFrameSchema.as_struct(entries)
    assert isinstance(struct, StructType)
    assert struct.fields == entries
    
def test_default_schema():
    assert Schemas.default_schema() == "sirens"

def test_from_named_stanza(global_config):
    config = global_config
    config.add_section("test_stanza")
    config.set("test_stanza", "schema", "test_schema")
    schema = Schemas._from_named_stanza(config, "test_stanza")
    assert schema.name == "test_schema"
    assert schema.origin == "stanza"

def test_from_named_stanza_no_section(global_config):
    config = global_config
    schema = Schemas._from_named_stanza(config, "non_existent_stanza")
    assert schema is None

def test_from_system_stanza(global_config):
    config = global_config
    config.add_section("test_module")
    config.set("test_module", "schema", "test_schema")
    schema = Schemas._from_system_stanza(config, "test_module")
    assert schema.name == "test_schema"
    assert schema.origin == "system_stanza"

def test_from_system_stanza_no_section(global_config):
    config = global_config
    schema = Schemas._from_system_stanza(config, "non_existent_module")
    assert schema is None