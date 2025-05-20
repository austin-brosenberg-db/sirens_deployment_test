import copy
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Union, Dict, Any, List, Callable
from abc import ABC, abstractmethod
import json

from pyspark.sql import DataFrame, SparkSession, Column, DataFrameReader
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.streaming import DataStreamReader

import databricks.sirens.parsers.internal.common as cm


@dataclass(frozen=True)
class DataField:
    """The data class representing a one field in the input data.  It has source name, optional source type, together
    with additional information, like, final name & final type (to perform transformation), and optional transformation
    function, plus some additional data.
    """
    # TODO: think how to specify complex fields, including final/logical types inside them
    source_name: str
    source_type: Union[str, T.DataType] = 'string'
    description: str = ""
    is_source_field: bool = True
    final_name: Optional[str] = None
    final_type: Optional[Union[str, T.DataType]] = None
    transform: Optional[Union[Column, Callable[[Column], Column]]] = None
    default: Optional[Union[Column, Any]] = None
    snake_case_name: bool = True

    @cached_property
    def name(self) -> str:
        """Returns the final name of the field, based on the source & final name, plus normalization rules.

        :return: final name of the field
        """
        return cm.normalize_name(self.source_name, self.final_name, self.snake_case_name)

    # make it cached property?
    def get_final_type_string(self) -> str:
        """Returns a string with name of the final type, depending on how it's defined (string or Spark type).
        It could be a logical type, like, ip, host, ...  Use the ``get_spark_type_for_data_type`` function
        to convert this type string into Spark type string.

        :return: string with the type name
        """
        if isinstance(self.final_type, str):
            return self.final_type
        if isinstance(self.final_type, T.DataType):
            return self.final_type.typeName()

        if isinstance(self.source_type, T.DataType):
            return self.source_type.typeName()
        return self.source_type

    def get_transform(self) -> Optional[Column]:
        """Returns a ``Column`` object representing a transformation that will be applied to the source column
        to obtain a final value.   Actual implementation depends on the parameters defined for the current field:
        transformation function, final type, final name, etc.

        :return: optional ``Column`` object
        """
        if not self.is_source_field:
            return None
        if self.transform is not None:
            if isinstance(self.transform, Column):
                return self.transform
            elif isinstance(self.transform, Callable):
                return self.transform(F.col(self.source_name))
            else:
                raise Exception(f"Unsupported type for the transform parameter: {type(self.transform)}")
        if self.final_type is not None:
            if isinstance(self.final_type, str):
                dest_type = cm.get_spark_type_for_data_type(self.final_type)
                if isinstance(self.source_type, str):
                    src_type = self.source_type
                else:
                    src_type = self.source_type.typeName()
                if dest_type != src_type:
                    return F.col(self.source_name).cast(dest_type)
                else:
                    return F.col(self.source_name)
            else:
                return F.col(self.source_name).cast(self.final_type)

        return None


@dataclass(frozen=True)
class SourceTable:
    """The data class to represent a table with data - we need to define at least a list of the source fields, plus
    optional transformations, description, etc.
    """
    source_fields: List[DataField]
    # TODO: sync it with Sirens or with OCSF or have function that will map between both of them
    category: str = "other"
    description: str = ""
    specification: str = ""
    transformations: Optional[List[Callable[[DataFrame], DataFrame]]] = None
    # TODO: also define fields for partial decoding?

    @cached_property
    def source_schema(self) -> T.StructType:
        """Returns a string with Spark schema to read the data.

        :return: Spark schema to read data.
        """
        fields = [T.StructField(f.source_name, cm.get_spark_type(f.source_type))
                  for f in self.source_fields if f.is_source_field]
        return T.StructType(fields)

    @staticmethod
    def from_mapping(field_names: List[str], mapping: Dict[str, DataField]) -> List[DataField]:
        """Creates an instance of the ``SourceTable`` class from the list of fields, and mapping between fields
        names and their definitions.

        :param field_names: list of fields in a given table
        :param mapping: dictionary that maps field name into ``DataField`` instances.
        :return: instance of the ``SourceTable`` class
        """
        fields = []
        for fn in field_names:
            f = mapping.get(fn)
            if f is None:
                raise Exception(f"There is no field '{fn}' in the mapping!")
            fields.append(f)
        return fields

    @staticmethod
    def _fill_defaults(df: DataFrame, fields: List[DataField]):
        mapping = dict([(f.source_name, f.default) for f in fields if f.default is not None])
        if len(mapping) == 0:
            return df
        cols = []
        for c in df.columns:
            default = mapping.get(c)
            cl = F.col(f"`{c}`")
            if default is None:
                cols.append(cl)
            elif isinstance(default, Column):
                cols.append(F.when(cl.isNotNull(), cl).otherwise(default).alias(c))
            else:
                cols.append(F.when(cl.isNotNull(), cl).otherwise(F.lit(default)).alias(c))

        return df.select(*cols)

    def normalize_dataframe(self, df: DataFrame) -> DataFrame:
        """Performs normalization of the data in DataFrame - fill defaults, apply transformations, type cases,
        rename columns, etc.

        :param df: DataFrame with data read from the source
        :return: transformed DataFrame
        """
        ndf = SourceTable._fill_defaults(df, self.source_fields)

        mapping: Dict[str, Any] = {}
        for f in self.source_fields:
            tf: Dict[str, Any] = {'alias': f.name}
            if f.transform is not None:
                tf['conversion'] = f.transform
            if f.final_type is not None:
                tf['type'] = f.final_type
            mapping[f.source_name] = tf

        tdf = cm.normalize_dataframe(ndf, mapping)
        if self.transformations is not None:
            for t in self.transformations:
                tdf = tdf.transform(t)

        return tdf


class BaseDataSource(ABC):
    """
    Base class for data sources.
    """
    # TODO add common fields, like, _ingest_date, etc.

    def __init__(self, name: str):
        self._name = name
        self.partial_parsing_support = False

    @property
    def name(self):
        return self._name

    @abstractmethod
    def parse(self, df: DataFrame, data_col: str = "value", drop_data_col: bool = True) -> Optional[DataFrame]:
        """
        Method to perform parsing of the data when they are coming in the line-by-line format
        :param drop_data_col:
        :param df: source dataframe with data
        :param data_col: name of the column with data
        :param drop_data_col: specify if data column should be dropped
        :return:
        """
        pass

    @staticmethod
    def partial_parsing(df: DataFrame, data_col: str = "value", **kwargs) -> Optional[DataFrame]:
        """
        Method to perform partial parsing of the data when they are coming in the line-by-line
        format - this could be useful when ingesting the raw data into bronze to extract
        event type, timestamps, etc. to create partitions out of the data

        :param df: source dataframe with data
        :param data_col: name of the column with data
        :return:
        """
        return df

    def supports_partial_parsing(self):
        return self.partial_parsing_support

    @abstractmethod
    def normalize(self, df: DataFrame) -> DataFrame:
        """
        Performs normalization of the dataframe content - cast types, rename columns, performs transformations, ...

        :param df: dataframe to normalize
        :return: normalized dataframe
        """
        pass

    @abstractmethod
    def load(self, fmt: str, path: Optional[str] = None, table: Optional[str] = None,
             opts: Optional[dict] = None, streaming=True) -> DataFrame:
        """
        Loads content from a given data source & normalize it, performing parsing if necessary

        :param fmt: data format
        :param path: path to the data (required if ``table`` isn't provided)
        :param table: table name (required if ``path`` isn't provided)
        :param opts: source options
        :param streaming: if it's a streaming source
        :return: parsed & normalized content
        """
        pass


# TODO: think how to handle corrupted data - add _corrupted_fields automatically, or not...
class BaseStructuredSource(BaseDataSource, ABC):
    """
    Base class for structured data - CSV, JSON
    """

    __default_autoloader_options__ = {
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.validateOptions": "true",
    }

    def __init__(self, name: str, cloudfiles_format: str,
                 schema: Optional[Union[str, T.StructType]] = None,
                 schema_file: Optional[str] = None):
        super().__init__(name)
        self.schema = schema
        self.cloudfiles_options = copy.copy(self.__default_autoloader_options__)
        self.cloudfiles_options["cloudFiles.format"] = cloudfiles_format
        if schema_file is not None:
            # TODO: should we use SparkSession.read.json() here?
            with open(schema_file, "r") as f:
                self.schema = T.StructType.fromJson(json.load(f))

    def get_schema(self) -> Union[str, T.StructType]:
        if self.schema is None:
            raise RuntimeError("Schema isn't specified!")
        return self.schema

    def normalize(self, df: DataFrame) -> DataFrame:
        return df

    def load(self, data_format: str = "cloudFiles", path: Optional[str] = None,
             table: Optional[str] = None, options: Optional[dict] = None,
             is_streaming=True) -> DataFrame:
        if path is None:
            raise RuntimeError("Please provide 'path' parameter")
        if options is None:
            options = {}
        if data_format == "cloudFiles":
            all_opts = {**self.cloudfiles_options, **options}
        else:
            all_opts = options
        spark = SparkSession.getActiveSession()
        rdr: Union[DataFrameReader, DataStreamReader]
        if is_streaming:
            rdr = spark.readStream
        else:
            rdr = spark.read
        df = rdr.format(data_format) \
            .options(**all_opts) \
            .schema(self.get_schema()) \
            .load(path)
        return self.normalize(df)


class BaseJsonSource(BaseStructuredSource, ABC):
    """
    Base class for structured data - JSON
    """

    def __init__(self, name: str, schema: Optional[Union[str, T.StructType]] = None,
                 schema_file: Optional[str] = None):
        super().__init__(name, cloudfiles_format="json", schema=schema, schema_file=schema_file)

    def parse(self, df: DataFrame, data_col: str = "value", drop_data_col: bool = True) -> Optional[DataFrame]:
        ndf = cm.extract_json(df, self.get_schema(), json_col=data_col, drop_json_col = drop_data_col)
        return self.normalize(ndf)


class BaseCsvSource(BaseStructuredSource, ABC):
    """
    Base class for structured data - CSV
    """
    __csv_col_name__ = "__csv__"

    def __init__(self, name: str, schema: Optional[Union[str, T.StructType]] = None,
                 schema_file: Optional[str] = None):
        super().__init__(name, cloudfiles_format="csv", schema=schema, schema_file=schema_file)

    def parse(self, df: DataFrame, data_col: str = "value", drop_data_col: bool = True) -> Optional[DataFrame]:
        ndf = df.select("*", F.from_csv(data_col, self.get_schema()).alias(self.__csv_col_name__)) \
            .select("*", f"{self.__csv_col_name__}.*").drop(self.__csv_col_name__)
        if drop_data_col:
            ndf = ndf.drop(data_col)
        return self.normalize(ndf)


class BaseTextSource(BaseDataSource, ABC):
    """
    Base class for text data - text files
    """
    __default_autoloader_options__ = {
        "cloudFiles.format": "text",
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.validateOptions": "true",
    }

    def __init__(self, name: str):
        super().__init__(name)

    def load(self, fmt: str = "cloudFiles", path: Optional[str] = None, table: Optional[str] = None,
             opts: Optional[dict] = None, streaming=True) -> DataFrame:
        if path is None:
            raise RuntimeError("Please provide 'path' parameter")
        if opts is None:
            opts = {}
        if fmt == "cloudFiles":
            all_opts = {**self.__default_autoloader_options__, **opts}
        else:
            all_opts = opts
        spark = SparkSession.getActiveSession()
        rdr: Union[DataFrameReader, DataStreamReader]
        if streaming:
            rdr = spark.readStream
        else:
            rdr = spark.read
        df = rdr.format(fmt) \
            .options(**all_opts) \
            .load(path)
        return self.normalize(df)

    def normalize(self, df: DataFrame) -> DataFrame:
        return df
