"""Given an inputs.yaml file validate the syntax, keys/values."""

from __future__ import annotations

import datetime
import enum
import os
from pathlib import Path
from typing import Any, Optional, Literal, List
from typing_extensions import Self

import yaml
from pydantic import field_validator, model_validator, BaseModel, ValidationError
from pyspark.sql import SparkSession

from databricks.sirens.logging import get_logger
from databricks.sirens.utils.base_utils import BaseUtils

logger = get_logger(__name__)

validKafkaOptions = ["kafka.bootstrap.servers", "startingTimestamp", "startingOffsetsByTimestamp", "startingOffsets",
                     "endingTimestamp", "endingOffsetsByTimestamp", "endingOffsets",
                     "failOnDataLoss", "kafkaConsumer.pollTimeoutMs", "flog_sourceshOffset.numRetries",
                     "flog_sourceshOffset.retryIntervalMs",
                     "maxOffsetsPerTrigger", "minOffsetsPerTrigger", "maxTriggerDelay", "minPartitions",
                     "groupIdPrefix", "kafka.group.id",
                     "includeHeaders", "startingOffsetsByTimestampStrategy", "assign", "subscribe", "subscribePattern",
                     "kafka.bootstrap.servers"]

validJsonOptions = ["timeZone", "primitiveAsString", "prefersDecimal", "allowComments", "allowUnquotedFieldNames",
                    "allowSingleQuotes", "allowNumericLeadingZero",
                    "mode", "columnNameOfCorruptRecord", "dateFormat", "timestampFormat", "timestampNTZFormat",
                    "multiLine", "encoding", "lineSep",
                    "samplingRatio", "dropFieldIfAllNull", "locale", "allowNonNumericNumbers", "compression",
                    "ignoreNullFields"]

validCsvOptions = ["sep", "encoding", "quote", "quoteAll", "escape", "escapeQuotes", "comment", "header", "inferSchema",
                   "enforceSchema",
                   "ignoreLeadingWhiteSpace", "ignoreTrailingWhiteSpace", "nullValue", "nanValue", "positiveInf",
                   "negativeInf", "dateFormat",
                   "timestampFormat", "timestampNTXFormat", "maxColumns", "maxCharsPerColumn", "mode",
                   "columnNameOfCorruptRecord", "multiLine",
                   "charToEscapeQuoteEscaping", "samplingRatio", "emptyValue", "locale", "lineSep",
                   "unescapedQuoteHandling",
                   "compression"]

validTextOptions = ["wholetext", "lineSep", "compression"]

validParquetOptions = ["datetimeRebaseMode", "int96RebaseMode", "mergeSchema", "readerCaseSensitive",
                       "rescuedDataColumn"]

validAutoLoaderOptions = ["modifiedAfter", "modifiedBefore", "pathGlobFilter", "recursiveFileLookup",
                          "cloudFiles.format", "cloudFiles.queueUrl",
                          "cloudFiles.awsAccessKey", "cloudFiles.awsSecretKey", "cloudFiles.roleArn",
                          "cloudFiles.roleExternalId",
                          "cloudFiles.roleSessionName", "cloudFiles.stsEndpoint", "cloudFiles.clientId",
                          "cloudFiles.clientSecret",
                          "cloudFiles.connectionString", "cloudFiles.resourceGroup", "cloudFiles.subscriptionId",
                          "cloudFiles.tenantId", "cloudFiles.queueName", "cloudFiles.client", "cloudFiles.clientEmail",
                          "cloudFiles.privateKey",
                          "cloudFiles.privateKeyId", "cloudFiles.projectId", "cloudFiles.subscription",
                          "cloudFiles.schemaEvolutionMode",
                          "cloudFiles.inferColumnTypes", "cloudFiles.schemaLocation", "cloudFiles.schemaHints",
                          "cloudFiles.includeExistingFiles"]


class Config(BaseModel):
    version: float
    updated: datetime.date
    author: str
    description: str


class Connector(BaseModel):
    name: Literal['json', 'kafka', 'csv', 'txt', 'autoloader', 'okta_api']
    options: Any = None


class Input(BaseModel):
    source: str
    sourcetype: str
    parser: str
    rawPath: Optional[str] = None
    rawSchemaFile: Optional[str] = None
    rawSchemaHintsFile: Optional[str] = None
    connector: Connector
    host: Optional[str] = None
    host_rawpath_segment: Optional[int] = None
    streamType: Literal['batch', 'streaming']

    # @validator('*')
    @model_validator(mode="before")
    def check_input_options(cls, values):
        # if streaming use case, then spark inferSchema is turned off by default. We must have a schema file set.
        stream_type = values.get("streamType")
        raw_schema_file = values.get("rawSchemaFile")
        connector: dict = values.get("connector")
        connector_name = connector.get("name")
        if 'streaming' in stream_type and raw_schema_file is None and connector_name != "autoloader":
            raise ValueError('rawSchemaFile must be used for streaming use cases')

        # if filebased in input (i.e not kafka log_sources) then path to the raw directory/file must be set.
        rawPath = values.get('rawPath')
        if connector_name in ('csv', 'json', 'txt') and rawPath is None:
            raise ValueError("rawPath must be set for filebased inputs")

        # if streamType, streaming - then rawPath must be a directory. This is a challenge, since you could have a rawPath defined outside
        # the package. Do a simple check for a file extension in rawPath
        if 'streaming' in stream_type and rawPath is not None:
            if os.path.splitext(rawPath)[1] != '':
                raise ValueError("rawPath appears to have a file extension - streaming inputs should be directories")

        # warning for kafka based inputs that appear to have set rawPath also.
        if connector_name == 'kafka' and (rawPath is not None and rawPath != 'None'):
            raise ValueError("kafka input configured, but rawPath also set - remove or set rawPath to None")

        connector_options = connector.get("options")
        # if kafka, then check for valid options
        if connector_name == 'kafka' and connector_options:
            for key in connector_options.keys():
                if key not in validKafkaOptions:
                    raise ValueError(
                        f"Kafka option {key} is not supported\n supported options are: {validKafkaOptions}")

        # if txt, check for valid spark options
        if 'txt' == connector_name and connector_options:
            for key in connector_options.keys():
                if key not in validTextOptions:
                    raise ValueError(f"Text option {key} is not supported\n supported options are: {validTextOptions}")

        # if json, check for valid json options
        if 'json' == connector_name and connector_options:
            for key in connector_options.keys():
                if key not in validJsonOptions:
                    raise ValueError(f"JSON option {key} is not supported\n supported options are: {validJsonOptions}")

        # if csv, check for valid options
        if 'csv' == connector_name and connector_options:
            for key in connector_options.keys():
                if key not in validCsvOptions:
                    raise ValueError(f"CSV option {key} is not supported\n supported options are: {validCsvOptions}")

        # if autoloader: true - check valid options.
        if 'autoloader' == connector_name and connector_options:
            valid_options = validAutoLoaderOptions

            # add valid options for the file format specified.
            if 'cloudFiles.format' in connector_options.keys():
                if 'csv' == connector_options['cloudFiles.format']:
                    valid_options = set().union(validAutoLoaderOptions, validCsvOptions)
                if 'json' == connector_options['cloudFiles.format']:
                    valid_options = set().union(validAutoLoaderOptions, validJsonOptions)
                if 'txt' == connector_options['cloudFiles.format']:
                    valid_options = set().union(validAutoLoaderOptions, validTextOptions)
                if 'parquet' == connector_options['cloudFiles.format']:
                    valid_options = set().union(validAutoLoaderOptions, validParquetOptions)

            for key in connector_options.keys():
                if key not in valid_options:
                    raise ValueError(
                        f"AutoLoader option {key} is not supported\n supported options are: {valid_options}")

            if stream_type == "batch":
                raise ValueError("autoloader is a streaming mode operation. Change to streamType: streaming")

        return values


class Meta(BaseModel):
    timestamp_column: Optional[str] = None
    timestamp_regex: Optional[str] = None
    timestamp_format: Optional[str] = None
    source: Optional[str] = None
    sourcetype: Optional[str] = None

    @field_validator('*')
    def check_regex_and_format_not_configured(cls, values):
        # timestamp_regex, timestamp_format = values.get("timestamp_regex"), values.get("timestamp_format")
        # if timestamp_regex is not None and timestamp_format is not None:
        #    raise ValueError('timestamp_regex and timestamp_format are mutually exclusive')
        # if timestamp_regex is None and timestamp_format is None:
        #    raise ValueError('timestamp_regex or timestamp_format must be set')

        return values


class TableConfig(BaseModel):
    partition_cols: Optional[list] = None
    zorder_cols: Optional[list] = None
    optimize_write: Optional[bool] = None
    auto_compact: Optional[bool] = None

    # @validator('*')
    @model_validator(mode="before")
    def check_table_config(cls, values):
        if len(values.get("partition_cols", [])) > 0:
            logger.warn(
                "Partitioning should only be applied to tables in TB+ size. Ensure you're not partitioning on columns "
                "with high cardinality. For more details, please see "
                "https://docs.databricks.com/tables/partitions.html")

        if len(values.get("zorder_cols", [])) == 0:
            logger.warn("No zorder columns detected")

        if values.get("optimize_write", "false") == "true":
            logger.warn("Optimize writes enabled, if you require low-latency this should be disabled")

        if values.get("auto_compact", "false") == "true":
            logger.warn("Auto compaction enabled, if you require low-latency this should be disabled")

        return values


class ActionEnum(enum.Enum):
    Copy = "copy"
    Add = "add"
    Alias = "alias"
    Rename = "rename"


class Silver(BaseModel):
    meta: Optional[Meta] = None
    table_config: Optional[TableConfig] = None
    event_type: Optional[List[EventTypeItem]] = None


class Bronze(BaseModel):
    meta: Meta
    table_config: Optional[TableConfig] = None


class CIMField(BaseModel):
    action: str
    value: Optional[str] = None

    @model_validator(mode='after')
    def check_value_present(self) -> Self:
        if self.action != "copy" and not self.value:
            raise ValueError("value must be set for action != copy")
        return self


class EventType(CIMField):
    type: Optional[str] = None


class EventResult(CIMField):
    type: Optional[str] = None


class CIMFields(BaseModel):
    _event_date: Optional[CIMField] = None
    _event_time: Optional[CIMField] = None
    _sourcetype: Optional[CIMField] = None
    _source: Optional[CIMField] = None
    dvc_hostname: Optional[CIMField] = None
    event_type: Optional[EventType] = None
    event_sub_type: Optional[CIMField] = None
    event_result: Optional[EventResult] = None


class EventTypeItem(BaseModel):
    target_table: str
    filter: Optional[str] = None
    fields: List[CIMFields]


class _EventDate1(BaseModel):
    action: str
    value: str


class Field1(BaseModel):
    _event_date: _EventDate1


class EventTypeItem1(BaseModel):
    target_table: str
    filter: Optional[str] = None
    fields: List[Field1]


class AggregateItem(BaseModel):
    target_table: str
    aggregation_name: Optional[str] = None
    source_table: str
    stream_type: Optional[Literal["batch", "streaming"]] = None
    save_mode: Literal["append", "complete"]
    filter: Optional[str] = None
    groupby: Optional[str] = None
    agg_columns: Optional[str] = None
    window_duration: Optional[str] = None
    wait_for_late_data: Optional[str] = None
    start_time: Optional[str] = None

    @model_validator(mode="before")
    def check_aggregate_config(cls, values):
        print(f"type={type(values)}, values={values}")
        if values.get("save_mode") != "append" and values.get("save_mode") != "complete":
            raise ValueError("invalid save_mode - must be one of (append | complete)")

        stream_type = values.get("stream_type")
        if stream_type and (stream_type != "batch" and stream_type != "streaming"):
            raise ValueError("stream_type must be one of (batch | streaming) if set")

        # update mode can only be used with watermarking, and window function.
        save_mode = values.get("save_mode")
        window_duration = values.get("window_duration")
        wait_for_late_data = values.get("wait_for_late_data")
        if stream_type == 'streaming' and save_mode == 'append':
            if not bool(window_duration) and not bool(wait_for_late_data):
                raise ValueError(
                    'streaming in append mode requires both (wait_for_late_data) and (window_duration) to be configured')

        # wait_for_late_data is a structured streaming operation ONLY. cannot be used for batch reads.
        if stream_type == 'batch' and bool(wait_for_late_data):
            raise ValueError(
                "watermarking defined by (wait_for_late_data) is a streaming operation only. correct stream_type, or remove watermarking")

        # windowing bit on but required window_duration not specified.
        if bool(wait_for_late_data) and not bool(window_duration):
            raise ValueError("wait_for_late_data set, bit window_duration is not")

        sort_order = values.get("sort_order")
        if bool(sort_order) and (sort_order != "ascending" and sort_order != "descending"):
            raise ValueError("sort_order must be ascending or descending")

        # if running in a databricks env - check sql can be parsed.
        if SparkSession.getActiveSession():
            sql_filter = values.get(filter)
            if bool(sql_filter) and not BaseUtils.validate_sql_expression(sql_filter):
                raise ValueError("invalid sql_filter")

            aggregation_functions = values.get("agg_columns")
            if bool(aggregation_functions):
                for agg in aggregation_functions:
                    if not BaseUtils.validate_sql_expression(agg):
                        raise ValueError("Invalid Aggregation Function")

        return values


class Gold(BaseModel):
    aggregate: List[AggregateItem]


class Transforms(BaseModel):
    bronze: Bronze
    silver: Optional[Silver] = None
    gold: Optional[Gold] = None


class Model(BaseModel):
    config: Config
    input: Input
    transforms: Transforms


def start(self, file):
    self.file = file
    path = Path(self.file)

    try:
        with open(path, 'r') as fp:
            con = yaml.safe_load(fp)
    except yaml.YAMLError as exc:
        print("Error while parsing YAML file:")
        if hasattr(exc, 'problem_mark'):
            if exc.context is not None:
                print(f"  parser says\n{str(exc.problem_mark)}")
                print(f"  {str(exc.problem)} {str(exc.context)}")
                print("Please correct data and retry.")
            else:
                print(f"  parser says\n{str(exc.problem_mark)}")
                print(f"{str(exc.problem)}Please correct data and retry")
                print("Please correct data and retry.")
                return 1
        else:
            print("Something went wrong while parsing yaml file")
            return 1

    try:
        Model.model_validate(con)
        return 0

    except ValidationError as exc:
        print(exc)
        return 1
