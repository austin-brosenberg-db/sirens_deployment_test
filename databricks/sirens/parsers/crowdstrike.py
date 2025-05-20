import pyspark.sql.functions as F
from pyspark.sql import DataFrame

import databricks.sirens.parsers.internal.base as cb
import databricks.sirens.parsers.internal.common as cm
import databricks.sirens.parsers.internal.crowdstrike_fields as cf
import databricks.sirens.parsers.internal.crowdstrike_schemas as cs
from databricks.sirens.datasource import DataSource, TimeStampInfo
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from databricks.sirens.utils.base_utils import *

logger = get_logger(__name__)


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    __partial_schema__ = "`event_simpleName` string, event_type string, `EventType` string, timestamp string, " \
                         "aip string, AgentIdString string, aid string, `UTCTimestamp` long"

    def __init__(self, spark):
        super().__init__("Crowdstrike Falcon logs parser", spark)

    def toBronze(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """extract an event timestamp and augment raw data with the required metadata

        :param df: Incoming DataFrame
        :type df: DataFrame
        :param data_source_obj: the datasource config object
        :type data_source_obj: DataSource
        :return: dataframe with event_timestamp, metadata and a partition column (_event_date)
        :rtype: DataFrame
        """

        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=adding row metadata")
        try:
            # Extract Timestamp column
            df = cm.extract_json(df, self.__partial_schema__).withColumns({
                self.timestamp_column_name: F.coalesce(F.col("timestamp").cast("timestamp"),
                                                       (F.col("timestamp").cast("long") / 1000.0).cast("timestamp"),
                                                       (F.col("UTCTimestamp").cast("long") / 1000.0).cast(
                                                           "timestamp"),
                                                       F.col("_metadata.file_modification_time"),
                                                       F.current_timestamp()),
                "host": F.coalesce("aip", "AgentIdString", "aid"),
                "event_name": F.coalesce("event_simpleName", "EventType", "event_type")
            })
            # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj=data_source_obj,
                                    timestamp_info=TimeStampInfo(timestamp_column=self.timestamp_column_name,
                                                                 timestamp_col_type="timestamp")) \
                .drop(self.timestamp_column_name, 'host', "event_simpleName", "EventType", "event_type", "aip",
                      "AgentIdString", "aid", "UTCTimestamp", "timestamp")

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    @staticmethod
    def process_special_table(event_type: str, src_df: DataFrame, dest_df: DataFrame):
        tbl = cs.get_crowdstrike_schema(event_type)
        tdf = src_df.filter(f"event_name = '{event_type}'")
        tdf = cm.extract_json(tdf, tbl.source_schema, drop_json_col=True)
        tdf = tbl.normalize_dataframe(tdf)

        return dest_df.unionByName(tdf, allowMissingColumns=True)

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        logger.info(
            f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation starting")

        # Define mappings for the data normalization

        try:
            # These event types don't share the schema with the rest of the events, and need a special handling,
            # but the data for these events could be in the same input files so we need to filter out them explicitly
            special_event_types = {"Event_ExternalApiEvent", "ZeroTrustHostAssessment"}
            event_types = set(
                data_source_obj.data_source_config.get("transforms", {}).get("silver", {}).get("meta", {})
                .get("event_names", []))
            if len(event_types) == 0:
                event_types = set(cs.supported_crowdstrike_schemas())
            target_fields = set()
            for event in event_types:
                # TODO: We may need to handle Event_ExternalApiEvent and ZeroTrustHostAssessment events
                # as they use different types for the same fields, and when reading they aren't loaded correctly
                scm = cs.get_crowdstrike_schema(event)
                if scm is None:
                    raise SirensConfigException(f"There is no mapping for Crowdstrike event type '{event}'")
                if event in special_event_types:
                    logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=event type {event} is "
                                 f"handled separately")
                    continue
                for f in scm.source_fields:
                    if f.is_source_field:
                        target_fields.add(f.source_name)

            ndf = df
            events_with_shared_schema = event_types.difference(special_event_types)
            if events_with_shared_schema:
                logger.debug(f"Collected {len(target_fields)} fields for the mapping, building the mapping...")
                source_table = cb.SourceTable(source_fields=cb.SourceTable.from_mapping(
                    list(target_fields), cf.crowdstrike_fields))
                ndf = ndf.filter(F.col("event_name").isin(events_with_shared_schema))
                ndf = cm.extract_json(ndf, source_table.source_schema, drop_json_col=True)
                ndf = source_table.normalize_dataframe(ndf)

            for scm in special_event_types:
                if scm not in event_types:
                    continue
                logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=processing {scm}")
                ndf = self.process_special_table(scm, df, ndf)

            logger.debug(
                f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return ndf
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
