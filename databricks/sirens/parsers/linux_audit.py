import re
from datetime import datetime
from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import unhex, decode, lower, col

from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensParsingError
from databricks.sirens.logging import get_logger
from databricks.sirens.parsers import plugins
from databricks.sirens.parsers.base_parser import BaseParser
from .internal.keyvalue_parser import GenericKVParser
from .internal.linux_audit_fields import field_names

logger = get_logger(__name__)


class LinuxAuditDKVParser(GenericKVParser):
    """ Example:
    spark.conf.set('spark.sql.caseSensitive', True)
    kvParser = LinuxAuditDKVParser()
    bronze_df = kvParser.parse(src_df, "value", "record")
    silver_df = kvParser.expand_fields(bronze_df, field_names)
    silver_df = kvParser.post_process(silver_df, field_names)
    """

    def parse_audit(self, msg: str):
        def extract_ts_id(text):
            match = re.search(r'\((.*?)\)', text)
            if match:
                return match.group(1)
            else:
                return None

        if msg.startswith("audit"):
            audit = msg[len("audit"):]
            ts_id = extract_ts_id(audit)
            ts, id = ts_id.split(":")
            return datetime.fromtimestamp(float(ts)).strftime("%Y/%m/%d %H:%M:%S"), id
        else:
            return None

    def parse_kv(self, text: str, fieldNames=[]):
        i = 0
        dct = dict()  # init_dct(field_names)
        encoded_bytes = text.encode("utf-8", errors="ignore")
        text = encoded_bytes.decode("utf-8").replace('\x1d', ' ')
        while i < len(text):
            i = self._skip(i, text)
            if i >= len(text):
                break
            field_name, i = self._find_fieldname(i, text, fieldNames, keyValueSep=self.value_sep)
            if not field_name:
                break
            field_value, i = self._extract_value(text, i)
            if field_name == "msg":
                msg_dct = self.parse_kv(field_value, fieldNames)
                if msg_dct:
                    dct.update(msg_dct)
                else:
                    dt, uid = self.parse_audit(field_value)
                    if dt:
                        dct["msg_dt"] = dt
                        dct["msg_uid"] = uid
                    else:
                        dct[field_name] = field_value
            else:
                dct[field_name] = field_value
        return dct

    def post_process(self, df: DataFrame, fieldNames: List[str]):
        for fn in fieldNames:
            if fn.upper() == fn:
                fn = fn.replace("-", "_")
                df = df.withColumnRenamed(fn, f"{fn.lower()}_uppercase")
        return df.withColumn("proctitle", decode(unhex("proctitle"), 'UTF-8'))


@plugins.register
class Parse(BaseParser):
    """Datasource specific. Responsible for event timestamp extraction to _raw_time, and providing
    a flattened DataFrame.
    """

    def __init__(self, spark):
        super().__init__("linux auditd enriched format parser", spark)
        spark.conf.set("spark.sql.caseSensitive", "true")
        self.kv_parser = LinuxAuditDKVParser()

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
            source, sourcetype = data_source_obj.get_source_sourcetype_info()
            all_df = self.kv_parser.parse(df, "value", "record")
            bronze_df = all_df.filter(lower(col("record.type")) == sourcetype.lower())
            # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(bronze_df, data_source_obj=data_source_obj)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toSilver).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation starting")

        # Flatten the record, and userIdentity structs into individual columns
        try:
            silver_df = self.kv_parser.expand_fields(df, field_names)
            silver_lowercase_df = self.kv_parser.post_process(silver_df, field_names)
            df = silver_lowercase_df.withColumn("dvc_hostname", col("hostname"))

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
