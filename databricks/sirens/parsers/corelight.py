from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, from_json

from databricks.sirens.datasource import DataSource
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

    def __init__(self, spark):
        super().__init__("corelight json_streaming_<files> parser", spark)

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
            # Transform data
            df = df.select(from_json(df.raw, "map<string, string>"))
            df = df.select(col("entries.`@rawstring`").alias("_raw"))
            df = df.select(from_json("_raw", "map<string, string>"))
            # Add metadata to frame, including _event_date (the partition column) needed to write to delta.
            df = self._add_metadata(df, data_source_obj=data_source_obj)
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=added metadata completed")
            return df

        except Exception as exc:
            logger.error(f"{exc}")
            raise SirensParsingError(f"{exc}") from exc

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toBronze).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation starting")

        try:
            # -------------- CHANGE ME OR DELETE ME ----------------------------- #
            source, sourcetype = data_source_obj.get_source_sourcetype_info()

            # TODO: really move it to the constants, maybe in a separate module
            SUPPORTED_SOURCETYPES = {
                "corelight_dns": ["entries.TC", "entries.RA", "entries.`id.resp_h`", "entries._write_ts",
                                  "entries.`id.orig_h`", "entries.ts",
                                  "entries.query", "entries.proto", "entries.AA", "entries.`id.orig_p`",
                                  "entries.qclass_name", "entries._path",
                                  "entries.qtype_name", "entries.RD", "entries.qtype", "entries.qclass",
                                  "entries.`id.resp_p`", "entries.uid",
                                  "entries.rejected", "entries._system_name", "entries.Z", "entries.trans_id"],
                "corelight_dhcp": ["entries.duration", "entries.mac", "entries._write_ts", "entries.msg_types",
                                   "entries.ts", "entries.host_name",
                                   "entries._path", "entries.uids", "entries.client_addr", "entries._system_name"],
                "corelight_dce_rpc": ["entries.`id.resp_h`", "entries._write_ts", "entries.`id.orig_h`", "entries.ts",
                                      "entries.`id.orig_p`",
                                      "entries._path", "entries.operation", "entries.named_pipe", "entries.`id.resp_p`",
                                      "entries.endpoint",
                                      "entries.uid", "entries.rtt", "entries.system_name"],
                "corelight_dpd": ["entries.`id.resp_h`", "entries.analyzer", "entries._write_ts",
                                  "entries.failure_reason", "entries.`id.orig.h`",
                                  "entries.ts", "entries.proto", "entries.`id.orig_p`", "entries._path",
                                  "entries.`id.resp_p`", "entries.uid",
                                  "entries._system_name"],
                "corelight_http": ["entries.request_body_len", "entries.method", "entries.orig_filenames",
                                   "entries.`id.resp_h`", "entries._write_ts",
                                   "entries.response_body_len", "entries.tags", "entries.host", "entries.`id.orig_h`",
                                   "entries.ts", "entries.uri",
                                   "entries.status_code", "entries.`id.orig_p`", "entries._path", "entries.user_agent",
                                   "entries.version",
                                   "entries.resp_fuids", "entries.trans_depth", "entries.status_msg",
                                   "entries.`id.resp_p`", "entries.uid",
                                   "entries.referrer", "entries._system_name", "entries.orig_fuids"],
                "corelight_files": ["entries.sha1", "entries.duration", "entries.source", "entries._write_ts",
                                    "entries.ts", "entries.rx_hosts",
                                    "entries.overflow_bytes", "entries.seen_bytes", "entries.total_bytes",
                                    "entries._path", "entries.md5",
                                    "entries.missing_bytes", "entries.fuid", "entries.timedout", "entries.tx_hosts",
                                    "entries.depth", "entries.analyzers",
                                    "entries.conn_uids", "entries.is_orig", "entries._system_name"],
                "corelight_snmp": ["entries.duration", "entries.community", "entries.`id.resp_h`", "entries._write_ts",
                                   "entries.`id.orig_h`",
                                   "entries.ts", "entries.get_responses", "entries.`id.orig_p`", "entries._path",
                                   "entries.version",
                                   "entries.get_requests", "entries.`id.resp_p`", "entries.uid", "entries.set_requests",
                                   "entries._system_name",
                                   "entries.get_bulk_requests"],
                "corelight_ssh": ["entries.mac_alg", "entries.`id.resp_h`", "entries.host_key_alg", "entries._write_ts",
                                  "entries.server",
                                  "entries.auth_attempts", "entries.`id.orig_h`", "entries.compression_alg",
                                  "entries.ts", "entries.`id.orig_p`",
                                  "entries._path", "entries.version", "entries.cipher_alg", "entries.host_key",
                                  "entries.inferences", "entries.client",
                                  "entries.`id.resp_p`", "entries.uid", "entries.kex_alg", "entries.auth_success",
                                  "entries._system_name"],
                "corelight_ssl": ["entries.server_name", "entries.`id.resp_h`", "entries.cert_chain_fps",
                                  "entries._write_ts", "entries.subject",
                                  "entries.curve", "entries.`id.orig_h`", "entries.ts", "entries.issuer",
                                  "entries.`id.orig_p`", "entries._path",
                                  "entries.sni_matches_cert", "entries.version", "entries.client_cert_chain_fps",
                                  "entries.cipher", "entries.resumed",
                                  "entries.`id.resp_p`", "entries.uid", "entries.established", "entries.ssl_history",
                                  "entries._system_name"],
                "corelight_smtp": ["entries.`id.resp_h`", "entries._write_ts", "entries.path", "entries.fuids",
                                   "entries.`id.orig_h`", "entries.ts",
                                   "entries.`id.orig_p`", "entries._path", "entries.tls", "entries.trans_depth",
                                   "entries.helo", "entries.`id.resp_p`",
                                   "entries.uid", "entries.is_webmail", "entries.last_reply", "entries._system_name"],
                "corelight_radius": ["entries.`id.resp_h`", "entries._write_ts", "entries.`id.orig_h`",
                                     "entries.result", "entries.ts",
                                     "entries.`id.orig_p`", "entries._path", "entries.`id.resp_p`", "entries.uid",
                                     "entries._system_name"],
                "corelight_kerberos": ["entries.forwardable", "entries.till", "entries.success", "entries.`id.resp_h`",
                                       "entries._write_ts",
                                       "entries.`id.orig_h`", "entries.ts", "entries.request_type",
                                       "entries.`id.orig_p`", "entries.service", "entries._path",
                                       "entries.renewable", "entries.cipher", "entries.client", "entries.`id.resp_p`",
                                       "entries.uid", "entries._system_name"],
                "corelight_ntp": ["entries.poll", "entries.root_delay", "entries.`id.resp_h`", "entries.precision",
                                  "entries._write_ts",
                                  "entries.`id.orig_h`", "entries.ts", "entries.org_time", "entries.rec_time",
                                  "entries.xmt_time", "entries.`id.orig_p`",
                                  "entries._path", "entries.version", "entries.root_disp", "entries.ref_time",
                                  "entries.`id.resp_p`", "entries.stratum",
                                  "entries.num_exts", "entries.uid", "entries.mode", "entries._system_name",
                                  "entries.ref_id"],
                "corelight_pe": ["entries.uses_seh", "entries._write_ts", "entries.ts", "entries.uses_aslr",
                                 "entries.os", "entries.has_cert_table",
                                 "entries.has_import_table", "entries.section_names", "entries._path", "entries.is_exe",
                                 "entries.compile_ts", "entries.id",
                                 "entries.machine", "entries.has_debug_data", "entries.uses_dep", "entries.uses_dep",
                                 "entries.is_64bit", "entries.subsystem",
                                 "entries.uses_code_integrity", "entries._system_name", "entries.has_export_table"],
                "corelight_suricata": ["entries.pcap_cnt", "entries.`alert.signature_id`", "entries._write_ts",
                                       "entries.`alert.rev`",
                                       "entries.`alert.action`", "entries.`id.orig_h`", "entries.ts",
                                       "entries.`id.orig_p`", "entries.service", "entries._path",
                                       "entries.`alert.metadata`", "entries.`alert.gid`", "entries.`id.resp_p`",
                                       "entries.tx_id", "entries.uid", "entries.metadata",
                                       "entries.`alert.severity`", "entries.`alert.category`", "entries.flow_id",
                                       "entries.suri_id",
                                       "entries.`alert.signature`", "entries._system_name"],
                "corelight_x509": ["entries.`basic_constraints.ca`", "entries.fingerprint", "entries._write_ts",
                                   "entries.`certificate.not_valid_before`", "entries.`certificate.sig_alg`",
                                   "entries.`certificate.exponent`",
                                   "entries.`certificate.issuer`", "entries.`certificate.key_alg`",
                                   "entries.`certificate.subject`", "entries.`san.dns`",
                                   "entries._path", "entries.`certificate.version`", "entries.`certificate.serial`",
                                   "entries.`certificate.key_type`",
                                   "entries.client_cert", "entries._system_name", "entries.host_cert",
                                   "entries.`certificate.key_length`",
                                   "entries.`certificate.not_valid_after`"]
            }

            cols = SUPPORTED_SOURCETYPES.get(sourcetype)
            if cols is None:
                raise SirensParsingError(
                    f"pipeline_run_id={data_source_obj.pipeline_run_id} message=No parser configured for {sourcetype}")

            df = df.select(df.colRegex("`_.*`"), "dvc_hostname", *cols)

            if sourcetype == "corelight_dhcp":
                df = df.select("*", from_json("msg_types", "array<string>").alias("msg_arr"))
                df = df.select("*", explode("msg_arr").alias("message_type")).drop("msg_arr")

            # ------------------------------------------------------------------- #
            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformation completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
