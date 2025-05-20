import pyspark.sql.functions as F
from pyspark.sql import DataFrame

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
        super().__init__("Cisco ISE Parser", spark)
        
        spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN") #needed for map_keys

    def toSilver(self, df: DataFrame, data_source_obj: DataSource) -> DataFrame:
        """Use specific domain knowledge to create columns that can be transformed using sql expressions downstream

        :param df: Dataframe already processed for metadata (toBronze).
        :type df: DataFrame
        :param data_source_obj: the datasource object
        :type data_source_obj: DataSource
        :return: flattened DataFrame ready for normalization transformations
        :rtype: DataFrame
        """
        logger.info(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations started")
        try:            
            REGEX_PAYLOAD_PATTERN = '<(\d+)>([a-zA-Z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})(.*)'
            REGEX_MESSAGE_PATTERN = '(\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)'
            REGEX_META_PATTERN = '(\d+-\d+-\d+\s+\d+:\d+:\d+\.\d+)\s+([+-]\d+:\d+)\s+(\d+)\s+(\d+)\s+(\w+)\s+(.*):\s+(.*)'
            
            slv_transforms = {
                'pri_num': F.regexp_extract(F.col('value'), REGEX_PAYLOAD_PATTERN, 1), # priority value
                'time': F.regexp_extract(F.col('value'), REGEX_PAYLOAD_PATTERN, 2), # timestamp
                'msg_text': F.regexp_extract(F.col('value'), REGEX_PAYLOAD_PATTERN, 3), # message
                'syslog_header': F.regexp_extract(F.col('msg_text'), REGEX_MESSAGE_PATTERN, 0),
                'ip_addr_host_name': F.regexp_extract(F.col('msg_text'), REGEX_MESSAGE_PATTERN, 1), # device ip or host
                'cat_name': F.regexp_extract(F.col('msg_text'), REGEX_MESSAGE_PATTERN, 2), # category
                'msg_id': F.regexp_extract(F.col('msg_text'), REGEX_MESSAGE_PATTERN, 3), # id
                'total_seg': F.regexp_extract(F.col('msg_text'), REGEX_MESSAGE_PATTERN, 4), # line
                'seg_num': F.regexp_extract(F.col('msg_text'), REGEX_MESSAGE_PATTERN, 5), # line item
                'split_payload': F.split(F.col('msg_text'), REGEX_MESSAGE_PATTERN)[1],
                'split_syslog': F.when(F.col('seg_num') == 0, F.concat(F.lit('Metadata='), F.col('split_payload'))).otherwise(F.col('split_payload')),
                'split_key_value': F.str_to_map(F.col('split_syslog'), F.lit(","), F.lit("=")),
                }
            
            meta_parse_fields = {
                'Metadata': F.col('split_key_value').getItem('Metadata'),
                'meta_seq_num': F.regexp_extract(F.col('Metadata'), REGEX_META_PATTERN, 3),
                'meta_code': F.regexp_extract(F.col('Metadata'), REGEX_META_PATTERN, 4),
                'meta_severity': F.regexp_extract(F.col('Metadata'), REGEX_META_PATTERN, 5),
                'meta_desc': F.regexp_extract(F.col('Metadata'), REGEX_META_PATTERN, 6),
                'meta_text': F.regexp_extract(F.col('Metadata'), REGEX_META_PATTERN, 7),
                }
            
            key_value_tags = {
                'Step_list': F.expr("regexp_extract_all(split_syslog, 'Step=([0-9]+)')"),
                'SelectedAuthenticationIdentityStores_list': F.expr("regexp_extract_all(split_syslog, 'SelectedAuthenticationIdentityStores=([^,]+)')"),
                'NetworkDeviceGroups_list': F.expr("regexp_extract_all(split_syslog, 'NetworkDeviceGroups=([^,]+)')"),
                'StepData_list': F.expr("regexp_extract_all(split_syslog, 'StepData=([^,]+)')"),
                'SysStatsUtilizationNetwork_list': F.expr("regexp_extract_all(split_syslog, 'SysStatsUtilizationNetwork=([^,]+)')"),
                'SysStatsUtilizationDiskSpace_list':  F.expr("regexp_extract_all(split_syslog, 'SysStatsUtilizationDiskSpace=([^,]+)')"),
                }
            
            df = (df
                  .withColumns(slv_transforms)
                  .withColumns(meta_parse_fields)
                  .withColumns(key_value_tags)
                  .drop('value', 'msg_text', 'syslog_header', 'split_payload')
                  )

            logger.debug(f"pipeline_run_id={data_source_obj.pipeline_run_id} message=silver transformations completed")
            return df
        except Exception as exc:
            raise SirensParsingError(f"{exc}") from exc
