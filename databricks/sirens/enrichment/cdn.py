import pyspark.sql.functions as F
from pyspark.sql.types import StringType

import pandas as pd

from .enrichment import PandasFunctionEnrichmentBase
from databricks.sirens.config_reader import ThreatIntelReader

import re

cdn_cnames = ThreatIntelReader().read_named_file(file_name='cdn_cnames')
if not cdn_cnames:
    cdn_cnames = {}

PATTERN = re.compile(
    '(' +
    '|'.join(
        [
            re.escape(domain)
            for name, domains in cdn_cnames.items()
            for domain in domains
        ]) +
    ')',
    re.IGNORECASE
)

CDN_LOOKUP = {}
for name, cdn_domains in cdn_cnames.items():
    for cdn_domain in cdn_domains:
        CDN_LOOKUP[cdn_domain.lower()] = name


class CdnEnrichment(PandasFunctionEnrichmentBase):
    EMPTY_RECORD = {
        'cdn_pattern': '',
        'cdn_name': ''
    }

    def __init__(self, src_column_name_or_expr: str, dest_column_name: str):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        """
        super().__init__("domain", src_column_name_or_expr, dest_column_name)

    def create_pandas_udf_function(self):
        def get_cdn(domain: str):
            if domain is None or domain.strip() == "":
                return self.EMPTY_RECORD
            mat = PATTERN.search(domain)
            if mat:
                cdn_pattern = mat.group(1).lower()
                return {
                    'cdn_pattern': cdn_pattern,
                    'cdn_name': CDN_LOOKUP.get(cdn_pattern, '')
                }
            return self.EMPTY_RECORD

        @F.pandas_udf("cdn_pattern string, cdn_name string")
        def cdn_udf_func(col: pd.Series) -> pd.DataFrame:
            extracted = col.apply(get_cdn)
            return pd.DataFrame(extracted.values.tolist())

        return cdn_udf_func
