import pyspark.sql.functions as F
import pandas as pd

from .enrichment import PandasFunctionEnrichmentBase

import tldextract

tld_extract = tldextract.TLDExtract(suffix_list_urls=())


class TLDExtractEnrichment(PandasFunctionEnrichmentBase):
    EMPTY_RECORD = {
        'suffix': '',
        'registered_domain': '',
        'subdomain': '',
    }

    def __init__(self, src_column_name_or_expr: str, dest_column_name: str):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        """
        super().__init__("domain", src_column_name_or_expr, dest_column_name)

    def create_pandas_udf_function(self):
        def get_tld_extract_details(domain: str):
            if domain is None or domain.strip() == "":
                return self.EMPTY_RECORD

            rec = tld_extract(domain)
            return {
                'suffix': rec.suffix,
                'registered_domain': rec.registered_domain,
                'subdomain': rec.subdomain
            }

        @F.pandas_udf("suffix string, registered_domain string, subdomain string")
        def tld_extract_udf_func(col: pd.Series) -> pd.DataFrame:
            extracted = col.apply(get_tld_extract_details)
            return pd.DataFrame(extracted.values.tolist())

        return tld_extract_udf_func
