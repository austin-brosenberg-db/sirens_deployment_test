import logging

import pandas as pd

import pyspark.sql.functions as F

import netaddr

from .enrichment import PandasFunctionEnrichmentBase


class MacAddressEnrichment(PandasFunctionEnrichmentBase):
    """"""

    EMPTY_RECORD = {'org': None, 'oui': None, 'formatted': None}
    DIALECTS = {'unix_expanded': netaddr.mac_unix_expanded(), 'bare': netaddr.mac_bare(),
                'cisco': netaddr.mac_cisco(), 'unix': netaddr.mac_unix(),
                'eui48': netaddr.mac_eui48(), 'pgsql': netaddr.mac_pgsql()}

    def __init__(self, src_column_name_or_expr: str, dest_column_name: str, format: str = "unix_expanded"):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        :param format: format used for Mac Address. Supported values are: `unix_expanded`, `bare`, `cisco`,
        `unix`, `eui48`, `pgsql`
        """
        super().__init__("mac_address", src_column_name_or_expr, dest_column_name)
        self.mac_format = self.DIALECTS[format]

    def create_pandas_udf_function(self):
        def get_mac_data(s: str):
            if s is None or s == "":
                return self.EMPTY_RECORD
            try:
                mac = netaddr.EUI(s)
                d = self.EMPTY_RECORD.copy()
                d["formatted"] = mac.format(self.mac_format)
                if mac.oui.reg_count > 0:
                    reg = mac.oui.registration()
                    d["oui"] = reg.oui
                    d["org"] = reg.org
                return d
            except (ValueError, netaddr.AddrFormatError, netaddr.core.NotRegisteredError) as e:
                logging.info(f"error parsing mac address {s}: {e}")
                return self.EMPTY_RECORD

        @F.pandas_udf("org string, oui string, formatted string")
        def mac_data_udf_func(col: pd.Series) -> pd.DataFrame:
            extracted = col.apply(get_mac_data)
            return pd.DataFrame(extracted.values.tolist())

        return mac_data_udf_func
