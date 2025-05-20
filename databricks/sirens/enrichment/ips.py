from typing import Dict, Union, Optional

import pandas as pd
import pyspark.sql.functions as F

from netaddr import IPAddress, AddrFormatError

from .enrichment import PandasFunctionEnrichmentBase
from .internal.ips import is_public_ip, is_ip

__NOT_EXISTING__ = "__not_exist__"


class IsIPEnrichment(PandasFunctionEnrichmentBase):
    """Class for detection if given column is an IP address or global IP address"""
    def __init__(self, src_column_name_or_expr: str, dest_column_name: str,
                 only_public_ips = False):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        :param only_public_ips: boolean flag defining if it should handle only public IP addresses
        """
        super().__init__("is_ip", src_column_name_or_expr, dest_column_name)
        self.only_public_ips = only_public_ips

    def create_pandas_udf_function(self):
        def is_ip_func(s: str):
            if s is None:
                return None
            if s == "":
                return False

            if self.only_public_ips:
                return is_public_ip(s)

            return is_ip(s)

        @F.pandas_udf(returnType="boolean")
        def is_ip_udf_func(col: pd.Series) -> pd.Series:
            return col.apply(is_ip_func)

        return is_ip_udf_func


class IPClassEnrichment(PandasFunctionEnrichmentBase):
    """Class for IP address classification"""
    def __init__(self, src_column_name_or_expr: str, dest_column_name: str):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        """
        super().__init__("ip_class", src_column_name_or_expr, dest_column_name)

    def create_pandas_udf_function(self):
        def ip_class_func(s: str, cache: Dict[str, Optional[str]]):
            if s is None or s == "":
                return None

            res = cache.get(s, __NOT_EXISTING__)
            if res != __NOT_EXISTING__:
                return res

            try:
                ip_addr = IPAddress(s)
                if ip_addr.is_multicast():
                    res = "multicast"
                elif ip_addr.is_loopback():
                    res = "loopback"
                elif ip_addr.is_link_local():
                    res = "link-local"
                elif ip_addr.is_reserved():
                    res = "reserved"
                elif ip_addr.is_unicast() and not (ip_addr.is_ipv4_private_use() or ip_addr.is_ipv6_unique_local()):
                    res = "public"
                elif ip_addr.is_ipv4_private_use() or ip_addr.is_ipv6_unique_local():
                    res = "private"
                else:
                    res = "unknown"
            except (ValueError, AddrFormatError):
                res = None

            # TODO: Cache negative results as well, so we don't return problematic entry
            cache[s] = res
            return res

        @F.pandas_udf(returnType="string")
        def ip_class_udf_func(col: pd.Series) -> pd.Series:
            cache: Dict[str, Optional[str]] = {}
            return col.apply(lambda s: ip_class_func(s, cache))

        return ip_class_udf_func
