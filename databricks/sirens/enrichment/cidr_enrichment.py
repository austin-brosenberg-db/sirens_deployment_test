from typing import Dict, Any, List, ClassVar
from dataclasses import dataclass, field
from collections import namedtuple

import SubnetTree
import pandas as pd
import pyspark.sql.functions as F
from .enrichment import PandasFunctionEnrichmentBase

ALLOW_CIDR_TUPLE = namedtuple("ALLOW_CIDR_TUPLE", "ip_prefix provider region service")


@dataclass
class CidrEnrichment(PandasFunctionEnrichmentBase):
    """Class that enriches DataFrame from given CIDR tuples

    :param ip_column_name_or_expr: name of the column with IP information or SQL expression
    :param dest_column_name: name of the column in which data will be stored
    :param Rows that contain ip_prefix, provider, region and service
    :param extras: more CIDR tuple added to the db

    aws_enrichment = CidrEnrichment(rows, "sourceIPAddress", "ai", extras = [
      ALLOW_CIDR_TUPLE("10.0.0.0/8","aws","private","private"),
      ALLOW_CIDR_TUPLE("192.168.0.0/16","aws","private","private")
          ])
    """

    rows: List[Any]
    ip_column_name_or_expr: str
    dest_column_name: str
    extras: List[ALLOW_CIDR_TUPLE] = field(default_factory=list)

    EMPTY_RECORD: ClassVar[Dict[str, str]] = {
        "ip_prefix": "NA",
        "provider": "NA",
        "region": "NA",
        "service": "NA",
    }

    def __post_init__(self):
        super().__init__("name", self.ip_column_name_or_expr, self.dest_column_name)

    def _add_cidr_record(self, tree, extra_cidrs: List[ALLOW_CIDR_TUPLE]):
        for t in extra_cidrs:
            tree[t.ip_prefix] = t

    def _get_database(self):
        _tree = SubnetTree.SubnetTree()
        for row in map(ALLOW_CIDR_TUPLE._make, self.rows):
            try:
                _tree[row.ip_prefix] = row
            except Exception as exc:
                print({exc})
                continue
        self._add_cidr_record(_tree, self.extras)
        return _tree

    def extract_isp_data(self, ip: str, tree):
        if ip is None or ip == "":
            return self.EMPTY_RECORD
        try:
            r = tree[ip]
        except KeyError:
            return self.EMPTY_RECORD
        except TypeError:
            return self.EMPTY_RECORD
        else:
            return dict(r._asdict())

    def create_pandas_udf_function(self):
        @F.pandas_udf(
            "ip_prefix string, provider string, region string, service string"
        )
        def build_datafame(ips: pd.Series) -> pd.DataFrame:
            the_database = self._get_database()
            extracted = ips.apply(lambda ip: self.extract_isp_data(ip, the_database))
            return pd.DataFrame(extracted.values.tolist())

        return build_datafame
