import pyspark.sql.functions as F
import pandas as pd

from .enrichment import PandasFunctionEnrichmentBase

from ua_parser import user_agent_parser


class UserAgentEnrichment(PandasFunctionEnrichmentBase):
    EMPTY_RECORD = {
        'device_brand': None,
        'device_family': None,
        'device_model': None,
        "os_family": None,
        'os_major': None,
        'os_minor': None,
        'os_patch': None,
        'os_patch_minor': None,
        'user_agent_family': None,
        'user_agent_major': None,
        'user_agent_minor': None,
        'user_agent_patch': None
    }

    def __init__(self, src_column_name_or_expr: str, dest_column_name: str):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        """
        super().__init__("user_agent", src_column_name_or_expr, dest_column_name)

    def create_pandas_udf_function(self):
        def get_user_agent_data(s: str):
            if s is None or s.strip() == "":
                return self.EMPTY_RECORD

            parsed = user_agent_parser.Parse(s)
            dvc = parsed.get("device", {})
            os = parsed.get("os", {})
            agent = parsed.get("user_agent", {})

            return {
                'device_brand': dvc.get("brand"),
                'device_family': dvc.get("family"),
                'device_model': dvc.get("model"),
                "os_family": os.get("family"),
                'os_major': os.get("major"),
                'os_minor': os.get("minor"),
                'os_patch': os.get("patch"),
                'os_patch_minor': os.get("patch_minor"),
                'user_agent_family': agent.get("family"),
                'user_agent_major': agent.get("major"),
                'user_agent_minor': agent.get("minor"),
                'user_agent_patch': agent.get("patch")
            }

        @F.pandas_udf("device_brand string, device_family string, device_model string, os_family string, os_major "
                      "string, os_minor string, os_patch string, os_patch_minor string, user_agent_family string, "
                      "user_agent_major string, user_agent_minor string, user_agent_patch string")
        def user_agent_udf_func(col: pd.Series) -> pd.DataFrame:
            extracted = col.apply(get_user_agent_data)
            return pd.DataFrame(extracted.values.tolist())

        return user_agent_udf_func
