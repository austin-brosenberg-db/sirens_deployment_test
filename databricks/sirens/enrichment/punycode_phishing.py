import re

import idna
import pandas as pd
import pyspark.sql.functions as F
import textdistance
import tldextract
from confusables import confusable_characters
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType

from .enrichment import PandasFunctionEnrichmentBase


def get_confusable_regex(name):
    pattern = ''
    for letter in name:
        c = confusable_characters(letter)
        pattern += '(' + ('|'.join(c)) + ')'
    return re.compile(pattern)


schema = StructType([
    StructField("matches", BooleanType(), True),
    StructField("pattern_name", StringType(), True),
    StructField("unicode_domain", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("normalized_domain", StringType(), True),
    StructField("error_msg", StringType(), True),
    StructField("score", FloatType(), True)
])

tld_extract = tldextract.TLDExtract(suffix_list_urls=())


class PunycodePhishingEnrichment(PandasFunctionEnrichmentBase):
    def __init__(self, src_column_name_or_expr: str, dest_column_name: str, patterns: list):
        """
        Initializes class
        :param src_column_name_or_expr: source column
        :param dest_column_name: destination column
        """
        super().__init__("domain", src_column_name_or_expr, dest_column_name)
        self.confusable_patterns = dict([(name, get_confusable_regex(name)) for name in patterns])

    def create_pandas_udf_function(self):
        def matches_phishing_pattern(domain):
            if not domain.startswith('xn--'):
                return {
                    'matches': False,
                    'pattern_name': '',
                    'unicode_domain': '',
                    'normalized_domain': '',
                    'domain': domain,
                    'error_msg': 'Not a punycode domain',
                    'score': 0.0
                }

            error_msg = ''
            try:
                unicode_domain = idna.decode(domain)
            except Exception as e:
                error_msg = str(e)
                unicode_domain = ''

            # remove '-' and equiv
            normalized_domain = re.sub(get_confusable_regex('-'), '', unicode_domain)
            tld = tld_extract(normalized_domain)
            normalized_domain = tld.subdomain + tld.domain

            scores = {}

            for name, regex in self.confusable_patterns.items():
                scores[name] = textdistance.jaro_winkler(normalized_domain, name)
                if re.search(regex, normalized_domain):
                    return {
                        'matches': True,
                        'pattern_name': name,
                        'unicode_domain': unicode_domain,
                        'normalized_domain': normalized_domain,
                        'domain': domain,
                        'error_msg': error_msg,
                        'score': scores[name]
                    }
            max_name, max_score = max(scores.items(), key=lambda x: x[1])
            if max_score < 0.7:
                max_name = ''
                max_score = 0.0

            return {
                'matches': False,
                'pattern_name': max_name,
                'unicode_domain': unicode_domain,
                'normalized_domain': normalized_domain,
                'domain': domain,
                'error_msg': error_msg,
                'score': max_score,
            }

        @F.pandas_udf(schema)
        def matches_phishing_pattern_func(col: pd.Series) -> pd.DataFrame:
            extracted = col.apply(matches_phishing_pattern)
            return pd.DataFrame(extracted.values.tolist())

        return matches_phishing_pattern_func
