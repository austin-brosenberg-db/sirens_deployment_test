"""Free Email Provider Features Extraction / Enrichment"""
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from .free_email_provider_domains import domains as _free_email_provider_domains


# TODO: switch to at least to pandas_udf, or implement as a join?
def uses_free_email_provider(email_or_domain: str):
    if email_or_domain is None:
        return False

    idx = email_or_domain.find('@')
    if idx != -1:
        domain = email_or_domain[idx + 1:]
    else:
        domain = email_or_domain

    return domain in _free_email_provider_domains


uses_free_email_provider_udf = udf(uses_free_email_provider, BooleanType())
