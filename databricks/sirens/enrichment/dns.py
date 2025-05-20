import requests
import pandas as pd

from typing import Dict, Any, Union
from concurrent.futures import ThreadPoolExecutor

from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

from .enrichment import PandasFunctionEnrichmentBase


def _http_get_json(url, session=None, timeout=None):
    if session is None:
        session = requests
    if timeout is None:
        timeout = 3
    try:
        resp = session.get(url, timeout=timeout)
        return resp.json()
    except:
        return {}


def _dns_raw(name, type, session=None, timeout=None):
    return _http_get_json(
        f'https://dns.google/resolve?name={name}&type={type}&edns_client_subnet=0.0.0.0/0',
        session=session,
        timeout=timeout
    )


def _dns_a_raw(name, session=None, timeout=None):
    return _dns_raw(name, 'A', session=session, timeout=timeout)


def _dns_ptr_raw(ip, session=None, timeout=None):
    ptr_ip = '.'.join(reversed(ip.split('.'))) + '.in-addr.arpa'
    return _dns_raw(ptr_ip, 'PTR', session=session, timeout=timeout)


def _dns_a(name, cache=None, session=None, timeout=None):
    if not name:
        return []

    if cache and cache.get(name) is not None:
        return cache.get(name)

    ips = []
    response = _dns_a_raw(name, session=session, timeout=timeout)
    for answer in response.get('Answer', []):
        if answer['type'] == 1:  # A record
            ips.append(answer['data'])
    if cache:
        cache[name] = ips
    return ips


def _dns_ptr(ip, cache=None, session=None, timeout=None):
    if not ip:
        return ''

    if cache:
        c = cache.get(ip)
        if c is not None:
            return c

    response = _dns_ptr_raw(ip, session=session, timeout=timeout)
    for answer in response.get('Answer', []):
        if answer['type'] == 12:  # PTR record
            name = answer['data']
            if cache:
                cache[ip] = name
            return name
    if cache:
        cache[ip] = ''
    return ''


class DnsResolutionEnrichment(PandasFunctionEnrichmentBase):
    def __init__(self, ip_column_name_or_expr: str, dest_column_name: str = 'dns', parallelism=50, timeout=3):
        super().__init__('dns', ip_column_name_or_expr, dest_column_name)
        self.parallelism = parallelism
        self.timeout = timeout

    def create_pandas_udf_function(self):
        @pandas_udf("array<string>")
        def dns_resolve_parallel(domain_series):
            cache = {}
            session = requests.Session()
            with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
                future = executor.map(lambda domain: _dns_a(domain, cache, session, self.timeout), domain_series)
                return pd.Series([item for item in future])

        return dns_resolve_parallel


class RDnsResolutionEnrichment(PandasFunctionEnrichmentBase):
    def __init__(self, ip_column_name_or_expr: str, dest_column_name: str = 'rdns', parallelism=50, timeout=3):
        super().__init__('rdns', ip_column_name_or_expr, dest_column_name)
        self.parallelism = parallelism
        self.timeout = timeout

    def create_pandas_udf_function(self):
        @pandas_udf("string")
        def rdns_resolve_parallel(ip_series):
            cache = {}
            session = requests.Session()
            with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
                future = executor.map(lambda ip: _dns_ptr(ip, cache, session, self.timeout), ip_series)
                return pd.Series([item for item in future])

        return rdns_resolve_parallel
