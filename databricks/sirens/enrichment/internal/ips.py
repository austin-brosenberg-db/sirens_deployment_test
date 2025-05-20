from typing import Optional

from netaddr import IPAddress, AddrFormatError

from pyspark.sql import Column
import pyspark.sql.functions as F


def is_ip(s: str) -> bool:
    """
    Checks if the given string is IP address (IPv4 or IPv6)
    :param s: string to check
    :return: true if string is IP address
    """
    if s:
        try:
            _ = IPAddress(s)
            return True
        except (ValueError, AddrFormatError):
            pass

    return False


def is_public_ip(s: str) -> Optional[bool]:
    """
    Check if the given string is IP and belongs to public IPs (not loopback, private, etc.).
    Primarily it's used to avoid lookup of MaxMind database for data that doesn't exist
    :param s: string to check
    :return: true if string is global IP address
    """
    if s:
        try:
            return IPAddress(s).is_global()
        except (ValueError, AddrFormatError):
            pass

    return None


def generate_ip_range_condition(cl: Column, tmpl: str, range_start, range_end) -> Column:
    """
    Generates condition that checks if IP address is in the specified range
    :param cl: column with IP address
    :param tmpl: template for IP address range
    :param range_start: start of the range
    :param range_end: end of the range
    :return: condition
    """
    cond = None
    for i in range(range_start, range_end + 1):
        new_cond = F.startswith(cl, F.lit(tmpl.format(i)))
        if cond is None:
            cond = new_cond
        else:
            cond = cond | new_cond

    return cond
