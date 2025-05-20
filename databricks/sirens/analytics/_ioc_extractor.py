"""Module to extract IOCs from a DataFrame
Module to extract IOCs from a DataFrame.
Classes:
    IOCTypes: A class to manage and define various types of IOCs (Indicators of Compromise).
    IOCExtractor: A class to extract IOCs from a given DataFrame.
Functions:
    _ioc_extract(df: DataFrame, search_cols: List[Column] = None,
        Extracts IOCs from the given DataFrame based on specified columns and IOC types.
IOCTypes:
    Attributes:
        _IPV4_REGEX (str): Regular expression for matching IPv4 addresses.
        _URL_REGEX (str): Regular expression for matching URLs.
        _DNS_REGEX (str): Regular expression for matching DNS names.
        _EMAIL_USER_REGEX (str): Regular expression for matching email usernames.
        _EMAIL_REGEX (str): Regular expression for matching email addresses.
        _WINPATH_REGEX (str): Regular expression for matching Windows file paths.
        _SHA256_REGEX (str): Regular expression for matching SHA-256 hashes.
        _MD5_REGEX (str): Regular expression for matching MD5 hashes.
        _SHA1_REGEX (str): Regular expression for matching SHA-1 hashes.
        _ioc_types (dict): Dictionary containing IOC types and their corresponding regex and weight.
    Methods:
        __init__(self, add_ioc_types: List[IOCType] = None):
            Initializes the IOCTypes class with optional additional IOC types.
        _set_ioc_type_attrs(self):
            Sets the IOC type attributes for the class.
        list(cls, ioc_type: str = None, regex: bool = False, detail: bool = False):
            Lists the available IOC types or details based on the parameters.
        _compile_regex(regex):
            Compiles the given regular expression with specific flags.
        show_ioc_types(self):
            Returns the instance variables of the class.
        instance_vars(self):
            Returns the instance variables of the class.
        show_ioc_type(self, ioc_type):
            Returns the specified IOC type.
IOCExtractor:
    Methods:
        __init__(self, search_ioc_types: List[str] = None, add_ioc_type: List[IOCType] = None):
            Initializes the IOCExtractor class with optional search and additional IOC types.
        _process_col(self, search_string, col_name, idx):
            Processes a column to extract IOCs.
        _process_row(self, df_row, idx, cols) -> List:
            Processes a row to extract IOCs.
        _extract(self, df, search_cols: List[Column] = None) -> Union[DataFrame | None]:
            Extracts IOCs from the given DataFrame based on specified columns.
_ioc_extract:
    Extracts IOCs from the given DataFrame based on specified columns and IOC types.
    Parameters:
        df (DataFrame): The DataFrame to extract IOCs from.
        search_cols (List[Column], optional): The columns to search for IOCs. Defaults to None.
        search_ioc_types (List[IOCType], optional): The IOC types to search for. Defaults to None.
        add_ioc_types (List[IOCType], optional): Additional IOC types to add. Defaults to None.
        output_format (OutputFormat, optional): The format of the output. Defaults to OutputFormat.asDataFrame.
    Returns:
        Union[DataFrame | None]: The DataFrame containing extracted IOCs or None if no IOCs are found.

    Author: Derek King
    Date: June 2024
    Version 1.0


"""
import re
from typing import List, Union, Optional

from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.types import StructType

from databricks.sirens.analytics.domain_utils import DomainUtils
from databricks.sirens.common.entities import OutputFormat, IOCType
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)

spark = SparkSession.getActiveSession()


class IOCTypes:
    """
    A class used to represent and manage different types of Indicators of Compromise (IOCs).
    Attributes
    ----------
    _IPV4_REGEX : str
        Regular expression pattern for matching IPv4 addresses.
    _URL_REGEX : str
        Regular expression pattern for matching URLs.
    _DNS_REGEX : str
        Regular expression pattern for matching DNS names.
    _EMAIL_USER_REGEX : str
        Regular expression pattern for matching email usernames.
    _EMAIL_REGEX : str
        Regular expression pattern for matching email addresses.
    _WINPATH_REGEX : str
        Regular expression pattern for matching Windows file paths.
    _SHA256_REGEX : str
        Regular expression pattern for matching SHA-256 hashes.
    _MD5_REGEX : str
        Regular expression pattern for matching MD5 hashes.
    _SHA1_REGEX : str
        Regular expression pattern for matching SHA-1 hashes.
    _ioc_types : dict
        Dictionary containing IOC types and their associated regex patterns and weights.
    Methods
    -------
    __init__(add_ioc_types: List[IOCType] = None)
        Initializes the IOCTypes instance, optionally adding additional IOC types.
    _set_ioc_type_attrs()
        Sets the IOC type attributes for the instance.
    list(ioc_type: str = None, regex: bool = False, detail: bool = False)
        Class method to list IOC types, optionally filtering by type, regex, or detail.
    _compile_regex(regex)
        Static method to compile a regex pattern with specific flags.
    show_ioc_types
        Property to show all IOC types as instance variables.
    instance_vars
        Property to show all instance variables.
    show_ioc_type(ioc_type)
        Method to show a specific IOC type.
    """

    _IPV4_REGEX = r"(?P<ipaddress>(?:[0-9]{1,3}\.){3}[0-9]{1,3})"
    _IPV4_REGEX = r"(?P<ipaddress>(\d+\.\d+\.\d+\.\d+))"
    _URL_REGEX = r"""
            (?P<protocol>(https?|s?ftps?|telnet|ldap|file)://)
            (?P<userinfo>([a-z0-9-._~!$&\'()*+,;=:]|%[0-9A-F]{2})*@)?
            (?P<host>([a-z0-9-._~!$&\'()*+,;=]|%[0-9A-F]{2})*)
            (:(?P<port>\d*))?
            (/(?P<path>([^?\#"<>\s]|%[0-9A-F]{2})*/?))?
            (\?(?P<query>([a-z0-9-._~!$&'()*+,;=:/?@]|%[0-9A-F]{2})*))?
            (\#(?P<fragment>([a-z0-9-._~!$&'()*+,;=:/?@]|%[0-9A-F]{2})*))?"""

    _DNS_REGEX = r"((?=[a-z0-9-]{1,63}\[?\.\]?)[a-z0-9]+(-[a-z0-9]+)*\[?\.\]?){1,126}[a-z]{2,63}"

    _EMAIL_USER_REGEX = r"(?P<user>[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+)"
    _EMAIL_REGEX = f"{_EMAIL_USER_REGEX}@(?P<domain>{_DNS_REGEX})"

    _WINPATH_REGEX = r"""
            (?P<root>[a-z]:|\\\\[a-z0-9_.$-]+||[.]+)
            (?P<folder>\\(?:[^\/:*?"\'<>|\r\n]+\\)*)
            (?P<file>[^\\/*?""<>|\r\n ]+)"""

    _SHA256_REGEX = r"(?:^|[^A-Fa-f0-9])(?P<hash>[A-Fa-f0-9]{64})(?:$|[^A-Fa-f0-9])"
    _MD5_REGEX = r"(?:^|[^A-Fa-f0-9])(?P<hash>[A-Fa-f0-9]{32})(?:$|[^A-Fa-f0-9])"
    _SHA1_REGEX = r"(?:^|[^A-Fa-f0-9])(?P<hash>[A-Fa-f0-9]{40})(?:$|[^A-Fa-f0-9])"

    _ioc_types = {
        "ipv4": {"regex": _IPV4_REGEX, "weight": 0},
        "email": {"regex": _EMAIL_REGEX, "weight": 1},
        "url": {"regex": _URL_REGEX, "weight": 0},
        "dns": {"regex": _DNS_REGEX, "weight": 0},
        "md5": {"regex": _MD5_REGEX, "weight": 1},
        "sha1": {"regex": _SHA1_REGEX, "weight": 1},
        "sha256": {"regex": _SHA256_REGEX, "weight": 1},
        "winpath": {"regex": _WINPATH_REGEX, "weight": 3}
    }

    def __init__(self, add_ioc_types: List[IOCType] = None):
        if add_ioc_types:
            for itm in add_ioc_types:
                self._ioc_types.update({itm.value: {"regex": itm.regex, "weight": itm.weight}})
        self._set_ioc_type_attrs()

    def _set_ioc_type_attrs(self):
        [setattr(self, k.upper(), IOCType(k, v.get('regex'), v.get('weight'))) for k, v in self._ioc_types.items()]
        return self

    @classmethod
    def list(cls, ioc_type: str = None, regex: bool = False, detail: bool = False):
        if ioc_type:
            return cls._ioc_types.get(ioc_type)
        if regex and ioc_type:
            return cls._ioc_types.get(ioc_type).get('regex')
        if detail:
            return list(cls._ioc_types.items())

        return list(cls._ioc_types.keys())

    @staticmethod
    def _compile_regex(regex):
        return re.compile(regex, re.I | re.X | re.M)

    @property
    def show_ioc_types(self):
        return vars(self)

    @property
    def instance_vars(self):
        return vars(self)

    @staticmethod
    def show_ioc_type(self, ioc_type):
        return ioc_type


class IOCExtractor:
    def __init__(self, search_ioc_types: List[str] = None, add_ioc_type: List[IOCType] = None):
        """
        Initializes the IOCExtractor.
        :param search_ioc_types: A list of IOC types to search for. If provided, the ioc_types will be scoped down to these types.
        :type search_ioc_types: List[str], optional
        :param add_ioc_type: A list of IOCType objects to add to the ioc_types.
        :type add_ioc_type: List[IOCType], optional
        """
        self.ioc_types = IOCTypes(add_ioc_type).instance_vars
        self.spark = SparkSession.getActiveSession()

        # update the ioc_types if requested by user to scope down.
        if search_ioc_types:
            new_ioc_types = {}
            [new_ioc_types.update({t.upper(): self.ioc_types[t.upper()]}) for t in search_ioc_types if
             t.upper() in self.ioc_types.keys()]
            self.ioc_types = new_ioc_types

    def _process_col(self, search_string, col_name, idx):
        """
        Processes a column to extract indicators of compromise (IOCs) based on a search string.
        :param search_string: The string to search for IOCs.
        :type search_string: str
        :param col_name: The name of the column being processed.
        :type col_name: str
        :param idx: The index of the row being processed.
        :type idx: int
        :return: A list of tuples containing the IOC type, matched string, index, column name, and search string.
        :rtype: list of tuple
        """

        iocs = []
        for k, v in self.ioc_types.items():
            rgx_def = IOCTypes._compile_regex(v.regex)
            match_pos = 0
            for rgx_match in rgx_def.finditer(str(search_string), match_pos):
                if rgx_match is None:
                    break

                match_str = rgx_match.group()

                # remove anything matched - but not a registered tld
                if 'DNS' in k and not DomainUtils.is_valid_tld(match_str):
                    continue

                # add match
                iocs.append((k, match_str, idx, col_name, search_string))

                match_pos = rgx_match.end()
        return iocs

    def _process_row(self, df_row, idx, cols) -> List:
        """
        Process a row of a DataFrame and extract information from specified columns.
        :param df_row: The row of the DataFrame to process.
        :type df_row: pandas.Series
        :param idx: The index of the row in the DataFrame.
        :type idx: int
        :param cols: The list of columns to process.
        :type cols: List[str]
        :return: A list of extracted information from the specified columns.
        :rtype: List
        """

        results = []
        # process each requested column
        [results.extend(self._process_col(df_row[c], c, idx)) for c in cols]
        return results

    def _extract(self, df, search_cols: List[Column] = None) -> Optional[DataFrame]:
        """
        Extracts indicators of compromise (IOCs) from the given DataFrame.
        This method processes each row of the DataFrame to extract IOCs based on the specified search columns.
        If no search columns are specified, all columns in the DataFrame are searched.
        :param df: The input DataFrame to search for IOCs.
        :type df: DataFrame
        :param search_cols: List of columns to search for IOCs. Can be a list of Column objects or a comma-separated string of column names. If None, all columns are searched.
        :type search_cols: List[Column] or str or None
        :return: A DataFrame containing the extracted IOCs with columns ["ioc_type", "observable", "orig_row", "orig_column", "src_string"]. If no IOCs are found, an empty DataFrame is returned.
        :rtype: Union[DataFrame, None]
        """
        results = []
        # add all columns to search if not specified
        if not search_cols:
            search_cols = df.columns

        # handle cols passed as comma sep string
        if isinstance(search_cols, str):
            search_cols = search_cols.replace(" ", "").split(",")

        # process each row
        for idx, df_row in df.toPandas().iterrows():
            results.extend(self._process_row(df_row, idx, search_cols))

        if results:
            return self.spark.createDataFrame(results,
                                              ["ioc_type", "observable", "orig_row", "orig_column", "src_string"])
        else:
            logger.info("No IOCs found in the DataFrame.")

        return self.spark.createDataFrame([], StructType([]))


def _ioc_extract(df: DataFrame, search_cols: List[Column] = None,
                 search_ioc_types: List[IOCType] = None,
                 add_ioc_types: List[IOCType] = None,
                 output_format: OutputFormat = OutputFormat.asDataFrame) -> Union[DataFrame, None]:
    """
    Extracts Indicators of Compromise (IOCs) from the given DataFrame based on specified search columns and IOC types.
    :param df: The input DataFrame containing the data to be searched.
    :type df: DataFrame
    :param search_cols: List of columns to search for IOCs. If None, all columns will be searched.
    :type search_cols: List[Column], optional
    :param search_ioc_types: List of IOC types to search for. If None, all IOC types will be considered.
    :type search_ioc_types: List[IOCType], optional
    :param add_ioc_types: Additional IOC types to include in the search. Can be a single IOCType or a list of IOCType.
    :type add_ioc_types: List[IOCType], optional
    :param output_format: The format of the output. Defaults to OutputFormat.asDataFrame.
    :type output_format: OutputFormat, optional
    :return: A DataFrame containing the extracted IOCs or None if invalid parameters are provided.
    :rtype: Union[DataFrame, None]
    """
    if search_ioc_types and not isinstance(search_ioc_types, list):
        print("Invalid IOC types - must be passed as list")
        return

    if add_ioc_types and isinstance(add_ioc_types, IOCType):
        add_ioc_types = [add_ioc_types]

    if add_ioc_types and (not isinstance(add_ioc_types, list) and not isinstance(add_ioc_types[0], IOCType)):
        print("Invalid IOC type addition - must be passed as List[IOCType]")
        return

    ioc_extractor = IOCExtractor(search_ioc_types, add_ioc_type=add_ioc_types)
    return ioc_extractor._extract(df, search_cols)
