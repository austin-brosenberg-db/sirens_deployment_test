import os
import re
import shutil
import tempfile
from abc import abstractmethod
from typing import Dict, Any, Union, Optional

import geoip2.errors
import pandas as pd
import pyspark.sql.functions as F
from geoip2 import database
from pyspark.sql import Column

from .enrichment import PandasFunctionEnrichmentBase
from .internal.ips import is_public_ip, generate_ip_range_condition


class MaxMindEnrichmentBase(PandasFunctionEnrichmentBase):
    """Base class for all enrichment implementations based on the MaxMind databases
    """

    def __init__(self, name: str, db_file: str, ip_column_name_or_expr: str, dest_column_name):
        """
        Initializer
        :param name: implementation name
        :param db_file: path to file with MaxMind database (it could be a local file, file on WSFS or Volumes)
        :param ip_column_name_or_expr: name of the column with IP information or SQL expression
        :param dest_column_name: name of the column in which data will be stored
        """
        super().__init__(name, ip_column_name_or_expr, dest_column_name)
        idx = db_file.rfind("/")
        if idx == -1:
            raise Exception(f"Please specify correct file name! got '{db_file}'")

        if db_file.startswith("dbfs:/"):
            self._dbfs_file_name = "/dbfs" + re.sub(r"^dbfs:(.*)$", r"\1", db_file)
        else:
            self._dbfs_file_name = db_file

        self._file_name = db_file[(idx + 1):]

        self._ip_column_name_or_expr = ip_column_name_or_expr
        self._dest_column_name = dest_column_name
        self.__local_tmp_directory__ = tempfile.gettempdir()

    def _copy_db_file(self, local_path):
        fd, tmp_name = tempfile.mkstemp(dir=self.__local_tmp_directory__)
        os.close(fd)
        shutil.copy2(self._dbfs_file_name, tmp_name)
        os.rename(tmp_name, local_path)

    def _get_database(self):
        """
        Returns a file name to read database from.
        :return: database file name
        """
        db_name = self._dbfs_file_name
        try:
            local_path = os.path.join(self.__local_tmp_directory__, self._file_name)
            if not os.path.exists(local_path):
                # print("No local copy found, copying")
                os.makedirs(self.__local_tmp_directory__, exist_ok=True)
                self._copy_db_file(local_path)
            else:
                # print("Local copy is older than remote copy, copying new version")
                lstat = os.stat(local_path)
                rstat = os.stat(self._dbfs_file_name)
                if rstat.st_mtime > lstat.st_mtime:
                    self._copy_db_file(local_path)
                db_name = local_path
        except OSError as e:
            print(f"OS error occurred, using remote file directly: {e}")

        return database.Reader(db_name)

    @abstractmethod
    def create_pandas_udf_function(self):
        """Implementations need to override this function with a function that will return
         UDF/Pandas UDF that will perform actual enrichment
        """
        pass

    @abstractmethod
    def __type__for_null__(self) -> str:
        pass

    def get_column(self, src: Optional[str] = None, alias: Optional[str] = None) -> Column:
        if not src:
            src = self._src_column_name_or_expr
        if not alias:
            alias = self._dest_column_name

        cl = F.expr(src)
        # TODO: add more checks for private IP addresses (RFC 3330)
        skip_cond = (cl.isNull() | (cl == "") | (~F.contains(cl, F.lit(".")) & ~F.contains(cl, F.lit(":"))) |
                     F.startswith(cl, F.lit("127.")) | F.startswith(cl, F.lit("192.168.")) |
                     F.startswith(cl, F.lit("10.")) | F.startswith(cl, F.lit("169.254.")) |
                     generate_ip_range_condition(cl, "172.{}.)", 16, 31))
        new_cl = (F.when(skip_cond, F.lit(None).cast(self.__type__for_null__())).otherwise(
            self.create_pandas_udf_function()(cl))).alias(alias)

        return new_cl


class GeoIPEnrichment(MaxMindEnrichmentBase):
    """Class that enriches DataFrame with GeoIP data
    """
    EMPTY_RECORD = {'city': None, 'country': None, 'country_code': None,
                    'latitude': None, 'longitude': None, 'accuracy_radius': None}

    def __init__(self, db_file: str, ip_column_name_or_expr: str, dest_column_name: str = "geo"):
        super().__init__("GeoIP", db_file, ip_column_name_or_expr, dest_column_name)
        """

        :param db_file: path to file with MaxMind database
        :param ip_column_name_or_expr: name of the column with IP information or SQL expression
        :param dest_column_name: name of the column in which data will be stored
        """

    def __type__for_null__(self):
        return ("struct<city:string, country:string, country_code:string, latitude:double, "
                "longitude:double, accuracy_radius:int>")

    def create_pandas_udf_function(self):
        def extract_geoip_data(ip: str, geocity, cache: Dict[str, Union[str, Dict[str, Any]]]):
            cv = cache.get(ip)
            if cv is not None:
                if isinstance(cv, str):
                    return None
                return cv
            rc: Union[str, Dict[str, Any]] = GeoIPEnrichment.EMPTY_RECORD
            if ip and is_public_ip(ip):
                try:
                    record = geocity.city(ip)
                    rc = {
                        'city': record.city.name,
                        'country': record.country.name,
                        'country_code': record.country.iso_code,
                        'latitude': record.location.latitude,
                        'longitude': record.location.longitude,
                        'accuracy_radius': record.location.accuracy_radius
                    }
                except (geoip2.errors.AddressNotFoundError, ValueError):
                    pass

            cache[ip] = rc

            return rc

        @F.pandas_udf(
            "city string, country string, country_code string, latitude double, longitude double, accuracy_radius int")
        def get_geoip_data(ips: pd.Series) -> pd.DataFrame:
            geocity = self._get_database()
            cache: Dict[str, Union[str, Dict[str, Any]]] = {}
            extracted = ips.apply(lambda ip: extract_geoip_data(ip, geocity, cache))

            return pd.DataFrame(extracted.values.tolist())

        return get_geoip_data

class ASNEnrichment(MaxMindEnrichmentBase):
    """Class that enriches DataFrame with data about Autonomous System (AS)
    """
    EMPTY_RECORD = {'as_number': None, 'as_org': None, 'as_network': None}

    def __init__(self, db_file: str, ip_column_name_or_expr: str, dest_column_name: str = "as_data"):
        super().__init__("ASN", db_file, ip_column_name_or_expr, dest_column_name)
        """

        :param db_file: path to file with MaxMind database
        :param ip_column_name_or_expr: name of the column with IP information or SQL expression
        :param dest_column_name: name of the column in which data will be stored
        """

    def __type__for_null__(self):
        return "struct<as_number:int, as_org:string, as_network:string>"

    def create_pandas_udf_function(self):
        def extract_asn_data(ip: str, asn, cache: Dict[str, Union[str, Dict[str, Any]]]):
            cv = cache.get(ip)
            if cv is not None:
                if isinstance(cv, str):
                    return None
                return cv
            rc: Union[str, Dict[str, Any]] = ASNEnrichment.EMPTY_RECORD
            if ip and is_public_ip(ip):
                try:
                    record = asn.asn(ip)
                    rc = {'as_number': record.autonomous_system_number,
                          'as_org': record.autonomous_system_organization,
                          'as_network': str(record.network)}
                except (geoip2.errors.AddressNotFoundError, ValueError):
                    pass

            cache[ip] = rc

            return rc

        @F.pandas_udf("as_number int, as_org string, as_network string")
        def get_asn_data(ips: pd.Series) -> pd.DataFrame:
            asn = self._get_database()
            cache: Dict[str, Union[str, Dict[str, Any]]] = {}
            extracted = ips.apply(lambda ip: extract_asn_data(ip, asn, cache))

            return pd.DataFrame(extracted.values.tolist())

        return get_asn_data
