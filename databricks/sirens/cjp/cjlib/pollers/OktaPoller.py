from databricks.sirens.cjp.cjlib.pollers.CrownJewelPoller import CrownJewelPoller
from datetime import datetime, timedelta
import logging
import re
import requests
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
    CjException,
    CjInfrastructureException,
)
from urllib.parse import urlparse
from http import HTTPStatus

"""
Implements class to retrieve data from designated source, filter+rename columns, and write to unique file per invocation in S3.
"""


class OktaPoller(CrownJewelPoller):
    def __init__(
        self,
        name=None,
        config=None,
        schema=None,
        bucket=None,
        profile=None,
        start_timestamp=None,
        end_timestamp=None,
    ):
        """
        Initialize the parent class of this object, then add items specific to this subclass
        :param bucket: (str) name of the bucket to which data should br written
        :param config_file: (str) path/name of JSON config file
        :param profile: (str) AWS profile to be used
        :param schema_file: (str) path/name of JSON schema mapping file
        :param start_timestamp: (str) optional datetime from which data retrieval should start
        :param end_timestamp: (str) optional datetime at which data retrieval should cease
        :return: (None)
        """
        super().__init__(
            name=name,
            config=config,
            schema=schema,
            bucket=bucket,
            profile=profile,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        self._credential_schema = {
            "token": "str",
        }

    def _extract_url(self, headers):
        """
        Extract URL for next record set from OKTA response headers
        :param: (dict) dictionary of HTTP response headers
        :return: (str) URL to retrieve next set of records or (None) if not found
        """
        if "link" in headers:
            for line in headers["link"].split(", "):
                m = re.match(r'<(https:\/\/.*)>; rel="next"', line.strip())
                if m:
                    return m.group(1)

    def _get_data(self):
        """
        OKTA-specific method to incrementally retrieve and store event records.
        :return : (None)
        """
        min_records_in = 100  # if fewer records are retrieved, stop iterating
        max_records_in = 1000  # Okta published limit is 1000
        max_records_out = 15000  # Max records per output file
        datetime_string_format = "%Y-%m-%dT%H%%3A%M%%3A%SZ"
        # see https://developer.okta.com/docs/api/resources/system_log#logevent-object
        cred_info = self._config.get_credentials(self._credential_schema)
        headers = {"Authorization": f"SSWS {cred_info['token']}"}
        params = {
            "limit": max_records_in,
            "sortOrder": "ASCENDING",
        }
        time_range = self._get_time_range()
        if not time_range:
            cursor = self._config.get_cursor()
        else:
            cursor = (
                self._config.get_url()
                + "?since="
                + time_range["start"]
                + "&until="
                + time_range["end"]
            )
        done = False
        buffered = 0
        data = list()

        while not done:
            if cursor is None:
                logging.info("cursor is empty")
                url = self._config.get_url()
            else:
                # Only print the cursor when starting a new output buffer or debugging
                if buffered:
                    logging.debug(f"cursor is valid, cursor: {cursor}")
                else:
                    logging.info(f"cursor is valid, cursor: {cursor}")
                url = cursor

            start = self._now()
            okta_domain = urlparse(url).netloc  # Extract domain name from API URL
            try:
                okta_response = requests.request(
                    "GET", url=url, headers=headers, params=params
                )
                okta_records = okta_response.json()
                cursor = self._extract_url(okta_response.headers)
                if okta_response.status_code < 200 or okta_response.status_code > 299:
                    raise requests.exceptions.RequestException(okta_response.text)
            except Exception as e:
                logging.critical(
                    f"Problem retrieving data from {self._config.get_url()} : {e}"
                )
                raise CjException(
                    self.name,
                    f"Problem retrieving data from {self._config.get_url()}",
                    e,
                ) from None

            end = self._now()
            ct = len(okta_records)
            logging.debug(f"{ct} events retrieved in {end - start}ms ")
            # Stop polling when a small number of records are returned
            done = True if ct < min_records_in else False
            start = end
            for record_in in okta_records:
                logging.debug(f"IN: {record_in}")
                record_in["retrievalTimestamp"] = end
                # If the record lacks source timestamp data, keep this one to be safe
                record_out = self._schema.transform_one(record_in)
                record_out["oktaURL"] = okta_domain
                data.append(record_out)

            end = self._now()
            logging.debug(f"{len(data)} new events after {end - start}ms of filtering")
            buffered += ct
            if done or buffered >= max_records_out:
                if not time_range:
                    yield (data, cursor)
                else:
                    yield (data, None)
                data = list()
                buffered = 0
