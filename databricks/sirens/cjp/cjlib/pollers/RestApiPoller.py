from databricks.sirens.cjp.cjlib.pollers.CrownJewelPoller import CrownJewelPoller
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjException,
)
import base64
from datetime import datetime, timedelta
import logging
import pytz
import requests

"""
Implements class to retrieve data from designated source, filter+rename columns, and write to unique file per invocation in S3.
"""


class RestApiPoller(CrownJewelPoller):
    def __init__(
        self: object,
        name: str = None,
        config: dict = None,
        schema: dict = None,
        bucket: str = None,
        profile: str = None,
        start_timestamp: str = None,
        end_timestamp: str = None,
    ) -> None:
        """
        Initialize the parent class of this object, then add items specific to this subclass

        Args:
            self: RestApiPoller object
            name: name of the poller -- typically alluding to data source
            config: poller configuration parameters
            schema:  desired data transformations, if any
            bucket: name of the bucket to which data should br written
            profile: name of the AWS profile to be used
            start_timestamp: optional datetime from which data retrieval should start
            end_timestamp: optional datatime at which data retrieval should cease
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

    def _get_headers(self: object) -> dict:
        """
        Crafts REST API headers based on information from config and secret storage request headers for the REST API calls.

        Args:
            self: RestApiPoller object
        Returns:
            headers required for API calls.
        Raises:
            CjException if insufficient or unusable information is available from config
        """

        _authentication = self._config.get_authentication()

        if _authentication:
            _auth_type = _authentication["type"]
            _auth_criteria = _authentication["auth_criteria"]

            logging.info(f"In _get_headers, authentication type is: {_auth_type}")

            if _auth_type == "Basic":

                cred_info = self._config.get_credentials(_auth_criteria)
                if cred_info:
                    username = cred_info[list(_auth_criteria)[0]]
                    password = cred_info[list(_auth_criteria)[1]]
                    auth_bytes = f"{username}:{password}".encode("ascii")
                    base64_auth = base64.b64encode(auth_bytes).decode("ascii")
                    headers = {"Authorization": "Basic %s" % base64_auth}
                else:
                    raise CjException(
                        self.name,
                        None,
                        "Authentication criteria is needed for Basic API authentication",
                    )
            elif _auth_type == "Bearer":
                cred_info = self._config.get_credentials(_auth_criteria)
                api_token = cred_info[list(_auth_criteria)[0]]
                headers = {
                    "Authorization": "Bearer %s" % api_token,
                    "Accept": "application/json",
                }
            else:
                raise CjException(
                    self.name,
                    None,
                    "Authentication type:{_auth_type} is currently not supported",
                )
        else:
            raise CjException(
                self.name, None, "Authentication information is mandatory"
            )

        return headers

    def _process_response_data(
        self: object, raw_data: dict, time_range: dict, cursor: str
    ) -> dict:
        """
        Processes the response data based on defined schema.

        Args:
            self: RestApiPoller object
            raw_data: the response json from the REST API calls.
            time_range: time_range
            cursor: value to override data cursor or (None)
        Returns:
            (dictionary) of (list of dict) new data records and (str) data cursor
        """
        # Read the timestamp of the last record previously ingested, if any
        if cursor:
            logging.info(f"cursor was {cursor}")
            prev_cursor = datetime.fromisoformat(cursor)
        else:
            logging.info(f"cursor was not set")
            prev_cursor = None

        _new_cursor = prev_cursor

        _processed_rspns_data = dict()
        _processed_data = list()

        start = self._now()
        _response_root_entry = None

        _api_response = self._config.get_api_response()

        if _api_response:
            _response_root_entry = (
                _api_response["root_entry"] if _api_response["root_entry"] else None
            )

        data_rows = (
            raw_data[_response_root_entry] if (_response_root_entry) else raw_data
        )
        ct = len(data_rows)

        end = self._now()
        logging.info(f"{ct} events retrieved in {end - start}ms ")

        if ct:
            # Determine which input field contains the source timestamp
            (cursor_in, cursor_out) = self._schema.get_cursor_field()
            stateless = cursor_in is None
            logging.debug(f"cursor: {cursor_out} ({cursor_in})")

            now = datetime.now()
            for record_in in data_rows:
                logging.debug(f"IN: {record_in}")
                record_in["retrievalTimestamp"] = now.isoformat()
                # If the record lacks source timestamp data, keep this one to be safe
                if stateless:
                    record_out = self._schema.transform_one(record_in)
                    logging.debug(f"OUT: {record_out}")
                    _processed_data.append(record_out)
                elif cursor_in and cursor_in not in record_in:
                    logging.debug(f"no {cursor_in} in {record_in}")
                    record_out = self._schema.transform_one(record_in)
                    _processed_data.append(record_out)
                else:
                    stamp = self._schema.get_datetime(record_in[cursor_in], cursor_out)
                    if stamp.tzinfo is None or stamp.tzinfo.utcoffset(stamp) is None:
                        stamp = stamp.replace(tzinfo=pytz.utc)
                        stamp = pytz.utc.localize(stamp)
                    logging.debug(f"stamp {stamp} from {cursor_in}")
                    # If the record had a source timestamp later than records previously retrieved
                    # or there were no previsouly retrieved records, keep this one
                    end_cursor = (
                        time_range["end_timestamp"]
                        if time_range
                        else datetime.now(tz=pytz.utc) + timedelta(hours=1)
                    )
                    if (prev_cursor is None) or (prev_cursor < stamp < end_cursor):
                        record_out = self._schema.transform_one(record_in)
                        logging.debug(f"OUT: {record_out}")
                        _processed_data.append(record_out)
                        # Note the latest source timestamp retrieved
                        if _new_cursor is None or stamp > _new_cursor:
                            _new_cursor = stamp
                    # If the record is older than previously retrieved ones, skip it (assume that it
                    # had already been imported
                    else:
                        logging.debug(f"stamp {stamp} <= {prev_cursor}")

            _processed_rspns_data["processed_data"] = _processed_data
            _processed_rspns_data["new_cursor"] = _new_cursor

        return _processed_rspns_data

    def _get_data(self, cursor: str = None) -> tuple:
        """
        retrieve new data from configured source

        Arge:
            cursor: (str) to override data cursor or (None)
        Returns:
            tuple of 1. (str) data cursor and 2.(list of dict) new data records
        """
        if not cursor:
            time_range = self._get_time_range()
            if time_range:
                cursor = time_range["start"]
                time_range["end_timestamp"] = datetime.fromisoformat(time_range["end"])
            else:
                cursor = self._config.get_cursor()

        headers = self._get_headers()
        start = self._now()
        try:
            rest_api_response = requests.request(
                "GET", self._config.get_url(), headers=headers
            )
            if rest_api_response.status_code != 200:
                raise CjException(self.name, None, "REST API call was not successful")
            raw_data = rest_api_response.json()
        except Exception as e:
            logging.critical(f"Problem data from {self._config.get_url()} : {e}")
            raise CjException(
                self.name, f"Problem data from {self._config.get_url()}", e
            ) from None

        _process_respns_data = self._process_response_data(raw_data, time_range, cursor)

        if _process_respns_data:
            _process_data = _process_respns_data["processed_data"]
            new_cursor = _process_respns_data["new_cursor"]

        end = self._now()
        logging.info(
            f"{len(_process_data)} new events after {end - start}ms of filtering"
        )

        cursor_str = str(new_cursor) if new_cursor else None

        if time_range:
            yield _process_data, None
        else:
            yield _process_data, cursor_str
