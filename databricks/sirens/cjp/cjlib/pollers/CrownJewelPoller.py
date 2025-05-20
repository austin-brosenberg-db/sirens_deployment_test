from databricks.sirens.cjp.cjlib.botohelper import boto_config, boto_endpoints
import databricks.sirens.cjp.cjlib.CrownJewelPollerConfig
import databricks.sirens.cjp.cjlib.CrownJewelSchema
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjException,
)
import boto3
from datetime import datetime
import json
import logging

try:
    from collections.abc import Generator
except ModuleNotFoundError as e:
    from typing import Generator

"""
Implements parent class to retrieve data from designated source, filter+rename columns, and write to unique file per 
invocation in S3.

NOTE: this class should NOT be itself instantiated, but rather a child class (e.g. RestApiPoller) should be used.
"""


class CrownJewelPoller:
    def __init__(
        self: object,
        name: str = None,
        config: dict = None,
        schema: dict = None,
        bucket: str = None,
        profile: str = None,
        start_timestamp: datetime = None,
        end_timestamp: datetime = None,
    ) -> None:
        """
        Initialize this object

        Args:
            self: CrownJewelPoller object
            name: name of the poller -- typically alluding to data source
            config: poller configuration parameters
            schema:  desired data transformations, if any
            bucket: name of the bucket to which data should br written
            profile: name of the AWS profile to be used
            start_timestamp: (optional) timestamp from which data retrieval should start
            end_timestamp: (optional) timestamp at which data retrieval should cease
        Raises:
            CjMissingParameterException if transform is set with schema specification
        """
        if type(self).__name__ == "CrownJewelPoller":
            raise CjException(
                poller=type(self).__name__,
                msg="Do not use this class directly, instead use derived class",
            )

        if name:
            self.name = name
        else:
            raise CjMissingParameterException(
                poller=type(self).__name__, parameter="name"
            )
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.credentials = None
        self._profile = profile
        self._credential_schema = None

        if config is not None:
            self._config = databricks.sirens.cjp.cjlib.CrownJewelPollerConfig.CrownJewelPollerConfig(
                config=config, storage_location=bucket, profile=profile
            )
        else:
            raise CjMissingParameterException(
                poller=type(self).__name__, parameter="config_file"
            )

        transform_mode = self._config.get_transform_mode()
        if transform_mode != "keep" and not schema:
            raise CjMissingParameterException(
                poller=name, parameter="schema", msg="required when transform is set"
            )
        self._schema = databricks.sirens.cjp.cjlib.CrownJewelSchema.CrownJewelSchema(
            schema=schema, transform=transform_mode
        )

    def _get_time_range(self: object) -> dict:
        """
        Returns formatted time range (start and end times)

        Args:
            self: CrownJewelPoller object
        Returns:
            start and end time stamps
        """
        if self.start_timestamp and self.end_timestamp:
            datetime_string_format = self._config.get_timestamp_format()
            if datetime_string_format and (datetime_string_format != "N/A"):
                return {
                    "start": self.start_timestamp.strftime(datetime_string_format),
                    "end": self.end_timestamp.strftime(datetime_string_format),
                }
        return {}

    def _get_data(self, **kwargs) -> tuple:
        """
        This function MUST be overridden by a derived class; subsclasses may redefine return format if also
        overriding process_data() method to accept alternate format.

        Args:
            (Varies per poller implementation, not specified for abstract method)
        Returns:
            tuple of data (array of dict) and cursor (str to be used as data cursor)
        Raises:
            CjException if parent/abstract method is run
        """
        raise CjException(
            poller=type(self).__name__,
            msg="Do not use this class directly, instead use derived class",
        )

    @staticmethod
    def _now() -> int:
        """
        This function returns the current ms since Unix Epoch

        Returns:
            number of ms since Unix Epoch
        """
        now = datetime.utcnow()
        return round(now.timestamp() * 1000)

    def _write_data(
        self: object, data: list = None, cursor: str = None, prefix: str = None
    ) -> None:
        """
        Write transformed data to a unique file in the bucket

        Args:
            data: dict for each input row of data
            cursor: data cursor to be stored for reference in future invocations
            prefix: bucket prefix to override default prefix calculation
        """
        num_events = len(data)
        if num_events:
            bucket_name = self._config.get_storage_location()
            bucket_prefix = prefix if (prefix) else self._config.get_bucket_prefix()
            now = datetime.utcnow()
            timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            date = now.date()
            object_name = f"{bucket_prefix}/{date}/{timestamp}.json"

            if self._profile:
                session = boto3.session.Session(profile_name=self._profile)
                s3 = session.resource(
                    service_name="s3",
                    endpoint_url=boto_endpoints["s3"][self._config.get_region()],
                    config=boto_config,
                )
            else:
                s3 = boto3.resource(
                    service_name="s3",
                    endpoint_url=boto_endpoints["s3"][self._config.get_region()],
                    config=boto_config,
                )
            object = s3.Object(
                bucket_name,
                object_name,
            )
            acl = "bucket-owner-full-control"
            object.put(Body=json.dumps(data), ACL=acl)
            logging.info(
                f"wrote {num_events} events to s3://{bucket_name}/{object_name}"
            )
        else:
            logging.info(f"no events to be written")
        if cursor:
            self._config.set_cursor(cursor)
        elif not self._config.is_stateless():
            logging.warning(f"poller is stateful but no cursor was stored")

    def get_data(self: object) -> Generator[dict, float, None]:
        """
        Generic method to read all input data and write transformed version to S3

        Args:
            self: CrownJewelPoller object
        Returns:
            Generator returning tuples of data(list) and cursor(string)
        """
        env = self._config.get_environment()
        if env != "library":
            raise CjException(
                msg=f"process_data() should be used in a/an {env} environment"
            )
        data_gen = self._get_data()
        for data, cursor in data_gen:
            logging.info(
                f"received {len(data)} new events, yielding cursor of '{cursor}'"
            )
            yield data, cursor

    def process_data(self: object) -> None:
        """
        Generic method to read all input data and write transformed version to S3

        Args:
            self: CrownJewelPoller object
        """
        env = self._config.get_environment()
        if env == "library":
            raise CjException(msg="get_data() should be used in a library environment")
        data_gen = self._get_data()
        for data, cursor in data_gen:
            logging.info(
                f"received {len(data)} new events, yielding cursor of '{cursor}'"
            )
            self._write_data(data, cursor)
