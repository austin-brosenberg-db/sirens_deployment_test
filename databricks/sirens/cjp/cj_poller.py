#!/usr/bin/env python3
import datetime
import logging
import sys
import os
from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
)
import argparse
import textwrap

"""
This file implements a poller function intended to retrieve data, optionally
perform basic filtering and transforms and write it to S3.  Typically it will
be executed as an AWS Lambda function, but it can be run locally for testing.

The data source, transforms, and output are influenced by a configuration file
and a schema mapping file which together modify the execution.
"""

_current_poller = "undefined"
datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"


class CrownJewelFilter(logging.Filter):
    """
    This class implements a filter that avoids sensitive data in logs
    """

    def filter(self: object, record: object) -> bool:
        """
        Check if the sensitive info like SecretAccessKey, Secret String and Security Token is present in the record

        Args:
            self: CrownJewelFilter object
            record: which need to be filtered.
        Returns:
            True or False based up on conditions.
        """
        if record.name == "botocore.parsers":
            if "SecretAccessKey" in record.getMessage():
                return False
            elif "SecretString" in record.getMessage():
                return False
        elif (
            record.name == "botocore.auth"
            and "x-amz-security-token:" in record.getMessage()
        ):
            return False
        elif (
            record.name == "botocore.endpoint"
            and "'X-Amz-Security-Token':" in record.getMessage()
        ):
            return False
        elif (
            record.name == "boto3.resources.action"
            and "Calling s3:put_object" in record.getMessage()
        ):
            return False
        return True


def _setup_logging(
    log_level: str, poller_name: str = "unspecified", interactive: bool = False
) -> None:
    """
    Configure global logging parameters, including filters to exclude records of low severity or with sensitive data

    Args:
        log_level: severity level of logs to be kept
        poller_name: name of the poller to be executed
        interactive: true if running interactively (for dev/debugging)
    """
    cjp_filter = CrownJewelFilter()
    logger = logging.getLogger()
    logger.setLevel(level=log_level)
    handlers = logger.handlers
    # Skip aws_request_id in test mode as it is undefined when running outside of AWS
    if interactive:
        cjp_formatter = logging.Formatter(f"[{poller_name}][%(levelname)s] %(message)s")
    else:
        cjp_formatter = logging.Formatter(
            f"[{poller_name}][%(levelname)s] %(aws_request_id)s %(message)s"
        )
    # Lamdba installs a log handler, so we must modify it rather than adding one if running within AWS
    if handlers:
        for h in handlers:
            h.addFilter(cjp_filter)
            h.setFormatter(cjp_formatter)
    else:
        cjp_handler = logging.StreamHandler(stream=sys.stdout)
        cjp_handler.addFilter(cjp_filter)
        cjp_handler.setFormatter(cjp_formatter)
        logger.addHandler(cjp_handler)
    # If debugging print message with log level
    if log_level == "DEBUG" or (type(log_level) == int and log_level < logging.INFO):
        logging.info(f"Setting log level to ({log_level})")
        logging.getLogger("botocore").setLevel(log_level)
    # Otherwise tune the botocore logging back as it's noisy (w.r.t. finding credentials per session)
    else:
        logging.getLogger("botocore").setLevel(logging.WARN)


def _usage(msg: str = None) -> None:
    """
    Prints the supported command line options and exits with non-zero return code

    Args:
        msg : (optional) error message to be printed prior to usage
    """
    if msg:
        print(f"ERROR: {msg}\n")
        exit(1)
    print(
        f"USAGE: cj_poller.py [-h] --poller/-p POLLER [--start/-s START_TIMESTAMP] [--end/-e END_TIMESTAMP]"
    )
    print(f"WHERE: poller : is the name of a specific poller configuration\n")
    print(
        f"Supported poller configurations are:  {','.join(databricks.sirens.cjp.lib.cjpcommon.get_poller_names())}"
    )
    print(
        f"WHERE: START_TIMESTAMP : get data from source from specified START_TIMESTAMP, example:2022-02-05T11:00:00\n"
    )
    print(
        f"WHERE: END_TIMESTAMP : get data from source till specified END_TIMESTAMP, example:2022-02-05T13:00:00\n"
    )
    exit(1)


def validate_datetimestamp(datetimestamp: str) -> bool:
    """
    Validate whether input 'datetimestamp' param is in expected format.

    Args:
        datetimestamp: string representing UTC timestamp in format "%Y-%m-%dT%H:%M:%S.%fZ"
    Returns
        True if datetimestamp matched successfully, False otherwise
    """
    try:
        return bool(datetime.datetime.strptime(datetimestamp, datetime_format))
    except ValueError:
        return False


def cj_poller(params: dict) -> bool:
    """
    Execute the poller to retrieve, process and store data as specified in the profile and schema specified

    Args:
        params: dictionary of parameters for poller invocation
    Returns:
        True if poller completed successfully, false otherwise
    Raises:
        CjInvalidParameterException if params was missing or not a dictionary
        CjMissingParameterException if required parameter was missing from passed dict
    """
    global _current_poller
    if not params or not isinstance(params, dict):
        raise CjInvalidParameterException(parameter="params")
    if "cjp_poller" in params and params["cjp_poller"]:
        if params["cjp_poller"] not in databricks.sirens.cjp.lib.cjpcommon.get_poller_names():
            _usage(f"invalid poller specified: {params['cjp_poller']}")
        _current_poller = params["cjp_poller"]
    else:
        raise CjMissingParameterException(
            poller=_current_poller, parameter="cjp_poller"
        )

    debug = (
        True
        if "cjp_debug" in params and params["cjp_debug"].lower() == "true"
        else False
    )
    interactive = True if "interactive" in params and params["interactive"] else False
    if debug:
        _setup_logging("DEBUG", _current_poller, interactive)
    else:
        _setup_logging("INFO", _current_poller, interactive)
    logging.debug(f"received event was {params}")

    poller_params = {}
    start = "start_timestamp" in params and params["start_timestamp"]
    end = "end_timestamp" in params and params["end_timestamp"]
    # start_timestamp and end_timestamp are dependency args
    # (start and not end) or (not start and end):
    if bool(start) ^ bool(end):
        _usage(
            f"start_timestamp and end_timestamp are dependency args, one cannot exist without other"
        )
    if start and end:
        if not validate_datetimestamp(params["start_timestamp"]):
            _usage(f"Invalid start_timestamp: {params['start_timestamp']}")
        if not validate_datetimestamp(params["end_timestamp"]):
            _usage(f"Invalid end_timestamp: {params['end_timestamp']}")
        poller_params["start_timestamp"] = datetime.datetime.strptime(
            params["start_timestamp"], datetime_format
        )
        poller_params["end_timestamp"] = datetime.datetime.strptime(
            params["end_timestamp"], datetime_format
        )
        current_time = datetime.datetime.utcnow()
        # start_timestamp, end_timestamp should be <= current_time
        if not (poller_params["start_timestamp"] <= current_time):
            _usage(
                f"start_timestamp: {poller_params['start_timestamp']} should be <= to current time: {current_time}"
            )
        if not (poller_params["end_timestamp"] <= current_time):
            _usage(
                f"end_timestamp: {poller_params['end_timestamp']} should be <= to current time: {current_time}"
            )
        # start_timestamp < end_timestamp
        if not (poller_params["start_timestamp"] < poller_params["end_timestamp"]):
            _usage(
                f"start_timestamp:{poller_params['start_timestamp']} should be less than end_timestamp: {poller_params['end_timestamp']}"
            )
        if _current_poller != "":
            logging.info(
                f"Limiting data retrieval from {_current_poller} within specified time range: [{poller_params['start_timestamp']} - {poller_params['end_timestamp']}]"
            )
    if "bucket" in params and params["bucket"]:
        poller_params["bucket"] = params["bucket"]
    elif "CJP_BUCKET" in os.environ and os.environ["CJP_BUCKET"]:
        poller_params["bucket"] = os.environ["CJP_BUCKET"]
    else:
        raise CjMissingParameterException(poller=_current_poller, parameter="bucket")

    logging.debug(
        f"selected '{_current_poller}' configuration storing to {poller_params['bucket']} bucket"
    )
    poller = databricks.sirens.cjp.lib.cjpcommon.create_poller_from_files(
        poller_name=_current_poller, **poller_params
    )
    poller.process_data()
    logging.info("completed successfully")
    return True


def get_args() -> object:
    """
    Define an argparse object to parse command line parameter

    Returns:
        argparse object to parse command line parameter
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """\
    Description:
    The Crown Jewel Poller, *cj_poller.py*
    --------------------------------------
    \t1.Retrieves data from a log source
    \t2.Optionally performs minor transformations
    \t3.Stores the data in a unique file (with timestamp in name).
    """
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--poller",
        "-p",
        type=str,
        dest="poller",
        help=f"Supported poller configurations are:  {', '.join(databricks.sirens.cjp.lib.cjpcommon.get_poller_names())}",
    )
    group.add_argument(
        "--list-pollers",
        "-l",
        action="store_true",
        default=False,
        help="List supported pollers and exit",
    )
    parser.add_argument(
        "--start",
        "-s",
        type=str,
        dest="start_timestamp",
        help="Example:2022-02-05T11:00:00.000000Z",
    )
    parser.add_argument(
        "--end",
        "-e",
        type=str,
        dest="end_timestamp",
        help="Example:2022-02-05T13:00:00.000000Z",
    )
    args = parser.parse_args()
    return args


def _run_interactive() -> None:
    """
    Function to create & execute a poller based on command line parameters
    """
    args = get_args()
    if args.list_pollers:
        print(" ".join(databricks.sirens.cjp.lib.cjpcommon.get_poller_names()))
        return
    # CJP_BUCKET is the name of the S3 bucket to which output data should be written
    # CJP_PROFILE is the name of the AWS profile in ~/.aws/credentials for interactive testing
    os.environ["CJP_BUCKET"] = os.environ.get("CJP_BUCKET", None)
    os.environ["CJP_PROFILE"] = os.environ.get("CJP_PROFILE", "")
    debug = (
        True
        if "CJP_DEBUG" in os.environ and os.environ["CJP_DEBUG"].lower() == "true"
        else False
    )

    interactive_event = {
        "cjp_poller": args.poller,
        "start_timestamp": args.start_timestamp,
        "end_timestamp": args.end_timestamp,
        "interactive": True,
        "cjp_debug": str(debug),
    }
    cj_poller(params=interactive_event)


# Allow for testing of a specified poller outside of Lambda
if __name__ == "__main__":
    _run_interactive()
