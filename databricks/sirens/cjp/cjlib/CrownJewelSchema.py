from collections import OrderedDict
from datetime import datetime
import logging
import pytz
from typing import Union

from databricks.sirens.cjp.cjlib.exceptions.CjExceptions import (
    CjMissingParameterException,
    CjInvalidParameterException,
    CjException,
)

"""
Loads and implements schema mapping (transformations)
"""


def str_to_bool(value_in: Union[str, bool]) -> bool:
    """
    Return a boolean interpretation of the passed value

    Args:
        value_in: (str or bool) value to be interpreted
    Returns:
        True if value implied a True value, false otherwise
    Raises:
        CjInvalidParameterException if value_in was not a supported type
    """
    if value_in is None:
        return False

    value_type = type(value_in)
    if value_type is bool:
        return value_in
    elif value_type is str:
        return not value_in.lower() in ["0", "false", "no", "null"]
    else:
        raise CjInvalidParameterException(
            parameter=value_in, msg="'value_type' is not supported"
        )


class CrownJewelSchema:
    """
    Class to represent desired transformations of input data
    """

    def __init__(
        self: object,
        schema: OrderedDict = None,
        transform: str = None,
        stateless: bool = True,
    ) -> object:
        """
        Initializes CrownJewelSchema object

        Args:
            self: CrownJewelSchema object
            schema: OrderedDict describing desired transformations
            transform: transformation mode for data not included in schema
            stateless: False if a cursor field must be defined in schema
        Returns:
            CrownJewelSchema object
        Raises:
            CjInvalidParameterException if required parameter was absent or had an unsupported value
        """
        self._schema = schema if schema else OrderedDict()
        self._transform_mode = transform

        valid_transforms = ["drop", "extra", "keep"]
        if not transform or transform not in valid_transforms:
            msg = f"must be one of {','.join(valid_transforms)}"
            raise CjInvalidParameterException(parameter="transform", msg=msg)

        if not isinstance(self._schema, OrderedDict):
            msg = f"must be OrderedDict, not {type(schema)}"
            raise CjInvalidParameterException(parameter="schema", msg=msg)

        cursor_field = self.get_cursor_field()[0]
        if not stateless and not cursor_field:
            msg = "poller is stateful, but schema lacks cursor specification"
            raise CjInvalidParameterException(parameter="cursor", msg=msg)

        if transform == "extra":
            logging.debug("Packing any unspecified input values into 'extra' column")
        elif transform == "drop":
            logging.debug("Dropping any unspecified input columns")
        elif transform == "keep":
            logging.debug("Keeping any unspecified input columns")

        # Print a warning message if the same input and output types are specified as it
        # may suggest that a conversion was desired (but not properly represented)
        for field_out in self._schema.keys():
            type_in = self._schema[field_out].get("type_in")
            type_out = self._schema[field_out].get("type_out")
            if type_in and type_in == type_out:
                logging.warning(
                    f"WARNING: no conversion for {field_out} as type_in == type_out ({type_in})"
                )

    def _transform(self: object, input_data: dict) -> dict:
        """
        Transform the field names and data format for a single data point

        Args:
            self: CrownJewelSchema object
            input_data: dictionary of input data for one data point
        Returns:
            dict of transformed data for one data point
        Raises:
            CjException if data cannot be transformed as requested
        """
        if type(input_data) is not dict:
            raise CjInvalidParameterException(
                parameter="input_data", msg="must be in a dictionary"
            )
        entry = {}
        schema_cols = self._schema.keys()
        cols = {k: None for k in input_data.keys()}

        for field_out in schema_cols:
            # Checks if input field is complex type.
            if (
                "field_in_complex" in self._schema[field_out] and self._schema[field_out]["field_in_complex"]
            ):
                if "field_in" in self._schema[field_out]:
                    field_in = self._schema[field_out]["field_in"]
                    complex_field = field_in.split(".")
                    temp_input = input_data
                    iterator = 0
                    # Iterates through the complex object and fetches child object that has the actual input field
                    while type(temp_input) is dict and iterator < (
                        len(complex_field) - 1
                    ):
                        if complex_field[iterator] in temp_input.keys():
                            temp_input = temp_input[complex_field[iterator]]
                        else:
                            temp_input = None
                        iterator += 1
                    if type(temp_input) is dict and iterator == (
                        len(complex_field) - 1
                    ):
                        entry[field_out] = self._field_value(
                            complex_field[iterator], field_out, temp_input
                        )
                else:
                    raise CjException(
                        msg="'field_in' field is required when 'field_in_complex' is set true"
                    )
            else:
                if "field_in" in self._schema[field_out]:
                    field_in = self._schema[field_out]["field_in"]
                else:
                    # assume the input field name is the same as the output if unspecified
                    field_in = field_out
                entry[field_out] = self._field_value(field_in, field_out, input_data)
            cols.pop(field_in, None)

        if self._transform_mode == "keep":
            for field_in in cols:
                entry[field_in] = input_data[field_in]
        elif self._transform_mode == "extra":
            entry["extra"] = {}
            for field_in in cols:
                field_out = field_in
                entry["extra"][field_out] = input_data[field_in]
        elif cols:
            logging.debug(f"dropping {','.join(cols)}")
        return entry

    def _field_value(
        self: object, field_in: str, field_out: str, input_data: dict
    ) -> Union[bool, int, float, str, datetime]:
        """
        Retrieves and transforms the field value for a single data point

        Args:
            self: CrownJewelSchema object
            field_in: Input field name in the data point
            field_out: key into schema for details on conversion
            input_data: dictionary for one data point
        Returns:
            transformed field value for one data point
        Raises:
            CjException if invalid transformation was requested
        """

        # default to no transformation if 'type_out' is not specified for a column
        if "type_out" in self._schema[field_out]:
            if "type_in" not in self._schema[field_out]:
                raise CjException(
                    msg="'type_in' field is required when 'type_out' is specified"
                    f" {field_in} -> {field_out} \n  {self._schema} "
                )
            type_in = self._schema[field_out]["type_in"]
            type_out = self._schema[field_out]["type_out"]
        else:
            type_in = None
            type_out = None

        if field_in not in input_data:
            if (
                "required" in self._schema[field_out] and self._schema[field_out]["required"]
            ):
                raise CjException(
                    msg=f"a value for '{field_in}' was required from input data, but not present"
                )
            elif type_out == "bool":
                return False
            else:
                return None
        if type_in == type_out:
            response_value = input_data[field_in]
        elif type_out:
            if type_out == "int":
                response_value = int(input_data[field_in])
            elif type_out == "float":
                response_value = float(input_data[field_in] * 1.0)
            elif type_out == "str":
                response_value = str(input_data[field_in])
            elif type_out == "date":
                if type_in == "datetime" or type_in == "str":
                    dt = self.get_datetime(input_data[field_in], field_out)
                    response_value = str(dt.date())
                else:
                    raise CjException(
                        msg=f"converting from {type_in} to {type_out} is not supported"
                    )
            elif type_out == "datetime":
                if type_in == "str":
                    dt = self.get_datetime(input_data[field_in], field_out)
                    response_value = str(dt)
                else:
                    raise CjException(
                        msg=f"converting from {type_in} to {type_out} is not supported"
                    )
            elif type_out == "timestamp":
                if type_in == "datetime" or type_in == "str":
                    dt = self.get_datetime(input_data[field_in], field_out)
                    response_value = str(dt.timestamp())
                else:
                    raise CjException(
                        msg=f"converting from {type_in} to {type_out} is not supported"
                    )
            elif type_out == "bool":
                if type_in == "str" or type_in == "bool":
                    response_value = str_to_bool(input_data[field_in])
                else:
                    raise CjException(
                        msg=f"converting from {type_in} to {type_out} is not supported"
                    )
            else:
                raise CjException(
                    msg=f"Data type_out '{type_out}' specified in schema is not supported"
                )
        return response_value

    def get_cursor_field(self: object) -> (str, str):
        """
        Return the name of the first column, if any, designated for use as a cursor

        Args:
            self: CrownJewelSchema object
        Returns:
            input and output field names for cursor field, None for each if not designated
        Raises:
            CjException if multiple fields are flagged as cursor
        """
        field_in = None
        field_out = None
        for k in self._schema:
            if "cursor" in self._schema[k] and self._schema[k]["cursor"]:
                if field_out:
                    raise CjException(
                        msg=f"Only 1 'cursor' field supported; both {k} and {field_in} are set"
                    )
                field_out = k
                if "field_in" in self._schema[k] and self._schema[k]["field_in"]:
                    field_in = self._schema[k]["field_in"]
                else:
                    field_in = field_out
        return field_in, field_out

    def get_datetime(
        self: object, value_in: str, field_out: str, tz: pytz = None
    ) -> datetime:
        """
        Return a Python datetime object representing the passed value

        Args:
            self: CrownJewelSchema object
            value_in: text value to be converted
            field_out: key into schema for details on conversion
            tz: (Optional) timezone information to which datetime should be normalized
        Returns:
            datetime object based on input value
        """
        field_info = self._schema[field_out]
        # if a 'format_in' key is found, use it as a datetime format string
        if "format_in" in field_info:
            if "%z" in field_info["format_in"]:
                value_in = value_in.replace("PDT", "-0700").replace("PST", "-0800")
            try:
                dt = datetime.strptime(value_in, field_info["format_in"])
            except ValueError as e:
                logging.error(
                    f"unable to convert '{value_in}' using '{field_info['format_in']}"
                )
                raise e
        # if not format string was specified, assume input was in ISO format
        else:
            dt = datetime.fromisoformat(value_in)
        if tz:
            dt = dt.replace(tzinfo=tz)
        return dt

    def get_str_from_datetime(self: object, value_in: datetime, field_out: str) -> str:
        """
        Return a string representation of a datetime object to be reversed by get_datetime

        Args:
            self: CrownJewelSchema object
            value_in: object representing a point in time
            field_out: key into schema for details on conversion
        Returns:
            string representation of the datetime object
        """
        field_info = self._schema[field_out]
        # if a 'format_in' key is found, use it as a datetime format string
        if "format_in" in field_info:
            stamp = value_in.strftime(field_info["format_in"])
        # if not format string was specified, default to ISO format
        else:
            stamp = value_in.isoformat()
        return stamp

    def transform_list(self: object, entries: list) -> list:
        """transforms list of records point according to schema mapping

        :param self: CrownJewelSchema object
        :type self: object
        :param entries: list of dictionaries containing log records
        :type entries: list
        :raises CjInvalidParameterException: _description_
        :return: list of dictionaries of transformed records
        :rtype: list
        """

        if not type(entries) is list:
            raise CjInvalidParameterException(
                parameter="entries", msg="was not a 'list'"
            )
        return list(map(self._transform, entries))

    def transform_one(self: object, entry: dict) -> dict:
        """
        transforms single record point according to schema mapping

        Args:
            self: CrownJewelSchema object
            entry: record data
        Returns:
            transformed record
        """
        if not type(entry) is dict:
            raise CjInvalidParameterException(parameter="entry", msg="was not a 'dict'")
        return self._transform(entry)
