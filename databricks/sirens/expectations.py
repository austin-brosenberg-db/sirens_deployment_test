"""Implements checks for DLT expectations.

If using DLT, and the common information model. You can set which fields can be enforced as expectations.

Authors:
    Derek King (August 2022)

Classes:
    Expectations()

"""
import sys
import json
from typing import Dict, Tuple, Literal, Optional

from pyspark.sql import Column

from databricks.sirens.exceptions import SirensExpectationsException

from databricks.sirens.config_reader import CommonInformationModel

from databricks.sirens.logging import get_logger

logger = get_logger(__name__)


class Expectations:
    """Class for Delta Live Table expectations, using common information model fields.
    """

    def __init__(self, target_table, **kwargs: Dict) -> None:
        """create instance for target_table

        :param target_table: common information model table name
        :type target_table: str
        """
        self.target_table = target_table
        self.enforce_metafields = kwargs.get("enforce_metafields", True)
        self.enforce_mandatory = kwargs.get("enforce_mandatory", True)
        self.enforce_recommended = kwargs.get("enforce_recommended", False)
        self.enforce_optional = kwargs.get("enforce_optional", False)
        self.expectations = {}
        self.inverse_expectations = {}

    def _get_metafield_expectations(self) -> Tuple[Dict, Dict]:
        """get the mandated meta_data that must be present in all tables.

        :return: tuple of expectations, and inverse expectations
        :rtype: Tuple[dict, dict]
        """
        self.expectations = {
            "_event_date_is_valid": "_event_date IS NOT NULL and _event_date > '2013-01-01'",
            "_event_time_is_valid": "_event_time IS NOT NULL and _event_time > '2013-01-01 00:00:00.01'",
            "_source_is_valid": "_source IS NOT NULL",
            "_sourcetype_is_valid": "_sourcetype IS NOT NULL",
            "_dvc_hostname_is_valid": "dvc_hostname IS NOT NULL"
        }
        self.inverse_expectations = {
            "_event_date_is_valid": "NOT(_event_date IS NOT NULL and _event_date > '2013-01-01')",
            "_event_time_is_valid": "NOT(_event_time IS NOT NULL and _event_time > '2013-01-01 00:00:00.01')",
            "_source_is_valid": "NOT(_source IS NOT NULL)",
            "_sourcetype_is_valid": "NOT(_sourcetype IS NOT NULL)",
            "_dvc_hostname_is_valid": "NOT(dvc_hostname IS NOT NULL)"
        }
        return self.expectations, self.inverse_expectations

    def _get_expectations(self, target_table: str, type: Literal["recommended", "mandatory", "optional"]) -> Tuple[
        Dict, Dict]:
        """read the common information model file, and return the fields for the called type.

        :param target_table: target common information model table
        :type target_table: str
        :param type: which fields - mandatory, recommended or optional
        :type type: Literal[&quot;recommended&quot;, &quot;mandatory&quot;, &quot;optional&quot;]
        :return: expectations and inverse expectations for the requested type.
        :rtype: Tuple[Dict, Dict]
        """

        # get the content of the common information model file from cim_defs directory.
        f_content = self._get_file_content(target_table)

        expects, inverse_expects = {}, {}
        if f_content is None:
            return expects, inverse_expects
        else:
            # content as dict
            f_content = json.loads(f_content)
            # get the common information model fields.
            fields = f_content.get("fields")

        for field in fields:
            # if the field of of the called type, add to the return dictionaories.
            if field.get("type") == type:
                fname = field.get("name") + "_is_valid"
                constraint, inverse = self._make_constraint(field.get("name"), field.get("type"),
                                                                      field.get("datatype"))
                expects[fname] = constraint
                inverse_expects[fname] = inverse

        return expects, inverse_expects

    @staticmethod
    def _maybe_add_and_condition(prev_cond: str, new_part: str) -> str:
        """Create the sql expectation constraint string

        :param prev_cond: previous condition
        :type prev_cond: str
        :param new_part: new condition
        :type new_part: str
        :return: generated condition
        :rtype: str
        """
        if prev_cond:
            return f"{prev_cond} and {new_part}"
        return new_part

    @staticmethod
    def _make_constraint(name: str, type: Literal['mandatory', 'recommended', 'optional'],
                         datatype: str) -> Tuple[str, str]:
        """Create the sql expectation constraint string

        :param name: pyspark column name
        :type name: str
        :param type: field type
        :type type: Literal[&#39;mandatory&#39;, &#39;recommended&#39;, &#39;optional&#39;]
        :param datatype: column type
        :type datatype: Literal[&#39;timestamp&#39;, &#39;date&#39;]
        :return: _description_
        :rtype: Tuple[str, str]
        """
        constraint = ""
        if type == "mandatory":
            constraint = f"{name} IS NOT NULL"
        if datatype == "timestamp":
            constraint = Expectations._maybe_add_and_condition(constraint, f"{name} > '2013-01-01 00:00:00.01'")
        elif datatype == "date":
            constraint = Expectations._maybe_add_and_condition(constraint, f"{name} > '2013-01-01'")

        inverse_constraint = f"NOT({constraint})"

        return constraint, inverse_constraint

    @staticmethod
    def _get_file_content(target_table: str) -> Optional[str]:
        """Find and read the common information model file, definitions.

        :param target_table: target common information model table.
        :type target_table: str
        :return: file content object
        :rtype: object
        """
        dir_names = CommonInformationModel().get_file_paths("cim_defs", level='system')
        #file_names = [f"../log_sources/cim_defs/local/{target_table}.json",
        #                  f"../log_sources/cim_defs/default/{target_table}.json",
        #                  f"{sys.prefix}/log_sources/cim_defs/local/{target_table}.json",
        #                  f"{sys.prefix}/log_sources/cim_defs/default/{target_table}.json",
        #                  f"log_sources/cim_defs/local/{target_table}.json",
        #                  f"log_sources/cim_defs/default/{target_table}.json"]

        try:
            for dir in dir_names:
                try:
                    file = f"{dir}/{target_table}.json"
                    with open(file, 'r') as f:
                        logger.debug(f"getting expectations for {target_table}")
                        return f.read()
                except FileNotFoundError as e:
                    pass

            raise SirensExpectationsException("cim file not found")
        except SirensExpectationsException as exc:
            logger.warn(f"Unable to find cim file for: {target_table} - ignoring")

        return None

    def get(self) -> Tuple[Dict, Dict]:
        """Get the expectations for metadata fields, mandatory, recommended or optional fields.

        :return: expectations, and inverse expectations
        :rtype: Tuple[Dict, Dict]
        """
        # Default case - metafields should always be in expectations as a minimum, from bronze     tables
        if self.enforce_metafields is True and self.target_table == "metafields":
            return self._get_metafield_expectations()

        # Check for fields listed as mandatory in the common information model
        if self.enforce_mandatory is True:
            self.mandatory_expectations, self.mandatory_inverse = self._get_expectations(self.target_table, "mandatory")
            if self.mandatory_expectations is not None and self.mandatory_expectations:
                self.expectations.update(self.mandatory_expectations)
                self.inverse_expectations.update(self.mandatory_inverse)

        # Check for fields listed as recommended in the common information model. (probably creates lots of issues) -
        # but allowed if you want.
        if self.enforce_recommended is True:
            self.recommended_expectations, self.recommended_inverse = self._get_expectations(self.target_table,
                                                                                             "recommended")
            if self.recommended_expectations is not None:
                self.expectations.update(self.recommended_expectations)
                self.inverse_expectations.update(self.recommended_inverse)

        # Check for fields listed as optional - really bad idea IMHO.
        if self.enforce_optional is True:
            self.optional_expectations, self.optional_inverse = self._get_expectations(self.target_table, "optional")
            if self.optional_expectations is not None:
                self.expectations.update(self.optional_expectations)
                self.inverse_expectations.update(self.optional_inverse)

        if not self.expectations:
            self.expectations.update({"cim_file_error": "_event_time is not null"})
            self.inverse_expectations.update({"cim_file_error": "NOT(_event_time is not null)"})

        return self.expectations, self.inverse_expectations
