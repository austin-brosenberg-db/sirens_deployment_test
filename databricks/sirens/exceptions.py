"""Implements Sirens Exception Classes.

Authors:
    Derek King July 2022.

Classes:
    SirensException()
    SirensActionException()
    SirensAggregationException()
    SirensAlertManagerException()
    SirensConnectorException()
    SirensConfigException()
    SirensEnrichmentException()
    SirensDetectionException()
    SirensGlobalConfigException()
    SirensExpectationsException()
    SirensNormalizeException()
    SirensParsingException()
    SirensSQLException()
    SirensUserException()
"""


class SirensException(Exception):
    """Base Application Class
    """


class SirensUserException(SirensException):
    """user related error messages
    """


class SirensAggregationException(SirensException):
    """table aggregation related error messages
    """


class SirensConnectorException(SirensException):
    """connector related error messages
    """


class SirensEnrichmentException(SirensException):
    """errors related to dataframe enrichments
    """


class SirensEnvironmentException(SirensException):
    """errors related to the environment
    """


class SirensExpectationsException(SirensException):
    """dlt expectations related errors
    """


class SirensParsingError(SirensException):
    """parser related errors
    """


class SirensParsingException(SirensException):
    """parser related errors
    """


class SirensSQLException(SirensException):
    """SQL related errors
    """


class SirensNormalizeException(SirensException):
    """normalize related errors"""


class SirensConfigException(SirensException):
    """config file related errors
    """


class SirensGlobalConfigException(SirensException):
    """global sirens.config file related exceptions
    """
    # def __init__(self, *args):
    #    self.output = []
    #    self.args = args
    #    self.url = "https://databricks.com/config_file_help.html"
    #    self.output.append(f"{self.__class__.__name__}")
    #    self.output.extend(self.args)
    #    self.output.append(self.url)

    #    print(f"{line}")
    #    for line in self.output:


class SirensDetectionException(SirensException):
    pass


class SirensActionException(SirensException):
    """actions related error"""


class SirensAlertManagerException(SirensException):
    """alert manager related errors"""
