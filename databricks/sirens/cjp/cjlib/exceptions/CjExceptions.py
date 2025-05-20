"""
Custom parent exception for Crown Jewel pollers
"""


class CjException(Exception):
    def __init__(
        self, poller: str = None, msg: str = None, e: Exception = None
    ) -> None:
        """
        Initialize the parent class of this object, then add items specific to this subclass

        Args:
            self: CjException object being initialized
            poller: name of the poller where the exception was raised
            msg: message containing the information of the cause of the exception
            e: Original exception if there is one
        """
        message = " ".join(
            [
                poller if poller else "Poller:<empty>",
                msg if msg else "Message:<empty>",
                str(e) if e else "Exception:<empty>",
            ]
        )
        super().__init__(message)


"""
Raise this exception when a parameter does not match the required format/value/type
"""


class CjInvalidParameterException(CjException):
    def __init__(
        self,
        poller: str = None,
        msg: str = None,
        e: Exception = None,
        parameter: str = None,
    ) -> None:
        """
        Initialize the parent class of this object, then add items specific to this subclass

        Args:
            self: CjInvalidParameterException object being initialized
            poller: name of the poller where the exception was raised
            msg: message containing the information of the cause of the exception
            e: Original exception if there is one
            parameter: Name of the parameter
        """
        message = " ".join(
            [
                "Invalid value for",
                parameter if parameter else "Parameter:<empty>",
                msg if msg else "Message:<empty>",
            ]
        )
        super().__init__(poller, message, e)


"""
Raise this exception when a mandatory parameter is missing
"""


class CjMissingParameterException(CjException):
    def __init__(
        self,
        poller: str = None,
        msg: str = None,
        e: Exception = None,
        parameter: str = None,
    ) -> None:
        """
        Initialize the parent class of this object, then add items specific to this subclass

        Args:
            self: CjMissingParameterException object being initialized
            poller: name of the poller where the exception was raised
            msg: message containing the information of the cause of the exception
            e: Original exception if there is one
            parameter:Name of the parameter
        """
        message = " ".join(
            [
                parameter if parameter else "Parameter:<empty>",
                "parameter missing",
                msg if msg else "Message:<empty>",
            ]
        )
        super().__init__(poller, message, e)


"""
This exception is used when something external happened, like out of memory or timeout exceptions
and was caught by our internal handlers
"""


class CjInfrastructureException(CjException):
    def __init__(
        self,
        poller: str = None,
        msg: str = None,
        e: Exception = None,
        signal: str = None,
    ) -> None:
        """
        Initialize the parent class of this object, then add items specific to this subclass

        Args:
            self: CjInfrastructureException object being initialized
            poller: name of the poller where the exception was raised
            msg: message containing the information of the cause of the exception
            e: Original exception if there is one
            signal: Signal type
        """
        message = " ".join(
            [
                signal if signal else "Signal:<empty>",
                "external signal handled.",
                msg if msg else "Message:<empty>",
            ]
        )
        super().__init__(poller, message, e)
