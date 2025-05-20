from . import plugins

__version__ = "0.0.1"

available_parsers = plugins.names_factory(__package__)()

Parse = plugins.call_factory()
toBronze = plugins.call_factory()
toSilver = plugins.call_factory()
