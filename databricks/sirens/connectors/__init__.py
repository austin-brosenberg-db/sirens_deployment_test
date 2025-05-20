from . import plugins

__version__ = "0.0.1"


def get(connector_string: str):
    # this connector map holds the parser name (how you refer to it in inputs.yaml) to the python module name
    # without the .py extension
    # Need to add to this dict everytime you want to add a new connector to sirens - not optimal.
    # TODO Find a way to automate this manual edit.
    connector_map = {"txt": "readTxt", "csv": "readCsv", "parquet": "readParquet",
                     "autoloader": "readAutoloader", "json": "readJson", "kafka": "readKafka",
                     "okta_api": "okta_api", "delta": "readDelta"}

    return connector_map.get(connector_string)


available_connectors = plugins.names_factory(__package__)

Reader = plugins.call_factory(__package__)
read = plugins.call_factory(__package__)
Writer = plugins.call_factory(__package__)
write = plugins.call_factory(__package__)
