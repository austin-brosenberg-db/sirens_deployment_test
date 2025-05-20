from . import plugins

__version__ = "0.0.1"

available_actions = plugins.names_factory(__package__)

Action = plugins.call_factory(__package__)
do_action = plugins.call_factory(__package__)
actions = plugins.call_factory(__package__)