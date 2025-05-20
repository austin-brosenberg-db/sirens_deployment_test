# plugins.py

import functools
import importlib
from importlib import resources

# Dictionary with information about all registered plugins
_PLUGINS = {}


def register(func):
    """Decorator for registering a new plugin"""
    _, _, plugin = func.__module__.rpartition(".")
    _PLUGINS[plugin] = func
    return func


def names(package):
    """List all plugins in one package"""
    _import_all(package)
    return sorted(_PLUGINS.keys())


def get(plugin):
    """Get a given plugin"""
    func = _PLUGINS.get(plugin)
    if func is None:
        raise ValueError(f"No plugin registered for '{plugin}'")
    package, _, _ = func.__module__.rpartition(".")
    _import(package, plugin)
    return func


def call(plugin, *args, **kwargs):
    """Call the given plugin"""
    plugin_func = get(plugin)
    return plugin_func(*args, **kwargs)


def _import(package, plugin):
    """Import the given plugin file from a package"""
    importlib.import_module(f"{package}.{plugin}")


def _import_all(package):
    """Import all plugins in a package"""
    files = resources.contents(package)
    plugins = [f[:-3] for f in files if f.endswith(".py") and f[0] != "_"]
    for plugin in plugins:
        _import(package, plugin)


def names_factory(package):
    """Create a names() function for one package"""
    return functools.partial(names, package)


def get_factory():
    """Create a get() function for one package"""
    return functools.partial(get)


def call_factory():
    """Create a call() function for one package"""
    return functools.partial(call)
