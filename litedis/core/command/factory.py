import inspect
import sys

from litedis.core.command import (
    basiccmds,
    hashcmds,
    listcmds,
    setcmds,
    zsetcmds,
)
from litedis.core.command.base import Command


def _import_class(module_name):
    return {cls.__dict__["name"]: cls
            for name, cls in inspect.getmembers(sys.modules[module_name], inspect.isclass)
            if issubclass(cls, Command) and cls.__dict__.get("name") is not None}


_parsers = {}
_parsers.update(_import_class(basiccmds.__name__))
_parsers.update(_import_class(hashcmds.__name__))
_parsers.update(_import_class(listcmds.__name__))
_parsers.update(_import_class(setcmds.__name__))
_parsers.update(_import_class(zsetcmds.__name__))


class CommandFactory:

    @staticmethod
    def create(command_name: str) -> Command:
        name = command_name
        if name not in _parsers:
            raise ValueError(f"unknown command name: {command_name}")

        return _parsers[name]()
