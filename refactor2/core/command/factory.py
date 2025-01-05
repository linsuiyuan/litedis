import inspect
import sys

from refactor2.core.command import basiccmds
from refactor2.core.command.base import Command


def _import_class(module_name):
    return {cls.__dict__["name"]: cls
            for name, cls in inspect.getmembers(sys.modules[module_name], inspect.isclass)
            if issubclass(cls, Command) and cls.__dict__.get("name") is not None}


_parsers = {}
_parsers.update(_import_class(basiccmds.__name__))


class CommandFactory:

    @staticmethod
    def create(command_line: str) -> Command:
        name, _ = command_line.split(maxsplit=1)
        name = name.lower()
        if name not in _parsers:
            raise ValueError(f"unknown command line: {command_line}")

        return _parsers[name](command_line)
