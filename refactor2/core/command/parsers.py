import inspect
import sys
import time
from abc import ABC, abstractmethod

from refactor2.commandline import parse_command_line
from refactor2.core.command.commands import SetCommand, Command, GetCommand


class CommandParser(ABC):
    name = None

    @abstractmethod
    def parse(self, command_line: str) -> Command: ...


class SetCommandParser(CommandParser):
    name = 'set'

    def parse(self, command_line: str) -> SetCommand:
        tokens = parse_command_line(command_line)

        if len(tokens) < 3:
            raise ValueError('set command requires key and value')

        key = tokens[1]
        value = tokens[2]
        options = {}

        lower_tokens = [t.lower() for t in tokens[3:]]
        for i, token in enumerate(lower_tokens):
            match token:
                case "nx":
                    options["nx"] = True
                case "xx":
                    options["xx"] = True
                case "get":
                    options["get"] = True
                case "keepttl":
                    options["keepttl"] = True
                case "ex":
                    now = int(time.time() * 1000)
                    seconds = int(lower_tokens[i + 1])
                    options["expiration"] = now + seconds * 1000
                case "px":
                    now = int(time.time() * 1000)
                    milliseconds = int(lower_tokens[i + 1])
                    options["expiration"] = now + milliseconds
                case "exat":
                    options["expiration"] = int(lower_tokens[i + 1]) * 1000
                case "pxat":
                    options["expiration"] = int(lower_tokens[i + 1])

        return SetCommand(key, value, options)


class GetCommandParser(CommandParser):
    name = 'get'

    def parse(self, command_line: str) -> Command:
        tokens = parse_command_line(command_line)
        if len(tokens) < 2:
            raise ValueError('get command requires key')
        key = tokens[1]
        return GetCommand(key)


_parsers = {cls.__dict__["name"]: cls
            for name, cls in inspect.getmembers(sys.modules[__name__], inspect.isclass)
            if issubclass(cls, CommandParser)}


def parse_command_line_to_command(command_line: str) -> Command:
    name, _ = command_line.split(maxsplit=1)
    name = name.lower()
    if name not in _parsers:
        raise ValueError(f"unknown command line: {command_line}")
    return _parsers[name]().parse(command_line)
