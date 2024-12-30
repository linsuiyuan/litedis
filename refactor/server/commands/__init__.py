import inspect
import sys

from .base import Command

from .basiccmds import (
    AppendCommand,
    CopyCommand,
    DecrbyCommand,
    DeleteCommand,
    ExistsCommand,
    ExpireCommand,
    ExpireTimeCommand,
    ExpireatCommand,
    GetCommand,
    IncrbyCommand,
    IncrbyfloatCommand,
    KeysCommand,
    MgetCommand,
    MsetCommand,
    MsetnxCommand,
    PersistCommand,
    RandomKeyCommand,
    RenameCommand,
    RenamenxCommand,
    SetCommand,
    StrlenCommand,
    SubstrCommand,
    TTLCommand,
    TypeCommand,
)

from ...utils import parse_string_command

COMMAND_CLASSES = {cls.__dict__['name']: cls
                   for name, cls in inspect.getmembers(sys.modules[__name__], inspect.isclass)}


def create_command_from_strcmd(db, strcmd) -> Command:
    name, args = parse_string_command(strcmd)
    cmd_class = COMMAND_CLASSES.get(name)
    if cmd_class is None:
        raise ValueError(f'Unknown command "{name}"')
    command = cmd_class(db, name, args, strcmd)
    return command
