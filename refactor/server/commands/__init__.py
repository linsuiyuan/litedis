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

COMMAND_CLASSES = {name.lower()[:-7]: cls
                   for name, cls in inspect.getmembers(sys.modules[__name__], inspect.isclass)}
