from abc import ABC

from refactor.server import LitedisDb
from refactor.server.commands import Command, AppendCommand, CopyCommand
from refactor.utils import parse_string_command


class CommandFactory(ABC):
    @staticmethod
    def create_cmd_from_str(db: LitedisDb, strcmd: str) -> Command:

        cmd_name, args = parse_string_command(strcmd)

        match cmd_name:
            case "append":
                return AppendCommand(db, cmd_name, args)

            case "copy":
                return CopyCommand(db, cmd_name, args)
