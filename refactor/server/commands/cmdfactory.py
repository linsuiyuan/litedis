from abc import ABC

from refactor.server import LitedisDb
from refactor.server.commands import Command, COMMAND_CLASSES
from refactor.utils import parse_string_command


class CommandFactory(ABC):
    @staticmethod
    def create_cmd_from_str(db: LitedisDb, strcmd: str) -> Command:

        cmd_name, args = parse_string_command(strcmd)
        command = COMMAND_CLASSES.get(cmd_name)
        if command is None:
            raise ValueError(f'Unknown command "{cmd_name}"')

        return command(db, cmd_name, args)
