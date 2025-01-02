from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.interfaces import CommandLogger, CommandProcessor
from refactor2.server.persistence import DBManager
from refactor2.typing import PersistenceType
from refactor2.utils import SingletonMeta


class LitedisServer(metaclass=SingletonMeta, CommandProcessor):

    def __init__(self,
                 dbmanager: DBManager,
                 persistence_type=PersistenceType.MIXED):

        self.persistence_type = persistence_type

        self.dbmanager = dbmanager
        self.command_logger: CommandLogger = self.dbmanager

    def process_command(self, dbname: str, cmdline: str):
        result = self._execute_command_line(dbname, cmdline)

        self.command_logger.log_command(dbname, cmdline)

        return result

    def replay_command(self, dbname: str, cmdline: str):
        self._execute_command_line(dbname, cmdline)

    def _execute_command_line(self, dbname: str, cmdline: str):
        db = self.dbmanager.get_or_create_db(dbname)
        ctx = CommandContext(db)
        command = parse_command_line_to_object(cmdline)
        return command.execute(ctx)
