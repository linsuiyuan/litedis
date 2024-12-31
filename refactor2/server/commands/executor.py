from refactor2.server.commands import CommandExecutionContext
from refactor2.server.commands.parsers import parse_command_line_to_object


class CommandExecutor:

    def execute(self, cmdline: str, ctx: CommandExecutionContext):
        command = parse_command_line_to_object(cmdline)
        return command.execute(ctx)
