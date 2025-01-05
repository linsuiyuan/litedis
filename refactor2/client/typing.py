from typing import Protocol, Any


class CommandsProtocol(Protocol):

    def execute_command(self, *args) -> Any: ...
