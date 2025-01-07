from typing import Protocol, Any


class ClientCommands(Protocol):

    def execute(self, *args) -> Any: ...
