from abc import ABC, abstractmethod

from refactor2.core.persistence import LitedisDB
from refactor2.typing import ReadWriteType


class CommandContext:

    def __init__(self, db: LitedisDB, attrs: dict = None):
        self.db = db
        self.attrs = {} if attrs is None else attrs


class Command(ABC):
    name = None

    @property
    @abstractmethod
    def rwtype(self) -> ReadWriteType: ...

    @abstractmethod
    def execute(self, ctx: CommandContext): ...


class ReadCommand(Command, ABC):
    @property
    def rwtype(self) -> ReadWriteType:
        return ReadWriteType.Read


class WriteCommand(Command, ABC):
    @property
    def rwtype(self) -> ReadWriteType:
        return ReadWriteType.Write
