from enum import Enum

from refactor2.server.persistence import LitedisDB


class CommandExecutionMode(Enum):
    NORMAL = "normal"
    REPLAY = "replay"


class CommandExecutionContext:

    def __init__(self, mode: CommandExecutionMode, db: LitedisDB, attrs: dict = None):
        self.db = db
        self.mode = mode
        self.attrs = {} if attrs is None else attrs

    def is_replay_mode(self):
        return self.mode == CommandExecutionMode.REPLAY
