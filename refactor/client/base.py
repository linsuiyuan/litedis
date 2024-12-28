from abc import ABC

from refactor.server import LitedisDb
from refactor.server import LitedisServer

class BaseClient(ABC):
    db: LitedisDb
    server: LitedisServer

    def process_command(self, strcommand):
        return self.server.process_command(self.db, strcommand)