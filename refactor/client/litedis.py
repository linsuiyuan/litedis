from refactor.client.commands import BasicCmds
from refactor.server import LitedisServer


class Litedis(BasicCmds):

    def __init__(self, data_path: str = "ldbdata", db_name: str = "litedis"):
        self.data_path = data_path
        self.db_name = db_name

        self.server = LitedisServer(self.data_path)
        self.db = self.server.get_or_create_db(self.db_name)


