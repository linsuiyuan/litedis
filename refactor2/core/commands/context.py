from refactor2.core.persistence import LitedisDB


class CommandContext:

    def __init__(self, db: LitedisDB, attrs: dict = None):
        self.db = db
        self.attrs = {} if attrs is None else attrs
