class Litedis:

    def __init__(self, db_dir: str = "lddata", db_name: str = "litedis"):
        self.db_dir = db_dir
        self.db_name = db_name
        self.db_id = f"{db_dir}/{db_name}"


