from pathlib import Path
from typing import TextIO


class AOF:

    def __init__(self, data_path: str | Path, filename="litedis.aof"):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self.filename = filename

        self._file: TextIO | None = None
        self._is_rewriting = False

    def __del__(self):
        self._close_file()

    def _close_file(self):
        if self._file is not None and not self._file.closed:
            self._file.close()

    def get_or_create_file(self):
        if self._file is None:
            self._file = open(self.data_path / self.filename, "a")
        return self._file

    def log_command(self, dbname: str, cmdline: str):
        file = self.get_or_create_file()
        file.write(f"{dbname}/{cmdline}\n")
        file.flush()

    def load_commands(self):
        self._close_file()
        with open(self.data_path / self.filename, "r") as f:
            for line in f:
                dbname, cmdline = line.strip().split(sep="/", maxsplit=1)
                yield dbname, cmdline
