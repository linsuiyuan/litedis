import os
import tempfile
from pathlib import Path
from typing import TextIO

from refactor2.typing import CommandLogger
from refactor2.typing import DBCommandLine


class AOF(CommandLogger):

    def __init__(self, data_path: str | Path, filename="litedis.aof"):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)

        self._filename = filename
        self._file_path = self.data_path / self._filename
        self._file: TextIO | None = None

    def __del__(self):
        self._close_file()

    def _close_file(self):
        if self._file is not None and not self._file.closed:
            self._file.close()

    def close(self):
        self._close_file()

    def get_or_create_file(self):
        if self._file is None:
            self._file = open(self._file_path, "a")
        return self._file

    def exists_file(self):
        return self._file_path.exists()

    def log_command(self, dbcmd: DBCommandLine):
        file = self.get_or_create_file()
        file.write(f"{dbcmd.dbname}/{dbcmd.cmdline}\n")
        file.flush()

    def load_commands(self):

        if not self._file_path.exists():
            return

        self._close_file()
        with open(self._file_path, "r") as f:
            for line in f:
                dbname, cmdline = line.strip().split(sep="/", maxsplit=1)
                yield DBCommandLine(dbname, cmdline)

    def rewrite_commands(self, commands: DBCommandLine):

        temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(self._file_path))

        try:
            with os.fdopen(temp_fd, 'w') as f:
                for dbname, cmdline in commands:
                    f.write(f"{dbname}/{cmdline}\n")

            os.replace(temp_path, self._file_path)
        except:
            os.unlink(temp_path)
            raise Exception(f"Failed to rewrite {self._file_path}")
