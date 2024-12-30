import queue
import threading
from pathlib import Path

from refactor.server.commands import Command


class AOF:
    def __init__(self, data_path: str | Path):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        self.data_path.mkdir(parents=True, exist_ok=True)

        self.buffer = queue.Queue()
        self.aof_file_lock = threading.Lock()

    def append_command(self, cmd: Command):
        self.buffer.put(f"{cmd.db.dbname}/{cmd.raw_cmd}")

    def _write_line_to_aof_file(self, line: str):
        with self.aof_file_lock:
            with open(self.data_path / "litedis.aof", "a") as f:
                f.write(line)
