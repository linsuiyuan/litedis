import queue
import threading
from pathlib import Path
from typing import Callable

from refactor.server.commands import Command


class AOFPersistenceThread:
    def __init__(self, item_queue: queue.Queue, item_callback: Callable):
        self.item_queue = item_queue
        self.item_callback = item_callback

        self._is_thread_running = False
        self.thread = None

    @property
    def is_thread_running(self):
        return self._is_thread_running

    def start(self):
        if self._is_thread_running:
            return
        self._is_thread_running = True

        def consumer():

            while True:
                item = self.item_queue.get()
                if item is None:
                    break
                self.item_callback(item)

        self.thread = threading.Thread(target=consumer, daemon=True)
        self.thread.start()

    def stop(self):

        self.item_queue.put(None)
        if self.thread is not None:
            self.thread.join()
        self._is_thread_running = False
        self.thread = None


class AOF:
    def __init__(self, data_path: str | Path):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        self.data_path.mkdir(parents=True, exist_ok=True)

        self.buffer = queue.Queue()
        self.aof_file_lock = threading.Lock()

        self.persistence_thread = AOFPersistenceThread(self.buffer, self._write_line_to_aof_file)

    def append_command(self, cmd: Command):
        # todo should check if it is synchronous or asynchronous
        self.buffer.put(f"{cmd.db.dbname}/{cmd.raw_cmd}")

    def start(self):
        self.persistence_thread.start()

    def stop(self):
        self.persistence_thread.stop()

    def _write_line_to_aof_file(self, line: str):
        with self.aof_file_lock:
            with open(self.data_path / "litedis.aof", "a") as f:
                f.write(line)
