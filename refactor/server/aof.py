import queue
import threading


# todo 抽象出 PersistenceThread 类
class AOF:
    def __init__(self, server):
        self.server = server
        self.buffer = queue.Queue()
        self.aof_file_lock = threading.Lock()

        self.persistence_thread = None
        self.is_persistence_thread_running = False

    def append_command(self, dbname: str, cmd: str | None):
        self.buffer.put(f"{dbname}/{cmd}\n")

    def start_persistence_thread(self):
        if self.is_persistence_thread_running:
            return

        def consumer():
            self.is_persistence_thread_running = True
            while True:
                item = self.buffer.get()
                if item is None:
                    break
                self._write_line_to_aof_file(item)
            self.is_persistence_thread_running = False

        thread = threading.Thread(target=consumer, daemon=True)
        thread.start()
        self.persistence_thread = thread

    def stop_persistence_thread(self):
        self.buffer.put(None)

        if self.persistence_thread:
            self.persistence_thread.join()

    def _write_line_to_aof_file(self, line: str):
        with self.aof_file_lock:
            with open(self.server.data_path / "litedis.aof", "a") as f:
                f.write(line)
