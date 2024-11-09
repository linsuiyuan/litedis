import json
import threading
import time
from pathlib import Path
from typing import Dict

from litedis.mytypes import AOFFsyncStrategy


class AOF:
    """AOF 持久化类"""

    def __init__(self, db_name, data_dir, aof_fsync):
        self.aof_fsync = aof_fsync
        self.data_dir = data_dir if isinstance(data_dir, Path) else Path(data_dir)
        self.aof_path = self.data_dir / f"{db_name}.aof"

        self._buffer = []
        self.buffer_lock = threading.Lock()

    def fsync_task(self):
        """AOF同步任务"""
        while True:
            time.sleep(1)
            self.flush_buffer()

    def flush_buffer(self):
        """刷新AOF缓冲区到磁盘"""
        with self.buffer_lock:
            if not self._buffer:
                return

            try:
                with open(self.aof_path, 'a', encoding='utf-8') as f:
                    for command in self._buffer:
                        f.write(json.dumps(command) + '\n')
                self._buffer.clear()
            except IOError as e:
                print(f"刷新AOF缓冲区出现错误: {e}")

    def append(self, command: Dict):
        """追加命令到AOF"""

        with self.buffer_lock:
            self._buffer.append(command)

        if self.aof_fsync == AOFFsyncStrategy.ALWAYS:
            self.flush_buffer()

    def read_aof(self):
        """读取 AOF 文件"""
        if not self.aof_path.exists():
            return

        with self.buffer_lock:
            try:
                with open(self.aof_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        command = json.loads(line.strip())
                        yield command
            except (IOError, json.JSONDecodeError) as e:
                raise Exception("读取 AOF 文件 出现错误") from e

    def clear_aof(self):
        """清理 AOF 文件"""
        self.aof_path.unlink(missing_ok=True)

    def run_fsync_task_in_background(self):
        if self.aof_fsync == AOFFsyncStrategy.EVERYSEC:
            aof_thread = threading.Thread(target=self.fsync_task, daemon=True)
            aof_thread.start()
