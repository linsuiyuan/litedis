"""持久化模块"""
import json
import threading
import time
from typing import Dict
from pathlib import Path

from litedis.types import AOFFsyncStrategy


class AOF:
    """AOF 持久化类"""
    def __init__(self, db_name, data_dir, aof_fsync):
        self.aof_fsync = aof_fsync
        self.data_dir = data_dir if isinstance(data_dir, Path) else Path(data_dir)
        self.aof_path = self.data_dir / f"{db_name}.aof"

        self._aof_buffer = []
        self._aof_buffer_lock = threading.Lock()

    def fsync_task(self):
        """AOF同步任务"""
        while True:
            time.sleep(1)
            self.flush_buffer()

    def flush_buffer(self):
        """刷新AOF缓冲区到磁盘"""
        with self._aof_buffer_lock:
            if not self._aof_buffer:
                return

            try:
                with open(self.aof_path, 'a', encoding='utf-8') as f:
                    for command in self._aof_buffer:
                        f.write(json.dumps(command) + '\n')
                self._aof_buffer.clear()
            except IOError as e:
                print(f"刷新AOF缓冲区出现错误: {e}")

    def append(self, command: Dict):
        """追加命令到AOF"""

        with self._aof_buffer_lock:
            self._aof_buffer.append(command)

        if self.aof_fsync == "always":
            self.flush_buffer()

    def read_commands(self, clear_aof=False):
        """从文件中读取 commands"""
        if not self.aof_path.exists():
            return

        with self._aof_buffer_lock:
            try:
                with open(self.aof_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        command = json.loads(line.strip())
                        yield command
            except IOError as e:
                print(f"从AOF文件中读取 commands出现错误: {e}")

    def clear_aof(self):
        """清理 AOF 文件"""
        self.aof_path.unlink(missing_ok=True)

    def run_fsync_task_in_background(self):
        if self.aof_fsync == AOFFsyncStrategy.EVERYSEC:
            aof_thread = threading.Thread(target=self.fsync_task, daemon=True)
            aof_thread.start()


class RDB:
    """RDB 持久化类"""
