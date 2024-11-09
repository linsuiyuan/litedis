"""持久化模块"""
import gzip
import json
import pickle
import shutil
import threading
import time
from typing import (Dict, Union)
from pathlib import Path

from litedis.types import AOFFsyncStrategy


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


class RDB:
    """RDB 持久化类"""

    def __init__(self,
                 db_name: str,
                 data_dir: Union[str, Path],
                 db_data,
                 db_lock: threading.Lock,
                 rdb_save_frequency: int = 600,
                 compression: bool = True):
        self.db_name = db_name
        self.data_dir = data_dir if isinstance(data_dir, Path) else Path(data_dir)
        self.db_data = db_data
        self.db_lock = db_lock
        self.rdb_save_frequency = rdb_save_frequency
        self.compression = compression

        # 文件路径
        self.rdb_path = self.data_dir / f"{db_name}.rdb"
        self.tmp_rdb_path = self.data_dir / f"{db_name}.rdb.tmp"

    def read_rdb(self):
        if not self.rdb_path.exists():
            return

        try:
            if self.compression:
                with gzip.open(self.rdb_path, 'rb') as f:
                    data = pickle.load(f)
            else:
                with open(self.rdb_path, 'rb') as f:
                    data = pickle.load(f)

            return data
        except pickle.PicklingError as e:
            raise Exception("读取 RBD 文件出错") from e

    def save_task_in_background(self):
        rdb_thread = threading.Thread(target=self.save_task, daemon=True)
        rdb_thread.start()

    def save_task(self):
        """RDB保存任务"""
        while True:
            time.sleep(self.rdb_save_frequency)
            self.save_rdb()

    def save_rdb(self) -> bool:
        """保存RDB文件"""

        with self.db_lock:
            try:

                # 先写入临时文件
                if self.compression:
                    with gzip.open(self.tmp_rdb_path, 'wb') as f:
                        pickle.dump(self.db_data, f)
                else:
                    with open(self.tmp_rdb_path, 'wb') as f:
                        pickle.dump(self.db_data, f)

                # 原子性地替换旧文件
                shutil.move(str(self.tmp_rdb_path), str(self.rdb_path))
                return True
            except pickle.UnpicklingError as e:
                if self.tmp_rdb_path.exists():
                    self.tmp_rdb_path.unlink()
                raise Exception("保存文件出错") from e
