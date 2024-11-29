"""
AOF（Append Only File）持久化模块。
用于记录所有对 Litedis 数据库的写操作。
AOF 通过将每个写命令追加到文件中来实现数据的持久化，确保在 Litedis 重启后能够恢复数据。
"""
import functools
import json
import threading
import time
import weakref
from contextlib import contextmanager
from typing import Dict

from litedis import BaseLitedis
from litedis.typing import AOFFsyncStrategy


def collect_command_to_aof(func):
    """记录 AOF 命令的装饰器"""

    @functools.wraps(func)
    def wrapper(db, *args, **kwargs):  # noqa

        # 先运行原函数，出现异常的话，该调用就不会被记录到 aof 文件里
        result = func(db, *args, **kwargs)

        # db.aof 属性存在则表示需要持久化，不存在则不需要 aof 持久化
        if db.aof:
            command = {
                "method": func.__name__,
                "args": args,
                "kwargs": kwargs,
                "exectime": time.time(),  # 运行命令时间
            }
            db.aof.append(command)

        return result

    return wrapper


@contextmanager
def execute_command_sanbox(exectime: float):
    """执行 aof 命令沙盒，以提供特定的执行环境"""

    # hook 原来命令的执行时间
    builtin_time = time.time
    time.time = lambda: exectime

    try:
        yield

    finally:
        # 恢复内置 time函数
        time.time = builtin_time


class AOF:
    """AOF 持久化类"""

    def __init__(self,
                 db: weakref.ReferenceType,
                 aof_fsync: AOFFsyncStrategy):
        self._db = db
        self.data_dir = self.db.data_dir
        self.aof_path = self.data_dir / f"{self.db.db_name}.aof"

        self.aof_fsync = aof_fsync

        self._buffer = []
        self._buffer_lock = threading.Lock()

        # 后台持久化任务
        if self.aof_fsync == "everysec":
            self.run_fsync_task_in_background()

    @property
    def db(self) -> BaseLitedis:
        """db属性，返回 self._db 的原引用"""
        return self._db()

    def _fsync_task(self):
        """AOF 同步任务"""
        while True:
            time.sleep(1)

            # 如果数据库关闭，退出任务
            if not self.db:
                break

            self.flush_buffer()

    def run_fsync_task_in_background(self):
        """后台运行持久化任务"""
        aof_thread = threading.Thread(target=self._fsync_task, daemon=True)
        aof_thread.start()

    def flush_buffer(self):
        """刷新 AOF 缓冲区到磁盘"""
        with self._buffer_lock:
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
        """追加命令到 AOF 缓冲区"""

        with self._buffer_lock:
            self._buffer.append(command)

        if self.aof_fsync == "always":
            self.flush_buffer()

    def read_aof_commands(self):
        """从 AOF 文件中读取记录"""
        if not self.aof_path.exists():
            return

        with self._buffer_lock:

            try:
                with open(self.aof_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        command = json.loads(line.strip())
                        yield command
            except (IOError, json.JSONDecodeError) as e:
                raise Exception("读取 AOF 文件 出现错误") from e

    def read_aof_to_db(self):
        """读取 AOF 文件，恢复到数据库"""

        for command in self.read_aof_commands():
            # 应用命令
            method, args, kwargs, exectime = command.values()
            with execute_command_sanbox(exectime):
                # 取原始函数进行调用
                getattr(self.db, method).__wrapped__(self.db, *args, **kwargs)

        return True

    def clear_aof(self):
        """清理 AOF 文件"""
        with self._buffer_lock:
            self.aof_path.unlink(missing_ok=True)
