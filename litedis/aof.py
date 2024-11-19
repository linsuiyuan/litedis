import functools
import json
import threading
import time
import weakref
from contextlib import contextmanager
from typing import Dict

from litedis import AOFFsyncStrategy, BaseLitedis


def collect_command_to_aof(func):
    """记录 aof 命令的装饰器"""

    @functools.wraps(func)
    def wrapper(db, *args, **kwargs):
        command_exectime = kwargs.pop("command_exectime", None)
        result = func(db, *args, **kwargs)

        # command_exectime 是重放 aof 命令时才有的参数，为 None 表示非重放
        if command_exectime is None:
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
                 db: weakref.ReferenceType[BaseLitedis],
                 aof_fsync: AOFFsyncStrategy):
        self._db = db
        self.data_dir = self.db.data_dir
        self.aof_path = self.data_dir / f"{self.db.db_name}.aof"

        self.aof_fsync = aof_fsync

        self._buffer = []
        self._buffer_lock = threading.Lock()

        # 后台持久化任务
        if self.aof_fsync == AOFFsyncStrategy.EVERYSEC:
            self.run_fsync_task_in_background()

    @property
    def db(self) -> BaseLitedis:
        return self._db()

    def fsync_task(self):
        """AOF同步任务"""
        while True:
            time.sleep(1)

            # 如果数据库关闭，退出任务
            if not self.db:
                break

            self.flush_buffer()

    def flush_buffer(self):
        """刷新AOF缓冲区到磁盘"""
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
        """追加命令到AOF缓冲区"""

        with self._buffer_lock:
            self._buffer.append(command)

        if self.aof_fsync == AOFFsyncStrategy.ALWAYS:
            self.flush_buffer()

    def read_aof_commands(self):
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

    def read_aof(self):
        """读取 AOF 文件"""

        for command in self.read_aof_commands():
            # 应用命令
            method, args, kwargs, exectime = command.values()
            with execute_command_sanbox(exectime):
                getattr(self.db, method)(*args, **kwargs, command_exectime=exectime)

        return True

    def clear_aof(self):
        """清理 AOF 文件"""
        with self._buffer_lock:
            self.aof_path.unlink(missing_ok=True)

    def run_fsync_task_in_background(self):
        aof_thread = threading.Thread(target=self.fsync_task, daemon=True)
        aof_thread.start()
