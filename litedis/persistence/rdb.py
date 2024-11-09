import gzip
import pickle
import shutil
import threading
import time
from pathlib import Path
from typing import Union


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
        except (pickle.PicklingError, TypeError) as e:
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
            except (pickle.UnpicklingError, EOFError, AttributeError, TypeError, MemoryError) as e:
                if self.tmp_rdb_path.exists():
                    self.tmp_rdb_path.unlink()
                raise Exception("保存文件出错") from e
