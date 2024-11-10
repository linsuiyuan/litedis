import gzip
import pickle
import shutil
import threading
import time

from litedis import BaseLitedis


class RDB:
    """RDB 持久化类"""

    def __init__(self,
                 db: BaseLitedis,
                 rdb_save_frequency: int = 600,
                 compression: bool = True):
        self.db = db
        self.rdb_save_frequency = rdb_save_frequency
        self.compression = compression

        # 文件路径
        self.rdb_path = self.db.data_dir / f"{self.db.db_name}.rdb"
        self.tmp_rdb_path = self.db.data_dir / f"{self.db.db_name}.rdb.tmp"

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

    def save_task_in_background(self, callback=None):
        rdb_thread = threading.Thread(target=self.save_task, args=[callback], daemon=True)
        rdb_thread.start()

    def save_task(self, callback=None):
        """RDB保存任务"""
        while True:
            time.sleep(self.rdb_save_frequency)
            self.save_rdb(callback)

    def save_rdb(self, callback=None) -> bool:
        """保存RDB文件"""

        with self.db.db_lock:
            try:
                # 先写入临时文件
                if self.compression:
                    with gzip.open(self.tmp_rdb_path, 'wb') as f:
                        pickle.dump(self.db.data, f)
                else:
                    with open(self.tmp_rdb_path, 'wb') as f:
                        pickle.dump(self.db.data, f)

                # 原子性地替换旧文件
                shutil.move(str(self.tmp_rdb_path), str(self.rdb_path))
            except (pickle.UnpicklingError, EOFError, AttributeError, TypeError, MemoryError) as e:
                if self.tmp_rdb_path.exists():
                    self.tmp_rdb_path.unlink()
                raise Exception("保存文件出错") from e
        if callback:
            callback()
        return True
