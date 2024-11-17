import gzip
import pickle
import shutil
import threading
import time
import weakref

from litedis import BaseLitedis, PersistenceType


class RDB:
    """RDB 持久化类"""

    def __init__(self,
                 db: weakref.ReferenceType[BaseLitedis],
                 rdb_save_frequency: int = 600,
                 compression: bool = True,
                 callback_after_save_rdb=None):
        self._db = db
        self.rdb_save_frequency = rdb_save_frequency
        self.compression = compression
        self.callback_after_save_rdb = callback_after_save_rdb

        # 文件路径
        self.rdb_path = self.db.data_dir / f"{self.db.db_name}.rdb"
        self.tmp_rdb_path = self.db.data_dir / f"{self.db.db_name}.rdb.tmp"

        # 后台持久化任务
        if self.db.persistence in (PersistenceType.RDB, PersistenceType.MIXED):
            self.save_task_in_background()

    @property
    def db(self) -> BaseLitedis:
        return self._db()

    @property
    def db_data(self):
        return {
            'data': self.db.data,
            'types': self.db.data_types,
            'expires': self.db.expires
        }

    @db_data.setter
    def db_data(self, value):
        self.db.data = value['data']
        self.db.data_types = value['types']
        self.db.expires = value['expires']

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

            self.db_data = data
            return True
        except (pickle.PicklingError, TypeError) as e:
            raise Exception("读取 RBD 文件出错") from e

    def save_task_in_background(self):
        rdb_thread = threading.Thread(target=self.save_task,
                                      daemon=True)
        rdb_thread.start()

    def save_task(self):
        """RDB保存任务"""
        while True:
            time.sleep(self.rdb_save_frequency)

            # 数据库关闭的话，退出任务
            if not self.db:
                break

            self.save_rdb()

    def save_rdb(self) -> bool:
        """保存RDB文件"""

        if not self.db:
            return False

        with self.db.db_lock:
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
            except (pickle.UnpicklingError, EOFError, AttributeError, TypeError, MemoryError) as e:
                if self.tmp_rdb_path.exists():
                    self.tmp_rdb_path.unlink()
                raise Exception("保存文件出错") from e
        if self.callback_after_save_rdb:
            self.callback_after_save_rdb()
        return True
