"""
持久化模块，将数据库保存到磁盘上。
"""
import gzip
import pickle
import shutil
import threading
import time
import weakref

from litedis import BaseLitedis


class LDB:
    """LDB 持久化类"""

    def __init__(self,
                 db: weakref.ReferenceType,
                 ldb_save_frequency: int = 600,
                 compression: bool = True,
                 callback_after_save_ldb=None):
        self._db = db
        self.ldb_save_frequency = ldb_save_frequency
        self.compression = compression
        self.callback_after_save_ldb = callback_after_save_ldb

        # 文件路径
        self.ldb_path = self.db.data_dir / f"{self.db.db_name}.ldb"
        self.tmp_ldb_path = self.db.data_dir / f"{self.db.db_name}.ldb.tmp"

        # 后台持久化任务
        self.save_task_in_background()

    @property
    def db(self) -> BaseLitedis:
        """db属性，返回 self._db 的原引用"""
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

    def read_ldb(self):
        """从文件中读取存储的数据库"""
        if not self.ldb_path.exists():
            return

        try:
            if self.compression:
                with gzip.open(self.ldb_path, 'rb') as f:
                    data = pickle.load(f)
            else:
                with open(self.ldb_path, 'rb') as f:
                    data = pickle.load(f)

            self.db_data = data
            return True
        except (pickle.PicklingError, TypeError) as e:
            raise Exception("读取 LBD 文件出错") from e

    def save_task_in_background(self):
        """后台持久化"""
        ldb_thread = threading.Thread(target=self.save_task,
                                      daemon=True)
        ldb_thread.start()

    def save_task(self):
        """LDB保存任务"""
        while True:
            time.sleep(self.ldb_save_frequency)

            # 数据库关闭的话，退出任务
            if not self.db:
                break

            self.save_ldb()

    def save_ldb(self) -> bool:
        """保存LDB文件"""

        if not self.db:
            return False

        with self.db.db_lock:
            try:
                # 先写入临时文件
                if self.compression:
                    with gzip.open(self.tmp_ldb_path, 'wb') as f:
                        pickle.dump(self.db_data, f)
                else:
                    with open(self.tmp_ldb_path, 'wb') as f:
                        pickle.dump(self.db_data, f)

                # 原子性地替换旧文件
                shutil.move(str(self.tmp_ldb_path), str(self.ldb_path))
            except (pickle.UnpicklingError, EOFError, AttributeError, TypeError, MemoryError) as e:
                if self.tmp_ldb_path.exists():
                    self.tmp_ldb_path.unlink()
                raise Exception("保存文件出错") from e
        if self.callback_after_save_ldb:
            self.callback_after_save_ldb()
        return True
