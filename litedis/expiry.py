import threading
import time
import weakref

from litedis import BaseLitedis


class Expiry:

    def __init__(self,
                 db: weakref.ReferenceType[BaseLitedis]):
        self._db = db

        # 过期检查任务
        self.run_handle_expired_keys_task()

    @property
    def db(self) -> BaseLitedis:
        return self._db()

    def is_expired(self, key: str) -> bool:
        """检查键是否已过期"""
        if key not in self.db.expires:
            # key 未设置, 返回 False
            return False
        tll = self.db.expires[key] - time.time()
        return tll <= 0

    def run_handle_expired_keys_task(self):
        """
        后台运行过期键任务
        """
        cleanup_thread = threading.Thread(target=self.handle_expired_keys_task,
                                          daemon=True)
        cleanup_thread.start()

    def handle_expired_keys_task(self):
        """过期键处理任务"""
        while True:
            # 数据库关闭的话，退出任务
            if not self.db:
                break

            expired_keys = [
                key for key in self.db.expires.keys()
                if self.is_expired(key)
            ]
            if expired_keys:
                self.db.delete(*expired_keys)
            time.sleep(1)

    def check_expired(self, key: str) -> bool:
        """
        检查键是否过期
        """
        if self.is_expired(key):
            self.db.delete(key)
            return True
        return False
