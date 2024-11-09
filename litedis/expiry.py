import threading
import time


class Expiry:

    def __init__(self, expires):
        self.expires = expires

    def is_expired(self, key: str) -> bool:
        """检查键是否已过期"""
        if key not in self.expires:
            # 未设置, 返回 False
            return False
        tll = self.expires[key] - time.time()
        return tll <= 0

    def run_handle_expired_keys_task(self, callback):
        """
        后台运行过期键任务
        :param callback: 处理过期键时调用
        :return:
        """
        cleanup_thread = threading.Thread(target=self.handle_expired_keys_task,
                                          args=[callback],
                                          daemon=True)
        cleanup_thread.start()

    def handle_expired_keys_task(self, callback):
        """过期键处理任务"""
        while True:
            expired_keys = [
                key for key in self.expires.keys()
                if self.is_expired(key)
            ]
            callback(*expired_keys)
            time.sleep(1)

    def check_expired(self, key: str, callback) -> bool:
        """
        检查键是否过期
        :param key:
        :param callback: 处理过期键时调用
        :return:
        """
        if self.is_expired(key):
            callback(key)
            return True
        return False
