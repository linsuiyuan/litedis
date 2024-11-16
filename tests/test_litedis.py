import pytest
import time
from pathlib import Path
from litedis.litedis import Litedis
from litedis import PersistenceType


class TestLitedis:
    """Litedis 基本功能测试"""

    @pytest.fixture(autouse=True)
    def setup_db(self):
        """创建测试用的数据库实例"""
        test_dir = Path("./test_data")
        self.db = Litedis(
            db_name="test_db",
            data_dir=str(test_dir),
            singleton=False
        )
        yield
        # 清理测试数据
        self.db.close()
        if test_dir.exists():
            for file in test_dir.glob("*"):
                file.unlink()
            test_dir.rmdir()

    def test_singletonmeta(self):
        """测试单例元类"""
        test_dir = Path("./connection")
        connection_string = "litedis:///connection/dbname"
        db = Litedis(connection_string=connection_string)

        # self.db 不是单例，不相等
        assert db is not self.db

        # db 和 db2 是单例，应相等
        db2 = Litedis(connection_string=connection_string)
        assert db is db2

        # 清理测试数据
        db.close()
        if test_dir.exists():
            for file in test_dir.glob("*"):
                file.unlink()
            test_dir.rmdir()

    def test_connection_string(self):
        """创建测试用的数据库实例"""
        test_dir = Path("./connection")
        connection_string = "litedis:///connection/dbname"
        db = Litedis(connection_string=connection_string)

        assert test_dir.exists()
        db.set("connection_key1", "connection_value1")
        assert db.get("connection_key1") == "connection_value1"

        # 清理测试数据
        db.close()
        if test_dir.exists():
            for file in test_dir.glob("*"):
                file.unlink()
            test_dir.rmdir()

    def test_close(self):
        """测试关闭数据库及释放资源"""
        # 设置一些数据
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")

        # 确保数据存在
        assert self.db.exists("key1") is 1
        assert self.db.exists("key2") is 1

        # 关闭数据库
        self.db.close()

        # 重新打开数据库，确保数据持久化
        self.db = Litedis(
            db_name="test_db",
            data_dir="./test_data",
            persistence=PersistenceType.MIXED
        )
        assert self.db.get("key1") == "value1"
        assert self.db.get("key2") == "value2"
