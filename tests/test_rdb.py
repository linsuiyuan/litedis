import threading
import time
import weakref

import pytest

from litedis import (BaseLitedis,
                     DataType,
                     PersistenceType)
from litedis.rdb import RDB


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录用于测试"""
    return tmp_path


@pytest.fixture
def db(temp_dir):
    """测试用的数据库数据"""
    db = BaseLitedis()
    db.db_lock = threading.Lock()
    db.db_name = "test_db"
    db.data_dir = temp_dir
    db.persistence = PersistenceType.MIXED
    db.data = {
        "key1": "value1",
        "key2": ["list", "items"]
    }
    db.data_types = {
        "key1": DataType.STRING,
        "key2": DataType.LIST
    }
    db.expires = {}
    return db


@pytest.fixture
def rdb(db):
    """创建RDB实例的fixture"""
    return RDB(
        db=weakref.ref(db),
        rdb_save_frequency=1,
        compression=True
    )


def test_save_and_read_rdb(rdb):
    """测试RDB的保存和读取功能"""
    # 保存数据
    assert rdb.save_rdb() is True

    # 验证文件是否存在
    assert rdb.rdb_path.exists()


def test_save_rdb_without_compression(db):
    """测试不使用压缩的RDB保存"""
    rdb = RDB(
        db=weakref.ref(db),
        compression=False
    )

    assert rdb.save_rdb() is True


def test_background_save(rdb):
    """测试后台保存功能"""
    rdb.save_task_in_background()
    # 等待后台任务执行
    time.sleep(2)

    # 验证文件是否已创建
    assert rdb.rdb_path.exists()


def test_read_nonexistent_rdb(rdb):
    """测试读取不存在的RDB文件"""
    # 确保文件不存在
    if rdb.rdb_path.exists():
        rdb.rdb_path.unlink()

    # 读取不存在的文件应该返回None
    assert rdb.read_rdb() is None


def test_save_rdb_with_invalid_data(rdb):
    """测试保存无效数据时的错误处理"""
    # 创建一个包含无法序列化对象的数据
    rdb.db.data = {"key": lambda x: x}  # lambda函数无法被pickle序列化

    with pytest.raises(Exception) as exc_info:
        rdb.save_rdb()
    assert "保存文件出错" in str(exc_info.value)
