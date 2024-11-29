import threading
import time
import weakref

import pytest  # noqa

from litedis import BaseLitedis, DataType, PersistenceType
from litedis.ldb import LDB


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
def ldb(db):
    """创建LDB实例的fixture"""
    return LDB(
        db=weakref.ref(db),
        ldb_save_frequency=1,
        compression=True
    )


def test_save_and_read_ldb(ldb):
    """测试LDB的保存和读取功能"""
    # 保存数据
    assert ldb.save_ldb() is True

    # 验证文件是否存在
    assert ldb.ldb_path.exists()


def test_save_ldb_without_compression(db):
    """测试不使用压缩的LDB保存"""
    ldb = LDB(
        db=weakref.ref(db),
        compression=False
    )

    assert ldb.save_ldb() is True


def test_background_save(ldb):
    """测试后台保存功能"""
    ldb.save_task_in_background()
    # 等待后台任务执行
    time.sleep(2)

    # 验证文件是否已创建
    assert ldb.ldb_path.exists()


def test_read_nonexistent_ldb(ldb):
    """测试读取不存在的LDB文件"""
    # 确保文件不存在
    if ldb.ldb_path.exists():
        ldb.ldb_path.unlink()

    # 读取不存在的文件应该返回None
    assert ldb.read_ldb() is None


def test_save_ldb_with_invalid_data(ldb):
    """测试保存无效数据时的错误处理"""
    # 创建一个包含无法序列化对象的数据
    ldb.db.data = {"key": lambda x: x}  # lambda函数无法被pickle序列化

    with pytest.raises(Exception) as exc_info:
        ldb.save_ldb()
    assert "保存文件出错" in str(exc_info.value)
