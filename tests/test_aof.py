import json
import time
import weakref

import pytest  # noqa

from litedis import AOFFsyncStrategy, BaseLitedis, DataType, PersistenceType
from litedis.aof import AOF


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录用于测试"""
    return tmp_path


@pytest.fixture
def db(temp_dir):
    """创建db实例用于测试"""
    db = BaseLitedis()
    db.db_name = "test_db"
    db.data_dir = temp_dir
    db.persistence = PersistenceType.MIXED
    db.data = {
        "key": "value"
    }
    db.data_types = {
        "key": DataType.STRING
    }
    return db


@pytest.fixture
def weakref_db(db):
    """创建db弱引用实例用于测试"""
    return weakref.ref(db)


@pytest.fixture
def aof_always(weakref_db):
    """创建一个 fsync=AOFFsyncStrategy.ALWAYS 的 AOF 实例"""
    return AOF(weakref_db, AOFFsyncStrategy.ALWAYS)


@pytest.fixture
def aof_everysec(weakref_db):
    """创建一个 fsync=AOFFsyncStrategy.EVERYSEC 的 AOF 实例"""
    return AOF(weakref_db, AOFFsyncStrategy.EVERYSEC)


class TestAOF:
    def test_append_and_read_with_always_fsync(self, aof_always):
        """测试 always 模式下的命令追加和读取"""
        test_commands = [
            {"cmd": "SET", "args": ["key1", "value1"]},
            {"cmd": "SET", "args": ["key2", "value2"]},
        ]

        # 追加命令
        for cmd in test_commands:
            aof_always.append(cmd)

        # 读取并验证命令
        read_commands = list(aof_always.read_aof_commands())
        assert read_commands == test_commands

    def test_append_with_everysec_fsync(self, aof_everysec):
        """
        测试 everysec 模式下的命令追加
        附带测试了 run_fsync_task_in_background
        """
        test_command = {"cmd": "SET", "args": ["key1", "value1"]}

        # 启动后台同步任务
        aof_everysec.run_fsync_task_in_background()

        # 追加命令
        aof_everysec.append(test_command)

        # 等待后台同步
        time.sleep(1.1)

        # 读取并验证命令
        read_commands = list(aof_everysec.read_aof_commands())
        assert read_commands == [test_command]

    def test_clear_aof(self, aof_always):
        """测试清理 AOF 文件"""
        test_command = {"cmd": "SET", "args": ["key1", "value1"]}
        aof_always.append(test_command)

        # 确认文件存在
        assert aof_always.aof_path.exists()

        # 清理文件
        aof_always.clear_aof()

        # 确认文件已删除
        assert not aof_always.aof_path.exists()

    def test_invalid_file_handling(self, aof_always):
        """测试文件操作异常处理"""

        # 创建一个无效的 AOF 文件
        with open(aof_always.aof_path, 'w', encoding='utf-8') as f:
            f.write("invalid json\n")

        # 验证读取无效文件时会抛出异常
        with pytest.raises(Exception) as exc_info:
            list(aof_always.read_aof_commands())
        assert "读取 AOF 文件 出现错误" in str(exc_info.value)
