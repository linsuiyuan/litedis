from threading import Thread
from unittest.mock import patch

import pytest

from refactor2.server.dbmanager import DBManager, PersistenceType, _dbs  # noqa
from refactor2.server.persistence.ldb import LitedisDB


@pytest.fixture
def temp_dir(tmp_path_factory):
    """Create a unique temporary directory for each test"""
    return tmp_path_factory.mktemp("litedis")

@pytest.fixture(autouse=True)
def reset_singleton():
    setattr(DBManager, '_instances', {})
    _dbs.clear()
    yield

@pytest.fixture
def mock_aof():
    with patch('refactor2.server.persistence.AOF') as mock:
        yield mock

@pytest.fixture
def db_manager(temp_dir):
    manager = DBManager(temp_dir)
    yield manager

def test_get_or_create_db(db_manager):
    db1 = db_manager.get_or_create_db("test_db1")
    assert isinstance(db1, LitedisDB)
    assert db1.name == "test_db1"

    db2 = db_manager.get_or_create_db("test_db1")
    assert db1 is db2

def test_concurrent_db_creation(db_manager):
    db_instances = []

    def create_db():
        db_ = db_manager.get_or_create_db("test_concurrent_db")
        db_instances.append(db_)

    threads = [Thread(target=create_db) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    first_db = db_instances[0]
    for db in db_instances[1:]:
        assert db is first_db

def test_persistence_type_properties(db_manager):
    # Test MIXED mode
    db_manager.persistence_type = PersistenceType.MIXED
    assert db_manager._need_aof_persistence is True
    assert db_manager._need_ldb_persistence is True

    # Test AOF mode
    db_manager.persistence_type = PersistenceType.AOF
    assert db_manager._need_aof_persistence is True
    assert db_manager._need_ldb_persistence is False

    # Test LDB mode
    db_manager.persistence_type = PersistenceType.LDB
    assert db_manager._need_aof_persistence is False
    assert db_manager._need_ldb_persistence is True
