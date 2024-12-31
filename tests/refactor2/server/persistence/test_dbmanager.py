from threading import Thread

from refactor2.server.persistence.dbmanager import DBManager
from refactor2.server.persistence.ldb import LitedisDB


def test_dbmanager_singleton():
    manager1 = DBManager()
    manager2 = DBManager()
    assert manager1 is manager2


def test_get_or_create_db():
    manager = DBManager()

    db1 = manager.get_or_create_db("test_db1")
    assert isinstance(db1, LitedisDB)
    assert db1.name == "test_db1"

    db2 = manager.get_or_create_db("test_db1")
    assert db1 is db2


def test_concurrent_db_creation():
    manager = DBManager()
    db_instances = []

    def create_db():
        db_ = manager.get_or_create_db("test_concurrent_db")
        db_instances.append(db_)

    threads = [Thread(target=create_db) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    first_db = db_instances[0]
    for db in db_instances[1:]:
        assert db is first_db
