from abc import ABC

from refactor.server import LitedisDb

class BaseClient(ABC):
    db: LitedisDb