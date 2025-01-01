from abc import ABC, abstractmethod


class CommandLogger(ABC):

    @abstractmethod
    def log_command(self, dbname: str, cmdline: str):...

class CommandProcessor(ABC):

    @abstractmethod
    def process_command(self, dbname: str, cmdline: str):...

    @abstractmethod
    def replay_command(self, dbname: str, cmdline: str):...