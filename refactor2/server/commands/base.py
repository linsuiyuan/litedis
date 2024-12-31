from abc import ABC, abstractmethod


class Command(ABC):
    name = None

    @abstractmethod
    def execute(self):...


class CommandParser(ABC):
    name = None

    @abstractmethod
    def parse(self):...

