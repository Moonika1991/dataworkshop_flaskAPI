from abc import ABC, abstractmethod


class Connector(ABC):
    # Factory method  product

    @abstractmethod
    def execute(self, function):
        pass

    @abstractmethod
    def alt(self, args):
        pass

    @abstractmethod
    def conj(self, args):
        pass

    @abstractmethod
    def col(self, args):
        pass

    @abstractmethod
    def exc(self, args):
        pass