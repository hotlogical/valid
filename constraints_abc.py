from abc import ABC, abstractmethod

class ConstraintsABC(ABC):

    @abstractmethod
    def greater_equal(self, field, value):
        pass

    @abstractmethod
    def greater(self, field, value):
        pass

    @abstractmethod
    def less_equal(self, field, value):
        pass

    @abstractmethod
    def less(self, field, value):
        pass
