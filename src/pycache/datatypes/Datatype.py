from abc import ABC, abstractmethod


class Datatype(ABC):
    def __init__(self, value):
        Datatype.validate(value)
        self._value = value

    @property
    @abstractmethod
    def value(self):
        """for getting the python native version of the datatype"""
        pass

    @staticmethod
    @abstractmethod
    def validate(value):
        pass
