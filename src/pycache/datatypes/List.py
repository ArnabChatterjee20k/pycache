from .Datatype import Datatype
from collections.abc import Iterable


class List(Datatype):
    def __init__(self, value):
        self._allowed_classes = [Iterable, list, tuple]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, list):
            return self._value
        return list(self._value)

    @staticmethod
    def get_name() -> str:
        return "list"
