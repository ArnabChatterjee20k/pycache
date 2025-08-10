from .Datatype import Datatype
from collections.abc import Iterable


class Set(Datatype):
    def __init__(self, value):
        self._allowed_classes = [Iterable, set, list, tuple]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, set):
            return self._value
        return set(self._value)

    @staticmethod
    def get_name() -> str:
        return "set"
