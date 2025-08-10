from .Datatype import Datatype
from collections.abc import Iterable


class Streams(Datatype):
    def __init__(self, value):
        self._allowed_classes = [Iterable, list, tuple, dict]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, list):
            return self._value
        elif isinstance(self._value, dict):
            return list(self._value.items())
        return list(self._value)

    @staticmethod
    def get_name() -> str:
        return "streams"
