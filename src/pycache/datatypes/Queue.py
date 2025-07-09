from .Datatype import Datatype
from collections import deque
from collections.abc import Iterable


class Queue(Datatype):
    def __init__(self, value):
        self._allowed_classes = [Iterable, list, deque]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, deque):
            return self._value
        return deque(self._value)
