from .Datatype import Datatype


class Numeric(Datatype):
    def __init__(self, value):
        self._allowed_classes = [int, float]
        self._disallowed_classes = [
            bool,
        ]
        super().__init__(value)

    @property
    def value(self) -> int | float:
        return self._value
