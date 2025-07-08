from .Datatype import Datatype


class String(Datatype):
    def __init__(self, value):
        super().__init__(value)

    @property
    def value(self):
        return str(self._value)

    @staticmethod
    def validate(value):
        if not isinstance(value, str):
            raise TypeError("Expected a string")
