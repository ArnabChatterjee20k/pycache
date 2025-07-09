from .Datatype import Datatype


class String(Datatype):
    def __init__(self, value):
        # not allowing explicitly any specific instance as we can have string of any properties
        self._allowed_classes = []
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, str):
            return self._value
        return str(self._value)
