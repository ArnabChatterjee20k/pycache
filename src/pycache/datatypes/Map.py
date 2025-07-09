from .Datatype import Datatype


class Map(Datatype):
    def __init__(self, value):
        self._allowed_classes = [dict]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, dict):
            return self._value
        return dict(self._value)
