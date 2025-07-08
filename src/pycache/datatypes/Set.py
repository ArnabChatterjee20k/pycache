from .Datatype import Datatype

class Set(Datatype):
    def __init__(self, value):
        super().__init__(value)
    
    @property
    def value(self):
        return set(self._value)
    
    @staticmethod
    def validate(value):
        try:
            iter(value)
        except TypeError:
            raise TypeError("Expected an iterable value")
