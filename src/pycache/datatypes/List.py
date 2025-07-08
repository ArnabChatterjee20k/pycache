from .Datatype import Datatype
class List(Datatype):
    def __init__(self, value):
        super().__init__(value)
    
    @property
    def value(self):
        return list(self._value)
    
    @staticmethod
    def validate(value):
        try:
            iter(value)
        except TypeError:
            raise TypeError("Expected an iterable value")
        