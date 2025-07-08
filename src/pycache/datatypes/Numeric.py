from .Datatype import Datatype

class Numeric(Datatype):
    def __init__(self, value):
        Numeric.validate(value)
        super().__init__(value)
    
    @property
    def value(self)->int|float:
        return self._value

    def validate(value):
        if not isinstance(value, (int, float)):
            raise TypeError("Expected int or float")