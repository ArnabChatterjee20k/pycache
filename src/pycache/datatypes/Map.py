from .Datatype import Datatype

class Map(Datatype):
    def __init__(self, value):
        super().__init__(value)
    
    @property
    def value(self):
        return dict(self._value)
    
    @staticmethod
    def validate(value):
        if not isinstance(value, dict):
            raise TypeError("Expected a dictionary")
