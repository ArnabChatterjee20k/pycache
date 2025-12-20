from .Datatype import Datatype
from ..collections.bitarray.BitArray import BitArray as BitArrayImpl


class BitArray(Datatype):
    def __init__(self, value):
        self._allowed_classes = [BitArrayImpl]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, BitArrayImpl):
            return self._value
        # If value is bytes/bytearray, we need to reconstruct the BitArray
        # This would require the size parameter
        # For now, we'll assume the value is already a BitArray instance
        raise TypeError("BitArray datatype requires a BitArray instance")

    @staticmethod
    def get_name() -> str:
        return "bitarray"
