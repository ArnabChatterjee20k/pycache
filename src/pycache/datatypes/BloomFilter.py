from .Datatype import Datatype
from ..collections.bloomfilters import BloomFilter as BloomFilterImpl


class BloomFilter(Datatype):
    def __init__(self, value):
        self._allowed_classes = [BloomFilterImpl]
        super().__init__(value)

    @property
    def value(self):
        if isinstance(self._value, BloomFilterImpl):
            return self._value
        # If value is bytes/bytearray, we need to reconstruct the BloomFilter
        # This would require additional metadata (size, false_positive_rate, etc.)
        # For now, we'll assume the value is already a BloomFilter instance
        raise TypeError("BloomFilter datatype requires a BloomFilter instance")

    @staticmethod
    def get_name() -> str:
        return "bloomfilter"
