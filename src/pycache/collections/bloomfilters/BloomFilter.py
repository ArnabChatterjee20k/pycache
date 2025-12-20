import xxhash
import math
from ..bitarray.BitArray import BitArray
from .storage import MAX_CHUNK_SIZE, Chain, Header

# large primes
_MAGIC_RANDOM_SEED_1 = 0x9E3779B97F4A7C15
_MAGIC_RANDOM_SEED_2 = 0xC3A5C85C97CB3127


def _double_hash(size: int, item: str, round: int):
    hash1 = xxhash.xxh64_intdigest(item, seed=_MAGIC_RANDOM_SEED_1)
    hash2 = xxhash.xxh64_intdigest(item, seed=_MAGIC_RANDOM_SEED_2)
    return (hash1 + (round * hash2)) % size


def _get_size(number_of_elements: int, false_positive_rate: float):
    if false_positive_rate <= 0:
        return number_of_elements

    return math.ceil(
        -(number_of_elements * math.log(false_positive_rate)) / (math.log(2) ** 2)
    )


def _get_number_of_hash_functions(size: int, number_of_elements: int):
    return (size / number_of_elements) * math.log(2)


class BloomFilter:
    def __init__(self, number_of_elements: int, false_positive_rate: float):
        self._size = _get_size(number_of_elements, false_positive_rate)
        self.number_of_elements = number_of_elements
        self.false_positive_rate = false_positive_rate
        self._number_of_hash_functions = round(
            _get_number_of_hash_functions(self._size, number_of_elements)
        )
        self._unique_elements_inserted = 0
        self._bit_array = BitArray(self._size)

    def exists(self, key: str) -> bool:
        for i in range(self._number_of_hash_functions):
            index = _double_hash(self.size, key, i)
            if self._bit_array[index] == 0:
                return False

        return True

    def add(self, key: str) -> bool:
        new_bits_toggled = False
        for i in range(self._number_of_hash_functions):
            index = _double_hash(self.size, key, i)
            if not self._bit_array[index]:
                new_bits_toggled = True
            self._bit_array[index] = 1

        if new_bits_toggled:
            self._unique_elements_inserted += 1

        return new_bits_toggled
    
    def dump_header(self) -> Header:
        pass

    def dump(self) -> bytes:
        return bytes(self._bit_array.value)

    def load_header(self) -> None:
        pass

    def load(self) -> None:
        return bytes(self._bit_array.value)

    def __len__(self) -> int:
        return self._unique_elements_inserted

    @property
    def size(self):
        return self._size

    @property
    def value(self) -> bytearray:
        """returns the whole bloom filter actual value"""
        return self._bit_array.value

    @value.setter
    def value(self, new_value: bytearray):
        """builds the bloom filter from scratch"""
        # TODO: need to set this logic correctly as it might need validation
        # need to check redis load and dump
        self._bit_array.value = new_value

    @property
    def chains(self) -> int:
        """return number of chains the bloom filter is having"""
        return 1

    @property
    def active(self) -> "BloomFilter":
        """return the current active bloom filter"""
        return self
