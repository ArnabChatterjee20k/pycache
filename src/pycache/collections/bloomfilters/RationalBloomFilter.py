import math
import xxhash
from .BloomFilter import BloomFilter, _double_hash


class RationalBloomFilter(BloomFilter):
    _MAGIC_RANDOM_SEED_ACTIVATION = 99991

    def __init__(self, number_of_elements: int, false_positive_rate: float):
        super().__init__(number_of_elements, false_positive_rate)
        self.floor_k = math.floor(self._number_of_hash_functions)
        self._activation_proabability = self._number_of_hash_functions - math.floor(
            self._number_of_hash_functions
        )

    def __len__(self):
        return self._unique_elements_inserted

    def add(self, key) -> bool:
        # not using super add as here floor_k is used
        new_bits_toggled = False
        for i in range(self.floor_k):
            index = _double_hash(self.size, key, i)
            if not self._bit_array[index]:
                new_bits_toggled = True
            self._bit_array[index] = 1

        if self._is_activation_required(key):
            # not using i+1 and using self.floor_k as if i is 0 then i+1 wont get 1
            index = _double_hash(self.size, key, self.floor_k)
            if not self._bit_array[index]:
                new_bits_toggled = True
            self._bit_array[index] = 1

        if new_bits_toggled:
            self._unique_elements_inserted += 1

        return new_bits_toggled

    def exists(self, key) -> bool:
        for i in range(self.floor_k):
            index = _double_hash(self.size, key, i)
            if self._bit_array[index] == 0:
                return False

        if self._is_activation_required(key):
            # not using i+1 and using self.floor_k as if i is 0 then i+1 wont get 1
            index = _double_hash(self.size, key, self.floor_k)
            return self._bit_array[index] == 1

        return True

    @property
    def size(self):
        return self._size

    @property
    def active(self) -> "BloomFilter":
        return self

    def _is_activation_required(self, key: str) -> bool:
        # deterministic hash per key
        # for a same key , activation will always same
        # using random seed which is different from hashing used for bits index finding
        hash_int = xxhash.xxh64_intdigest(key, seed=self._MAGIC_RANDOM_SEED_ACTIVATION)
        # making it in range [0,1)
        max_range_of_hash = 2**64 - 1
        hashed_value = hash_int / max_range_of_hash
        return hashed_value < self._activation_proabability
