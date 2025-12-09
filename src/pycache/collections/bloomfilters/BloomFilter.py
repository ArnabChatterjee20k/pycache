from abc import ABC, abstractmethod

import xxhash
import math

_MAGIC_RANDOM_SEED_1 = 0
_MAGIC_RANDOM_SEED_2 = 1


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


class BloomFilter(ABC):
    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def add(self, key: str) -> bool:
        pass

    # get the number of unique elements inserted
    @abstractmethod
    def __len__(self) -> int:
        pass

    @property
    @abstractmethod
    def value(self):
        pass
