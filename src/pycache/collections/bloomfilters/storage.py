import struct
from typing import ClassVar
from dataclasses import dataclass

"""
Header and Chain are not dependent on each other and they all calculates from the main bloom filter.
Header for metadata
Chain for actual bloom filter

Dumping
Why not storing bytes per chain?
It will be stored at the end after the Header and Chain metadata are stored
Reason -> for eaiser iteration over the large chunks and can be sent and stored easily via backups or send to other services over networks

[ File Header ]
[ Chain 0 metadata ]
[ Chain 1 metadata ]
[ Chain 2 metadata ]
...
[ Chain N-1 metadata ]
[ Raw bytes for chain 0 ]
[ Raw bytes for chain 1 ]
[ Raw bytes for chain 2 ]
...
[ Raw bytes for chain N-1 ]

Each raw bytes
- chunk1
- chunk2

Loading
Header creates skeleton filters and Chain fill those skeleton
"""


@dataclass
class Header:
    chains: int
    unique_elements: int
    growth: int
    tightening: int

    filters: list["Chain"]

    packing_format: ClassVar["str"] = "<QIII"
    size: ClassVar[int] = 20  # bytes(8+4*3)

    @classmethod
    def pack(cls, chains, unique_elements, growth, tightening) -> bytes:
        """
        uint64_t unique_elements;     // Total unique elements across all chains
        uint32_t growth;              // Growth multiplier
        uint32_t tightening;          // Error tightening factor
        uint32_t chains;              // Number of chains/filters
        """
        return struct.pack(
            cls.packing_format, chains, unique_elements, growth, tightening
        )

    @classmethod
    def unpack(cls, raw_bytes: bytes) -> "Header":
        """
        Raw bytes include header + chains metadata
        """
        if len(raw_bytes) < cls.size:
            raise ValueError(
                f"Data too small: expected at least {cls.STRUCT_SIZE} bytes"
            )

        unique_elements, growth, tightening, chains = struct.unpack(
            cls.packing_format, raw_bytes
        )
        filters = []

        filters_metadata = raw_bytes[cls.size :]
        # for _ in range(chains):

        for i in range(chains):
            filters.append(Chain())

        return Header(unique_elements, growth, tightening, chains, filters)


@dataclass
class Chain:
    false_positive_rate: float
    number_of_elements: int
    # taking hash_functions as double as RationalBloomfilter uses rational hash_functions
    hash_functions: float
    unique_elements: int

    packing_format: ClassVar["str"] = "<dQdQ"
    size: ClassVar[int] = 32  # bytes(8*4)

    @classmethod
    def pack(cls) -> bytes:
        """
        double false_positive_rate
        uint64 number_of_elements
        double hash_functions
        uint64 unique_elements
        """
        pass

    @classmethod
    def unpack() -> "Chunk":
        pass


@dataclass
class Chunk:
    raw_bytes: bytes | None
    next_chunk: int | None

    @classmethod
    def pack(cls) -> bytes:
        pass

    @classmethod
    def unpack() -> "Chunk":
        pass
