import struct
from typing import ClassVar, Iterator
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

* Here each chunk is not distinguished separately and are continuous. Means we go on writing chunks1,2,3,4,5,... and all the chain's 
bytes are present

Loading
Header creates skeleton filters and Chain fill those skeleton
"""


@dataclass
class Header:
    """
    Global header containing metadata for entire bloom filter chain
    """

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
            raise ValueError(f"Data too small: expected at least {cls.size} bytes")

        chains, unique_elements, growth, tightening = struct.unpack(
            cls.packing_format, raw_bytes[: cls.size]
        )
        filters = []

        filters_metadata = raw_bytes[cls.size :]
        for i in range(chains):
            current_chain_raw_bytes = filters_metadata[
                i * Chain.size : (i + 1) * Chain.size
            ]
            filters.append(Chain.unpack(current_chain_raw_bytes))

        return Header(
            unique_elements=unique_elements,
            growth=growth,
            tightening=tightening,
            chains=chains,
            filters=filters,
        )


@dataclass
class Chain:
    """
    Metadata for a single bloom filter in the chain
    """

    bytes_size: int  # amount of bytes stored in the chain bit array
    false_positive_rate: float
    number_of_elements: int
    # taking hash_functions as double as RationalBloomfilter uses rational hash_functions
    hash_functions: float
    unique_elements: int

    packing_format: ClassVar["str"] = "<QdQdQ"
    size: ClassVar[int] = 40  # bytes(8*5)

    @classmethod
    def pack(
        cls,
        bytes_size,
        false_positive_rate,
        number_of_elements,
        hash_functions,
        unique_elements,
    ) -> bytes:
        """
        double false_positive_rate
        uint64 number_of_elements
        double hash_functions
        uint64 unique_elements
        """
        return struct.pack(
            cls.packing_format,
            bytes_size,
            false_positive_rate,
            number_of_elements,
            hash_functions,
            unique_elements,
        )

    @classmethod
    def unpack(cls, raw_bytes) -> "Chain":
        if len(raw_bytes) < cls.size:
            raise ValueError(f"Data too small: expected at least {cls.size} bytes")
        (
            bytes_size,
            false_positive_rate,
            number_of_elements,
            hash_functions,
            unique_elements,
        ) = struct.unpack(cls.packing_format, raw_bytes[: cls.size])
        return Chain(
            bytes_size=bytes_size,
            false_positive_rate=false_positive_rate,
            number_of_elements=number_of_elements,
            hash_functions=hash_functions,
            unique_elements=unique_elements,
        )


@dataclass
class Chunk:
    """
    A chunk of raw bit array data

    Used for streaming large filters piece by piece
    Iterator pattern allows resumable, stateless transfers
    """

    raw_bytes: bytes | None
    next_chunk: int | None

    _max_chunk_size_in_bytes = 128

    def stream(self, start_offset: int = 0) -> Iterator["Chunk"]:
        """
        Zero-copy, index-based chunk streaming.
        """
        size = 0
        if self.raw_bytes:
            size = len(self.raw_bytes)

        chunk = 0
        bytes_offset = start_offset * self.max_chunk_size
        while bytes_offset < size:
            next_bytes_offset = min(bytes_offset + self.max_chunk_size, size)
            next_chunk = (
                chunk + 1 if bytes_offset + self.max_chunk_size < size else None
            )

            yield Chunk(self.raw_bytes[bytes_offset:next_bytes_offset], next_chunk)

            bytes_offset = next_bytes_offset
            chunk += 1

    @property
    def max_chunk_size(self):
        return self._max_chunk_size_in_bytes

    @max_chunk_size.setter
    def max_chunk_size(self, value: int):
        self._max_chunk_size_in_bytes = value
