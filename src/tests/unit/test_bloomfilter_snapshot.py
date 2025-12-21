import pytest
import struct
from src.pycache.collections.bloomfilters.snapshot import Header, Chain, Chunk


class TestChain:
    """Unit tests for Chain class - metadata for a single bloom filter"""

    def test_chain_pack_basic(self):
        """Test packing a Chain with basic values"""
        chain = Chain.pack(
            bytes_size=1024,
            false_positive_rate=0.01,
            number_of_elements=100,
            hash_functions=7.5,
            unique_elements=95,
        )

        assert isinstance(chain, bytes)
        assert len(chain) == Chain.size

    def test_chain_unpack_basic(self):
        """Test unpacking a Chain with basic values"""
        packed = Chain.pack(
            bytes_size=1024,
            false_positive_rate=0.01,
            number_of_elements=100,
            hash_functions=7.5,
            unique_elements=95,
        )

        unpacked = Chain.unpack(packed)

        assert unpacked.bytes_size == 1024
        assert unpacked.false_positive_rate == pytest.approx(0.01)
        assert unpacked.number_of_elements == 100
        assert unpacked.hash_functions == pytest.approx(7.5)
        assert unpacked.unique_elements == 95

    def test_chain_pack_unpack_round_trip(self):
        """Test that pack and unpack are inverse operations"""
        original = Chain(
            bytes_size=2048,
            false_positive_rate=0.001,
            number_of_elements=1000,
            hash_functions=10.25,
            unique_elements=987,
        )

        packed = Chain.pack(
            original.bytes_size,
            original.false_positive_rate,
            original.number_of_elements,
            original.hash_functions,
            original.unique_elements,
        )

        unpacked = Chain.unpack(packed)

        assert unpacked.bytes_size == original.bytes_size
        assert unpacked.false_positive_rate == pytest.approx(
            original.false_positive_rate
        )
        assert unpacked.number_of_elements == original.number_of_elements
        assert unpacked.hash_functions == pytest.approx(original.hash_functions)
        assert unpacked.unique_elements == original.unique_elements

    def test_chain_pack_large_values(self):
        """Test packing Chain with large values"""
        packed = Chain.pack(
            bytes_size=2**63 - 1,  # Max uint64
            false_positive_rate=0.999999,
            number_of_elements=2**63 - 1,
            hash_functions=100.0,
            unique_elements=2**63 - 1,
        )

        unpacked = Chain.unpack(packed)

        assert unpacked.bytes_size == 2**63 - 1
        assert unpacked.number_of_elements == 2**63 - 1
        assert unpacked.unique_elements == 2**63 - 1

    def test_chain_pack_zero_values(self):
        """Test packing Chain with zero values"""
        packed = Chain.pack(
            bytes_size=0,
            false_positive_rate=0.0,
            number_of_elements=0,
            hash_functions=0.0,
            unique_elements=0,
        )

        unpacked = Chain.unpack(packed)

        assert unpacked.bytes_size == 0
        assert unpacked.false_positive_rate == pytest.approx(0.0)
        assert unpacked.number_of_elements == 0
        assert unpacked.hash_functions == pytest.approx(0.0)
        assert unpacked.unique_elements == 0

    def test_chain_unpack_insufficient_data(self):
        """Test that unpack raises ValueError for insufficient data"""
        insufficient_data = b"x" * (Chain.size - 1)

        with pytest.raises(ValueError, match="Data too small"):
            Chain.unpack(insufficient_data)

    def test_chain_unpack_empty_bytes(self):
        """Test that unpack raises ValueError for empty bytes"""
        with pytest.raises(ValueError, match="Data too small"):
            Chain.unpack(b"")

    def test_chain_unpack_exact_size(self):
        """Test unpack with exactly the required size"""
        packed = Chain.pack(
            bytes_size=100,
            false_positive_rate=0.05,
            number_of_elements=50,
            hash_functions=5.0,
            unique_elements=48,
        )

        # Should work with exact size
        unpacked = Chain.unpack(packed)
        assert unpacked.bytes_size == 100

    def test_chain_unpack_extra_data(self):
        """Test unpack with extra data beyond required size"""
        packed = Chain.pack(
            bytes_size=100,
            false_positive_rate=0.05,
            number_of_elements=50,
            hash_functions=5.0,
            unique_elements=48,
        )

        # Add extra bytes
        packed_with_extra = packed + b"extra"

        # Should work fine - implementation slices to exact size
        unpacked = Chain.unpack(packed_with_extra)
        assert unpacked.bytes_size == 100

    def test_chain_size_class_var(self):
        """Test that Chain.size is correctly defined"""
        assert Chain.size == 40  # 8*5 bytes as per comment
        assert isinstance(Chain.size, int)

    def test_chain_packing_format(self):
        """Test that packing format matches expected structure"""
        # Verify the format string matches the expected types
        assert Chain.packing_format == "<QdQdQ"
        # Q = uint64 (8 bytes), d = double (8 bytes)
        # Total: 8 + 8 + 8 + 8 + 8 = 40 bytes


class TestHeader:
    """Unit tests for Header class - global header for bloom filter chain"""

    def test_header_pack_basic(self):
        """Test packing a Header with basic values"""
        header_bytes = Header.pack(
            chains=3,
            unique_elements=1000,
            growth=2,
            tightening=2,
        )

        assert isinstance(header_bytes, bytes)
        assert len(header_bytes) == Header.size

    def test_header_unpack_basic(self):
        """Test unpacking a Header with basic values"""
        # Create header bytes with chain metadata
        header_bytes = Header.pack(
            chains=2,
            unique_elements=500,
            growth=2,
            tightening=2,
        )

        # Add chain metadata
        chain1 = Chain.pack(
            bytes_size=1024,
            false_positive_rate=0.01,
            number_of_elements=100,
            hash_functions=7.0,
            unique_elements=95,
        )
        chain2 = Chain.pack(
            bytes_size=2048,
            false_positive_rate=0.005,
            number_of_elements=200,
            hash_functions=8.0,
            unique_elements=190,
        )

        full_bytes = header_bytes + chain1 + chain2
        header = Header.unpack(full_bytes)

        assert header.chains == 2
        assert header.unique_elements == 500
        assert header.growth == 2
        assert header.tightening == 2
        assert len(header.filters) == 2
        assert header.filters[0].bytes_size == 1024
        assert header.filters[1].bytes_size == 2048

    def test_header_pack_unpack_round_trip(self):
        """Test that pack and unpack are inverse operations"""
        # Create header with chains
        header_bytes = Header.pack(
            chains=3,
            unique_elements=1500,
            growth=3,
            tightening=2,
        )

        # Verify header packing
        assert len(header_bytes) == Header.size

        # Add chain metadata
        chains_data = b""
        expected_chains = []
        for i in range(3):
            chain = Chain.pack(
                bytes_size=1024 * (i + 1),
                false_positive_rate=0.01 / (i + 1),
                number_of_elements=100 * (i + 1),
                hash_functions=7.0 + i,
                unique_elements=95 * (i + 1),
            )
            chains_data += chain
            expected_chains.append(Chain.unpack(chain))

        full_bytes = header_bytes + chains_data
        header = Header.unpack(full_bytes)

        assert header.chains == 3
        assert header.unique_elements == 1500
        assert header.growth == 3
        assert header.tightening == 2
        assert len(header.filters) == 3

        # Verify chain data
        for i, filter_chain in enumerate(header.filters):
            assert filter_chain.bytes_size == expected_chains[i].bytes_size
            assert filter_chain.false_positive_rate == pytest.approx(
                expected_chains[i].false_positive_rate
            )

    def test_header_unpack_zero_chains(self):
        """Test unpacking Header with zero chains"""
        header_bytes = Header.pack(
            chains=0,
            unique_elements=0,
            growth=1,
            tightening=1,
        )

        header = Header.unpack(header_bytes)

        assert header.chains == 0
        assert header.unique_elements == 0
        assert len(header.filters) == 0

    def test_header_unpack_single_chain(self):
        """Test unpacking Header with single chain"""
        header_bytes = Header.pack(
            chains=1,
            unique_elements=100,
            growth=2,
            tightening=2,
        )

        chain = Chain.pack(
            bytes_size=512,
            false_positive_rate=0.01,
            number_of_elements=50,
            hash_functions=5.0,
            unique_elements=48,
        )

        full_bytes = header_bytes + chain
        header = Header.unpack(full_bytes)

        assert header.chains == 1
        assert len(header.filters) == 1
        assert header.filters[0].bytes_size == 512

    def test_header_unpack_multiple_chains(self):
        """Test unpacking Header with multiple chains"""
        num_chains = 5
        header_bytes = Header.pack(
            chains=num_chains,
            unique_elements=5000,
            growth=2,
            tightening=2,
        )

        chains_data = b""
        for i in range(num_chains):
            chain = Chain.pack(
                bytes_size=1024 * (i + 1),
                false_positive_rate=0.01 / (i + 1),
                number_of_elements=100 * (i + 1),
                hash_functions=7.0 + i,
                unique_elements=95 * (i + 1),
            )
            chains_data += chain

        full_bytes = header_bytes + chains_data
        header = Header.unpack(full_bytes)

        assert header.chains == num_chains
        assert len(header.filters) == num_chains

        # Verify each chain
        for i, filter_chain in enumerate(header.filters):
            assert filter_chain.bytes_size == 1024 * (i + 1)
            assert filter_chain.number_of_elements == 100 * (i + 1)

    def test_header_unpack_insufficient_data(self):
        """Test that unpack raises ValueError for insufficient header data"""
        insufficient_data = b"x" * (Header.size - 1)

        with pytest.raises(ValueError, match="Data too small"):
            Header.unpack(insufficient_data)

    def test_header_unpack_empty_bytes(self):
        """Test that unpack raises ValueError for empty bytes"""
        with pytest.raises(ValueError, match="Data too small"):
            Header.unpack(b"")

    def test_header_unpack_insufficient_chain_data(self):
        """Test unpack with header but insufficient chain data"""
        header_bytes = Header.pack(
            chains=2,
            unique_elements=500,
            growth=2,
            tightening=2,
        )

        # Only provide one chain instead of two
        chain1 = Chain.pack(
            bytes_size=1024,
            false_positive_rate=0.01,
            number_of_elements=100,
            hash_functions=7.0,
            unique_elements=95,
        )

        # This should raise an error when trying to unpack the second chain
        incomplete_data = header_bytes + chain1

        # The unpack will try to read beyond available data
        # This might raise an IndexError or struct.error
        with pytest.raises((IndexError, struct.error, ValueError)):
            Header.unpack(incomplete_data)

    def test_header_size_class_var(self):
        """Test that Header.size is correctly defined"""
        assert Header.size == 20  # 8+4*3 bytes as per comment
        assert isinstance(Header.size, int)

    def test_header_packing_format(self):
        """Test that packing format matches expected structure"""
        # Verify the format string matches the expected types
        assert Header.packing_format == "<QIII"
        # Q = uint64 (8 bytes), I = uint32 (4 bytes)
        # Total: 8 + 4 + 4 + 4 = 20 bytes

    def test_header_large_values(self):
        """Test Header with large values (within format limits)"""
        # Format is "<QIII" which means:
        # Q (uint64) = chains (max: 2^64 - 1)
        # I (uint32) = unique_elements (max: 2^32 - 1 = 4294967295)
        # I (uint32) = growth (max: 2^32 - 1 = 4294967295)
        # I (uint32) = tightening (max: 2^32 - 1 = 4294967295)

        # Test packing with maximum values for each field
        header_bytes = Header.pack(
            chains=2**63 - 1,  # Max uint64 (though chains is Q in format)
            unique_elements=2**32 - 1,  # Max uint32
            growth=2**32 - 1,  # Max uint32
            tightening=2**32 - 1,  # Max uint32
        )

        # Verify packing works
        assert len(header_bytes) == Header.size

        # Verify unpacking header part only
        header_data_only = header_bytes[: Header.size]
        chains, unique_elements, growth, tightening = struct.unpack(
            Header.packing_format, header_data_only
        )

        assert chains == 2**63 - 1
        assert unique_elements == 2**32 - 1
        assert growth == 2**32 - 1
        assert tightening == 2**32 - 1

        # Test with reasonable large values
        header_bytes2 = Header.pack(
            chains=1000,
            unique_elements=1000000,
            growth=10,
            tightening=5,
        )

        header_data_only2 = header_bytes2[: Header.size]
        chains2, unique_elements2, growth2, tightening2 = struct.unpack(
            Header.packing_format, header_data_only2
        )

        assert chains2 == 1000
        assert unique_elements2 == 1000000
        assert growth2 == 10
        assert tightening2 == 5


class TestChunk:
    """Unit tests for Chunk class - streaming raw bit array data"""

    def test_chunk_initialization(self):
        """Test Chunk initialization"""
        chunk = Chunk(raw_bytes=b"test data", next_chunk=1)

        assert chunk.raw_bytes == b"test data"
        assert chunk.next_chunk == 1

    def test_chunk_initialization_none_values(self):
        """Test Chunk initialization with None values"""
        chunk = Chunk(raw_bytes=None, next_chunk=None)

        assert chunk.raw_bytes is None
        assert chunk.next_chunk is None

    def test_chunk_max_chunk_size_property(self):
        """Test max_chunk_size property getter and setter"""
        chunk = Chunk(raw_bytes=b"test", next_chunk=None)

        # Default value
        assert chunk.max_chunk_size == 128

        # Set new value
        chunk.max_chunk_size = 256
        assert chunk.max_chunk_size == 256

        # Set back
        chunk.max_chunk_size = 128
        assert chunk.max_chunk_size == 128

    def test_chunk_stream_empty_bytes(self):
        """Test streaming with empty bytes"""
        chunk = Chunk(raw_bytes=b"", next_chunk=None)

        chunks = list(chunk.stream())
        assert len(chunks) == 0

    def test_chunk_stream_none_bytes(self):
        """Test streaming with None bytes"""
        chunk = Chunk(raw_bytes=None, next_chunk=None)

        chunks = list(chunk.stream())
        assert len(chunks) == 0

    def test_chunk_stream_single_chunk(self):
        """Test streaming data that fits in a single chunk"""
        data = b"x" * 64  # Less than default chunk size (128)
        chunk = Chunk(raw_bytes=data, next_chunk=None)

        chunks = list(chunk.stream())

        assert len(chunks) == 1
        assert chunks[0].raw_bytes == data
        assert chunks[0].next_chunk is None

    def test_chunk_stream_exact_chunk_size(self):
        """Test streaming data exactly matching chunk size"""
        chunk = Chunk(raw_bytes=b"x" * 128, next_chunk=None)
        chunk.max_chunk_size = 128

        chunks = list(chunk.stream())

        assert len(chunks) == 1
        assert chunks[0].raw_bytes == b"x" * 128
        assert chunks[0].next_chunk is None

    def test_chunk_stream_multiple_chunks(self):
        """Test streaming data that requires multiple chunks"""
        data = b"x" * 300  # More than default chunk size (128)
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 128

        chunks = list(chunk.stream())

        assert len(chunks) == 3  # 300 / 128 = 2.34, so 3 chunks

        # First chunk
        assert len(chunks[0].raw_bytes) == 128
        assert chunks[0].next_chunk == 1

        # Second chunk
        assert len(chunks[1].raw_bytes) == 128
        assert chunks[1].next_chunk == 2

        # Last chunk
        assert len(chunks[2].raw_bytes) == 44  # 300 - 128*2
        assert chunks[2].next_chunk is None

    def test_chunk_stream_custom_chunk_size(self):
        """Test streaming with custom chunk size"""
        data = b"x" * 200
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 50

        chunks = list(chunk.stream())

        assert len(chunks) == 4  # 200 / 50 = 4 chunks

        for i in range(3):
            assert len(chunks[i].raw_bytes) == 50
            assert chunks[i].next_chunk == i + 1

        # Last chunk
        assert len(chunks[3].raw_bytes) == 50
        assert chunks[3].next_chunk is None

    def test_chunk_stream_start_offset_zero(self):
        """Test streaming with start_offset=0 (default)"""
        data = b"x" * 200
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 50

        chunks = list(chunk.stream(start_offset=0))

        assert len(chunks) == 4
        assert chunks[0].raw_bytes == data[0:50]

    def test_chunk_stream_start_offset_non_zero(self):
        """Test streaming with non-zero start_offset"""
        data = b"x" * 200
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 50

        # Start from offset 2 (should skip first 2 chunks = 100 bytes)
        chunks = list(chunk.stream(start_offset=2))

        assert len(chunks) == 2  # Remaining 100 bytes = 2 chunks
        assert chunks[0].raw_bytes == data[100:150]
        assert chunks[1].raw_bytes == data[150:200]

    def test_chunk_stream_start_offset_beyond_data(self):
        """Test streaming with start_offset beyond data length"""
        data = b"x" * 100
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 50

        # Start from offset 3 (150 bytes, but only 100 bytes available)
        chunks = list(chunk.stream(start_offset=3))

        assert len(chunks) == 0

    def test_chunk_stream_large_data(self):
        """Test streaming with large data"""
        data = b"x" * 10000
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 128

        chunks = list(chunk.stream())

        expected_chunks = (10000 + 127) // 128  # Ceiling division
        assert len(chunks) == expected_chunks

        # Verify total bytes
        total_bytes = sum(len(c.raw_bytes) for c in chunks)
        assert total_bytes == 10000

    def test_chunk_stream_iterator_behavior(self):
        """Test that stream() returns an iterator"""
        chunk = Chunk(raw_bytes=b"test", next_chunk=None)

        stream = chunk.stream()
        assert hasattr(stream, "__iter__")
        assert hasattr(stream, "__next__")

        # Should be able to iterate
        chunks = list(stream)
        assert len(chunks) == 1

    def test_chunk_stream_preserves_data(self):
        """Test that streaming doesn't modify original data"""
        original_data = b"x" * 200
        chunk = Chunk(raw_bytes=original_data, next_chunk=None)
        chunk.max_chunk_size = 50

        chunks = list(chunk.stream())

        # Original data should be unchanged
        assert chunk.raw_bytes == original_data

        # Verify we can stream again
        chunks2 = list(chunk.stream())
        assert len(chunks2) == len(chunks)

    def test_chunk_stream_chunk_boundaries(self):
        """Test that chunks align correctly at boundaries"""
        data = b"0123456789" * 20  # 200 bytes
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 50

        chunks = list(chunk.stream())

        # Verify chunk boundaries
        assert chunks[0].raw_bytes == data[0:50]
        assert chunks[1].raw_bytes == data[50:100]
        assert chunks[2].raw_bytes == data[100:150]
        assert chunks[3].raw_bytes == data[150:200]

    def test_chunk_stream_very_small_chunk_size(self):
        """Test streaming with very small chunk size"""
        data = b"x" * 100
        chunk = Chunk(raw_bytes=data, next_chunk=None)
        chunk.max_chunk_size = 1

        chunks = list(chunk.stream())

        assert len(chunks) == 100
        for i, c in enumerate(chunks):
            assert len(c.raw_bytes) == 1
            assert c.raw_bytes == b"x"
            if i < 99:
                assert c.next_chunk == i + 1
            else:
                assert c.next_chunk is None
