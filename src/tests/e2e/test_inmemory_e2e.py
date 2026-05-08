import pytest
from src.pycache.py_cache import PyCache
from src.pycache.adapters.InMemory import InMemory
from src.tests.e2e.base_cache_tests import BaseCacheTests
from src.pycache.datatypes import BloomFilter, BitArray
from src.pycache.collections.bloomfilters import BloomFilter as BloomFilterImpl
from src.pycache.collections.bitarray.BitArray import BitArray as BitArrayImpl


class TestInMemoryCache(BaseCacheTests):
    def create_cache(self):
        """Create InMemory cache with memory URI."""
        return PyCache(InMemory("memory://"))

    # BloomFilter Datatype Tests

    async def test_bloomfilter_set_and_get(self):
        """Test setting and getting a BloomFilter."""
        async with self.cache.session() as cache:
            bf = BloomFilterImpl(1000, 0.01)  # 1000 elements, 1% false positive rate
            bf.add("test_key")
            bf.add("another_key")

            await cache.set("bloom_key", BloomFilter(bf))
            result = await cache.get("bloom_key")

            # Verify it's a BloomFilter instance
            assert isinstance(result, BloomFilterImpl)
            assert result.exists("test_key")
            assert result.exists("another_key")
            assert not result.exists("non_existent_key")

    async def test_bloomfilter_operations(self):
        """Test BloomFilter operations after retrieval."""
        async with self.cache.session() as cache:
            bf = BloomFilterImpl(500, 0.05)  # 500 elements, 5% false positive rate
            bf.add("item1")
            bf.add("item2")

            await cache.set("bloom_ops", BloomFilter(bf))
            retrieved_bf = await cache.get("bloom_ops")

            # Test that we can continue using the BloomFilter
            assert retrieved_bf.exists("item1")
            assert retrieved_bf.exists("item2")

            # Add more items after retrieval
            retrieved_bf.add("item3")
            assert retrieved_bf.exists("item3")
            assert len(retrieved_bf) == 3

    async def test_bloomfilter_overwrite(self):
        """Test overwriting a BloomFilter."""
        async with self.cache.session() as cache:
            bf1 = BloomFilterImpl(100, 0.01)
            bf1.add("first")

            await cache.set("bloom_overwrite", BloomFilter(bf1))
            result1 = await cache.get("bloom_overwrite")
            assert result1.exists("first")

            # Overwrite with new BloomFilter
            bf2 = BloomFilterImpl(200, 0.02)
            bf2.add("second")
            await cache.set("bloom_overwrite", BloomFilter(bf2))

            result2 = await cache.get("bloom_overwrite")
            assert not result2.exists("first")
            assert result2.exists("second")

    async def test_bloomfilter_delete(self):
        """Test deleting a BloomFilter."""
        async with self.cache.session() as cache:
            bf = BloomFilterImpl(100, 0.01)
            bf.add("test")

            await cache.set("bloom_delete", BloomFilter(bf))
            assert await cache.exists("bloom_delete")

            await cache.delete("bloom_delete")
            assert not await cache.exists("bloom_delete")
            result = await cache.get("bloom_delete")
            assert result is None

    async def test_bloomfilter_batch_operations(self):
        """Test batch operations with BloomFilter."""
        async with self.cache.session() as cache:
            bf1 = BloomFilterImpl(100, 0.01)
            bf1.add("batch1")

            bf2 = BloomFilterImpl(200, 0.02)
            bf2.add("batch2")

            await cache.batch_set(
                {"bloom_batch1": BloomFilter(bf1), "bloom_batch2": BloomFilter(bf2)}
            )

            results = await cache.batch_get(["bloom_batch1", "bloom_batch2"])

            assert isinstance(results["bloom_batch1"], BloomFilterImpl)
            assert isinstance(results["bloom_batch2"], BloomFilterImpl)
            assert results["bloom_batch1"].exists("batch1")
            assert results["bloom_batch2"].exists("batch2")

    async def test_bloomfilter_different_configurations(self):
        """Test BloomFilters with different configurations."""
        async with self.cache.session() as cache:
            # Small BloomFilter
            bf_small = BloomFilterImpl(10, 0.1)
            bf_small.add("small")
            await cache.set("bloom_small", BloomFilter(bf_small))

            # Large BloomFilter
            bf_large = BloomFilterImpl(10000, 0.001)
            bf_large.add("large")
            await cache.set("bloom_large", BloomFilter(bf_large))

            small_result = await cache.get("bloom_small")
            large_result = await cache.get("bloom_large")

            assert small_result.exists("small")
            assert large_result.exists("large")
            assert small_result.size < large_result.size

    # BitArray Datatype Tests

    async def test_bitarray_set_and_get(self):
        """Test setting and getting a BitArray."""
        async with self.cache.session() as cache:
            ba = BitArrayImpl(64)  # 64 bits
            ba[0] = 1
            ba[10] = 1
            ba[63] = 1

            await cache.set("bitarray_key", BitArray(ba))
            result = await cache.get("bitarray_key")

            # Verify it's a BitArray instance
            assert isinstance(result, BitArrayImpl)
            assert result[0] == 1
            assert result[10] == 1
            assert result[63] == 1
            assert result[1] == 0
            assert result.size == 64

    async def test_bitarray_operations(self):
        """Test BitArray operations after retrieval."""
        async with self.cache.session() as cache:
            ba = BitArrayImpl(128)
            ba[0] = 1
            ba[50] = 1

            await cache.set("bitarray_ops", BitArray(ba))
            retrieved_ba = await cache.get("bitarray_ops")

            # Test that we can continue using the BitArray
            assert retrieved_ba[0] == 1
            assert retrieved_ba[50] == 1
            assert retrieved_ba[1] == 0

            # Modify after retrieval
            retrieved_ba[100] = 1
            assert retrieved_ba[100] == 1

            # Toggle a bit
            retrieved_ba.toggle_bit(50)
            assert retrieved_ba[50] == 0

    async def test_bitarray_overwrite(self):
        """Test overwriting a BitArray."""
        async with self.cache.session() as cache:
            ba1 = BitArrayImpl(32)
            ba1[0] = 1
            ba1[15] = 1

            await cache.set("bitarray_overwrite", BitArray(ba1))
            result1 = await cache.get("bitarray_overwrite")
            assert result1[0] == 1
            assert result1[15] == 1

            # Overwrite with new BitArray
            ba2 = BitArrayImpl(64)
            ba2[32] = 1
            await cache.set("bitarray_overwrite", BitArray(ba2))

            result2 = await cache.get("bitarray_overwrite")
            assert result2[32] == 1
            assert result2.size == 64

    async def test_bitarray_delete(self):
        """Test deleting a BitArray."""
        async with self.cache.session() as cache:
            ba = BitArrayImpl(32)
            ba[10] = 1

            await cache.set("bitarray_delete", BitArray(ba))
            assert await cache.exists("bitarray_delete")

            await cache.delete("bitarray_delete")
            assert not await cache.exists("bitarray_delete")
            result = await cache.get("bitarray_delete")
            assert result is None

    async def test_bitarray_batch_operations(self):
        """Test batch operations with BitArray."""
        async with self.cache.session() as cache:
            ba1 = BitArrayImpl(32)
            ba1[0] = 1

            ba2 = BitArrayImpl(64)
            ba2[32] = 1

            await cache.batch_set(
                {"bitarray_batch1": BitArray(ba1), "bitarray_batch2": BitArray(ba2)}
            )

            results = await cache.batch_get(["bitarray_batch1", "bitarray_batch2"])

            assert isinstance(results["bitarray_batch1"], BitArrayImpl)
            assert isinstance(results["bitarray_batch2"], BitArrayImpl)
            assert results["bitarray_batch1"][0] == 1
            assert results["bitarray_batch2"][32] == 1
            assert results["bitarray_batch1"].size == 32
            assert results["bitarray_batch2"].size == 64

    async def test_bitarray_different_sizes(self):
        """Test BitArrays with different sizes."""
        async with self.cache.session() as cache:
            # Small BitArray
            ba_small = BitArrayImpl(8)
            ba_small[0] = 1
            await cache.set("bitarray_small", BitArray(ba_small))

            # Large BitArray
            ba_large = BitArrayImpl(1024)
            ba_large[1000] = 1
            await cache.set("bitarray_large", BitArray(ba_large))

            small_result = await cache.get("bitarray_small")
            large_result = await cache.get("bitarray_large")

            assert small_result.size == 8
            assert large_result.size == 1024
            assert small_result[0] == 1
            assert large_result[1000] == 1

    async def test_bitarray_toggle_and_delete_operations(self):
        """Test BitArray toggle and delete operations."""
        async with self.cache.session() as cache:
            ba = BitArrayImpl(64)
            ba[10] = 1
            ba[20] = 1

            await cache.set("bitarray_toggle", BitArray(ba))
            retrieved_ba = await cache.get("bitarray_toggle")

            # Toggle bits
            retrieved_ba.toggle_bit(10)
            assert retrieved_ba[10] == 0

            retrieved_ba.toggle_bit(10)
            assert retrieved_ba[10] == 1

            # Delete bits
            del retrieved_ba[20]
            assert retrieved_ba[20] == 0

            # Verify other bits are still set
            assert retrieved_ba[10] == 1

    async def test_bloomfilter_and_bitarray_mixed(self):
        """Test using both BloomFilter and BitArray in the same session."""
        async with self.cache.session() as cache:
            bf = BloomFilterImpl(100, 0.01)
            bf.add("bloom_item")

            ba = BitArrayImpl(64)
            ba[0] = 1

            await cache.set("bloom_mixed", BloomFilter(bf))
            await cache.set("bitarray_mixed", BitArray(ba))

            bloom_result = await cache.get("bloom_mixed")
            bitarray_result = await cache.get("bitarray_mixed")

            assert isinstance(bloom_result, BloomFilterImpl)
            assert isinstance(bitarray_result, BitArrayImpl)
            assert bloom_result.exists("bloom_item")
            assert bitarray_result[0] == 1
