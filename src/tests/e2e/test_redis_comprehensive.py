import pytest
import asyncio
import tempfile
import os
from src.pycache import PyCache, Redis
from src.pycache.datatypes import String, List, Map, Numeric, Set, Queue, Streams
from collections import deque


@pytest.mark.asyncio
class TestRedisComprehensive:
    """Comprehensive Redis adapter test suite."""
    
    def setup_method(self):
        """Setup method called before each test."""
        # Use Redis database 15 for testing to avoid conflicts
        self.redis_adapter = Redis("redis://localhost:6379/15", tablename="test-redis")
        self.cache = PyCache(self.redis_adapter)

    def teardown_method(self):
        try:
            # Run async cleanup synchronously
            asyncio.run(self.async_teardown_method())
        except Exception:
            pass
    async def async_teardown_method(self):
        """Clean up after each test."""
        try:
            client = self.redis_adapter._client  # access the redis client directly
            await client.flushdb()  # flush the current selected DB (DB 15 in your setup)
        except:
            pass


    def extract_value(self, datatype_instance):
        """Extract the native value from a datatype instance."""
        if hasattr(datatype_instance, "value"):
            return datatype_instance.value
        return datatype_instance

    # =====================================================
    # Basic CRUD Operations with All Datatypes
    # =====================================================

    async def test_string_operations(self):
        """Test Redis string operations with String datatype."""
        async with self.cache.session() as session:
            # Basic set/get
            await session.set("str_key", String("hello world"))
            result = await session.get("str_key", String(""))
            assert result == "hello world"
            
            # Empty string
            await session.set("empty_str", String(""))
            result = await session.get("empty_str", String(""))
            assert result == ""
            
            # Unicode strings
            await session.set("unicode_key", String("🚀 Redis тест"))
            result = await session.get("unicode_key", String(""))
            assert result == "🚀 Redis тест"
            
            # Large string
            large_string = "x" * 10000
            await session.set("large_str", String(large_string))
            result = await session.get("large_str", String(""))
            assert result == large_string

    async def test_numeric_operations(self):
        """Test Redis numeric operations with Numeric datatype."""
        async with self.cache.session() as session:
            # Integer
            await session.set("int_key", Numeric(42))
            result = await session.get("int_key", Numeric(0))
            assert result == 42
            assert isinstance(result, int)
            
            # Float
            await session.set("float_key", Numeric(3.14159))
            result = await session.get("float_key", Numeric(0.0))
            assert result == 3.14159
            assert isinstance(result, float)
            
            # Zero
            await session.set("zero_key", Numeric(0))
            result = await session.get("zero_key", Numeric(1))
            assert result == 0
            
            # Negative numbers
            await session.set("neg_key", Numeric(-123))
            result = await session.get("neg_key", Numeric(0))
            assert result == -123

    async def test_list_operations(self):
        """Test Redis list operations with List datatype."""
        async with self.cache.session() as session:
            # Basic list
            test_list = [1, 2, 3, "hello"]
            print(await session.get("list_key_test_list", List([])))
            await session.set("list_key_test_list", List(test_list))
            result = await session.get("list_key_test_list", List([]))
            assert result == ["1", "2", "3", "hello"]  # Redis stores as strings
            
            # Empty list
            await session.set("empty_list", List([]))
            result = await session.get("empty_list", List([]))
            assert result == []
            
            # Mixed types list
            mixed_list = [1, "string", True, None]
            await session.set("mixed_list", List(mixed_list))
            result = await session.get("mixed_list", List([]))
            assert result == ["1", "string", "True", "None"]

    async def test_set_operations(self):
        """Test Redis set operations with Set datatype."""
        async with self.cache.session() as session:
            # Basic set
            test_set = {1, 2, 3, "hello"}
            await session.set("set_key", Set(test_set))
            result = await session.get("set_key", Set(set()))
            expected = {"1", "2", "3", "hello"}  # Redis stores as strings
            assert result == expected
            
            # Empty set
            await session.set("empty_set", Set(set()))
            result = await session.get("empty_set", Set(set()))
            assert result == set()
            
            # Set deduplication
            await session.set("dup_set", Set([1, 1, 2, 2, 3]))
            result = await session.get("dup_set", Set(set()))
            assert result == {"1", "2", "3"}

    async def test_map_operations(self):
        """Test Redis hash operations with Map datatype."""
        async with self.cache.session() as session:
            # Basic map
            test_map = {"name": "John", "age": 30, "city": "NYC"}
            await session.set("map_key", Map(test_map))
            result = await session.get("map_key", Map({}))
            expected = {"name": "John", "age": "30", "city": "NYC"}  # Redis stores as strings
            assert result == expected
            
            # Empty map
            await session.set("empty_map", Map({}))
            result = await session.get("empty_map", Map({}))
            assert result == {}
            
            # Complex map
            complex_map = {"user": "Alice", "settings": {"theme": "dark"}}
            await session.set("complex_map", Map(complex_map))
            result = await session.get("complex_map", Map({}))
            assert result["user"] == "Alice"

    async def test_queue_operations(self):
        """Test Redis queue operations with Queue datatype."""
        async with self.cache.session() as session:
            # Basic queue
            test_queue = [1, 2, 3, "hello"]
            await session.set("queue_key", Queue(test_queue))
            result = await session.get("queue_key", Queue([]))
            assert isinstance(result, deque)
            assert list(result) == ["1", "2", "3", "hello"]
            
            # Empty queue
            await session.set("empty_queue", Queue([]))
            result = await session.get("empty_queue", Queue([]))
            assert isinstance(result, deque)
            assert list(result) == []

    async def test_streams_operations(self):
        """Test Redis streams operations with Streams datatype."""
        async with self.cache.session() as session:
            # Basic stream
            test_stream = [("field1", "value1"), ("field2", "value2")]
            await session.set("stream_key", Streams(test_stream))
            result = await session.get("stream_key", Streams([]))
            assert isinstance(result, list)
            assert len(result) == 1

            test_stream = [("field1", "value1"), ("field2", "value2")]
            await session.set("stream_key", Streams(test_stream))
            result = await session.get("stream_key", Streams([]))
            assert isinstance(result, list)
            assert len(result) == 2
            
            # Stream with dict entries
            dict_stream = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
            await session.set("dict_stream", Streams(dict_stream))
            result = await session.get("dict_stream", Streams([]))
            assert isinstance(result, list)

    # =====================================================
    # Batch Operations
    # =====================================================

    async def test_batch_operations_uniform_datatype(self):
        """Test batch operations with uniform datatypes."""
        async with self.cache.session() as session:
            # Batch set strings
            string_data = {
                "str1": String("value1"),
                "str2": String("value2"),
                "str3": String("value3")
            }
            await session.batch_set(string_data)
            
            # Batch get strings with datatype specified
            results = await session.batch_get(["str1", "str2", "str3"], datatype=String)
            assert results["str1"] == "value1"
            assert results["str2"] == "value2"
            assert results["str3"] == "value3"

    async def test_batch_operations_mixed_datatypes(self):
        """Test batch operations with mixed datatypes using new Redis functionality."""
        async with self.cache.session() as session:
            # Set up mixed data
            mixed_data = {
                "str_key": String("hello"),
                "num_key": Numeric(42),
                "list_key": List([1, 2, 3]),
                "set_key": Set({1, 2, 3}),
                "map_key": Map({"a": 1, "b": 2})
            }
            await session.batch_set(mixed_data)
            
            # Batch get with mixed datatypes using Redis adapter's new functionality
            datatype_map = {
                "str_key": String,
                "num_key": Numeric, 
                "list_key": List,
                "set_key": Set,
                "map_key": Map
            }
            
            # Use the Redis adapter's batch_get with mixed datatypes
            results = await self.redis_adapter.batch_get(datatype_map)
            
            assert results["str_key"] == "hello"
            assert results["num_key"] == 42
            assert results["list_key"] == ["1", "2", "3"]
            assert results["set_key"] == {"1", "2", "3"}
            assert results["map_key"] == {"a": "1", "b": "2"}

    async def test_batch_operations_performance(self):
        """Test batch operations with large datasets."""
        async with self.cache.session() as session:
            # Create large dataset
            large_data = {}
            for i in range(100):
                large_data[f"key_{i}"] = String(f"value_{i}")
            
            # Batch set
            await session.batch_set(large_data)
            
            # Batch get
            keys = [f"key_{i}" for i in range(100)]
            results = await session.batch_get(keys)
            
            # Verify all data
            for i in range(100):
                assert results[f"key_{i}"] == f"value_{i}"

    # =====================================================
    # Transaction Operations (Redis MULTI/EXEC)
    # =====================================================

    async def test_transaction_basic(self):
        """Test basic Redis transaction functionality."""
        if not self.cache.get_support_transactions():
            pytest.skip("Redis adapter supports transactions")
            
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))
            await session.set("key2", String("initial2"))
            
            # Transaction to update values
            async with session.with_transaction() as tx:
                await tx.set("key1", String("updated1"))
                await tx.set("key2", String("updated2"))
                await tx.set("key3", String("new3"))
            
            # Verify all changes committed
            assert (await session.get("key1", String(""))) == "updated1"
            assert (await session.get("key2", String(""))) == "updated2"
            assert (await session.get("key3", String(""))) == "new3"

    async def test_transaction_rollback(self):
        """Test Redis transaction rollback."""
        if not self.cache.get_support_transactions():
            pytest.skip("Redis adapter supports transactions")
            
        async with self.cache.session() as session:
            await session.set("key1", String("initial"))
            
            try:
                async with session.with_transaction() as tx:
                    await tx.set("key1", String("updated"))
                    await tx.set("key2", String("new"))
                    raise Exception("Force rollback")
            except Exception:
                pass
            
            # Verify rollback
            assert (await session.get("key1", String(""))) == "initial"
            assert await session.get("key2", String("")) is None

    async def test_transaction_with_all_datatypes(self):
        """Test transactions with all Redis datatypes."""
        if not self.cache.get_support_transactions():
            pytest.skip("Redis adapter supports transactions")
            
        async with self.cache.session() as session:
            async with session.with_transaction() as tx:
                await tx.set("str_key", String("hello"))
                await tx.set("num_key", Numeric(42))
                await tx.set("list_key", List([1, 2, 3]))
                await tx.set("set_key", Set({1, 2, 3}))
                await tx.set("map_key", Map({"a": 1}))
                await tx.set("queue_key", Queue([1, 2, 3]))
                await tx.set("stream_key", Streams([("field", "value")]))
            
            # Verify all committed
            assert (await session.get("str_key", String(""))) == "hello"
            assert (await session.get("num_key", Numeric(0))) == 42
            assert (await session.get("list_key", List([]))) == ["1", "2", "3"]
            assert (await session.get("set_key", Set(set()))) == {"1", "2", "3"}
            assert (await session.get("map_key", Map({}))) == {"a": "1"}

    # =====================================================
    # TTL and Expiration (Redis Native)
    # =====================================================

    async def test_redis_native_expiration(self):
        """Test Redis native expiration functionality."""
        async with self.cache.session() as session:
            await session.set("expire_key", String("expire_value"))
            await session.set_expire("expire_key", 1)  # 1 second
            
            # Key should exist
            assert await session.exists("expire_key")
            
            # Wait for expiration
            await asyncio.sleep(1.5)
            
            # Key should be expired (Redis handles this automatically)
            assert not await session.exists("expire_key")

    async def test_redis_ttl_operations(self):
        """Test Redis TTL operations."""
        async with self.cache.session() as session:
            await session.set("ttl_key", String("ttl_value"))
            await session.set_expire("ttl_key", 10)  # 10 seconds
            
            # Check TTL
            ttl = await session.get_expire("ttl_key")
            assert ttl is not None
            assert ttl <= 10
            assert ttl > 0

    async def test_redis_expire_multiple_keys(self):
        """Test expiration on multiple keys."""
        async with self.cache.session() as session:
            # Set multiple keys with different TTLs
            await session.set("key1", String("value1"))
            await session.set("key2", String("value2"))
            await session.set("key3", String("value3"))
            
            await session.set_expire("key1", 1)  # 1 second
            await session.set_expire("key2", 2)  # 2 seconds
            await session.set_expire("key3", 10)  # 10 seconds
            
            # Wait 1.5 seconds
            await asyncio.sleep(1.5)
            
            # key1 should be expired, others should exist
            assert not await session.exists("key1")
            assert await session.exists("key2")
            assert await session.exists("key3")

    # =====================================================
    # Error Handling and Edge Cases
    # =====================================================

    async def test_nonexistent_key_operations(self):
        """Test operations on nonexistent keys."""
        async with self.cache.session() as session:
            # Get nonexistent key
            result = await session.get("nonexistent", String(""))
            assert result is None
            
            # Delete nonexistent key
            await session.delete("nonexistent")  # Should not raise error
            
            # Check existence of nonexistent key
            assert not await session.exists("nonexistent")

    @pytest.mark.xfail(reason="Overwriting keys with different Redis data types causes WRONGTYPE error")
    async def test_overwrite_operations(self):
        """Test overwriting existing keys with different datatypes."""
        async with self.cache.session() as session:
            # Set as string
            await session.set("multi_type", String("hello"))
            assert (await session.get("multi_type", String(""))) == "hello"
            
            # Overwrite as numeric
            await session.set("multi_type", Numeric(42))
            assert (await session.get("multi_type", Numeric(0))) == 42
            
            # Overwrite as list
            await session.set("multi_type", List([1, 2, 3]))
            assert (await session.get("multi_type", List([]))) == ["1", "2", "3"]

    async def test_special_characters_in_keys(self):
        """Test keys with special characters."""
        async with self.cache.session() as session:
            special_keys = [
                "key with spaces",
                "key!@#$%^&*()",
                "key_with_underscores",
                "key-with-dashes",
                "key.with.dots",
                "ключ_с_кириллицей",
                "キー_with_japanese",
                "🔑_emoji_key"
            ]
            
            for key in special_keys:
                await session.set(key, String(f"value_for_{key}"))
                result = await session.get(key, String(""))
                assert result == f"value_for_{key}"

    async def test_large_data_operations(self):
        """Test operations with large data."""
        async with self.cache.session() as session:
            # Large string (1MB)
            large_string = "x" * (1024 * 1024)
            await session.set("large_string", String(large_string))
            result = await session.get("large_string", String(""))
            assert result == large_string
            
            # Large list
            large_list = list(range(10000))
            await session.set("large_list", List(large_list))
            result = await session.get("large_list", List([]))
            assert len(result) == 10000

    # =====================================================
    # Redis-Specific Features
    # =====================================================

    async def test_redis_pipeline_efficiency(self):
        """Test that Redis pipelines are being used efficiently."""
        async with self.cache.session() as session:
            # This test verifies that batch operations complete quickly
            # indicating pipeline usage
            import time
            
            start_time = time.time()
            
            # Batch set 100 keys
            batch_data = {f"pipeline_key_{i}": String(f"value_{i}") for i in range(100)}
            await session.batch_set(batch_data)
            
            # Batch get 100 keys
            keys = [f"pipeline_key_{i}" for i in range(100)]
            results = await session.batch_get(keys)
            
            end_time = time.time()
            
            # Should complete quickly (pipeline efficiency)
            assert end_time - start_time < 1.0  # Less than 1 second
            assert len(results) == 100

    async def test_redis_connection_handling(self):
        """Test Redis connection handling."""
        async with self.cache.session() as session:
            # Test that connections are properly managed
            await session.set("connection_test", String("test_value"))
            
            # Multiple operations should reuse connection
            for i in range(10):
                await session.set(f"conn_key_{i}", String(f"value_{i}"))
                result = await session.get(f"conn_key_{i}", String(""))
                assert result == f"value_{i}"

    async def test_redis_datatype_conversions(self):
        """Test Redis-specific datatype conversions."""
        async with self.cache.session() as session:
            # Test that data is properly converted to Redis formats
            
            # Complex nested data
            complex_data = {
                "nested": {"deep": {"value": 42}},
                "array": [1, 2, {"inner": "value"}],
                "boolean": True,
                "null": None
            }
            await session.set("complex", Map(complex_data))
            result = await session.get("complex", Map({}))
            
            # Should be converted to strings in Redis
            assert isinstance(result, dict)
            assert "nested" in result

    # =====================================================
    # Concurrent Operations Simulation
    # =====================================================

    async def test_concurrent_operations_simulation(self):
        """Simulate concurrent operations to test Redis adapter robustness."""
        async with self.cache.session() as session:
            # Simulate interleaved operations
            await session.set("concurrent1", String("value1"))
            await session.set("concurrent2", Numeric(42))
            await session.set("concurrent3", List([1, 2, 3]))
            
            # Rapid updates
            for i in range(10):
                await session.set("rapid_update", String(f"value_{i}"))
                result = await session.get("rapid_update", String(""))
                assert result == f"value_{i}"

    # =====================================================
    # Performance and Load Tests
    # =====================================================

    async def test_high_volume_operations(self):
        """Test high volume operations."""
        async with self.cache.session() as session:
            # Set many keys quickly
            tasks = []
            for i in range(50):  # Reduced for CI environments
                task = session.set(f"volume_key_{i}", String(f"volume_value_{i}"))
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            
            # Verify all keys exist
            keys = await session.keys()
            volume_keys = [k for k in keys if k.startswith("volume_key_")]
            assert len(volume_keys) == 50

    # =====================================================
    # Redis-Specific Error Conditions
    # =====================================================

    async def test_redis_error_handling(self):
        """Test Redis-specific error handling."""
        async with self.cache.session() as session:
            # Test operations that should handle gracefully
            
            # Very long key name
            long_key = "x" * 1000
            await session.set(long_key, String("long_key_value"))
            result = await session.get(long_key, String(""))
            assert result == "long_key_value"
            
            # Empty key (should work in Redis)
            await session.set("", String("empty_key_value"))
            result = await session.get("", String(""))
            assert result == "empty_key_value"

    # =====================================================
    # TTL Worker Integration
    # =====================================================

    async def test_ttl_worker_integration(self):
        """Test TTL worker integration with Redis."""
        async with self.cache.session() as session:
            # Set keys with short expiration
            await session.set("ttl_worker_key", String("ttl_worker_value"))
            await session.set_expire("ttl_worker_key", 1)
            
            # Start TTL worker
            await self.cache.start_ttl_deletion(delete_interval=0.1)
            
            # Wait for expiration + cleanup
            await asyncio.sleep(1.5)
            
            # Key should be cleaned up
            assert not await session.exists("ttl_worker_key")
            
            # Stop TTL worker
            await self.cache.stop_ttl_deletion()

    # =====================================================
    # Mixed Workload Scenarios
    # =====================================================

    async def test_mixed_workload_scenario(self):
        """Test a realistic mixed workload scenario."""
        async with self.cache.session() as session:
            # Simulate a realistic application workload
            
            # 1. Initial data setup
            await session.batch_set({
                "user:1": Map({"name": "Alice", "age": "30"}),
                "user:2": Map({"name": "Bob", "age": "25"}),
                "session:abc123": String("user:1"),
                "cache:popular_items": List(["item1", "item2", "item3"]),
                "counter:page_views": Numeric(1000)
            })
            
            # 2. Read operations
            user_data = await session.get("user:1", Map({}))
            session_data = await session.get("session:abc123", String(""))
            assert user_data["name"] == "Alice"
            assert session_data == "user:1"
            
            # 3. Update operations
            await session.set("counter:page_views", Numeric(1001))
            await session.set_expire("session:abc123", 3600)  # 1 hour session
            
            # 4. Batch operations
            batch_keys = {"user:1":Map, "user:2":Map, "counter:page_views":Numeric}
            results = await session.batch_get(batch_keys)
            assert len(results) == 3
            
            # 5. Cleanup some data
            await session.delete("session:abc123")
            assert not await session.exists("session:abc123")

    # =====================================================
    # Redis Adapter Direct Testing
    # =====================================================

    async def test_redis_adapter_direct_access(self):
        """Test Redis adapter methods directly."""
        # Test direct adapter methods
        await self.redis_adapter.set("direct_key", String("direct_value"))
        result = await self.redis_adapter.get("direct_key", String(""))
        assert result == "direct_value"
        
        # Test adapter-specific features
        assert self.redis_adapter.get_support_transactions() == True
        assert self.redis_adapter.get_support_for_streams() == True
        assert self.redis_adapter.get_support_datatype_serializer() == False

    async def test_redis_adapter_batch_methods(self):
        """Test Redis adapter batch methods directly."""
        # Test uniform datatype batch get
        await self.redis_adapter.set("batch1", String("value1"))
        await self.redis_adapter.set("batch2", String("value2"))
        
        results = await self.redis_adapter.batch_get(["batch1", "batch2"], String)
        assert results["batch1"] == "value1"
        assert results["batch2"] == "value2"
        
        # Test mixed datatype batch get
        await self.redis_adapter.set("mix_str", String("hello"))
        await self.redis_adapter.set("mix_num", Numeric(42))
        
        mixed_results = await self.redis_adapter.batch_get({
            "mix_str": String,
            "mix_num": Numeric
        })
        assert mixed_results["mix_str"] == "hello"
        assert mixed_results["mix_num"] == 42 