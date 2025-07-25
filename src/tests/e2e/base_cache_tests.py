import pytest
import tempfile
import os
import asyncio
from abc import ABC, abstractmethod
from src.pycache.py_cache import PyCache
from src.pycache.datatypes import String, List, Map, Numeric, Set, Queue
from collections import deque
from datetime import datetime


@pytest.mark.asyncio
class BaseCacheTests(ABC):
    def setup_method(self):
        """Setup method called before each test."""
        self.cache: PyCache = self.create_cache()

    def teardown_method(self):
        """Teardown method called after each test."""
        # Note: We can't use async teardown here, so we'll handle cleanup in the adapter
        pass

    @abstractmethod
    def create_cache(self):
        raise NotImplementedError

    def extract_value(self, datatype_instance):
        """Extract the native value from a datatype instance."""
        if hasattr(datatype_instance, "value"):
            return datatype_instance.value
        return datatype_instance

    # Basic CRUD Tests with String datatype

    async def test_set_and_get_string(self):
        async with self.cache.session() as cache:
            await cache.set("foo", String("bar"))
            result = await cache.get("foo")
            assert self.extract_value(result) == "bar"

    async def test_delete_string(self):
        async with self.cache.session() as cache:
            await cache.set("foo", String("bar"))
            await cache.delete("foo")
            assert not await cache.exists("foo")

    async def test_exists_string(self):
        async with self.cache.session() as cache:
            await cache.set("foo", String("bar"))
            assert await cache.exists("foo")
            await cache.delete("foo")
            assert not await cache.exists("foo")

    async def test_keys_string(self):
        async with self.cache.session() as cache:
            await cache.set("foo", String("bar"))
            await cache.set("baz", String("qux"))
            keys = await cache.keys()
            assert "foo" in keys and "baz" in keys

    async def test_overwrite_value_string(self):
        async with self.cache.session() as cache:
            await cache.set("foo", String("bar"))
            await cache.set("foo", String("baz"))
            result = await cache.get("foo")

            assert result == "baz", f"{result} "

    async def test_delete_nonexistent_key(self):
        async with self.cache.session() as cache:
            await cache.delete("doesnotexist")
            assert not await cache.exists("doesnotexist")

    async def test_get_nonexistent_key(self):
        async with self.cache.session() as cache:
            assert await cache.get("nope") is None

    # Batch Operations with String datatype

    async def test_batch_set_and_get_string(self):
        async with self.cache.session() as cache:
            data = {"a": String("1"), "b": String("2")}
            await cache.batch_set(data)
            results = await cache.batch_get(["a", "b", "c"])
            assert self.extract_value(results["a"]) == "1"
            assert self.extract_value(results["b"]) == "2"
            assert results.get("c") is None

    async def test_keys_after_deletion_string(self):
        async with self.cache.session() as cache:
            await cache.set("x", String("y"))
            await cache.set("z", String("w"))
            await cache.delete("x")
            keys = await cache.keys()
            assert "x" not in keys and "z" in keys

    # List Datatype Tests

    async def test_list_datatype(self):
        async with self.cache.session() as cache:
            test_list = [1, 2, 3, "hello"]
            await cache.set("list_key", List(test_list))
            result = await cache.get("list_key")
            assert self.extract_value(result) == [1, 2, 3, "hello"]

    async def test_list_operations(self):
        async with self.cache.session() as cache:
            # Test empty list
            await cache.set("empty_list", List([]))
            assert self.extract_value(await cache.get("empty_list")) == []

            # Test list with mixed types
            mixed_list = [1, "string", True, None, {"key": "value"}]
            await cache.set("mixed_list", List(mixed_list))
            assert self.extract_value(await cache.get("mixed_list")) == mixed_list

    async def test_list_overwrite(self):
        async with self.cache.session() as cache:
            await cache.set("list_key", List([1, 2, 3]))
            await cache.set("list_key", List([4, 5, 6]))
            result = await cache.get("list_key")
            assert self.extract_value(result) == [4, 5, 6]

    # Map Datatype Tests

    async def test_map_datatype(self):
        async with self.cache.session() as cache:
            test_dict = {"name": "John", "age": 30, "city": "New York"}
            await cache.set("map_key", Map(test_dict))
            result = await cache.get("map_key")
            assert self.extract_value(result) == test_dict

    async def test_map_operations(self):
        async with self.cache.session() as cache:
            # Test empty dict
            await cache.set("empty_map", Map({}))
            assert self.extract_value(await cache.get("empty_map")) == {}

            # Test nested dict
            nested_dict = {
                "user": {
                    "name": "Alice",
                    "preferences": {"theme": "dark", "language": "en"},
                }
            }
            await cache.set("nested_map", Map(nested_dict))
            assert self.extract_value(await cache.get("nested_map")) == nested_dict

    async def test_map_overwrite(self):
        async with self.cache.session() as cache:
            await cache.set("map_key", Map({"old": "value"}))
            await cache.set("map_key", Map({"new": "value"}))
            result = await cache.get("map_key")
            assert self.extract_value(result) == {"new": "value"}

    # Numeric Datatype Tests

    async def test_numeric_integer(self):
        async with self.cache.session() as cache:
            await cache.set("int_key", Numeric(42))
            result = await cache.get("int_key")
            assert self.extract_value(result) == 42

    async def test_numeric_float(self):
        async with self.cache.session() as cache:
            await cache.set("float_key", Numeric(3.14159))
            result = await cache.get("float_key")
            assert self.extract_value(result) == 3.14159

    async def test_numeric_operations(self):
        async with self.cache.session() as cache:
            # Test zero
            await cache.set("zero", Numeric(0))
            assert self.extract_value(await cache.get("zero")) == 0

            # Test negative numbers
            await cache.set("negative", Numeric(-100))
            assert self.extract_value(await cache.get("negative")) == -100

            # Test large numbers
            await cache.set("large", Numeric(999999999))
            assert self.extract_value(await cache.get("large")) == 999999999

    async def test_numeric_overwrite(self):
        async with self.cache.session() as cache:
            await cache.set("num_key", Numeric(10))
            await cache.set("num_key", Numeric(20))
            result = await cache.get("num_key")
            assert self.extract_value(result) == 20

    # Set Datatype Tests

    async def test_set_datatype(self):
        async with self.cache.session() as cache:
            test_set = {1, 2, 3, "hello"}
            await cache.set("set_key", Set(test_set))
            result = await cache.get("set_key")
            assert self.extract_value(result) == test_set

    async def test_set_operations(self):
        async with self.cache.session() as cache:
            # Test empty set
            await cache.set("empty_set", Set(set()))
            assert self.extract_value(await cache.get("empty_set")) == set()

            # Test set with mixed types
            mixed_set = {1, "string", True, None}
            await cache.set("mixed_set", Set(mixed_set))
            assert self.extract_value(await cache.get("mixed_set")) == mixed_set

            # Test set from list (duplicates should be removed)
            await cache.set("set_from_list", Set([1, 1, 2, 2, 3]))
            result = await cache.get("set_from_list")
            assert self.extract_value(result) == {1, 2, 3}

    async def test_set_overwrite(self):
        async with self.cache.session() as cache:
            await cache.set("set_key", Set({1, 2, 3}))
            await cache.set("set_key", Set({4, 5, 6}))
            result = await cache.get("set_key")
            assert self.extract_value(result) == {4, 5, 6}

    # Queue Datatype Tests

    async def test_queue_datatype(self):
        async with self.cache.session() as cache:
            test_queue = [1, 2, 3, "hello"]
            await cache.set("queue_key", Queue(test_queue))
            result = await cache.get("queue_key")
            assert list(self.extract_value(result)) == [1, 2, 3, "hello"]

    async def test_queue_operations(self):
        async with self.cache.session() as cache:
            # Test empty queue
            await cache.set("empty_queue", Queue([]))
            result = await cache.get("empty_queue")
            assert list(self.extract_value(result)) == []

            # Test queue with mixed types
            mixed_queue = [1, "string", True, None, {"key": "value"}]
            await cache.set("mixed_queue", Queue(mixed_queue))
            result = await cache.get("mixed_queue")
            assert list(self.extract_value(result)) == mixed_queue

            # Test queue from deque
            from collections import deque

            original_deque = deque([1, 2, 3])
            await cache.set("deque_queue", Queue(original_deque))
            result = await cache.get("deque_queue")
            assert list(self.extract_value(result)) == [1, 2, 3]

    async def test_queue_overwrite(self):
        async with self.cache.session() as cache:
            await cache.set("queue_key", Queue([1, 2, 3]))
            await cache.set("queue_key", Queue([4, 5, 6]))
            result = await cache.get("queue_key")
            assert list(self.extract_value(result)) == [4, 5, 6]

    # TTL and Expiration Tests

    async def test_set_expire_basic(self):
        """Test basic expiration functionality."""
        async with self.cache.session() as cache:
            # Set a key with a short TTL
            await cache.set("expire_key", String("expire_value"))
            await cache.set_expire("expire_key", 1)  # 1 second TTL

            # Key should exist immediately
            assert await cache.exists("expire_key")
            assert await cache.get("expire_key") == "expire_value"

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Key should be expired
            assert not await cache.exists("expire_key")
            assert await cache.get("expire_key") is None

    async def test_set_expire_multiple_keys(self):
        """Test expiration on multiple keys with different TTLs."""
        async with self.cache.session() as cache:
            # Set multiple keys with different TTLs
            await cache.set("key1", String("value1"))
            await cache.set("key2", String("value2"))
            await cache.set("key3", String("value3"))

            await cache.set_expire("key1", 1)  # 1 second
            await cache.set_expire("key2", 7)  # 7 seconds
            await cache.set_expire("key3", 7)  # 7 seconds

            # All keys should exist initially
            assert await cache.exists("key1")
            assert await cache.exists("key2")
            assert await cache.exists("key3")

            # Wait 1.5 seconds - key1 should be expired
            await asyncio.sleep(1.5)
            assert not await cache.exists("key1")
            assert await cache.exists("key2")
            assert await cache.exists("key3")

            await cache.set_expire("key2", 1)
            await cache.set_expire("key3", 1)
            # Wait another 1 second
            await asyncio.sleep(1)
            assert not await cache.exists("key1")
            assert not await cache.exists("key2")
            assert not await cache.exists("key3")

    async def test_set_expire_on_nonexistent_key(self):
        """Test setting expiration on a key that doesn't exist."""
        async with self.cache.session() as cache:
            # Try to set expiration on non-existent key
            await cache.set_expire("nonexistent", 10)

            # Key should still not exist
            assert not await cache.exists("nonexistent")
            assert await cache.get("nonexistent") is None

    async def test_set_expire_overwrite(self):
        """Test overwriting expiration time."""
        async with self.cache.session() as cache:
            await cache.set("overwrite_key", String("overwrite_value"))

            # Set initial expiration
            await cache.set_expire("overwrite_key", 1)

            # Overwrite with longer expiration
            await cache.set_expire("overwrite_key", 3)

            # Wait 1.5 seconds - key should still exist due to overwritten TTL
            await asyncio.sleep(1.5)
            assert await cache.exists("overwrite_key")

            # Wait another 2 seconds - key should be expired
            await asyncio.sleep(2)
            assert not await cache.exists("overwrite_key")

    async def test_set_expire_zero_ttl(self):
        """Test setting zero TTL(nothing should happen)"""
        async with self.cache.session() as cache:
            await cache.set("zero_ttl_key", String("zero_ttl_value"))
            try:
                await cache.set_expire("zero_ttl_key", 0)
            except Exception as e:
                assert isinstance(e, ValueError)

            assert await cache.exists("zero_ttl_key")
            assert await cache.get("zero_ttl_key") is not None

    async def test_set_expire_negative_ttl(self):
        """Test setting negative TTL (should not expire)."""
        async with self.cache.session() as cache:
            await cache.set("negative_ttl_key", String("negative_ttl_value"))
            try:
                await cache.set_expire("negative_ttl_key", -1)
            except Exception as e:
                assert isinstance(e, ValueError)

            assert await cache.exists("negative_ttl_key")
            assert await cache.get("negative_ttl_key") == "negative_ttl_value"

    async def test_keys_after_expiration(self):
        """Test that expired keys are not returned by keys() method."""
        async with self.cache.session() as cache:
            await self.cache.stop_ttl_deletion()
            await cache.set("persistent_key", String("persistent_value"))
            await cache.set("expire_key", String("expire_value"))

            await cache.set_expire("expire_key", 1)

            # Both keys should exist initially
            keys = await cache.keys()
            assert "persistent_key" in keys
            assert "expire_key" in keys

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Only persistent key should remain
            keys = await cache.keys()
            assert "persistent_key" in keys
            assert "expire_key" not in keys

    async def test_batch_get_with_expired_keys(self):
        """Test batch_get behavior with expired keys."""
        await self.cache.stop_ttl_deletion()
        async with self.cache.session() as cache:
            await cache.set("persistent_key", String("persistent_value"))
            await cache.set("expire_key", String("expire_value"))

            await cache.set_expire("expire_key", 1)

            # Both keys should be returned initially
            results = await cache.batch_get(["persistent_key", "expire_key"])
            assert "persistent_key" in results
            assert "expire_key" in results

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Only persistent key should be returned
            results = await cache.batch_get(["persistent_key", "expire_key"])
            assert "persistent_key" in results
            assert "expire_key" not in results

    async def test_delete_expired_key(self):
        """Test that deleting an expired key doesn't cause errors."""
        async with self.cache.session() as cache:
            await cache.set("expire_key", String("expire_value"))
            await cache.set_expire("expire_key", 1)

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Deleting expired key should not raise an error
            await cache.delete("expire_key")
            assert not await cache.exists("expire_key")

    async def test_set_expire_after_set(self):
        """Test setting expiration after setting a value."""
        async with self.cache.session() as cache:
            await cache.set("key", String("value"))

            # Set expiration after setting value
            await cache.set_expire("key", 1)

            # Key should exist
            assert await cache.exists("key")
            assert await cache.get("key") == "value"

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Key should be expired
            assert not await cache.exists("key")
            assert await cache.get("key") is None

    async def test_set_expire_before_set(self):
        """Test setting expiration before setting a value."""
        async with self.cache.session() as cache:
            # Set expiration before setting value
            key = await cache.set_expire("key", 1)
            key_exists = await cache.get("key")
            if key_exists:
                assert key == 1
            else:
                assert key == 0

            # Key should not exist
            if key_exists:
                assert await cache.exists("key")
            else:
                assert not await cache.exists("key")

            # Set the value
            await cache.set("key", String("value"))

            # Key should exist
            assert await cache.exists("key")
            assert await cache.get("key") == "value"

            key = await cache.set_expire("key", 1)
            assert key == 1
            # Wait for expiration
            await asyncio.sleep(1.5)

            # Key should be expired
            assert not await cache.exists("key")
            assert await cache.get("key") is None

    async def test_very_long_ttl(self):
        """Test very long TTL values."""
        async with self.cache.session() as cache:
            await cache.set("long_ttl_key", String("long_ttl_value"))
            await cache.set_expire("long_ttl_key", 3600)  # 1 hour

            # Key should exist
            assert await cache.exists("long_ttl_key")
            assert await cache.get("long_ttl_key") == "long_ttl_value"

            # Wait a short time - key should still exist
            await asyncio.sleep(0.1)
            assert await cache.exists("long_ttl_key")

    async def test_expiration_with_special_characters(self):
        """Test expiration with keys containing special characters."""
        async with self.cache.session() as cache:
            special_key = "key with spaces!@#$%^&*()"
            await cache.set(special_key, String("special_value"))
            await cache.set_expire(special_key, 1)

            # Key should exist initially
            assert await cache.exists(special_key)
            assert await cache.get(special_key) == "special_value"

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Key should be expired
            assert not await cache.exists(special_key)
            assert await cache.get(special_key) is None

    async def test_expiration_with_unicode_keys(self):
        """Test expiration with unicode keys."""
        async with self.cache.session() as cache:
            unicode_key = "ключ_с_кириллицей"
            await cache.set(unicode_key, String("unicode_value"))
            await cache.set_expire(unicode_key, 1)

            # Key should exist initially
            assert await cache.exists(unicode_key)
            assert await cache.get(unicode_key) == "unicode_value"

            # Wait for expiration
            await asyncio.sleep(1.5)

            # Key should be expired
            assert not await cache.exists(unicode_key)
            assert await cache.get(unicode_key) is None

    # Mixed Datatype Tests

    async def test_mixed_datatypes(self):
        async with self.cache.session() as cache:
            # Test all datatypes in one session
            await cache.set("string_key", String("hello"))
            await cache.set("list_key", List([1, 2, 3]))
            await cache.set("map_key", Map({"a": 1, "b": 2}))
            await cache.set("numeric_key", Numeric(42))
            await cache.set("set_key", Set({1, 2, 3}))
            await cache.set("queue_key", Queue([1, 2, 3]))

            # Verify all values
            assert self.extract_value(await cache.get("string_key")) == "hello"
            assert self.extract_value(await cache.get("list_key")) == [1, 2, 3]
            assert self.extract_value(await cache.get("map_key")) == {"a": 1, "b": 2}
            assert self.extract_value(await cache.get("numeric_key")) == 42
            assert self.extract_value(await cache.get("set_key")) == {1, 2, 3}
            assert list(self.extract_value(await cache.get("queue_key"))) == [1, 2, 3]

    async def test_batch_mixed_datatypes(self):
        async with self.cache.session() as cache:
            # Test batch operations with mixed datatypes
            mixed_data = {
                "string_key": String("hello"),
                "list_key": List([1, 2, 3]),
                "map_key": Map({"a": 1, "b": 2}),
                "numeric_key": Numeric(42),
                "set_key": Set({1, 2, 3}),
                "queue_key": Queue([1, 2, 3]),
            }

            await cache.batch_set(mixed_data)
            results = await cache.batch_get(list(mixed_data.keys()))

            assert self.extract_value(results["string_key"]) == "hello"
            assert self.extract_value(results["list_key"]) == [1, 2, 3]
            assert self.extract_value(results["map_key"]) == {"a": 1, "b": 2}
            assert self.extract_value(results["numeric_key"]) == 42
            assert self.extract_value(results["set_key"]) == {1, 2, 3}
            assert list(self.extract_value(results["queue_key"])) == [1, 2, 3]

    async def test_empty_string_key_mixed(self):
        async with self.cache.session() as cache:
            await cache.set("", String("empty_key_value"))
            result = await cache.get("")
            assert self.extract_value(result) == "empty_key_value"

    async def test_special_characters_in_key_mixed(self):
        async with self.cache.session() as cache:
            special_key = "key with spaces!@#$%^&*()"
            await cache.set(special_key, String("special_value"))
            result = await cache.get(special_key)
            assert self.extract_value(result) == "special_value"

    async def test_unicode_characters_mixed(self):
        async with self.cache.session() as cache:
            unicode_key = "ключ_с_кириллицей"
            await cache.set(unicode_key, String("unicode_value"))
            result = await cache.get(unicode_key)
            assert self.extract_value(result) == "unicode_value"

    async def test_large_value_mixed(self):
        async with self.cache.session() as cache:
            large_string = "x" * 10000
            await cache.set("large_key", String(large_string))
            result = await cache.get("large_key")
            assert self.extract_value(result) == large_string

    async def test_multiple_operations_sequence_mixed(self):
        async with self.cache.session() as cache:
            # test_multiple_operations_sequence_mixed
            # Complex sequence of operations
            await cache.set("key1", String("value1"))
            await cache.set("key2", List([1, 2, 3]))
            await cache.set("key3", Map({"a": 1}))

            # Update values
            await cache.set("key1", String("updated_value1"))
            await cache.set("key2", List([4, 5, 6]))

            # Delete and recreate
            await cache.delete("key3")
            await cache.set("key3", Numeric(999))

            # Verify final state
            assert self.extract_value(await cache.get("key1")) == "updated_value1"
            assert self.extract_value(await cache.get("key2")) == [4, 5, 6]
            assert self.extract_value(await cache.get("key3")) == 999

            # Test batch operations
            batch_data = {
                "batch_key1": String("batch_value1"),
                "batch_key2": Set({1, 2, 3}),
                "batch_key3": Queue([1, 2, 3]),
            }
            await cache.batch_set(batch_data)

            # Verify batch results
            batch_results = await cache.batch_get(
                ["batch_key1", "batch_key2", "batch_key3"]
            )
            assert self.extract_value(batch_results["batch_key1"]) == "batch_value1"
            assert self.extract_value(batch_results["batch_key2"]) == {1, 2, 3}
            assert list(self.extract_value(batch_results["batch_key3"])) == [1, 2, 3]

            # Test keys method
            all_keys = await cache.keys()
            expected_keys = {
                "key1",
                "key2",
                "key3",
                "batch_key1",
                "batch_key2",
                "batch_key3",
            }
            for key in expected_keys:
                assert key in all_keys

    async def test_concurrent_access_simulation_mixed(self):
        async with self.cache.session() as cache:
            # test_multiple_operations_sequence_mixed
            # Simulate concurrent-like operations by interleaving them
            await cache.set("concurrent_key1", String("value1"))
            await cache.set("concurrent_key2", List([1, 2, 3]))
            await cache.set("concurrent_key3", Map({"a": 1}))

            # Interleaved operations
            await cache.set("concurrent_key1", String("updated1"))
            await cache.set("concurrent_key4", Numeric(42))
            await cache.delete("concurrent_key2")
            await cache.set("concurrent_key5", Set({1, 2, 3}))

            # Verify final state
            assert self.extract_value(await cache.get("concurrent_key1")) == "updated1"
            assert await cache.get("concurrent_key2") is None
            assert self.extract_value(await cache.get("concurrent_key3")) == {"a": 1}
            assert self.extract_value(await cache.get("concurrent_key4")) == 42
            assert self.extract_value(await cache.get("concurrent_key5")) == {1, 2, 3}

            # Test exists method
            assert await cache.exists("concurrent_key1")
            assert not await cache.exists("concurrent_key2")
            assert await cache.exists("concurrent_key3")
            assert await cache.exists("concurrent_key4")
            assert await cache.exists("concurrent_key5")

            # Test keys method
            keys = await cache.keys()
            expected_keys = {
                "concurrent_key1",
                "concurrent_key3",
                "concurrent_key4",
                "concurrent_key5",
            }
            for key in expected_keys:
                assert key in keys

    # TTL Worker Tests
    @pytest.mark.flaky(reruns=2, reruns_delay=1)
    async def test_expired_keys_identification(self):
        """Test that expired keys are properly identified without TTL worker."""
        await self.cache.stop_ttl_deletion()
        cache = self.create_cache()  # No TTL worker
        async with cache.session() as session:
            # Set keys with different expiration times
            await session.set("expired_key", String("expired_value"))
            await session.set("future_key", String("future_value"))
            await session.set("persistent_key", String("persistent_value"))

            # Set expiration times
            await session.set_expire("expired_key", 1)  # Will expire in 1 second
            await session.set_expire("future_key", 10)  # Will expire in 10 seconds

            # Wait for expired_key to expire
            await asyncio.sleep(1.5)

            # Check that expired keys are identified correctly
            expired_count = cache.count_expired_keys()
            assert expired_count >= 1  # Only expired_key should be expired

    async def test_ttl_worker_basic_deletion(self):
        """Test that TTL worker actually deletes expired keys."""
        cache = self.create_cache()  # No TTL worker initially

        async with cache.session() as session:
            # Set keys with expiration
            await session.set("expire_key1", String("value1"))
            await session.set("expire_key2", String("value2"))
            await session.set("persistent_key", String("persistent_value"))

            # Set expiration times
            await session.set_expire("expire_key1", 1)
            await session.set_expire("expire_key2", 1)

            # Wait for keys to expire
            await asyncio.sleep(1.5)

            # Verify keys are expired but still in database
            expired_count = cache.count_expired_keys()
            assert expired_count >= 2

            # Start TTL worker
            await cache.start_ttl_deletion(delete_interval=0.2)  # Cleanup every 200ms

            # Wait for TTL worker to clean up (give it multiple cycles)
            await asyncio.sleep(1.0)

            # Check that expired keys are now deleted
            expired_count = cache.count_expired_keys()
            assert expired_count == 0

            # Verify persistent key still exists
            assert await session.exists("persistent_key")
            assert not await session.exists("expire_key1")
            assert not await session.exists("expire_key2")

    async def test_ttl_worker_stop_and_restart(self):
        """Test stopping and restarting TTL worker."""
        cache = self.create_cache()

        async with cache.session() as session:
            # test_multiple_operations_sequence_mixed
            # Set keys with expiration
            await session.set("expire_key", String("expire_value"))
            await session.set_expire("expire_key", 1)

            # Wait for key to expire
            await asyncio.sleep(1.5)

            # Verify key is expired
            expired_count = cache.count_expired_keys()
            assert expired_count == 1

            # Start TTL worker
            await cache.start_ttl_deletion(delete_interval=0.2)

            # Wait for cleanup
            await asyncio.sleep(1.0)

            # Verify key is deleted
            expired_count = cache.count_expired_keys()
            assert expired_count == 0

            # Stop TTL worker
            await cache.stop_ttl_deletion()

            # Set another expired key
            await session.set("expire_key2", String("expire_value2"))
            await session.set_expire("expire_key2", 1)

            # Wait for key to expire
            await asyncio.sleep(1.5)

            # Verify key is expired but not deleted (TTL worker stopped)
            expired_count = cache.count_expired_keys()
            assert expired_count == 1

            # Restart TTL worker
            await cache.start_ttl_deletion(delete_interval=0.2)

            # Wait for cleanup
            await asyncio.sleep(1.0)

            # Verify key is now deleted
            expired_count = cache.count_expired_keys()
            assert expired_count == 0

    # Transaction Tests

    async def test_successful_transaction(self):
        """Test that a successful transaction commits all changes."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))
            await session.set("key2", String("initial2"))

            # Start transaction and modify values
            async with session.with_transaction() as tx:
                await tx.set("key1", String("updated1"))
                await tx.set("key2", String("updated2"))
                await tx.set("key3", String("new3"))

            # Verify all changes were committed
            assert self.extract_value(await session.get("key1")) == "updated1"
            assert self.extract_value(await session.get("key2")) == "updated2"
            assert self.extract_value(await session.get("key3")) == "new3"

    async def test_transaction_rollback_on_exception(self):
        """Test that a transaction rolls back when an exception occurs."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))
            await session.set("key2", String("initial2"))

            # Start transaction and modify values, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.set("key1", String("updated1"))
                    await tx.set("key2", String("updated2"))
                    await tx.set("key3", String("new3"))
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify all changes were rolled back
            assert self.extract_value(await session.get("key1")) == "initial1"
            assert self.extract_value(await session.get("key2")) == "initial2"
            assert await session.get("key3") is None

    async def test_transaction_with_batch_operations(self):
        """Test transactions with batch operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))

            # Start transaction with batch operations
            async with session.with_transaction() as tx:
                await tx.batch_set(
                    {
                        "key1": String("updated1"),
                        "key2": String("new2"),
                        "key3": String("new3"),
                    }
                )

            # Verify all changes were committed
            assert self.extract_value(await session.get("key1")) == "updated1"
            assert self.extract_value(await session.get("key2")) == "new2"
            assert self.extract_value(await session.get("key3")) == "new3"

    async def test_transaction_rollback_with_batch_operations(self):
        """Test transaction rollback with batch operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))

            # Start transaction with batch operations, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.batch_set(
                        {
                            "key1": String("updated1"),
                            "key2": String("new2"),
                            "key3": String("new3"),
                        }
                    )
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify all changes were rolled back
            assert self.extract_value(await session.get("key1")) == "initial1"
            assert await session.get("key2") is None
            assert await session.get("key3") is None

    async def test_transaction_with_delete_operations(self):
        """Test transactions with delete operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("value1"))
            await session.set("key2", String("value2"))
            await session.set("key3", String("value3"))

            # Start transaction and delete some keys
            async with session.with_transaction() as tx:
                await tx.delete("key1")
                await tx.delete("key2")

            # Verify deletions were committed
            assert await session.get("key1") is None
            assert await session.get("key2") is None
            assert self.extract_value(await session.get("key3")) == "value3"

    async def test_transaction_rollback_with_delete_operations(self):
        """Test transaction rollback with delete operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("value1"))
            await session.set("key2", String("value2"))

            # Start transaction and delete keys, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.delete("key1")
                    await tx.delete("key2")
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify deletions were rolled back
            assert self.extract_value(await session.get("key1")) == "value1"
            assert self.extract_value(await session.get("key2")) == "value2"

    async def test_transaction_with_mixed_operations(self):
        """Test transactions with mixed operations (set, delete, batch)."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))
            await session.set("key2", String("initial2"))
            await session.set("key3", String("initial3"))

            # Start transaction with mixed operations
            async with session.with_transaction() as tx:
                await tx.set("key1", String("updated1"))  # Update
                await tx.delete("key2")  # Delete
                await tx.batch_set(
                    {"key3": String("updated3"), "key4": String("new4")}  # Batch set
                )

            # Verify all operations were committed
            assert self.extract_value(await session.get("key1")) == "updated1"
            assert await session.get("key2") is None
            assert self.extract_value(await session.get("key3")) == "updated3"
            assert self.extract_value(await session.get("key4")) == "new4"

    async def test_transaction_rollback_with_mixed_operations(self):
        """Test transaction rollback with mixed operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("initial1"))
            await session.set("key2", String("initial2"))
            await session.set("key3", String("initial3"))

            # Start transaction with mixed operations, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.set("key1", String("updated1"))  # Update
                    await tx.delete("key2")  # Delete
                    await tx.batch_set(
                        {  # Batch set
                            "key3": String("updated3"),
                            "key4": String("new4"),
                        }
                    )
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify all operations were rolled back
            assert self.extract_value(await session.get("key1")) == "initial1"
            assert self.extract_value(await session.get("key2")) == "initial2"
            assert self.extract_value(await session.get("key3")) == "initial3"
            assert await session.get("key4") is None

    async def test_transaction_with_all_datatypes(self):
        """Test transactions with all datatypes."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Start transaction with all datatypes
            async with session.with_transaction() as tx:
                await tx.set("string_key", String("hello"))
                await tx.set("list_key", List([1, 2, 3]))
                await tx.set("map_key", Map({"a": 1, "b": 2}))
                await tx.set("numeric_key", Numeric(42))
                await tx.set("set_key", Set({1, 2, 3}))
                await tx.set("queue_key", Queue([1, 2, 3]))

            # Verify all values were committed
            assert self.extract_value(await session.get("string_key")) == "hello"
            assert self.extract_value(await session.get("list_key")) == [1, 2, 3]
            assert self.extract_value(await session.get("map_key")) == {"a": 1, "b": 2}
            assert self.extract_value(await session.get("numeric_key")) == 42
            assert self.extract_value(await session.get("set_key")) == {1, 2, 3}
            assert list(self.extract_value(await session.get("queue_key"))) == [1, 2, 3]

    async def test_transaction_rollback_with_all_datatypes(self):
        """Test transaction rollback with all datatypes."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Start transaction with all datatypes, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.set("string_key", String("hello"))
                    await tx.set("list_key", List([1, 2, 3]))
                    await tx.set("map_key", Map({"a": 1, "b": 2}))
                    await tx.set("numeric_key", Numeric(42))
                    await tx.set("set_key", Set({1, 2, 3}))
                    await tx.set("queue_key", Queue([1, 2, 3]))
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify all values were rolled back
            assert await session.get("string_key") is None
            assert await session.get("list_key") is None
            assert await session.get("list_key") is None
            assert await session.get("map_key") is None
            assert await session.get("numeric_key") is None
            assert await session.get("set_key") is None
            assert await session.get("queue_key") is None

    async def test_transaction_with_expire_operations(self):
        """Test transactions with expire operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("value1"))
            await session.set("key2", String("value2"))

            # Start transaction and set expiration
            async with session.with_transaction() as tx:
                await tx.set_expire("key1", 60)  # 60 seconds
                await tx.set_expire("key2", 120)  # 120 seconds

            # Verify expiration was set
            assert await session.get_expire("key1") == 60
            assert await session.get_expire("key2") == 120

    async def test_transaction_rollback_with_expire_operations(self):
        """Test transaction rollback with expire operations."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial values
            await session.set("key1", String("value1"))
            await session.set("key2", String("value2"))

            # Start transaction and set expiration, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.set_expire("key1", 60)  # 60 seconds
                    await tx.set_expire("key2", 120)  # 120 seconds
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify expiration was rolled back
            assert await session.get_expire("key1") is None
            assert await session.get_expire("key2") is None

    async def test_transaction_with_large_data(self):
        """Test transactions with large amounts of data."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Create large dataset
            large_data = {}
            for i in range(100):
                large_data[f"key_{i}"] = String(f"value_{i}")

            # Start transaction and set large dataset
            async with session.with_transaction() as tx:
                await tx.batch_set(large_data)

            # Verify all data was committed
            keys = [f"key_{i}" for i in range(100)]
            results = await session.batch_get(keys)
            for i in range(100):
                assert self.extract_value(results[f"key_{i}"]) == f"value_{i}"

    async def test_transaction_rollback_with_large_data(self):
        """Test transaction rollback with large amounts of data."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Create large dataset
            large_data = {}
            for i in range(100):
                large_data[f"key_{i}"] = String(f"value_{i}")

            # Start transaction and set large dataset, then raise exception
            try:
                async with session.with_transaction() as tx:
                    await tx.batch_set(large_data)
                    raise Exception("Simulated error")
            except Exception:
                pass  # Expected exception

            # Verify all data was rolled back
            keys = [f"key_{i}" for i in range(100)]
            results = await session.batch_get(keys)
            for i in range(100):
                assert results.get(f"key_{i}") is None

    async def test_transaction_error_handling(self):
        """Test that different types of errors are properly handled in transactions."""
        if not self.cache.get_support_transactions():
            pytest.skip("Adapter does not support transactions")
        async with self.cache.session() as session:
            # Set initial value
            await session.set("key1", String("initial"))

            # Test with ValueError
            try:
                async with session.with_transaction() as tx:
                    await tx.set("key1", String("updated"))
                    raise ValueError("Test ValueError")
            except ValueError:
                pass

            assert self.extract_value(await session.get("key1")) == "initial"

            # Test with TypeError
            try:
                async with session.with_transaction() as tx:
                    await tx.set("key1", String("updated"))
                    raise TypeError("Test TypeError")
            except TypeError:
                pass

            assert self.extract_value(await session.get("key1")) == "initial"

            # Test with RuntimeError
            try:
                async with session.with_transaction() as tx:
                    await tx.set("key1", String("updated"))
                    raise RuntimeError("Test RuntimeError")
            except RuntimeError:
                pass

            assert self.extract_value(await session.get("key1")) == "initial"


class FileBasedCacheTests(BaseCacheTests):
    def create_temp_file(self):
        return tempfile.NamedTemporaryFile(delete=False, suffix=".db").name

    def teardown_method(self):
        """Clean up temporary files after tests."""
        super().teardown_method()
        # Clean up any temporary files created during tests
        for attr in dir(self):
            if attr.startswith("temp_file"):
                file_path = getattr(self, attr, None)
                if file_path and os.path.exists(file_path):
                    try:
                        os.unlink(file_path)
                    except OSError:
                        pass  # File might already be deleted

    def create_cache(self):
        self.temp_file = self.create_temp_file()
        from src.pycache.adapters.SQLite import SQLite

        adapter = SQLite(self.temp_file)
        return PyCache(adapter)  # No TTL worker by default
