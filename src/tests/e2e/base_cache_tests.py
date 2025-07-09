import pytest
import tempfile
import os
from abc import ABC, abstractmethod
from src.pycache.py_cache import PyCache
from src.pycache.datatypes import String, List, Map, Numeric, Set, Queue
from collections import deque


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

    # Mixed Datatype Tests

    async def test_mixed_datatypes(self):
        async with self.cache.session() as cache:
            # Store different datatypes
            await cache.set("string_key", String("hello"))
            await cache.set("list_key", List([1, 2, 3]))
            await cache.set("map_key", Map({"name": "John"}))
            await cache.set("numeric_key", Numeric(42))
            await cache.set("set_key", Set({1, 2, 3}))
            await cache.set("queue_key", Queue([1, 2, 3]))

            # Retrieve and verify
            assert self.extract_value(await cache.get("string_key")) == "hello"
            assert self.extract_value(await cache.get("list_key")) == [1, 2, 3]
            assert self.extract_value(await cache.get("map_key")) == {"name": "John"}
            assert self.extract_value(await cache.get("numeric_key")) == 42
            assert self.extract_value(await cache.get("set_key")) == {1, 2, 3}
            assert list(self.extract_value(await cache.get("queue_key"))) == [1, 2, 3]

    async def test_batch_mixed_datatypes(self):
        async with self.cache.session() as cache:
            # Batch set mixed datatypes
            mixed_data = {
                "str": String("hello"),
                "lst": List([1, 2, 3]),
                "mp": Map({"key": "value"}),
                "num": Numeric(42),
                "st": Set({1, 2, 3}),
                "q": Queue([1, 2, 3]),
            }
            await cache.batch_set(mixed_data)

            # Batch get and verify
            results = await cache.batch_get(["str", "lst", "mp", "num", "st", "q"])
            assert self.extract_value(results["str"]) == "hello"
            assert self.extract_value(results["lst"]) == [1, 2, 3]
            assert self.extract_value(results["mp"]) == {"key": "value"}
            assert self.extract_value(results["num"]) == 42
            assert self.extract_value(results["st"]) == {1, 2, 3}
            assert list(self.extract_value(results["q"])) == [1, 2, 3]

    # Edge Cases and Special Characters

    async def test_empty_string_key_mixed(self):
        async with self.cache.session() as cache:
            await cache.set("", String("empty key"))
            result = await cache.get("")
            assert self.extract_value(result) == "empty key"

    async def test_special_characters_in_key_mixed(self):
        async with self.cache.session() as cache:
            special_key = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
            await cache.set(special_key, String("special value"))
            result = await cache.get(special_key)
            assert self.extract_value(result) == "special value"

    async def test_unicode_characters_mixed(self):
        async with self.cache.session() as cache:
            unicode_key = "🚀🌟✨"
            await cache.set(unicode_key, String("unicode value"))
            result = await cache.get(unicode_key)
            assert self.extract_value(result) == "unicode value"

    async def test_large_value_mixed(self):
        async with self.cache.session() as cache:
            large_value = "x" * 10000
            await cache.set("large_key", String(large_value))
            result = await cache.get("large_key")
            assert self.extract_value(result) == large_value

    # Complex Operations

    async def test_multiple_operations_sequence_mixed(self):
        async with self.cache.session() as cache:
            # Set multiple values
            await cache.set("key1", String("value1"))
            await cache.set("key2", List([1, 2, 3]))
            await cache.set("key3", Map({"nested": "value"}))

            # Verify they exist
            assert await cache.exists("key1")
            assert await cache.exists("key2")
            assert await cache.exists("key3")

            # Get all keys
            keys = await cache.keys()
            assert "key1" in keys
            assert "key2" in keys
            assert "key3" in keys

            # Update values
            await cache.set("key1", String("updated_value1"))
            await cache.set("key2", List([4, 5, 6]))

            # Verify updates
            assert self.extract_value(await cache.get("key1")) == "updated_value1"
            assert self.extract_value(await cache.get("key2")) == [4, 5, 6]

            # Delete one key
            await cache.delete("key1")
            assert not await cache.exists("key1")
            assert await cache.exists("key2")
            assert await cache.exists("key3")

            # Verify remaining keys
            keys = await cache.keys()
            assert "key1" not in keys
            assert "key2" in keys
            assert "key3" in keys

    async def test_concurrent_access_simulation_mixed(self):
        async with self.cache.session() as cache:
            # Simulate concurrent-like operations by interleaving them
            await cache.set("counter", Numeric(0))
            await cache.set("list_data", List([]))
            await cache.set("map_data", Map({}))

            # Simulate multiple operations that might happen concurrently
            for i in range(10):
                # Update counter
                current = await cache.get("counter")
                await cache.set("counter", Numeric(self.extract_value(current) + 1))

                # Update list
                current_list = await cache.get("list_data")
                updated_list = self.extract_value(current_list) + [i]
                await cache.set("list_data", List(updated_list))

                # Update map
                current_map = await cache.get("map_data")
                updated_map = self.extract_value(current_map).copy()
                updated_map[f"key_{i}"] = f"value_{i}"
                await cache.set("map_data", Map(updated_map))

            # Verify final state
            final_counter = await cache.get("counter")
            assert self.extract_value(final_counter) == 10

            final_list = await cache.get("list_data")
            assert self.extract_value(final_list) == list(range(10))

            final_map = await cache.get("map_data")
            expected_map = {f"key_{i}": f"value_{i}" for i in range(10)}
            assert self.extract_value(final_map) == expected_map


class FileBasedCacheTests(BaseCacheTests):
    def create_temp_file(self):
        """Create a temporary file for the cache."""
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        return self.temp_file.name

    def teardown_method(self):
        """Clean up temporary file."""
        super().teardown_method()
        if hasattr(self, "temp_file") and self.temp_file:
            try:
                os.unlink(self.temp_file.name)
            except OSError:
                pass  # File might already be deleted
