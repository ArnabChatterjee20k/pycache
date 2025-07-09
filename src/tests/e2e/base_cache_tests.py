import pytest
import tempfile
import os
from abc import ABC, abstractmethod
from src.pycache.py_cache import PyCache
from src.pycache.datatypes import String, List, Map, Numeric, Set, Queue
from collections import deque


class BaseCacheTests(ABC):
    @pytest.fixture(autouse=True)
    def setup_cache(self, request):
        self.cache: PyCache = self.create_cache()
        yield
        self.teardown_cache()

    @abstractmethod
    def create_cache(self):
        raise NotImplementedError

    @abstractmethod
    def teardown_cache(self):
        raise NotImplementedError

    def extract_value(self, datatype_instance):
        """Extract the native value from a datatype instance."""
        if hasattr(datatype_instance, "value"):
            return datatype_instance.value
        return datatype_instance

    # Basic CRUD Tests with String datatype

    def test_set_and_get_string(self):
        with self.cache.session() as cache:
            cache.set("foo", String("bar"))
            result = cache.get("foo")
            assert self.extract_value(result) == "bar"

    def test_delete_string(self):
        with self.cache.session() as cache:
            cache.set("foo", String("bar"))
            cache.delete("foo")
            assert not cache.exists("foo")

    def test_exists_string(self):
        with self.cache.session() as cache:
            cache.set("foo", String("bar"))
            assert cache.exists("foo")
            cache.delete("foo")
            assert not cache.exists("foo")

    def test_keys_string(self):
        with self.cache.session() as cache:
            cache.set("foo", String("bar"))
            cache.set("baz", String("qux"))
            keys = cache.keys()
            assert "foo" in keys and "baz" in keys

    def test_overwrite_value_string(self):
        with self.cache.session() as cache:
            cache.set("foo", String("bar"))
            cache.set("foo", String("baz"))
            result = cache.get("foo")

            assert result == "baz", f"{result} "

    def test_delete_nonexistent_key(self):
        with self.cache.session() as cache:
            cache.delete("doesnotexist")
            assert not cache.exists("doesnotexist")

    def test_get_nonexistent_key(self):
        with self.cache.session() as cache:
            assert cache.get("nope") is None

    # Batch Operations with String datatype

    def test_batch_set_and_get_string(self):
        with self.cache.session() as cache:
            data = {"a": String("1"), "b": String("2")}
            cache.batch_set(data)
            results = cache.batch_get(["a", "b", "c"])
            assert self.extract_value(results["a"]) == "1"
            assert self.extract_value(results["b"]) == "2"
            assert results.get("c") is None

    def test_keys_after_deletion_string(self):
        with self.cache.session() as cache:
            cache.set("x", String("y"))
            cache.set("z", String("w"))
            cache.delete("x")
            keys = cache.keys()
            assert "x" not in keys and "z" in keys

    # List Datatype Tests

    def test_list_datatype(self):
        with self.cache.session() as cache:
            test_list = [1, 2, 3, "hello"]
            cache.set("list_key", List(test_list))
            result = cache.get("list_key")
            assert self.extract_value(result) == [1, 2, 3, "hello"]

    def test_list_operations(self):
        with self.cache.session() as cache:
            # Test empty list
            cache.set("empty_list", List([]))
            assert self.extract_value(cache.get("empty_list")) == []

            # Test list with mixed types
            mixed_list = [1, "string", True, None, {"key": "value"}]
            cache.set("mixed_list", List(mixed_list))
            assert self.extract_value(cache.get("mixed_list")) == mixed_list

    def test_list_overwrite(self):
        with self.cache.session() as cache:
            cache.set("list_key", List([1, 2, 3]))
            cache.set("list_key", List([4, 5, 6]))
            result = cache.get("list_key")
            assert self.extract_value(result) == [4, 5, 6]

    # Map Datatype Tests

    def test_map_datatype(self):
        with self.cache.session() as cache:
            test_dict = {"name": "John", "age": 30, "city": "New York"}
            cache.set("map_key", Map(test_dict))
            result = cache.get("map_key")
            assert self.extract_value(result) == test_dict

    def test_map_operations(self):
        with self.cache.session() as cache:
            # Test empty dict
            cache.set("empty_map", Map({}))
            assert self.extract_value(cache.get("empty_map")) == {}

            # Test nested dict
            nested_dict = {
                "user": {
                    "name": "Alice",
                    "preferences": {"theme": "dark", "language": "en"},
                }
            }
            cache.set("nested_map", Map(nested_dict))
            assert self.extract_value(cache.get("nested_map")) == nested_dict

    def test_map_overwrite(self):
        with self.cache.session() as cache:
            cache.set("map_key", Map({"old": "value"}))
            cache.set("map_key", Map({"new": "value"}))
            result = cache.get("map_key")
            assert self.extract_value(result) == {"new": "value"}

    # Numeric Datatype Tests

    def test_numeric_integer(self):
        with self.cache.session() as cache:
            cache.set("int_key", Numeric(42))
            result = cache.get("int_key")
            assert self.extract_value(result) == 42

    def test_numeric_float(self):
        with self.cache.session() as cache:
            cache.set("float_key", Numeric(3.14159))
            result = cache.get("float_key")
            assert self.extract_value(result) == 3.14159

    def test_numeric_operations(self):
        with self.cache.session() as cache:
            # Test zero
            cache.set("zero", Numeric(0))
            assert self.extract_value(cache.get("zero")) == 0

            # Test negative numbers
            cache.set("negative", Numeric(-100))
            assert self.extract_value(cache.get("negative")) == -100

            # Test large numbers
            cache.set("large", Numeric(999999999))
            assert self.extract_value(cache.get("large")) == 999999999

    def test_numeric_overwrite(self):
        with self.cache.session() as cache:
            cache.set("num_key", Numeric(10))
            cache.set("num_key", Numeric(20))
            result = cache.get("num_key")
            assert self.extract_value(result) == 20

    # Set Datatype Tests

    def test_set_datatype(self):
        with self.cache.session() as cache:
            test_set = {1, 2, 3, "hello"}
            cache.set("set_key", Set(test_set))
            result = cache.get("set_key")
            assert self.extract_value(result) == test_set

    def test_set_operations(self):
        with self.cache.session() as cache:
            # Test empty set
            cache.set("empty_set", Set(set()))
            assert self.extract_value(cache.get("empty_set")) == set()

            # Test set with mixed types
            mixed_set = {1, "string", True, None}
            cache.set("mixed_set", Set(mixed_set))
            assert self.extract_value(cache.get("mixed_set")) == mixed_set

    def test_set_overwrite(self):
        with self.cache.session() as cache:
            cache.set("set_key", Set({1, 2, 3}))
            cache.set("set_key", Set({4, 5, 6}))
            result = cache.get("set_key")
            assert self.extract_value(result) == {4, 5, 6}

    # Queue Datatype Tests

    def test_queue_datatype(self):
        with self.cache.session() as cache:
            test_queue = [1, 2, 3, 4, 5]  # Queue is implemented as a list
            cache.set("queue_key", Queue(test_queue))
            result = cache.get("queue_key")
            assert isinstance(result, deque)
            for i, val in enumerate(test_queue):
                assert result[i] == val

    def test_queue_operations(self):
        with self.cache.session() as cache:
            # Test empty queue
            cache.set("empty_queue", Queue([]))
            assert len(self.extract_value(cache.get("empty_queue"))) == 0

            # Test queue with mixed types
            mixed_queue = [1, "string", True, None, {"key": "value"}]
            cache.set("mixed_queue", Queue(mixed_queue))
            result = self.extract_value(cache.get("mixed_queue"))
            for i, val in enumerate(result):
                assert val == mixed_queue[i]

    # Mixed Datatype Tests

    def test_mixed_datatypes(self):
        with self.cache.session() as cache:
            # Store different datatypes
            cache.set("string_key", String("hello"))
            cache.set("list_key", List([1, 2, 3]))
            cache.set("map_key", Map({"name": "John"}))
            cache.set("numeric_key", Numeric(42))
            cache.set("set_key", Set({1, 2, 3}))

            # Retrieve and verify
            assert self.extract_value(cache.get("string_key")) == "hello"
            assert self.extract_value(cache.get("list_key")) == [1, 2, 3]
            assert self.extract_value(cache.get("map_key")) == {"name": "John"}
            assert self.extract_value(cache.get("numeric_key")) == 42
            assert self.extract_value(cache.get("set_key")) == {1, 2, 3}

    def test_batch_mixed_datatypes(self):
        with self.cache.session() as cache:
            data = {
                "string": String("hello"),
                "list": List([1, 2, 3]),
                "map": Map({"key": "value"}),
                "numeric": Numeric(42),
                "set": Set({1, 2, 3}),
                "queue": Queue([1, 2, 3]),
            }
            cache.batch_set(data)

            results = cache.batch_get(
                ["string", "list", "map", "numeric", "set", "queue"]
            )
            for key in results:
                assert self.extract_value(results[key]) == data[key].value

    # Edge Cases with different datatypes

    def test_empty_string_key_mixed(self):
        with self.cache.session() as cache:
            cache.set("", String("empty_key_value"))
            result = cache.get("")
            assert self.extract_value(result) == "empty_key_value"
            assert cache.exists("")

    def test_special_characters_in_key_mixed(self):
        with self.cache.session() as cache:
            special_key = "key_with_special_chars!@#$%^&*()"
            cache.set(special_key, Map({"special": "value"}))
            result = cache.get(special_key)
            assert self.extract_value(result) == {"special": "value"}
            assert cache.exists(special_key)

    def test_unicode_characters_mixed(self):
        with self.cache.session() as cache:
            unicode_key = "key_with_unicode_🚀"
            unicode_value = "value_with_unicode_🌟"
            cache.set(unicode_key, String(unicode_value))
            result = cache.get(unicode_key)
            assert self.extract_value(result) == unicode_value

    def test_large_value_mixed(self):
        with self.cache.session() as cache:
            large_list = list(range(1000))
            cache.set("large_key", List(large_list))
            result = cache.get("large_key")
            assert self.extract_value(result) == large_list

    def test_multiple_operations_sequence_mixed(self):
        with self.cache.session() as cache:
            # Set multiple values with different datatypes
            cache.set("string_key", String("value1"))
            cache.set("list_key", List([1, 2, 3]))
            cache.set("map_key", Map({"key": "value"}))

            # Verify all exist
            assert cache.exists("string_key")
            assert cache.exists("list_key")
            assert cache.exists("map_key")

            # Get all values
            assert self.extract_value(cache.get("string_key")) == "value1"
            assert self.extract_value(cache.get("list_key")) == [1, 2, 3]
            assert self.extract_value(cache.get("map_key")) == {"key": "value"}

            # Update one value
            cache.set("list_key", List([4, 5, 6]))
            assert self.extract_value(cache.get("list_key")) == [4, 5, 6]

            # Delete one key
            cache.delete("string_key")
            assert not cache.exists("string_key")
            assert cache.get("string_key") is None

            # Verify remaining keys
            keys = cache.keys()
            assert "string_key" not in keys
            assert "list_key" in keys
            assert "map_key" in keys

    def test_concurrent_access_simulation_mixed(self):
        with self.cache.session() as cache:
            # Rapid set operations with different datatypes
            for i in range(50):
                cache.set(f"string_{i}", String(f"value_{i}"))
                cache.set(f"list_{i}", List([i, i + 1, i + 2]))
                cache.set(f"map_{i}", Map({"id": i, "value": f"val_{i}"}))

            # Verify all were set
            for i in range(50):
                assert self.extract_value(cache.get(f"string_{i}")) == f"value_{i}"
                assert self.extract_value(cache.get(f"list_{i}")) == [i, i + 1, i + 2]
                assert self.extract_value(cache.get(f"map_{i}")) == {
                    "id": i,
                    "value": f"val_{i}",
                }

            # Rapid delete operations
            for i in range(25):
                cache.delete(f"string_{i}")
                cache.delete(f"list_{i}")

            # Verify deletions
            for i in range(25):
                assert not cache.exists(f"string_{i}")
                assert not cache.exists(f"list_{i}")
                assert cache.get(f"string_{i}") is None
                assert cache.get(f"list_{i}") is None

            # Verify remaining keys
            for i in range(25, 50):
                assert cache.exists(f"string_{i}")
                assert cache.exists(f"list_{i}")
                assert self.extract_value(cache.get(f"string_{i}")) == f"value_{i}"
                assert self.extract_value(cache.get(f"list_{i}")) == [i, i + 1, i + 2]


class FileBasedCacheTests(BaseCacheTests):
    """
    Base class for file-based cache tests (SQLite, etc.).

    Provides common file management functionality.
    """

    def create_temp_file(self):
        """Create a temporary file for testing."""
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        return self.temp_file.name

    def teardown_cache(self):
        """Clean up temporary files."""
        # if hasattr(self, 'cache') and self.cache:
        #     self.cache.adapter.close()

        # if hasattr(self, 'temp_file') and self.temp_file and os.path.exists(self.temp_file.name):
        #     os.unlink(self.temp_file.name)
        pass
