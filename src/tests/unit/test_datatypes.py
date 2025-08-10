import pytest
from collections import deque
from src.pycache.datatypes import String, List, Map, Numeric, Set, Queue, Streams


class TestString:
    """Unit tests for String datatype"""

    def test_valid_string(self):
        """Test creating String with valid string value"""
        string_dt = String("hello world")
        assert string_dt.value == "hello world"

    def test_string_conversion(self):
        """Test String converts non-string values to string"""
        string_dt = String(123)
        assert string_dt.value == "123"

    def test_empty_string(self):
        """Test String with empty string"""
        string_dt = String("")
        assert string_dt.value == ""

    def test_unicode_string(self):
        """Test String with unicode characters"""
        string_dt = String("🚀🌟")
        assert string_dt.value == "🚀🌟"

    def test_string_with_special_chars(self):
        """Test String with special characters"""
        string_dt = String("!@#$%^&*()")
        assert string_dt.value == "!@#$%^&*()"


class TestList:
    """Unit tests for List datatype"""

    def test_valid_list(self):
        """Test creating List with valid list value"""
        list_dt = List([1, 2, 3])
        assert list_dt.value == [1, 2, 3]

    def test_list_from_tuple(self):
        """Test List from tuple"""
        list_dt = List((1, 2, 3))
        assert list_dt.value == [1, 2, 3]

    def test_list_from_set(self):
        """Test List from set"""
        list_dt = List({1, 2, 3})
        # Order may vary with sets
        assert set(list_dt.value) == {1, 2, 3}

    def test_empty_list(self):
        """Test List with empty list"""
        list_dt = List([])
        assert list_dt.value == []

    def test_list_with_mixed_types(self):
        """Test List with mixed data types"""
        mixed_data = [1, "string", True, None, {"key": "value"}]
        list_dt = List(mixed_data)
        assert list_dt.value == mixed_data

    def test_list_from_string(self):
        """Test List from string (iterable)"""
        list_dt = List("hello")
        assert list_dt.value == ["h", "e", "l", "l", "o"]

    def test_list_from_range(self):
        """Test List from range object"""
        list_dt = List(range(3))
        assert list_dt.value == [0, 1, 2]


class TestMap:
    """Unit tests for Map datatype"""

    def test_valid_dict(self):
        """Test creating Map with valid dict value"""
        map_dt = Map({"name": "John", "age": 30})
        assert map_dt.value == {"name": "John", "age": 30}

    def test_empty_dict(self):
        """Test Map with empty dict"""
        map_dt = Map({})
        assert map_dt.value == {}

    def test_nested_dict(self):
        """Test Map with nested dict"""
        nested_dict = {
            "user": {
                "name": "Alice",
                "preferences": {"theme": "dark", "language": "en"},
            }
        }
        map_dt = Map(nested_dict)
        assert map_dt.value == nested_dict

    def test_dict_with_mixed_types(self):
        """Test Map with mixed value types"""
        mixed_dict = {
            "string": "value",
            "number": 42,
            "boolean": True,
            "list": [1, 2, 3],
            "none": None,
        }
        map_dt = Map(mixed_dict)
        assert map_dt.value == mixed_dict


class TestNumeric:
    """Unit tests for Numeric datatype"""

    def test_valid_integer(self):
        """Test creating Numeric with valid integer"""
        numeric_dt = Numeric(42)
        assert numeric_dt.value == 42

    def test_valid_float(self):
        """Test creating Numeric with valid float"""
        numeric_dt = Numeric(3.14159)
        assert numeric_dt.value == 3.14159

    def test_zero_values(self):
        """Test Numeric with zero values"""
        int_zero = Numeric(0)
        float_zero = Numeric(0.0)
        assert int_zero.value == 0
        assert float_zero.value == 0.0

    def test_negative_values(self):
        """Test Numeric with negative values"""
        negative_int = Numeric(-100)
        negative_float = Numeric(-3.14)
        assert negative_int.value == -100
        assert negative_float.value == -3.14

    def test_large_numbers(self):
        """Test Numeric with large numbers"""
        large_int = Numeric(999999999)
        large_float = Numeric(1.23456789e10)
        assert large_int.value == 999999999
        assert large_float.value == 1.23456789e10


class TestSet:
    """Unit tests for Set datatype"""

    def test_valid_set(self):
        """Test creating Set with valid set value"""
        set_dt = Set({1, 2, 3})
        assert set_dt.value == {1, 2, 3}

    def test_set_from_list(self):
        """Test Set from list"""
        set_dt = Set([1, 2, 3])
        assert set_dt.value == {1, 2, 3}

    def test_set_from_tuple(self):
        """Test Set from tuple"""
        set_dt = Set((1, 2, 3))
        assert set_dt.value == {1, 2, 3}

    def test_empty_set(self):
        """Test Set with empty set"""
        set_dt = Set(set())
        assert set_dt.value == set()

    def test_set_with_mixed_types(self):
        """Test Set with mixed data types"""
        mixed_data = {1, "string", True, None}
        set_dt = Set(mixed_data)
        assert set_dt.value == mixed_data

    def test_set_from_string(self):
        """Test Set from string (iterable)"""
        set_dt = Set("hello")
        assert set_dt.value == {"h", "e", "l", "o"}

    def test_set_duplicates_removed(self):
        """Test Set removes duplicates"""
        set_dt = Set([1, 1, 2, 2, 3, 3])
        assert set_dt.value == {1, 2, 3}


class TestQueue:
    """Unit tests for Queue datatype"""

    def test_valid_list(self):
        """Test creating Queue with valid list value"""
        queue_dt = Queue([1, 2, 3])
        assert isinstance(queue_dt.value, deque)
        assert list(queue_dt.value) == [1, 2, 3]

    def test_valid_deque(self):
        """Test creating Queue with valid deque value"""
        original_deque = deque([1, 2, 3])
        queue_dt = Queue(original_deque)
        assert queue_dt.value is original_deque
        assert list(queue_dt.value) == [1, 2, 3]

    def test_queue_from_tuple(self):
        """Test Queue from tuple"""
        queue_dt = Queue((1, 2, 3))
        assert isinstance(queue_dt.value, deque)
        assert list(queue_dt.value) == [1, 2, 3]

    def test_empty_queue(self):
        """Test Queue with empty list"""
        queue_dt = Queue([])
        assert isinstance(queue_dt.value, deque)
        assert list(queue_dt.value) == []

    def test_queue_with_mixed_types(self):
        """Test Queue with mixed data types"""
        mixed_data = [1, "string", True, None, {"key": "value"}]
        queue_dt = Queue(mixed_data)
        assert isinstance(queue_dt.value, deque)
        assert list(queue_dt.value) == mixed_data

    def test_queue_from_string(self):
        """Test Queue from string (iterable)"""
        queue_dt = Queue("hello")
        assert isinstance(queue_dt.value, deque)
        assert list(queue_dt.value) == ["h", "e", "l", "l", "o"]

    def test_queue_from_range(self):
        """Test Queue from range object"""
        queue_dt = Queue(range(3))
        assert isinstance(queue_dt.value, deque)
        assert list(queue_dt.value) == [0, 1, 2]


class TestStreams:
    """Unit tests for Streams datatype"""

    def test_valid_list(self):
        """Test creating Streams with valid list value"""
        streams_dt = Streams([1, 2, 3])
        assert streams_dt.value == [1, 2, 3]

    def test_valid_tuple(self):
        """Test creating Streams with valid tuple value"""
        streams_dt = Streams((1, 2, 3))
        assert streams_dt.value == [1, 2, 3]

    def test_valid_dict(self):
        """Test creating Streams with valid dict value"""
        streams_dt = Streams({"a": 1, "b": 2})
        assert streams_dt.value == [("a", 1), ("b", 2)]

    def test_empty_iterables(self):
        """Test Streams with empty iterables"""
        empty_list = Streams([])
        empty_tuple = Streams(())
        empty_dict = Streams({})
        assert empty_list.value == []
        assert empty_tuple.value == []
        assert empty_dict.value == []

    def test_streams_with_mixed_types(self):
        """Test Streams with mixed data types"""
        mixed_data = [1, "string", True, None, {"key": "value"}]
        streams_dt = Streams(mixed_data)
        assert streams_dt.value == mixed_data

    def test_streams_from_string(self):
        """Test Streams from string (iterable)"""
        streams_dt = Streams("hello")
        assert streams_dt.value == ["h", "e", "l", "l", "o"]

    def test_streams_from_range(self):
        """Test Streams from range object"""
        streams_dt = Streams(range(3))
        assert streams_dt.value == [0, 1, 2]

    def test_streams_from_set(self):
        """Test Streams from set (order may vary)"""
        streams_dt = Streams({1, 2, 3})
        # Order may vary with sets
        assert set(streams_dt.value) == {1, 2, 3}

    def test_streams_dict_items_order(self):
        """Test Streams from dict maintains key-value pairs"""
        test_dict = {"x": 10, "y": 20, "z": 30}
        streams_dt = Streams(test_dict)
        # Convert back to dict to check key-value pairs
        result_dict = dict(streams_dt.value)
        assert result_dict == test_dict

    def test_streams_nested_structure(self):
        """Test Streams with nested data structures"""
        nested_data = [{"a": [1, 2]}, {"b": {"c": 3}}]
        streams_dt = Streams(nested_data)
        assert streams_dt.value == nested_data

    def test_streams_get_name(self):
        """Test Streams get_name static method"""
        assert Streams.get_name() == "streams"


class TestDatatypeValidation:
    """Unit tests for datatype validation"""

    def test_list_invalid_type(self):
        """Test List with invalid type raises TypeError"""
        with pytest.raises(
            TypeError,
            match="Invalid type: expected one of \\(Iterable, list, tuple\\), but got",
        ):
            List(123)

    def test_map_invalid_type(self):
        """Test Map with invalid type raises TypeError"""
        with pytest.raises(
            TypeError, match="Invalid type: expected one of \\(dict\\), but got"
        ):
            Map([1, 2, 3])

    def test_numeric_invalid_type(self):
        """Test Numeric with invalid type raises TypeError"""
        with pytest.raises(
            TypeError, match="Invalid type: expected one of \\(int, float\\), but got"
        ):
            Numeric("not a number")

    def test_set_invalid_type(self):
        """Test Set with invalid type raises TypeError"""
        with pytest.raises(
            TypeError,
            match="Invalid type: expected one of \\(Iterable, set, list, tuple\\), but got",
        ):
            Set(123)

    def test_queue_invalid_type(self):
        """Test Queue with invalid type raises TypeError"""
        with pytest.raises(
            TypeError,
            match="Invalid type: expected one of \\(Iterable, list, deque\\), but got",
        ):
            Queue(123)

    def test_streams_invalid_type(self):
        """Test Streams with invalid type raises TypeError"""
        with pytest.raises(
            TypeError,
            match="Invalid type: expected one of \\(Iterable, list, tuple, dict\\), but got",
        ):
            Streams(123)


class TestDatatypeEdgeCases:
    """Unit tests for datatype edge cases"""

    def test_string_none_value(self):
        """Test String with None value"""
        string_dt = String(None)
        assert string_dt.value == "None"

    def test_list_none_value(self):
        """Test List with None value should fail"""
        with pytest.raises(TypeError):
            List(None)

    def test_map_none_value(self):
        """Test Map with None value should fail"""
        with pytest.raises(TypeError):
            Map(None)

    def test_numeric_none_value(self):
        """Test Numeric with None value should fail"""
        with pytest.raises(TypeError):
            Numeric(None)

    def test_set_none_value(self):
        """Test Set with None value should fail"""
        with pytest.raises(TypeError):
            Set(None)

    def test_queue_none_value(self):
        """Test Queue with None value should fail"""
        with pytest.raises(TypeError):
            Queue(None)

    def test_streams_none_value(self):
        """Test Streams with None value should fail"""
        with pytest.raises(TypeError):
            Streams(None)

    def test_string_boolean_conversion(self):
        """Test String with boolean values"""
        true_string = String(True)
        false_string = String(False)
        assert true_string.value == "True"
        assert false_string.value == "False"

    def test_numeric_boolean_values(self):
        """Test Numeric with boolean values should fail"""
        with pytest.raises(TypeError):
            Numeric(True)
        with pytest.raises(TypeError):
            Numeric(False)
