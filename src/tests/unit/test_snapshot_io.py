import pytest
import struct
from io import BytesIO
from collections import deque
from datetime import datetime
from src.pycache.snapshot.Writer import Writer
from src.pycache.snapshot.Reader import Reader
from src.pycache.snapshot.Identifier import (
    DataTypesIdentifier,
    Encoder,
    LengthSizeMarkers,
)


class TestSnapshotIO:
    """Comprehensive tests for Writer and Reader working together"""

    def test_basic_write_read_cycle(self):
        """Test basic write and read cycle with simple data"""
        test_data = {"key1": "value1", "key2": "value2"}
        buffer = BytesIO()

        # Write data
        writer = Writer(test_data, buffer)
        writer.save()

        # Read data back
        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify data integrity
        assert loaded_data == test_data

    def test_length_encoding_write_read(self):
        """Test all length encoding methods work correctly"""
        # Test 6-bit encoding (0-63)
        test_data = {"key": "a" * 42}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

        # Test 14-bit encoding (64-16383)
        test_data = {"key": "a" * 1000}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

        # Test 32-bit encoding (>16383)
        test_data = {"key": "a" * 20000}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

    def test_numeric_encoding_write_read(self):
        """Test numeric encoding and reading"""
        # Test INT8
        test_data = {"key": 100}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

        # Test INT16
        test_data = {"key": 1000}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

        # Test INT32
        test_data = {"key": 100000}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

    def test_compression_write_read(self):
        """Test compression and decompression"""
        # Create data that benefits from compression
        test_data = {"key": "a" * 1000}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

    def test_datetime_write_read(self):
        """Test datetime serialization and deserialization"""
        test_datetime = datetime.now()
        test_data = {"key": test_datetime}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Compare ISO strings to avoid microsecond precision issues
        assert loaded_data["key"].isoformat() == test_datetime.isoformat()

    def test_none_value_write_read(self):
        """Test None value handling"""
        test_data = {"key": None}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data["key"] is None

    def test_sequence_types_write_read(self):
        """Test all sequence types (list, dict, set, deque)"""
        test_data = {
            "list_key": [1, 2, 3],
            "dict_key": {"nested": "value"},
            "set_key": {1, 2, 3},
            "deque_key": deque([1, 2, 3]),
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify all sequence types are preserved
        assert loaded_data["list_key"] == [1, 2, 3]
        assert loaded_data["dict_key"] == {"nested": "value"}
        assert loaded_data["set_key"] == {1, 2, 3}
        assert list(loaded_data["deque_key"]) == [1, 2, 3]

    def test_nested_structures_write_read(self):
        """Test deeply nested structures"""
        test_data = {
            "level1": {"level2": {"level3": {"level4": [1, 2, {"deep": "value"}]}}}
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify nested structure is preserved
        assert loaded_data["level1"]["level2"]["level3"]["level4"][0] == 1
        assert loaded_data["level1"]["level2"]["level3"]["level4"][1] == 2
        assert loaded_data["level1"]["level2"]["level3"]["level4"][2]["deep"] == "value"

    def test_mixed_data_types_write_read(self):
        """Test mixed data types in complex structures"""
        test_data = {
            "string": "hello world",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "boolean_false": False,
            "none": None,
            "datetime": datetime.now(),
            "list": [1, "string", True, False, None],
            "dict": {"key": "value", "nested": {"list": [1, 2, 3], "bool": True}},
            "set": {1, 2, 3},
            "deque": deque([1, 2, 3]),
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify all data types are preserved
        assert loaded_data["string"] == "hello world"
        assert loaded_data["integer"] == 42
        assert loaded_data["float"] == 3.14
        assert loaded_data["boolean"] is True
        assert loaded_data["boolean_false"] is False
        assert loaded_data["none"] is None
        assert loaded_data["datetime"].isoformat() == test_data["datetime"].isoformat()
        assert loaded_data["list"] == [1, "string", True, False, None]
        assert loaded_data["dict"]["key"] == "value"
        assert loaded_data["dict"]["nested"]["list"] == [1, 2, 3]
        assert loaded_data["dict"]["nested"]["bool"] is True
        assert loaded_data["set"] == {1, 2, 3}
        assert list(loaded_data["deque"]) == [1, 2, 3]

    def test_empty_structures_write_read(self):
        """Test empty structures"""
        test_data = {
            "empty_list": [],
            "empty_dict": {},
            "empty_set": set(),
            "empty_deque": deque(),
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify empty structures are preserved
        assert loaded_data["empty_list"] == []
        assert loaded_data["empty_dict"] == {}
        assert loaded_data["empty_set"] == set()
        assert list(loaded_data["empty_deque"]) == []

    def test_large_data_write_read(self):
        """Test large data structures"""
        # Create large data
        large_list = [i for i in range(1000)]
        large_dict = {f"key_{i}": f"value_{i}" for i in range(1000)}

        test_data = {"large_list": large_list, "large_dict": large_dict}

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify large data is preserved
        assert loaded_data["large_list"] == large_list
        assert loaded_data["large_dict"] == large_dict

    def test_eof_handling(self):
        """Test EOF marker handling"""
        test_data = {"key": "value"}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        # Verify EOF marker is present
        buffer.seek(-1, 2)  # Go to last byte
        eof_marker = buffer.read(1)[0]
        assert eof_marker == 0x00  # EOF marker

        # Verify reading works correctly
        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

    def test_error_handling_malformed_data(self):
        """Test error handling with malformed data"""
        buffer = BytesIO()
        # Write invalid data that doesn't match expected format
        buffer.write(bytes([1]))  # Key count
        buffer.write(bytes([0xFF]))  # Invalid object type
        buffer.seek(0)

        reader = Reader(buffer)

        # Should handle gracefully or raise appropriate error
        with pytest.raises(Exception):
            reader.load()

    def test_boundary_values(self):
        """Test boundary values for length encoding"""
        # Test 6-bit boundary
        test_data = {"key": "a" * 63}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

        # Test 14-bit boundary
        test_data = {"key": "a" * 16383}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

        # Test 32-bit boundary
        test_data = {"key": "a" * 16384}
        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()
        assert loaded_data == test_data

    def test_boolean_write_read(self):
        """Test boolean serialization and deserialization"""
        test_data = {
            "true_value": True,
            "false_value": False,
            "mixed": [True, False, "string", 42],
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify boolean values are preserved
        assert loaded_data["true_value"] is True
        assert loaded_data["false_value"] is False
        assert loaded_data["mixed"][0] is True
        assert loaded_data["mixed"][1] is False
        assert loaded_data["mixed"][2] == "string"
        assert loaded_data["mixed"][3] == 42

    def test_boolean_datatype_markers(self):
        """Test that boolean datatype markers are properly identified and handled"""
        from src.pycache.snapshot.Identifier import DataTypesIdentifier

        # Test with explicit boolean values
        test_data = {"bool_true": True, "bool_false": False}

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify boolean values are preserved with correct types
        assert loaded_data["bool_true"] is True
        assert loaded_data["bool_false"] is False
        assert isinstance(loaded_data["bool_true"], bool)
        assert isinstance(loaded_data["bool_false"], bool)

    def test_boolean_edge_cases(self):
        """Test boolean edge cases and conversions"""
        test_data = {
            "true_from_one": 1,
            "false_from_zero": 0,
            "true_from_any_int": 42,
            "false_from_zero_string": "0",
            "true_from_one_string": "1",
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify integer values are preserved as-is
        assert loaded_data["true_from_one"] == 1
        assert loaded_data["false_from_zero"] == 0
        assert loaded_data["true_from_any_int"] == 42
        assert loaded_data["false_from_zero_string"] == "0"
        assert loaded_data["true_from_one_string"] == "1"

    def test_unicode_and_special_characters(self):
        """Test unicode and special characters"""
        test_data = {
            "unicode": "🚀🌟✨",
            "special_chars": "!@#$%^&*()",
            "newlines": "line1\nline2\r\nline3",
            "tabs": "tab1\ttab2\t\ttab3",
        }

        buffer = BytesIO()
        writer = Writer(test_data, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data = reader.load()

        # Verify unicode and special characters are preserved
        assert loaded_data["unicode"] == "🚀🌟✨"
        assert loaded_data["special_chars"] == "!@#$%^&*()"
        assert loaded_data["newlines"] == "line1\nline2\r\nline3"
        assert loaded_data["tabs"] == "tab1\ttab2\t\ttab3"

    def test_multiple_write_read_cycles(self):
        """Test multiple write-read cycles on the same buffer"""
        buffer = BytesIO()

        # First cycle
        test_data1 = {"key1": "value1"}
        writer = Writer(test_data1, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data1 = reader.load()
        assert loaded_data1 == test_data1

        # Second cycle (overwrite)
        buffer.seek(0)
        buffer.truncate(0)
        test_data2 = {"key2": "value2"}
        writer = Writer(test_data2, buffer)
        writer.save()

        buffer.seek(0)
        reader = Reader(buffer)
        loaded_data2 = reader.load()
        assert loaded_data2 == test_data2
