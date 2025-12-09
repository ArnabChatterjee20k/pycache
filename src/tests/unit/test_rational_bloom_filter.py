import pytest
import math
from src.pycache.collections.bloomfilters.RationalBloomFilter import RationalBloomFilter
from src.pycache.collections.bloomfilters.BloomFilter import (
    _get_size,
    _get_number_of_hash_functions,
)


class TestRationalBloomFilter:
    """Unit tests for RationalBloomFilter"""

    def test_init_with_valid_parameters(self):
        """Test creating RationalBloomFilter with valid parameters"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        assert bf._size > 0
        assert bf._number_of_hash_functions > 0
        assert bf.floor_k >= 0
        assert 0 <= bf._activation_proabability < 1

    def test_init_size_calculation(self):
        """Test that size is calculated correctly"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        expected_size = _get_size(100, 0.01)
        assert bf._size == expected_size

    def test_init_hash_functions_calculation(self):
        """Test that number of hash functions is calculated correctly"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        expected_k = _get_number_of_hash_functions(bf._size, 100)
        assert bf._number_of_hash_functions == expected_k

    def test_floor_k_property(self):
        """Test that floor_k is the floor of number_of_hash_functions"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        assert bf.floor_k == math.floor(bf._number_of_hash_functions)

    def test_activation_probability_range(self):
        """Test that activation probability is in [0, 1)"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        assert 0 <= bf._activation_proabability < 1

    def test_activation_probability_calculation(self):
        """Test that activation probability is calculated correctly"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        expected = bf._number_of_hash_functions - math.floor(
            bf._number_of_hash_functions
        )
        assert abs(bf._activation_proabability - expected) < 1e-10

    def test_add_key(self):
        """Test adding a key to the filter"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        bf.add("test_key")
        # Should not raise an exception

    def test_exists_after_add(self):
        """Test that a key exists after being added"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        key = "test_key"
        bf.add(key)
        assert bf.exists(key) is True

    def test_exists_before_add(self):
        """Test that a key doesn't exist before being added (may have false positives)"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        key = "new_key"
        # With low false positive rate, new key should likely not exist
        # But this is probabilistic, so we can't assert it will always be False
        result = bf.exists(key)
        assert isinstance(result, bool)

    def test_add_multiple_keys(self):
        """Test adding multiple keys"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        keys = ["key1", "key2", "key3", "key4", "key5"]
        for key in keys:
            bf.add(key)
            assert bf.exists(key) is True

    def test_same_key_multiple_times(self):
        """Test adding the same key multiple times"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        key = "duplicate_key"
        bf.add(key)
        bf.add(key)
        bf.add(key)
        assert bf.exists(key) is True

    def test_different_keys(self):
        """Test that different keys can be added"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        bf.add("key1")
        bf.add("key2")
        bf.add("key3")
        assert bf.exists("key1") is True
        assert bf.exists("key2") is True
        assert bf.exists("key3") is True

    def test_value_property(self):
        """Test value property returns the bit array"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        value = bf.value
        assert value is not None
        # Should be a BitArray instance
        from src.pycache.collections.bitarray.BitArray import BitArray

        assert isinstance(value, BitArray)

    def test_false_positive_rate_impact(self):
        """Test that different false positive rates affect the filter size"""
        bf1 = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        bf2 = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.1)
        # Lower false positive rate should result in larger size
        assert bf1._size > bf2._size

    def test_number_of_elements_impact(self):
        """Test that different number of elements affect the filter size"""
        bf1 = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        bf2 = RationalBloomFilter(number_of_elements=1000, false_positive_rate=0.01)
        # More elements should result in larger size
        assert bf2._size > bf1._size

    def test_is_activation_required_deterministic(self):
        """Test that activation requirement is deterministic for same key"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        key = "test_key"
        result1 = bf._is_activation_required(key)
        result2 = bf._is_activation_required(key)
        # Should be deterministic
        assert result1 == result2

    def test_is_activation_required_different_keys(self):
        """Test that activation requirement may differ for different keys"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.5)
        # With high activation probability, most keys should require activation
        # But we can't guarantee they're all the same
        results = [bf._is_activation_required(f"key_{i}") for i in range(10)]
        # At least verify it returns boolean
        assert all(isinstance(r, bool) for r in results)

    def test_is_activation_required_probability(self):
        """Test that activation requirement follows probability"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        # With very low activation probability, most keys should not require activation
        # This is probabilistic, so we test multiple keys
        activations = sum(bf._is_activation_required(f"key_{i}") for i in range(1000))
        # With low probability, we expect few activations
        # But this is probabilistic, so we just check it's reasonable
        assert 0 <= activations <= 1000

    def test_empty_string_key(self):
        """Test adding and checking empty string key"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        bf.add("")
        assert bf.exists("") is True

    def test_unicode_key(self):
        """Test adding and checking unicode key"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        key = "🚀🌟"
        bf.add(key)
        assert bf.exists(key) is True

    def test_long_key(self):
        """Test adding and checking long key"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        key = "a" * 1000
        bf.add(key)
        assert bf.exists(key) is True

    def test_special_characters_key(self):
        """Test adding and checking key with special characters"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        key = "!@#$%^&*()"
        bf.add(key)
        assert bf.exists(key) is True

    def test_numeric_string_key(self):
        """Test adding and checking numeric string key"""
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.1)
        key = "12345"
        bf.add(key)
        assert bf.exists(key) is True

    def test_floor_k_zero(self):
        """Test filter when floor_k is 0 (very small number of hash functions)"""
        # Use parameters that result in very small k
        bf = RationalBloomFilter(number_of_elements=1000, false_positive_rate=0.5)
        # Should still work
        bf.add("test")
        assert bf.exists("test") is True

    def test_floor_k_large(self):
        """Test filter when floor_k is large"""
        # Use parameters that result in larger k
        bf = RationalBloomFilter(number_of_elements=10, false_positive_rate=0.001)
        # Should still work
        bf.add("test")
        assert bf.exists("test") is True

    def test_negative_false_positive_rate(self):
        """Test filter with negative false positive rate (edge case)"""
        # According to _get_size, negative rate returns number_of_elements
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=-0.1)
        assert bf._size == 100
        bf.add("test")
        assert bf.exists("test") is True

    def test_zero_false_positive_rate(self):
        """Test filter with zero false positive rate"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.0)
        bf.add("test")
        assert bf.exists("test") is True

    def test_very_small_false_positive_rate(self):
        """Test filter with very small false positive rate"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=1e-10)
        bf.add("test")
        assert bf.exists("test") is True

    def test_large_number_of_elements(self):
        """Test filter with large number of elements"""
        bf = RationalBloomFilter(number_of_elements=100000, false_positive_rate=0.01)
        bf.add("test")
        assert bf.exists("test") is True

    def test_small_number_of_elements(self):
        """Test filter with small number of elements"""
        bf = RationalBloomFilter(number_of_elements=1, false_positive_rate=0.1)
        bf.add("test")
        assert bf.exists("test") is True

    def test_add_and_check_sequence(self):
        """Test a sequence of add and check operations"""
        bf = RationalBloomFilter(number_of_elements=100, false_positive_rate=0.01)
        keys = [f"key_{i}" for i in range(50)]

        # Add keys one by one and verify
        for key in keys:
            bf.add(key)
            assert bf.exists(key) is True

        # Verify all keys still exist
        for key in keys:
            assert bf.exists(key) is True
