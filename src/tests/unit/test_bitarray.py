import pytest
from src.pycache.collections.bitarray.BitArray import BitArray


class TestBitArray:
    """Unit tests for BitArray"""

    def test_init_with_valid_size(self):
        """Test creating BitArray with valid size"""
        bit_array = BitArray(10)
        assert bit_array.size == 10
        assert isinstance(bit_array.value, bytearray)

    def test_init_with_zero_size(self):
        """Test creating BitArray with zero size"""
        bit_array = BitArray(0)
        assert bit_array.size == 0
        assert len(bit_array.value) == 0

    def test_init_size_calculation(self):
        """Test that BitArray calculates correct number of bytes"""
        # 10 bits should need 2 bytes (10 + 7) // 8 = 2
        bit_array = BitArray(10)
        assert len(bit_array.value) == 2

        # 8 bits should need 1 byte
        bit_array = BitArray(8)
        assert len(bit_array.value) == 1

        # 9 bits should need 2 bytes
        bit_array = BitArray(9)
        assert len(bit_array.value) == 2

    def test_getitem_unset_bit(self):
        """Test getting an unset bit returns 0"""
        bit_array = BitArray(10)
        assert bit_array[0] == 0
        assert bit_array[5] == 0
        assert bit_array[9] == 0

    def test_setitem_and_getitem(self):
        """Test setting a bit and then getting it"""
        bit_array = BitArray(10)
        bit_array[0] = 1
        assert bit_array[0] == 1

        bit_array[5] = 1
        assert bit_array[5] == 1

        bit_array[9] = 1
        assert bit_array[9] == 1

    def test_setitem_with_value_one(self):
        """Test setting bit with explicit value=1"""
        bit_array = BitArray(10)
        bit_array[3] = 1
        assert bit_array[3] == 1

    def test_setitem_with_value_zero(self):
        """Test setting bit to 0 clears the bit"""
        bit_array = BitArray(10)
        bit_array[5] = 1
        assert bit_array[5] == 1
        bit_array[5] = 0
        assert bit_array[5] == 0

    def test_setitem_with_truthy_value(self):
        """Test setting bit with truthy values sets the bit"""
        bit_array = BitArray(10)
        bit_array[2] = True
        assert bit_array[2] == 1

        bit_array[3] = 5  # non-zero number
        assert bit_array[3] == 1

    def test_setitem_with_falsy_value(self):
        """Test setting bit with falsy values clears the bit"""
        bit_array = BitArray(10)
        bit_array[2] = 1
        assert bit_array[2] == 1

        bit_array[2] = False
        assert bit_array[2] == 0

        bit_array[3] = 1
        bit_array[3] = 0
        assert bit_array[3] == 0

    def test_delitem_clears_bit(self):
        """Test deleting a bit clears it"""
        bit_array = BitArray(10)
        bit_array[5] = 1
        assert bit_array[5] == 1

        del bit_array[5]
        assert bit_array[5] == 0

    def test_multiple_bits_same_byte(self):
        """Test setting multiple bits in the same byte"""
        bit_array = BitArray(10)
        bit_array[0] = 1
        bit_array[1] = 1
        bit_array[2] = 1
        assert bit_array[0] == 1
        assert bit_array[1] == 1
        assert bit_array[2] == 1

    def test_multiple_bits_different_bytes(self):
        """Test setting bits across different bytes"""
        bit_array = BitArray(20)
        bit_array[0] = 1
        bit_array[8] = 1
        bit_array[16] = 1
        assert bit_array[0] == 1
        assert bit_array[8] == 1
        assert bit_array[16] == 1

    def test_toggle_bit(self):
        """Test toggling a bit"""
        bit_array = BitArray(10)
        # Toggle from 0 to 1
        bit_array.toggle_bit(5)
        assert bit_array[5] == 1

        # Toggle from 1 to 0
        bit_array.toggle_bit(5)
        assert bit_array[5] == 0

    def test_toggle_bit_multiple_times(self):
        """Test toggling a bit multiple times"""
        bit_array = BitArray(10)
        bit_array.toggle_bit(3)
        assert bit_array[3] == 1
        bit_array.toggle_bit(3)
        assert bit_array[3] == 0
        bit_array.toggle_bit(3)
        assert bit_array[3] == 1

    def test_getitem_index_error_negative(self):
        """Test getting bit with negative index raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            _ = bit_array[-1]

    def test_getitem_index_error_too_large(self):
        """Test getting bit with index >= size raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            _ = bit_array[10]

        with pytest.raises(IndexError, match="Bit index out of range"):
            _ = bit_array[100]

    def test_setitem_index_error_negative(self):
        """Test setting bit with negative index raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            bit_array[-1] = 1

    def test_setitem_index_error_too_large(self):
        """Test setting bit with index >= size raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            bit_array[10] = 1

    def test_delitem_index_error_negative(self):
        """Test deleting bit with negative index raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            del bit_array[-1]

    def test_delitem_index_error_too_large(self):
        """Test deleting bit with index >= size raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            del bit_array[10]

    def test_toggle_bit_index_error_negative(self):
        """Test toggling bit with negative index raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            bit_array.toggle_bit(-1)

    def test_toggle_bit_index_error_too_large(self):
        """Test toggling bit with index >= size raises IndexError"""
        bit_array = BitArray(10)
        with pytest.raises(IndexError, match="Bit index out of range"):
            bit_array.toggle_bit(10)

    def test_value_property(self):
        """Test value property returns the underlying bytearray"""
        bit_array = BitArray(10)
        value = bit_array.value
        assert isinstance(value, bytearray)
        assert value is bit_array._array

    def test_str_repr(self):
        """Test string representation"""
        bit_array = BitArray(10)
        # Just check that str and repr don't crash
        str_repr = str(bit_array)
        repr_repr = repr(bit_array)
        assert isinstance(str_repr, str)
        assert isinstance(repr_repr, str)

    def test_all_bits_in_range(self):
        """Test setting all bits in the array"""
        size = 16
        bit_array = BitArray(size)
        for i in range(size):
            bit_array[i] = 1
            assert bit_array[i] == 1

    def test_clear_all_bits(self):
        """Test clearing all bits"""
        size = 16
        bit_array = BitArray(size)
        # Set all bits
        for i in range(size):
            bit_array[i] = 1
        # Clear all bits
        for i in range(size):
            bit_array[i] = 0
            assert bit_array[i] == 0

    def test_alternating_bits(self):
        """Test setting alternating bits"""
        size = 10
        bit_array = BitArray(size)
        for i in range(0, size, 2):
            bit_array[i] = 1
        for i in range(size):
            expected = 1 if i % 2 == 0 else 0
            assert bit_array[i] == expected

    def test_large_size(self):
        """Test BitArray with large size"""
        size = 1000
        bit_array = BitArray(size)
        assert bit_array.size == size
        # Should be able to set and get bits
        bit_array[0] = 1
        bit_array[size - 1] = 1
        assert bit_array[0] == 1
        assert bit_array[size - 1] == 1
