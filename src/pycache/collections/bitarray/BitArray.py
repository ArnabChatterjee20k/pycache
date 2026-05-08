class BitArray:
    ONE_BYTE = 8

    def __init__(self, size: int):
        """
        :param size: number of bits in the bitarray
        :type size: int
        """
        number_of_bytes = (size + 7) // self.ONE_BYTE
        self.size = size
        self._array = bytearray(number_of_bytes)

    def __getitem__(self, key):
        self._check_bounds(key)
        index = key // self.ONE_BYTE
        byte = self._array[index]
        bit_offset = key % self.ONE_BYTE
        # pusing bit to the LSB and masking the rest to get the bit only
        return (byte >> bit_offset) & 1

    def __setitem__(self, key, value=1):
        self._check_bounds(key)
        # value == 0
        if not value:
            return self.__delitem__(key)
        index = key // self.ONE_BYTE
        byte = self._array[index]
        bit_offset = key % self.ONE_BYTE
        self._array[index] = byte | (1 << bit_offset)

    def __delitem__(self, key):
        self._check_bounds(key)
        index = key // self.ONE_BYTE
        byte = self._array[index]
        bit_offset = key % self.ONE_BYTE
        self._array[index] = byte & ~(1 << bit_offset)

    def __str__(self):
        return str(self._array)

    def __repr__(self):
        return str(self._array)

    def toggle_bit(self, key):
        self._check_bounds(key)
        index = key // self.ONE_BYTE
        byte = self._array[index]
        bit_offset = key % self.ONE_BYTE
        self._array[index] = byte ^ (1 << bit_offset)

    def _check_bounds(self, key):
        if key < 0 or key >= self.size:
            raise IndexError("Bit index out of range")

    @property
    def value(self):
        return self._array

    @value.setter
    def value(self, new_value: bytearray):
        if isinstance(new_value, bytearray):
            raise ValueError("Cannot set value other than bytearray")
        self._array = new_value
