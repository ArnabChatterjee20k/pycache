import multiprocessing
import os
import struct
from pathlib import Path
from io import BytesIO
from typing import BinaryIO
from collections import deque
from ..compressor import decompress
from .Identifier import (
    DataTypesIdentifier,
    SequenceTypes,
    DataTypeIdentifer_TO_TYPE,
    TYPE_TO_DataTypeIdentifer,
    Encoder,
    LengthSizeMarkers,
)


class Reader:
    def __init__(self, source_buffer: BinaryIO):
        self.buffer = source_buffer
        self.data = {}

    def load(self):
        keys = self._read_length()
        for _ in range(keys):
            key, value = self._read_key_value()
            self.data[key] = value
        return self.data

    def _read_length(self):
        # reading a byte of length 1
        length = self.buffer.read(1)
        marker = length[0]

        if marker <= LengthSizeMarkers.SIX_BIT_ENCODING.value:
            return marker

        first_byte = marker & 0x3F
        if marker <= LengthSizeMarkers.FORTEEN_BIT_ENCODING.value:
            # (buffer[0] & 0x3F) << 8 | buffer[1]
            second_byte = self.buffer.read(1)[0]
            # 0x3F gives the last 6bits excluding the marker
            return (first_byte << 8) | second_byte

        if marker == LengthSizeMarkers.THIRTY_TWO_BIT_ENCODING.value:
            return struct.unpack("<I", self.buffer.read(4))[0]

        raise ValueError(f"Unknown encoding marker: {marker}")

    def _read_encoding(self):
        first_byte = self.buffer.read(1)[0]
        # the top 2 bits(11) due to 3<<6
        if first_byte >> 6 == 3:
            return Encoder(first_byte & 0x3F)
        # otherwise losing the lost byte
        self.buffer.seek(-1, 1)
        return None

    def _read_value(self, key_length=None):
        encoding = self._read_encoding()

        if encoding == Encoder.INT8:
            return struct.unpack("<b", self.buffer.read(1))[0]

        elif encoding == Encoder.INT16:
            return struct.unpack("<h", self.buffer.read(2))[0]

        elif encoding == Encoder.INT32:
            return struct.unpack("<i", self.buffer.read(4))[0]

        elif encoding == Encoder.COMPRESSED:
            comp_len = self._read_length()
            data = self.buffer.read(comp_len)
            return decompress(data).decode()

        else:
            original_length = self._read_length()
            return self.buffer.read(original_length).decode("utf-8")

    def _read_key_value(self):
        object_type = self.buffer.read(1)[0]
        value_datatype = DataTypeIdentifer_TO_TYPE[DataTypesIdentifier(object_type)]
        key = self._read_value()

        if TYPE_TO_DataTypeIdentifer[value_datatype] in SequenceTypes:
            sequences = []
            size = self._read_length()
            for _ in range(size):
                entry_type_byte = self.buffer.read(1)[0]
                entry_type = DataTypeIdentifer_TO_TYPE[
                    DataTypesIdentifier(entry_type_byte)
                ]
                entry = self._read_value()
                sequences.append(entry_type(entry))
            value = value_datatype(sequences)  # Construct the proper sequence type
        else:
            value = value_datatype(self._read_value())

        # keys are always string format
        return str(key), value
