import multiprocessing
import os
import struct
from pathlib import Path
from io import BytesIO
from collections import deque
from ..compressor import compress
from .Identifier import DataTypesIdentifier,TYPE_TO_DataTypeIdentifer,DataTypeIdentifer_TO_TYPE, Encoder, LengthSizeMarkers

class Writer:
    def __init__(self,source:dict,path:str):
        self.source = source
        self.path = Path(path)
        # self.buffer = BytesIO()
        self.buffer = open("arnab.txt","wb")

    def save(self):
        # key count
        self._write_length(len(self.source))
        for key,value in self.source.items():
            self._write_key_value(key,value)

    def _write_length(self,length):
        """
            returns bytes of data written
        """
        if length < LengthSizeMarkers.SIX_BIT_ENCODING.value:
            self.buffer.write(bytes([length]))
            return 1

        elif length < LengthSizeMarkers.FORTEEN_BIT_ENCODING.value:
            # bytes([64 | ((length>>8)) , length & 0xFF])
            # right shift to get 8bits
            first_byte = length >> 8
            # adding prefix(01) to the byte
            # 64 in 8bit format starts with 01 and first byte is bit
            # and '|' will add the prefix
            first_byte = 64 | first_byte
            # masking the first 6bits to get the next byte
            second_byte = length & 0xFF

            self.buffer.write(bytes([first_byte, second_byte]))
            return 2
            
        else:
            self.buffer.write(bytes([0x80]))
            self.buffer.write(struct.pack("<I",length))
            return 5

    def _write_key_value(self,key,value):
        object_type = TYPE_TO_DataTypeIdentifer[type(value)]
        self.buffer.write(bytes([object_type.value]))

        key_length = len(key)
        self._write_length(key_length)
        self._write_string(key.encode())
        self._write_string(str(value).encode())

    
    def _write_encoding(self,encoding:Encoder):
        # all encoding will have a prefix of 11
        byte = 3<<6 | encoding.value
        self.buffer.write(bytes([byte]))

    def _write_string(self,value:bytes)->int:
        # both numeric and strings will be handling
        # compression and integer encoding
        is_numeric = value.decode().isnumeric()
        original_length = len(value)
        # 1byte encoding + rest for the bytes
        if original_length<=11 and is_numeric:
            value = int(value.decode('ascii'))
            if -128 <= value <= 127:
                self._write_encoding(Encoder.INT8)
                self.buffer.write(struct.pack('<b', value))
                return 2
            elif -32768 <= value <= 32767:
                self._write_encoding(Encoder.INT16)
                self.buffer.write(struct.pack('<h', value))
                return 3
            elif -2147483648 <= value <= 2147483647:
                self._write_encoding(Encoder.INT32)
                self.buffer.write(struct.pack('<i', value))
                return 5

        # try compression
        compressed_data = compress(value)
        compressed_length = len(compressed_data)
        if compressed_length < original_length:
            self._write_encoding(Encoder.COMPRESSED)
            return sum(
                        (
                        self._write_length(compressed_length),
                        self.buffer.write(compressed_data))
                    )

        # # store directly(no compression marker)
        return sum(
                    (self._write_length(original_length),
                    self.buffer.write(value))
                )