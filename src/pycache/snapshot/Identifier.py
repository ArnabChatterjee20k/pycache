"""
[EXPIRETIME_MS] == 253
[expire_timestamp] => if epxire_time present then it will be present
[OBJECT_TYPE]
[KEY_LENGTH]
[KEY_DATA]
[Compressed Length/Original Length]
[ENCODING_MARKER] | if 3 then read the data for compressed length else the original length
[VALUE_ENCODING/DATA]

# Reading data
Read [OBJECT_TYPE]
       ↓
Read [KEY_LENGTH] [KEY_DATA]
       ↓
Peek next byte
       ↓
   ┌─────────┐
   │ 0xC3?   │
   └─────────┘
       ↓
   ┌───YES──────────NO───┐
   ↓                     ↓
LZF Path:           Regular Path:
- Read compressed_len    - Read value_length
- Read original_len      - Read value_data
- Read compressed_data   - Decode as string
- Decompress
- Return string


# Numbers are also written as string
so used the Encoder to write them to the bytes
mask = 3 << 6 => 3 means 11 => so getting a 8bit
encoding = mask | encoding => gives the value
so we will have something like 11000000, 11000001, 11000010
-> read MSB == 11 => integer encoding => read the LSB
"""

from enum import Enum
from collections import deque


class DataTypesIdentifier(Enum):
    STRING = 1
    MAP = 2
    LIST = 3
    INT = 4
    FLOAT = 5
    SET = 6
    DEQUE = 7


TYPE_TO_DataTypeIdentifer = {
    str: DataTypesIdentifier.STRING,
    dict: DataTypesIdentifier.MAP,
    list: DataTypesIdentifier.LIST,
    int: DataTypesIdentifier.INT,
    float: DataTypesIdentifier.FLOAT,
    set: DataTypesIdentifier.SET,
    deque: DataTypesIdentifier.DEQUE,
}

DataTypeIdentifer_TO_TYPE = {v: k for k, v in TYPE_TO_DataTypeIdentifer.items()}

SequenceTypes = (
    DataTypesIdentifier.MAP,
    DataTypesIdentifier.LIST,
    DataTypesIdentifier.SET,
    DataTypesIdentifier.DEQUE,
)


class Encoder(Enum):
    EXPIRETIME = 253
    INT8 = 0
    INT16 = 1
    INT32 = 2
    COMPRESSED = 3


# Variable length encoding for length extraction(Limit markers)
"""
00 for 6-bit (length in lower 6 bits). => 0x00 to 0x3F | 00 is the prefix
01 for 14-bit (length split across two bytes). => 0x40 to 0x7F | prefix added => 01
10 for 32-bit (length in next four bytes). => 0x80(prefix) and actually 5 bytes present. First byte represents its 32 bit and next 4bytes represents the value
"""


class LengthSizeMarkers(Enum):
    SIX_BIT_ENCODING = 63
    FORTEEN_BIT_ENCODING = 127
    THIRTY_TWO_BIT_ENCODING = 0x80  # 128
