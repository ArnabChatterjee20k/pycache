import multiprocessing
import os
import struct
from pathlib import Path
from io import BytesIO
from collections import deque
from ..compressor import compress
from .Identifier import DataTypesIdentifier,TYPE_TO_DataTypeIdentifer,DataTypeIdentifer_TO_TYPE, Encoder, LengthSizeMarkers

class Reader:
    def __init__(self,path:str):
        self.buffer = open("arnab.txt","rb")
    
    def load(self):
        self._read_length()

    def _read_length():
        pass

    def _read_encoding():
        pass

    def _read_string():
        pass