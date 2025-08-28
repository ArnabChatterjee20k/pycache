import zlib

def compress(data):
    return zlib.compress(data)

def decompress(data):
    return zlib.decompress(data)