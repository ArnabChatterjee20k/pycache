from src.pycache.snapshot.Writer import Writer
from src.pycache.snapshot.Reader import Reader
from io import BytesIO
from collections import deque

data = {}
for i in range(1):
    capital_letter = 65 + i
    # data[str(capital_letter + 100)] = [capital_letter, capital_letter]
    # data["a"] = [{"a":1},2]
    data[str(capital_letter + 100)] = [
        {"a": {"a": {"b": "1212"}}, "b": 121},
        set([1, 2, 3, 4]),
        [1212, [2, 24]],
        deque([1, 2, 4, "skldjf"]),
    ]
    # data[chr(capital_letter)] = capital_letter
    # data[str(capital_letter+100)] = capital_letter
file_buffer = BytesIO()
Writer(data, file_buffer).save()

file_buffer.seek(0)
print(Reader(file_buffer).load() == data)
