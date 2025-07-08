from contextlib import contextmanager
from .adapters.Adapter import Adapter
from .datatypes.Datatype import Datatype


class PyCache:
    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    def __enter__(self):
        self.adapter.connect()
        return self

    def __exit__(self):
        self.adapter.close()

    def set(self, key, value: Datatype):
        if not isinstance(value, Datatype):
            return TypeError("Value is not an instance of Datatype")
        return self.adapter.set(key, value.value)

    def get(self, key):
        value = self.adapter.get(key)
        return value if value else value

    @contextmanager
    def session(self):
        try:
            self.adapter.connect()
            self.adapter.create()
            yield self
        finally:
            self.adapter.close()
