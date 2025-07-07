from contextlib import contextmanager
from .adapters.Adapter import Adapter


class PyCache:
    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    def __enter__(self):
        self.adapter.connect()
        return self

    def __exit__(self):
        self.adapter.close()

    def set(self, key, value):
        return self.adapter.set(key, self.adapter.to_bytes(value))

    def get(self, key):
        value = self.adapter.get(key)
        return self.adapter.to_value(value) if value else value

    @contextmanager
    def session(self):
        try:
            self.adapter.connect()
            self.adapter.create()
            yield self
        finally:
            self.adapter.close()
