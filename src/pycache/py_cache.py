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

    def set(self, key, value: Datatype | dict[str, Datatype]):
        if isinstance(value, Datatype):
            return self.adapter.set(key, value.value)

        if isinstance(value, dict):
            return self.adapter.batch_set(key, value)

        raise TypeError("Value is not an instance of dictionary of string and Datatype")

    def batch_set(self, value: dict[str, Datatype]):
        if isinstance(value, dict):
            return self.adapter.batch_set(value)

        raise TypeError("Value is not an instance of dictionary of string and Datatype")

    def batch_get(self, keys: list[str]):
        if isinstance(keys, list):
            return self.adapter.batch_get(keys)

        raise TypeError("keys must be a list of string")

    def get(self, key):
        value = self.adapter.get(key)
        return value if value else value

    def delete(self, key):
        return self.adapter.delete(key)

    def exists(self, key):
        return self.adapter.exists(key)

    def keys(self):
        return self.adapter.keys()

    def set_expire(self, key, expires_at):
        return self.adapter.set_expire(key, expires_at)

    @contextmanager
    def session(self):
        try:
            self.adapter.connect()
            self.adapter.create()
            yield self
        finally:
            self.adapter.close()
