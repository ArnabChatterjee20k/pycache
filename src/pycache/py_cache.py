from contextlib import asynccontextmanager
from .adapters.Adapter import Adapter
from .datatypes.Datatype import Datatype


class PyCache:
    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    async def __aenter__(self):
        await self.adapter.connect()
        await self.adapter.create()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.adapter.close()

    async def set(self, key, value: Datatype | dict[str, Datatype]):
        if isinstance(value, Datatype):
            return await self.adapter.set(key, value.value)

        if isinstance(value, dict):
            return await self.adapter.batch_set(value)

        raise TypeError("Value must be a Datatype or a dictionary of string → Datatype")

    async def batch_set(self, value: dict[str, Datatype]):
        if isinstance(value, dict):
            return await self.adapter.batch_set(value)

        raise TypeError("Value must be a dictionary of string → Datatype")

    async def batch_get(self, keys: list[str]):
        if not isinstance(keys, list):
            raise TypeError("keys must be a list of strings")

        return await self.adapter.batch_get(keys)

    async def get(self, key):
        return await self.adapter.get(key)

    async def delete(self, key):
        return await self.adapter.delete(key)

    async def exists(self, key):
        return await self.adapter.exists(key)

    async def keys(self):
        return await self.adapter.keys()

    async def set_expire(self, key, expires_at):
        return await self.adapter.set_expire(key, expires_at)

    @asynccontextmanager
    async def session(self):
        try:
            await self.adapter.connect()
            await self.adapter.create()
            yield self
        finally:
            await self.adapter.close()
