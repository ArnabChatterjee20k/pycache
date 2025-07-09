import asyncio
from contextlib import asynccontextmanager
from .adapters.Adapter import Adapter
from .datatypes.Datatype import Datatype


class PyCache:
    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    async def __aenter__(self):
        await asyncio.to_thread(self.adapter.connect)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.to_thread(self.adapter.close)

    async def set(self, key, value: Datatype | dict[str, Datatype]):
        if isinstance(value, Datatype):
            return await asyncio.to_thread(self.adapter.set, key, value.value)

        if isinstance(value, dict):
            return await asyncio.to_thread(self.adapter.batch_set, key, value)

        raise TypeError("Value is not an instance of dictionary of string and Datatype")

    async def batch_set(self, value: dict[str, Datatype]):
        if isinstance(value, dict):
            return await asyncio.to_thread(self.adapter.batch_set, value)

        raise TypeError("Value is not an instance of dictionary of string and Datatype")

    async def batch_get(self, keys: list[str]):
        if isinstance(keys, list):
            return await asyncio.to_thread(self.adapter.batch_get, keys)

        raise TypeError("keys must be a list of string")

    async def get(self, key):
        value = await asyncio.to_thread(self.adapter.get, key)
        return value if value else value

    async def delete(self, key):
        return await asyncio.to_thread(self.adapter.delete, key)

    async def exists(self, key):
        return await asyncio.to_thread(self.adapter.exists, key)

    async def keys(self):
        return await asyncio.to_thread(self.adapter.keys)

    async def set_expire(self, key, expires_at):
        return await asyncio.to_thread(self.adapter.set_expire, key, expires_at)

    @asynccontextmanager
    async def session(self):
        try:
            await asyncio.to_thread(self.adapter.connect)
            await asyncio.to_thread(self.adapter.create)
            yield self
        finally:
            await asyncio.to_thread(self.adapter.close)
