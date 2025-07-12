from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from .adapters.Adapter import Adapter
from .datatypes.Datatype import Datatype
from .worker.TTLWorker import TTLWorker


class PyCache:
    def __init__(self, adapter: Adapter, ttl_interval=-1):
        self.adapter = adapter
        self.ttl = ttl_interval
        self._ttl_worker = None
        if ttl_interval != -1 and ttl_interval <= 0:
            raise ValueError("TTL interval should be more than 0 seconds")
        elif ttl_interval > 0:
            self._ttl_worker = TTLWorker(
                self.adapter.delete_expired_attributes, interval=ttl_interval
            )
            self._ttl_worker.start()

    async def __aenter__(self):
        await self.adapter.connect()
        await self.adapter.create()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.adapter.close()

    async def set(self, key, value: Datatype | dict[str, Datatype]) -> int:
        if isinstance(value, Datatype):
            return await self.adapter.set(key, value.value)

        if isinstance(value, dict):
            return await self.adapter.batch_set(value)

        raise TypeError("Value must be a Datatype or a dictionary of string → Datatype")

    async def batch_set(self, value: dict[str, Datatype]) -> int:
        if isinstance(value, dict):
            return await self.adapter.batch_set(value)

        raise TypeError("Value must be a dictionary of string → Datatype")

    async def batch_get(self, keys: list[str]) -> dict:
        if not isinstance(keys, list):
            raise TypeError("keys must be a list of strings")

        return await self.adapter.batch_get(keys)

    async def get(self, key):
        return await self.adapter.get(key)

    async def delete(self, key) -> int:
        return await self.adapter.delete(key)

    async def exists(self, key) -> bool:
        return await self.adapter.exists(key)

    async def keys(self) -> list:
        return await self.adapter.keys()

    async def set_expire(self, key, ttl) -> int:
        if not isinstance(ttl, (float, int)):
            raise TypeError("ttl must be numeric")

        if ttl < 1:
            raise ValueError("ttl must be atleast 1second")

        return await self.adapter.set_expire(key, ttl)

    async def get_expire(self, key) -> int:
        return await self.adapter.get_expire(key)

    async def stop_ttl(self):
        if self._ttl_worker:
            self._ttl_worker.stop()

    @asynccontextmanager
    async def session(self):
        try:
            await self.adapter.connect()
            await self.adapter.create()
            await self.adapter.create_index()
            yield self
        finally:
            await self.adapter.close()
