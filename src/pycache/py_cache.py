from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from .adapters.Adapter import Adapter
from .datatypes.Datatype import Datatype
from .worker.TTLWorker import TTLWorker


class Session:
    def __init__(self, adapter: Adapter):
        self._adapter = adapter

    async def __aenter__(self):
        await self._adapter.connect()
        await self._adapter.create()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._adapter.close()

    async def set(self, key, value: Datatype | dict[str, Datatype]) -> int:
        if isinstance(value, Datatype):
            if self._adapter.get_support_datatype_serializer():
                return await self._adapter.set(key, value.value)
            else:
                # Pass the full datatype object for adapters that don't support serialization
                return await self._adapter.set(key, value)

        if isinstance(value, dict):
            return await self._adapter.batch_set(value)

        raise TypeError("Value must be a Datatype or a dictionary of string → Datatype")

    async def batch_set(self, value: dict[str, Datatype]) -> int:
        if isinstance(value, dict):
            return await self._adapter.batch_set(value)

        raise TypeError("Value must be a dictionary of string → Datatype")

    async def batch_get(self, keys: list[str]) -> dict:
        if not isinstance(keys, list):
            raise TypeError("keys must be a list of strings")

    async def batch_get(self, keys, datatype: str = None) -> dict:
        if isinstance(keys, list):
            # List of strings - uniform datatype
            return await self._adapter.batch_get(keys, datatype)
        elif isinstance(keys, dict):
            # Dict mapping keys to datatypes - mixed datatypes
            return await self._adapter.batch_get(keys)
        else:
            raise TypeError("keys must be either a list of strings or a dict mapping keys to datatype names")

    async def get(self, key, expected_datatype: Datatype = None):
        if not self._adapter.get_support_datatype_serializer() and expected_datatype is None:
            raise ValueError("Adapter doesn't support datatype serialization. You must provide expected_datatype parameter.")
        
        return await self._adapter.get(key, expected_datatype)

    async def delete(self, key) -> int:
        return await self._adapter.delete(key)

    async def exists(self, key) -> bool:
        return await self._adapter.exists(key)

    async def keys(self) -> list:
        return await self._adapter.keys()

    async def set_expire(self, key, ttl) -> int:
        if not isinstance(ttl, (float, int)):
            raise TypeError("ttl must be numeric")

        if ttl < 1:
            raise ValueError("ttl must be atleast 1second")

        return await self._adapter.set_expire(key, ttl)

    async def get_expire(self, key) -> int:
        return await self._adapter.get_expire(key)

    @asynccontextmanager
    async def with_transaction(self):
        if not self._adapter.get_support_transactions():
            raise NotImplementedError("This adapter does not support transactions")

        adapter: Adapter = self._adapter
        await adapter.begin()
        try:
            yield Session(adapter)
            await adapter.commit()
        except:
            await adapter.rollback()


class PyCache:
    def __init__(self, adapter: Adapter):
        self._adapter = adapter
        self._ttl_worker = None

    @asynccontextmanager
    async def session(self):
        try:
            adapter = await self._adapter.connect()
            await adapter.create()
            await adapter.create_index()
            yield Session(adapter)
        finally:
            await adapter.close()

    async def start_ttl_deletion(self, delete_interval=0.5):
        if delete_interval <= 0:
            return
        self._ttl_worker = TTLWorker(
            self._adapter.delete_expired_attributes, delete_interval
        )
        await self._ttl_worker.start()

    async def stop_ttl_deletion(self):
        if self._ttl_worker:
            await self._ttl_worker.stop()

    def get_all_keys_with_expiry(self):
        return self._adapter.get_all_keys_with_expiry()

    def count_expired_keys(self) -> int:
        return self._adapter.count_expired_keys()

    def get_all_keys_with_expiry(self) -> list[tuple[str, str]]:
        return self._adapter.get_all_keys_with_expiry()

    def get_support_transactions(self) -> bool:
        """Check if the adapter supports transactions."""
        return self._adapter.get_support_transactions()

    def delete_expired_attributes(self):
        return self._adapter.delete_expired_attributes()
