import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from .Adapter import Adapter
from ..datatypes.Datatype import Datatype


class InMemory(Adapter):
    _shared_db: Dict[str, Any] = {}
    _shared_locks: Dict[str, threading.Lock] = {}
    _global_lock = threading.Lock()
    _debug_mode = False

    def __init__(self, connection_uri="", *args):
        super().__init__(connection_uri=connection_uri, *args)
        self._connected = False

    async def connect(self) -> "InMemory":
        self._connected = True
        return self

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        self._connected = False

    def _check_connected(self):
        if not self._connected:
            raise ValueError("Connection is closed")

    def _get_lock(self, key: str) -> threading.Lock:
        with self._global_lock:
            if key not in self._shared_locks:
                self._shared_locks[key] = threading.Lock()
            return self._shared_locks[key]

    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        return entry["expires_at"] and datetime.now(timezone.utc) > entry["expires_at"]

    async def create(self):
        pass

    async def create_index(self):
        pass

    async def get(self, key: str, datatype: Datatype = None) -> Any:
        self._check_connected()
        lock = self._get_lock(key)
        with lock:
            entry = self._shared_db.get(key)
            if entry and not self._is_expired(entry):
                return self.to_value(entry["value"])
            return None

    async def set(self, key: str, value: bytes) -> int:
        self._check_connected()
        lock = self._get_lock(key)
        with lock:
            now = datetime.now(timezone.utc)
            entry = self._shared_db.get(key)
            if entry:
                entry["value"] = value
                entry["created_at"] = now
                if entry["ttl"] is not None:
                    entry["expires_at"] = now + timedelta(seconds=entry["ttl"])
                else:
                    entry["expires_at"] = None
            else:
                self._shared_db[key] = {
                    "value": value,
                    "created_at": now,
                    "expires_at": None,
                    "ttl": None,
                }
            return 1

    async def batch_get(self, keys: list[str],datatype: str = None) -> dict:
        self._check_connected()
        result = {}
        locks = [self._get_lock(k) for k in keys]

        for lock in locks:
            lock.acquire()
        try:
            for key in keys:
                entry = self._shared_db.get(key)
                if entry and not self._is_expired(entry):
                    result[key] = self.to_value(entry["value"])
        finally:
            for lock in reversed(locks):
                lock.release()
        return result

    async def batch_set(self, key_values: dict[str, Datatype]) -> int:
        self._check_connected()
        now = datetime.now(timezone.utc)
        sorted_keys = key_values.keys()
        locks = [self._get_lock(k) for k in sorted_keys]

        for lock in locks:
            lock.acquire()
        count = 0
        try:
            for key in sorted_keys:
                value = key_values[key].value
                entry = self._shared_db.get(key)
                if entry:
                    entry["value"] = value
                    entry["created_at"] = now
                    if entry["ttl"] is not None:
                        entry["expires_at"] = now + timedelta(seconds=entry["ttl"])
                    else:
                        entry["expires_at"] = None
                else:
                    self._shared_db[key] = {
                        "value": value,
                        "created_at": now,
                        "expires_at": None,
                        "ttl": None,
                    }
                count += 1
        finally:
            for lock in reversed(locks):
                lock.release()
        return count

    async def delete(self, key: str) -> int:
        self._check_connected()
        lock = self._get_lock(key)
        with lock:
            if key in self._shared_db:
                del self._shared_db[key]
                return 1
            return 0

    async def exists(self, key: str) -> bool:
        self._check_connected()
        lock = self._get_lock(key)
        with lock:
            entry = self._shared_db.get(key)
            return bool(entry and not self._is_expired(entry))

    async def keys(self) -> list:
        self._check_connected()
        valid_keys = []
        now = datetime.now(timezone.utc)
        with self._global_lock:
            keys_snapshot = list(self._shared_db.keys())
        for key in keys_snapshot:
            lock = self._get_lock(key)
            with lock:
                entry = self._shared_db.get(key)
                if entry and (not entry["expires_at"] or now <= entry["expires_at"]):
                    valid_keys.append(key)
        return valid_keys

    async def set_expire(self, key: str, ttl: int) -> int:
        self._check_connected()
        lock = self._get_lock(key)
        with lock:
            entry = self._shared_db.get(key)
            if entry:
                now = datetime.now(timezone.utc)
                entry["expires_at"] = now + timedelta(seconds=ttl)
                entry["ttl"] = ttl
                return 1
            return 0

    async def get_expire(self, key: str) -> Optional[int]:
        self._check_connected()
        lock = self._get_lock(key)
        with lock:
            entry = self._shared_db.get(key)
            if entry and entry["expires_at"]:
                now = datetime.now(timezone.utc)
                if now > entry["expires_at"]:
                    return None
                return max(0, int((entry["expires_at"] - now).total_seconds()))
            return None

    def delete_expired_attributes(self):
        self._check_connected()
        now = datetime.now(timezone.utc)
        with self._global_lock:
            keys_snapshot = list(self._shared_db.keys())
        for key in keys_snapshot:
            lock = self._get_lock(key)
            with lock:
                entry = self._shared_db.get(key)
                if entry and entry["expires_at"] and now > entry["expires_at"]:
                    del self._shared_db[key]

    def count_expired_keys(self) -> int:
        self._check_connected()
        now = datetime.now(timezone.utc)
        count = 0
        with self._global_lock:
            keys_snapshot = list(self._shared_db.keys())
        for key in keys_snapshot:
            lock = self._get_lock(key)
            with lock:
                entry = self._shared_db.get(key)
                if entry and entry["expires_at"] and now > entry["expires_at"]:
                    count += 1
        return count

    def get_all_keys_with_expiry(self) -> list[tuple[str, str]]:
        self._check_connected()
        result = []
        with self._global_lock:
            keys_snapshot = list(self._shared_db.keys())
        for key in keys_snapshot:
            lock = self._get_lock(key)
            with lock:
                entry = self._shared_db.get(key)
                if entry and entry["expires_at"]:
                    result.append(
                        (key, entry["expires_at"].strftime(self.get_datetime_format()))
                    )
        return result

    def get_datetime_format(self) -> str:
        return "%Y-%m-%d %H:%M:%S"

    def commit(self):
        pass

    def rollback(self):
        pass

    def begin(self):
        pass

    def get_support_transactions(self):
        return False

    def to_bytes(self, data):
        return data

    def to_value(self, data):
        return data
