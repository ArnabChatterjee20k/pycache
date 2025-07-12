from ..datatypes.Datatype import Datatype
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
import pickle


class Adapter(ABC):
    def __init__(self, connection_uri, tablename="kv-store"):
        self._tablename = tablename
        self._connection_uri = connection_uri
        self._db = None

    @abstractmethod
    def connect(self) -> "Adapter":
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def create(self) -> None:
        pass

    @abstractmethod
    def create_index(self) -> None:
        pass

    @abstractmethod
    def get(self, key: str) -> any:
        pass

    @abstractmethod
    def set(self, key: str, value) -> int:
        pass

    @abstractmethod
    def batch_get(self, keys: list[str]) -> list:
        pass

    @abstractmethod
    def batch_set(self, key_values: dict[str, Datatype]) -> int:
        pass

    @abstractmethod
    def delete(self, key: str) -> int:
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def keys(self) -> list:
        pass

    @abstractmethod
    def set_expire(self, ttl) -> int:
        pass

    @abstractmethod
    def get_expire(self, key) -> int | None:
        pass

    @abstractmethod
    async def delete_expired_attributes(self):
        pass

    @abstractmethod
    def get_datetime_format(self) -> str:
        pass

    def to_bytes(self, data):
        return pickle.dumps(data)

    def to_value(self, data):
        return pickle.loads(data)

    def get_expires_at(self, ttl):
        return (datetime.now(timezone.utc) + timedelta(seconds=ttl)).strftime(
            self.get_datetime_format()
        )
