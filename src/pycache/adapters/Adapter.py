from ..datatypes.Datatype import Datatype
from abc import ABC, abstractmethod
import pickle


class Adapter(ABC):
    def __init__(self, connection_uri, tablename="kv-store"):
        self._tablename = tablename
        self._connection_uri = connection_uri
        self._db = None

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def create_index(self):
        pass

    @abstractmethod
    def get(self, key: str) -> bytes:
        pass

    @abstractmethod
    def set(self, key: str, value) -> None:
        pass

    @abstractmethod
    def batch_get(self, keys: list[str]) -> bytes:
        pass

    @abstractmethod
    def batch_set(self, key_values: dict[str, Datatype]) -> None:
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def keys(self) -> list:
        pass

    @abstractmethod
    def set_expire(self):
        pass

    def to_bytes(self, data):
        return pickle.dumps(data)

    def to_value(self, data):
        return pickle.loads(data)
