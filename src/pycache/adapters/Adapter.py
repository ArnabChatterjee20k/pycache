from abc import ABC, abstractmethod


class Adapter(ABC):
    def __init__(self):
        self._tablename = "kv-store"

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
    def set(self, key: str, value: bytes) -> None:
        pass

    @abstractmethod
    def batch_get(self, key: str) -> bytes:
        pass

    @abstractmethod
    def batch_set(self, key: str, value: bytes) -> None:
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
