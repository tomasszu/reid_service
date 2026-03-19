from abc import ABC, abstractmethod
from typing import Iterable

class StorageBackend(ABC):
    @abstractmethod
    def list_objects(self, prefix: str = "") -> Iterable[str]:
        pass

    @abstractmethod
    def get_object(self, key: str) -> bytes:
        pass

    @abstractmethod
    def bucket_exists(self) -> bool:
        pass