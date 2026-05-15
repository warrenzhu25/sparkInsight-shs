from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    @abstractmethod
    def list_eventlogs(self) -> list[Path]:
        raise NotImplementedError

    @abstractmethod
    def read_eventlog(self, path: str) -> Path:
        raise NotImplementedError
