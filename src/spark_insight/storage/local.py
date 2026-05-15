from __future__ import annotations

from pathlib import Path

from spark_insight.storage.base import StorageBackend


class LocalStorage(StorageBackend):
    def __init__(self, log_dir: str | Path) -> None:
        self.log_dir = Path(log_dir)

    def list_eventlogs(self) -> list[Path]:
        return [path for path in self.log_dir.glob("**/*") if path.is_file()]

    def read_eventlog(self, path: str) -> Path:
        return Path(path)
