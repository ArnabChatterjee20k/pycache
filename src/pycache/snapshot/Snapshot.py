import os
import asyncio
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from .Writer import Writer
from .Reader import Reader
import multiprocessing


@dataclass
class SnapshotConfig:
    dir: str = "./snapshot"
    min_changes: int = 1
    interval_hours: int = 1
    auto: bool = False
    max_snapshots: int = 4


default_config = SnapshotConfig(min_changes=100, interval_hours=1, auto=False)


class SnapshotManager:
    _datetime_format = "%Y-%m-%d_%H-%M-%S-%f"

    def __init__(self, config: SnapshotConfig = default_config):
        self._config = config
        self._path: Path = None
        self._last_snapshot_time: datetime = datetime.now()
        self._changes = multiprocessing.Value("i", 0)
        self._trigger = multiprocessing.Event()
        self._worker = None
        self.source = {}
        self._init_snapshot_dir()

    def record_change(self, value=1):
        self._changes.value += value
        if self._changes.value >= self._config.min_changes:
            self._trigger.set()

    def _init_snapshot_dir(self):
        path = Path(self._config.dir)
        if not path.exists():
            path.mkdir(parents=True)
        elif not path.is_dir():
            raise TypeError("The path config needs to be a directory")
        self._path = path

    def force_snapshot(self, source: dict):
        now = datetime.now()
        base_filename = now.strftime(self._datetime_format)
        path = self._path / base_filename

        counter = 0
        while path.exists():
            counter += 1
            unique_filename = f"{base_filename}_{counter}"
            path = self._path / unique_filename

        with open(path, "wb") as f:
            Writer(source, f).save()

            f.flush()
            os.fsync(f.fileno())

        with self._changes.get_lock():
            self._changes.value = 0

        # pruning old snapshot
        self.prune_old_snapshots()

    def load_snapshot(self, target_timestamp: str = None):
        """Load snapshot closest to the timestamp"""
        files = list(self._path.glob("*"))
        if not files:
            return {}

        if not target_timestamp:
            snapshot = max(files, key=lambda f: f.stat().st_mtime)
        else:
            # Handle both old and new timestamp formats
            target = datetime.strptime(target_timestamp, self._datetime_format)

            if target:
                snapshot = min(
                    files,
                    key=lambda f: abs(
                        target - self._parse_timestamp_from_filename(f.name)
                    ),
                )
            else:
                snapshot = max(files, key=lambda f: f.stat().st_mtime)

        with open(snapshot, "rb") as f:
            reader = Reader(f)
            data = reader.load()
            return data if data else {}

    def _parse_timestamp_from_filename(self, filename: str) -> datetime:
        return datetime.strptime(filename, self._datetime_format)

    def prune_old_snapshots(self):
        snapshots = list(self._path.glob("*"))
        if len(snapshots) > self._config.max_snapshots:
            # sort -> moodification time, name(datetime format)
            snapshots.sort(key=lambda f: (f.stat().st_mtime, f.name), reverse=True)
            for old_file in snapshots[self._config.max_snapshots :]:
                try:
                    old_file.unlink()
                except Exception as e:
                    print(f"Warning: Could not delete old snapshot {old_file}: {e}")

    def _run(self, source: dict):
        interval = timedelta(hours=self._config.interval_hours)
        while True:
            # wake every minutes
            triggered = self._trigger.wait(1)
            now = datetime.now()
            time_passed = now - self._last_snapshot_time

            if triggered:
                self._trigger.clear()

            with self._changes.get_lock():
                changes = self._changes.value

            # Case 1: enough changes -> snapshot
            if changes >= self._config.min_changes:
                self.force_snapshot(source)

            # Case 2: time passed + at least one change -> snapshot
            elif time_passed >= interval and self._changes.value > 0:
                self.force_snapshot(source)

    def start(self, source: dict):
        # starting the copy on write functionality(not for windows)
        self.source = source

        if self._config.auto:
            self._worker = multiprocessing.Process(
                target=self._run, args=(source,), daemon=True
            )
            self._worker.start()
        else:
            self._worker = None

    def stop(self):
        if hasattr(self, "source") and self.source:
            self.force_snapshot(self.source)
        if self._worker:
            self._worker.terminate()
            self._worker.join()

    def is_processing(self) -> bool:
        if self._worker is None:
            return False
        return self._worker.is_alive()

    def is_enabled(self) -> bool:
        return self._config.auto or self._config.auto
