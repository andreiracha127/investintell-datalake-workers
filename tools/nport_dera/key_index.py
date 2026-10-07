"""Exact disk-backed conflict keys for large N-PORT CSV plans.

The SQLite B-tree holds the keys, while its page cache and transfer batches
stay bounded. The database is private temporary scratch data: journaling and
durability are unnecessary, and closing the index removes its files.
"""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, MutableSet
from itertools import islice
from pathlib import Path

Key = tuple[str, str, str]
_BATCH_SIZE = 10_000
_INSERT = "INSERT OR IGNORE INTO keys VALUES (?, ?, ?)"


def _key(value: object) -> Key:
    if not isinstance(value, tuple) or len(value) != 3 or not all(isinstance(part, str) for part in value):
        raise TypeError("a conflict key must be a tuple of three strings")
    return value


class KeyIndex(MutableSet[Key]):
    """A temporary exact set of ``(report_date, series_id, cusip)`` keys.

    Use ``close()`` or a context manager for deterministic cleanup. Values are
    compared byte-for-byte by SQLite's binary text collation, without hashing,
    case folding or Unicode normalization. Each index belongs to its creating
    thread, as the CSV planning passes do.
    """

    def __init__(self, values: Iterable[Key] = ()) -> None:
        self._directory = tempfile.TemporaryDirectory(prefix="nport-keys-")
        self.path = Path(self._directory.name) / "keys.sqlite"
        self._db: sqlite3.Connection | None = None
        self._size = 0
        self._pending = 0
        try:
            self._db = sqlite3.connect(self.path)
            self._db.execute("PRAGMA journal_mode = OFF")
            self._db.execute("PRAGMA synchronous = OFF")
            self._db.execute("PRAGMA cache_size = -2048")
            self._db.execute("PRAGMA temp_store = FILE")
            self._db.execute(
                "CREATE TABLE keys (report_date TEXT NOT NULL, series_id TEXT NOT NULL, cusip TEXT NOT NULL, "
                "PRIMARY KEY (report_date, series_id, cusip)) WITHOUT ROWID"
            )
            self.update(values)
        except Exception:
            self.close()
            raise

    def _connection(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("conflict-key index is closed")
        return self._db

    def _changed(self, before: int, direction: int = 1) -> None:
        db = self._connection()
        changes = db.total_changes - before
        self._size += direction * changes
        self._pending += changes
        if self._pending >= _BATCH_SIZE:
            db.commit()
            self._pending = 0

    def __contains__(self, value: object) -> bool:
        db = self._connection()
        if not isinstance(value, tuple) or len(value) != 3 or not all(isinstance(part, str) for part in value):
            return False
        if not self._size:
            return False
        return db.execute(
            "SELECT 1 FROM keys WHERE report_date = ? AND series_id = ? AND cusip = ?", value,
        ).fetchone() is not None

    def __iter__(self) -> Iterator[Key]:
        cursor = self._connection().execute("SELECT report_date, series_id, cusip FROM keys")
        try:
            yield from cursor
        finally:
            cursor.close()

    def __len__(self) -> int:
        self._connection()
        return self._size

    def add(self, value: Key) -> None:
        db = self._connection()
        before = db.total_changes
        db.execute(_INSERT, _key(value))
        self._changed(before)

    def discard(self, value: Key) -> None:
        db = self._connection()
        before = db.total_changes
        db.execute("DELETE FROM keys WHERE report_date = ? AND series_id = ? AND cusip = ?", _key(value))
        self._changed(before, direction=-1)

    def update(self, *others: Iterable[Key]) -> None:
        db = self._connection()
        for other in others:
            if other is self:
                continue
            iterator = iter(other)
            while batch := list(islice(iterator, _BATCH_SIZE)):
                before = db.total_changes
                try:
                    db.executemany(_INSERT, (_key(value) for value in batch))
                finally:
                    # Keep length correct if a malformed key follows valid keys
                    # in the same batch, matching a set's partial update behavior.
                    self._changed(before)

    def __ior__(self, other: Iterable[Key]) -> KeyIndex:
        self.update(other)
        return self

    def close(self) -> None:
        db, self._db = self._db, None
        try:
            if db is not None:
                db.close()
        finally:
            self._directory.cleanup()

    def __enter__(self) -> KeyIndex:
        self._connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass  # partial construction or interpreter shutdown
