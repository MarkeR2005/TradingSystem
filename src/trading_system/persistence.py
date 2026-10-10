"""One durable writer: fsynced journal with a rebuildable SQLite projection."""

import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO


class JournalError(RuntimeError):
    pass


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(',', ':'))


class DurableJournal:
    """A directory is owned by one process until close, including during DB failure."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self._lock: BinaryIO | None = None
        self._stream: BinaryIO | None = None
        self._db: sqlite3.Connection | None = None
        self._entries: list[dict[str, Any]] = []
        self._failed = False
        self.degraded = False
        try:
            self._acquire_lock()
            path = directory / 'events.jsonl'
            if path.exists():
                self._read(path)
            self._stream = path.open('ab', buffering=0)
            # Persist the directory entry as well as journal contents on POSIX.
            if os.name != 'nt':
                descriptor = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            self.retry_projection()
        except BaseException:
            self.close()
            raise

    def _acquire_lock(self) -> None:
        stream = (self.directory / 'writer.lock').open('a+b')
        if stream.seek(0, os.SEEK_END) == 0:
            stream.write(b'0')
            stream.flush()
        stream.seek(0)
        try:
            if sys.platform == 'win32':
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            stream.close()
            raise JournalError('journal already has a writer') from error
        self._lock = stream

    @property
    def writable(self) -> bool:
        return self._stream is not None and not self._failed

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        # JSON copies prevent callers mutating canonical records.
        return tuple(json.loads(canonical(entry['payload'])) for entry in self._entries)

    def _read(self, path: Path) -> None:
        previous = ''
        try:
            with path.open('rb') as stream:
                for sequence, line in enumerate(stream, 1):
                    if not line.endswith(b'\n'):
                        raise JournalError('incomplete journal tail; explicit repair is required')
                    entry = json.loads(line)
                    body = {key: entry[key] for key in ('sequence', 'previous', 'payload')}
                    checksum = hashlib.sha256(canonical(body).encode()).hexdigest()
                    if (set(entry) != {'sequence', 'previous', 'payload', 'checksum'}
                            or entry['sequence'] != sequence or entry['previous'] != previous
                            or entry['checksum'] != checksum or not isinstance(entry['payload'], dict)):
                        raise JournalError('journal sequence or checksum is invalid')
                    self._entries.append(entry)
                    previous = checksum
        except (ValueError, KeyError, TypeError, UnicodeError) as error:
            raise JournalError('invalid journal record') from error

    def append(self, payload: dict[str, Any]) -> None:
        if self._failed or self._stream is None:
            raise JournalError('journal is closed or durability is uncertain; restart required')
        # Copy and validate before touching the file.
        copied = json.loads(canonical(payload))
        body = {'sequence': len(self._entries) + 1,
                'previous': self._entries[-1]['checksum'] if self._entries else '',
                'payload': copied}
        entry = dict(body, checksum=hashlib.sha256(canonical(body).encode()).hexdigest())
        data = (canonical(entry) + '\n').encode()
        try:
            if self._stream.write(data) != len(data):
                raise OSError('short journal write')
            os.fsync(self._stream.fileno())
        except OSError as error:
            self._failed = True
            raise JournalError('journal write failed; durability is uncertain') from error
        self._entries.append(entry)
        try:
            self._project()
            self.degraded = False
        except sqlite3.Error:
            self._disconnect()
            self.degraded = True
        except JournalError:
            self._failed = True
            raise

    def _project(self) -> None:
        if self._db is None:
            self._db = sqlite3.connect(self.directory / 'events.sqlite3', timeout=0.1)
            self._db.execute('PRAGMA synchronous=FULL')
            self._db.execute('PRAGMA journal_mode=WAL')
            self._db.execute('CREATE TABLE IF NOT EXISTS events '
                             '(sequence INTEGER PRIMARY KEY, checksum TEXT NOT NULL, payload TEXT NOT NULL)')
        rows = self._db.execute('SELECT sequence, checksum, payload FROM events ORDER BY sequence').fetchall()
        if len(rows) > len(self._entries):
            raise JournalError('SQLite contains events missing from the journal')
        for index, row in enumerate(rows):
            entry = self._entries[index]
            if row != (entry['sequence'], entry['checksum'], canonical(entry['payload'])):
                raise JournalError('SQLite conflicts with the canonical journal')
        with self._db:
            self._db.executemany('INSERT INTO events VALUES (?, ?, ?)',
                                 [(e['sequence'], e['checksum'], canonical(e['payload']))
                                  for e in self._entries[len(rows):]])

    def retry_projection(self) -> bool:
        if self._stream is None or self._failed:
            raise JournalError('journal is not writable')
        try:
            self._project()
        except sqlite3.Error:
            self._disconnect()
            self.degraded = True
            return False
        except JournalError:
            self._disconnect()
            self._failed = True
            raise
        self.degraded = False
        return True

    def _disconnect(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None

    def close(self) -> None:
        self._disconnect()
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def __enter__(self) -> 'DurableJournal':
        return self

    def __exit__(self, exc_type: type[BaseException] | None,
                 exc: BaseException | None, traceback: TracebackType | None) -> None:
        self.close()
