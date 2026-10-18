"""Bounded, journaled, read-only client for the pinned QUIK Lua wire protocol.

No trading methods, retries, automatic reconnect, OMS mutations or subscriptions.
Requires our read-only Lua guard to identify the intended terminal profile.
"""

import asyncio
import base64
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from math import isfinite
import re
from typing import Any
from uuid import uuid4

from .persistence import DurableJournal, JournalError, canonical
from .quik_contract import identifier, integer
from .quik_parameters import PARAMETER_NAMES

EMPTY_REQUESTS = frozenset({'tsRuntimeInfo', 'isConnected', 'getClassesList', 'getTradeAccounts',
    'get_orders', 'get_trades', 'getFuturesClientHoldings', 'getFuturesClientLimits'})
FATAL_CALLBACKS = frozenset({'OnDisconnected', 'OnClose', 'OnStop', 'lua_error'})


class QuikTransportError(RuntimeError):
    pass


def validate_read(command: str, data: str) -> None:
    if not isinstance(command, str) or not isinstance(data, str):
        raise ValueError('read-only request requires string command and argument')
    if command in EMPTY_REQUESTS and data == '':
        return
    if command == 'ping' and data == 'Ping':
        return
    if command == 'getInfoParam' and data in ('VERSION', 'TRADEDATE', 'SERVERTIME'):
        return
    if command == 'getClassSecurities':
        identifier(data)
        return
    if command == 'getSecurityInfo' and len(data.split('|')) == 2:
        for part in data.split('|'):
            identifier(part)
        return
    if command == 'getParamEx' and len(data.split('|')) == 3:
        class_code, symbol, parameter = data.split('|')
        if class_code == 'SPBFUT' and parameter in PARAMETER_NAMES:
            identifier(symbol)
            return
    raise ValueError('command or argument is outside the read-only allowlist')


def version_parts(value: str) -> tuple[int, ...]:
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,5}){1,3}', value):
        raise ValueError('expected a numeric QUIK version with two to four components')
    return tuple(int(part) for part in value.split('.'))


def _diagnostic_value(value: object) -> str:
    """Bound runtime metadata and escape controls; never expand nested values."""
    if isinstance(value, str):
        return json.dumps(value[:80], ensure_ascii=True) + ('...' if len(value) > 80 else '')
    if value is None or type(value) in (bool, float):
        return json.dumps(value, allow_nan=False)
    if type(value) is int and -10**20 < value < 10**20:
        return str(value)
    return '<' + type(value).__name__ + '>'


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise ValueError('non-finite JSON constant')


def decode_frame(raw: bytes) -> dict[str, Any]:
    if not raw.endswith(b'\n'):
        raise ValueError('incomplete JSON frame')
    obj = json.loads(raw.decode('cp1251'), object_pairs_hook=_pairs, parse_constant=_constant)
    # Also reject overflow from JSON scientific notation, e.g. 1e999.
    canonical(obj)
    if not isinstance(obj, dict) or not isinstance(obj.get('cmd'), str) or 'data' not in obj:
        raise ValueError('invalid QUIK response envelope')
    return obj


@dataclass(frozen=True)
class ReadOnlyCallback:
    sequence: int
    command: str
    received_at: datetime
    data_json: str

    @property
    def data(self) -> Any:
        return json.loads(self.data_json)


class ReadOnlyQuik:
    def __init__(self, *, journal: DurableJournal, profile: str, expected_version: str,
                 host: str = '127.0.0.1', request_port: int = 34130, callback_port: int = 34131,
                 timeout: float = 10, max_frame_bytes: int = 1_048_576, max_callbacks: int = 1000,
                 max_callback_bytes: int = 4_194_304, max_capture_bytes: int = 16_777_216) -> None:
        if host != '127.0.0.1':
            raise ValueError('only the numeric IPv4 loopback address is supported')
        identifier(profile)
        self._expected_version = version_parts(expected_version)
        for port in (request_port, callback_port):
            integer(port, maximum=65535)
        if request_port == callback_port:
            raise ValueError('response and callback ports must be distinct')
        if isinstance(timeout, bool) or not isfinite(timeout) or not 0 < timeout <= 60:
            raise ValueError('timeout must be in (0, 60] seconds')
        integer(max_frame_bytes, maximum=16_777_216)
        integer(max_callbacks, maximum=100000)
        integer(max_callback_bytes, maximum=16_777_216)
        integer(max_capture_bytes, maximum=67_108_864)
        if max_frame_bytes < 512:
            raise ValueError('frame limit must be at least 512 bytes')
        if any(r.get('kind') != 'quik-wire-v1' for r in journal.records):
            raise JournalError('wire capture requires a separate journal directory')
        self.journal, self.profile, self.expected_version = journal, profile, expected_version
        self.host, self.request_port, self.callback_port = host, request_port, callback_port
        self.timeout, self.max_frame_bytes, self.max_callbacks = timeout, max_frame_bytes, max_callbacks
        self.max_callback_bytes, self.max_capture_bytes = max_callback_bytes, max_capture_bytes
        self._callback_bytes = self._capture_bytes = 0
        self.connection_id = uuid4().hex
        self._tail = journal.record_count
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._callback_writer: asyncio.StreamWriter | None = None
        self._callback_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._used = self._closed = self._ready = False
        self._fault: str | None = None
        self._next_id = 1
        self._callback_count = 0
        self._callbacks: deque[ReadOnlyCallback] = deque()
        self._runtime: dict[str, Any] = {}

    @property
    def healthy(self) -> bool:
        return self._ready and not self._closed and self._fault is None and self.journal.writable

    @property
    def fault(self) -> str | None:
        return self._fault

    @property
    def runtime_info(self) -> dict[str, Any]:
        return deepcopy(self._runtime)

    @property
    def callback_count(self) -> int:
        return self._callback_count

    def drain_callbacks(self) -> tuple[ReadOnlyCallback, ...]:
        result = tuple(self._callbacks)
        self._callbacks.clear()
        self._callback_bytes = 0
        return result

    def _record(self, event: str, raw: bytes | None = None, **details: object) -> None:
        if self.journal.record_count != self._tail:
            raise JournalError('wire journal was changed by another owner')
        record = {'kind': 'quik-wire-v1', 'connection_id': self.connection_id, 'event': event,
                  'at': datetime.now(UTC).isoformat(), **details}
        if raw is not None:
            if self._capture_bytes + len(raw) > self.max_capture_bytes:
                raise JournalError('wire capture byte budget exhausted')
            record['bytes_b64'] = base64.b64encode(raw).decode('ascii')
        self.journal.append(record)
        self._tail += 1
        if raw is not None:
            self._capture_bytes += len(raw)

    def _trip(self, reason: str) -> None:
        if self._fault is None:
            self._fault = reason
            try:
                self._record('fault', reason=reason)
            except Exception:
                pass
        for writer in (self._writer, self._callback_writer):
            if writer is not None:
                writer.close()

    async def connect(self) -> dict[str, Any]:
        if self._used or self._closed:
            raise QuikTransportError('create a new client for each explicit connection attempt')
        self._used = True
        try:
            self._record('session', profile=self.profile, expected_version=self.expected_version,
                         host=self.host, request_port=self.request_port, callback_port=self.callback_port)
            async with asyncio.timeout(self.timeout):
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.request_port, limit=self.max_frame_bytes)
                callback_reader, self._callback_writer = await asyncio.open_connection(
                    self.host, self.callback_port, limit=self.max_frame_bytes)
            self._callback_task = asyncio.create_task(self._read_callbacks(callback_reader))
            info = await self.request('tsRuntimeInfo')
            if not isinstance(info, dict):
                raise ValueError('tsRuntimeInfo mismatch: expected object, observed '
                                 + _diagnostic_value(info))
            expected: dict[str, object] = {'protocol': 1, 'mode': 'read_only', 'profile': self.profile,
                'lua_version': 'Lua 5.4', 'quik_version': self.expected_version}
            mismatches = []
            for field, required in expected.items():
                observed = info.get(field)
                label = 'expected'
                matches = type(observed) is type(required) and observed == required
                if field == 'quik_version':
                    label = 'expected_prefix'
                    try:
                        matches = (isinstance(observed, str)
                            and version_parts(observed)[:len(self._expected_version)] == self._expected_version)
                    except ValueError:
                        matches = False
                if not matches:
                    actual = _diagnostic_value(observed) if field in info else '<missing>'
                    mismatches.append(f'{field} {label}={_diagnostic_value(required)} observed={actual}')
            if mismatches:
                raise ValueError('tsRuntimeInfo mismatch: ' + '; '.join(mismatches))
            if await self.request('ping', 'Ping') != 'Pong':
                raise ValueError('unexpected ping response')
            connected = await self.request('isConnected')
            if type(connected) is not int or connected != 1:
                raise ValueError('QUIK terminal is not connected to its server')
            if self._fault is not None:
                raise QuikTransportError(self._fault)
            self._runtime = info
            self._ready = True
            return self.runtime_info
        except asyncio.CancelledError:
            self._trip('connection attempt cancelled')
            raise
        except Exception as error:
            self._trip('connection handshake failed: ' + str(error))
            raise QuikTransportError(self._fault) from error

    async def _frame(self, reader: asyncio.StreamReader, event: str) -> dict[str, Any]:
        raw = await reader.readline()
        if not raw:
            raise QuikTransportError(event + ' stream closed')
        if len(raw) > self.max_frame_bytes:
            raise QuikTransportError(event + ' frame exceeds configured limit')
        self._record(event, raw)  # Persist even malformed/incomplete frames before parsing.
        return decode_frame(raw)

    async def request(self, command: str, data: str = '') -> Any:
        validate_read(command, data)
        async with self._lock:
            if self._closed or self._fault is not None or self._reader is None or self._writer is None:
                raise QuikTransportError(self._fault or 'client is not connected')
            try:
                request_id = integer(self._next_id, maximum=2**31 - 1)
                self._next_id += 1
                raw = (canonical({'cmd': command, 'id': request_id, 'data': data, 't': ''}) + '\n').encode('cp1251')
                self._record('request', raw)
                async with asyncio.timeout(self.timeout):
                    self._writer.write(raw)
                    await self._writer.drain()
                    response = await self._frame(self._reader, 'response')
                if self._fault is not None:
                    raise QuikTransportError(self._fault)
                if type(response.get('id')) is not int or response['id'] != request_id:
                    raise ValueError('response request id mismatch')
                if response['cmd'] == 'lua_error' or 'lua_error' in response:
                    if (command == 'tsRuntimeInfo' and response['cmd'] == 'lua_error'
                            and response.get('lua_error') ==
                            'Command not implemented in Lua qsfunctions module: tsRuntimeInfo'):
                        raise QuikTransportError('tsRuntimeInfo is not supported by the connected bridge; '
                            'start TradingSystemReadOnly.lua and check which bridge uses the configured ports')
                    # Full Lua errors may contain account data; retain them only in the raw journal.
                    raise QuikTransportError('Lua rejected read-only command ' + command +
                                             '; details retained in the local wire journal')
                if response['cmd'] != command:
                    raise ValueError('response command mismatch')
                if command == 'isConnected' and (type(response['data']) is not int or response['data'] != 1):
                    raise QuikTransportError('terminal is not connected to its broker server')
                return response['data']
            except asyncio.CancelledError:
                self._trip('request cancelled; explicit new connection required')
                raise
            except Exception as error:
                self._trip('request failed; no retry: ' + (str(error) or type(error).__name__))
                raise QuikTransportError(self._fault) from error

    async def _read_callbacks(self, reader: asyncio.StreamReader) -> None:
        try:
            while not self._closed and self._fault is None:
                frame = await self._frame(reader, 'callback')
                self._callback_count += 1
                data = canonical(frame['data'])
                size = len(data.encode('utf-8')) + len(frame['cmd'].encode('utf-8'))
                if len(self._callbacks) >= self.max_callbacks or self._callback_bytes + size > self.max_callback_bytes:
                    raise QuikTransportError('callback queue overflow; capture stopped')
                self._callback_bytes += size
                self._callbacks.append(ReadOnlyCallback(self._callback_count, frame['cmd'], datetime.now(UTC),
                                                        data))
                if frame['cmd'] in FATAL_CALLBACKS:
                    raise QuikTransportError('terminal callback: ' + frame['cmd'])
        except asyncio.CancelledError:
            if not self._closed:
                self._trip('callback reader cancelled unexpectedly')
            raise
        except Exception as error:
            if not self._closed:
                self._trip('callback capture failed: ' + (str(error) or type(error).__name__))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._callback_task is not None:
            self._callback_task.cancel()
            await asyncio.gather(self._callback_task, return_exceptions=True)
        for writer in (self._writer, self._callback_writer):
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(self.timeout):
                        await writer.wait_closed()
                except (OSError, TimeoutError):
                    pass
        if self._used:
            try:
                self._record('closed', fault=self._fault)
            except Exception:
                self._trip('wire journal unavailable at close')
