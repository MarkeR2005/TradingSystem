"""Bulk historical snapshots and cancellable spawned CPU workers."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import inspect
import json
import multiprocessing
from multiprocessing.connection import Connection
from typing import Protocol

from .domain import Candle, InstrumentId, utc
from .persistence import canonical
from .state import JsonObject, json_object


@dataclass(frozen=True)
class HistoryRequest:
    instruments: tuple[InstrumentId, ...]
    start: datetime
    end: datetime
    timeframe: timedelta

    def __post_init__(self) -> None:
        object.__setattr__(self, 'start', utc(self.start))
        object.__setattr__(self, 'end', utc(self.end))
        if (self.start >= self.end or self.timeframe <= timedelta(0)
                or not isinstance(self.instruments, tuple) or not self.instruments
                or not all(isinstance(item, InstrumentId) for item in self.instruments)
                or len(set(self.instruments)) != len(self.instruments)):
            raise ValueError('invalid history request')


class HistorySource(Protocol):
    async def load(self, request: HistoryRequest) -> tuple[Candle, ...]: ...


class InMemoryHistory:
    """Controlled completed bars; not a market-data gateway or backtester."""

    def __init__(self, candles: tuple[Candle, ...]) -> None:
        self.candles = candles
        self.requests: list[HistoryRequest] = []

    async def load(self, request: HistoryRequest) -> tuple[Candle, ...]:
        self.requests.append(request)
        return tuple(sorted((bar for bar in self.candles
                             if bar.instrument_id in request.instruments and bar.timeframe == request.timeframe
                             and bar.opened_at >= request.start and bar.closed_at <= request.end),
                            key=lambda bar: (bar.opened_at, bar.instrument_id.gateway_id,
                                             bar.instrument_id.venue, bar.instrument_id.symbol)))


@dataclass(frozen=True)
class OptimizationInput:
    history: tuple[Candle, ...]
    parameters: JsonObject
    state: JsonObject


type OptimizationFunction = Callable[[OptimizationInput], JsonObject]


class Optimizer(Protocol):
    async def run(self, worker: OptimizationFunction, data: OptimizationInput) -> JsonObject: ...


class OptimizationError(RuntimeError):
    pass


def validate_worker(worker: OptimizationFunction) -> None:
    if not inspect.isfunction(worker) or '<locals>' in worker.__qualname__ or worker.__name__ == '<lambda>':
        raise ValueError('optimizer must be an importable module-level function')


def validate_history(request: HistoryRequest, candles: tuple[Candle, ...]) -> tuple[Candle, ...]:
    if not isinstance(candles, tuple):
        raise ValueError('history source must return an immutable tuple')
    keys: set[tuple[InstrumentId, datetime]] = set()
    for bar in candles:
        if (not isinstance(bar, Candle) or bar.instrument_id not in request.instruments
                or bar.timeframe != request.timeframe or bar.opened_at < request.start
                or bar.closed_at > request.end or (bar.instrument_id, bar.opened_at) in keys):
            raise ValueError('history does not match the requested snapshot')
        keys.add((bar.instrument_id, bar.opened_at))
    return candles


def _worker_entry(sender: Connection, worker: OptimizationFunction, data: OptimizationInput) -> None:
    try:
        result = json_object(worker(data))
        sender.send_bytes(canonical({'ok': True, 'parameters': result}).encode())
    except BaseException as error:
        sender.send_bytes(canonical({'ok': False, 'error': f'{type(error).__name__}: {error}'}).encode())
    finally:
        sender.close()


class ProcessOptimizer:
    """One independently terminable spawned process per job, with a concurrency cap."""

    def __init__(self, max_workers: int = 1) -> None:
        if max_workers < 1:
            raise ValueError('optimizer capacity must be positive')
        self._capacity = asyncio.Semaphore(max_workers)

    async def run(self, worker: OptimizationFunction, data: OptimizationInput) -> JsonObject:
        validate_worker(worker)
        async with self._capacity:
            context = multiprocessing.get_context('spawn')
            receiver, sender = context.Pipe(duplex=False)
            process = context.Process(target=_worker_entry, args=(sender, worker, data))
            started = False
            try:
                process.start()
                started = True
                sender.close()
                while not receiver.poll():
                    if not process.is_alive():
                        if receiver.poll():
                            break
                        raise OptimizationError(f'optimization worker exited: {process.exitcode}')
                    await asyncio.sleep(0.01)
                payload = json.loads(await asyncio.to_thread(receiver.recv_bytes))
                if not payload['ok']:
                    raise OptimizationError(payload['error'])
                return json_object(payload['parameters'])
            except EOFError as error:
                raise OptimizationError('optimization worker returned no result') from error
            finally:
                sender.close()
                if started:
                    if process.is_alive():
                        process.terminate()
                    await asyncio.to_thread(process.join, 2)
                    if process.is_alive():
                        process.kill()
                        await asyncio.to_thread(process.join)
                    process.close()
                receiver.close()


class OptimizationHandle:
    def __init__(self, task: asyncio.Task[JsonObject], may_wait: Callable[[], bool]) -> None:
        self._task = task
        self._may_wait = may_wait

    @property
    def done(self) -> bool:
        return self._task.done()

    async def wait(self) -> JsonObject:
        if not self._may_wait():
            raise RuntimeError('a strategy callback cannot wait for its own optimization')
        return json_object(await asyncio.shield(self._task))
