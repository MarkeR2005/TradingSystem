"""One sequential mailbox per named consumer; enqueue without awaiting handlers."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .domain import Message


type Handler = Callable[[Message], Awaitable[None]]


async def deliver_pending_cancellation() -> None:
    """Surface self-cancellation before leaving the protected callback boundary."""
    task = asyncio.current_task()
    if task is not None and task.cancelling():
        await asyncio.sleep(0)


@dataclass(frozen=True)
class HandlerFailure:
    recipient: str
    message: Message
    error: Exception


class DeliveryError(Exception):
    def __init__(self, failures: tuple[HandlerFailure, ...]) -> None:
        self.failures = failures
        super().__init__('; '.join(f'{f.recipient}: {f.error}' for f in failures))


@dataclass
class _Mailbox:
    queue: asyncio.Queue[Message]
    task: asyncio.Task[None]
    accepting: bool = True


class EventBus:
    def __init__(self) -> None:
        self._mailboxes: dict[str, _Mailbox] = {}
        self._pending = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._failures: list[HandlerFailure] = []
        self._closed = False

    def subscribe(self, recipient: str, handler: Handler) -> None:
        if self._closed or recipient in self._mailboxes:
            raise ValueError('bus is closed or recipient already exists')
        queue: asyncio.Queue[Message] = asyncio.Queue()
        task = asyncio.create_task(self._consume(recipient, queue, handler), name=recipient)
        self._mailboxes[recipient] = _Mailbox(queue, task)

    async def publish(self, recipient: str, message: Message) -> None:
        mailbox = self._mailboxes.get(recipient)
        if self._closed or mailbox is None or not mailbox.accepting:
            raise ValueError(f'no active recipient: {recipient}')
        self._pending += 1
        self._idle.clear()
        mailbox.queue.put_nowait(message)

    async def _consume(self, recipient: str, queue: asyncio.Queue[Message], handler: Handler) -> None:
        while True:
            message = await queue.get()
            try:
                await handler(message)
                await deliver_pending_cancellation()
            except asyncio.CancelledError as error:
                if self._closed:
                    raise
                failure = RuntimeError('handler was cancelled')
                failure.__cause__ = error
                self._failures.append(HandlerFailure(recipient, message, failure))
            except Exception as error:
                self._failures.append(HandlerFailure(recipient, message, error))
            finally:
                # Only the bus owns worker shutdown. A callback may cancel itself
                # or await an independently cancelled task without losing its queue.
                task = asyncio.current_task()
                if not self._closed and task is not None:
                    while task.cancelling():
                        task.uncancel()
                queue.task_done()
                self._pending -= 1
                if self._pending == 0:
                    self._idle.set()

    async def drain(self) -> None:
        self._check_not_consumer()
        await self._idle.wait()
        if self._failures:
            failures = tuple(self._failures)
            self._failures.clear()
            raise DeliveryError(failures)

    @property
    def in_consumer(self) -> bool:
        return any(asyncio.current_task() is mailbox.task for mailbox in self._mailboxes.values())

    def _check_not_consumer(self) -> None:
        if self.in_consumer:
            raise RuntimeError('a consumer cannot wait for its own mailbox')

    async def unsubscribe(self, recipient: str) -> None:
        self._check_not_consumer()
        mailbox = self._mailboxes[recipient]
        mailbox.accepting = False
        await mailbox.queue.join()
        mailbox.task.cancel()
        await asyncio.gather(mailbox.task, return_exceptions=True)
        del self._mailboxes[recipient]

    async def close(self) -> None:
        self._check_not_consumer()
        try:
            await self.drain()
        finally:
            self._closed = True
            tasks = [mailbox.task for mailbox in self._mailboxes.values()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._mailboxes.clear()
