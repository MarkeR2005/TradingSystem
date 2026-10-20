"""Two real loopback sockets: a callback deliberately arrives after the RPC reply."""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from trading_system.quik_probe import run_probe


async def main() -> None:
    writers: list[asyncio.StreamWriter] = []
    tasks: set[asyncio.Task[None]] = set()
    callback_writer: asyncio.StreamWriter | None = None
    callback_ready = asyncio.Event()
    sequence, fences = 0, 0

    def cursor() -> dict[str, Any]:
        return {'bridge_session': 'a' * 32, 'sequence': sequence, 'failed_sends': 0}

    async def callbacks(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal callback_writer
        callback_writer = writer
        writers.append(writer)
        callback_ready.set()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal sequence, fences
        writers.append(writer)
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        try:
            while raw := await reader.readline():
                request = json.loads(raw.decode('cp1251'))
                command = request['cmd']
                delayed: dict[str, Any] | None = None
                data: Any
                if command == 'tsRuntimeInfo':
                    data = {'protocol': 1, 'mode': 'read_only', 'profile': 'demo-quik',
                            'quik_version': '13.2.0.1', 'lua_version': 'Lua 5.4',
                            'capabilities': ['callback_cursor_v1'], 'callback_cursor': cursor()}
                elif command == 'tsCallbackCursor':
                    fences += 1
                    if fences == 2:
                        sequence += 1
                        delayed = {'cmd': 'OnOrder', 'data': {'order_num': 501}, 'callback_cursor': cursor()}
                    data = cursor()
                elif command == 'ping':
                    data = 'Pong'
                elif command == 'isConnected':
                    data = 1
                elif command == 'getInfoParam':
                    data = '20.10.2026'
                elif command == 'getClassesList':
                    data = 'SPBFUT,'
                elif command == 'getTradeAccounts':
                    data = [{'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'class_codes': '|SPBFUT|'}]
                else:
                    assert command in ('get_orders', 'get_trades', 'getFuturesClientHoldings', 'getFuturesClientLimits')
                    data = []
                writer.write(json.dumps({**request, 'data': data}).encode('cp1251') + b'\n')
                await writer.drain()
                if delayed is not None:
                    # The response channel has replied, but the callback channel
                    # still owes this frame. No financial reconciliation is done.
                    await asyncio.sleep(0.01)
                    await callback_ready.wait()
                    assert callback_writer is not None
                    callback_writer.write(json.dumps(delayed).encode('cp1251') + b'\n')
                    await callback_writer.drain()
        finally:
            writer.close()
            tasks.discard(task)

    response_server = await asyncio.start_server(serve, '127.0.0.1', 0)
    callback_server = await asyncio.start_server(callbacks, '127.0.0.1', 0)
    try:
        with TemporaryDirectory() as directory:
            report, code = await run_probe(Path(directory) / 'probe', profile='demo-quik', expected_version='13.2',
                request_port=response_server.sockets[0].getsockname()[1],
                callback_port=callback_server.sockets[0].getsockname()[1], observe_seconds=0,
                require_callback_fence=True)
            assert code == 2, report
            snapshot = report['snapshot']
            boundary = snapshot['callback_boundary']
            assert boundary['delivery_verified'] and not boundary['quiet'] and not boundary['broker_atomic']
            assert boundary['end']['sequence'] == 1 and snapshot['callbacks_after'] == 1
            assert not snapshot['repeatable_observation'] and not snapshot['reconciled']
            assert 'bridge_callbacks_during_scan' in snapshot['issues']
            print('The RPC replied first; the client waited for the delayed callback to reach its durable journal.')
            print('Both table reads matched, but callback activity invalidated the quiet observation window.')
            print('Transport delivery verified. Broker atomicity and OMS reconciliation remain false.')
    finally:
        response_server.close()
        callback_server.close()
        for writer in writers:
            writer.close()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(writer.wait_closed() for writer in writers), return_exceptions=True)
        await asyncio.gather(response_server.wait_closed(), callback_server.wait_closed())


if __name__ == '__main__':
    asyncio.run(main())
