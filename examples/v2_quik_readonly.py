"""Run the real read-only TCP client against a synthetic loopback terminal."""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from trading_system.quik_probe import run_probe


async def main() -> None:
    writers: list[asyncio.StreamWriter] = []
    tasks: set[asyncio.Task[None]] = set()
    requests: list[dict[str, Any]] = []
    responses: dict[str, Any] = {
        'tsRuntimeInfo': {'protocol': 1, 'mode': 'read_only', 'profile': 'demo-quik',
                          'quik_version': '13.2.0.1', 'lua_version': 'Lua 5.4'},
        'ping': 'Pong', 'isConnected': 1, 'getClassesList': 'SPBFUT,QJSIM,',
        'getTradeAccounts': [{'trdaccid': 'synthetic-demo', 'firmid': 'DEMO', 'class_codes': '|SPBFUT|'}],
        'get_orders': [], 'get_trades': [], 'getFuturesClientHoldings': [], 'getFuturesClientLimits': [],
    }

    async def callbacks(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        try:
            while raw := await reader.readline():
                message = json.loads(raw.decode('cp1251'))
                requests.append(message.copy())
                if message['cmd'] == 'getInfoParam':
                    message['data'] = '14.10.2026'
                else:
                    message['data'] = responses[message['cmd']]
                writer.write(json.dumps(message).encode('cp1251') + b'\n')
                await writer.drain()
        finally:
            writer.close()
            tasks.discard(task)

    response_server = await asyncio.start_server(serve, '127.0.0.1', 0)
    callback_server = await asyncio.start_server(callbacks, '127.0.0.1', 0)
    try:
        with TemporaryDirectory() as directory:
            report, code = await run_probe(Path(directory) / 'probe', profile='demo-quik', expected_version='13.2',
                request_port=response_server.sockets[0].getsockname()[1],
                callback_port=callback_server.sockets[0].getsockname()[1], observe_seconds=0)
            assert code == 0 and report['snapshot']['repeatable_observation']
            assert not report['snapshot']['reconciled']
            assert all(r['cmd'] != 'sendTransaction' for r in requests)
            assert sum(r['cmd'] == 'get_orders' for r in requests) == 2
            print('Synthetic loopback terminal: two sockets, read-only profile and QUIK version verified.')
            print('Two independent table scans captured; wire frames and report saved in a temporary directory.')
            print('No transactions or OMS changes; repeatable observation is not permission to trade.')
    finally:
        response_server.close()
        callback_server.close()
        for writer in writers:
            writer.close()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(w.wait_closed() for w in writers), return_exceptions=True)
        await asyncio.gather(response_server.wait_closed(), callback_server.wait_closed())


if __name__ == '__main__':
    asyncio.run(main())
