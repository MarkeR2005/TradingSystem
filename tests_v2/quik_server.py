"""Loopback-only stand-in for the two sockets of the pinned Lua bridge."""
import asyncio
import json


class QuikServer:
    def __init__(self):
        self.requests = []
        self.writers = []
        self.tasks = set()
        self.callback = None
        self.callback_ready = asyncio.Event()
        self.responses = {'tsRuntimeInfo': {'protocol': 1, 'mode': 'read_only', 'profile': 'demo-quik',
            'quik_version': '13.2.0.1', 'lua_version': 'Lua 5.4'}, 'ping': 'Pong', 'isConnected': 1,
            'getClassesList': 'SPBFUT,QJSIM,', 'getClassSecurities': 'TEST,',
            'getTradeAccounts': [{'trdaccid': 'demo', 'firmid': 'DEMO', 'class_codes': '|SPBFUT|'}],
            'get_orders': [], 'get_trades': [], 'getFuturesClientHoldings': [], 'getFuturesClientLimits': [],
            'getSecurityInfo': {'class_code': 'SPBFUT', 'code': 'TEST', 'min_price_step': 0.5, 'lot_size': 1}}
        self.override = None
        self.fragment = False

    async def start(self):
        self.request_server = await asyncio.start_server(self.handle_request, '127.0.0.1', 0)
        self.callback_server = await asyncio.start_server(self.handle_callback, '127.0.0.1', 0)
        self.request_port = self.request_server.sockets[0].getsockname()[1]
        self.callback_port = self.callback_server.sockets[0].getsockname()[1]
        return self

    async def handle_callback(self, reader, writer):
        self.writers.append(writer)
        self.callback = writer
        self.callback_ready.set()

    async def handle_request(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.append(writer)
        try:
            while line := await reader.readline():
                request = json.loads(line.decode('cp1251'))
                self.requests.append(request)
                if request['cmd'] == 'getInfoParam':
                    data = {'VERSION': '13.2.0.1', 'TRADEDATE': '14.10.2026', 'SERVERTIME': '09:30:00'}[request['data']]
                else:
                    data = self.responses.get(request['cmd'])
                result = {**request, 'data': data}
                raw = json.dumps(result, ensure_ascii=False).encode('cp1251') + b'\n'
                if self.override is not None:
                    raw = await self.override(request, raw)
                if raw is None:
                    continue
                if self.fragment:
                    for chunk in (raw[:3], raw[3:11], raw[11:]):
                        writer.write(chunk)
                        await writer.drain()
                        await asyncio.sleep(0)
                else:
                    writer.write(raw)
                    await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            self.tasks.discard(task)

    async def emit(self, data):
        await self.callback_ready.wait()
        raw = data if isinstance(data, bytes) else json.dumps(data, ensure_ascii=False).encode('cp1251') + b'\n'
        self.callback.write(raw)
        await self.callback.drain()

    async def close(self):
        for server in (self.request_server, self.callback_server):
            server.close()
        for writer in self.writers:
            writer.close()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.gather(*(w.wait_closed() for w in self.writers), return_exceptions=True)
        await asyncio.gather(self.request_server.wait_closed(), self.callback_server.wait_closed())
