from pathlib import Path
from unittest import TestCase

from lupa.lua54 import LuaError, LuaRuntime


SCRIPT = Path(__file__).parents[1] / 'quik' / 'TradingSystemReadOnly.lua'


class LuaReadOnlyTests(TestCase):
    def runtime(self, version='Lua 5.4'):
        lua = LuaRuntime(unpack_returned_tuples=True)
        lua.execute('''
            called = {}; callbacks = {}; ran = false
            function getScriptPath() return 'C:/QUIK' end
            function getInfoParam(name) return '13.2.0.1' end
            function dofile(path) loaded_path = path end
            function sendCallback(msg) table.insert(callbacks, msg); return true end
            function paramsFromConfig(name) return {'127.0.0.1',34130,'127.0.0.1',34131} end
            function setup(name) return true end
            function run() ran = true end
            package.preload.qsfunctions = function()
                return {dispatch_and_process = function(msg)
                    table.insert(called,msg.cmd); return msg
                end}
            end
        ''')
        lua.globals()['_VERSION'] = version
        return lua

    def load(self):
        lua = self.runtime()
        lua.execute(SCRIPT.read_text())
        return lua

    def request(self, lua, command, data=''):
        return lua.eval('require("qsfunctions").dispatch_and_process')(
            lua.table_from({'id': 1, 'cmd': command, 'data': data, 't': ''}))

    def test_trade_and_unknown_commands_never_reach_underlying_dispatcher(self):
        lua = self.load()
        for command in ('sendTransaction', 'CreateDataSource', 'message', 'eval', 'stop', 'getWorkingFolder'):
            self.assertEqual(self.request(lua, command)['cmd'], 'lua_error')
        self.assertEqual(len(lua.globals().called), 0)

    def test_read_arguments_are_checked_on_the_server(self):
        lua = self.load()
        for command, data in (('getInfoParam','PASSWORD'), ('getSecurityInfo','A|B|C'),
                              ('get_orders','SPBFUT|TEST'), ('ping','wrong')):
            self.assertEqual(self.request(lua, command, data)['cmd'], 'lua_error')
        self.assertEqual(len(lua.globals().called), 0)
        self.assertEqual(self.request(lua, 'getSecurityInfo', 'SPBFUT|TEST')['cmd'], 'getSecurityInfo')
        self.assertEqual(len(lua.globals().called), 1)

    def test_runtime_handshake_reports_version_mode_and_profile(self):
        lua = self.load()
        response = self.request(lua, 'tsRuntimeInfo')['data']
        self.assertEqual((response['protocol'], response['profile'], response['lua_version'], response['mode']),
                         (1, 'demo-quik', 'Lua 5.4', 'read_only'))
        self.assertEqual(len(lua.globals().called), 0)

    def test_wrong_lua_stops_before_loading_any_dll(self):
        lua = self.runtime('Lua 5.3')
        with self.assertRaises(LuaError):
            lua.execute(SCRIPT.read_text())
        self.assertIsNone(lua.globals().loaded_path)

    def test_only_local_endpoints_and_distinct_ports_can_start(self):
        lua = self.load()
        lua.execute("function paramsFromConfig(name) return {'0.0.0.0',34130,'127.0.0.1',34131} end")
        with self.assertRaises(LuaError):
            lua.globals().main()
        self.assertFalse(lua.globals().ran)

    def test_market_noise_is_filtered_but_financial_callbacks_are_forwarded(self):
        lua = self.load()
        lua.execute('sendCallback({cmd="OnAllTrade",data={}}); sendCallback({cmd="OnQuote",data={}})')
        lua.execute('sendCallback({cmd="OnTrade",data={trade_num=9007199254740993}})')
        self.assertEqual(len(lua.globals().callbacks), 1)
        self.assertEqual(lua.globals().callbacks[1]['data']['trade_num'], 9007199254740993)

    def test_fractional_or_missing_request_id_is_rejected(self):
        lua = self.load()
        dispatch = lua.eval('require("qsfunctions").dispatch_and_process')
        for request_id in (None, 1.5, True, '1'):
            response = dispatch(lua.table_from({'id': request_id, 'cmd': 'get_orders', 'data': ''}))
            self.assertEqual(response['cmd'], 'lua_error')
        self.assertEqual(len(lua.globals().called), 0)
