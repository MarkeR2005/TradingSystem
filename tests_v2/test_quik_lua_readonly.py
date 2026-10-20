from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from lupa.lua54 import LuaError, LuaRuntime


SCRIPT = Path(__file__).parents[1] / 'quik' / 'TradingSystemReadOnly.lua'


class LuaReadOnlyTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.script_directory = Path(self.temp.name) / 'bridge'
        self.script_directory.mkdir()
        self.profile_path = Path(str(self.script_directory) + '\\TradingSystemReadOnly.profile.local')

    def runtime(self, version='Lua 5.4'):
        lua = LuaRuntime(unpack_returned_tuples=True)
        lua.globals().script_directory = str(self.script_directory)
        lua.execute('''
            called = {}; callbacks = {}; ran = false; is_connected = true
            function getScriptPath() return script_directory end
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
        self.assertEqual(list(response['capabilities'].values()), ['instrument_parameters_v1', 'callback_cursor_v1'])

    def test_cursor_is_stable_per_script_and_is_not_a_callback(self):
        lua = self.load()
        initial = self.request(lua, 'tsRuntimeInfo')['data']['callback_cursor']
        current = self.request(lua, 'tsCallbackCursor')['data']
        self.assertRegex(initial['bridge_session'], r'^[0-9a-f]{32}$')
        self.assertEqual(dict(initial), dict(current))
        self.assertEqual(current['sequence'], 0)
        self.assertEqual(current['failed_sends'], 0)
        self.assertEqual(len(lua.globals().callbacks), 0)
        self.assertEqual(len(lua.globals().called), 0)

    def test_financial_callbacks_get_monotonic_metadata_without_mutating_input(self):
        lua = self.load()
        lua.execute('original = {cmd="OnTrade",data={trade_num=123}}; sendCallback(original)')
        lua.execute('sendCallback({cmd="OnOrder",data={}})')
        first, second = lua.globals().callbacks[1], lua.globals().callbacks[2]
        self.assertEqual(first['callback_cursor']['sequence'], 1)
        self.assertEqual(second['callback_cursor']['sequence'], 2)
        self.assertEqual(first['callback_cursor']['bridge_session'], second['callback_cursor']['bridge_session'])
        self.assertEqual(self.request(lua, 'tsCallbackCursor')['data']['sequence'], 2)
        self.assertIsNone(lua.globals().original['callback_cursor'])

    def test_ignored_market_noise_does_not_advance_cursor(self):
        lua = self.load()
        lua.execute('sendCallback({cmd="OnAllTrade",data={}}); sendCallback({cmd="OnQuote",data={}})')
        self.assertEqual(self.request(lua, 'tsCallbackCursor')['data']['sequence'], 0)

    def test_failed_or_throwing_callback_send_is_visible_without_error_details(self):
        for implementation in ('return nil', 'error("private-account-details")'):
            lua = self.runtime()
            lua.execute('function sendCallback(msg) ' + implementation + ' end')
            lua.execute(SCRIPT.read_text())
            self.assertFalse(lua.eval('sendCallback({cmd="OnTrade",data={}})'))
            value = self.request(lua, 'tsCallbackCursor')['data']
            self.assertEqual(value['sequence'], 1)
            self.assertEqual(value['failed_sends'], 1)
            self.assertNotIn('private-account-details', str(dict(value)))

    def test_disconnected_period_is_outside_next_connection_delivery_baseline(self):
        lua = self.load()
        lua.execute('is_connected = false; sendCallback({cmd="OnTrade",data={}})')
        self.assertEqual(len(lua.globals().callbacks), 0)
        value = self.request(lua, 'tsCallbackCursor')['data']
        self.assertEqual((value['sequence'], value['failed_sends']), (1, 0))

    def test_cursor_requires_empty_argument_and_keeps_trade_commands_blocked(self):
        lua = self.load()
        self.assertEqual(self.request(lua, 'tsCallbackCursor', 'anything')['cmd'], 'lua_error')
        self.assertEqual(self.request(lua, 'sendTransaction')['cmd'], 'lua_error')
        self.assertEqual(len(lua.globals().called), 0)

    def test_local_profile_survives_loading_unmodified_wrapper_and_preserves_read_only(self):
        for contents in (b'psb-live', b'psb-live\n', b'psb-live\r\n', b'\xef\xbb\xbfpsb-live\r\n'):
            with self.subTest(contents=contents):
                self.profile_path.write_bytes(contents)
                lua = self.load()
                info = self.request(lua, 'tsRuntimeInfo')['data']
                self.assertEqual(info['profile'], 'psb-live')
                self.assertEqual(info['mode'], 'read_only')
                self.assertEqual(self.request(lua, 'sendTransaction')['cmd'], 'lua_error')
                lua.globals().main()
                self.assertTrue(lua.globals().ran)
                self.assertEqual(self.profile_path.read_bytes(), contents)
                self.assertEqual(len(lua.globals().called), 0)

    def test_profile_length_boundaries_and_ascii_identifier_characters(self):
        for name in ('a', 'a' * 64, 'Desk_1.psb:live-1'):
            with self.subTest(name=name):
                self.profile_path.write_bytes(b'\xef\xbb\xbf' + name.encode() + b'\r\n')
                self.assertEqual(self.request(self.load(), 'tsRuntimeInfo')['data']['profile'], name)

    def test_invalid_local_profile_stops_before_loading_bridge(self):
        for contents in (b'', b' ', b'psb-live\nother', b'psb-live\x00', b'psb-live\n\n',
                         b'psb-live|other', b'return "psb-live"', b'a' * 65, b'a' * 100000,
                         'псб'.encode(), 'psb-live'.encode('utf-16')):
            with self.subTest(contents=contents[:30]):
                self.profile_path.write_bytes(contents)
                lua = self.runtime()
                with self.assertRaisesRegex(LuaError, 'Invalid local read-only profile'):
                    lua.execute(SCRIPT.read_text())
                self.assertIsNone(lua.globals().loaded_path)
                self.assertFalse(lua.globals().ran)

    def test_unreadable_profile_does_not_silently_use_demo(self):
        lua = self.runtime()
        lua.execute('io.open = function() return nil, "private file details", 13 end')
        with self.assertRaisesRegex(LuaError, 'Cannot open local read-only profile') as error:
            lua.execute(SCRIPT.read_text())
        self.assertNotIn('private file details', str(error.exception))
        self.assertIsNone(lua.globals().loaded_path)

    def test_profile_read_failure_closes_file_and_stops_loading(self):
        lua = self.runtime()
        lua.execute('''
            profile_closed = false
            io.open = function()
                return {read = function() return nil end,
                        close = function() profile_closed = true; return true end}
            end
        ''')
        with self.assertRaisesRegex(LuaError, 'Invalid local read-only profile'):
            lua.execute(SCRIPT.read_text())
        self.assertTrue(lua.globals().profile_closed)
        self.assertIsNone(lua.globals().loaded_path)

    def test_only_named_spbfut_parameters_reach_upstream(self):
        lua = self.load()
        for data in ('SPBFUT|SiZ6|LAST', 'QJSIM|SiZ6|STEPPRICE', 'SPBFUT|SiZ6|STEPPRICE|X',
                     'SPBFUT||STEPPRICE', 'SPBFUT|SiZ6|stepprice'):
            self.assertEqual(self.request(lua, 'getParamEx', data)['cmd'], 'lua_error')
        self.assertEqual(len(lua.globals().called), 0)
        for name in ('SEC_PRICE_STEP', 'STEPPRICE', 'CURSTEPPRICE', 'BUYDEPO', 'SELLDEPO'):
            self.assertEqual(self.request(lua, 'getParamEx', 'SPBFUT|SiZ6|' + name)['cmd'], 'getParamEx')
        self.assertEqual(len(lua.globals().called), 5)

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
