from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, GatewaySubmit, OrderIntent, OrderStatus, Side
from trading_system.quik_contract import MAX_TRANSACTION_ID, QuikRoute, limit_transaction, parse_trade
from trading_system.quik_replay import QuikReplayGateway
from trading_system.runtime import TradingRuntime
from trading_system.strategy import AutoStrategy
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE as BASE_ROUTE, START

ROUTE = replace(BASE_ROUTE, instrument=replace(BASE_ROUTE.instrument, id=replace(BASE_ROUTE.instrument.id, gateway_id='quik')))
SPEC = QuikRoute(ROUTE, Decimal('0.5'), ('broker_comission',))
BROKER_ID = 9007199254740993


def order_report(transaction_id, *, balance=4, flags=9, order_num=BROKER_ID):
    return dict(trans_id=transaction_id, order_num=order_num, account=ROUTE.account_id,
                class_code='SPBFUT', sec_code='TEST', qty=4, balance=balance, flags=flags, price=100)


def trade_report(*, trade_num=9007199254740995, order_num=BROKER_ID, quantity=1, price=100):
    return dict(trade_num=trade_num, order_num=order_num, account=ROUTE.account_id,
                class_code='SPBFUT', sec_code='TEST', qty=quantity, flags=0, price=price,
                broker_comission='1.25', datetime=dict(year=2026, month=1, day=1, hour=13, min=0, sec=0, ms=0, mcs=0))


class Quiet(AutoStrategy):
    def __init__(self):
        super().__init__('a', (ROUTE.instrument.id,))

    async def on_candle(self, context, candle: Candle):
        pass


class QuikContractTests(TestCase):
    def test_limit_encoding_preserves_integer_ids_and_decimal_price(self):
        transaction = limit_transaction(SPEC, OrderIntent(ROUTE, Side.SELL, 4, 100.5), 42)
        self.assertEqual(transaction, {'TRANS_ID': '42', 'ACTION': 'NEW_ORDER', 'CLASSCODE': 'SPBFUT',
            'SECCODE': 'TEST', 'ACCOUNT': 'demo', 'OPERATION': 'S', 'TYPE': 'L', 'QUANTITY': '4', 'PRICE': '100.5'})

    def test_limit_rejects_market_off_tick_fractional_lots_and_wrong_metadata(self):
        for intent in (OrderIntent(ROUTE, Side.BUY, 1), OrderIntent(ROUTE, Side.BUY, 1, 100.1),
                       OrderIntent(ROUTE, Side.BUY, 1.5, 100),
                       OrderIntent(replace(ROUTE, instrument=replace(ROUTE.instrument, contract_multiplier=2)), Side.BUY, 1, 100)):
            with self.subTest(intent=intent), self.assertRaises(ValueError):
                limit_transaction(SPEC, intent, 1)

    def test_trade_uses_explicit_commission_policy_timezone_and_lossless_ids(self):
        trade = parse_trade(trade_report(), (SPEC,), timedelta(hours=3))
        self.assertEqual((trade.trade_id, trade.order_id, trade.timestamp, trade.commission),
                         (9007199254740995, BROKER_ID, START, 1.25))
        data = trade_report()
        data['datetime'].update(ms=123, mcs=123456)
        self.assertEqual(parse_trade(data, (SPEC,), timedelta(hours=3)).timestamp.microsecond, 123456)

    def test_trade_rejects_missing_commission_rounded_ids_wrong_route_and_invalid_time(self):
        base = trade_report()
        cases = []
        for key, value in (('trade_num', float(BROKER_ID)), ('order_num', True), ('qty', 0),
                           ('price', 'NaN'), ('account', 'other'), ('broker_comission', None),
                           ('broker_comission', '1e-999')):
            cases.append({**base, key: value})
        missing = deepcopy(base)
        missing.pop('broker_comission')
        cases.append(missing)
        bad_time = deepcopy(base)
        bad_time['datetime']['month'] = 13
        cases.append(bad_time)
        for data in cases:
            with self.subTest(data=data), self.assertRaises(ValueError):
                parse_trade(data, (SPEC,), timedelta(hours=3))

    def test_explicit_commission_components_and_negative_prices(self):
        spec = replace(SPEC, commission_fields=('broker_comission', 'exchange_comission'))
        report = {**trade_report(price=-5), 'exchange_comission': '0.15'}
        self.assertEqual(parse_trade(report, (spec,), timedelta(hours=3)).commission, 1.4)
        self.assertEqual(limit_transaction(spec, OrderIntent(ROUTE, Side.BUY, 1, -5), 2)['PRICE'], '-5')
        self.assertEqual(parse_trade(trade_report(), (replace(SPEC, commission_fields=()),), timedelta(hours=3)).commission, 0)
        with self.assertRaises(ValueError):
            replace(SPEC, commission_fields=('broker_comission', 'broker_comission'))


class QuikReplayTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = ManualClock(START)
        self.runtime = TradingRuntime(self.clock, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway = QuikReplayGateway('quik', (SPEC,), self.clock, session_id='fixture-2026-01-01',
                                         timezone_offset=timedelta(hours=3))
        self.runtime.add_gateway(self.gateway)
        self.context = await self.runtime.start_strategy(Quiet())
        account = AccountKey.from_route(ROUTE)
        await self.runtime.configure_capital(CapitalPolicy(1, START, START + timedelta(hours=1), 'RUB',
            (AccountLimit(account, 2000),), (RouteValuation(ROUTE, 150),),
            (RouteGrant('a', ROUTE),), (StrategyLimit('a', account, 2000),)))

    async def asyncTearDown(self):
        await self.runtime.__aexit__(None, None, None)

    async def submit(self):
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
        await self.runtime.drain()
        return self.gateway.transactions[-1].transaction_id

    async def feed(self, name, data):
        await self.gateway.feed(name, data)
        await self.runtime.drain()

    async def registered(self):
        transaction = await self.submit()
        await self.feed('OnOrder', order_report(transaction))
        return transaction

    async def test_sending_and_intermediate_replies_do_not_accept_or_fill(self):
        transaction = await self.submit()
        await self.gateway.transport_response(transaction, {'cmd': 'sendTransaction', 'data': True})
        for status in (0, 1):
            await self.feed('OnTransReply', dict(trans_id=transaction, status=status))
        self.assertEqual(self.context.orders()[0].status, OrderStatus.SUBMITTED)
        self.assertEqual(self.runtime.ledger.positions(), ())
        await self.feed('OnTransReply', dict(trans_id=transaction, status=3, order_num=BROKER_ID))
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)
        self.assertEqual(self.runtime.ledger.positions(), ())

    async def test_trade_before_order_is_buffered_then_applied_once(self):
        transaction = await self.submit()
        trade = trade_report()
        await self.feed('OnTrade', trade)
        await self.feed('OnTrade', trade)
        self.assertEqual(self.runtime.ledger.positions(), ())
        await self.feed('OnOrder', order_report(transaction, balance=3))
        await self.feed('OnTrade', trade)
        self.assertEqual(self.context.orders()[0].filled_quantity, 1)
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).realized_pnl, -1.25)

    async def test_cancel_reply_is_not_a_barrier_and_late_fill_precedes_replacement(self):
        transaction = await self.registered()
        original = self.context.orders()[0]
        await self.feed('OnTrade', trade_report())
        await self.context.replace_order(original.order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
        await self.runtime.drain()
        cancel = self.gateway.transactions[-1]
        self.assertEqual(dict(cancel.fields)['ORDER_KEY'], str(BROKER_ID))
        await self.feed('OnTransReply', dict(trans_id=cancel.transaction_id, status=3, order_num=BROKER_ID))
        self.assertEqual(len(self.context.orders()), 1)
        await self.feed('OnOrder', order_report(transaction, balance=2, flags=10))
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        await self.feed('OnTrade', trade_report(trade_num=9007199254740996))
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.context.orders()[1].intent.quantity, 2)
        self.assertEqual(dict(self.gateway.transactions[-1].fields)['PRICE'], '99')

    async def test_cancel_before_registration_waits_for_broker_key(self):
        transaction = await self.submit()
        await self.context.cancel_order(self.context.orders()[0].order_id)
        await self.runtime.drain()
        self.assertEqual(len(self.gateway.transactions), 1)
        await self.feed('OnOrder', order_report(transaction))
        self.assertEqual(len(self.gateway.transactions), 2)
        self.assertEqual(dict(self.gateway.transactions[-1].fields)['ACTION'], 'KILL_ORDER')

    async def test_timeout_holds_reservation_and_late_trade_still_applies(self):
        transaction = await self.submit()
        self.clock.advance_to(START + timedelta(seconds=90))
        await self.gateway.check_deadlines()
        await self.runtime.drain()
        self.assertTrue(self.gateway.blocked)
        self.assertFalse(self.runtime.orders.ready)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.SUBMITTED)
        self.assertGreater(sum(u.orders for u in self.runtime.orders.capital_usage()), 0)
        await self.feed('OnOrder', order_report(transaction, balance=3))
        await self.feed('OnTrade', trade_report())
        self.assertEqual(self.context.orders()[0].filled_quantity, 1)
        await self.feed('OnConnected', {})
        self.assertFalse(self.runtime.orders.ready)

    async def test_status_12_and_disconnect_do_not_fabricate_rejection(self):
        transaction = await self.submit()
        await self.feed('OnTransReply', dict(trans_id=transaction, status=12))
        await self.feed('OnDisconnected', {})
        self.assertEqual(self.context.orders()[0].status, OrderStatus.SUBMITTED)
        self.assertFalse(self.runtime.orders.ready)
        self.assertEqual(len(self.gateway.transactions), 1)

    async def test_definitive_rejection_and_cancel_rejection_have_different_effects(self):
        transaction = await self.submit()
        await self.feed('OnTransReply', dict(trans_id=transaction, status=6, result_msg='limit'))
        self.assertEqual(self.context.orders()[0].status, OrderStatus.REJECTED)
        await self.registered()
        order = self.context.orders()[-1]
        await self.context.cancel_order(order.order_id)
        await self.runtime.drain()
        cancel = self.gateway.transactions[-1]
        await self.feed('OnTransReply', dict(trans_id=cancel.transaction_id, status=4, result_msg='cannot cancel'))
        self.assertEqual(self.context.orders()[-1].status, OrderStatus.ACCEPTED)
        self.assertTrue(self.context.orders()[-1].cancel_rejection_reason)

    async def test_bad_known_event_blocks_without_financial_mutation(self):
        await self.registered()
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnTrade', {**trade_report(), 'flags': 4})
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.positions(), ())
        self.assertFalse(self.runtime.orders.ready)

    async def test_unknown_external_order_is_not_assigned_to_strategy(self):
        await self.submit()
        await self.feed('OnOrder', order_report(0, order_num=765))
        self.assertTrue(self.gateway.blocked)
        self.assertFalse(self.runtime.orders.ready)
        self.assertEqual(len(self.gateway.unmatched), 1)
        self.assertEqual(self.runtime.ledger.positions(), ())

    async def test_external_trade_without_possible_pending_entry_blocks_immediately(self):
        await self.feed('OnTrade', trade_report())
        self.assertEqual(len(self.gateway.unmatched), 1)
        self.assertFalse(self.runtime.orders.ready)
        self.assertEqual(self.runtime.ledger.positions(), ())

    async def test_full_order_report_does_not_invent_missing_trade(self):
        transaction = await self.submit()
        await self.feed('OnOrder', order_report(transaction, balance=0, flags=8))
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)
        self.assertEqual(self.runtime.ledger.positions(), ())
        self.clock.advance_to(START + timedelta(seconds=90))
        await self.gateway.check_deadlines()
        await self.runtime.drain()
        self.assertFalse(self.runtime.orders.ready)

    async def test_rejected_entry_also_finishes_waiting_cancellation(self):
        transaction = await self.submit()
        await self.context.cancel_order(self.context.orders()[0].order_id)
        await self.runtime.drain()
        await self.feed('OnTransReply', dict(trans_id=transaction, status=5))
        self.clock.advance_to(START + timedelta(seconds=90))
        await self.gateway.check_deadlines()
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.REJECTED)
        self.assertTrue(self.runtime.orders.ready)
        self.assertEqual(len(self.gateway.transactions), 1)

    async def test_lua_error_is_definitive_but_false_transport_result_is_uncertain(self):
        transaction = await self.submit()
        await self.gateway.transport_response(transaction, dict(cmd='lua_transaction_error', lua_error='bad account'))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.REJECTED)
        transaction = await self.submit()
        await self.gateway.transport_response(transaction, dict(cmd='sendTransaction', data=False))
        await self.runtime.drain()
        self.assertFalse(self.runtime.orders.ready)
        self.assertEqual(self.context.orders()[-1].status, OrderStatus.SUBMITTED)

    async def test_contradictory_transport_failure_and_trade_duplicate_block(self):
        transaction = await self.registered()
        await self.feed('OnTrade', trade_report())
        for operation in (
            lambda: self.gateway.transport_response(transaction, dict(cmd='lua_transaction_error', lua_error='error')),
            lambda: self.gateway.feed('OnTrade', trade_report(quantity=2)),
        ):
            with self.assertRaises(ValueError):
                await operation()
            await self.runtime.drain()
            self.assertFalse(self.runtime.orders.ready)
            self.assertEqual(self.context.orders()[0].filled_quantity, 1)

    async def test_out_of_order_active_callback_cannot_resurrect_final_order(self):
        transaction = await self.registered()
        await self.context.cancel_order(self.context.orders()[0].order_id)
        await self.runtime.drain()
        await self.feed('OnOrder', order_report(transaction, balance=3, flags=10))
        await self.feed('OnOrder', order_report(transaction))
        await self.feed('OnTrade', trade_report())
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.context.orders()[0].filled_quantity, 1)
        self.assertTrue(self.runtime.orders.ready)

    async def test_unknown_trade_buffer_is_bounded_and_expiration_blocks(self):
        self.gateway.max_unmatched = 1
        await self.feed('OnTrade', trade_report(order_num=901))
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnTrade', trade_report(order_num=902, trade_num=123))
        await self.runtime.drain()
        self.assertEqual(len(self.gateway.unmatched), 1)
        self.assertFalse(self.runtime.orders.ready)
        self.clock.advance_to(START + timedelta(seconds=90))
        await self.gateway.check_deadlines()
        await self.runtime.drain()
        self.assertTrue(any('deadline' in reason for reason in self.gateway.incidents))

    async def test_transaction_ids_never_wrap_and_duplicate_submit_does_not_record_again(self):
        self.gateway._next_id = MAX_TRANSACTION_ID
        await self.submit()
        original = self.context.orders()[0]
        await self.runtime.bus.publish('gateway:quik', GatewaySubmit(original))
        await self.runtime.drain()
        self.assertEqual(len(self.gateway.transactions), 1)
        self.assertEqual(self.gateway.transactions[0].transaction_id, MAX_TRANSACTION_ID)
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 1, 100))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[-1].status, OrderStatus.REJECTED)
        self.assertEqual(len(self.gateway.transactions), 1)

    async def test_cancel_timeout_does_not_release_replacement_reservation(self):
        await self.registered()
        await self.context.replace_order(self.context.orders()[0].order_id, OrderIntent(ROUTE, Side.BUY, 5, 100))
        await self.runtime.drain()
        before = self.runtime.orders.capital_usage()
        self.clock.advance_to(START + timedelta(seconds=90))
        await self.gateway.check_deadlines()
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(self.runtime.orders.capital_usage(), before)
        self.assertFalse(self.runtime.orders.ready)

    async def test_mapping_collision_and_missing_status_never_guess_success(self):
        transaction = await self.registered()
        for data in (dict(trans_id=transaction), dict(trans_id=transaction, status=3, order_num=BROKER_ID + 1)):
            with self.assertRaises(ValueError):
                await self.gateway.feed('OnTransReply', data)
            await self.runtime.drain()
            self.assertFalse(self.runtime.orders.ready)
        self.assertEqual(self.runtime.ledger.positions(), ())
