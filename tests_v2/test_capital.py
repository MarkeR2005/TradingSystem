from dataclasses import FrozenInstanceError, replace
from itertools import permutations
from random import Random
import unittest

from trading_system.capital import (
    AccountCapital, AccountKey, CapitalLeg, CapitalUsage, FxRate, FxSnapshot,
    StrategyCapital, allocate_capital,
)
from trading_system.domain import Instrument, InstrumentId, Route
from tests_v2.helpers import START


A = AccountKey('sim', 'a')
B = AccountKey('sim', 'b')
RUB = Instrument(InstrumentId('sim', 'SPBFUT', 'RUB-ASSET'), 'RUB')
USD = Instrument(InstrumentId('sim', 'FOREIGN', 'USD-ASSET'), 'USD')
FX = FxSnapshot('RUB', START, (FxRate('USD', 100),))


def leg(account=A, price=1, quantity=1, instrument=RUB):
    return CapitalLeg(Route(instrument, account.account_id), quantity, price)


def strategy(name, weight=1, legs=None, desired=1):
    return StrategyCapital(name, weight, (leg(),) if legs is None else legs, desired)


def account(key=A, equity=100, leverage=1, reserve=0, currency='RUB'):
    return AccountCapital(key, currency, equity, leverage, reserve)


class CapitalAllocationTests(unittest.TestCase):
    def test_desired_leverage_modifies_normalized_weight_not_account_capacity(self):
        plan = allocate_capital((account(equity=900),),
            (strategy('a', 80, desired=2), strategy('b', 20)), (), FX)
        self.assertAlmostEqual(plan.strategy('a').effective_weight, 800 / 9)
        self.assertAlmostEqual(plan.strategy('b').effective_weight, 100 / 9)
        self.assertEqual(plan.strategy('a').additional, 800)
        self.assertEqual(plan.strategy('b').additional, 100)
        self.assertEqual(plan.account(A).capacity, 900)
        doubled = allocate_capital((account(equity=900),),
            (strategy('a', 80, desired=4), strategy('b', 20, desired=2)), (), FX)
        self.assertEqual(plan, doubled)

    def test_account_leverage_and_cash_reserve_apply_before_allocation(self):
        plan = allocate_capital((account(equity=100, leverage=3, reserve=10),),
                                (strategy('one', desired=9),), (), FX)
        self.assertEqual(plan.account(A).capacity, 270)
        self.assertEqual(plan.strategy('one').units, 270)
        self.assertEqual(plan.strategy('one').effective_weight, 100)

    def test_manual_zero_weight_usage_is_preserved_and_reduces_free_capacity(self):
        usage = (CapitalUsage('manual', A, positions=100, orders=50),)
        plan = allocate_capital((account(equity=1000),),
            (strategy('manual', 0, ()), strategy('auto')), usage, FX)
        self.assertEqual(plan.strategy('manual').occupied, 150)
        self.assertEqual(plan.strategy('manual').additional, 0)
        self.assertEqual(plan.strategy('manual').effective_weight, 0)
        self.assertEqual(plan.strategy('auto').additional, 850)
        self.assertEqual(plan.account(A).positions, 100)
        self.assertEqual(plan.account(A).orders, 50)

    def test_occupied_floor_does_not_receive_another_share_of_the_free_pool(self):
        plan = allocate_capital((account(),), (strategy('a'), strategy('b')),
                                (CapitalUsage('a', A, positions=80),), FX)
        self.assertEqual(plan.strategy('a').total, 80)
        self.assertEqual(plan.strategy('a').additional, 0)
        self.assertEqual(plan.strategy('b').additional, 20)

    def test_reweighting_to_zero_never_reclaims_existing_positions_or_orders(self):
        plan = allocate_capital((account(),), (strategy('a', 0), strategy('b')),
                                (CapitalUsage('a', A, positions=60, orders=20),), FX)
        self.assertEqual(plan.strategy('a').occupied, 80)
        self.assertEqual(plan.strategy('a').units, 0)
        self.assertEqual(plan.strategy('b').additional, 20)

    def test_unknown_external_usage_is_counted_without_inventing_a_strategy(self):
        plan = allocate_capital((account(),), (strategy('auto'),),
                                (CapitalUsage('external', A, positions=30, orders=10),), FX)
        self.assertEqual(plan.strategy('auto').additional, 60)
        self.assertEqual(len(plan.strategies), 1)
        self.assertEqual(plan.account(A).positions + plan.account(A).orders, 40)

    def test_basket_preserves_two_to_one_quantities_and_shared_account_capacity(self):
        basket = strategy('basket', legs=(leg(A, 10, 2), leg(B, 30, 1)))
        single = strategy('single', legs=(leg(B, 10),))
        plan = allocate_capital((account(A, 100), account(B, 300)), (basket, single), (), FX)
        self.assertEqual(plan.strategy('basket').units, 5)
        self.assertEqual([item.quantity for item in plan.strategy('basket').legs], [10, 5])
        self.assertEqual(plan.strategy('basket').additional, 250)
        self.assertEqual(plan.strategy('single').additional, 150)
        self.assertEqual(plan.total_available, 0)

    def test_unusable_basket_share_is_redistributed_to_feasible_strategy(self):
        basket = strategy('basket', legs=(leg(A, 10), leg(B, 10)))
        single = strategy('single', legs=(leg(B, 1),))
        plan = allocate_capital((account(A, 0), account(B, 100)), (basket, single), (), FX)
        self.assertEqual(plan.strategy('basket').additional, 0)
        self.assertEqual(plan.strategy('single').additional, 100)

    def test_lot_rounding_and_repeated_pass_use_remaining_cash(self):
        plan = allocate_capital((account(equity=100),),
            (strategy('large', 9, (leg(price=60),)), strategy('small', 1, (leg(price=10),))), (), FX)
        self.assertEqual(plan.strategy('large').units, 1)
        self.assertEqual(plan.strategy('small').units, 4)
        self.assertEqual(plan.total_available, 0)

    def test_rounding_priority_prefers_more_accounts_then_weight_then_id(self):
        basket = strategy('z-basket', legs=(leg(A, 5), leg(B, 5)))
        solo = strategy('a-solo', legs=(leg(A, 10),))
        plan = allocate_capital((account(A, 10), account(B, 5)), (solo, basket), (), FX)
        self.assertEqual(plan.strategy('z-basket').units, 1)
        self.assertEqual(plan.strategy('a-solo').units, 0)
        tie = allocate_capital((account(equity=60),),
            (strategy('b', 1, (leg(price=60),)), strategy('a', 1, (leg(price=60),))), (), FX)
        self.assertEqual(tie.strategy('a').units, 1)
        weighted = allocate_capital((account(equity=60),),
            (strategy('a', 1, (leg(price=60),)), strategy('z', 2, (leg(price=60),))), (), FX)
        self.assertEqual(weighted.strategy('z').units, 1)

    def test_idle_accounts_and_unspendable_lot_dust_are_reported(self):
        plan = allocate_capital((account(A, 99), account(B, 20)),
                                (strategy('one', legs=(leg(price=10),)),), (), FX)
        self.assertEqual(plan.strategy('one').units, 9)
        self.assertEqual(plan.account(A).available, 9)
        self.assertEqual(plan.account(B).available, 20)
        self.assertEqual(plan.total_available, 29)

    def test_deficit_is_reported_without_removing_occupied_usage(self):
        usage = (CapitalUsage('manual', A, positions=120),)
        plan = allocate_capital((account(A, 100), account(B, 100)),
            (strategy('blocked'), strategy('other', legs=(leg(B),))), usage, FX)
        self.assertEqual(plan.account(A).deficit, 20)
        self.assertEqual(plan.account(A).positions, 120)
        self.assertEqual(plan.strategy('blocked').units, 0)
        self.assertEqual(plan.strategy('other').additional, 100)
        self.assertEqual(plan.total_deficit, 20)
        self.assertEqual(usage[0].positions, 120)

    def test_negative_equity_and_reserve_above_equity_give_zero_capacity(self):
        for source in (account(equity=-10), account(equity=10, reserve=20)):
            plan = allocate_capital((source,), (strategy('one'),),
                                    (CapitalUsage('one', A, positions=5),), FX)
            self.assertEqual(plan.account(A).capacity, 0)
            self.assertEqual(plan.account(A).deficit, 5)
            self.assertEqual(plan.strategy('one').total, 5)

    def test_fx_converts_capacity_legs_and_usage_to_one_currency(self):
        plan = allocate_capital((account(currency='USD', equity=10),),
            (strategy('one', legs=(leg(price=2, instrument=USD),)),),
            (CapitalUsage('external', A, positions=1, orders=1),), FX)
        self.assertEqual(plan.report_currency, 'RUB')
        self.assertEqual(plan.as_of, START)
        self.assertEqual(plan.account(A).capacity, 1000)
        self.assertEqual(plan.account(A).positions, 100)
        self.assertEqual(plan.strategy('one').units, 4)
        self.assertEqual(plan.strategy('one').additional, 800)

    def test_instrument_currency_can_differ_from_account_currency(self):
        plan = allocate_capital((account(equity=1000),),
            (strategy('one', legs=(leg(price=2, instrument=USD),)),), (), FX)
        self.assertEqual(plan.strategy('one').units, 5)
        self.assertEqual(plan.strategy('one').additional, 1000)

    def test_fractional_quantity_step_and_contract_multiplier_are_respected(self):
        instrument = replace(RUB, quantity_step=0.1, contract_multiplier=10)
        plan = allocate_capital((account(equity=1),),
            (strategy('one', legs=(leg(price=0.1, quantity=0.3, instrument=instrument),)),), (), FX)
        self.assertEqual(plan.strategy('one').units, 3)
        self.assertAlmostEqual(plan.strategy('one').legs[0].quantity, 0.9)
        self.assertAlmostEqual(plan.account(A).available, 0.1)

    def test_quantity_with_float_noise_is_valued_on_the_instrument_lot_grid(self):
        instrument = replace(RUB, quantity_step=0.1)
        plan = allocate_capital((account(equity=0.9),),
            (strategy('one', legs=(leg(quantity=0.1 + 0.2, instrument=instrument),)),), (), FX)
        self.assertEqual(plan.strategy('one').units, 3)
        self.assertEqual(plan.strategy('one').legs[0].quantity, 0.9)
        self.assertEqual(plan.total_available, 0)

    def test_large_weight_products_are_normalized_without_float_overflow(self):
        plan = allocate_capital((account(),),
            (strategy('a', 1e308, desired=1e308), strategy('b', 1e308, desired=1e308)), (), FX)
        self.assertEqual(plan.strategy('a').effective_weight, 50)
        self.assertEqual(plan.strategy('b').additional, 50)

    def test_zero_weights_keep_free_funds_and_usage_visible(self):
        plan = allocate_capital((account(),), (strategy('manual', 0, ()),),
                                (CapitalUsage('manual', A, positions=20),), FX)
        self.assertEqual(plan.total_additional, 0)
        self.assertEqual(plan.total_available, 80)
        self.assertEqual(plan.strategy('manual').total, 20)

    def test_input_permutation_does_not_change_plan(self):
        accounts = (account(A, 100), account(B, 100))
        strategies = (strategy('a'), strategy('b', legs=(leg(B),)),
                      strategy('c', legs=(leg(A, 3), leg(B, 2))))
        expected = allocate_capital(accounts, strategies, (), FX)
        for ordering in permutations(strategies):
            self.assertEqual(allocate_capital(tuple(reversed(accounts)), ordering, (), FX), expected)

    def test_large_lot_counts_are_batched_not_iterated_one_by_one(self):
        plan = allocate_capital((account(equity=10**12),), (strategy('one'),), (), FX)
        self.assertEqual(plan.strategy('one').units, 10**12)
        self.assertEqual(plan.total_available, 0)

    def test_capacity_conservation_across_shared_account_scenarios(self):
        random = Random(20261011)
        for _ in range(60):
            sources = (account(A, random.randrange(101), leverage=2, reserve=10),
                       account(B, random.randrange(101)))
            occupied = (CapitalUsage('a', A, positions=random.randrange(100)),
                        CapitalUsage('external', B, orders=random.randrange(100)))
            requests = (strategy('a', random.randrange(3), (leg(A, 3),)),
                        strategy('b', 1, (leg(B, 2),)),
                        strategy('basket', 2, (leg(A, 3, 2), leg(B, 5))))
            plan = allocate_capital(sources, requests, occupied, FX)
            for item in plan.accounts:
                self.assertGreaterEqual(item.additional, 0)
                self.assertGreaterEqual(item.available, 0)
                self.assertEqual(item.capacity + item.deficit,
                                 item.positions + item.orders + item.additional + item.available)
                if item.deficit:
                    self.assertEqual(item.additional, 0)
            self.assertEqual(sum(item.additional for item in plan.strategies), plan.total_additional)
            for request in requests:
                budget = plan.strategy(request.strategy_id)
                self.assertEqual(sum(item.capital for item in budget.legs), budget.additional)
                for source_leg, allocation in zip(request.legs, budget.legs):
                    self.assertEqual(allocation.quantity, source_leg.quantity_per_unit * budget.units)

    def test_result_is_immutable_and_no_input_snapshot_is_changed(self):
        source = account()
        plan = allocate_capital((source,), (strategy('one'),), (), FX)
        with self.assertRaises(FrozenInstanceError):
            plan.accounts[0].available = 100
        self.assertEqual(source.equity, 100)


class CapitalValidationTests(unittest.TestCase):
    def test_duplicate_identifiers_routes_and_usage_are_rejected(self):
        invalid = [((account(), account()), (strategy('a'),), ()),
                   ((account(),), (strategy('a'), strategy('a')), ()),
                   ((account(),), (strategy('a'),), (CapitalUsage('a', A), CapitalUsage('a', A)))]
        for accounts, strategies, usage in invalid:
            with self.subTest(accounts=accounts, strategies=strategies), self.assertRaises(ValueError):
                allocate_capital(accounts, strategies, usage, FX)
        with self.assertRaises(ValueError):
            strategy('a', legs=(leg(), leg()))

    def test_unknown_accounts_and_conflicting_instrument_metadata_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'account'):
            allocate_capital((account(),), (strategy('a', legs=(leg(B),)),), (), FX)
        with self.assertRaisesRegex(ValueError, 'account'):
            allocate_capital((account(),), (strategy('a'),), (CapitalUsage('external', B),), FX)
        other = replace(RUB, contract_multiplier=10)
        with self.assertRaisesRegex(ValueError, 'instrument'):
            allocate_capital((account(),), (strategy('a'), strategy('b', legs=(leg(instrument=other),))), (), FX)

    def test_missing_fx_is_rejected_instead_of_assuming_one_to_one(self):
        missing = FxSnapshot('RUB', START, ())
        for accounts, strategies in [((account(currency='USD'),), (strategy('a'),)),
                                     ((account(),), (strategy('a', legs=(leg(instrument=USD),)),))]:
            with self.assertRaisesRegex(ValueError, 'FX'):
                allocate_capital(accounts, strategies, (), missing)

    def test_invalid_amounts_weights_rates_and_lot_sizes_are_rejected(self):
        creators = [lambda: account(leverage=0), lambda: account(reserve=-1),
                    lambda: account(equity=float('nan')), lambda: strategy('a', -1),
                    lambda: strategy('a', desired=0), lambda: strategy('a', desired=float('inf')),
                    lambda: CapitalUsage('a', A, orders=-1), lambda: leg(quantity=0.5),
                    lambda: leg(price=0), lambda: FxRate('USD', 0),
                    lambda: FxSnapshot('RUB', START.replace(tzinfo=None), ()),
                    lambda: FxSnapshot('RUB', START, (FxRate('USD', 100), FxRate('USD', 90))),
                    lambda: strategy('a', legs=())]
        for creator in creators:
            with self.subTest(creator=creator), self.assertRaises(ValueError):
                creator()

    def test_account_identity_includes_gateway(self):
        other = AccountKey('other', 'a')
        other_instrument = replace(RUB, id=replace(RUB.id, gateway_id='other'))
        plan = allocate_capital((account(A, 10), account(other, 20)),
            (strategy('a'), strategy('b', legs=(leg(other, instrument=other_instrument),))), (), FX)
        self.assertEqual(plan.strategy('a').additional, 10)
        self.assertEqual(plan.strategy('b').additional, 20)
