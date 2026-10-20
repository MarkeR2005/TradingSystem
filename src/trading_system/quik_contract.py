"""Strict SPBFUT contract for quik-python 1.2.0 raw callbacks; no network I/O."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from math import isfinite
import re

from .domain import OrderIntent, Route, Side

MAX_TRANSACTION_ID = 2_147_483_647  # Application namespace, deliberately bounded; never wrap.
COMMISSION_FIELDS = frozenset({'broker_comission', 'exchange_comission', 'clearing_comission', 'tech_center_comission'})


def integer(value: object, *, zero: bool = False, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, str) and re.fullmatch(r'[0-9]{1,19}', value):
        value = int(value)
    if type(value) is not int or not (0 if zero else 1) <= value <= maximum:
        raise ValueError('expected a lossless integer in the supported range')
    return value


def decimal(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError('expected a numeric price or commission')
    if isinstance(value, str) and len(value) > 80:
        raise ValueError('numeric field is too long')
    try:
        result = Decimal(str(value))
        if not result.is_finite() or not isfinite(float(result)) or (result != 0 and float(result) == 0):
            raise ValueError('numeric value is not finite or reportable')
        return result
    except (InvalidOperation, OverflowError) as error:
        raise ValueError('invalid numeric field') from error


def identifier(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,64}', value):
        raise ValueError('invalid routing identifier')
    return value


@dataclass(frozen=True)
class QuikRoute:
    route: Route
    price_step: Decimal
    commission_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        instrument = self.route.instrument
        for value in (self.route.account_id, instrument.id.gateway_id, instrument.id.symbol):
            identifier(value)
        if instrument.id.venue != 'SPBFUT' or instrument.quantity_step != 1 or instrument.currency != 'RUB':
            raise ValueError('this contract supports RUB SPBFUT futures in integer lots')
        if not isinstance(self.price_step, Decimal) or decimal(self.price_step) <= 0:
            raise ValueError('price step must be a positive Decimal')
        if (not isinstance(self.commission_fields, tuple)
                or len(set(self.commission_fields)) != len(self.commission_fields)
                or any(field not in COMMISSION_FIELDS for field in self.commission_fields)):
            raise ValueError('declare distinct known commission components; empty explicitly means zero')

    def price(self, value: object) -> Decimal:
        result = decimal(value)
        try:
            if result % self.price_step:
                raise ValueError('price is off the configured tick grid')
        except InvalidOperation as error:
            raise ValueError('price precision exceeds contract range') from error
        return result


def limit_transaction(spec: QuikRoute, intent: OrderIntent, transaction_id: int) -> dict[str, str]:
    integer(transaction_id, maximum=MAX_TRANSACTION_ID)
    if intent.route != spec.route or intent.limit_price is None:
        raise ValueError('exact configured route and limit price are required')
    lots = decimal(intent.quantity)
    if lots != lots.to_integral_value():
        raise ValueError('futures quantity must contain integer lots')
    quantity = integer(int(lots), maximum=MAX_TRANSACTION_ID)
    price = spec.price(intent.limit_price)
    return {'TRANS_ID': str(transaction_id), 'ACTION': 'NEW_ORDER', 'CLASSCODE': spec.route.instrument.id.venue,
            'SECCODE': spec.route.instrument.id.symbol, 'ACCOUNT': spec.route.account_id,
            'OPERATION': 'B' if intent.side is Side.BUY else 'S', 'TYPE': 'L',
            'QUANTITY': str(quantity), 'PRICE': format(price, 'f')}


def route_spec(data: Mapping[str, object], specs: tuple[QuikRoute, ...]) -> QuikRoute:
    key = tuple(identifier(data.get(field)) for field in ('class_code', 'sec_code', 'account'))
    match = next((s for s in specs if (s.route.instrument.id.venue, s.route.instrument.id.symbol,
                                      s.route.account_id) == key), None)
    if match is None:
        raise ValueError('callback has no exact configured account/instrument route')
    return match


def timestamp(value: object, offset: timedelta) -> datetime:
    if not isinstance(value, Mapping):
        raise ValueError('trade datetime must be supplied')
    names = ('year', 'month', 'day', 'hour', 'min', 'sec')
    parts = [integer(value.get(name), zero=name in ('hour', 'min', 'sec'), maximum=9999) for name in names]
    ms = integer(value.get('ms', 0), zero=True, maximum=999)
    micro = integer(value.get('mcs', ms * 1000), zero=True, maximum=999999)
    if 'ms' in value and 'mcs' in value and micro // 1000 != ms:
        raise ValueError('conflicting millisecond and microsecond timestamps')
    year, month, day, hour, minute, second = parts
    return datetime(year, month, day, hour, minute, second, micro, timezone(offset)).astimezone(UTC)


@dataclass(frozen=True)
class QuikTrade:
    trade_id: int
    order_id: int
    route: Route
    side: Side
    quantity: int
    price: float
    commission: float
    timestamp: datetime


def parse_trade(data: Mapping[str, object], specs: tuple[QuikRoute, ...], offset: timedelta) -> QuikTrade:
    spec = route_spec(data, specs)
    flags = integer(data.get('flags'), zero=True)
    if flags & 16:
        raise ValueError('cancelled trade requires explicit correction reconciliation')
    commission = sum((decimal(data.get(field)) for field in spec.commission_fields), Decimal())
    return QuikTrade(integer(data.get('trade_num')), integer(data.get('order_num')), spec.route,
                     Side.SELL if flags & 4 else Side.BUY, integer(data.get('qty'), maximum=MAX_TRANSACTION_ID),
                     float(spec.price(data.get('price'))), float(decimal(commission)), timestamp(data.get('datetime'), offset))


@dataclass(frozen=True)
class QuikOrder:
    transaction_id: int
    order_id: int
    route: Route
    side: Side
    quantity: int
    balance: int
    price: float
    active: bool
    cancelled: bool


def parse_order(data: Mapping[str, object], specs: tuple[QuikRoute, ...]) -> QuikOrder:
    spec = route_spec(data, specs)
    flags = integer(data.get('flags'), zero=True)
    quantity = integer(data.get('qty'), maximum=MAX_TRANSACTION_ID)
    balance = integer(data.get('balance'), zero=True, maximum=quantity)
    active, cancelled = bool(flags & 1), bool(flags & 2)
    if not flags & 8 or (active and cancelled) or (not active and not cancelled and balance != 0):
        raise ValueError('unsupported or contradictory order flags/balance')
    return QuikOrder(integer(data.get('trans_id'), zero=True, maximum=MAX_TRANSACTION_ID),
                     integer(data.get('order_num')), spec.route, Side.SELL if flags & 4 else Side.BUY,
                     quantity, balance, float(spec.price(data.get('price'))), active, cancelled)
