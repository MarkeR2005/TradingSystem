"""Explicit JSON state, independent of the financial ledger."""

from math import isfinite


type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]


def _copy(value: object) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError('state numbers must be finite')
        return value
    if isinstance(value, list):
        return [_copy(item) for item in value]
    if isinstance(value, dict):
        result: JsonObject = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError('state keys must be strings')
            result[key] = _copy(item)
        return result
    raise ValueError('state must contain JSON values only')


def json_object(value: object) -> JsonObject:
    try:
        result = _copy(value)
    except RecursionError as error:
        raise ValueError('state must not contain cycles') from error
    if not isinstance(result, dict):
        raise ValueError('state must be a JSON object')
    return result
