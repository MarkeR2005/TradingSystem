"""Synthetic financial observations: units, unavailable data and a changing margin."""

from copy import deepcopy
from typing import Any

from trading_system.quik_parameters import build_financial_report


def main() -> None:
    # Public SiZ6-like metadata; margin values below are invented, not a broker quote.
    security: dict[str, Any] = {'class_code': 'SPBFUT', 'code': 'SiZ6', 'sec_code': 'SiZ6',
        'min_price_step': 1, 'lot_size': 1000, 'qty_multiplier': 1, 'qty_scale': 0,
        'step_price_currency': 'SUR', 'trade_currency': 'SUR'}
    parameters: dict[str, Any] = {name: {'result': '1', 'param_type': '1', 'param_value': value,
                                       'param_image': value}
        for name, value in {'SEC_PRICE_STEP': '1.000000', 'STEPPRICE': '1.000000',
                            'BUYDEPO': '10000.000000', 'SELLDEPO': '12000.000000'}.items()}
    parameters['CURSTEPPRICE'] = {'result': '1', 'param_type': '3', 'param_value': '0', 'param_image': 'SUR'}
    result = build_financial_report(security, parameters, security, parameters)
    assert result['complete'] and result['repeatable_observation']
    assert result['values']['lot_size'] == '1000' and result['point_value_candidate'] == '1'
    assert not result['eligible_for_trading']
    print('Synthetic SiZ6: underlying lot 1000; price step 1; step value 1 RUB; point value candidate 1.')

    unavailable = deepcopy(parameters)
    unavailable['BUYDEPO'].update(result='0', param_value='0')
    result = build_financial_report(security, unavailable, security, unavailable)
    assert not result['complete'] and result['values']['buy_depo'] is None
    assert result['point_value_candidate'] is None
    print('Missing BUYDEPO stays unavailable; no zero margin or default multiplier is substituted.')

    changed = deepcopy(parameters)
    changed['SELLDEPO']['param_value'] = '13000'
    result = build_financial_report(security, parameters, security, changed)
    assert 'parameters_changed' in result['issues'] and result['point_value_candidate'] is None
    print('Changed margin invalidates the observation; no Route, capital policy or trading permission is created.')


if __name__ == '__main__':
    main()
