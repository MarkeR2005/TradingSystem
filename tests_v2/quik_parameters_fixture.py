"""SiZ6 metadata, synthetic parameters and observed instrument margin; no account data."""


def security():
    return {'class_code': 'SPBFUT', 'code': 'SiZ6', 'sec_code': 'SiZ6', 'name': 'Si-12.26',
            'min_price_step': 1.0, 'lot_size': 1000, 'qty_multiplier': 1, 'qty_scale': 0,
            'scale': 0, 'step_price_currency': 'SUR', 'trade_currency': 'SUR', 'exp_date': 20261217}


def parameters():
    result = {name: {'result': '1', 'param_type': '1', 'param_value': value, 'param_image': value}
              for name, value in {'SEC_PRICE_STEP': '1.000000', 'STEPPRICE': '1.000000',
                                  'BUYDEPO': '10000.000000', 'SELLDEPO': '12000.000000'}.items()}
    result['CURSTEPPRICE'] = {'result': '1', 'param_type': '3', 'param_value': '0', 'param_image': 'SUR'}
    return result


def demo_fractional_margin_parameters():
    """Public instrument fields observed on demo QUIK 13.2.0.31, 2026-10-16."""
    result = parameters()
    result['BUYDEPO'] = {'result': '1', 'param_type': '2',
                         'param_value': '7876.050000', 'param_image': '7\u00a0876.05'}
    result['SELLDEPO'] = {'result': '1', 'param_type': '2',
                          'param_value': '7999.560000', 'param_image': '7\u00a0999.56'}
    return result
