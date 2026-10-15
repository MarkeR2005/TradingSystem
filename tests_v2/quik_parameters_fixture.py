"""Public SiZ6-shaped metadata and invented financial values; no account data."""


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
