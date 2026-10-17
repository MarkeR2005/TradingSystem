"""Invented account identities and amounts, shaped like demo futures tables."""


def accounts():
    return [{'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'class_codes': '|SPBFUT|SPBOPT|'},
            {'firmid': 'DEMO', 'trdaccid': 'EQUITY', 'class_codes': '|QJSIM|'}]


def limit_row(limit_type=0, currcode='SUR'):
    return {'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'limit_type': limit_type, 'currcode': currcode,
            'cbplimit': 100000.25, 'cbplused': 15000.5, 'cbplplanned': 84999.75,
            'cbplused_for_orders': 5000.25, 'cbplused_for_positions': 10000.25,
            'varmargin': -12.5, 'real_varmargin': 25.5, 'kgo': 1,
            'cbp_prev_limit': 90000, 'go_without_orders': 11000.5, 'go_planned': 12000.5,
            'risk_level': 0}


def limit_rows():
    cash = limit_row()
    clearing = limit_row(3, '')
    clearing.update(cbplimit=0, cbplplanned=0)
    collateral = limit_row(6, 'SUR')
    collateral.update(cbplimit=-9000, cbplplanned=0)
    fx_collateral = limit_row(7, 'RUB')
    fx_collateral.update(cbplimit=-9000, cbplplanned=0)
    return [cash, clearing, collateral, fx_collateral]
