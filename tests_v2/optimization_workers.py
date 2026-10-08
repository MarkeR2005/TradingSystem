import os

from trading_system.optimization import OptimizationInput
from trading_system.state import JsonObject


def select_period(data: OptimizationInput) -> JsonObject:
    return {'period': len(data.history) + 2, 'worker_pid': os.getpid()}


def failing_worker(data: OptimizationInput) -> JsonObject:
    raise ValueError('invalid optimization sample')


def busy_worker(data: OptimizationInput) -> JsonObject:
    from pathlib import Path
    marker = data.parameters['marker']
    if not isinstance(marker, str):
        raise ValueError('marker is required')
    Path(marker).write_text(str(os.getpid()))
    value = 0
    while True:
        value = (value + 1) % 99991
