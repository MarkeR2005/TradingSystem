"""Explicit local terminal diagnostic: python -m trading_system.quik_probe --help."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from typing import Any

from .persistence import DurableJournal
from .quik_readonly import ReadOnlyQuik
from .quik_snapshot import capture_snapshot


async def run_probe(output: Path, *, profile: str, expected_version: str, request_port: int = 34130,
                    callback_port: int = 34131, class_code: str | None = None, symbol: str | None = None,
                    timeout: float = 10, observe_seconds: float = 2) -> tuple[dict[str, Any], int]:
    if not 0 <= observe_seconds <= 10:
        raise ValueError('observation period must be between zero and ten seconds')
    # Never overwrite a previous diagnostic run or mix demo/live captures.
    output.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {'schema': 1, 'mode': 'read_only', 'profile': profile,
                              'expected_version': expected_version, 'status': 'failed', 'reconciled': False}
    code = 1
    try:
        with DurableJournal(output / 'wire') as journal:
            client = ReadOnlyQuik(journal=journal, profile=profile, expected_version=expected_version,
                                 request_port=request_port, callback_port=callback_port, timeout=timeout)
            try:
                async with asyncio.timeout(60):
                    await client.connect()
                    await asyncio.sleep(observe_seconds)
                    snapshot = await capture_snapshot(client, class_code=class_code, symbol=symbol)
            finally:
                await client.close()
            if client.fault is not None:
                raise RuntimeError(client.fault)
            report.update(status='captured', snapshot=snapshot, callbacks=client.callback_count)
            code = 0 if snapshot['repeatable_observation'] else 2
    except Exception as error:
        report['error'] = str(error) or type(error).__name__
    with (output / 'report.json').open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(report, stream, ensure_ascii=False, allow_nan=False, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    return report, code


def main() -> int:
    parser = argparse.ArgumentParser(description='Read-only QUIK diagnostic. No order submission or OMS changes.')
    parser.add_argument('--profile', required=True, choices=('demo-quik', 'psb-live'))
    parser.add_argument('--expect-version', required=True, help='Expected numeric version prefix, e.g. 13.2')
    parser.add_argument('--output', required=True, type=Path, help='New directory for this run; must not already exist')
    parser.add_argument('--request-port', type=int, default=34130)
    parser.add_argument('--callback-port', type=int, default=34131)
    parser.add_argument('--class-code', help='Optional exact class from discovery output')
    parser.add_argument('--symbol', help='Optional instrument; requires --class-code')
    parser.add_argument('--timeout', type=float, default=10)
    parser.add_argument('--observe-seconds', type=float, default=2)
    args = parser.parse_args()
    try:
        report, code = asyncio.run(run_probe(args.output, profile=args.profile, expected_version=args.expect_version,
            request_port=args.request_port, callback_port=args.callback_port, class_code=args.class_code,
            symbol=args.symbol, timeout=args.timeout, observe_seconds=args.observe_seconds))
    except (OSError, ValueError) as error:
        print(f'Cannot start/save diagnostic: {error}', file=sys.stderr)
        return 1
    if report['status'] == 'captured':
        snapshot = report['snapshot']
        runtime = snapshot['runtime']
        print(f"READ_ONLY {runtime['profile']}: QUIK {runtime['quik_version']}; {runtime['lua_version']}")
        print('Classes: ' + ', '.join(snapshot['classes']))
        print('Rows: ' + ', '.join(f'{name}={len(rows)}' for name, rows in snapshot['tables'].items()))
        print(f"Repeatable observation: {snapshot['repeatable_observation']}; reconciled: False")
    else:
        print('READ_ONLY diagnostic failed: ' + report['error'], file=sys.stderr)
    print(f"Report: {args.output / 'report.json'}")
    return code


if __name__ == '__main__':
    sys.exit(main())
