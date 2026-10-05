from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import socket
import sys


CHINA = timezone(timedelta(hours=8))
OWNER = Path('/home/pi/apps/stock-fund-fetch-20260929')


def timestamp():
    return datetime.now(CHINA).isoformat()


def save(path, value):
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
    with Path(path).open('xb') as stream:
        stream.write(encoded)
    return hashlib.sha256(encoded).hexdigest()


def capture(output, target, previous_messages=4614):
    current = datetime.now(CHINA)
    if '16:10' <= current.strftime('%H:%M') <= '20:30':
        raise ValueError('existing_collection_window_do_not_start')
    if not 0 <= previous_messages <= 14900:
        raise ValueError('insufficient_cumulative_research_budget')
    if target > current.date().isoformat():
        raise ValueError('future_classification_date')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if (output / 'ATTEMPT_STARTED.json').exists() or (output / 'report.json').exists():
        raise ValueError('single_attempt_already_recorded')
    sys.path.insert(0, str(OWNER))
    from shared_baostock import owner_session
    import baostock as client
    socket.setdefaulttimeout(45)
    report = {'target': target, 'started_at': timestamp(), 'source': 'BaoStock query_stock_industry',
              'status': 'partial', 'run_messages': 0, 'previous_research_messages': previous_messages,
              'max_run_messages': 100, 'soft_daily': 35000, 'hard_daily': 40000, 'retries': 0}
    guard = None

    def first_send():
        save(output / 'ATTEMPT_STARTED.json', {'first_transport_send_at': timestamp()})

    try:
        with owner_session(purpose='llm-sector-first-v0-industry', max_calls=100,
                           wait_seconds=120, work_seconds=180, target_date=current.date().isoformat(),
                           stop_file=output / 'STOP', on_first_send=first_send) as guard:
            report['daily_messages_before'] = guard.ledger.get('days', {}).get(guard.today(), {}).get('calls', 0)
            with redirect_stdout(io.StringIO()):
                login = guard.login(client)
            if login is None or login.error_code != '0':
                raise ValueError('login_failed')
            try:
                requested_at = timestamp()
                result = client.query_stock_industry(date=target)
                rows = []
                while result.error_code == '0' and result.next():
                    rows.append(result.get_row_data())
                payload = {'source': 'BaoStock', 'method': 'query_stock_industry', 'requested_date': target,
                           'fields': list(result.fields), 'rows': rows, 'provider_code': result.error_code,
                           'requested_at': requested_at, 'received_at': timestamp(), 'attempts': 1,
                           'cache_hit': 'unknown', 'hash_kind': 'SDK_string_table_not_raw_TCP'}
                report['table_sha256'] = save(output / 'industry.json', payload)
                report['rows'] = len(rows)
                report['status'] = 'complete' if result.error_code == '0' and rows else 'partial'
            finally:
                with redirect_stdout(io.StringIO()):
                    guard.logout(client)
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
    if guard is not None:
        report['run_messages'] = guard.run_calls
        report['daily_messages_after'] = guard.ledger.get('days', {}).get(guard.today(), {}).get('calls', 0)
    report['cumulative_research_messages'] = previous_messages + report['run_messages']
    report['finished_at'] = timestamp()
    save(output / 'report.json', report)
    print(json.dumps(report, ensure_ascii=False))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--target', default='2026-09-30')
    parser.add_argument('--previous-messages', type=int, default=4614)
    args = parser.parse_args()
    report = capture(args.output, args.target, args.previous_messages)
    return 0 if report['status'] == 'complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
