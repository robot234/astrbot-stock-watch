from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sys

CHINA = timezone(timedelta(hours=8))
OWNER = Path('/home/pi/apps/stock-fund-fetch-20260929')
BOARDS = ('sh.600', 'sh.601', 'sh.603', 'sh.605', 'sz.000', 'sz.001',
          'sz.002', 'sz.003', 'sz.300', 'sz.301')
FIELDS = 'date,code,open,high,low,close,preclose,volume,amount,adjustflag,tradestatus,isST'


def now():
    return datetime.now(CHINA).isoformat()


def save(path, value):
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
    if path.suffix == '.gz':
        encoded = gzip.compress(encoded, mtime=0)
    with path.open('xb') as stream:
        stream.write(encoded)
    return hashlib.sha256(encoded).hexdigest()


def drain(result):
    rows = []
    if result is None:
        return {'provider_code': None, 'fields': [], 'rows': rows, 'status': 'partial'}
    while result.error_code == '0' and result.next():
        rows.append(result.get_row_data())
    return {'provider_code': result.error_code, 'fields': list(result.fields), 'rows': rows,
            'status': 'observed' if result.error_code == '0' else 'partial'}


def universe(payload, target):
    identities = [dict(zip(payload['fields'], row)) for row in payload['rows']]
    codes = sorted(identity['code'] for identity in identities if identity.get('type') == '1'
                   and identity['code'].startswith(BOARDS) and identity.get('ipoDate')
                   and identity['ipoDate'] <= target
                   and (not identity.get('outDate') or target < identity['outDate'])
                   and identity.get('status') == '1')
    if len(set(codes)) != len(codes):
        raise ValueError('duplicate current identities')
    return codes


def capture(output, target='2026-09-30', max_calls=10000):
    local = datetime.now(CHINA)
    if '16:10' <= local.strftime('%H:%M') <= '20:30':
        raise RuntimeError('legacy_capture_window_do_not_start')
    if max_calls > 15000 or max_calls < 3:
        raise ValueError('research message budget must be 3..15000')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if (output / 'ATTEMPT_STARTED.json').exists():
        raise RuntimeError('no automatic repeat of this batch')
    sys.path.insert(0, str(OWNER))
    from shared_baostock import owner_session
    import baostock as client
    socket.setdefaulttimeout(45)
    report = {'source': 'BaoStock guarded unadjusted daily', 'started': now(), 'target': target,
              'max_run_messages': max_calls, 'soft_daily': 35000, 'hard_daily': 40000,
              'run_messages': 0, 'captured_codes': 0, 'failed_codes': [], 'status': 'partial',
              'raw_manifest': [], 'retries': 0, 'process_pid': os.getpid()}
    guard = None

    def started():
        save(output / 'ATTEMPT_STARTED.json', {'first_transport_send_at': now(), 'target': target,
                                              'process_pid': os.getpid()})

    try:
        with owner_session(purpose='ultrashort-reversal-v1-latest-input', max_calls=max_calls,
                           wait_seconds=7200, work_seconds=7200, target_date=local.date().isoformat(),
                           stop_file=output / 'STOP', on_first_send=started) as guard:
            report['daily_messages_before'] = guard.ledger.get('days', {}).get(guard.today(), {}).get('calls', 0)
            with redirect_stdout(io.StringIO()):
                login = guard.login(client)
            if login is None or login.error_code != '0':
                raise RuntimeError('guarded_login_failed')
            try:
                basic = drain(client.query_stock_basic())
                basic.update(received_at=now(), requested_code=None)
                save(output / 'basic.json', basic)
                if basic['status'] != 'observed' or not basic['rows']:
                    raise RuntimeError('basic_all_unverified')
                calendar = drain(client.query_trade_dates(start_date='2026-08-24', end_date=target))
                calendar.update(received_at=now())
                save(output / 'calendar.json', calendar)
                if calendar['status'] != 'observed' or not calendar['rows']:
                    raise RuntimeError('calendar_unverified')
                trading = [row[0] for row in calendar['rows'] if row[1] == '1']
                if not trading or trading[-1] != target:
                    raise RuntimeError('target_not_latest_complete_trading_day')
                forward_calendar = drain(client.query_trade_dates(start_date='2026-10-08', end_date='2027-03-31'))
                forward_calendar.update(received_at=now(), purpose='calendar_only_no_future_quotes')
                save(output / 'forward_calendar.json', forward_calendar)
                codes = universe(basic, target)
                report['requested_codes'] = codes
                report['identity_count'] = len(codes)
                if not codes or len(codes) + 2 > guard.remaining():
                    raise RuntimeError('budget_insufficient_for_universe')
                raw = output / 'raw'
                raw.mkdir(exist_ok=True)
                for code in codes:
                    requested_at = now()
                    payload = drain(client.query_history_k_data_plus(code, FIELDS, start_date='2026-08-24',
                                                                    end_date=target, frequency='d', adjustflag='3'))
                    payload.update(requested_code=code, adjustflag='3', requested_at=requested_at,
                                   received_at=now(), start='2026-08-24', end=target, attempts=1, frequency='d')
                    dates = [row[payload['fields'].index('date')] for row in payload['rows']] if 'date' in payload['fields'] else []
                    if payload['status'] != 'observed' or not dates or dates[-1] != target:
                        payload['status'] = 'partial'
                        report['failed_codes'].append(code)
                    path = raw / (code + '.json.gz')
                    checksum = save(path, payload)
                    report['raw_manifest'].append({'code': code, 'sha256': checksum,
                                                   'file': str(path.relative_to(output)), 'status': payload['status'],
                                                   'rows': len(payload['rows']), 'last_date': dates[-1] if dates else None})
                    report['captured_codes'] += 1
                    if report['captured_codes'] % 250 == 0:
                        print(json.dumps({'stage': 'latest_capture', 'captured': report['captured_codes'],
                                          'run_messages': guard.run_calls, 'time': now()}), flush=True)
                report['status'] = 'complete' if not report['failed_codes'] else 'partial'
            finally:
                with redirect_stdout(io.StringIO()):
                    guard.logout(client)
                report['run_messages'] = guard.run_calls
                report['daily_messages_after'] = guard.ledger.get('days', {}).get(guard.today(), {}).get('calls', 0)
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        if guard is not None:
            report['run_messages'] = guard.run_calls
    report['finished'] = now()
    save(output / 'capture_report.json', report)
    print(json.dumps({key: value for key, value in report.items() if key not in ('raw_manifest', 'requested_codes')}, ensure_ascii=False), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--target', default='2026-09-30')
    parser.add_argument('--max-calls', type=int, default=10000)
    args = parser.parse_args()
    capture(args.output, args.target, args.max_calls)


if __name__ == '__main__':
    main()
