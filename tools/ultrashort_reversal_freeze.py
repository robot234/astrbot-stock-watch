from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ultrashort_reversal_research import BOARDS, FIELDS, REGISTRATION, RULE, digest, features, selection, write_json

CHINA = timezone(timedelta(hours=8))
DEADLINE = datetime(2026, 10, 8, 9, tzinfo=CHINA)


def read_capture(source):
    source = Path(source)
    report = json.loads((source / 'capture_report.json').read_text(encoding='utf-8'))
    basic = json.loads((source / 'basic.json').read_text(encoding='utf-8'))
    calendar = json.loads((source / 'calendar.json').read_text(encoding='utf-8'))
    if basic['status'] != 'observed' or calendar['status'] != 'observed':
        raise ValueError('unverified basic or calendar')
    dates = sorted(row[0] for row in calendar['rows'] if row[1] == '1')
    if dates[-1] != report['target']:
        raise ValueError('target calendar mismatch')
    identities = sorted([dict(zip(basic['fields'], row)) for row in basic['rows']
                         if dict(zip(basic['fields'], row)).get('type') == '1' and row[0].startswith(BOARDS)
                         and dict(zip(basic['fields'], row)).get('status') == '1'
                         and dict(zip(basic['fields'], row)).get('ipoDate')
                         and dict(zip(basic['fields'], row))['ipoDate'] <= dates[-1]
                         and (not dict(zip(basic['fields'], row))['outDate']
                              or dates[-1] < dict(zip(basic['fields'], row))['outDate'])], key=lambda identity: identity['code'])
    codes = [identity['code'] for identity in identities]
    if codes != report['requested_codes'] or len(set(codes)) != len(codes):
        raise ValueError('identity denominator mismatch')
    market = {name: np.full((len(dates), len(codes)), np.nan) for name in FIELDS}
    expected = np.zeros(market['close'].shape, dtype=bool)
    age = np.zeros(expected.shape, dtype=int)
    ordinary = np.ones(expected.shape, dtype=bool)
    date_index = {date: index for index, date in enumerate(dates)}
    manifest = {item['code']: item for item in report['raw_manifest']}
    dates_array = np.array(dates, dtype='datetime64[D]')
    for column, identity in enumerate(identities):
        expected[:, column] = dates_array >= np.datetime64(identity['ipoDate'])
        age[:, column] = (dates_array - np.datetime64(identity['ipoDate'])).astype(int)
        if '退' in identity.get('code_name', '') or 'ST' in identity.get('code_name', '').upper():
            ordinary[:, column] = False
        item = manifest.get(identity['code'])
        if item is None:
            continue
        path = source / item['file']
        if digest(path) != item['sha256']:
            raise ValueError('latest raw hash mismatch')
        payload = json.loads(gzip.decompress(path.read_bytes()))
        if payload['requested_code'] != identity['code'] or payload['adjustflag'] != '3':
            raise ValueError('latest raw provenance mismatch')
        if payload.get('provider_code') != '0':
            continue
        seen = set()
        for row in payload['rows']:
            record = dict(zip(payload['fields'], row))
            if record['date'] in seen or record['code'] != identity['code'] or record['adjustflag'] != '3':
                raise ValueError('duplicate or wrong raw row')
            seen.add(record['date'])
            if record['date'] not in date_index:
                raise ValueError('unexpected raw date')
            index = date_index[record['date']]
            for name in FIELDS:
                value = record.get(name, '')
                market[name][index, column] = float(value) if value else np.nan
    market.update(dates=np.array(dates), codes=np.array(codes), expected=expected, age=age,
                  ordinary=ordinary, identities=identities)
    return market, report


def freeze(source, report_path, output, current_time=None):
    current_time = current_time or datetime.now(CHINA)
    if current_time >= DEADLINE:
        raise ValueError('opening_deadline_passed_do_not_select_from_future')
    market, capture = read_capture(source)
    report = json.loads(Path(report_path).read_text(encoding='utf-8'))
    if report['registration_commit'] != REGISTRATION or report['rule'] != RULE or report['kind'] != 'test':
        raise ValueError('research report registration mismatch')
    derived = features(market)
    index = len(market['dates']) - 1
    choices = selection(market, derived, index)
    listing = []
    for rank, column in enumerate(choices, 1):
        identity = market['identities'][column]
        listing.append({'rank': rank, 'code': identity['code'], 'name': identity.get('code_name'),
                        'five_day_continuous_return': float(derived['score'][index, column]),
                        'amount20': float(derived['amount20'][index, column]),
                        'close': float(market['close'][index, column]), 'preclose': float(market['preclose'][index, column]),
                        'isST': int(market['isST'][index, column]), 'tradestatus': int(market['tradestatus'][index, column]),
                        'volume': float(market['volume'][index, column]), 'listed_calendar_days': int(market['age'][index, column]),
                        'derived_upper': float(derived['upper'][index, column]), 'derived_lower': float(derived['lower'][index, column]),
                        'raw_sha256': next(item['sha256'] for item in capture['raw_manifest'] if item['code'] == identity['code']),
                        'risk_basis': 'BaoStock_status_and_preregistered_regime_proxy_not_formal_permission'})
    frozen = {'rule': RULE, 'registration_commit': REGISTRATION, 'frozen_at': current_time.isoformat(),
              'input_as_of': str(market['dates'][-1]), 'latest_capture_status': capture['status'],
              'selected_version': report['selected'], 'selection_reason': report['selection_reason'],
              'label': report['label'], 'list': listing, 'empty_slots': 5 - len(listing),
              'identity_denominator': len(market['codes']), 'eligible_count': int(derived['eligible'][index].sum()),
              'unknown_qualification_count': int((~derived['known'][index] & market['expected'][index]).sum()),
              'ST_count_retained': int((market['isST'][index] == 1).sum()),
              'suspended_count_retained': int((market['tradestatus'][index] == 0).sum()),
              'source_capture_started': capture['started'], 'source_capture_finished': capture['finished'],
              'source_capture_report_sha256': digest(Path(source) / 'capture_report.json'),
              'basic_sha256': digest(Path(source) / 'basic.json'), 'calendar_sha256': digest(Path(source) / 'calendar.json'),
              'test_report_sha256': digest(report_path), 'source_manifest_sha256': digest(Path(report_path).parent / 'source_manifest.json'),
              'historical_evaluation_status': 'incomplete_account_and_noncausal_random_pool_not_passed_no_return_retest',
              'implementation_revision': 'point_in_time_delisting_fix_after_historical_run_no_return_retest',
              'research_messages': capture['run_messages'], 'not_formal_recommendation': True,
              'hash_kind': 'SDK_original_string_table_serialization_gzip_bytes_not_raw_TCP',
              'price_basis': 'unadjusted',
              'not_qq_push': True, 'opening_not_seen': True,
              'next_observation': '2026-10-08 original 18:00 BaoStock archive; missing means unknown; no refetch'}
    write_json(output, frozen)
    return frozen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--report', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result = freeze(args.source, args.report, output)
    print(json.dumps({'frozen_at': result['frozen_at'], 'as_of': result['input_as_of'],
                      'label': result['label'], 'codes': [row['code'] for row in result['list']]}, ensure_ascii=False))


if __name__ == '__main__':
    main()
