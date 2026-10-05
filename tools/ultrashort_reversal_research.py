from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BOARDS = ('sh.600', 'sh.601', 'sh.603', 'sh.605', 'sz.000', 'sz.001',
          'sz.002', 'sz.003', 'sz.300', 'sz.301')
FIELDS = ('open', 'high', 'low', 'close', 'preclose', 'volume', 'amount',
          'tradestatus', 'isST')
REGISTRATION = '818e9fb4f0af748b3f47ba161dc5239fc403e20d'
RULE = 'ULTRASHORT_REVERSAL_V1'
SEEDS = tuple(range(2026100500, 2026100520))


def stamp():
    return datetime.now(timezone.utc).astimezone().isoformat()


def digest(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            checksum.update(block)
    return checksum.hexdigest()


def write_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def money(value):
    return float(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def cents(values):
    return np.floor(np.asarray(values) * 100 + 0.50000001)


def read_market(source, kind):
    source = Path(source)
    if kind == 'design':
        basic_path = source / 'inputs/baostock_basic.json'
        calendar_path = source / 'calendar.json'
    else:
        basic_path = source / 'baostock_basic.json'
        calendar_path = source / 'baostock_calendar.json'
    basic = json.loads(basic_path.read_text(encoding='utf-8'))
    calendar = json.loads(calendar_path.read_text(encoding='utf-8'))
    dates = sorted(row[0] for row in calendar['rows'] if row[1] == '1')
    if kind == 'test':
        dates = [date for date in dates if '2024-01-02' <= date <= '2026-09-24']
    identities = sorted([dict(zip(basic['fields'], row)) for row in basic['rows']
                         if dict(zip(basic['fields'], row)).get('type') == '1'
                         and row[0].startswith(BOARDS)
                         and dict(zip(basic['fields'], row))['ipoDate'] <= dates[-1]
                         and (not dict(zip(basic['fields'], row))['outDate']
                              or dict(zip(basic['fields'], row))['outDate'] >= dates[0])],
                        key=lambda identity: identity['code'])
    if len({identity['code'] for identity in identities}) != len(identities):
        raise ValueError('duplicate basic identities')
    codes = [identity['code'] for identity in identities]
    code_index = {code: column for column, code in enumerate(codes)}
    date_index = {date: index for index, date in enumerate(dates)}
    market = {name: np.full((len(dates), len(codes)), np.nan) for name in FIELDS}
    expected = np.zeros(market['close'].shape, dtype=bool)
    age = np.zeros(expected.shape, dtype=int)
    ordinary = np.ones(expected.shape, dtype=bool)
    dates_array = np.asarray(dates, dtype='datetime64[D]')
    for column, identity in enumerate(identities):
        expected[:, column] = dates_array >= np.datetime64(identity['ipoDate'])
        age[:, column] = (dates_array - np.datetime64(identity['ipoDate'])).astype(int)
        if identity['outDate']:
            expected[:, column] &= dates_array < np.datetime64(identity['outDate'])
            exit_index = np.searchsorted(dates, identity['outDate'])
            ordinary[max(0, exit_index - 30):, column] = False
    manifest = []
    observed = set()

    def consume(code, payload, checksum, filename):
        if code not in code_index or code in observed:
            raise ValueError('unknown or duplicate raw identity')
        if (payload.get('requested_code') != code or payload.get('adjustflag') != '3'
                or str(payload.get('provider_code', payload.get('error_code', '0'))) != '0'):
            raise ValueError('raw provenance mismatch')
        observed.add(code)
        column = code_index[code]
        seen = set()
        for row in payload['rows']:
            record = dict(zip(payload['fields'], row))
            date = record['date']
            if date in seen or record['code'] != code or record['adjustflag'] != '3':
                raise ValueError('duplicate, adjusted or wrong-code row')
            seen.add(date)
            if date not in date_index:
                continue
            index = date_index[date]
            for name in FIELDS:
                value = record.get(name, '')
                market[name][index, column] = float(value) if value else np.nan
        manifest.append({'code': code, 'file': filename, 'sha256': checksum})

    if kind == 'design':
        archive_path = source / 'holdout_data_20261001.tgz'
        if digest(archive_path) != '870c8826a33bcf0dc565f9b52c10797c84e17d0efc891392c5817e8180a8e1bb':
            raise ValueError('design archive hash mismatch')
        expected_hashes = {line.split()[1].removeprefix('./'): line.split()[0]
                           for line in (source / 'holdout_raw.sha256').read_text().splitlines() if line.strip()}
        with tarfile.open(archive_path, 'r:gz') as archive:
            for member in archive:
                if not member.isfile() or not member.name.startswith('raw/'):
                    continue
                filename = member.name.removeprefix('raw/')
                compressed = archive.extractfile(member).read()
                checksum = hashlib.sha256(compressed).hexdigest()
                if checksum != expected_hashes[filename]:
                    raise ValueError('design raw checksum mismatch')
                consume(filename.removesuffix('.json.gz'), json.loads(gzip.decompress(compressed)), checksum, member.name)
    else:
        source_manifest = json.loads((ROOT / '.local_records/ultrashort_20261005/historical_source_manifest.json').read_text(encoding='utf-8'))
        entries = source_manifest['files'] if isinstance(source_manifest, dict) else source_manifest
        for entry in entries:
            path = Path(entry['path'])
            checksum = digest(path)
            if checksum != entry['sha256']:
                raise ValueError('test raw checksum mismatch')
            consume(entry['code'], json.loads(gzip.decompress(path.read_bytes())), checksum, str(path))
    if observed != set(codes):
        raise ValueError('missing source files')
    market.update(dates=np.array(dates), codes=np.array(codes), expected=expected,
                  age=age, ordinary=ordinary, identities=identities)
    return market, {'files': manifest, 'basic_sha256': digest(basic_path),
                    'calendar_sha256': digest(calendar_path), 'outcomes_computed': False}


def features(market):
    shape = market['close'].shape
    dates = market['dates']
    growth = np.array([code.startswith(('sz.300', 'sz.301')) for code in market['codes']])
    rates = np.where(growth[None, :] & (dates[:, None] >= '2020-08-24'), 0.20, 0.10)
    preclose = market['preclose']
    upper = cents(preclose * (1 + rates)) / 100
    lower = cents(preclose * (1 - rates)) / 100
    ohlc = (np.isfinite(market['open']) & np.isfinite(market['close'])
            & np.isfinite(market['high']) & np.isfinite(market['low'])
            & (market['low'] > 0) & (market['high'] >= market['low'])
            & (market['open'] >= market['low']) & (market['open'] <= market['high'])
            & (market['close'] >= market['low']) & (market['close'] <= market['high']))
    state_known = (np.isfinite(market['isST']) & np.isfinite(market['tradestatus'])
                   & np.isfinite(market['volume']) & (market['volume'] >= 0))
    regular = (market['ordinary'] & (market['age'] >= 120) & (preclose > 0)
               & (market['isST'] == 0) & (market['tradestatus'] == 1) & (market['volume'] > 0))
    valid_ratio = ohlc & (preclose > 0) & (market['tradestatus'] == 1) & (market['volume'] > 0)
    ratio = np.where(valid_ratio, market['close'] / np.where(preclose > 0, preclose, np.nan), np.nan)
    score = pd.DataFrame(ratio).rolling(5, min_periods=5).apply(np.prod, raw=True).to_numpy() - 1
    amounts = np.where(market['amount'] > 0, market['amount'], np.nan)
    amount20 = pd.DataFrame(amounts).rolling(20, min_periods=20).mean().to_numpy()
    known = state_known & ohlc & (preclose > 0) & np.isfinite(score) & np.isfinite(amount20) & market['ordinary']
    eligible = (known & market['expected'] & regular & (amount20 >= 1e8)
                & (cents(market['close']) < cents(upper)) & (cents(market['close']) > cents(lower)))
    previous_close = np.vstack([np.full((1, shape[1]), np.nan), market['close'][:-1]])
    action = np.isfinite(previous_close) & (cents(previous_close) != cents(preclose))
    return {'score': score, 'amount20': amount20, 'eligible': eligible,
            'known': known, 'upper': upper, 'lower': lower, 'ohlc': ohlc,
            'regular': regular, 'action': action}


def selection(market, derived, index, generator=None):
    pool = np.flatnonzero(derived['eligible'][index])
    if generator is not None:
        return generator.choice(pool, size=min(5, len(pool)), replace=False).tolist()
    return sorted(pool.tolist(), key=lambda column: (derived['score'][index, column], market['codes'][column]))[:5]


def quote(market, derived, index, column):
    return bool(derived['ohlc'][index, column] and derived['regular'][index, column]
                and market['expected'][index, column])


def ticket(market, derived, signal, column, horizon, budget, pressure=False):
    entry, exit_index = signal + 1, signal + 1 + horizon
    result = {'signal': str(market['dates'][signal]), 'code': str(market['codes'][column]),
              'entry_date': str(market['dates'][entry]), 'exit_date': str(market['dates'][exit_index]),
              'status': 'unknown_entry', 'net_return': None, 'shares': None,
              'spent': None, 'received': None, 'pnl': None}
    if not quote(market, derived, entry, column):
        return result
    if cents(market['open'][entry, column]) >= cents(derived['upper'][entry, column]):
        result['status'] = 'entry_unfilled_limit'
        return result
    multiplier = 2 if pressure else 1
    commission = 0.00025 * multiplier
    minimum = 5 * multiplier
    slip = 0.001 * multiplier
    buy_price = market['open'][entry, column] * (1 + slip)
    shares = int(budget // (buy_price * 100)) * 100
    while shares > 0 and money(shares * buy_price + max(minimum, shares * buy_price * commission)) > budget:
        shares -= 100
    if shares <= 0:
        result['status'] = 'entry_unfilled_budget'
        return result
    spent = money(shares * buy_price + money(max(minimum, shares * buy_price * commission)))
    result.update(shares=shares, spent=spent, buy_price=float(buy_price))
    for index in range(entry, exit_index + 1):
        if index > entry and derived['action'][index, column]:
            result.update(status='unknown_corporate_action', unknown_at=index)
            return result
        if not quote(market, derived, index, column):
            result.update(status='unknown_holding_quote', unknown_at=index)
            return result
    if cents(market['open'][exit_index, column]) <= cents(derived['lower'][exit_index, column]):
        result.update(status='unknown_exit_limit', unknown_at=exit_index)
        return result
    sell_price = market['open'][exit_index, column] * (1 - slip)
    proceeds = shares * sell_price
    tax = 0.001 if market['dates'][exit_index] < '2023-08-28' else 0.0005
    received = money(proceeds - money(max(minimum, proceeds * commission)) - money(proceeds * tax))
    profit = money(received - spent)
    result.update(status='completed', received=received, pnl=profit,
                  net_return=profit / spent, sell_price=float(sell_price))
    return result


def month_statistics(monthly):
    if not monthly or any(value is None for value in monthly.values()):
        return {'mean_monthly_net': None, 'monthly_t': None, 'months': len(monthly),
                'known_months': sum(value is not None for value in monthly.values())}
    values = np.array(list(monthly.values()))
    deviation = values.std(ddof=1) if len(values) > 1 else np.nan
    return {'mean_monthly_net': float(values.mean()),
            'monthly_t': float(values.mean() / deviation * np.sqrt(len(values))) if deviation > 0 else None,
            'months': len(values), 'known_months': len(values)}


def simulate(market, derived, start, end, horizon, pressure=False, seed=None, detail=False):
    count = horizon + 1
    cash = [100000 / count] * count
    blocked = [False] * count
    positions = [[] for _ in range(count)]
    generator = np.random.default_rng(seed) if seed is not None else None
    signal_indices = [index for index, date in enumerate(market['dates']) if start <= date <= end]
    first, last = signal_indices[0], signal_indices[-1]
    last_signal = last - 3
    orders = {}
    probes = []
    account_events = []
    navigation = []
    planned = 0
    for index in range(first, last + 1):
        date = str(market['dates'][index])
        for sleeve in range(count):
            remaining = []
            for position in positions[sleeve]:
                if position.get('unknown_at') == index:
                    blocked[sleeve] = True
                    remaining.append(position)
                elif position['exit_index'] == index and position['received'] is not None:
                    cash[sleeve] = money(cash[sleeve] + position['received'])
                else:
                    remaining.append(position)
            positions[sleeve] = remaining
        if index in orders:
            signal, columns, sleeve = orders.pop(index)
            if not blocked[sleeve]:
                budget = cash[sleeve] / 5
                for column in columns:
                    trade = ticket(market, derived, signal, column, horizon, budget, pressure)
                    if detail:
                        account_events.append({'sleeve': sleeve, **trade})
                    if trade['status'] == 'unknown_entry':
                        blocked[sleeve] = True
                    elif trade['spent'] is not None:
                        cash[sleeve] = money(cash[sleeve] - trade['spent'])
                        positions[sleeve].append({'column': column, 'shares': trade['shares'],
                                                  'received': trade['received'], 'exit_index': index + horizon,
                                                  'unknown_at': trade.get('unknown_at')})
        if index <= last_signal:
            columns = selection(market, derived, index, generator)
            planned += len(columns)
            sleeve = (index + 1 - first) % count
            orders[index + 1] = (index, columns, sleeve)
            for column in columns:
                probes.append(ticket(market, derived, index, column, horizon, 100000 / count / 5, pressure))
        if any(blocked):
            navigation.append({'date': date, 'nav': None})
        else:
            equity = sum(cash)
            for position_list in positions:
                for position in position_list:
                    equity += position['shares'] * market['close'][index, position['column']]
            navigation.append({'date': date, 'nav': money(equity)})
    monthly = {}
    grouped = {}
    for observation in navigation:
        grouped.setdefault(observation['date'][:7], []).append(observation)
    previous = 100000.0
    for month, observations in grouped.items():
        ending = observations[-1]['nav']
        monthly[month] = (ending / previous - 1 if ending is not None and previous is not None
                          and all(item['nav'] is not None for item in observations) else None)
        previous = ending
    statistics = month_statistics(monthly)
    complete = all(item['nav'] is not None for item in navigation)
    nav_values = np.array([item['nav'] for item in navigation]) if complete else None
    statuses = Counter(trade['status'] for trade in probes)
    completed = [trade['net_return'] for trade in probes if trade['status'] == 'completed']
    unknown = sum(amount for status, amount in statuses.items() if status.startswith('unknown'))
    result = {'horizon': horizon, 'pressure': pressure, 'seed': seed, 'planned_tickets': planned,
              'probe_status_counts': dict(statuses), 'probe_known_mean_net': float(np.mean(completed)) if completed else None,
              'unknown_tickets': unknown, 'unknown_fraction': unknown / planned if planned else None,
              'account_complete': complete, 'account_end_value': navigation[-1]['nav'],
              'account_total_return': float(nav_values[-1] / 100000 - 1) if complete else None,
              'max_drawdown': float(np.max(1 - nav_values / np.maximum.accumulate(np.r_[100000, nav_values])[1:])) if complete else None,
              'monthly': monthly, **statistics}
    if detail:
        result.update(probe_trades=probes, account_events=account_events, navigation=navigation)
    return result


def index_context(index_path, dates):
    if not index_path or not Path(index_path).exists():
        return {'status': 'missing', 'returns': {}, 'monthly1000': {}}
    payload = json.loads(Path(index_path).read_text(encoding='utf-8'))
    returns = {}
    monthly1000 = {}
    for symbol in ('000852', '000905'):
        series = payload.get('series', {}).get(symbol, [])
        table = {row['date']: row for row in series}
        if not all(str(date) in table for date in dates):
            returns[symbol] = None
            continue
        opening = float(table[str(dates[0])]['open'])
        closing = float(table[str(dates[-1])]['close'])
        returns[symbol] = closing / opening - 1 if opening > 0 else None
        if symbol == '000852':
            all_dates = sorted(table)
            months = sorted({str(date)[:7] for date in dates})
            for month in months:
                month_dates = [str(date) for date in dates if str(date).startswith(month)]
                before = [date for date in all_dates if date < month + '-01']
                monthly1000[month] = (float(table[month_dates[-1]]['close']) / float(table[before[-1]]['close']) - 1
                                      if before and float(table[before[-1]]['close']) > 0 else None)
    return {'status': 'observed', 'input_sha256': digest(index_path), 'returns': returns, 'monthly1000': monthly1000,
            'basis': 'index_price_proxy_first_open_last_close_not_executable_ETF'}


def gates(normal, pressure, random_accounts, indices):
    mean = normal['mean_monthly_net']
    statistic = normal['monthly_t']
    wins = sum(normal['account_total_return'] is not None and random['account_total_return'] is not None
               and normal['account_total_return'] > random['account_total_return'] for random in random_accounts)
    regimes = {'bull': [], 'bear': [], 'sideways': []}
    for month, benchmark in indices['monthly1000'].items():
        category = 'bull' if benchmark is not None and benchmark > 0.03 else 'bear' if benchmark is not None and benchmark < -0.03 else 'sideways'
        if benchmark is not None:
            regimes[category].append(normal['monthly'].get(month))
    regime_means = {category: float(np.mean(values)) if values and all(value is not None for value in values) else None
                    for category, values in regimes.items()}
    known_regimes = all(value is not None for value in regime_means.values())
    benchmark = indices['returns'].get('000852')
    checks = {
        'positive_mean_and_t_ge_2': mean is not None and mean > 0 and statistic is not None and statistic >= 2,
        'positive_pressure_mean': pressure['mean_monthly_net'] is not None and pressure['mean_monthly_net'] > 0,
        'beat_16_of_20_random': wins >= 16,
        'beat_csi1000': normal['account_total_return'] is not None and benchmark is not None and normal['account_total_return'] > benchmark,
        'regimes_two_positive_none_below_minus_0_002': known_regimes and sum(value > 0 for value in regime_means.values()) >= 2 and min(regime_means.values()) >= -0.002,
        'drawdown_le_0_30': normal['max_drawdown'] is not None and normal['max_drawdown'] <= 0.30,
        'unknown_le_0_10': normal['unknown_fraction'] is not None and normal['unknown_fraction'] <= 0.10,
    }
    return {'checks': checks, 'all_pass': all(checks.values()), 'random_wins': wins,
            'random_complete_accounts': sum(random['account_complete'] for random in random_accounts),
            'regime_month_counts': {category: len(values) for category, values in regimes.items()},
            'regime_means': regime_means,
            'unverifiable_is_not_pass': True}


def choose(results):
    eligible = [version for version in ('A', 'B') if results[version]['normal']['mean_monthly_net'] is not None
                and results[version]['normal']['mean_monthly_net'] > 0
                and results[version]['normal']['monthly_t'] is not None]
    if eligible:
        return max(eligible, key=lambda version: (results[version]['normal']['monthly_t'], version == 'A')), 'monthly_t_selection'
    return 'A', 'pre_registered_observation_fallback_not_performance_selection'


def run(kind, source, output, index_path=None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    marker = output / 'ONCE_STARTED.json'
    write_json(marker, {'rule': RULE, 'registration_commit': REGISTRATION, 'kind': kind,
                        'started': stamp(), 'tool_sha256': digest(__file__)})
    market, manifest = read_market(source, kind)
    if kind == 'design':
        np.savez_compressed(output / 'warmup_raw.npz', **{name: market[name][-20:] for name in FIELDS},
                            dates=market['dates'][-20:], codes=market['codes'])
    else:
        warmup_path = ROOT / '.local_records/ultrashort_20261005/design/warmup_raw.npz'
        with np.load(warmup_path, allow_pickle=False) as warmup:
            warm_codes = {str(code): column for column, code in enumerate(warmup['codes'])}
            for name in FIELDS:
                prefix = np.full((len(warmup['dates']), len(market['codes'])), np.nan)
                for column, code in enumerate(market['codes']):
                    if str(code) in warm_codes:
                        prefix[:, column] = warmup[name][:, warm_codes[str(code)]]
                market[name] = np.vstack([prefix, market[name]])
            length = len(warmup['dates'])
            market['dates'] = np.r_[warmup['dates'], market['dates']]
            for name in ('expected', 'ordinary', 'age'):
                market[name] = np.vstack([np.zeros((length, len(market['codes'])), dtype=market[name].dtype), market[name]])
            manifest['warmup_sha256'] = digest(warmup_path)
    write_json(output / 'source_manifest.json', manifest)
    derived = features(market)
    start = '2021-01-01' if kind == 'design' else '2024-01-02'
    end = '2023-12-29' if kind == 'design' else '2026-09-24'
    mask = (market['dates'] >= start) & (market['dates'] <= end)
    expected = market['expected'][mask]
    write_json(output / 'availability_before_outcomes.json', {
        'recorded': stamp(), 'outcomes_computed': False, 'expected_stock_days': int(expected.sum()),
        'known_qualification_stock_days': int((derived['known'][mask] & expected).sum()),
        'unknown_qualification_stock_days': int((~derived['known'][mask] & expected).sum()),
        'eligible_stock_days': int(derived['eligible'][mask].sum()),
        'scope': 'historical_basic_identity_not_current_survivors',
        'special_regime_independent_evidence': 'not_complete_not_formal_risk_acceptance'})
    indices = index_context(index_path, market['dates'][mask])
    results = {}
    for version, horizon in (('A', 1), ('B', 2)):
        normal = simulate(market, derived, start, end, horizon, detail=True)
        pressure = simulate(market, derived, start, end, horizon, pressure=True)
        random_accounts = [simulate(market, derived, start, end, horizon, seed=seed) for seed in SEEDS]
        write_json(output / f'{version}_details.json', normal)
        summary = {key: value for key, value in normal.items() if key not in ('probe_trades', 'account_events', 'navigation')}
        results[version] = {'normal': summary, 'pressure': pressure, 'random': random_accounts,
                            'acceptance': gates(summary, pressure, random_accounts, indices)}
        print(json.dumps({'stage': 'version_finished', 'kind': kind, 'version': version,
                          'known_probe_mean': summary['probe_known_mean_net'],
                          'account_complete': summary['account_complete']}), flush=True)
    selected, selection_reason = choose(results)
    report = {'rule': RULE, 'registration_commit': REGISTRATION, 'started': json.loads(marker.read_text())['started'],
              'finished': stamp(), 'kind': kind, 'period': [start, end], 'indices': indices,
              'results': results, 'selected': selected, 'selection_reason': selection_reason,
              'label': '研究有效名单' if results[selected]['acceptance']['all_pass'] else '未通过检验，仅观察',
              'half_blind_not_independent': kind == 'test'}
    write_json(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', required=True, choices=('design', 'test'))
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--indices')
    args = parser.parse_args()
    registered = subprocess.check_output(['git', 'show', f'{REGISTRATION}:docs/ULTRASHORT_REVERSAL_V1_20261005.md'], cwd=ROOT)
    if not registered or subprocess.check_output(['git', 'merge-base', '--is-ancestor', REGISTRATION, 'HEAD'], cwd=ROOT):
        raise ValueError('registration not in current history')
    run(args.kind, args.source, args.output, args.indices)


if __name__ == '__main__':
    main()
