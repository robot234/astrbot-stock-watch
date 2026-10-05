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
import sys
import tarfile
import time

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / '.local_records/holdout_2021_2023_20261001'
RUNTIME = ROOT / '.local_records/macd_runtime'
if RUNTIME.is_dir():
    sys.path.insert(0, str(RUNTIME))
try:
    from numba import njit
except ImportError:
    def njit(function):
        return function

BOARDS = ('sh.600', 'sh.601', 'sh.603', 'sh.605', 'sz.000', 'sz.001',
          'sz.002', 'sz.003', 'sz.300', 'sz.301')
FIELDS = ('open', 'high', 'low', 'close', 'preclose', 'volume', 'amount',
          'turn', 'tradestatus', 'isST')
SOURCE_HASHES = {
    'holdout_data_20261001.tgz': '870c8826a33bcf0dc565f9b52c10797c84e17d0efc891392c5817e8180a8e1bb',
    'inputs/baostock_basic.json': 'f6255f4df45da383c5875b88c9959bd058a6c9dbd2f287c8f5912d892b8e40bb',
    'calendar.json': '920eb276ada166fe91535434126ce2848fb2c57f190e7c5df42a38e51e36e6ea',
}
RULE = {
    'version': 'MACD_SHAPE_EXP_V1', 'pre_outcome_revision': 2,
    'window': 20, 'warmup_before_window': 120, 'neighbors': 50,
    'minimum_known': 45, 'same_stock_spacing': 20,
    'price': 'past_only_close_over_provider_preclose_product',
    'atr': 'Wilder14_on_continuous_OHLC', 'volume_action_reset': 40,
    'dictionary': ['2021-01-04', '2021-12-17'],
    'signals': ['2022-01-04', '2023-12-15'], 'maturity_end': '2023-12-29',
    'horizon_after_entry': 5, 'exit_extension': 3, 'cash': 100000,
    'sleeves': 5, 'label_cash': 20000, 'commission': 0.0003,
    'minimum_commission': 5, 'slippage': 0.001,
    'cost_evidence': 'simulation_assumptions_historical_official_rules_not_verified',
    'random_seeds': list(range(2026100500, 2026100520)),
    'unknown_warning': 0.20, 'completed_warning': 80, 'year_completed_warning': 30,
    'approval': '2026-10-05 user approved availability-first revisions and EXP_V1',
}
ENTRY_FAIL, MATURED, UNKNOWN_ENTRY, UNKNOWN_EXIT, PENDING, UNKNOWN_PATH = range(1, 7)


def digest(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            checksum.update(block)
    return checksum.hexdigest()


def now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def write_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def append_event(out, event):
    with (out / 'events.jsonl').open('a', encoding='utf-8') as stream:
        stream.write(json.dumps({'time': now(), **event}, ensure_ascii=False, allow_nan=False) + '\n')


def round_cent(value):
    return float(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def read_market():
    for filename, expected_hash in SOURCE_HASHES.items():
        if digest(SOURCE / filename) != expected_hash:
            raise ValueError(f'source hash mismatch: {filename}')
    calendar = json.loads((SOURCE / 'calendar.json').read_text(encoding='utf-8'))
    dates = [row[0] for row in calendar['rows'] if row[1] == '1']
    basic = json.loads((SOURCE / 'inputs/baostock_basic.json').read_text(encoding='utf-8'))
    identities = sorted([dict(zip(basic['fields'], row)) for row in basic['rows']
                         if dict(zip(basic['fields'], row))['type'] == '1'
                         and row[0].startswith(BOARDS)
                         and dict(zip(basic['fields'], row))['ipoDate'] <= dates[-1]
                         and (not dict(zip(basic['fields'], row))['outDate']
                              or dict(zip(basic['fields'], row))['outDate'] >= dates[0])],
                        key=lambda row: row['code'])
    codes = [row['code'] for row in identities]
    if len(codes) != 4650 or len(set(codes)) != len(codes):
        raise ValueError('unexpected source universe')
    shape = len(dates), len(codes)
    market = {name: np.full(shape, np.nan) for name in FIELDS}
    expected = np.zeros(shape, dtype=bool)
    age = np.zeros(shape, dtype=np.int32)
    date_index = {date: index for index, date in enumerate(dates)}
    code_index = {code: index for index, code in enumerate(codes)}
    for column, identity in enumerate(identities):
        for index, date in enumerate(dates):
            expected[index, column] = (date >= identity['ipoDate']
                                      and (not identity['outDate'] or date < identity['outDate']))
            age[index, column] = (1000 + index if identity['ipoDate'] < dates[0]
                                 else index - np.searchsorted(dates, identity['ipoDate']) + 1)
    hash_lines = (SOURCE / 'holdout_raw.sha256').read_text(encoding='utf-8').splitlines()
    checksums = {line.split()[1].removeprefix('./'): line.split()[0]
                 for line in hash_lines if line.strip()}
    observed = set()
    with tarfile.open(SOURCE / 'holdout_data_20261001.tgz', 'r:gz') as archive:
        for member in archive:
            if not member.isfile() or not member.name.startswith('raw/'):
                continue
            filename = member.name.removeprefix('raw/')
            compressed = archive.extractfile(member).read()
            if filename not in checksums or hashlib.sha256(compressed).hexdigest() != checksums[filename]:
                raise ValueError(f'raw checksum mismatch: {filename}')
            code = filename.removesuffix('.json.gz')
            if code in observed or code not in code_index:
                raise ValueError('duplicate or unexpected raw identity')
            observed.add(code)
            payload = json.loads(gzip.decompress(compressed))
            if payload.get('requested_code') != code or payload.get('adjustflag') != '3':
                raise ValueError('raw provenance mismatch')
            fields = payload['fields']
            seen_dates = set()
            for row in payload['rows']:
                record = dict(zip(fields, row))
                date = record['date']
                if date in seen_dates or date not in date_index or record['code'] != code:
                    raise ValueError('duplicate or invalid raw row')
                seen_dates.add(date)
                if record['adjustflag'] != '3':
                    raise ValueError('adjusted raw row')
                index, column = date_index[date], code_index[code]
                for name in FIELDS:
                    value = record.get(name, '')
                    market[name][index, column] = float(value) if value else np.nan
            if len(observed) % 500 == 0:
                print(json.dumps({'stage': 'raw_integrity', 'files': len(observed)}), flush=True)
    if observed != set(codes):
        raise ValueError('missing raw stock files')
    market.update(dates=np.array(dates), codes=np.array(codes), expected=expected, age=age)
    return market


def continuous_features(market):
    close, preclose = market['close'], market['preclose']
    high, low, opening = market['high'], market['low'], market['open']
    valid = (market['expected'] & (market['tradestatus'] == 1)
             & np.isfinite(close) & np.isfinite(preclose) & (close > 0) & (preclose > 0)
             & np.isfinite(high) & np.isfinite(low) & np.isfinite(opening)
             & (low > 0) & (high >= low) & (close >= low) & (close <= high)
             & (opening >= low) & (opening <= high))
    previous_raw = np.vstack([np.full((1, close.shape[1]), np.nan), close[:-1]])
    action = (np.isfinite(previous_raw) & np.isfinite(preclose)
              & (np.floor(preclose * 100 + 0.50000001) != np.floor(previous_raw * 100 + 0.50000001)))
    volume_valid = valid & np.isfinite(market['volume']) & (market['volume'] > 0)
    shape = close.shape
    continuous = np.full(shape, np.nan)
    atr = np.full(shape, np.nan)
    macd = np.full(shape, np.nan)
    count = np.zeros(shape, dtype=np.int32)
    volume_count = np.zeros(shape, dtype=np.int32)
    ema12 = np.full(shape[1], np.nan)
    ema26 = np.full(shape[1], np.nan)
    dea = np.full(shape[1], np.nan)
    sum12, sum26, sumdea = (np.zeros(shape[1]) for _ in range(3))
    tr_sum = np.zeros(shape[1])
    for index in range(shape[0]):
        previous_count = count[index - 1] if index else np.zeros(shape[1], dtype=int)
        active = valid[index]
        continuing = active & (previous_count > 0)
        starting = active & ~continuing
        count[index] = np.where(active, previous_count + 1, 0)
        continuous[index, starting] = 100.0
        if index:
            continuous[index, continuing] = continuous[index - 1, continuing] * close[index, continuing] / preclose[index, continuing]
        length = count[index]
        current = continuous[index]
        sum12[~continuing] = 0
        sum26[~continuing] = 0
        sumdea[~continuing] = 0
        ema12[~active] = np.nan
        ema26[~active] = np.nan
        dea[~active] = np.nan
        sum12[active & (length <= 12)] += current[active & (length <= 12)]
        sum26[active & (length <= 26)] += current[active & (length <= 26)]
        ema12[length == 12] = sum12[length == 12] / 12
        ema26[length == 26] = sum26[length == 26] / 26
        update12, update26 = length > 12, length > 26
        ema12[update12] += (current[update12] - ema12[update12]) * (2 / 13)
        ema26[update26] += (current[update26] - ema26[update26]) * (2 / 27)
        dif = ema12 - ema26
        seed_dea = active & (length >= 26) & (length <= 34)
        sumdea[seed_dea] += dif[seed_dea]
        dea[length == 34] = sumdea[length == 34] / 9
        update_dea = length > 34
        dea[update_dea] += (dif[update_dea] - dea[update_dea]) * 0.2
        macd[index, active & (length >= 34)] = (2 * (dif - dea) / current)[active & (length >= 34)]
        tr_sum[~continuing] = 0
        if index:
            scale = current / close[index]
            tr = np.maximum.reduce([high[index] * scale - low[index] * scale,
                                    np.abs(high[index] * scale - continuous[index - 1]),
                                    np.abs(low[index] * scale - continuous[index - 1])])
            accumulating = continuing & (length <= 15)
            tr_sum[accumulating] += tr[accumulating]
            atr[index, length == 15] = tr_sum[length == 15] / 14
            recurrent = continuing & (length > 15)
            atr[index, recurrent] = (13 * atr[index - 1, recurrent] + tr[recurrent]) / 14
        previous_volume_count = volume_count[index - 1] if index else 0
        volume_count[index] = np.where(volume_valid[index] & ~action[index], previous_volume_count + 1, 0)
    reference = pd.DataFrame(market['volume']).shift(1).rolling(20, min_periods=20).median().to_numpy()
    with np.errstate(invalid='ignore', divide='ignore'):
        volume_channel = np.log(market['volume'] / reference)
    feature_valid = valid & (count >= 140) & (volume_count >= 40) & np.isfinite(atr)
    safe = market['expected'] & (market['isST'] == 0) & (market['tradestatus'] == 1) & (market['age'] > 5)
    atr_raw = atr * close / continuous
    return dict(continuous=continuous, macd=macd, volume_channel=volume_channel,
                feature_valid=feature_valid, safe=safe, count=count,
                volume_count=volume_count, action=action, atr_raw=atr_raw)


def make_vectors(derived, indices):
    vectors = np.empty((len(indices), 60), dtype=np.float64)
    for offset in range(20):
        rows, columns = indices[:, 0] - 19 + offset, indices[:, 1]
        vectors[:, offset] = (derived['continuous'][rows, columns]
                              / derived['continuous'][indices[:, 0] - 19, columns] - 1)
        vectors[:, 20 + offset] = derived['macd'][rows, columns]
        vectors[:, 40 + offset] = derived['volume_channel'][rows, columns]
    if not np.isfinite(vectors).all():
        raise ValueError('nonfinite feature vector')
    return vectors


def standardize(training, query):
    mean = training.reshape(-1, 3, 20).mean(axis=(0, 2))
    std = training.reshape(-1, 3, 20).std(axis=(0, 2))
    if not np.isfinite(std).all() or np.any(std <= 0):
        raise ValueError('invalid training statistics')
    center, spread = np.repeat(mean, 20), np.repeat(std, 20)
    return (training - center) / spread, (query - center) / spread, mean, std


def fees(gross, date, side, multiplier=1):
    transfer = 0.00002 if date < '2022-04-29' else 0.00001
    stamp = (0.001 if date < '2023-08-28' else 0.0005) if side == 'sell' else 0
    return max(5 * multiplier, gross * 0.0003 * multiplier) + gross * (transfer * multiplier + stamp)


def entry_lots(opening, cash, date, multiplier=1):
    fill = opening * (1 + 0.001 * multiplier)
    lots = int(cash // (100 * fill))
    while lots > 0 and lots * 100 * fill + fees(lots * 100 * fill, date, 'buy', multiplier) > cash + 1e-8:
        lots -= 1
    return lots * 100


def execution_state(market, index, column, side, multiplier=1):
    if index >= len(market['dates']):
        return 'pending'
    status, st = market['tradestatus'][index, column], market['isST'][index, column]
    if status == 0:
        return 'blocked'
    if side == 'buy' and st == 1:
        return 'blocked'
    if status != 1 or st not in (0, 1) or market['age'][index, column] <= 5:
        return 'unknown'
    opening, previous = market['open'][index, column], market['preclose'][index, column]
    if not np.isfinite(opening) or not np.isfinite(previous) or opening <= 0 or previous <= 0:
        return 'unknown'
    code, date = str(market['codes'][column]), str(market['dates'][index])
    growth = code.startswith(('sz.300', 'sz.301')) and date >= '2020-08-24'
    limit = 0.20 if growth else (0.05 if st == 1 else 0.10)
    upper, lower = round_cent(previous * (1 + limit)), round_cent(previous * (1 - limit))
    if opening > upper + 1e-8 or opening < lower - 1e-8:
        return 'unknown'
    if (side == 'buy' and opening >= upper - 1e-8) or (side == 'sell' and opening <= lower + 1e-8):
        return 'unknown'
    fill = opening * (1 + 0.001 * multiplier if side == 'buy' else 1 - 0.001 * multiplier)
    if fill > upper + 1e-8 or fill < lower - 1e-8:
        return 'blocked'
    return 'proxy'


def entry_decision(market, derived, row, column, cash=20000, multiplier=1):
    entry = row + 1
    state = execution_state(market, entry, column, 'buy', multiplier)
    if state == 'pending':
        return PENDING, 0
    if state == 'blocked':
        return ENTRY_FAIL, 0
    if state != 'proxy':
        return UNKNOWN_ENTRY, 0
    band = max(0.5 * derived['atr_raw'][row, column], 0.01 * market['close'][row, column])
    lower = round_cent(market['close'][row, column] - band)
    upper = round_cent(market['close'][row, column] + band)
    opening = market['open'][entry, column]
    if not lower <= opening <= upper:
        return ENTRY_FAIL, 0
    shares = entry_lots(opening, cash, str(market['dates'][entry]), multiplier)
    if shares < 100:
        return ENTRY_FAIL, 0
    return MATURED, shares


def label_structure(market, derived, row, column, cash=20000, multiplier=1):
    entry = row + 1
    decision, shares = entry_decision(market, derived, row, column, cash, multiplier)
    if decision != MATURED:
        return decision, entry, -1, 0
    for exit_index in range(entry + 5, entry + 9):
        exit_state = execution_state(market, exit_index, column, 'sell', multiplier)
        if exit_state == 'pending':
            return PENDING, entry, -1, shares
        if exit_state == 'blocked':
            continue
        if exit_state != 'proxy':
            return UNKNOWN_EXIT, entry, -1, shares
        if (not np.isfinite(derived['continuous'][entry, column])
                or not np.isfinite(derived['continuous'][exit_index, column])
                or derived['count'][exit_index, column] < exit_index - entry + 1):
            return UNKNOWN_PATH, entry, exit_index, shares
        return MATURED, entry, exit_index, shares
    return UNKNOWN_EXIT, entry, -1, shares


def label_values(market, derived, indices, structures):
    values = np.full(len(indices), np.nan)
    for position, ((row, column), (status, entry, exit_index, shares)) in enumerate(zip(indices, structures)):
        if status == ENTRY_FAIL:
            values[position] = 0
        elif status == MATURED:
            entry_open = market['open'][entry, column]
            proxy_entry = derived['continuous'][entry, column] * entry_open / market['close'][entry, column]
            proxy_exit = (derived['continuous'][exit_index, column] * market['open'][exit_index, column]
                          / market['close'][exit_index, column])
            buy_gross = shares * entry_open * 1.001
            sell_gross = shares * entry_open * (proxy_exit / proxy_entry) * 0.999
            net = (sell_gross - fees(sell_gross, str(market['dates'][exit_index]), 'sell')
                   - buy_gross - fees(buy_gross, str(market['dates'][entry]), 'buy'))
            values[position] = net / 20000
    return values


@njit
def choose_spaced(order, dictionary_indices, limit=50):
    selected = np.empty(limit, dtype=np.int64)
    count = 0
    for candidate in order:
        row, column = dictionary_indices[candidate]
        valid = True
        for previous in range(count):
            prior_row, prior_column = dictionary_indices[selected[previous]]
            if column == prior_column and abs(row - prior_row) < 20:
                valid = False
                break
        if valid:
            selected[count] = candidate
            count += 1
            if count == limit:
                break
    return selected[:count]


def exact_neighbors(distances, query, training, dictionary_indices):
    size = min(256, len(distances))
    while True:
        if size == len(distances):
            candidates = np.arange(len(distances))
            boundary = np.inf
        else:
            boundary = np.partition(distances, size - 1)[size - 1]
            candidates = np.flatnonzero(distances <= boundary + 1e-9)
        direct = np.mean((training[candidates] - query) ** 2, axis=1)
        order = candidates[np.lexsort((candidates, direct))]
        chosen = choose_spaced(order, dictionary_indices)
        chosen_distances = np.mean((training[chosen] - query) ** 2, axis=1)
        if (len(chosen) == 50 and chosen_distances[-1] < boundary - 1e-9) or size == len(distances):
            return chosen, chosen_distances
        size = min(len(distances), size * 2)


def certified_neighbors(approximate_distances, candidates, query, training, dictionary_indices):
    direct = np.mean((training[candidates] - query) ** 2, axis=1)
    chosen = choose_spaced(candidates[np.lexsort((candidates, direct))], dictionary_indices)
    if len(chosen) == 50:
        selected_distances = np.mean((training[chosen] - query) ** 2, axis=1)
        radius = float(selected_distances[-1] * 60)
        query_norm = float(np.linalg.norm(query))
        unit = np.finfo(np.float32).eps
        cast_error = unit * (2 * query_norm + np.sqrt(radius))
        gamma = 256 * unit / (1 - 256 * unit)
        error = (gamma * (2 * query_norm + np.sqrt(radius) + cast_error) ** 2
                 + 2 * np.sqrt(radius) * cast_error + cast_error ** 2 + 1e-8)
        if float(approximate_distances[-1]) > radius + error:
            return chosen, selected_distances, False
    distances = np.mean((training - query) ** 2, axis=1)
    chosen = choose_spaced(np.lexsort((np.arange(len(distances)), distances)), dictionary_indices)
    return chosen, distances[chosen], True


def score_known(values, neighbors):
    known = np.isfinite(values[neighbors])
    return (float(np.mean(values[neighbors][known])) if known.sum() >= 45 else None,
            int((~known).sum()))


def prepare(out, cache=None):
    out.mkdir(parents=True, exist_ok=False)
    write_json(out / 'rules.json', RULE)
    write_json(out / 'registration.json', {
        'time': now(), 'stage': 'availability_only', 'code_sha256': digest(__file__),
        'rule_sha256': digest(out / 'rules.json'), 'source_hashes': SOURCE_HASHES,
        'source_manifest_sha256': digest(SOURCE / 'holdout_raw.sha256'),
        'git_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'approval': RULE['approval'], 'outcome_values_computed': False,
    })
    append_event(out, {'event': 'prepare_start', 'outcome_values_computed': False})
    if cache is None:
        market = read_market()
        derived = continuous_features(market)
    else:
        old_rule = json.loads((cache / 'rules.json').read_text(encoding='utf-8'))
        old_registration = json.loads((cache / 'registration.json').read_text(encoding='utf-8'))
        if old_rule != RULE or old_registration['outcome_values_computed']:
            raise ValueError('cached rules/state mismatch')
        for filename, expected_hash in SOURCE_HASHES.items():
            if digest(SOURCE / filename) != expected_hash:
                raise ValueError('source changed during matcher recovery')
        cached_names = ['market.npz', 'derived.npz', 'indices.npz', 'training.npy', 'query.npy']
        hashes = {name: digest(cache / name) for name in cached_names}
        write_json(out / 'cache_recovery.json', {'time': now(), 'origin': str(cache),
                                               'origin_registration_sha256': digest(cache / 'registration.json'),
                                               'cached_files': hashes, 'outcomes_computed': False})
        with np.load(cache / 'market.npz', allow_pickle=False) as data:
            market = {name: data[name] for name in data.files}
        with np.load(cache / 'derived.npz', allow_pickle=False) as data:
            derived = {name: data[name] for name in data.files}
    dates = market['dates']
    training_days = (dates >= RULE['dictionary'][0]) & (dates <= RULE['dictionary'][1])
    query_days = (dates >= RULE['signals'][0]) & (dates <= RULE['signals'][1])
    eligible = derived['feature_valid'] & derived['safe']
    dictionary_indices = np.argwhere(eligible & training_days[:, None])
    query_indices = np.argwhere(eligible & query_days[:, None])
    if len(dictionary_indices) < 50 or not len(query_indices):
        raise ValueError('insufficient training/query features')
    if cache is None:
        training = make_vectors(derived, dictionary_indices)
        query = make_vectors(derived, query_indices)
        training, query, mean, std = standardize(training, query)
        structures = np.array([label_structure(market, derived, int(row), int(column))
                               for row, column in dictionary_indices], dtype=np.int32)
    else:
        training = np.load(cache / 'training.npy')
        query = np.load(cache / 'query.npy')
        with np.load(cache / 'indices.npz', allow_pickle=False) as data:
            if not np.array_equal(data['dictionary'], dictionary_indices) or not np.array_equal(data['query'], query_indices):
                raise ValueError('cache index mismatch')
            structures, mean, std = data['structures'], data['mean'], data['std']
    known = np.isin(structures[:, 0], [ENTRY_FAIL, MATURED])
    print(json.dumps({'stage': 'features_ready', 'dictionary': len(training), 'queries': len(query),
                      'outcome_values_computed': False}), flush=True)
    np.savez(out / 'market.npz', **market)
    np.savez(out / 'derived.npz', **derived)
    np.save(out / 'training.npy', training)
    np.save(out / 'query.npy', query)
    np.savez(out / 'indices.npz', dictionary=dictionary_indices, query=query_indices,
             structures=structures, mean=mean, std=std)
    neighbors = np.lib.format.open_memmap(out / 'neighbors.npy', mode='w+', dtype=np.int32,
                                        shape=(len(query), 50))
    distance_summary = np.empty((len(query), 2))
    counts = np.zeros(len(query), dtype=np.int8)
    started = time.monotonic()
    import faiss
    faiss.omp_set_num_threads(12)
    matcher = faiss.IndexFlatL2(60)
    matcher.add(training.astype(np.float32))
    fallbacks = 0
    for start in range(0, len(query), 1024):
        batch = query[start:start + 1024]
        approximate, candidates = matcher.search(batch.astype(np.float32), 256)
        for offset, vector in enumerate(batch):
            chosen, direct, fallback = certified_neighbors(approximate[offset], candidates[offset], vector, training, dictionary_indices)
            fallbacks += int(fallback)
            if len(chosen) != 50:
                raise ValueError('fewer than fifty spaced neighbors')
            position = start + offset
            neighbors[position] = chosen
            counts[position] = known[chosen].sum()
            distance_summary[position] = direct.mean(), direct.max()
        if start % 16384 == 0:
            neighbors.flush()
            progress = {'stage': 'neighbors', 'done': min(start + len(batch), len(query)),
                        'total': len(query), 'seconds': round(time.monotonic() - started, 1),
                        'outcome_values_computed': False, 'exact_float64_fallbacks': fallbacks}
            print(json.dumps(progress), flush=True)
            append_event(out, progress)
    neighbors.flush()
    np.save(out / 'known_counts.npy', counts)
    np.save(out / 'distances.npy', distance_summary)
    report = {'time': now(), 'version': RULE['version'], 'outcome_values_computed': False,
              'dictionary_statuses': dict(Counter(map(int, structures[:, 0]))),
              'source_files_verified': 4650, 'training_vectors': len(training), 'years': {},
              'matcher': 'flat_float32_seed_certified_float64_order_or_exhaustive_fallback',
              'exact_float64_fallbacks': fallbacks}
    action_recent = pd.DataFrame(derived['action']).rolling(40, min_periods=1).max().to_numpy() > 0
    for year in ('2021', '2022', '2023'):
        stage_days = training_days if year == '2021' else query_days
        mask = np.char.startswith(dates, year) & stage_days
        expected_count = int(market['expected'][mask].sum())
        available_features = int((derived['feature_valid'][mask] & market['expected'][mask]).sum())
        year_query = np.char.startswith(dates[query_indices[:, 0]], year)
        query_count = int(year_query.sum())
        score_count = int((counts[year_query] >= 45).sum())
        values = dict(support_stock_days=expected_count, feature_available=available_features,
                      feature_ratio=available_features / expected_count if expected_count else None,
                      safe_feature_queries=int(eligible[mask].sum()),
                      volume_action_window_unknown=int((market['expected'][mask] & (derived['count'][mask] >= 140)
                                                       & (derived['volume_count'][mask] < 40) & action_recent[mask]).sum()),
                      reasons=dict(st=int((market['expected'][mask] & (market['isST'][mask] == 1)).sum()),
                                   suspended=int((market['expected'][mask] & (market['tradestatus'][mask] == 0)).sum()),
                                   price_warmup=int((market['expected'][mask] & (derived['count'][mask] < 140)).sum()),
                                   volume_window=int((market['expected'][mask] & (derived['volume_count'][mask] < 40)).sum())))
        if year != '2021':
            values.update(score_denominator=query_count, score_available=score_count,
                          score_ratio=score_count / query_count if query_count else None,
                          unknown_neighbor_histogram=dict(Counter(map(int, 50 - counts[year_query]))))
        report['years'][year] = values
    write_json(out / 'availability.json', report)
    artifacts = ['market.npz', 'derived.npz', 'indices.npz', 'training.npy', 'query.npy',
                 'neighbors.npy', 'known_counts.npy', 'distances.npy', 'availability.json']
    write_json(out / 'availability_artifacts.json', {name: digest(out / name) for name in artifacts})
    append_event(out, {'event': 'availability_complete', 'report_sha256': digest(out / 'availability.json'),
                       'outcome_values_computed': False})
    print(json.dumps(report, ensure_ascii=False), flush=True)


def load_prepared(out):
    if json.loads((out / 'rules.json').read_text(encoding='utf-8')) != RULE:
        raise ValueError('rules changed after availability')
    registration = json.loads((out / 'registration.json').read_text(encoding='utf-8'))
    if registration['code_sha256'] != digest(__file__):
        raise ValueError('code changed after availability; register a correction')
    artifacts = json.loads((out / 'availability_artifacts.json').read_text(encoding='utf-8'))
    for name, expected_hash in artifacts.items():
        if digest(out / name) != expected_hash:
            raise ValueError(f'prepared artifact changed: {name}')
    with np.load(out / 'market.npz', allow_pickle=False) as data:
        market = {name: data[name] for name in data.files}
    with np.load(out / 'derived.npz', allow_pickle=False) as data:
        derived = {name: data[name] for name in data.files}
    with np.load(out / 'indices.npz', allow_pickle=False) as data:
        indices = {name: data[name] for name in data.files}
    return market, derived, indices


def inspect_matches(out, market, indices, training, query, neighbors, counts, distances):
    import html
    records = []
    plots = out / 'shape_checks'
    plots.mkdir(exist_ok=False)
    for year in ('2022', '2023'):
        available = np.flatnonzero(np.char.startswith(market['dates'][indices['query'][:, 0]], year)
                                   & (counts >= 45))
        distance_order = available[np.argsort(distances[available, 0], kind='stable')]
        for group_index, group in enumerate(np.array_split(distance_order, 3)):
            for position in sorted(group)[:10]:
                chosen = neighbors[position]
                direct = np.mean((training - query[position]) ** 2, axis=1)
                reference = choose_spaced(np.lexsort((np.arange(len(direct)), direct)), indices['dictionary'])
                if not np.array_equal(reference, chosen):
                    raise ValueError('exact full dictionary match check failed')
                detailed_distances = np.mean((training[chosen] - query[position]) ** 2, axis=1)
                if abs(detailed_distances.mean() - distances[position, 0]) > 1e-10:
                    raise ValueError('match distance tolerance failed')
                row, column = indices['query'][position]
                record = {'query_position': int(position), 'date': str(market['dates'][row]),
                          'code': str(market['codes'][column]), 'distance_group': group_index,
                          'unknown_neighbors': int(50 - counts[position]), 'neighbors': []}
                for candidate, distance in zip(chosen, detailed_distances):
                    train_row, train_column = indices['dictionary'][candidate]
                    if train_row >= row:
                        raise ValueError('future neighbor leakage')
                    record['neighbors'].append({'dictionary_index': int(candidate),
                                                'date': str(market['dates'][train_row]),
                                                'code': str(market['codes'][train_column]),
                                                'distance': float(distance),
                                                'status': int(indices['structures'][candidate, 0]),
                                                'channel_distance': np.mean((training[candidate] - query[position]).reshape(3, 20) ** 2, axis=1).tolist()})
                paths = [query[position], training[chosen[0]], training[chosen[24]], training[chosen[49]]]
                colors = ['black', '#0072b2', '#009e73', '#d55e00']
                svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="700">',
                       '<rect width="900" height="700" fill="white"/>',
                       f'<text x="30" y="25">{html.escape(record["date"] + " " + record["code"])}: query / nearest / median / farthest</text>']
                for channel in range(3):
                    values = np.array([path[channel * 20:(channel + 1) * 20] for path in paths])
                    minimum, maximum = float(values.min()), float(values.max())
                    spread = max(maximum - minimum, 1e-12)
                    for path, color in zip(values, colors):
                        points = ' '.join(f'{40 + step * 42:.2f},{80 + channel * 205 + 160 * (maximum - value) / spread:.2f}'
                                          for step, value in enumerate(path))
                        svg.append(f'<polyline points="{points}" stroke="{color}" fill="none" stroke-width="2"/>')
                svg.append('</svg>')
                (plots / f'{year}_{position}.svg').write_text('\n'.join(svg), encoding='utf-8')
                records.append(record)
    write_json(out / 'shape_checks.json', records)


def simulate_account(market, derived, candidate_lists, name, multiplier=1):
    dates, codes = market['dates'], market['codes']
    if RULE['signals'][0] not in dates or RULE['maturity_end'] not in dates:
        raise ValueError('account calendar does not cover frozen range')
    first = int(np.searchsorted(dates, RULE['signals'][0]))
    last = int(np.searchsorted(dates, RULE['maturity_end']))
    sleeves = [dict(cash=20000.0, holding=None, unknown=False) for _ in range(5)]
    orders, trades, days = [], [], []
    scheduled = []
    capital_blocked = 0
    for index in range(first, last + 1):
        date = str(dates[index])
        for sleeve in sleeves:
            holding = sleeve['holding']
            if holding is None or sleeve['unknown']:
                continue
            column = holding['column']
            if derived['action'][index, column]:
                sleeve['unknown'] = True
                orders.append(dict(date=date, side='holding', code=str(codes[column]),
                                   state='unknown_account_action', shares=holding['shares']))
                continue
            if index < holding['entry'] + 5:
                continue
            state = execution_state(market, index, column, 'sell', multiplier)
            orders.append(dict(date=date, side='sell', code=str(codes[column]), state=state))
            if state == 'proxy':
                gross = holding['shares'] * market['open'][index, column] * (1 - 0.001 * multiplier)
                proceeds = gross - fees(gross, date, 'sell', multiplier)
                sleeve['cash'] += proceeds
                trades.append(dict(code=str(codes[column]), entry_date=str(dates[holding['entry']]),
                                   exit_date=date, shares=holding['shares'], buy_cost=holding['cost'],
                                   sell_proceeds=float(proceeds), profit=float(proceeds - holding['cost'])))
                sleeve['holding'] = None
            elif state != 'blocked' or index >= holding['entry'] + 8:
                sleeve['unknown'] = True
        for sleeve_index, column in scheduled:
            sleeve = sleeves[sleeve_index]
            decision, shares = entry_decision(market, derived, index - 1, column, sleeve['cash'], multiplier)
            record = dict(date=date, signal_date=str(dates[index - 1]), side='buy',
                          code=str(codes[column]), state=int(decision), shares=shares)
            if decision == MATURED:
                gross = shares * market['open'][index, column] * (1 + 0.001 * multiplier)
                cost = gross + fees(gross, date, 'buy', multiplier)
                sleeve['cash'] -= cost
                if sleeve['cash'] < -1e-8:
                    raise ValueError('negative sleeve cash')
                sleeve['holding'] = dict(column=column, entry=index, shares=shares, cost=float(cost))
                record['cost'] = float(cost)
            elif decision not in (ENTRY_FAIL,):
                sleeve['unknown'] = True
            orders.append(record)
        scheduled = []
        used_codes = {sleeve['holding']['column'] for sleeve in sleeves if sleeve['holding'] is not None}
        free = [position for position, sleeve in enumerate(sleeves)
                if sleeve['holding'] is None and not sleeve['unknown']]
        candidates = candidate_lists.get(index, [])
        for column in candidates:
            if column in used_codes:
                continue
            if not free:
                capital_blocked += 1
                continue
            scheduled.append((free.pop(0), column))
            used_codes.add(column)
        net_value = 0.0
        complete = True
        for sleeve in sleeves:
            if sleeve['unknown']:
                complete = False
                continue
            net_value += sleeve['cash']
            if sleeve['holding'] is not None:
                column = sleeve['holding']['column']
                close = market['close'][index, column]
                if not np.isfinite(close) or close <= 0:
                    complete = False
                else:
                    net_value += close * sleeve['holding']['shares']
        days.append(dict(date=date, net_value=float(net_value) if complete else None,
                         known_component_value=float(net_value),
                         uncertain_sleeves=sum(sleeve['unknown'] for sleeve in sleeves)))
    profits = [trade['profit'] for trade in trades]
    net_values = [day['net_value'] for day in days]
    complete = all(value is not None for value in net_values)
    realized_balance = sum(sleeve['cash'] for sleeve in sleeves)
    for trade in trades:
        if not np.isclose(trade['sell_proceeds'] - trade['buy_cost'], trade['profit'], atol=1e-8):
            raise ValueError('trade reconciliation failure')
    summary = dict(name=name, multiplier=multiplier, ledger_complete=complete,
                   end_value=net_values[-1] if complete else None, completed_trades=len(trades),
                   trade_win_ratio=sum(value > 0 for value in profits) / len(profits) if profits else None,
                   completed_by_year=dict(Counter(trade['exit_date'][:4] for trade in trades)),
                   unknown_buy=sum(order['side'] == 'buy' and order['state'] not in (ENTRY_FAIL, MATURED) for order in orders),
                   buy_attempts=sum(order['side'] == 'buy' for order in orders),
                   unknown_sell=sum(order['side'] == 'sell' and order['state'] == 'unknown' for order in orders),
                   sell_attempts=sum(order['side'] == 'sell' for order in orders),
                   unknown_action=sum(order['state'] == 'unknown_account_action' for order in orders),
                   capital_blocked=capital_blocked, cash_subtotal_not_account_value=float(realized_balance),
                   unresolved_sleeves=sum(sleeve['unknown'] or sleeve['holding'] is not None for sleeve in sleeves),
                   accounting_check='nonnegative_cash_and_each_completed_trade_reconciled',
                   official_costs_verified=False)
    summary['periods'] = {}
    for frequency in ('W-SUN', 'M'):
        buckets = {}
        previous = 100000.0
        for day in days:
            period = str(pd.Period(day['date'], freq=frequency))
            if period not in buckets:
                buckets[period] = {'start': previous, 'end': day['net_value'], 'known': True}
            bucket = buckets[period]
            bucket['end'] = day['net_value']
            bucket['known'] &= previous is not None and day['net_value'] is not None
            previous = day['net_value']
        wins = sum(bucket['known'] and bucket['end'] > bucket['start'] for bucket in buckets.values())
        summary['periods'][frequency] = dict(periods=len(buckets), known_periods=sum(bucket['known'] for bucket in buckets.values()),
                                            wins=int(wins), full_account_win_ratio=wins / len(buckets) if all(bucket['known'] for bucket in buckets.values()) else None)
    if complete:
        values = np.array([100000.0] + net_values)
        summary['maximum_drawdown'] = float(np.max(1 - values / np.maximum.accumulate(values)))
        summary['remove_top3_diagnostic'] = float(values[-1] - sum(sorted(profits, reverse=True)[:3]))
    else:
        summary['maximum_drawdown'] = None
        summary['remove_top3_diagnostic'] = None
    return dict(summary=summary, orders=orders, trades=trades, daily=days, sleeves=sleeves)


def evaluate(out):
    if not (out / 'availability_reported.json').exists():
        raise ValueError('availability must be reported before evaluating outcomes')
    with (out / 'outcome_stage.lock').open('x', encoding='utf-8') as stream:
        stream.write(now())
    market, derived, indices = load_prepared(out)
    append_event(out, {'event': 'outcome_start', 'version': RULE['version'], 'outcome_values_computed': True})
    training = np.load(out / 'training.npy', mmap_mode='r')
    query = np.load(out / 'query.npy', mmap_mode='r')
    neighbors = np.load(out / 'neighbors.npy', mmap_mode='r')
    counts = np.load(out / 'known_counts.npy')
    distances = np.load(out / 'distances.npy')
    inspect_matches(out, market, indices, training, query, neighbors, counts, distances)
    labels = label_values(market, derived, indices['dictionary'], indices['structures'])
    scores = np.full(len(query), np.nan)
    for position, chosen in enumerate(neighbors):
        score, unknown = score_known(labels, chosen)
        if unknown != 50 - int(counts[position]):
            raise ValueError('availability/evaluation known-label mismatch')
        if score is not None:
            scores[position] = score
    np.save(out / 'label_values.npy', labels)
    np.save(out / 'scores.npy', scores)
    signal_records = []
    strategy_lists, momentum_lists = {}, {}
    random_lists = {seed: {} for seed in RULE['random_seeds']}
    random_generators = {seed: np.random.default_rng(seed) for seed in RULE['random_seeds']}
    for row, date in enumerate(market['dates']):
        if not RULE['signals'][0] <= date <= RULE['signals'][1]:
            continue
        positions = np.flatnonzero(indices['query'][:, 0] == row)
        candidates = positions[np.isfinite(scores[positions]) & (scores[positions] > 0)]
        ranked = candidates[np.lexsort((indices['query'][candidates, 1], -scores[candidates]))]
        strategy_lists[row] = indices['query'][ranked[:5], 1].tolist()
        safe_pool = indices['query'][positions, 1]
        if len(safe_pool):
            momentum = derived['continuous'][row, safe_pool] / derived['continuous'][row - 20, safe_pool] - 1
            momentum_lists[row] = safe_pool[np.lexsort((safe_pool, -momentum))[:5]].tolist()
        else:
            momentum_lists[row] = []
        for seed, generator in random_generators.items():
            random_lists[seed][row] = generator.permutation(safe_pool)[:5].tolist()
        selected = [dict(code=str(market['codes'][indices['query'][position, 1]]),
                         score=float(scores[position]), unknown_neighbors=int(50 - counts[position]),
                         query_position=int(position)) for position in ranked[:5]]
        signal_records.append(dict(date=str(date), support=int(market['expected'][row].sum()),
                                   feature_available=int(derived['feature_valid'][row].sum()),
                                   safe_queries=len(positions), score_available=int(np.isfinite(scores[positions]).sum()),
                                   positive_scores=len(candidates), selected=selected))
    write_json(out / 'signals.json', signal_records)
    summaries = []
    account_dir = out / 'accounts'
    account_dir.mkdir(exist_ok=False)
    candidates = {'macd': strategy_lists, 'r20': momentum_lists,
                  **{f'random_{seed}': value for seed, value in random_lists.items()}}
    for multiplier in (1, 2):
        for name, candidate_lists in candidates.items():
            result = simulate_account(market, derived, candidate_lists, name, multiplier)
            write_json(account_dir / f'{name}_cost{multiplier}.json', result)
            summaries.append(result['summary'])
    years = {}
    for year in ('2022', '2023'):
        records = [record for record in signal_records if record['date'].startswith(year)]
        empty = sum(not record['selected'] for record in records)
        years[year] = dict(signal_days=len(records), empty_days=empty,
                           empty_ratio=empty / len(records), selected_signals=sum(len(record['selected']) for record in records))
    report = dict(time=now(), version=RULE['version'], outcome_stage_runs=1,
                  conclusion='incomplete_until_actual_account_and_cost_evidence_are_complete',
                  historical_exploration_only=True, independent_validation=False, official_costs_verified=False,
                  labels_known=int(np.isfinite(labels).sum()), labels_positive=int((labels > 0).sum()),
                  labels_negative=int((labels < 0).sum()), labels_zero=int((labels == 0).sum()),
                  signal_years=years, accounts=summaries,
                  sample_check_count=len(json.loads((out / 'shape_checks.json').read_text(encoding='utf-8'))))
    write_json(out / 'results.json', report)
    append_event(out, {'event': 'outcome_complete', 'results_sha256': digest(out / 'results.json'),
                       'state': report['conclusion']})
    print(json.dumps(report, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['prepare', 'evaluate'])
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--cache', type=Path)
    args = parser.parse_args()
    if not args.out.resolve().is_relative_to((ROOT / '.local_records').resolve()):
        raise ValueError('research output must stay in local records')
    if args.stage == 'prepare':
        prepare(args.out, args.cache)
    else:
        evaluate(args.out)


if __name__ == '__main__':
    main()
