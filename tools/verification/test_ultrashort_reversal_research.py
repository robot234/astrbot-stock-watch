import importlib.util
from pathlib import Path

import numpy as np
import pytest

SPEC = importlib.util.spec_from_file_location('ultrashort_research', Path(__file__).resolve().parents[1] / 'ultrashort_reversal_research.py')
research = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(research)


def market(days=55, stocks=6):
    dates = np.array([str(np.datetime64('2024-01-01') + np.timedelta64(index, 'D')) for index in range(days)])
    shape = (days, stocks)
    data = {name: np.ones(shape) for name in research.FIELDS}
    data.update(dates=dates, codes=np.array([f'sh.600{column:03d}' for column in range(stocks)]),
                open=np.full(shape, 10.), close=np.full(shape, 10.), high=np.full(shape, 10.5),
                low=np.full(shape, 9.5), preclose=np.full(shape, 10.),
                volume=np.full(shape, 1000000.), amount=np.full(shape, 2e8), isST=np.zeros(shape),
                expected=np.ones(shape, dtype=bool), ordinary=np.ones(shape, dtype=bool), age=np.full(shape, 500))
    return data


def test_half_up_and_board_rates():
    data = market()
    data['codes'][0] = 'sz.300001'
    derived = research.features(data)
    assert derived['upper'][30, 0] == 12
    assert derived['lower'][30, 0] == 8
    assert derived['upper'][30, 1] == 11
    assert research.money(1.005) == 1.01


def test_qualification_unknown_not_false_and_no_fill():
    data = market()
    data['isST'][30, 0] = np.nan
    data['tradestatus'][30, 1] = 0
    data['volume'][30, 2] = 0
    data['amount'][25, 3] = np.nan
    data['age'][30, 4] = 119
    derived = research.features(data)
    assert not derived['known'][30, 0]
    assert not derived['known'][30, 2]
    assert research.selection(data, derived, 30) == [5]


def test_invalid_state_codes_are_unknown_not_known_exclusions():
    data = market()
    data['isST'][30, 0] = -1
    data['tradestatus'][30, 1] = 2
    derived = research.features(data)
    assert not derived['known'][30, 0]
    assert not derived['known'][30, 1]
    assert not derived['eligible'][30, 0]


def test_continuous_ratio_split_does_not_create_fake_loss():
    data = market()
    original = research.features(data)
    for name in ('open', 'high', 'low', 'close', 'preclose'):
        data[name][30:] /= 2
    derived = research.features(data)
    np.testing.assert_allclose(derived['score'][30:], original['score'][30:])
    assert derived['action'][30, 0]
    trade = research.ticket(data, derived, 28, 0, 2, 10000)
    assert trade['status'] == 'unknown_corporate_action'
    assert trade['net_return'] is None
    assert trade['unknown_at'] == 30


def test_future_does_not_change_current_rank():
    data = market()
    baseline = research.selection(data, research.features(data), 25)
    data['close'][26:, 0] = 100
    assert research.selection(data, research.features(data), 25) == baseline


def test_costs_commission_tax_and_pressure():
    data = market()
    derived = research.features(data)
    ordinary = research.ticket(data, derived, 25, 0, 1, 10000)
    pressure = research.ticket(data, derived, 25, 0, 1, 10000, True)
    assert ordinary['status'] == 'completed'
    assert ordinary['spent'] <= 10000
    assert ordinary['shares'] % 100 == 0
    assert pressure['net_return'] < ordinary['net_return'] < 0
    assert ordinary['pnl'] == research.money(ordinary['received'] - ordinary['spent'])


def test_limit_entry_cash_known_exit_unknown():
    data = market()
    data['open'][26, 0] = 11
    data['high'][26, 0] = 11
    derived = research.features(data)
    assert research.ticket(data, derived, 25, 0, 1, 10000)['status'] == 'entry_unfilled_limit'
    data['open'][26, 0] = 10
    data['open'][27, 0] = 9
    data['low'][27, 0] = 9
    derived = research.features(data)
    assert research.ticket(data, derived, 25, 0, 1, 10000)['status'] == 'unknown_exit_limit'


def test_actual_nav_becomes_unknown_at_event_not_in_advance():
    data = market(stocks=1)
    for name in ('open', 'high', 'low', 'close', 'preclose'):
        data[name][23:] /= 2
    result = research.simulate(data, research.features(data), '2024-01-21', '2024-02-24', 2, detail=True)
    assert result['navigation'][1]['nav'] is not None
    assert result['navigation'][2]['nav'] is not None
    assert result['navigation'][3]['nav'] is None
    assert result['account_total_return'] is None
    assert result['monthly_t'] is None
    assert result['unknown_tickets'] > 0


def test_one_shot_marker_refuses_rerun(tmp_path):
    research.write_json(tmp_path / 'ONCE_STARTED.json', {'started': 'test'})
    with pytest.raises(FileExistsError):
        research.run('test', tmp_path, tmp_path)


def test_new_output_directory_cannot_repeat_completed_half_blind(tmp_path, monkeypatch):
    monkeypatch.setattr(research, 'ROOT', tmp_path)
    report = tmp_path / '.local_records/ultrashort_20261005/test/report.json'
    report.parent.mkdir(parents=True)
    research.write_json(report, {'already': 'completed'})
    with pytest.raises(RuntimeError, match='already_completed'):
        research.run('test', tmp_path, tmp_path / 'another_output')


def test_random_seed_reproducibility_and_two_versions_only():
    data = market()
    derived = research.features(data)
    first = research.simulate(data, derived, '2024-01-21', '2024-02-24', 1, seed=123)
    second = research.simulate(data, derived, '2024-01-21', '2024-02-24', 1, seed=123)
    assert first == second
    assert first['unknown_fraction'] == 0
    assert len(research.SEEDS) == 20


def test_missing_month_or_benchmark_cannot_pass():
    statistics = research.month_statistics({'2024-01': 0.1, '2024-02': None})
    assert statistics['mean_monthly_net'] is None
    normal = {'mean_monthly_net': 0.1, 'monthly_t': 3, 'account_total_return': 0.3,
              'monthly': {'2024-01': 0.1}, 'max_drawdown': 0.1, 'unknown_fraction': 0.01}
    checks = research.gates(normal, normal, [], {'returns': {}, 'monthly1000': {}})
    assert not checks['all_pass']
    assert not checks['checks']['beat_csi1000']
    assert not checks['checks']['beat_16_of_20_random']


def test_exact_signal_maturity_same_for_a_and_b():
    data = market()
    derived = research.features(data)
    short = research.simulate(data, derived, '2024-01-21', '2024-02-24', 1)
    longer = research.simulate(data, derived, '2024-01-21', '2024-02-24', 2)
    assert short['planned_tickets'] == longer['planned_tickets']
