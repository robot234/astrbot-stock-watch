import importlib.util
from pathlib import Path

import numpy as np
import pytest

SPEC = importlib.util.spec_from_file_location('macd_shape_research', Path(__file__).resolve().parents[1] / 'macd_shape_research.py')
research = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(research)


def toy_market(days=200):
    close = (100 + np.arange(days) * 0.1)[:, None]
    market = {name: np.ones((days, 1)) for name in research.FIELDS}
    market.update(close=close.copy(), preclose=np.vstack([close[:1], close[:-1]]),
                  open=close.copy(), high=close + 1, low=close - 1,
                  volume=np.ones_like(close) * 1000, isST=np.zeros_like(close),
                  expected=np.ones_like(close, dtype=bool), age=np.arange(days)[:, None] + 1000,
                  dates=np.array([str(np.datetime64('2021-01-01') + np.timedelta64(index, 'D')) for index in range(days)]),
                  codes=np.array(['sh.600000']))
    return market


def test_continuous_split_does_not_reset_price_or_atr():
    market = toy_market()
    reference = research.continuous_features(market)
    for name in ('open', 'high', 'low', 'close', 'preclose'):
        market[name][150:] /= 2
    market['volume'][150:] *= 2
    derived = research.continuous_features(market)
    np.testing.assert_allclose(derived['continuous'], reference['continuous'], atol=1e-10)
    np.testing.assert_allclose(derived['macd'], reference['macd'], atol=1e-10)
    np.testing.assert_allclose(derived['atr_raw'][150:] * 2, reference['atr_raw'][150:], atol=1e-10)
    assert derived['count'][150, 0] == 151
    assert derived['volume_count'][150, 0] == 0
    assert not derived['feature_valid'][189, 0]
    assert derived['feature_valid'][190, 0]


def test_atr_seed_recurrence_and_gap():
    market = toy_market()
    market['close'][:] = 100
    market['open'][:] = 100
    market['high'][:] = 101
    market['low'][:] = 99
    market['preclose'][:] = 100
    derived = research.continuous_features(market)
    assert np.isnan(derived['atr_raw'][13, 0])
    assert derived['atr_raw'][14, 0] == 2
    market['close'][15:] = 110
    market['open'][15:] = 110
    market['high'][15:] = 111
    market['low'][15:] = 109
    market['preclose'][16:] = 110
    derived = research.continuous_features(market)
    assert np.isclose(derived['atr_raw'][15, 0], (13 * 2 + 11) / 14)


def test_missing_and_suspension_reset_not_zero():
    market = toy_market()
    market['preclose'][150] = np.nan
    market['tradestatus'][160] = 0
    derived = research.continuous_features(market)
    assert derived['count'][150, 0] == 0
    assert derived['count'][151, 0] == 1
    assert np.isnan(derived['continuous'][160, 0])
    assert np.isnan(derived['atr_raw'][161, 0])


def test_feature_is_causal_under_future_change():
    market = toy_market()
    reference = research.continuous_features(market)
    for name in research.FIELDS:
        market[name][180:] = np.nan
    changed = research.continuous_features(market)
    for name in ('continuous', 'macd', 'atr_raw', 'volume_channel', 'count', 'feature_valid'):
        np.testing.assert_allclose(changed[name][:180], reference[name][:180], atol=1e-10)


def test_known_45_of_fixed_50_no_replacement():
    values = np.arange(51, dtype=float)
    values[:5] = np.nan
    neighbors = np.arange(50)
    score, unknown = research.score_known(values, neighbors)
    assert unknown == 5 and score == np.mean(np.arange(5, 50))
    values[5] = np.nan
    assert research.score_known(values, neighbors) == (None, 6)


def test_spaced_neighbors_exact_ties_and_no_label_filter():
    training = np.zeros((100, 60))
    training[:, 0] = np.arange(100) / 10
    indices = np.column_stack([np.arange(100), np.arange(100) % 3])
    query = np.zeros(60)
    distances = np.mean(training ** 2, axis=1)
    chosen, direct = research.exact_neighbors(distances, query, training, indices)
    reference = research.choose_spaced(np.lexsort((np.arange(100), distances)), indices)
    np.testing.assert_array_equal(chosen, reference)
    np.testing.assert_allclose(direct, distances[chosen], atol=1e-10)


def test_continuous_label_split_is_not_artificial_loss():
    market = toy_market()
    for name in ('open', 'high', 'low', 'close', 'preclose'):
        market[name][153:] /= 2
    derived = research.continuous_features(market)
    indices = np.array([[150, 0]])
    structures = np.array([research.label_structure(market, derived, 150, 0)])
    assert structures[0, 0] == research.MATURED
    values = research.label_values(market, derived, indices, structures)
    assert -0.01 < values[0] < 0.01


def test_rounding_fees_and_integer_lots():
    assert research.round_cent(2.655) == 2.66
    shares = research.entry_lots(20, 20000, '2022-01-04')
    gross = shares * 20 * 1.001
    assert shares % 100 == 0 and gross + research.fees(gross, '2022-01-04', 'buy') <= 20000
    assert research.fees(10000, '2023-08-28', 'sell') < research.fees(10000, '2023-08-25', 'sell')


def test_unknown_open_order_is_not_zero_label():
    market = toy_market()
    market['open'][151] = research.round_cent(market['preclose'][151, 0] * 1.1)
    market['high'][151] = market['open'][151]
    derived = research.continuous_features(market)
    status = research.label_structure(market, derived, 150, 0)
    assert status[0] == research.UNKNOWN_ENTRY
    assert np.isnan(research.label_values(market, derived, np.array([[150, 0]]), np.array([status]))[0])


def test_standardization_does_not_fit_query():
    training = np.arange(600, dtype=float).reshape(10, 60)
    query = training[:1].copy()
    _, _, mean, std = research.standardize(training, query)
    _, _, changed_mean, changed_std = research.standardize(training, query * 1000)
    np.testing.assert_array_equal(mean, changed_mean)
    np.testing.assert_array_equal(std, changed_std)


def test_account_action_does_not_fabricate_cash_or_shares(monkeypatch):
    market = toy_market()
    market['dates'] = np.array([str(np.datetime64('2021-09-01') + np.timedelta64(index, 'D')) for index in range(200)])
    monkeypatch.setitem(research.RULE, 'maturity_end', str(market['dates'][-1]))
    row = int(np.searchsorted(market['dates'], '2022-01-20'))
    for name in ('open', 'high', 'low', 'close', 'preclose'):
        market[name][row + 3:] /= 2
    derived = research.continuous_features(market)
    result = research.simulate_account(market, derived, {row: [0]}, 'toy')
    assert result['summary']['unknown_action'] == 1
    assert result['summary']['end_value'] is None
    assert result['summary']['completed_trades'] == 0
    assert result['sleeves'][0]['holding']['shares'] > 0


def test_certified_matcher_falls_back_when_boundary_unproved():
    training = np.zeros((100, 60))
    indices = np.column_stack([np.zeros(100, dtype=int), np.arange(100)])
    candidates = np.arange(50, 100)
    chosen, distances, fallback = research.certified_neighbors(np.zeros(50), candidates, np.zeros(60), training, indices)
    assert fallback
    np.testing.assert_array_equal(chosen, np.arange(50))
    np.testing.assert_array_equal(distances, np.zeros(50))


def test_certified_matcher_proves_omitted_candidates_outside_radius():
    training = np.repeat(np.arange(400)[:, None], 60, axis=1).astype(float)
    indices = np.column_stack([np.zeros(400, dtype=int), np.arange(400)])
    candidates = np.arange(256)
    chosen, distances, fallback = research.certified_neighbors(np.sum(training[candidates] ** 2, axis=1), candidates,
                                                              np.zeros(60), training, indices)
    assert not fallback
    np.testing.assert_array_equal(chosen, np.arange(50))
    np.testing.assert_allclose(distances, np.arange(50) ** 2, atol=1e-10)


def test_faiss_certification_matches_exhaustive_float64():
    faiss = pytest.importorskip('faiss')
    generator = np.random.default_rng(20261005)
    training = generator.normal(size=(4000, 60))
    query = generator.normal(size=(10, 60))
    identities = np.column_stack([np.arange(4000), np.arange(4000) % 500])
    matcher = faiss.IndexFlatL2(60)
    matcher.add(training.astype(np.float32))
    approximate, candidates = matcher.search(query.astype(np.float32), 256)
    for position, vector in enumerate(query):
        chosen, distances, _ = research.certified_neighbors(approximate[position], candidates[position], vector, training, identities)
        direct = np.mean((training - vector) ** 2, axis=1)
        reference = research.choose_spaced(np.lexsort((np.arange(len(direct)), direct)), identities)
        np.testing.assert_array_equal(chosen, reference)
        np.testing.assert_allclose(distances, direct[chosen], atol=1e-10)
