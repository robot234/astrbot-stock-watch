from datetime import datetime

from core import Quote, calc_rsi, in_trading_session, is_tradable, parse_codes, score_quote


def test_parse_codes_deduplicates_and_normalizes():
    assert parse_codes("sh600000，600000 sz000001") == ["600000", "000001"]


def test_rsi_is_bounded():
    assert calc_rsi([1, 2, 3, 4, 5, 6, 7, 8]) == 100.0


def test_score_explains_components():
    quote = Quote("600000", "测试", 10, pct_change=1, rsi6=20, ma5=12, ma10=11, ma20=10, volume_ratio=1.5)
    candidate = score_quote(quote)
    assert candidate.score == 30
    assert len(candidate.reasons) == 3


def test_tradable_rejects_limits_and_price():
    assert is_tradable(Quote("600000", "测试", 10))
    assert not is_tradable(Quote("600000", "测试", 1))
    assert not is_tradable(Quote("600000", "测试", 10, limit_up=True))


def test_session_window():
    assert in_trading_session(datetime(2026, 8, 27, 10, 0).astimezone())
    assert not in_trading_session(datetime(2026, 8, 27, 12, 0).astimezone())
