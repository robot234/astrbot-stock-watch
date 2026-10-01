import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("fetch_missing_tushare_history.py")
SPEC = importlib.util.spec_from_file_location("missing_tushare_history", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_target_window_has_exact_64_dates_and_known_bounds():
    assert len(MODULE.TARGET_DATES) == 64
    assert MODULE.TARGET_DATES[0] == "2025-12-24"
    assert MODULE.TARGET_DATES[-1] == "2026-04-02"
    assert list(MODULE.TARGET_DATES) == sorted(set(MODULE.TARGET_DATES))


def test_row_validation_marks_daily_omissions_unknown():
    rows = [{
        "trade_date": "2026-04-02", "code": "600000", "ts_code": "600000.SH",
        "open": 10, "high": 11, "low": 9, "close": 10.5, "pre_close": 10,
        "pct_change": 5, "volume": 1, "amount": 2, "source": "tushare", "basis": "unadjusted",
    }]
    result = MODULE._validate_rows(rows, "2026-04-02")
    assert result["row_count"] == 1
    assert result["unique_code_count"] == 1
    assert result["invalid_row_checks"] == []
    assert result["daily_omitted_suspended_unknown"] is True
    assert result["historical_universe_status"] == "unknown_without_bak_basic"


def test_row_validation_rejects_duplicate_and_provenance_mismatch():
    row = {
        "trade_date": "2026-04-02", "code": "600000", "ts_code": "600000.SH",
        "open": 10, "high": 11, "low": 9, "close": 10.5, "pre_close": 10,
        "pct_change": 5, "volume": 1, "amount": 2, "source": "eastmoney", "basis": "adjusted",
    }
    result = MODULE._validate_rows([row, row], "2026-04-02")
    assert result["duplicate_codes"] == ["600000"]
    assert "provenance" in result["invalid_row_checks"]
