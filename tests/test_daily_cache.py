from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_stock_watch.core import CHINA_TZ, Quote
from astrbot_stock_watch.providers import SinaQuoteProvider
from astrbot_stock_watch.storage import StockStore


def test_snapshot_row_parses_eastmoney_fields():
    quote = SinaQuoteProvider._snapshot_row({"f12": "600000", "f14": "浦发银行", "f2": 10.5, "f3": 5.0, "f5": 100, "f6": 123456})
    assert quote is not None
    assert quote.code == "600000"
    assert quote.name == "浦发银行"
    assert quote.pct_change == 5.0
    assert round(quote.prev_close, 2) == 10.0


def test_daily_quotes_replace_and_read(tmp_path: Path):
    store = StockStore(tmp_path / "stock.sqlite3")
    quote = Quote("600000", "测试", 10.0, amount=123, fetched_at=datetime.now(CHINA_TZ))
    assert store.save_daily_quotes("2026-08-27", [quote]) == 1
    saved = store.daily_quotes("2026-08-27")
    assert len(saved) == 1
    assert saved[0].code == "600000"
    assert saved[0].amount == 123
