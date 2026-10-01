"""Small synthetic, self-contained checks for the offline research harness."""

from datetime import date, timedelta
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("historical_reselection.py")
CORE = SCRIPT.parents[2] / "core.py"
spec = importlib.util.spec_from_file_location("historical_reselection", SCRIPT)
historical = importlib.util.module_from_spec(spec)
spec.loader.exec_module(historical)


class HistoricalReselectionTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / "fixture.sqlite3"
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.executescript("""
            CREATE TABLE datasets(dataset_id TEXT PRIMARY KEY,dataset_key TEXT);
            CREATE TABLE batches(batch_id TEXT PRIMARY KEY,dataset_id TEXT,status TEXT,quality TEXT,source TEXT,basis TEXT,published_at TEXT);
            CREATE TABLE day_partitions(partition_id TEXT PRIMARY KEY,row_count INTEGER,validation_status TEXT,source TEXT,basis TEXT);
            CREATE TABLE batch_days(batch_id TEXT,trade_date TEXT,partition_id TEXT,row_count INTEGER);
            CREATE TABLE partition_bars(partition_id TEXT,trade_date TEXT,code TEXT,name TEXT,open REAL,high REAL,low REAL,close REAL,pre_close REAL,pct_change REAL,volume REAL,amount REAL,source TEXT,basis TEXT);
            INSERT INTO datasets VALUES('d','tushare_daily');
            INSERT INTO batches VALUES('b','d','published','good','tushare','unadjusted','2026-09-24');
        """)
        self.dates = [(date(2026, 6, 1) + timedelta(days=offset)).isoformat() for offset in range(29)]
        for offset, day in enumerate(self.dates):
            partition = f"p{offset}"
            self.db.execute("INSERT INTO day_partitions VALUES(?,2,'validated','tushare','unadjusted')", (partition,))
            self.db.execute("INSERT INTO batch_days VALUES('b',?,?,2)", (day, partition))
            for code, amount in (("000001", 1000), ("000002", 500)):
                close = 10 + offset * (0.15 if code == "000001" else 0.10)
                previous = close - (0.15 if code == "000001" else 0.10)
                self.db.execute("INSERT INTO partition_bars VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (partition, day, code, code, close, close + 0.2, close - 0.2,
                                 close, previous, 1.0, 100, amount, "tushare", "unadjusted"))
        self.db.commit()
        self.core = historical.load_core(CORE)

    def run_fixture(self):
        with historical.connect_readonly(self.path) as reader:
            return historical.run(reader, self.core, batch_id="b", as_of=self.dates[22],
                                  through=self.dates[-1], horizon=2, deep_limit=2,
                                  min_market_rows=2)

    def test_reselect_without_saved_candidates_or_future_leakage(self):
        original = self.run_fixture()
        first = original["days"][0]
        self.assertEqual(original["history_dates_at_start"], 23)
        self.assertTrue(any("not published" in text for text in original["warnings"]))
        self.assertEqual(first["history_usable"], 2)
        self.assertTrue(first["candidates"])
        self.assertTrue(all(item["risk"] == "unknown" for item in first["candidates"]))
        self.assertEqual(original["summary"]["pending"], 2 * len(original["days"][-2]["candidates"]))
        observed = original["summary"]["observed_returns"]
        self.assertEqual(observed["denominator_observed_only"], original["summary"]["observed_raw_close"])
        self.assertEqual(observed["positive"] + observed["negative"] + observed["zero"],
                         observed["denominator_observed_only"])
        self.assertGreater(observed["mean_raw_return_pct"], 0)
        self.db.execute("UPDATE partition_bars SET close=close+100,high=high+100 "
                        "WHERE trade_date=? AND code='000001'", (self.dates[24],))
        self.db.commit()
        modified = self.run_fixture()
        self.assertEqual([item["code"] for item in first["candidates"]],
                         [item["code"] for item in modified["days"][0]["candidates"]])
        self.assertNotEqual(first["candidates"][0]["raw_return_pct"],
                            modified["days"][0]["candidates"][0]["raw_return_pct"])

    def test_observed_only_mean_and_sign_denominator(self):
        rows = [
            {"outcome": "observed_unadjusted_close", "as_of_close": 10, "target_close": 11},
            {"outcome": "observed_unadjusted_close", "as_of_close": 10, "target_close": 9},
            {"outcome": "observed_unadjusted_close", "as_of_close": 10, "target_close": 10},
            {"outcome": "pending", "as_of_close": 10, "target_close": None},
            {"outcome": "unknown", "as_of_close": 10, "target_close": None},
        ]
        summary = historical.observed_return_summary(rows)
        self.assertEqual((summary["denominator_observed_only"], summary["positive"],
                          summary["negative"], summary["zero"], summary["mean_raw_return_pct"]),
                         (3, 1, 1, 1, 0.0))
        self.assertIn("non_executable_retrospective", summary["label"])
        empty = historical.observed_return_summary(rows[-2:])
        self.assertEqual(empty["denominator_observed_only"], 0)
        self.assertIsNone(empty["mean_raw_return_pct"])

    def test_readonly_and_provenance_fail_closed(self):
        with historical.connect_readonly(self.path) as reader:
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute("DELETE FROM partition_bars")
            with self.assertRaises(ValueError):
                historical.run(reader, self.core, batch_id="missing", as_of=self.dates[22], through=self.dates[-1])
        self.db.execute("UPDATE day_partitions SET validation_status='rejected' WHERE partition_id='p22'")
        self.db.commit()
        with historical.connect_readonly(self.path) as reader:
            with self.assertRaisesRegex(ValueError, "provenance"):
                historical.run(reader, self.core, batch_id="b", as_of=self.dates[22], through=self.dates[-1])

    def test_missing_local_data(self):
        with self.assertRaisesRegex(ValueError, "database absent"):
            with historical.connect_readonly(Path(self.folder.name) / "absent.sqlite3"):
                pass


if __name__ == "__main__":
    unittest.main()
