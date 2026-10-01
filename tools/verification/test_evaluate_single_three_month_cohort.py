"""Tests for the one-time 90-day holdout comparison."""

import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("evaluate_single_three_month_cohort.py")
SPEC = importlib.util.spec_from_file_location("evaluate_single_three_month_cohort", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SingleCohortTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.database = Path(folder.name) / "bars.sqlite3"
        connection = sqlite3.connect(self.database)
        self.addCleanup(connection.close)
        connection.executescript("""
            CREATE TABLE datasets(dataset_id TEXT PRIMARY KEY,dataset_key TEXT);
            CREATE TABLE batches(batch_id TEXT PRIMARY KEY,dataset_id TEXT,status TEXT,quality TEXT,source TEXT,basis TEXT,published_at TEXT);
            CREATE TABLE day_partitions(partition_id TEXT PRIMARY KEY,row_count INTEGER,validation_status TEXT,source TEXT,basis TEXT);
            CREATE TABLE batch_days(batch_id TEXT,trade_date TEXT,partition_id TEXT,row_count INTEGER);
            CREATE TABLE partition_bars(partition_id TEXT,trade_date TEXT,code TEXT,name TEXT,open REAL,close REAL,source TEXT,basis TEXT);
            INSERT INTO datasets VALUES('d','tushare_daily');
            INSERT INTO batches VALUES('b','d','published','good','tushare','unadjusted','2026-09-24');
        """)
        self.dates = ["2026-06-26", "2026-06-29", "2026-09-24"]
        for index, day in enumerate(self.dates):
            partition = f"p{index}"
            connection.execute("INSERT INTO day_partitions VALUES(?,2,'validated','tushare','unadjusted')", (partition,))
            connection.execute("INSERT INTO batch_days VALUES('b',?,?,2)", (day, partition))
            for code, closes in (("000001", (10, 11, 12)), ("000002", (10, 9, 8))):
                connection.execute("INSERT INTO partition_bars VALUES(?,?,?,?,?,?,?,?)",
                                   (partition, day, code, code, closes[index], closes[index], "tushare", "unadjusted"))
        connection.commit()
        self.connection = connection
        self.packet = {
            "schema_version": "historical_reasoning_input.v1", "date": self.dates[0],
            "candidates": [
                {"code": code, "technical_score": 35, "technical_risk": "unknown",
                 "reasoning_status": "not_run", "as_of_unadjusted_close": 10}
                for code in ("000001", "000002")
            ],
        }

    def evaluate(self):
        with MODULE.connect_readonly(self.database) as reader:
            return MODULE.evaluate(reader, self.packet, batch_id="b", min_market_rows=2)

    def test_one_frozen_selection_exact_90_day_endpoint(self):
        result = self.evaluate()
        self.assertEqual((result["as_of"], result["target"], result["trading_sessions_in_window"]),
                         ("2026-06-26", "2026-09-24", 3))
        self.assertEqual([item["code"] for item in result["candidates"]], ["000001", "000002"])
        self.assertEqual([item["raw_90d_close_change_pct"] for item in result["candidates"]], [20, -20])
        self.assertEqual(result["summary"]["positive"], 1)
        self.assertEqual(result["summary"]["observed_positive_pct"], 50)
        self.assertNotIn("required_wins_for_85pct", result["summary"])
        self.assertNotIn("goal_85pct_technical_price_direction_met", result["summary"])
        self.assertFalse(result["selection_was_repeated"])

    def test_missing_endpoint_is_unknown_not_a_loss(self):
        self.connection.execute("DELETE FROM partition_bars WHERE partition_id='p2' AND code='000002'")
        self.connection.commit()
        result = self.evaluate()
        self.assertEqual(result["summary"]["unknown"], 1)
        self.assertEqual(result["summary"]["observed"], 1)
        self.assertEqual(result["summary"]["strict_positive_pct_of_all_selected"], 50)
        self.assertEqual(result["candidates"][1]["outcome"], "unknown_endpoint_bar")

    def test_future_mutation_changes_only_outcome(self):
        original = self.evaluate()
        self.connection.execute("UPDATE partition_bars SET close=30 WHERE partition_id='p2' AND code='000002'")
        self.connection.commit()
        updated = self.evaluate()
        self.assertEqual([item["code"] for item in original["candidates"]],
                         [item["code"] for item in updated["candidates"]])
        self.assertNotEqual(original["summary"]["positive"], updated["summary"]["positive"])

    def test_frozen_price_and_partition_provenance_required(self):
        self.packet["candidates"][0]["as_of_unadjusted_close"] = 11
        with self.assertRaisesRegex(ValueError, "frozen selection price changed"):
            self.evaluate()
        self.packet["candidates"][0]["as_of_unadjusted_close"] = 10
        self.connection.execute("UPDATE day_partitions SET validation_status='rejected' WHERE partition_id='p2'")
        self.connection.commit()
        with self.assertRaisesRegex(ValueError, "unverified market-day provenance"):
            self.evaluate()


if __name__ == "__main__":
    unittest.main()
