"""Checks for retrospective inference input isolation."""

import importlib.util
import json
from pathlib import Path
import unittest


SCRIPT = Path(__file__).with_name("freeze_historical_inputs.py")
SPEC = importlib.util.spec_from_file_location("freeze_historical_inputs", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FreezeHistoricalInputsTests(unittest.TestCase):
    def setUp(self):
        self.report = {
            "mode": "retrospective_only_not_point_in_time",
            "strategy": "core.apply_daily_indicators + core.score_quote, technical only; top amount deep screen",
            "as_of": "2026-06-26", "through": "2026-06-29",
            "days": [{"date": "2026-06-26", "status": "selected", "history_coverage": 1.0,
                      "candidates": [{"code": "600367", "score": 35, "risk": "unknown",
                                      "as_of_close": 58.28, "history_days": 120,
                                      "target_date": "2026-07-03", "target_close": 1.0,
                                      "outcome": "observed_unadjusted_close", "raw_return_pct": -98}]},
                     {"date": "2026-06-29", "status": "selected", "history_coverage": 1.0,
                      "candidates": [{"code": "600367", "score": 30, "risk": "unknown",
                                      "as_of_close": 57.0, "history_days": 120,
                                      "target_close": 100.0}]}],
        }
        self.index = {"scope": {"as_of": "2026-06-26", "sample_kind": "official_index_only"},
                      "codes": [{"code": "600367", "status": "complete_reconciled",
                                 "unique_announcement_ids": 3,
                                 "records": [{"announcement_id": "1", "published_at": "2026-06-25",
                                              "source": "https://static.cninfo.com.cn/1.PDF"},
                                             {"announcement_id": "1", "published_at": "2026-06-25",
                                              "source": "https://static.cninfo.com.cn/1.PDF"},
                                             {"announcement_id": "2", "published_at": "2026-06-26",
                                              "source": "https://static.cninfo.com.cn/2.PDF"}]}]}

    def test_no_future_outcomes_and_conservative_index(self):
        packets = MODULE.freeze(self.report, self.index)
        encoded = MODULE.encode(packets)
        for forbidden in (b"target_close", b"target_date", b"raw_return_pct", b"outcome"):
            self.assertNotIn(forbidden, encoded)
        first = packets[0]["candidates"][0]["announcement_evidence"]
        self.assertEqual(first["status"], "retrospective_index_rows_partial_not_as_of_verified")
        self.assertEqual([item["announcement_id"] for item in first["records"]], ["1"])
        self.assertEqual(packets[1]["candidates"][0]["announcement_evidence"]["status"], "not_collected")

    def test_conflicting_duplicate_is_rejected(self):
        self.index["codes"][0]["records"][1]["source"] = "https://static.cninfo.com.cn/other.PDF"
        with self.assertRaisesRegex(ValueError, "conflicting announcement"):
            MODULE.freeze(self.report, self.index)

    def test_scope_and_duplicate_dates_rejected(self):
        self.index["scope"]["as_of"] = "2026-06-25"
        with self.assertRaisesRegex(ValueError, "scope mismatch"):
            MODULE.freeze(self.report, self.index)
        self.index["scope"]["as_of"] = "2026-06-26"
        self.report["days"].append(self.report["days"][1])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            MODULE.freeze(self.report, self.index)


if __name__ == "__main__":
    unittest.main()
