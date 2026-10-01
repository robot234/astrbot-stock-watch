"""Test announcement staging boundaries."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("stage_single_cohort_announcements.py")
SPEC = importlib.util.spec_from_file_location("stage_single_cohort_announcements", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class StagingTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.output = Path(folder.name) / "staged"
        self.packet = {"date": "2026-06-26", "candidates": [{"code": "000001"}]}
        self.index = {"scope": {"as_of": "2026-06-26", "sample_kind": "official_index_only"},
                      "codes": [{"code": "000001", "status": "complete_reconciled", "records": [
                          {"published_at": "2026-06-25", "announcement_id": "before",
                           "source": "https://static.cninfo.com.cn/before.PDF"},
                          {"published_at": "2026-06-26", "announcement_id": "cutoff",
                           "source": "https://static.cninfo.com.cn/cutoff.PDF"}]}]}

    def test_same_day_and_future_excluded(self):
        self.index["codes"][0]["records"][0]["published_at"] = "2026-06-27"
        result = MODULE.run(self.packet, self.index, self.output)
        self.assertEqual((result["selected"], result["extracted"]), (0, 0))

    def test_mismatched_cutoff(self):
        self.index["scope"]["as_of"] = "2026-06-27"
        with self.assertRaisesRegex(ValueError, "mismatch"):
            MODULE.run(self.packet, self.index, self.output)

    def test_untrusted_host(self):
        self.index["codes"][0]["records"][0]["source"] = "https://example.com/a.PDF"
        with self.assertRaisesRegex(ValueError, "non-official"):
            MODULE.run(self.packet, self.index, self.output)

    def test_conflicting_duplicate_identifier(self):
        self.index["codes"][0]["records"].append({"published_at": "2026-06-24",
            "announcement_id": "before", "source": "https://static.cninfo.com.cn/other.PDF"})
        with self.assertRaisesRegex(ValueError, "conflicting index"):
            MODULE.run(self.packet, self.index, self.output)
