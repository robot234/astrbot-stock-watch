import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("historical_reselection_archives_20260927.py")
SPEC = importlib.util.spec_from_file_location("historical_reselection_archives_20260927", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class HistoricalArchiveHelpersTest(unittest.TestCase):
    def test_missing_ssh_target_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "SSH target is required"):
                MODULE.validate_ssh_target(None)

    def test_ssh_target_rejects_argument_injection(self):
        for value in ("-oProxyCommand=bad", "pi@host -oProxyCommand=bad", "pi@host;whoami"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    MODULE.validate_ssh_target(value)
        command = MODULE.build_ssh_command("pi@host", "docker", "exec", "astrbot")
        self.assertEqual(command[5], "pi@host")
        self.assertNotIn(" ", command[5])

    def test_raw_partition_digest_is_order_independent(self):
        first = {
            "trade_date": "2026-04-03", "code": "000001", "ts_code": "000001.SZ", "name": "",
            "open": 10, "high": 11, "low": 9, "close": 10.5, "pre_close": 10,
            "pct_change": 5, "volume": 100, "amount": 200, "source": "tushare", "basis": "unadjusted",
        }
        second = dict(first, code="000002", ts_code="000002.SZ", close=20, pre_close=20, pct_change=0)
        self.assertEqual(MODULE.raw_partition_digest([first, second]), MODULE.raw_partition_digest([second, first]))

    def test_raw_payload_keeps_public_provenance(self):
        row = {
            "trade_date": "2026-04-03", "code": "000001", "ts_code": "000001.SZ", "name": None,
            "open": "10", "high": "11", "low": "9", "close": "10.5", "pre_close": "10",
            "pct_change": "5", "volume": "100", "amount": "200", "source": "tushare", "basis": "unadjusted",
        }
        payload = MODULE.raw_payload(row)
        self.assertEqual(payload["name"], "")
        self.assertEqual(payload["basis"], "unadjusted")
        self.assertEqual(payload["source"], "tushare")


if __name__ == "__main__":
    unittest.main()
