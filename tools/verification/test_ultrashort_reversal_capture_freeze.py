import gzip
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ultrashort_reversal_capture as capture
import ultrashort_reversal_freeze as freeze


def test_scope_denominator_keeps_no_quote_and_st():
    payload = {'fields': ['code', 'type', 'status', 'ipoDate', 'outDate'], 'rows': [
        ['sh.600000', '1', '1', '1999-01-01', ''],
        ['sz.300001', '1', '1', '2009-01-01', ''],
        ['sh.688001', '1', '1', '2019-01-01', ''],
        ['sh.600001', '1', '0', '1999-01-01', '2020-01-01']]}
    assert capture.universe(payload, '2026-09-30') == ['sh.600000', 'sz.300001']


def test_duplicate_identity_rejected():
    payload = {'fields': ['code', 'type', 'status', 'ipoDate', 'outDate'],
               'rows': [['sh.600000', '1', '1', '1999-01-01', '']] * 2}
    with pytest.raises(ValueError, match='duplicate'):
        capture.universe(payload, '2026-09-30')


def test_opening_deadline_cannot_freeze_late(tmp_path):
    with pytest.raises(ValueError, match='deadline'):
        freeze.freeze(tmp_path, tmp_path / 'report.json', tmp_path / 'frozen.json',
                      datetime(2026, 10, 8, 9, tzinfo=freeze.CHINA))


def test_archive_window_stops_before_transport(tmp_path, monkeypatch):
    class FixedTime(datetime):
        @classmethod
        def now(cls, timezone=None):
            return cls(2026, 10, 5, 18, tzinfo=timezone)
    monkeypatch.setattr(capture, 'datetime', FixedTime)
    with pytest.raises(RuntimeError, match='legacy_capture_window'):
        capture.capture(tmp_path)
    assert not (tmp_path / 'ATTEMPT_STARTED.json').exists()
    with pytest.raises(ValueError, match='budget'):
        capture.capture(tmp_path, max_calls=15001, operator_start_now=True)
    assert not (tmp_path / 'ATTEMPT_STARTED.json').exists()


def test_immutable_raw_hash(tmp_path):
    path = tmp_path / 'row.json.gz'
    first = capture.save(path, {'empty_amount': '', 'tradestatus': '0'})
    assert first == freeze.digest(path)
    assert json.loads(gzip.decompress(path.read_bytes()))['empty_amount'] == ''
    with pytest.raises(FileExistsError):
        capture.save(path, {})


def test_resultset_late_error_is_partial():
    class Result:
        error_code = '0'
        fields = ['code']
        def next(self):
            self.error_code = 'bad'
            return False
    assert capture.drain(Result())['status'] == 'partial'


def test_current_freeze_uses_true_asof_keeps_st_in_denominator(tmp_path):
    fields = ['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'adjustflag', 'tradestatus', 'isST']
    codes = ['sh.600000', 'sz.300001']
    dates = [f'2026-09-{number:02d}' for number in range(1, 31)]
    basic = {'status': 'observed', 'fields': ['code', 'code_name', 'type', 'status', 'ipoDate', 'outDate'],
             'rows': [[code, 'sample', '1', '1', '1999-01-01', '2026-11-01'] for code in codes]}
    capture.save(tmp_path / 'basic.json', basic)
    capture.save(tmp_path / 'calendar.json', {'status': 'observed', 'rows': [[date, '1'] for date in dates]})
    manifest = []
    for offset, code in enumerate(codes):
        path = tmp_path / (code + '.json.gz')
        rows = [[date, code, '10', '10.5', '9.5', '10', '10', '100000', '200000000', '3', '1', str(offset)] for date in dates]
        checksum = capture.save(path, {'requested_code': code, 'adjustflag': '3', 'provider_code': '0', 'fields': fields, 'rows': rows})
        manifest.append({'code': code, 'file': path.name, 'sha256': checksum})
    capture.save(tmp_path / 'capture_report.json', {'target': '2026-09-30', 'requested_codes': codes,
                 'raw_manifest': manifest, 'status': 'complete', 'started': 'start', 'finished': 'finish', 'run_messages': 6})
    capture.save(tmp_path / 'source_manifest.json', {})
    capture.save(tmp_path / 'report.json', {'registration_commit': freeze.REGISTRATION, 'rule': freeze.RULE,
                 'kind': 'test', 'selected': 'A', 'selection_reason': 'observation', 'label': '未通过检验，仅观察'})
    result = freeze.freeze(tmp_path, tmp_path / 'report.json', tmp_path / 'frozen.json',
                           datetime(2026, 10, 5, 22, tzinfo=freeze.CHINA))
    assert result['identity_denominator'] == 2
    assert result['ST_count_retained'] == 1
    assert [item['code'] for item in result['list']] == ['sh.600000']
    assert result['empty_slots'] == 4
    assert result['input_as_of'] == '2026-09-30'
    assert result['label'] == '未通过检验，仅观察'
