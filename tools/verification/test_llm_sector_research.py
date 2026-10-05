from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import llm_sector_research as research
import llm_sector_industry_capture as capture


def news(identity='news1', publisher='publisher1', event='event1'):
    text = 'audited source evidence'
    return {'id': identity, 'url': 'https://example.org/news', 'publisher': publisher, 'event_id': event,
            'published_at': '2026-09-30T16:00:00+08:00', 'received_at': '2026-10-05T21:00:00+08:00',
            'publication_precision': 'minute', 'body_verified': True, 'expired': 'unknown',
            'evidence_text': text, 'evidence_sha256': hashlib.sha256(text.encode()).hexdigest(),
            'sector_ids': ['sector1']}


def test_news_rejects_future_missing_body_and_out_of_window():
    future, no_body, old = news('future'), news('no_body'), news('old')
    future['published_at'] = '2026-10-08T10:00:00+08:00'
    no_body['body_verified'] = False
    old['published_at'] = '2026-09-20T10:00:00+08:00'
    accepted, rejected = research.news_validation([future, no_body, old, news()])
    assert list(accepted) == ['news1']
    assert len(rejected) == 3


def test_news_hash_mismatch_duplicate_and_timezone_missing_fail_closed():
    bad = news()
    bad['evidence_text'] = 'changed'
    with pytest.raises(ValueError, match='hash_mismatch'):
        research.news_validation([bad])
    with pytest.raises(ValueError, match='duplicate'):
        research.news_validation([news(), news()])
    bad = news()
    bad['received_at'] = '2026-10-05T21:00:00'
    assert not research.news_validation([bad])[0]


def test_date_only_same_day_not_assumed_midnight():
    item = news()
    item.update(published_at='2026-10-05', publication_precision='date')
    assert not research.news_validation([item])[0]
    item['published_at'] = '2026-10-04'
    assert research.news_validation([item])[0]


def decision():
    return {'mode': research.MODE, 'input_sha256': 'hash', 'generated_at': '2026-10-05T21:05:00+08:00',
            'sectors': [{'sector_id': 'sector1', 'news_ids': ['news1', 'news2'], 'reason': 'two real events'}]}


def bundle():
    return {'generated_at': '2026-10-05T21:01:00+08:00', 'sector_ids': ['sector1']}


def test_same_owner_or_same_event_not_two_independent_references():
    records = {'news1': news(), 'news2': news('news2', event='event2')}
    assert research.llm_validation(decision(), bundle(), records, 'hash')[0] == []
    records['news2'] = news('news2', publisher='publisher2')
    assert research.llm_validation(decision(), bundle(), records, 'hash')[0] == []
    records['news2'] = news('news2', 'publisher2', 'event2')
    assert research.llm_validation(decision(), bundle(), records, 'hash')[0] == ['sector1']


def test_hallucinated_sector_citation_or_wrong_input_is_failure():
    output = decision()
    with pytest.raises(ValueError, match='input_identity'):
        research.llm_validation(output, bundle(), {}, 'wrong')
    output['sectors'][0]['sector_id'] = 'invented'
    with pytest.raises(ValueError, match='unknown_or_duplicate_sector'):
        research.llm_validation(output, bundle(), {}, 'hash')
    with pytest.raises(ValueError, match='citation'):
        research.llm_validation(decision(), bundle(), {'news1': news()}, 'hash')


def test_citation_topic_mismatch_does_not_pass():
    unrelated = news('news2', 'publisher2', 'event2')
    unrelated['sector_ids'] = ['sector2']
    assert not research.llm_validation(decision(), bundle(), {'news1': news(), 'news2': unrelated}, 'hash')[0]


def classification():
    return {'provider_code': '0', 'requested_date': research.TARGET, 'received_at': '2026-10-05T21:00:00+08:00',
            'fields': ['code', 'industry', 'industryClassification', 'updateDate'],
            'rows': [['sh.600000', 'sector1', '证监会行业分类', '2026-09-28']]}


def test_classification_no_quote_filter_future_stale_and_duplicate():
    payload = classification()
    labels, unknown = research.mapping(payload, {'sh.600000', 'sh.600001'}, research.TARGET)
    assert labels == {'sh.600000': 'sector1'} and unknown == {'sh.600001': 'missing_classification'}
    for invalid in ('2026-10-01', '2026-08-01', ''):
        payload['rows'][0][-1] = invalid
        assert not research.mapping(payload, {'sh.600000'}, research.TARGET)[0]
    payload = classification()
    payload['rows'] *= 2
    with pytest.raises(ValueError, match='duplicate'):
        research.mapping(payload, {'sh.600000'}, research.TARGET)


def test_sector_denominator_keeps_missing_and_real_zero_is_valid():
    count = 10
    market = {'codes': np.array([f'sh.{600000 + index}' for index in range(count)]),
              'dates': np.array([research.TARGET] * 25), 'amount': np.full((25, count), 100.0)}
    labels = dict.fromkeys(market['codes'], 'sector1')
    derived = {'score': np.full((25, count), 0.05)}
    market['amount'][0, 0] = 0
    metrics = research.sector_metrics(market, derived, labels)
    assert metrics[0]['known_members'] == 10
    market['amount'][0, 0] = np.nan
    metrics = research.sector_metrics(market, derived, labels)
    assert metrics[0]['coverage'] == 0.9 and metrics[0]['status'] == 'known'
    market['amount'][0, 1] = np.nan
    metrics = research.sector_metrics(market, derived, labels)
    assert metrics[0]['members'] == 10 and metrics[0]['status'] == 'unknown'
    assert metrics[0]['heat_score'] is None


def test_cutoff_stops_without_touching_files_and_output_exclusive(tmp_path):
    late = datetime(2026, 10, 8, 10, tzinfo=research.CHINA)
    with pytest.raises(ValueError, match='cutoff_passed'):
        research.prepare('missing', 'missing', 'missing', tmp_path, late)
    with pytest.raises(ValueError, match='cutoff_passed'):
        research.execute('missing', 'missing', tmp_path, 'missing', late)
    output = tmp_path / 'snapshot.json'
    research.write_json(output, {'frozen': True})
    with pytest.raises(FileExistsError):
        research.write_json(output, {'frozen': False})
    assert json.loads(output.read_text()) == {'frozen': True}


def test_snapshot_mismatch_or_traversal_rejected_without_secret_read(tmp_path):
    with pytest.raises(ValueError, match='snapshot_path'):
        research.verify_news_snapshots([{'snapshot': '../credentials.json'}], tmp_path)
    snapshot = tmp_path / 'open_1.json'
    snapshot.write_text(json.dumps({'sealed_at': '2026-10-05 12:00:00 UTC'}), encoding='utf-8')
    item = news()
    item.update(snapshot='open_1.json', snapshot_sha256=research.digest(snapshot))
    research.verify_news_snapshots([item], tmp_path)
    item['snapshot_sha256'] = 'wrong'
    with pytest.raises(ValueError, match='snapshot_hash'):
        research.verify_news_snapshots([item], tmp_path)


def test_capture_rejects_insufficient_cumulative_budget_without_import_or_send(tmp_path):
    class Evening(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 5, 21, tzinfo=tz)
    previous = capture.datetime
    capture.datetime = Evening
    try:
        with pytest.raises(ValueError, match='cumulative_research_budget'):
            capture.capture(tmp_path, research.TARGET, 14901)
        assert not (tmp_path / 'ATTEMPT_STARTED.json').exists()
    finally:
        capture.datetime = previous


def prepared_run(tmp_path, monkeypatch, missing_labels=0):
    count = 10
    dates = np.array(pd_dates())
    codes = np.array([f'sh.{600000 + index}' for index in range(count)])
    shape = (len(dates), count)
    market = {key: np.full(shape, 10.0) for key in ('open', 'close', 'preclose')}
    market.update(high=np.full(shape, 10.5), low=np.full(shape, 9.5), volume=np.full(shape, 1000.0),
                  amount=np.full(shape, 2e8), tradestatus=np.ones(shape), isST=np.zeros(shape),
                  ordinary=np.ones(shape, dtype=bool), expected=np.ones(shape, dtype=bool),
                  age=np.full(shape, 1000), dates=dates, codes=codes,
                  identities=[{'code': code, 'code_name': code} for code in codes])
    report = {'finished': '2026-10-05T20:00:00+08:00', 'run_messages': 5}
    monkeypatch.setattr(research, 'read_capture', lambda path: (market, report))
    source = tmp_path / 'capture'
    source.mkdir()
    research.write_json(source / 'capture_report.json', report)
    payload = classification()
    payload['rows'] = [[code, 'sector1', '证监会行业分类', '2026-09-28']
                       for code in codes[:count - missing_labels]]
    industry = tmp_path / 'industry.json'
    research.write_json(industry, payload)
    snapshot = tmp_path / 'open_1.json'
    research.write_json(snapshot, {'sealed_at': '2026-10-05 12:00:00 UTC'})
    articles = [news(), news('news2', 'publisher2', 'event2')]
    for article in articles:
        article.update(snapshot='open_1.json', snapshot_sha256=research.digest(snapshot))
    news_path = tmp_path / 'news.json'
    research.write_json(news_path, {'items': articles})
    output = tmp_path / 'output'
    current = datetime(2026, 10, 5, 21, 1, tzinfo=research.CHINA)
    research.prepare(source, industry, news_path, output, current)
    model_output = decision()
    model_output['input_sha256'] = research.digest(output / 'llm_input.json')
    decision_path = tmp_path / 'decision.json'
    research.write_json(decision_path, model_output)
    return source, industry, output, decision_path, market


def pd_dates():
    import pandas as pd
    return pd.bdate_range(end=research.TARGET, periods=25).strftime('%Y-%m-%d').tolist()


def test_end_to_end_selection_and_second_write_blocked(tmp_path, monkeypatch):
    source, industry, output, decision_path, market = prepared_run(tmp_path, monkeypatch)
    now = datetime(2026, 10, 5, 21, 6, tzinfo=research.CHINA)
    result = research.execute(source, industry, output, decision_path, now)
    assert result['status'] == 'flow_complete'
    assert [row['code'] for row in result['list']] == market['codes'][:5].tolist()
    assert result['baseline_overlap'] == 5
    assert result['future_outcomes_computed'] is False
    with pytest.raises(FileExistsError):
        research.execute(source, industry, output, decision_path, now)


def test_global_mapping_failure_keeps_unknown_denominator_and_empty_list(tmp_path, monkeypatch):
    source, industry, output, decision_path, _ = prepared_run(tmp_path, monkeypatch, missing_labels=2)
    result = research.execute(source, industry, output, decision_path,
                              datetime(2026, 10, 5, 21, 6, tzinfo=research.CHINA))
    assert result['status'] == 'incomplete' and result['industry_denominator'] == 10
    assert len(result['industry_unknown']) == 2 and result['list'] == []
    assert 'industry_mapping_below_90_percent' in result['failure_reasons']


def test_future_model_output_and_changed_industry_fail_before_result(tmp_path, monkeypatch):
    source, industry, output, decision_path, _ = prepared_run(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='LLM_decision_from_future'):
        research.execute(source, industry, output, decision_path,
                         datetime(2026, 10, 5, 21, 4, tzinfo=research.CHINA))
    assert not (output / 'result.json').exists()
    with industry.open('ab') as stream:
        stream.write(b' ')
    with pytest.raises(ValueError, match='industry_input_changed'):
        research.execute(source, industry, output, decision_path,
                         datetime(2026, 10, 5, 21, 6, tzinfo=research.CHINA))
    assert not (output / 'result.json').exists()
