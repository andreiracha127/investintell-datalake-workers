"""CLI safety checks independent of a database or provider."""

import pytest
from io import StringIO
import json
import csv
import datetime as dt
from tools.nport_secapi import convert

from tools.nport_dera import nport_parallel_load as loader


@pytest.mark.parametrize('floor', ['nan', 'inf', '-0.01', '1.01'])
def test_loader_rejects_invalid_verification_floor(tmp_path, floor):
    with pytest.raises(SystemExit):
        loader.main(['--seed-dir', str(tmp_path), '--dry-run', '--verify-floor', floor])


@pytest.mark.parametrize('workers', ['0', '-1'])
def test_loader_rejects_invalid_worker_count(tmp_path, workers):
    with pytest.raises(SystemExit):
        loader.main(['--seed-dir', str(tmp_path), '--dry-run', '--workers', workers])


@pytest.mark.parametrize('stream', ['a,b\r\nc,d\n', 'a,b\nc,d\r\n', 'a,b\rc,d\n'])
def test_copy_parser_refuses_mixed_record_delimiters(stream):
    records = list(loader.copy_csv_records(StringIO(stream, newline='')))
    assert any(row is None for _, row in records)


def test_converter_chooses_latest_filing_by_actual_instant(tmp_path):
    source = tmp_path / '2026-07.jsonl'
    base = {'submissionType': 'NPORT-P', 'genInfo': {
        'repPdDate': '2026-05-31', 'regCik': '123', 'seriesId': 'S1'},
        'invstOrSecs': [{'cusip': '123456789', 'valUSD': 100, 'pctVal': 100}]}
    filings = [base | {'accessionNo': 'LATER', 'filedAt': '2026-07-01T10:00:00-04:00'},
               base | {'accessionNo': 'EARLIER', 'filedAt': '2026-07-01T13:00:00Z'}]
    source.write_text('\n'.join(json.dumps(f) for f in filings), encoding='utf-8')
    out = tmp_path / 'seed'
    manifest = convert.convert([str(source)], str(out))
    quality = manifest['report_dates']['2026-05-31']['filing_quality']
    assert quality[0]['accession'] == 'LATER'
    with (out / '2026-05-31.csv').open(newline='', encoding='utf-8') as fh:
        assert len(list(csv.DictReader(fh))) == 1


@pytest.mark.parametrize('failure', ['query', 'connection_exit'])
def test_table_state_closes_conflict_index_when_a_database_read_fails(monkeypatch, failure):
    states = []
    original = loader.TableState
    def state(**kwargs):
        value = original(**kwargs)
        states.append(value)
        return value
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, query, params):
            if failure == 'query' and 'current_date' not in query:
                raise RuntimeError('read failed')
        def fetchone(self):
            return dt.date(2026, 10, 7), True
        def fetchall(self):
            return []
        def __iter__(self):
            return iter([])
    class Connection(Cursor):
        def cursor(self, **kwargs):
            return Cursor()
        def __exit__(self, *args):
            if failure == 'connection_exit':
                raise RuntimeError('read failed')
            return False
    monkeypatch.setattr(loader, 'TableState', state)
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: Connection())
    with pytest.raises(RuntimeError, match='read failed'):
        loader.read_table_state('fake', ['2026-05-31'], rows=True, keys=True, cleanup=False)
    assert not states[0].keys.path.exists()


@pytest.mark.parametrize('month', ['2026-1', '2026-10-01', '2026-13'])
def test_converter_refuses_bad_partial_months_before_overwriting_seed(tmp_path, month):
    out = tmp_path / 'seed'
    out.mkdir()
    sentinel = out / 'keep.csv'
    sentinel.write_text('evidence', encoding='utf-8')
    with pytest.raises(ValueError):
        convert.convert([], str(out), partial_months={month}, overwrite=True)
    assert sentinel.read_text(encoding='utf-8') == 'evidence'


def test_converter_normalizes_compact_date_filters(tmp_path):
    source = tmp_path / '2026-07.jsonl'
    source.write_text(json.dumps({'submissionType': 'NPORT-P', 'accessionNo': 'A1',
        'filedAt': '2026-07-01T00:00:00Z',
        'genInfo': {'repPdDate': '2026-05-31', 'regCik': '123', 'seriesId': 'S1'},
        'invstOrSecs': [{'cusip': '123456789', 'valUSD': 100, 'pctVal': 100}]}) + '\n', encoding='utf-8')
    manifest = convert.convert([str(source)], str(tmp_path / 'seed'), min_report_date='20260501',
                               report_dates={'20260531'})
    assert set(manifest['report_dates']) == {'2026-05-31'}


def test_secapi_mode_requires_its_manifest(tmp_path, capsys):
    with pytest.raises(SystemExit) as error:
        loader.main(['--seed-dir', str(tmp_path), '--dry-run', '--secapi'])
    assert error.value.code == 2
    assert 'requires a sec-api converter manifest' in capsys.readouterr().err
