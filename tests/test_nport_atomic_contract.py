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


def _secapi_seed(tmp_path, series=('S1',), rows=1):
    source = tmp_path / '2026-07.jsonl'
    source.write_text('\n'.join(json.dumps({
        'submissionType': 'NPORT-P', 'accessionNo': f'A-{sid}',
        'filedAt': '2026-07-01T00:00:00Z',
        'genInfo': {'repPdDate': '2026-05-31', 'regCik': '123', 'seriesId': sid},
        'invstOrSecs': [{'cusip': f'KEY-{sid}-{i}', 'valUSD': 100, 'pctVal': 100 / rows,
                         'identifiers': {'isin': {'value': 'US1234567890'}}, 'curCd': 'USD'}
                        for i in range(rows)],
    }) for sid in series), encoding='utf-8')
    directory = tmp_path / 'seed'
    manifest = convert.convert([str(source)], str(directory))
    return directory, manifest


@pytest.mark.parametrize('explicit', [False, True])
@pytest.mark.parametrize('dry', [False, True])
def test_secapi_plain_mode_is_refused_before_database_work(tmp_path, monkeypatch, capsys, explicit, dry):
    directory, _ = _secapi_seed(tmp_path)
    def connect(*args, **kwargs):
        pytest.fail('plain sec-api mode reached the database')
    monkeypatch.setattr(loader.psycopg, 'connect', connect)
    argv = ['--seed-dir', str(directory), '--dsn', 'unused', '--only-report-dates', '2026-05-31']
    if explicit:
        argv.append('--secapi')
    if dry:
        argv.append('--dry-run')
    with pytest.raises(SystemExit) as error:
        loader.main(argv)
    assert error.value.code == 2
    message = capsys.readouterr().err
    assert '--new-series-only' in message and '--delete-first' in message


class _Transaction:
    """A transaction with trigger-adjusted RETURNING aggregates and a healthy old date."""

    def __init__(self, aggregates):
        self.aggregates = aggregates
        self.committed = self.rolled_back = False
        self.query = ''
        self.rowcount = sum(row[2] for row in aggregates)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *args):
        self.rolled_back = exc_type is not None

    def cursor(self):
        from contextlib import nullcontext
        return nullcontext(self)

    def copy(self, query):
        from contextlib import nullcontext
        class Copy(StringIO):
            def write_row(self, row):
                csv.writer(self).writerow(row)
        return nullcontext(Copy())

    def execute(self, query, params=None):
        self.query = query

    def fetchall(self):
        if '_nport_inserted' in self.query:
            return self.aggregates
        if 'GROUP BY 1 ORDER BY 1' in self.query:
            n = sum(row[2] for row in self.aggregates)
            filled = sum(row[3] for row in self.aggregates)
            return [('2026-05-31', 10000 + n, 9900 + filled)]
        return [('2026-05-31', 'EXISTING')]

    def commit(self):
        self.committed = True

    def fetchone(self):
        return (False,)


@pytest.mark.parametrize('returned', [(), ('S1',)])
def test_selected_series_without_returning_rows_rolls_back(tmp_path, monkeypatch, returned):
    directory, manifest = _secapi_seed(tmp_path, ('S1', 'S2'))
    paths = [str(directory / '2026-05-31.csv')]
    state = loader.TableState(today=dt.date(2026, 10, 7), keys=set(),
                              series={'2026-05-31': {'EXISTING'}},
                              counts={'2026-05-31': [10000, 9900]})
    monkeypatch.setattr(loader, 'read_table_state', lambda *args, **kwargs: state)
    assert loader.dry_run(paths, ['2026-05-31'], .9, dsn='fake', new_series_only=True,
                          skip_matview=True, quality_manifest=manifest) == 0
    tx = _Transaction([('2026-05-31', sid, 1, 1, 100, 0, 0, 100, 1) for sid in returned])
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: tx)
    with pytest.raises(loader.VerificationError, match='inserted.*plan.*S2'):
        loader.load_batch('fake', paths, dt.datetime.now(dt.UTC), ['2026-05-31'],
                          new_series_only=True, quality_manifest=manifest)
    assert tx.rolled_back and not tx.committed


def test_new_cohort_isin_failure_rolls_back_despite_healthy_existing_date(tmp_path, monkeypatch):
    directory, manifest = _secapi_seed(tmp_path)
    # A BEFORE INSERT trigger removed the new row's ISIN after a clean preflight.
    tx = _Transaction([('2026-05-31', 'S1', 1, 0, 100, 0, 0, 100, 1)])
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: tx)
    with pytest.raises(loader.VerificationError, match='isin_fill'):
        loader.load_batch('fake', [str(directory / '2026-05-31.csv')], dt.datetime.now(dt.UTC),
                          ['2026-05-31'], new_series_only=True, quality_manifest=manifest)
    assert tx.rolled_back and not tx.committed


@pytest.mark.parametrize('phase', ['plan', 'insert'])
def test_direct_secapi_api_refuses_plain_mode_before_database_work(tmp_path, monkeypatch, phase):
    directory, manifest = _secapi_seed(tmp_path)
    def connect(*args, **kwargs):
        pytest.fail('plain sec-api API call reached the database')
    monkeypatch.setattr(loader.psycopg, 'connect', connect)
    paths = [str(directory / '2026-05-31.csv')]
    with pytest.raises(ValueError, match='--new-series-only or --delete-first'):
        if phase == 'plan':
            loader.dry_run(paths, ['2026-05-31'], .9, dsn='fake', quality_manifest=manifest)
        else:
            loader.load_batch('fake', paths, dt.datetime.now(dt.UTC), ['2026-05-31'],
                              quality_manifest=manifest)


def test_clean_secapi_replacement_preflight_passes(tmp_path):
    directory, _ = _secapi_seed(tmp_path)
    assert loader.main(['--seed-dir', str(directory), '--only-report-dates', '2026-05-31',
                        '--dry-run', '--delete-first', '--skip-matview', '--secapi']) == 0


def test_series_loaded_during_lock_wait_is_a_valid_whole_series_skip(tmp_path, monkeypatch):
    directory, manifest = _secapi_seed(tmp_path)
    tx = _Transaction([])
    original = tx.fetchall
    tx.fetchall = lambda: [('2026-05-31', 'S1')] if 'SELECT DISTINCT' in tx.query else original()
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: tx)
    assert loader.load_batch('fake', [str(directory / '2026-05-31.csv')], dt.datetime.now(dt.UTC),
                             ['2026-05-31'], new_series_only=True, quality_manifest=manifest,
                             expected_rows={'2026-05-31': {'S1': 1}})[1] == 0
    assert tx.committed and not tx.rolled_back


def test_partial_returning_rows_roll_back_the_selected_series(tmp_path, monkeypatch):
    directory, manifest = _secapi_seed(tmp_path, rows=2)
    tx = _Transaction([('2026-05-31', 'S1', 1, 1, 100, 0, 0, 100, 1)])
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: tx)
    with pytest.raises(loader.VerificationError, match="S1', 2, 1"):
        loader.load_batch('fake', [str(directory / '2026-05-31.csv')], dt.datetime.now(dt.UTC),
                          ['2026-05-31'], new_series_only=True, quality_manifest=manifest)
    assert tx.rolled_back and not tx.committed


def test_changed_returning_key_rolls_back_even_when_series_counts_match(tmp_path, monkeypatch):
    directory, manifest = _secapi_seed(tmp_path)
    tx = _Transaction([('2026-05-31', 'S1', 1, 1, 100, 0, 0, 100, 1)])
    tx.fetchone = lambda: (True,)  # planned key minus trigger-rewritten target key is nonempty
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: tx)
    with pytest.raises(loader.VerificationError, match='inserted keys mismatch preflight plan'):
        loader.load_batch('fake', [str(directory / '2026-05-31.csv')], dt.datetime.now(dt.UTC),
                          ['2026-05-31'], new_series_only=True, quality_manifest=manifest)
    assert tx.rolled_back and not tx.committed


def test_preflight_retains_exact_selected_keys_for_the_transaction(tmp_path, monkeypatch):
    directory, manifest = _secapi_seed(tmp_path, ('S1', 'S2'))
    paths = [str(directory / '2026-05-31.csv')]
    state = loader.TableState(today=dt.date(2026, 10, 7), keys=set())
    monkeypatch.setattr(loader, 'read_table_state', lambda *args, **kwargs: state)
    rows = {}
    keys = {'2026-05-31': str(tmp_path / 'planned-keys.csv')}
    assert loader.dry_run(paths, ['2026-05-31'], .9, dsn='fake', new_series_only=True,
                          skip_matview=True, quality_manifest=manifest,
                          expected_rows=rows, expected_keys=keys) == 0
    assert rows == {'2026-05-31': {'S1': 1, 'S2': 1}}
    with open(keys['2026-05-31'], encoding='utf-8', newline='') as fh:
        assert list(csv.reader(fh)) == [
            ['2026-05-31', 'S1', 'KEY-S1-0'], ['2026-05-31', 'S2', 'KEY-S2-0']]
    tx = _Transaction([('2026-05-31', sid, 1, 1, 100, 0, 0, 100, 1) for sid in ('S1', 'S2')])
    monkeypatch.setattr(loader.psycopg, 'connect', lambda *args, **kwargs: tx)
    assert loader.load_batch('fake', paths, dt.datetime.now(dt.UTC), ['2026-05-31'],
                             new_series_only=True, quality_manifest=manifest,
                             expected_rows=rows, expected_keys=keys)[1] == 2
    assert tx.committed and not tx.rolled_back
