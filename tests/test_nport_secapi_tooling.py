"""sec-api monthly N-PORT -> ``sec_nport_holdings`` CSVs, pinned on a hand-built fixture.

``tests/fixtures/nport_secapi/form-nport/2026`` is two monthly containers, eleven
filings, built so each rule the converter applies has exactly one filing that
breaks if the rule does:

* every branch of the conflict key (real / ``IS:`` / ``LE:`` / last resort), and
  the two placeholder identifiers that decide the ``dera`` vs ``strict`` policies;
* the three ``*Conditional`` shapes sec-api uses for asset, issuer and currency;
* an NPORT-P/A in a later container that restates a series wholesale, and an
  empty NPORT-P/A that must NOT erase the filing it amends;
* a series that exists only under ``filerInfo``, a fund with no series at all,
  a late filing for an old month, a duplicated accession and a holdings-less form.

What the real data showed, and these tests hold the converter to: run over the
2026-04/05/06 containers it reproduces production's per-series ``n_holdings``,
market value, ``coverage_pct`` and ``n_synthetic`` for every series it shares
with the 2026-08-06 load.
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import json
import shutil
import traceback
from contextlib import contextmanager
from pathlib import Path

import pytest

from src.workers import nport_secapi_monthly as worker
from tools.nport_dera import nport_parallel_load as loader
from tools.nport_dera.nport_bulk_parse import CSV_COLS
from tools.nport_secapi import convert, download, validate

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "nport_secapi" / "form-nport" / "2026"
CONTAINERS = [str(FIXTURE / "2026-07.jsonl"), str(FIXTURE / "2026-08.jsonl")]
F1 = "0000000001-26-000001"


def _rows(path: Path) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture(scope="module")
def converted(tmp_path_factory) -> tuple[Path, dict]:
    out = tmp_path_factory.mktemp("seed")
    manifest = convert.convert(
        CONTAINERS, str(out), min_report_date="2026-05-01", partial_months={"2026-08"},
    )
    return out, manifest


def _series(out: Path, report_date: str, series_id: str) -> list[dict]:
    return [r for r in _rows(out / f"{report_date}.csv") if r["series_id"] == series_id]


def test_one_loader_csv_per_report_date(converted):
    out, manifest = converted
    assert sorted(p.name for p in out.iterdir()) == ["2026-05-31.csv", "2026-06-30.csv", "manifest.json"]
    for name in ("2026-05-31.csv", "2026-06-30.csv"):
        with open(out / name, encoding="utf-8", newline="") as fh:
            assert next(csv.reader(fh)) == CSV_COLS == loader.CSV_COLS
    assert manifest["report_dates"]["2026-05-31"]["rows"] == 10
    assert manifest["report_dates"]["2026-05-31"]["series"] == 3
    assert manifest["report_dates"]["2026-06-30"]["rows"] == 4
    assert manifest["key_policy"] == "dera"


def test_field_mapping_matches_the_loader_conventions(converted):
    out, _ = converted
    rows = {r["issuer_name"]: r for r in _series(out, "2026-05-31", "S000000001")}
    alpha = rows["ALPHA CORP"]
    assert [alpha[c] for c in CSV_COLS] == [
        "2026-05-31", "0000000101", "123456789", "US1234567890", "ALPHA CORP", "EC", "CORP",
        "1000", "100", "USD", "10.5", "false", "1", "S000000001",
    ]
    euro = rows["EURO NOTE"]
    assert euro["cusip"] == "IS:XS0000000001"
    assert (euro["asset_class"], euro["sector"], euro["currency"]) == ("OTHER", "OTHER", "EUR")
    assert (euro["fair_value_level"], euro["is_restricted"]) == ("", "true")
    # Source digits, positional: no float repr, no exponent, no x100.
    assert (euro["quantity"], euro["pct_of_nav"], euro["market_value"]) == ("29466964.87", "0.00000015", "2500")
    lei = rows["LEI ONLY"]
    assert lei["cusip"] == "LE:5493000000000000LEI1"
    assert (lei["market_value"], lei["pct_of_nav"]) == ("-225409", "-0.0091723714")  # truncated toward zero


def test_dera_policy_folds_placeholder_identifiers_like_production(converted):
    """The rule every existing row was keyed with: LE:N/A and 999999999 are keys."""
    out, manifest = converted
    keys = [r["cusip"] for r in _series(out, "2026-05-31", "S000000001")]
    assert keys == ["123456789", "IS:XS0000000001", "LE:5493000000000000LEI1", "LE:N/A", "999999999"]
    assert manifest["report_dates"]["2026-05-31"]["conflict_key_dupes"] == 3


def test_strict_policy_keeps_every_holding(tmp_path):
    convert.convert(CONTAINERS, str(tmp_path), report_dates={"2026-05-31"}, key_policy=convert.STRICT_POLICY)
    keys = [r["cusip"] for r in _series(tmp_path, "2026-05-31", "S000000001")]
    assert keys == [
        "123456789", "IS:XS0000000001", "LE:5493000000000000LEI1",
        f"H:{F1}:4", f"H:{F1}:5", f"H:{F1}:7", f"H:{F1}:8",
    ]


def test_newest_filing_replaces_the_series_wholesale(converted):
    out, manifest = converted
    rows = _series(out, "2026-05-31", "S000000002")
    assert [r["issuer_name"] for r in rows] == ["FRESH ONE", "FRESH THREE", "FRESH FOUR"]
    assert manifest["report_dates"]["2026-05-31"]["amendments"] == 1
    assert manifest["superseded_filings_by_report_date"]["2026-05-31"] == 1


def test_an_empty_amendment_does_not_erase_the_filing(converted):
    out, _ = converted
    assert len(_series(out, "2026-05-31", "S000000001")) == 5


def test_series_fallbacks(converted):
    out, _ = converted
    assert len(_series(out, "2026-05-31", "S000000003")) == 2  # filerInfo.seriesClassInfo
    cik_rows = _series(out, "2026-06-30", "CIK:0000000104")  # no series anywhere, regCik unpadded
    assert {r["cik"] for r in cik_rows} == {"0000000104"} and len(cik_rows) == 2


def test_old_report_dates_are_counted_not_written(converted):
    _, manifest = converted
    assert manifest["excluded_report_dates"] == {"2026-03-31": {"rows": 1, "filings": 1, "series": 1}}
    assert manifest["skipped_filings"] == {"form:NT NPORT-P": 1, "no_holdings": 2, "duplicate_accession": 1}


def test_partial_flags_follow_the_publication_month(converted):
    _, manifest = converted
    may, june = manifest["report_dates"]["2026-05-31"], manifest["report_dates"]["2026-06-30"]
    assert (may["publication_month"], may["partial"], may["straggler_month_complete"]) == ("2026-07", False, False)
    assert (june["publication_month"], june["partial"]) == ("2026-08", True)


def test_output_is_deterministic_and_reads_gzip(tmp_path):
    gz = tmp_path / "gz"
    gz.mkdir()
    for src in CONTAINERS:
        with open(src, "rb") as fh, gzip.open(gz / (Path(src).name + ".gz"), "wb") as out:
            shutil.copyfileobj(fh, out)
    a, b = tmp_path / "a", tmp_path / "b"
    convert.convert(CONTAINERS, str(a), min_report_date="2026-05-01")
    convert.convert(sorted((str(p) for p in gz.iterdir()), reverse=True), str(b), min_report_date="2026-05-01")
    for name in ("2026-05-31.csv", "2026-06-30.csv"):
        assert (a / name).read_bytes() == (b / name).read_bytes()


def test_a_reused_seed_directory_keeps_no_stale_csv(tmp_path):
    """The loader and validate glob *.csv: a date from an earlier, wider run must not survive."""
    convert.convert(CONTAINERS, str(tmp_path), min_report_date="2026-05-01")
    with pytest.raises(FileExistsError):
        convert.convert(CONTAINERS, str(tmp_path), report_dates={"2026-06-30"})
    convert.convert(CONTAINERS, str(tmp_path), report_dates={"2026-06-30"}, overwrite=True)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-06-30.csv", "manifest.json"]


def test_loader_dry_run_accepts_converter_output(converted, capsys):
    out, _ = converted
    # The fixture is ISIN-poor by design (every key branch); this is about loadability.
    rc = loader.main(["--seed-dir", str(out), "--only", "2026-05-31.csv",
                      "--only-report-dates", "2026-05-31", "--dry-run", "--verify-floor", "0"])
    assert rc == 0
    assert "dry run clean (offline: models no rows" in capsys.readouterr().out


def test_loader_dry_run_catches_what_copy_would_reject(tmp_path):
    bad = tmp_path / "bad.csv"
    good = ["2026-05-31", "0000000101", "123456789", "", "A", "EC", "CORP", "1", "1", "USD", "1", "false", "1", "S1"]
    rows = [
        good,
        good,  # repeated conflict key: dropped by ON CONFLICT
        [*good[:2], "", *good[3:]],  # NULL cusip: fails the whole CSV
        [*good[:7], "1.5", *good[8:]],  # bigint column with a decimal
        ["2026-06-30", *good[1:2], "X", *good[3:]],  # outside --only-report-dates
    ]
    with open(bad, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_COLS)
        writer.writerows(rows)
    result = loader.check_csv(str(bad), ["2026-05-31"], today=dt.date(2026, 10, 6))
    assert result["would_insert"] == 1 and result["conflict_key_dupes"] == 1
    assert result["errors"] == {"not_null:cusip": 1, "type:market_value": 1}
    assert result["dropped"] == {"outside_only_report_dates": 1}

    swapped = tmp_path / "swapped.csv"
    swapped.write_text(",".join([CSV_COLS[1], CSV_COLS[0], *CSV_COLS[2:]]) + "\n", encoding="utf-8")
    assert loader.check_csv(str(swapped))["errors"] == {"header": 1}
    assert loader.dry_run([str(bad)], ["2026-05-31"], 0.9) == 2


def test_loader_dry_run_refuses_a_key_carried_by_two_csvs(tmp_path, capsys):
    row = ["2026-05-31", "0000000101", "123456789", "US1234567890", "A", "EC", "CORP", "1", "1", "USD", "1",
           "false", "1", "S1"]
    files = []
    for name, isin in (("a.csv", "US1234567890"), ("b.csv", "")):  # same key, different content
        path = tmp_path / name
        with open(path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_COLS)
            writer.writerow([*row[:3], isin, *row[4:]])
        files.append(str(path))
    assert loader.dry_run(files, ["2026-05-31"], 0.9) == 2
    assert "1 conflict key(s) repeat across CSVs" in capsys.readouterr().out
    assert loader.dry_run(files[:1], ["2026-05-31"], 0.9) == 0


_ROW = {
    "report_date": "2026-05-31", "cik": "0000000101", "cusip": "123456789", "isin": "US1234567890",
    "issuer_name": "A", "asset_class": "EC", "sector": "CORP", "market_value": "1", "quantity": "1",
    "currency": "USD", "pct_of_nav": "1", "is_restricted": "false", "fair_value_level": "1", "series_id": "S1",
}


def _line(**values: str) -> str:
    """One raw CSV line: values are written verbatim, so '""' is a quoted empty field."""
    return ",".join({**_ROW, **values}[c] for c in CSV_COLS)


def _seed(directory: Path, name: str, lines: list[str]) -> str:
    path = directory / name
    path.write_text(",".join(CSV_COLS) + "\n" + "".join(line + "\n" for line in lines), encoding="utf-8")
    return str(path)


def _table(**state) -> loader.TableState:
    return loader.TableState(**{"today": dt.date(2026, 10, 6), "matview_exists": True, "source": "test", **state})


def test_dry_run_reads_nulls_and_dates_the_way_copy_does(tmp_path):
    path = _seed(tmp_path, "a.csv", [
        _line(cusip="C1", market_value='""'),        # quoted empty in a bigint: COPY rejects the CSV
        _line(cusip="C2", series_id='""'),           # quoted empty series: '' IS NOT NULL, inserted
        _line(cusip="C3", cik='""'),                 # quoted empty cik: satisfies NOT NULL
        _line(cusip=""),                             # unquoted empty cusip: NULL, fails NOT NULL
        _line(cusip="C3", report_date="20260531"),   # the same date to the table: a repeated key
        _line(cusip="C6", issuer_name='"A, ""B"""'),  # quoted delimiter and quote
        _line(cusip="C7", issuer_name="A\x00B"),     # PostgreSQL text cannot hold NUL
        '"2026-05-31,unterminated',
    ])
    result = loader.check_csv(path, ["2026-05-31"], today=dt.date(2026, 10, 6))
    assert result["errors"] == {"type:market_value": 1, "not_null:cusip": 1, "unparseable": 2}
    assert (result["would_insert"], result["conflict_key_dupes"], result["dropped"]) == (3, 1, {})


def test_dry_run_verdict_is_the_post_load_verify(tmp_path):
    thin = _seed(tmp_path, "a.csv", [_line(isin=""), _line(cusip="C2")])  # 2 rows, fill 0.5
    assert loader.dry_run([thin], ["2026-05-31"], 0.9) == 2  # no size exemption: the verify has none
    assert loader.dry_run([thin], ["2026-05-31"], 0.9, verify=False) == 0  # --no-verify / unscoped
    june = _seed(tmp_path, "b.csv", [_line(report_date="2026-06-30")])
    assert loader.dry_run([june], ["2026-06-30"], 0.9) == 0
    assert loader.dry_run([june], ["2026-06-30", "2026-07-31"], 0.9) == 2  # a scope date left empty reads 0


def test_dry_run_models_cleanup_placeholders(tmp_path):
    path = _seed(tmp_path, "a.csv", [_line(), _line(cusip=loader.PLACEHOLDER_CUSIP, isin="")])
    assert loader.dry_run([path], ["2026-05-31"], 0.9) == 2
    assert loader.dry_run([path], ["2026-05-31"], 0.9, cleanup=True) == 0  # deleted before the verify


def test_new_series_only_dry_run_plans_against_the_table(tmp_path, monkeypatch, capsys):
    """A revisit: the table's S1 has no ISIN, the CSV's corrected S1 is skipped by the load."""
    seed = tmp_path / "seed"
    seed.mkdir()
    _seed(seed, "2026-05-31.csv", [_line(cusip=f"C{i}") for i in range(9)] + [_line(series_id="S2")])
    reads = []

    def fake_state(dsn, report_dates, *, rows, keys, cleanup):
        reads.append((dsn, report_dates, rows, keys, cleanup))
        return _table(series={"2026-05-31": {"S1"}}, counts={"2026-05-31": [9, 0]})

    monkeypatch.setattr(loader, "read_table_state", fake_state)
    argv = ["--seed-dir", str(seed), "--only-report-dates", "2026-05-31", "--dsn", "x", "--workers", "1",
            "--skip-matview", "--new-series-only", "--dry-run"]
    assert loader.main(argv) == 2  # after the load the date holds 9 + 1 rows, 1 with an ISIN
    assert reads == [("x", ["2026-05-31"], True, False, False)]
    assert "'series_already_loaded': 9" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        loader.main([a for a in argv if a not in ("--dsn", "x")])  # no table, no plan


def test_plain_dry_run_skips_the_keys_the_table_holds(tmp_path, monkeypatch):
    path = _seed(tmp_path, "a.csv", [_line(cusip="K1"), _line(cusip="K2")])
    state = _table(series={"2026-05-31": {"S1"}}, counts={"2026-05-31": [3, 0]}, keys={("2026-05-31", "S1", "K1")})
    monkeypatch.setattr(loader, "read_table_state", lambda *_, **__: state)
    assert loader.check_csv(path, ["2026-05-31"], plan=loader._Plan(state))["dropped"] == {"key_already_loaded": 1}
    assert loader.dry_run([path], ["2026-05-31"], 0.9, dsn="x", skip_matview=True) == 2  # 3 + 1 rows, 1 ISIN


def test_dry_run_refuses_what_the_parallel_load_would_decide_by_timing(tmp_path, monkeypatch):
    a = _seed(tmp_path, "a.csv", [_line(cusip="C1")])
    b = _seed(tmp_path, "b.csv", [_line(cusip="C2")])  # same series, another CSV
    monkeypatch.setattr(loader, "read_table_state", lambda *_, **__: _table())
    assert loader.dry_run([a, b], ["2026-05-31"], 0.9, dsn="x", new_series_only=True, skip_matview=True) == 2
    assert loader.dry_run([a, b], ["2026-05-31"], 0.9, dsn="x", skip_matview=True) == 0  # distinct keys


def test_dry_run_refuses_a_missing_matview_unless_skipped(tmp_path, monkeypatch):
    path = _seed(tmp_path, "a.csv", [_line()])
    monkeypatch.setattr(loader, "read_table_state", lambda *_, **__: _table(matview_exists=False))
    assert loader.dry_run([path], ["2026-05-31"], 0.9, dsn="x") == 2  # finalize() would fail after the commit
    assert loader.dry_run([path], ["2026-05-31"], 0.9, dsn="x", skip_matview=True) == 0


class _StateCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return dt.date(2026, 10, 6), True

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _StateConn(_StateCursor):
    read_only = False

    def cursor(self, name=None):
        return _StateCursor(self.rows)


def test_table_state_counts_what_the_verify_will_count(monkeypatch):
    rows = [  # report_date, series, placeholder cusip, has ISIN, rows
        (dt.date(2026, 5, 31), "S1", False, True, 5),
        (dt.date(2026, 5, 31), "S1", True, False, 2),
        (dt.date(2026, 5, 31), "S2", True, False, 3),  # only placeholder rows: still a series the table holds
    ]
    monkeypatch.setattr(loader.psycopg, "connect", lambda *a, **k: _StateConn(rows))
    kept = loader.read_table_state("x", ["2026-05-31"], rows=True, keys=False, cleanup=False)
    cleaned = loader.read_table_state("x", ["2026-05-31"], rows=True, keys=False, cleanup=True)
    assert kept.series == cleaned.series == {"2026-05-31": {"S1", "S2"}}
    assert (kept.counts, cleaned.counts) == ({"2026-05-31": [10, 5]}, {"2026-05-31": [5, 5]})


def test_loader_flag_contract():
    with pytest.raises(SystemExit):
        loader.main(["--seed-dir", ".", "--dsn", "x", "--new-series-only"])  # unscoped
    with pytest.raises(SystemExit):
        loader.main(["--seed-dir", ".", "--only-report-dates", "2026-05-31"])  # no dsn, not a dry run
    sql = " ".join(loader.INSERT_NEW_SERIES_SQL.split())
    assert "NOT EXISTS ( SELECT 1 FROM sec_nport_holdings h WHERE h.report_date = s.report_date " \
           "AND h.series_id = s.series_id )" in sql
    assert "s.report_date = ANY(%(dates)s::date[])" in sql


def test_validate_profiles_percentages_as_percent(converted):
    out, _ = converted
    profile = validate.profile_csv(str(out / "2026-06-30.csv"))
    assert profile["series"] == 2 and profile["pct_sum_within_5"] == 1.0
    assert profile["pct_sum_median"] == 100.0 and validate.verdict(profile) == []


def test_validate_judges_series_without_any_percentage(tmp_path):
    seed = tmp_path / "2026-05-31.csv"
    row = ["2026-05-31", "0000000101", "123456789", "US1234567890", "A", "EC", "CORP", "1000", "1", "USD", "",
           "false", "1", "S1"]
    with open(seed, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_COLS)
        writer.writerow([*row[:10], "100", *row[11:]])  # S1 sums to 100
        writer.writerow([*row[:2], "987654321", *row[3:13], "S2"])  # S2: market value, no pct_of_nav
    profile = validate.profile_csv(str(seed))
    assert profile["series"] == 2 and profile["pct_sum_within_5"] == 0.5 and profile["pct_sum_within_10"] == 0.5


class _FakeDatasets:
    def __init__(self, payloads: dict[str, bytes], failures: list[Exception] | None = None,
                 listing_error: Exception | None = None):
        self.payloads = payloads
        self.failures = list(failures or [])
        self.listing_error = listing_error
        self.updated: dict[str, str] = {}
        self.calls: list[str] = []

    def get_dataset_details(self, name):
        assert name == "form-nport"
        if self.listing_error is not None:
            raise self.listing_error
        return {"containers": [
            {"key": key, "size": len(body), "updatedAt": self.updated.get(key, "2026-10-06"),
             "downloadUrl": f"https://x/{key}"}
            for key, body in self.payloads.items()
        ]}

    def _download_file(self, url, dest, expected_size=None):
        self.calls.append(url)
        if self.failures:
            raise self.failures.pop(0)
        key = url.split("https://x/", 1)[1]
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_bytes(self.payloads[key])
        return dest


def test_download_selects_months_and_resyncs_by_size(tmp_path):
    payloads = {"2026/2026-06.jsonl.gz": b"six", "2026/2026-07.jsonl.gz": b"seven", "2025/2025-12.jsonl.gz": b"x"}
    fake = _FakeDatasets(payloads, failures=[Exception("API error: 503 - busy")])
    sleeps: list[float] = []
    summary = download.download_months("2026-06", "2026-07", str(tmp_path), datasets=fake,
                                       sleep=sleeps.append, log=lambda m: None)
    assert [c["month"] for c in summary["containers"]] == ["2026-06", "2026-07"]
    assert summary["bytes_transferred"] == 8 and sleeps == [5.0]  # one transient retry
    again = download.download_months("2026-06", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    assert again["bytes_transferred"] == 0  # sizes match: nothing fetched
    fake.payloads["2026/2026-07.jsonl.gz"] = b"seven, grown"  # the current month keeps growing
    grown = download.download_months("2026-06", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    assert grown["bytes_transferred"] == 12


def test_download_refetches_a_same_size_republication(tmp_path):
    key = "2026/2026-07.jsonl.gz"
    fake = _FakeDatasets({key: b"seven"})
    download.download_months("2026-07", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    fake.payloads[key], fake.updated[key] = b"SEVEN", "2026-10-07"  # corrected, same length
    again = download.download_months("2026-07", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    assert again["bytes_transferred"] == 5
    assert Path(again["containers"][0]["path"]).read_bytes() == b"SEVEN"


def test_download_requires_every_requested_month(tmp_path):
    fake = _FakeDatasets({"2026/2026-06.jsonl.gz": b"six", "2026/2026-08.jsonl.gz": b"eight"})
    with pytest.raises(RuntimeError, match=r"\['2026-07'\]"):
        download.download_months("2026-06", "2026-08", str(tmp_path), datasets=fake, log=lambda m: None)
    assert fake.calls == []  # nothing fetched for a window that cannot be complete
    assert download.months_between("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]


def test_download_never_leaks_the_key(tmp_path):
    key = "a" * 64
    fake = _FakeDatasets({"2026/2026-07.jsonl.gz": b"x"},
                         failures=[Exception(f"404 for https://api.sec-api.io/x?token={key}")])
    with pytest.raises(RuntimeError) as err:
        download.download_months("2026-07", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    assert key not in str(err.value) and "token=***" in str(err.value)
    assert download.month_of("2026/2026-07.jsonl.gz") == "2026-07"  # not the '2026/20' prefix


def test_dataset_listing_failure_never_leaks_the_key(tmp_path):
    key = "b" * 64
    fake = _FakeDatasets({}, listing_error=Exception(f"API error: 500 - https://api.sec-api.io/d?token={key}"))
    with pytest.raises(RuntimeError) as err:
        download.download_months("2026-07", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    printed = "".join(traceback.format_exception(err.value))
    assert key not in printed and "token=***" in printed


def test_worker_dry_run_is_the_load_plus_dry_run(monkeypatch, tmp_path):
    calls: list[list[str]] = []
    monkeypatch.setattr(worker.loader, "main", lambda argv: calls.append(argv) or 0)
    assert worker.load_report_date("dsn", tmp_path, "2026-05-31") == 0
    assert calls[0] == [*calls[1], "--dry-run"] and "--new-series-only" in calls[1]


def test_worker_lock_id_registered_and_unique():
    from src import db

    assert db.LOCK_NPORT_SECAPI_MONTHLY == 900_363
    ids = [v for k, v in vars(db).items() if k.startswith("LOCK_") and isinstance(v, int)]
    assert ids.count(900_363) == 1


def test_worker_window():
    plan = worker.plan_for(dt.date(2026, 10, 6))
    assert (plan.container_from, plan.container_to, sorted(plan.partial_months)) == ("2026-07", "2026-10", ["2026-10"])
    assert (plan.report_date_from, plan.report_date_to) == ("2026-05-01", "2026-07-31")
    plan = worker.plan_for(dt.date(2026, 1, 3))
    assert (plan.container_from, plan.report_date_from, plan.report_date_to) == ("2025-10", "2025-08-01", "2025-10-31")


@pytest.fixture
def wired(monkeypatch, tmp_path):
    """The worker with its I/O replaced: fixture containers, a fake table, no lock contention."""
    calls: dict = {"loads": [], "refresh": [], "existing": {"2026-05-31": {"S000000002"}}, "lock": True, "rc": {}}

    @contextmanager
    def fake_connect(dsn, autocommit=False):
        yield object()

    @contextmanager
    def fake_lock(conn, lock_id):
        assert lock_id == worker.LOCK_NPORT_SECAPI_MONTHLY
        yield calls["lock"]

    def fake_download(month_from, month_to, out_dir, *, api_key):
        assert api_key == "k" * 64
        return {"containers": [{"key": Path(p).name, "path": p} for p in CONTAINERS], "bytes_transferred": 123}

    def fake_load(dsn, seed_dir, report_date):
        calls["loads"].append((report_date, sorted({r["series_id"] for r in _rows(seed_dir / f"{report_date}.csv")})))
        return calls["rc"].get(report_date, 0)

    monkeypatch.setenv("SEC_API_IO_KEY", "k" * 64)
    monkeypatch.setenv(worker.ENV_CACHE_DIR, str(tmp_path))
    monkeypatch.setattr(worker, "connect", fake_connect)
    monkeypatch.setattr(worker, "advisory_lock", fake_lock)
    monkeypatch.setattr(worker.downloader, "download_months", fake_download)
    monkeypatch.setattr(worker, "existing_series", lambda dsn, rd: calls["existing"].get(rd, set()))
    monkeypatch.setattr(worker, "load_report_date", fake_load)
    monkeypatch.setattr(worker, "refresh_cagg", lambda dsn, lo, hi: calls["refresh"].append((lo, hi)))
    monkeypatch.setattr(worker, "report_date_counts",
                        lambda dsn, rds: {rd: {"rows": 1, "series": 1} for rd in rds})
    return calls


def test_worker_loads_the_window_one_date_at_a_time(wired):
    # As of 2026-09: containers 06..09, report_dates 2026-04-01..2026-06-30.
    stats = worker.run("dsn", calc_date="2026-09-10")
    assert stats["state"] == "ok" and stats["bytes_transferred"] == 123
    assert wired["loads"] == [
        ("2026-05-31", ["S000000001", "S000000002", "S000000003"]),
        ("2026-06-30", ["CIK:0000000104", "S000000004"]),
    ]
    assert stats["report_dates"]["2026-05-31"]["new_series"] == 2  # S000000002 already loaded
    assert stats["outside_window"] == ["2026-03-31"]  # older than M-5: left to an operator
    assert wired["refresh"] == [("2026-05-31", "2026-07-01")]


def test_worker_skips_dates_with_nothing_new_and_reports_failures(wired):
    wired["existing"]["2026-06-30"] = {"CIK:0000000104", "S000000004"}
    wired["rc"]["2026-05-31"] = 2
    stats = worker.run("dsn", calc_date="2026-09-10")
    assert stats["report_dates"]["2026-06-30"]["result"] == "no_new_series"
    assert stats["report_dates"]["2026-05-31"]["result"] == "failed"
    assert stats["state"] == "failed" and wired["refresh"] == []


def _converter_writing(csvs: dict[str, list[list[str]]]):
    """A stand-in for ``convert.convert`` that writes these rows, one complete CSV per report_date."""
    def fake_convert(paths, out_dir, *, min_report_date, partial_months):
        Path(out_dir).mkdir(parents=True)
        for rd, rows in csvs.items():
            with open(Path(out_dir) / f"{rd}.csv", "w", encoding="utf-8", newline="") as fh:
                csv.writer(fh).writerows([CSV_COLS, *rows])
        return {"report_dates": {rd: {"rows": len(rows), "series": 1, "partial": False} for rd, rows in csvs.items()},
                "excluded_report_dates": {}}
    return fake_convert


def test_worker_never_refreshes_across_a_failed_date(wired, monkeypatch):
    """A date the loader's verify rejected has committed rows; no refresh range may cover it."""
    dates = ["2026-04-30", "2026-05-31", "2026-06-30"]
    monkeypatch.setattr(worker.converter, "convert",
                        _converter_writing({rd: [[rd, *[""] * 12, f"S-{rd}"]] for rd in dates}))
    wired["rc"]["2026-05-31"] = 2  # post-load verify: rows committed, run failed
    stats = worker.run("dsn", calc_date="2026-09-10")
    assert stats["state"] == "failed"
    assert wired["refresh"] == [("2026-04-30", "2026-05-01"), ("2026-06-30", "2026-07-01")]


def test_worker_validates_values_before_it_loads(wired, monkeypatch):
    row = ["2026-05-31", "0000000101", "123456789", "US1", "A", "EC", "CORP", "1", "1", "USD", "100", "false", "1"]
    monkeypatch.setattr(worker.converter, "convert", _converter_writing({
        "2026-05-31": [[*row[:10], "5000", *row[11:], "S9"]],  # a x50 unit bug: ISIN fill is perfect
        "2026-06-30": [["2026-06-30", *row[1:], "S8"]],
    }))
    stats = worker.run("dsn", calc_date="2026-09-10")
    assert [rd for rd, _ in wired["loads"]] == ["2026-06-30"]
    assert stats["report_dates"]["2026-05-31"]["result"] == "failed"
    assert "above 1000%" in stats["report_dates"]["2026-05-31"]["validation"][0]
    assert stats["state"] == "failed"


def test_worker_fails_when_conversion_yields_no_target_date(wired, monkeypatch):
    monkeypatch.setattr(worker.converter, "convert", _converter_writing({}))
    stats = worker.run("dsn", calc_date="2026-09-10")
    assert stats["state"] == "failed" and "no complete report_date" in stats["reason"]


def test_worker_refuses_a_date_whose_main_month_it_does_not_have(wired):
    # As of 2026-08 the window reaches 2026-03-31, whose main month (2026-05) the
    # fixture does not carry: the date must be refused, not loaded half-filed.
    stats = worker.run("dsn", calc_date="2026-08-15")
    assert stats["report_dates"]["2026-03-31"]["result"] == "partial"
    assert [rd for rd, _ in wired["loads"]] == ["2026-05-31"]


def test_worker_lock_and_credential_contract(wired, monkeypatch):
    wired["lock"] = False
    assert worker.run("dsn", calc_date="2026-08-15") == {"status": "lock_busy"}
    monkeypatch.delenv("SEC_API_IO_KEY")
    assert worker.run("dsn", calc_date="2026-08-15")["state"] == "failed"


def test_fixture_is_what_the_docstring_says():
    lines = [json.loads(line) for path in CONTAINERS for line in Path(path).read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 11
    assert {f["submissionType"] for f in lines} == {"NPORT-P", "NPORT-P/A", "NT NPORT-P"}
