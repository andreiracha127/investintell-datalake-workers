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


def test_loader_dry_run_accepts_converter_output(converted, capsys):
    out, _ = converted
    rc = loader.main(["--seed-dir", str(out), "--only", "2026-05-31.csv",
                      "--only-report-dates", "2026-05-31", "--dry-run"])
    assert rc == 0
    assert "dry run clean" in capsys.readouterr().out


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


class _FakeDatasets:
    def __init__(self, payloads: dict[str, bytes], failures: list[Exception] | None = None):
        self.payloads = payloads
        self.failures = list(failures or [])
        self.calls: list[str] = []

    def get_dataset_details(self, name):
        assert name == "form-nport"
        return {"containers": [
            {"key": key, "size": len(body), "updatedAt": "2026-10-06", "downloadUrl": f"https://x/{key}"}
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


def test_download_never_leaks_the_key(tmp_path):
    key = "a" * 64
    fake = _FakeDatasets({"2026/2026-07.jsonl.gz": b"x"},
                         failures=[Exception(f"404 for https://api.sec-api.io/x?token={key}")])
    with pytest.raises(RuntimeError) as err:
        download.download_months("2026-07", "2026-07", str(tmp_path), datasets=fake, log=lambda m: None)
    assert key not in str(err.value) and "token=***" in str(err.value)
    assert download.month_of("2026/2026-07.jsonl.gz") == "2026-07"  # not the '2026/20' prefix


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
