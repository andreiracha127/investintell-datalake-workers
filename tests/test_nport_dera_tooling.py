"""The write path of ``sec_nport_holdings``, pinned against a real DERA slice.

``tests/fixtures/nport_dera/2023q4_slice`` is eight filings carved verbatim out of
the real ``2023q4`` package — the package whose ISIN side was lost in production —
across two of the eight damaged report_dates. It is small enough to commit and
complete enough to exercise all four branches of ``_synthetic_cusip``.

What these tests exist to prevent is narrow and specific. The historical defect
was not a crash: the parser ran, the loader ran, 96M rows landed, every column
except ``isin`` was fully populated, and the run exited zero. So the assertions
here are about the two things that were true then and must never be true again —
that a short ``HOLDING_ID -> ISIN`` map is indistinguishable from a good one, and
that a repair reload can silently do nothing.
"""
from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import pytest

from tools.nport_dera import nport_bulk_parse as parse
from tools.nport_dera import nport_parallel_load as load

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "nport_dera" / "2023q4_slice"

#: Pinned to what the shipped parser actually produces over this slice. If the
#: parsing logic drifts these move, and the repair stops being comparable to the
#: measurements the 2026-08 laudo was built on.
EXPECTED = {
    "2023-09-30": {"rows": 724, "isin": 614, "real_cusip": 606, "IS": 33, "LE": 77, "H": 8},
    "2023-10-31": {"rows": 448, "isin": 442, "real_cusip": 299, "IS": 143, "LE": 6, "H": 0},
}
TOTAL_ROWS = sum(v["rows"] for v in EXPECTED.values())


@pytest.fixture(scope="module")
def parsed(tmp_path_factory) -> tuple[dict, list[dict]]:
    out = tmp_path_factory.mktemp("nport") / "slice.csv"
    stats = parse.parse_dataset(str(FIXTURE), str(out))
    with open(out, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    return stats, rows


def test_every_holding_is_emitted_with_its_columns_populated(parsed):
    """No truncation gate, and no column silently empty except by source."""
    stats, rows = parsed
    assert stats["rows"] == stats["written"] == TOTAL_ROWS
    assert len(rows) == TOTAL_ROWS
    assert list(rows[0]) == parse.CSV_COLS

    # The key columns are populated for every row, by construction: that is why
    # a lost ISIN map is invisible in production. ``cusip`` in particular is
    # ALWAYS non-empty — it just silently stops being the right key.
    for column in ("report_date", "cik", "cusip", "series_id", "is_restricted"):
        assert all(row[column] for row in rows), f"{column} came back empty"

    # The optional columns come back at their source fill rate and stay there
    # whether or not the ISIN side was lost. If one of these moves, the failure
    # is a different one than the ISIN defect and must not be read as it.
    fill = {c: sum(1 for r in rows if r[c]) for c in parse.CSV_COLS}
    assert fill["issuer_name"] == 1156
    assert fill["sector"] == fill["asset_class"] == fill["quantity"] == 1164
    assert fill["currency"] == 1157
    assert fill["fair_value_level"] == 1105
    assert fill["isin"] == sum(v["isin"] for v in EXPECTED.values())


def test_isin_is_recovered_from_the_identifiers_side(parsed):
    """The reading that separates a good load from a bad one."""
    stats, _ = parsed
    assert stats["isin_map_size"] == 1056
    fills = parse.isin_fill_by_report_date(stats)
    assert set(fills) == set(EXPECTED)
    for report_date, expected in EXPECTED.items():
        counter = stats["per_report_date"][report_date]
        assert counter["rows"] == expected["rows"]
        assert counter["isin"] == expected["isin"]
        assert fills[report_date] == pytest.approx(expected["isin"] / expected["rows"])


def test_all_four_cusip_branches_are_exercised(parsed):
    """Real CUSIP, ``IS:``, ``LE:`` and ``H:`` — the fixture covers the whole ladder."""
    stats, rows = parsed
    for report_date, expected in EXPECTED.items():
        counter = stats["per_report_date"][report_date]
        assert counter["real_cusip"] == expected["real_cusip"]
        assert counter["synthetic_is"] == expected["IS"]
        assert counter["synthetic_le"] == expected["LE"]
        assert counter["synthetic_h"] == expected["H"]

    # Every ``IS:`` key carries the ISIN that the identifiers side supplied, and
    # nothing else in the table does. That equality is the join the defect broke.
    is_rows = [r for r in rows if r["cusip"].startswith("IS:")]
    assert len(is_rows) == EXPECTED["2023-09-30"]["IS"] + EXPECTED["2023-10-31"]["IS"]
    assert all(r["cusip"] == f"IS:{r['isin']}" for r in is_rows)

    # A synthetic key must never be mistakeable for a real CUSIP-9 downstream.
    synthetic = [r["cusip"] for r in rows if r["cusip"][:3] in {"IS:", "LE:", "H:"} or r["cusip"].startswith("H:")]
    assert synthetic, "fixture no longer exercises the synthetic branch"
    assert not any(len(c) == 9 and c.isalnum() for c in synthetic)


def test_only_report_dates_bounds_what_is_written(tmp_path):
    """The repair scope guard: a package carries its neighbours' report_dates too."""
    out = tmp_path / "scoped.csv"
    stats = parse.parse_dataset(str(FIXTURE), str(out), {"2023-10-31"})
    assert set(stats["per_report_date"]) == {"2023-10-31"}
    assert stats["written"] == EXPECTED["2023-10-31"]["rows"]
    assert stats["filtered_report_date"] == EXPECTED["2023-09-30"]["rows"]
    with open(out, encoding="utf-8", newline="") as fh:
        assert {r["report_date"] for r in csv.DictReader(fh)} == {"2023-10-31"}


def test_a_short_isin_map_reproduces_the_production_signature(tmp_path):
    """Gut the ISIN side and the parse degrades exactly the way 2023q4 did.

    In production the tell was never an error. It was ``isin`` collapsing while
    every other column held, ``IS:`` collapsing with it, and ``LE:`` exploding to
    absorb the difference. This reproduces that from the same fixture, so the
    signature the monitor gates on is pinned to a mechanism and not to a memory.
    """
    crippled = tmp_path / "crippled"
    crippled.mkdir()
    for src in FIXTURE.iterdir():
        (crippled / src.name).write_bytes(src.read_bytes())
    # Keep every row and every column; drop only the ISIN values, which is what a
    # partially populated map amounts to at the point of use.
    identifiers = crippled / "IDENTIFIERS.tsv"
    lines = identifiers.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    isin_at = header.index("IDENTIFIER_ISIN")
    out_lines = [lines[0]]
    for line in lines[1:]:
        parts = line.split("\t")
        parts[isin_at] = ""
        out_lines.append("\t".join(parts))
    identifiers.write_text("\n".join(out_lines) + "\n", encoding="utf-8")

    stats = parse.parse_dataset(str(crippled), str(tmp_path / "bad.csv"))

    assert stats["isin_map_size"] == 0
    assert stats["written"] == TOTAL_ROWS, "the damage is invisible in the row count"
    for report_date, expected in EXPECTED.items():
        counter = stats["per_report_date"][report_date]
        assert counter["rows"] == expected["rows"]
        assert counter["isin"] == 0
        assert counter["real_cusip"] == expected["real_cusip"], "real CUSIPs are untouched"
        assert counter["synthetic_is"] == 0, "the IS: branch collapses"
        # everything that had an ISIN and no CUSIP falls through to LE:/H:
        assert counter["synthetic_le"] + counter["synthetic_h"] == (
            expected["IS"] + expected["LE"] + expected["H"]
        )

    flagged = parse.report_dates_below_floor(stats, parse.DEFAULT_FILL_FLOOR, min_rows=1)
    assert [rd for rd, _ in flagged] == sorted(EXPECTED)


def test_a_thin_report_date_is_reported_but_not_judged(parsed):
    """One filer's off-cycle month-end must not read as a package failure."""
    stats, _ = parsed
    # 2023-09-30 parses at 0.848 in this 724-row slice; under the production
    # min_rows it is below the floor and still not flagged.
    assert parse.isin_fill_by_report_date(stats)["2023-09-30"] < parse.DEFAULT_FILL_FLOOR
    assert parse.report_dates_below_floor(stats, parse.DEFAULT_FILL_FLOOR, min_rows=1000) == []
    assert [rd for rd, _ in parse.report_dates_below_floor(stats, parse.DEFAULT_FILL_FLOOR, min_rows=1)] == [
        "2023-09-30"
    ]


def test_the_cli_refuses_to_hand_over_a_parse_below_the_floor(tmp_path):
    """Exit code, not a log line: a bad parse must not reach the loader."""
    proc = subprocess.run(
        [sys.executable, "-m", "tools.nport_dera.nport_bulk_parse", str(FIXTURE),
         "-o", str(tmp_path / "cli.csv"), "--dedupe-conflict-key",
         "--fill-floor", "0.99", "--fill-floor-min-rows", "1"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True,
    )
    assert proc.returncode == 2
    assert "REFUSING" in proc.stderr
    assert "ON CONFLICT DO NOTHING" in proc.stderr


def test_the_floor_is_not_applied_to_a_reading_that_cannot_predict_the_table(tmp_path):
    """A raw parse under-reads its own ISIN fill; refusing on it would be noise.

    2023-08-31 parses at 0.586 over the raw file and lands at 0.991 in the table,
    because the loader discards the ISIN-poor ``LE:`` collisions on the way in.
    Gating the raw number would fail every correct repair parse, which is the
    fastest way to teach an operator to ignore the gate.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "tools.nport_dera.nport_bulk_parse", str(FIXTURE),
         "-o", str(tmp_path / "raw.csv"), "--fill-floor", "0.99", "--fill-floor-min-rows", "1"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True,
    )
    assert proc.returncode == 0
    assert "NOT applied" in proc.stderr
    assert "REFUSING" not in proc.stderr


def test_a_scoped_parse_defers_the_verdict_instead_of_faking_one(tmp_path):
    """Outside its own quarter a package carries only late and amended filings.

    Those are a few thousand rows from a handful of filers, and their ISIN fill is
    a property of the filers, not of the package's ISIN map — 2024q1 reads 0.733
    over its 6,743 rows on 2023-09-30 and is a perfectly healthy package. Refusing
    on that would fail every correct repair parse.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "tools.nport_dera.nport_bulk_parse", str(FIXTURE),
         "-o", str(tmp_path / "scoped.csv"), "--dedupe-conflict-key",
         "--only-report-dates", "2023-09-30", "--fill-floor", "0.99",
         "--fill-floor-min-rows", "1"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True,
    )
    assert proc.returncode == 0
    assert "NOT applied" in proc.stderr
    assert "post-load verify" in proc.stderr
    assert "REFUSING" not in proc.stderr


def test_dedupe_applies_the_loaders_conflict_key_and_keeps_the_first_row(tmp_path):
    """``ON CONFLICT DO NOTHING`` resolves in scan order, so first-row-wins matches."""
    plain = parse.parse_dataset(str(FIXTURE), str(tmp_path / "plain.csv"))
    deduped = parse.parse_dataset(str(FIXTURE), str(tmp_path / "dedup.csv"), dedupe_conflict_key=True)

    assert plain["conflict_key_dupes"] == 0 and plain["deduped"] is False
    assert deduped["deduped"] is True
    assert deduped["written"] + deduped["conflict_key_dupes"] == plain["written"]

    with open(tmp_path / "dedup.csv", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    keys = [(r["report_date"], r["series_id"], r["cusip"]) for r in rows]
    assert len(keys) == len(set(keys)), "the emitted CSV still collides on the loader's key"

    # first-row-wins: the surviving row is the one the plain parse emitted first
    with open(tmp_path / "plain.csv", encoding="utf-8", newline="") as fh:
        first_seen: dict = {}
        for r in csv.DictReader(fh):
            first_seen.setdefault((r["report_date"], r["series_id"], r["cusip"]), r)
    assert rows == [first_seen[k] for k in keys]


# --------------------------------------------------------------------------
# loader — the half that turns a correct parse into a no-op if used wrong
# --------------------------------------------------------------------------


def test_a_scoped_load_cannot_write_outside_its_report_dates():
    """Second guard, at the DB boundary, independent of the parser's."""
    assert "report_date = ANY(%(dates)s::date[])" in load.INSERT_SCOPED_SQL
    assert "report_date = ANY" not in load.INSERT_SQL
    # and the reason the guard has to exist at all
    assert "ON CONFLICT (report_date, series_id, cusip) DO NOTHING" in load.INSERT_SCOPED_SQL


def test_delete_first_refuses_to_run_unscoped(tmp_path):
    """An unscoped DELETE on a 96M-row table is not a thing this tool offers."""
    with pytest.raises(SystemExit) as excinfo:
        load.main(["--seed-dir", str(tmp_path), "--dsn", "postgresql:///nope", "--delete-first"])
    assert excinfo.value.code == 2


def test_delete_report_dates_refuses_an_empty_list():
    with pytest.raises(ValueError):
        load.delete_report_dates("postgresql:///nope", [])


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, rows):
        self.cur = _FakeCursor(rows)

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_verify_isin_fill_catches_the_reading_that_went_unnoticed_for_two_years(monkeypatch):
    """The post-load check the original loader did not have."""
    import datetime as dt

    rows = [
        (dt.date(2023, 9, 30), 1_554_959, 1_529_925),   # repaired: 0.9839
        (dt.date(2023, 10, 31), 1_042_545, 158_084),    # as production stood: 0.1516
    ]
    monkeypatch.setattr(load.psycopg, "connect", lambda *a, **k: _FakeConn(rows))

    readings, bad = load.verify_isin_fill("postgresql:///x", ["2023-09-30", "2023-10-31"])
    assert [r["isin_fill"] for r in readings] == [0.9839, 0.1516]
    assert [r["report_date"] for r in bad] == ["2023-10-31"]


def test_a_report_date_that_came_back_empty_is_a_failure_not_a_pass(monkeypatch):
    """A DELETE that ran and a load that did not is the worst outcome available."""
    monkeypatch.setattr(load.psycopg, "connect", lambda *a, **k: _FakeConn([]))
    readings, bad = load.verify_isin_fill("postgresql:///x", ["2023-09-30"])
    assert readings == [{"report_date": "2023-09-30", "rows": 0, "isin": 0, "isin_fill": 0.0}]
    assert bad == readings
