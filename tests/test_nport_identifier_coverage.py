"""A DERA package that lost its ISIN side must not load in silence.

Twice already, ``sec_nport_holdings`` took a quarterly package whose ISIN join
had been lost upstream of the COPY. Every row arrived, every other column was
populated, and the only trace was that holdings which should carry
``cusip='IS:<isin>'`` with a populated ``isin`` arrived as ``cusip='LE:<lei>'``
with ``isin`` NULL. Nothing failed; a slice of history simply stopped joining.
These tests pin the three properties that close that hole: the probe reads the
column that actually separates the two populations, an empty window is not
mistaken for a pass, and the probe observes without stopping the worker that
hosts it.
"""
from __future__ import annotations

import datetime as dt
import logging

from src.workers import nport_identifier_coverage as coverage


class _FakeCursor:
    def __init__(self, rows: list[tuple], sink: dict) -> None:
        self._rows = rows
        self._sink = sink

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self._sink.setdefault("sql", []).append(sql)
        self._sink.setdefault("params", []).append(params)

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.sink: dict = {}

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._rows, self.sink)


def _row(report_date: str, rows: int, isin_fill: float, ident: float = 0.99) -> tuple:
    return (dt.date.fromisoformat(report_date), rows,
            int(round(rows * isin_fill)), int(round(rows * ident)))


# The real readings. Clean side: the two worst report_dates ever observed with
# >= 1000 rows over 2019-09-30..2026-01-31. Degraded side: the best of the eight
# report_dates the two lost packages produced.
_WORST_CLEAN = _row("2025-11-28", 53_327, 0.9445, 0.9854)
_SECOND_WORST_CLEAN = _row("2025-05-30", 51_538, 0.9509, 0.9870)
_BEST_DEGRADED = _row("2024-11-29", 49_679, 0.6198, 0.9108)
_WORST_DEGRADED = _row("2023-10-31", 1_042_545, 0.1516, 0.7983)


def test_clean_tail_is_clean() -> None:
    verdict = coverage.probe(_FakeConn([
        _row("2025-09-30", 1_861_409, 0.9835),
        _WORST_CLEAN,
        _SECOND_WORST_CLEAN,
        _row("2026-01-31", 1_087_270, 0.9921),
    ]))
    assert verdict["state"] == "clean"
    assert verdict["degraded_report_dates"] == []
    assert verdict["report_dates_judged"] == 4


def test_the_worst_clean_reading_ever_observed_still_passes() -> None:
    """The floor is 0.90 and the worst clean date measured is 0.9445.

    If a future edit tightens the floor past that reading, this test is the one
    that says the gate started firing on healthy data.
    """
    verdict = coverage.probe(_FakeConn([_WORST_CLEAN, _SECOND_WORST_CLEAN]))
    assert verdict["state"] == "clean"
    assert verdict["worst_isin_fill"] == 0.9445


def test_the_best_degraded_reading_ever_observed_is_caught() -> None:
    verdict = coverage.probe(_FakeConn([_row("2024-10-31", 1_176_135, 0.9861),
                                        _BEST_DEGRADED, _WORST_DEGRADED]))
    assert verdict["state"] == "degraded"
    assert verdict["degraded_report_dates"] == ["2024-11-29", "2023-10-31"]
    assert verdict["worst_isin_fill"] == 0.1516


def test_identifiability_alone_would_not_have_separated_them() -> None:
    """Why the gate is the ISIN column and not "does the row have an id".

    The consumer-visible symptom is an unidentifiable row, and over the same
    history the two populations DO order correctly on it -- by 0.44 pp, worst
    clean 0.9657 against best degraded 0.9613. No floor lives inside that. The
    ISIN fill orders the same two readings 0.9652 against 0.5130, a 45 pp gap,
    which is where a floor can actually sit.
    """
    clean_ident = _row("2023-04-28", 125_036, 0.9652, 0.9657)
    degraded_ident = _row("2024-11-30", 835_838, 0.5130, 0.9613)
    verdict = coverage.probe(_FakeConn([clean_ident, degraded_ident]))

    by_date = {e["report_date"]: e for e in verdict["checked"]}
    ident_margin = (by_date["2023-04-28"]["identifiable_share"]
                    - by_date["2024-11-30"]["identifiable_share"])
    isin_margin = by_date["2023-04-28"]["isin_fill"] - by_date["2024-11-30"]["isin_fill"]
    assert ident_margin < 0.005          # 0.44 pp: not a gate
    assert isin_margin > 0.40            # 45 pp: a gate
    # And the floor the probe ships with calls both of them right.
    assert by_date["2023-04-28"]["degraded"] is False
    assert by_date["2024-11-30"]["degraded"] is True


def test_thin_report_dates_are_not_judged() -> None:
    """2020-08-30 carries 31 rows at 61.3 % fill and is not a package failure."""
    verdict = coverage.probe(_FakeConn([
        _row("2020-08-30", 31, 0.6129),
        _row("2020-08-31", 753_819, 0.9807),
    ]))
    assert verdict["state"] == "clean"
    assert [e["judged"] for e in verdict["checked"]] == [False, True]


def test_an_empty_window_is_undecidable_not_clean() -> None:
    verdict = coverage.probe(_FakeConn([]))
    assert verdict["state"] == "undecidable"
    assert verdict["report_dates_judged"] == 0

    thin_only = coverage.probe(_FakeConn([_row("2020-08-30", 31, 0.6129)]))
    assert thin_only["state"] == "undecidable"


def test_degraded_verdict_is_logged_with_the_repair(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger=coverage.LOGGER.name):
        coverage.probe(_FakeConn([_WORST_DEGRADED]))
    message = caplog.text
    assert "2023-10-31" in message
    # A plain re-run of the loader is a no-op against rows that already own the
    # conflict key; the operator has to be told that, not left to discover it.
    assert "DO NOTHING" in message


def test_window_is_bounded_and_passed_as_a_parameter() -> None:
    conn = _FakeConn([_row("2026-01-31", 1_087_270, 0.9921)])
    coverage.probe(conn, window_days=90)
    assert conn.sink["params"] == [(90,)]
    assert "make_interval(days => %s)" in conn.sink["sql"][0]
    assert "max(report_date)" in conn.sink["sql"][0]


# ──────────────────────────────────────────────────────────────────────────────
# Wiring: the probe rides on the weekly worker that reads the column
# ──────────────────────────────────────────────────────────────────────────────
def _stub_lookthrough(monkeypatch, verdict: dict):
    """Reduce nport_lookthrough.run to its control flow, keeping the seam real."""
    import contextlib

    from src.workers import nport_lookthrough as lt

    @contextlib.contextmanager
    def _connect(dsn, **kwargs):
        yield object()

    @contextlib.contextmanager
    def _lock(conn, key):
        yield True

    monkeypatch.setattr(lt, "connect", _connect)
    monkeypatch.setattr(lt, "advisory_lock", _lock)
    monkeypatch.setattr(lt, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(lt, "build_fund_map", lambda conn: {})
    monkeypatch.setattr(lt, "build_sector_map", lambda conn: {})
    monkeypatch.setattr(lt, "_list_parents", lambda conn, cdate, limit: ["S1"])
    monkeypatch.setattr(lt, "_process_shard", lambda *a, **k: (1, 1, 3))
    monkeypatch.setattr(lt.nport_identifier_coverage, "probe", lambda conn, **k: verdict)
    return lt


def test_lookthrough_reports_the_verdict_in_its_stats(monkeypatch) -> None:
    verdict = {"state": "clean", "degraded_report_dates": []}
    lt = _stub_lookthrough(monkeypatch, verdict)
    stats = lt.run("postgres://lake", calc_date="2026-01-31", serial=True)
    assert stats["identifier_coverage"] == verdict


def test_a_degraded_verdict_does_not_stop_the_lookthrough(monkeypatch) -> None:
    """The damage is to history already written.

    Failing the weekly run would cost a week of exposures without repairing one
    row, so the probe observes and the worker finishes.
    """
    verdict = {"state": "degraded", "degraded_report_dates": ["2025-01-31"]}
    lt = _stub_lookthrough(monkeypatch, verdict)
    stats = lt.run("postgres://lake", calc_date="2026-01-31", serial=True)
    assert stats["identifier_coverage"]["state"] == "degraded"
    assert stats["upserted_series"] == 1
