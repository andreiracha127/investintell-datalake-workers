from __future__ import annotations

import datetime as dt
import math
from contextlib import contextmanager
from typing import Any

import pytest

import src.workers.market_overview_snapshot as snapshot
from src.workers.market_overview_snapshot import OverviewRow, build_payload


AS_OF = dt.date(2026, 7, 13)


def _row(
    ticker: str,
    *,
    last: float,
    prev: float,
    volume: int,
    high: float,
    low: float,
    sector: str | None = "Technology",
) -> OverviewRow:
    return OverviewRow(
        ticker=ticker,
        name=f"{ticker} Corp",
        sector=sector,
        last=last,
        prev=prev,
        volume=volume,
        high_52w=high,
        low_52w=low,
    )


def test_build_payload_preserves_overview_product_rules() -> None:
    rows = [
        _row("HIGH", last=110, prev=100, volume=100_000, high=110, low=50),
        _row("LOW", last=90, prev=100, volume=100_000, high=140, low=90),
        _row(
            "FLAT",
            last=50,
            prev=50,
            volume=200_000,
            high=70,
            low=30,
            sector="Energy",
        ),
        _row("NEAR", last=98.1, prev=97, volume=100_000, high=100, low=60),
        _row("PENNY", last=4, prev=3, volume=2_000_000, high=5, low=1),
        _row("ILLIQ", last=100, prev=90, volume=10_000, high=110, low=70),
    ]
    index_closes = {
        "SPY": [500.0, 505.0],
        "QQQ": [450.0],
        "DIA": [float(value) for value in range(1, 32)],
        "IWM": [200.0, 200.0],
    }

    payload = build_payload(AS_OF, rows, index_closes)

    assert set(payload) == {
        "as_of",
        "universe_size",
        "indices",
        "most_active",
        "gainers",
        "losers",
        "highs_52w",
        "lows_52w",
        "sectors",
        "breadth",
    }
    assert payload["as_of"] == "2026-07-13"
    assert payload["universe_size"] == 6
    assert [row["ticker"] for row in payload["most_active"]] == [
        "HIGH",
        "FLAT",
        "NEAR",
        "LOW",
    ]
    assert [row["ticker"] for row in payload["gainers"]] == ["HIGH", "NEAR"]
    assert [row["ticker"] for row in payload["losers"]] == ["LOW"]
    assert [row["ticker"] for row in payload["highs_52w"]] == ["HIGH", "NEAR"]
    assert [row["ticker"] for row in payload["lows_52w"]] == ["LOW"]

    sectors = {row["sector"]: row for row in payload["sectors"]}
    assert sectors["Technology"]["n"] == 3
    assert sectors["Technology"]["change_pct_median"] == pytest.approx(98.1 / 97 - 1)
    assert sectors["Energy"] == {"sector": "Energy", "change_pct_median": 0.0, "n": 1}

    assert payload["breadth"] == {
        "tracked": 4,
        "advancing": 2,
        "declining": 1,
        "unchanged": 1,
        "advance_decline_ratio": 2.0,
        "new_highs_52w": 1,
        "new_lows_52w": 1,
        "up_volume_share": pytest.approx(0.4),
    }

    assert [card["ticker"] for card in payload["indices"]] == ["SPY", "DIA", "IWM"]
    assert payload["indices"][0]["change_pct"] == pytest.approx(0.01)
    assert payload["indices"][1]["spark"] == [float(value) for value in range(2, 32)]
    assert len(payload["indices"][1]["spark"]) == 30


def test_build_payload_caps_each_ranking_at_twenty_five() -> None:
    rows = [
        _row(
            f"T{index:02d}",
            last=100 + index,
            prev=100,
            volume=100_000,
            high=200,
            low=50,
        )
        for index in range(30)
    ]

    payload = build_payload(AS_OF, rows, {})

    assert len(payload["most_active"]) == 25
    assert len(payload["gainers"]) == 25


@pytest.mark.parametrize("bad_value", [math.nan, math.inf, -math.inf])
def test_build_payload_rejects_non_finite_values(bad_value: float) -> None:
    rows = [
        _row(
            "BAD",
            last=bad_value,
            prev=100,
            volume=100_000,
            high=110,
            low=90,
        )
    ]

    with pytest.raises(ValueError, match="finite"):
        build_payload(AS_OF, rows, {})


def test_build_payload_rejects_unavailable_source_rows() -> None:
    with pytest.raises(ValueError, match="source rows"):
        build_payload(AS_OF, [], {})


def test_payload_contract_accepts_no_liquid_breadth() -> None:
    payload = build_payload(
        AS_OF,
        [_row("PENNY", last=4, prev=3, volume=2_000_000, high=5, low=1)],
        {},
    )

    assert payload["breadth"] is None
    snapshot._validate_payload(payload, AS_OF)


def test_payload_contract_rejects_unversioned_top_level_fields() -> None:
    payload = build_payload(
        AS_OF,
        [_row("AAA", last=110, prev=100, volume=100_000, high=110, low=50)],
        {},
    )
    payload["unexpected"] = True

    with pytest.raises(ValueError, match="unexpected fields"):
        snapshot._validate_payload(payload, AS_OF)


class _FakeCursor:
    def __init__(self, sink: dict[str, Any]) -> None:
        self._sink = sink
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        normalized = " ".join(sql.lower().split())
        self._sink.setdefault("queries", []).append((normalized, params))
        if "select max(as_of) from price_latest_mv" in normalized:
            self._rows = [(self._sink.get("watermark", AS_OF),)]
        elif "from price_latest_mv" in normalized:
            last = self._sink.get("last", 110.0)
            self._rows = [("AAA", "Alpha", "Technology", last, 100.0, 100_000)]
        elif "max(ep.close)" in normalized:
            self._rows = [("AAA", 110.0, 90.0)]
        elif "row_number() over" in normalized:
            self._rows = [
                ("SPY", dt.date(2026, 7, 10), 500.0),
                ("SPY", AS_OF, 505.0),
            ]
        else:
            self._rows = []

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(self, sink: dict[str, Any], *, autocommit: bool) -> None:
        self._sink = sink
        self._autocommit = autocommit

    def __enter__(self) -> _FakeConnection:
        self._sink.setdefault("entered", []).append(self._autocommit)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self._sink.setdefault("exits", []).append((self._autocommit, exc_type))
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._sink)


def _install_fake_db(monkeypatch: pytest.MonkeyPatch, sink: dict[str, Any]) -> None:
    def fake_connect(dsn: str, *, autocommit: bool = False) -> _FakeConnection:
        sink.setdefault("connects", []).append((dsn, autocommit))
        return _FakeConnection(sink, autocommit=autocommit)

    @contextmanager
    def fake_lock(conn: _FakeConnection, lock_id: int):
        sink["lock_id"] = lock_id
        yield sink.get("got_lock", True)

    monkeypatch.setattr(snapshot, "connect", fake_connect, raising=False)
    monkeypatch.setattr(snapshot, "advisory_lock", fake_lock, raising=False)


def test_run_uses_mv_watermark_bounded_queries_and_atomic_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink: dict[str, Any] = {}
    _install_fake_db(monkeypatch, sink)
    computed_at = dt.datetime(2026, 7, 15, 12, tzinfo=dt.UTC)

    result = snapshot.run("postgres://app", now=computed_at)

    queries = [sql for sql, _params in sink["queries"]]
    assert queries[0] == "select max(as_of) from price_latest_mv"
    assert any(
        "ep.ticker = pl.ticker and ep.date = pl.as_of" in sql
        for sql in queries
    )
    extremes_query = next(sql for sql in queries if "max(ep.close)" in sql)
    assert "ep.date >= %s" in extremes_query
    assert "ep.date <= %s" in extremes_query
    index_query = next(sql for sql in queries if "row_number() over" in sql)
    assert "ep.ticker = any(%s)" in index_query
    assert "ep.date >= %s" in index_query
    assert "ep.date <= %s" in index_query
    upsert_query = next(sql for sql in queries if sql.startswith("insert into"))
    assert "on conflict (snapshot_key) do update" in upsert_query
    assert sink["entered"] == [True, False]
    assert sink["exits"] == [(False, None), (True, None)]
    assert result["as_of"] == "2026-07-13"
    assert result["computed_at"] == "2026-07-15T12:00:00+00:00"
    assert result["source_rows"] == 1
    assert result["indices"] == 1
    assert result["published"] == 1
    assert result["elapsed_ms"] >= 0


def test_run_rejects_non_finite_input_before_opening_write_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink: dict[str, Any] = {"last": math.nan}
    _install_fake_db(monkeypatch, sink)

    with pytest.raises(ValueError, match="finite"):
        snapshot.run("postgres://app")

    assert sink["entered"] == [True]
    assert not any(sql.startswith("insert into") for sql, _params in sink["queries"])


def test_run_rejects_missing_mv_watermark_without_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink: dict[str, Any] = {"watermark": None}
    _install_fake_db(monkeypatch, sink)

    with pytest.raises(RuntimeError, match="watermark"):
        snapshot.run("postgres://app")

    assert sink["entered"] == [True]
    assert not any(sql.startswith("insert into") for sql, _params in sink["queries"])


def test_run_fails_when_dedicated_lock_is_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    sink: dict[str, Any] = {"got_lock": False}
    _install_fake_db(monkeypatch, sink)

    with pytest.raises(RuntimeError, match="lock is busy"):
        snapshot.run("postgres://app")

    assert sink["entered"] == [True]
    assert sink.get("queries", []) == []


def test_run_rejects_invalid_payload_shape_before_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink: dict[str, Any] = {}
    _install_fake_db(monkeypatch, sink)
    monkeypatch.setattr(
        snapshot,
        "build_payload",
        lambda *_args: {"as_of": AS_OF.isoformat(), "indices": "invalid"},
    )

    with pytest.raises(ValueError, match="payload contract"):
        snapshot.run("postgres://app")

    assert sink["entered"] == [True]
    assert not any(sql.startswith("insert into") for sql, _params in sink["queries"])
