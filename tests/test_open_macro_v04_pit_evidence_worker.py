"""Contract tests for the worker-owned Open Macro v04 PIT evidence materializer."""

from __future__ import annotations

import datetime as dt
import os
from copy import deepcopy

import pytest

import src.workers.open_macro_v04_pit_evidence as evidence


DECISION_MONTH = dt.date(2026, 2, 28)
SEED_IDS = (
    "INDPRO", "PCEC96", "PAYEMS", "ACOGNO", "CPILFESL", "PPIFIS", "AHETPI", "MICH",
)
MIRROR_IDS = ("MTSDS133FMS", "GDP", "SUBLPDCILSLGNQ", "M2SL")
PROXY_IDS = ("CFNAI", "CPIAUCSL")
FRED_IDS = (*SEED_IDS, *MIRROR_IDS)
PUBLIC_KEYS = {
    "group_key", "group_label", "group_role", "series_key", "series_label", "role",
    "display_state", "availability_state", "evidence_state", "freshness_state", "pit_state",
}


def _decision(*, quadrant_source: str = "chain_fresh", carry_age: int = 0) -> dict:
    validity = "fresh" if quadrant_source == "chain_fresh" else "carried" if quadrant_source == "chain_carry" else "no_signal"
    return {
        "as_of": DECISION_MONTH,
        "fiscal_state": "contained",
        "fiscal_boundary": False,
        "guard_level": "off",
        "guard_coverage": "full",
        "quadrant": "recovery",
        "quadrant_source": quadrant_source,
        "carry_age": carry_age,
        "decision_validity": validity,
        "decision_basis": "live",
        "input_digest_sha256": "a" * 64,
        "created_at": "2026-03-03T12:00:00+00:00",
        "run_id": "open-macro-v04-test",
        "updated_at": "2026-03-03T12:00:00+00:00",
    }


def _vintage(series_id: str, *, available_at: str = "2026-02-01T00:00:00+00:00",
             value: float = 1.0, vintage_date: str = "2026-02-01") -> dict:
    return {
        "series_id": series_id,
        "observation_period": "2026-01-01",
        "vintage_date": vintage_date,
        "value": value,
        "available_at": available_at,
        "revision_number": 0,
        "source": "alfred",
        "source_spec_version": "macro_quadrant_us_v1.0",
    }


def _complete_vintages() -> list[dict]:
    return [_vintage(series_id, value=float(index + 1)) for index, series_id in enumerate(SEED_IDS)]


def _mirror(series_id: str, *, value: float = 10.0) -> dict:
    return {
        "series_id": series_id,
        "obs_date": "2026-01-01",
        "value": value,
        "source": "fred",
        "created_at": "2026-02-03T00:00:00+00:00",
        "updated_at": "2026-02-03T00:00:00+00:00",
    }


def _complete_mirrors() -> list[dict]:
    return [_mirror(series_id, value=float(index + 10)) for index, series_id in enumerate(MIRROR_IDS)]


def _proxy_mirrors(*, missing: str | None = None) -> list[dict]:
    rows = _complete_mirrors()
    for offset in range(1, 27):
        month_end = evidence._month_end_shift(DECISION_MONTH, -offset)
        if missing != "CFNAI":
            rows.append({
                **_mirror("CFNAI", value=float(offset)),
                "obs_date": month_end.isoformat(),
            })
        if missing != "CPIAUCSL":
            rows.append({
                **_mirror("CPIAUCSL", value=100.0 + offset),
                "obs_date": month_end.isoformat(),
            })
    return rows


def _captures(mirrors: list[dict]) -> list[dict]:
    by_series = {series_id: [row for row in mirrors if row["series_id"] == series_id]
                 for series_id in MIRROR_IDS}
    return [
        {
            "series_id": series_id,
            "series_digest_sha256": evidence._producer_series_digest(rows),
            "row_count": len(rows),
            "min_obs_date": min(row["obs_date"] for row in rows),
            "max_obs_date": max(row["obs_date"] for row in rows),
            "producer_run_id": "open-macro-v04-test",
            "global_input_digest_sha256": "a" * 64,
            "captured_at": "2026-03-03T12:00:00+00:00",
        }
        for series_id, rows in by_series.items()
        if rows
    ]


def _materialization(*, vintages: list[dict] | None = None,
                     spy_rows: list[dict] | None = None,
                     mirrors: list[dict] | None = None,
                      decision: dict | None = None,
                      captures: list[dict] | None = None):
    selected_mirrors = _complete_mirrors() if mirrors is None else mirrors
    return evidence.build_materialization(
        decision or _decision(),
        vintages=_complete_vintages() if vintages is None else vintages,
        spy_rows=[{"ticker": "SPY", "date": "2026-02-27", "adj_close": 500.0}]
        if spy_rows is None else spy_rows,
        mirror_rows=selected_mirrors,
        captures=_captures(selected_mirrors) if captures is None else captures,
    )


def _item(materialization, series_key: str):
    return next(item for item in materialization.public_items if item["series_key"] == series_key)


def test_build_materialization_derives_closed_public_categorical_taxonomy() -> None:
    materialization = _materialization()

    assert materialization.public_taxonomy == {
        "decision_month": "2026-02",
        "taxonomy_state": "cycle_led",
        "fiscal_state": "contained",
        "fiscal_boundary": False,
        "guard_level": "off",
        "guard_coverage": "full",
        "quadrant": "recovery",
        "cycle_direction": "up",
        "decision_validity": "fresh",
        "decision_basis": "live",
        "quadrant_source": "chain_fresh",
        "book": "center",
    }
    assert not ({"value", "unit", "updated_at", "run_id", "fingerprint"}
                & materialization.public_taxonomy.keys())


@pytest.mark.parametrize(
    ("decision", "expected"),
    [
        (
            {
                **_decision(),
                "fiscal_state": "dominance",
                "guard_level": "off",
                "quadrant": "slowdown",
                "quadrant_source": "chain_fresh",
                "decision_validity": "dominance_baseline",
            },
            ("liquidity_led", "down", "expansionary_baseline"),
        ),
        (
            {
                **_decision(),
                "guard_level": "severe",
                "quadrant": "contraction",
            },
            ("aligned_contraction", "down", "defensive"),
        ),
        (
            {
                **_decision(quadrant_source="no_signal"),
                "quadrant": None,
                "decision_validity": "no_signal",
            },
            ("no_signal", "unavailable", "center"),
        ),
    ],
)
def test_public_categorical_taxonomy_uses_corrected_two_by_two_and_closed_books(
    decision: dict, expected: tuple[str, str, str]
) -> None:
    taxonomy = _materialization(decision=decision).public_taxonomy

    assert (taxonomy["taxonomy_state"], taxonomy["cycle_direction"], taxonomy["book"]) == expected


def test_selects_only_vintages_known_at_decision_cutoff_and_keeps_latest_revision() -> None:
    """A post-cutoff revision must not restate the February decision."""
    vintages = [row for row in _complete_vintages() if row["series_id"] != "INDPRO"]
    vintages.extend([
        _vintage("INDPRO", available_at="2026-01-31T23:59:59+00:00", value=10.0,
                 vintage_date="2026-01-31"),
        _vintage("INDPRO", available_at="2026-03-01T00:00:00+00:00", value=99.0,
                 vintage_date="2026-03-01"),
    ])

    materialization = _materialization(vintages=vintages)

    lineage = materialization.private_lineage["INDPRO"]
    assert lineage["selected_value"] == 10.0
    assert lineage["selected_available_at"] == "2026-01-31T23:59:59+00:00"
    assert _item(materialization, "INDPRO")["pit_state"] == "verified"


def test_revision_becomes_selectable_at_its_later_month_end_cutoff() -> None:
    """The same observation changes only when its later release is knowable."""
    later = dt.date(2026, 3, 31)
    vintages = [row for row in _complete_vintages() if row["series_id"] != "INDPRO"]
    vintages.extend([
        _vintage("INDPRO", available_at="2026-02-01T00:00:00+00:00", value=10.0,
                 vintage_date="2026-02-01"),
        _vintage("INDPRO", available_at="2026-03-01T00:00:00+00:00", value=99.0,
                 vintage_date="2026-03-01"),
    ])
    decision = _decision()
    decision["as_of"] = later

    materialization = _materialization(vintages=vintages, decision=decision)

    assert materialization.private_lineage["INDPRO"]["selected_value"] == 99.0
    assert materialization.private_lineage["INDPRO"]["selected_vintage_date"] == "2026-03-01"


def test_live_chain_delta_inherits_the_certified_macro_source_contract(monkeypatch) -> None:
    """The v03 delta shape is enriched before the v04 lineage builder reads it."""
    from src.workers import open_macro_v03_chain

    certified = [_vintage("INDPRO")]
    delta = {
        "series_id": "INDPRO",
        "observation_period": "2026-02-01",
        "vintage_date": "2026-03-01",
        "value": 10.0,
        "available_at": "2026-03-01T00:00:00+00:00",
        "revision_number": 1,
    }
    monkeypatch.setattr(open_macro_v03_chain, "verify_pack", lambda: {"input_pack_sha256": "b" * 64})
    monkeypatch.setattr(
        open_macro_v03_chain,
        "load_pack_inputs",
        lambda: (certified, [], dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc), dt.date(2026, 1, 31)),
    )
    monkeypatch.setattr(open_macro_v03_chain, "read_macro_delta", lambda conn, boundary: [delta])
    monkeypatch.setattr(open_macro_v03_chain, "read_eod_delta", lambda conn, boundary: [])

    macro_rows, _eod_rows, _pack_sha = evidence._certified_chain_inputs(object())

    assert macro_rows[-1] | {"source": "alfred", "source_spec_version": "macro_quadrant_us_v1.0"} == macro_rows[-1]


def test_v04_vintages_discard_periods_after_a_historical_decision_horizon() -> None:
    """A delayed materialization must not attach future observations to its decision."""
    decision = {
        **_decision(),
        "as_of": dt.date(2024, 2, 29),
        "updated_at": "2026-03-03T12:00:00+00:00",
    }
    historical_gdp = {
        **_vintage("GDP", available_at="2024-02-15T00:00:00+00:00", value=11.0),
        "observation_period": "2024-01-01",
    }
    future_gdp = {
        **_vintage("GDP", available_at="2025-01-15T00:00:00+00:00", value=99.0),
        "observation_period": "2024-03-01",
    }
    mirrors = _complete_mirrors()
    mirrors[1]["obs_date"] = "2024-01-01"

    materialization = _materialization(
        decision=decision,
        vintages=[*_complete_vintages(), historical_gdp, future_gdp],
        mirrors=mirrors,
    )

    assert materialization.private_lineage["GDP"]["selected_observation_period"] == "2024-01-01"
    assert materialization.private_lineage["GDP"]["selected_value"] == 11.0
    assert _item(materialization, "GDP")["evidence_state"] == "observed"


def test_missing_vintage_is_unavailable_while_non_vintage_sources_are_unverified() -> None:
    """The materializer must never promote a current mirror or dated price to PIT proof."""
    vintages = [row for row in _complete_vintages() if row["series_id"] != "MICH"]
    mirrors = [row for row in _complete_mirrors() if row["series_id"] != "GDP"]

    materialization = _materialization(vintages=vintages, mirrors=mirrors)

    assert _item(materialization, "MICH") | {"display_state": "unavailable", "pit_state": "unavailable"} == _item(materialization, "MICH")
    assert _item(materialization, "GDP") | {"display_state": "unavailable", "pit_state": "unavailable"} == _item(materialization, "GDP")
    for series_key in ("SPY", "MTSDS133FMS", "SUBLPDCILSLGNQ", "M2SL"):
        item = _item(materialization, series_key)
        assert item["display_state"] == "limited"
        assert item["pit_state"] == "unverified"


def test_public_items_have_the_fixed_catalogue_order_and_no_private_lineage_fields() -> None:
    """A public payload cannot leak values, vintages, timestamps, or decision internals."""
    materialization = evidence.build_materialization(
        _decision(quadrant_source="chain_carry", carry_age=2),
        vintages=_complete_vintages(),
        spy_rows=[{"ticker": "SPY", "date": "2025-12-30", "adj_close": 490.0}],
        mirror_rows=_complete_mirrors(),
        chain_seed={
            "as_of": dt.date(2025, 12, 31),
            "status": "valid",
            "basis": "certified_chain",
            "pack_sha256": "b" * 64,
        },
    )

    assert [item["series_key"] for item in materialization.public_items] == [
        "INDPRO", "PCEC96", "PAYEMS", "ACOGNO", "CPILFESL", "PPIFIS", "AHETPI", "MICH",
        "SPY", "MTSDS133FMS", "GDP", "SUBLPDCILSLGNQ", "M2SL",
    ]
    assert all(set(item) == PUBLIC_KEYS for item in materialization.public_items)
    assert materialization.private_decision["quadrant_source"] == "chain_carry"
    assert materialization.private_decision["carry_age"] == 2


def test_proxy_decision_publishes_only_its_seven_truthful_inputs() -> None:
    decision = {
        **_decision(quadrant_source="proxy"),
        "decision_basis": "bootstrap_replay",
    }
    materialization = _materialization(
        decision=decision,
        mirrors=_proxy_mirrors(),
        captures=[],
    )

    assert [item["series_key"] for item in materialization.public_items] == [
        "CFNAI", "CPIAUCSL", "SPY", "MTSDS133FMS", "GDP",
        "SUBLPDCILSLGNQ", "M2SL",
    ]
    assert not set(SEED_IDS) & {
        item["series_key"] for item in materialization.public_items
    }
    for series_key in PROXY_IDS:
        item = _item(materialization, series_key)
        assert item["role"] == "proxy_input"
        assert item["availability_state"] == "available"
        assert item["pit_state"] == "unverified"
        assert set(item) == PUBLIC_KEYS
        assert materialization.private_lineage[series_key]["selected_value"] is not None
    assert len(evidence._private_records(materialization)) == 7


@pytest.mark.parametrize("missing", PROXY_IDS)
def test_proxy_missing_identifies_the_unusable_transformed_arm(missing: str) -> None:
    decision = {
        **_decision(quadrant_source="proxy_missing"),
        "quadrant": None,
        "decision_basis": "bootstrap_replay",
    }
    materialization = _materialization(
        decision=decision,
        mirrors=_proxy_mirrors(missing=missing),
        captures=[],
    )

    assert _item(materialization, missing)["availability_state"] == "not_available"
    assert "selected_value" not in materialization.private_lineage[missing]
    assert "selected_observation_period" not in materialization.private_lineage[missing]
    companion = next(series_id for series_id in PROXY_IDS if series_id != missing)
    assert _item(materialization, companion)["availability_state"] == "available"
    assert {item["series_key"] for item in materialization.public_items} == {
        *PROXY_IDS, "SPY", *MIRROR_IDS,
    }


@pytest.mark.parametrize(
    "chain_seed",
    [
        None,
        {
            "as_of": dt.date(2026, 1, 31),
            "status": "invalid",
            "basis": "certified_chain",
            "pack_sha256": "b" * 64,
        },
        {
            "as_of": dt.date(2026, 1, 31),
            "status": "valid",
            "basis": "certified_chain",
            "pack_sha256": "not-a-sha256",
        },
    ],
)
def test_carried_chain_requires_valid_certified_seed_provenance(chain_seed) -> None:
    with pytest.raises(ValueError, match="valid certified seed provenance"):
        evidence.build_materialization(
            _decision(quadrant_source="chain_carry", carry_age=1),
            vintages=_complete_vintages(),
            spy_rows=[{"ticker": "SPY", "date": "2026-01-30", "adj_close": 490.0}],
            mirror_rows=_complete_mirrors(),
            chain_seed=chain_seed,
        )


def test_spy_market_leg_is_pinned_to_the_chain_seed_not_the_later_v04_month() -> None:
    """A carried decision cannot attach a post-seed market observation as lineage."""
    materialization = evidence.build_materialization(
        _decision(quadrant_source="chain_carry", carry_age=1),
        vintages=_complete_vintages(),
        spy_rows=[
            {"ticker": "SPY", "date": "2026-01-30", "adj_close": 490.0},
            {"ticker": "SPY", "date": "2026-02-27", "adj_close": 500.0},
        ],
        mirror_rows=_complete_mirrors(),
        chain_seed={
            "as_of": dt.date(2026, 1, 31),
            "status": "valid",
            "basis": "certified_chain",
            "pack_sha256": "b" * 64,
        },
    )

    assert materialization.private_lineage["SPY"]["selected_date"] == "2026-01-30"
    assert materialization.private_lineage["SPY"]["decision_cutoff"] == (
        "2026-01-31T00:00:00+00:00"
    )
    assert materialization.private_lineage["SPY"]["selected_value"] == 490.0
    spy_record = next(
        record for record in evidence._private_records(materialization)
        if record["series_key"] == "SPY"
    )
    assert spy_record["cutoff_at"] == dt.datetime(
        2026, 1, 31, tzinfo=dt.timezone.utc
    )


def test_missing_spy_retains_the_chain_cutoff() -> None:
    materialization = _materialization(spy_rows=[])

    assert materialization.private_lineage["SPY"] == {
        "source_kind": "eod_prices",
        "pit_state": "unavailable",
        "decision_cutoff": "2026-02-28T00:00:00+00:00",
    }
    spy_record = next(
        record for record in evidence._private_records(materialization)
        if record["series_key"] == "SPY"
    )
    assert spy_record["cutoff_at"] == dt.datetime(
        2026, 2, 28, tzinfo=dt.timezone.utc
    )


def test_spy_market_leg_ignores_later_non_spy_certified_market_rows() -> None:
    """The certified EOD pack contains several market tickers, not one shared series."""
    materialization = _materialization(
        spy_rows=[
            {"ticker": "SPY", "date": "2026-02-27", "adj_close": 500.0},
            {"ticker": "DBC", "date": "2026-02-28", "adj_close": 999.0},
        ]
    )

    assert materialization.private_lineage["SPY"]["selected_date"] == "2026-02-27"
    assert materialization.private_lineage["SPY"]["selected_value"] == 500.0


class _InputCursor:
    def __init__(self, conn: "_InputConn") -> None:
        self._conn = conn
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None) -> None:
        self._conn.calls.append((sql, deepcopy(params)))
        self._rows = self._conn.respond(sql, params or {})

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _InputConn:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def cursor(self):
        return _InputCursor(self)

    def respond(self, sql: str, params: dict) -> list[tuple]:
        if sql is evidence.DECISION_SQL:
            return [(
                DECISION_MONTH,
                "contained",
                False,
                "off",
                "full",
                "recovery",
                "chain_carry",
                2,
                "carried",
                "live",
                "a" * 64,
                dt.datetime(2026, 3, 3, 12, tzinfo=dt.timezone.utc),
            )]
        if sql is evidence.CHAIN_SEED_SQL:
            return [(
                dt.date(2025, 12, 31),
                "valid",
                "certified_chain",
                "b" * 64,
                dt.datetime(2026, 1, 31, tzinfo=dt.timezone.utc),
            )]
        if sql is evidence.VINTAGE_SQL:
            assert params["decision_cutoff"] == dt.datetime(
                2026, 3, 3, 12, tzinfo=dt.timezone.utc
            )
            return [
                tuple(row[key] for key in (
                    "series_id", "observation_period", "vintage_date", "value", "available_at",
                    "revision_number", "source", "source_spec_version",
                ))
                for row in _complete_vintages()
            ]
        if sql is evidence.SPY_SQL:
            return [("SPY", "2026-02-27", 500.0)]
        if sql is evidence.MIRROR_SQL:
            return [
                tuple(row[key] for key in (
                    "series_id", "obs_date", "value", "source", "created_at", "updated_at",
                ))
                for row in _complete_mirrors()
            ]
        if sql is evidence.CAPTURE_SQL:
            return [
                tuple(row[key] for key in (
                    "series_id", "series_digest_sha256", "row_count", "min_obs_date",
                    "max_obs_date", "producer_run_id", "global_input_digest_sha256",
                    "captured_at",
                ))
                for row in _captures(_complete_mirrors())
            ]
        if sql in {
            evidence.MIRROR_SQL,
            evidence.SPY_SQL,
        }:
            raise AssertionError(f"unexpected duplicate SQL: {sql!r}")
        if "SELECT obs_date, value FROM macro_data" in sql:
            return [(dt.date(2026, 1, 1), 1.0)]
        if "SELECT ticker, date, adj_close FROM eod_prices" in sql:
            tickers = params.get("tickers")
            if tickers:
                return [(ticker, dt.date(2026, 2, 27), 100.0) for ticker in tickers]
        if "SELECT as_of, quadrant, status, candidate_confidence" in sql:
            return [(DECISION_MONTH, "expansion", "valid", 0.8)]
        raise AssertionError(f"unexpected SQL: {sql!r}")


def test_acquired_connection_reads_certified_chain_inputs_and_private_carry_link(
    monkeypatch,
) -> None:
    """The materializer's PIT cutoff and carried source come from the decision row."""
    conn = _InputConn()
    pack_sha = "b" * 64
    certified_rows = [
        _vintage(
            series_id,
            available_at="2025-12-01T00:00:00+00:00",
            vintage_date="2025-12-01",
            value=float(index + 1),
        )
        for index, series_id in enumerate(SEED_IDS)
    ]
    spy_rows = [{"ticker": "SPY", "date": "2025-12-30", "adjusted_close": 490.0}]

    monkeypatch.setattr(
        evidence,
        "_certified_chain_inputs",
        lambda actual_conn: (certified_rows, spy_rows, pack_sha),
    )

    materialization = evidence.materialize_from_connection(conn, DECISION_MONTH)

    assert materialization.decision_month == "2026-02"
    assert materialization.private_decision["quadrant_source"] == "chain_carry"
    assert materialization.private_decision["carry_age"] == 2
    assert [sql for sql, _ in conn.calls[:4]] == [
        evidence.DECISION_SQL,
        evidence.CHAIN_SEED_SQL,
        evidence.VINTAGE_SQL,
        evidence.MIRROR_SQL,
    ]
    assert conn.calls[4][0] is evidence.CAPTURE_SQL
    assert len(conn.calls) == 5
    assert materialization.private_decision["carry_seed_fingerprint"] == pack_sha
    assert materialization.private_lineage["SPY"]["selected_date"] == "2025-12-30"


def test_chain_verification_requires_the_decision_seed_to_match_the_certified_pack(
    monkeypatch,
) -> None:
    """A plausible chain row cannot certify inputs from a different pack identity."""
    conn = _InputConn()
    monkeypatch.setattr(
        evidence,
        "_certified_chain_inputs",
        lambda actual_conn: (
            [
                _vintage(
                    series_id,
                    available_at="2025-12-01T00:00:00+00:00",
                    vintage_date="2025-12-01",
                    value=float(index + 1),
                )
                for index, series_id in enumerate(SEED_IDS)
            ],
            [{"ticker": "SPY", "date": "2025-12-30", "adjusted_close": 490.0}],
            "c" * 64,
        ),
    )

    materialization = evidence.materialize_from_connection(conn, DECISION_MONTH)

    assert all(_item(materialization, key)["pit_state"] == "unverified" for key in SEED_IDS)


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None) -> None:
        self._conn.calls.append((sql, deepcopy(params)))
        self._rows = self._conn.respond(sql, params or {})

    def executemany(self, sql, rows) -> None:
        for row in rows:
            self.execute(sql, row)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Transaction:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn
        self._before = None

    def __enter__(self):
        self._before = (
            deepcopy(self._conn.snapshots), deepcopy(self._conn.items),
            deepcopy(self._conn.taxonomies), deepcopy(self._conn.private_rows),
        )
        return self

    def __exit__(self, exc_type, *exc):
        if exc_type is not None:
            (self._conn.snapshots, self._conn.items, self._conn.taxonomies,
             self._conn.private_rows) = self._before
        return False


class _Conn:
    def __init__(
        self,
        *,
        fail_item_insert: bool = False,
        relation_presence: tuple[object | None, ...] | None = None,
    ) -> None:
        self.snapshots: dict[str, dict] = {}
        self.items: dict[str, list[dict]] = {}
        self.taxonomies: dict[str, dict] = {}
        self.private_rows: dict[str, list[dict]] = {}
        self.calls: list[tuple] = []
        self.fail_item_insert = fail_item_insert
        self.relation_presence = relation_presence or ("present",) * 4

    def cursor(self):
        return _Cursor(self)

    def transaction(self):
        return _Transaction(self)

    def respond(self, sql: str, params: dict) -> list[tuple]:
        if "to_regclass" in sql:
            return [self.relation_presence]
        if sql is evidence.EXISTING_SNAPSHOT_SQL:
            row = self.snapshots.get(params["decision_month"])
            return [] if row is None else [(row["publication_status"], row["coverage_state"])]
        if sql is evidence.EXISTING_ITEMS_SQL:
            return [tuple(row[key] for key in evidence.ITEM_COLUMNS)
                    for row in self.items.get(params["decision_month"], [])]
        if sql is evidence.EXISTING_TAXONOMY_SQL:
            row = self.taxonomies.get(params["decision_month"])
            return [] if row is None else [tuple(row[key] for key in evidence.CATEGORICAL_COLUMNS)]
        if sql is evidence.EXISTING_PRIVATE_SQL:
            return [tuple(row[key] for key in evidence.PRIVATE_COMPARE_COLUMNS)
                    for row in self.private_rows.get(params["decision_month"], [])]
        if sql is evidence.INSERT_SNAPSHOT_SQL:
            self.snapshots[params["decision_month"]] = dict(params)
            return []
        if sql is evidence.INSERT_PRIVATE_SQL:
            self.private_rows.setdefault(params["decision_month"], []).append(dict(params))
            return []
        if sql is evidence.INSERT_ITEM_SQL:
            if self.fail_item_insert:
                raise RuntimeError("item write failed")
            self.items.setdefault(params["decision_month"], []).append(dict(params))
            return []
        if sql is evidence.INSERT_TAXONOMY_SQL:
            self.taxonomies[params["decision_month"]] = dict(params)
            return []
        raise AssertionError(f"unexpected SQL: {sql!r}")


def test_exact_replay_is_a_no_op() -> None:
    """A later producer digest cannot fork already-published immutable evidence."""
    conn = _Conn()
    vintages = [
        _vintage(series_id, value=float(index + 1))
        for index, series_id in enumerate(FRED_IDS)
    ]
    mirrors = [
        _mirror(series_id, value=float(FRED_IDS.index(series_id) + 1))
        for series_id in MIRROR_IDS
    ]
    materialization = _materialization(vintages=vintages, mirrors=mirrors)
    replayed = _materialization(decision={
        **_decision(),
        "input_digest_sha256": "b" * 64,
        "run_id": "open-macro-v04-later-run",
        "updated_at": "2026-03-04T12:00:00+00:00",
    }, vintages=vintages, mirrors=mirrors)

    first_records = evidence._private_records(materialization)
    replay_records = evidence._private_records(replayed)
    assert all(_item(materialization, key)["pit_state"] == "verified" for key in MIRROR_IDS)
    assert all(_item(replayed, key)["pit_state"] == "verified" for key in MIRROR_IDS)
    assert [record["fingerprint"] for record in first_records] == [
        record["fingerprint"] for record in replay_records
    ]
    assert evidence._ordered_private(first_records) == evidence._ordered_private(replay_records)
    assert {record["decision_input_digest_sha256"] for record in first_records} == {"a" * 64}
    assert {record["decision_input_digest_sha256"] for record in replay_records} == {"b" * 64}

    assert evidence.publish(conn, materialization) == "published"
    calls_after_first = len(conn.calls)
    assert evidence.publish(conn, replayed) == "no_op"

    assert len(conn.calls) == calls_after_first + 5
    assert len(conn.snapshots) == 1
    assert len(conn.items["2026-02"]) == 13
    assert len(conn.private_rows["2026-02"]) == 13
    assert {
        record["decision_input_digest_sha256"]
        for record in conn.private_rows["2026-02"]
    } == {"a" * 64}
    assert conn.taxonomies["2026-02"] == materialization.public_taxonomy


def test_publication_outcome_handles_unbootstrapped_and_partial_evidence_relations() -> None:
    """A dry-run may plan a first publish, but never treats partial DDL as writable."""
    materialization = _materialization()
    absent = _Conn(relation_presence=(None, None, None, None))
    partial = _Conn(relation_presence=("present", None, None, None))

    assert evidence.publication_outcome(absent, materialization) == "would_publish"
    assert evidence.publication_outcome(partial, materialization) == "conflict"
    assert not any(
        sql in {
            evidence.INSERT_PRIVATE_SQL,
            evidence.INSERT_ITEM_SQL,
            evidence.INSERT_TAXONOMY_SQL,
            evidence.INSERT_SNAPSHOT_SQL,
        }
        for sql, _params in [*absent.calls, *partial.calls]
    )


def test_private_replay_comparison_canonicalizes_postgres_numeric_values() -> None:
    """A DB Decimal and the producer's same finite float describe one lineage value."""
    from decimal import Decimal

    expected = list(evidence._private_records(_materialization()))
    expected[0] = {**expected[0], "value": 4.6}
    stored = [dict(row) for row in expected]
    stored[0]["value"] = Decimal("4.6")

    assert evidence._ordered_private(expected) == evidence._ordered_private(stored)


def test_divergent_existing_snapshot_fails_closed() -> None:
    """A changed public status for an existing month is a conflict, not an overwrite."""
    conn = _Conn()
    materialization = _materialization()
    evidence.publish(conn, materialization)
    changed = _materialization(vintages=[row for row in _complete_vintages()
                                         if row["series_id"] != "MICH"])

    with pytest.raises(evidence.EvidenceConflictError, match="diverges"):
        evidence.publish(conn, changed)

    assert _item(type("M", (), {"public_items": conn.items["2026-02"]})(), "MICH")["pit_state"] == "verified"


def test_item_write_failure_rolls_back_the_header_and_all_items() -> None:
    """A partial public catalogue is never committed when one item write fails."""
    conn = _Conn(fail_item_insert=True)

    with pytest.raises(RuntimeError, match="item write failed"):
        evidence.publish(conn, _materialization())

    assert conn.snapshots == {}
    assert conn.items == {}


def test_v04_fred_arms_compare_vintages_only_within_the_producer_horizon(
) -> None:
    """Older ALFRED history is harmless, but every stored mirror row must match."""
    decision = {
        **_decision(),
        "decision_basis": "live",
        "input_digest_sha256": "a" * 64,
        "run_id": "open-macro-v04-test",
        "updated_at": "2026-03-03T12:00:00+00:00",
    }
    vintages = [_vintage(series_id, value=float(index + 1))
                for index, series_id in enumerate(FRED_IDS)]
    vintages.append({
        **_vintage(
            "GDP",
            available_at="2000-02-01T00:00:00+00:00",
            vintage_date="2000-02-01",
            value=5.0,
        ),
        "observation_period": "2000-01-01",
    })
    mirrors = [_mirror(series_id, value=float(FRED_IDS.index(series_id) + 1))
               for series_id in MIRROR_IDS]
    chain_seed = {
        "as_of": DECISION_MONTH,
        "status": "valid",
        "basis": "certified_chain",
        "pack_sha256": "b" * 64,
        "loaded_at": "2026-03-01T00:00:00+00:00",
    }

    verified = evidence.build_materialization(
        decision,
        vintages=vintages,
        spy_rows=[{"ticker": "SPY", "date": "2026-02-27", "adj_close": 500.0}],
        mirror_rows=mirrors,
        captures=_captures(mirrors),
        chain_seed=chain_seed,
    )
    assert _item(verified, "GDP")["pit_state"] == "verified"
    assert _item(verified, "GDP")["evidence_state"] == "observed"
    assert verified.private_lineage["GDP"]["selected_value"] == 10.0

    mirrors[1]["value"] = 999.0
    mismatch = evidence.build_materialization(
        decision,
        vintages=vintages,
        spy_rows=[{"ticker": "SPY", "date": "2026-02-27", "adj_close": 500.0}],
        mirror_rows=mirrors,
        captures=_captures([_mirror(series_id, value=float(FRED_IDS.index(series_id) + 1))
                            for series_id in MIRROR_IDS]),
        chain_seed=chain_seed,
    )
    assert _item(mismatch, "GDP")["pit_state"] == "unverified"
    assert _item(mismatch, "GDP")["evidence_state"] == "invalid"


def test_mirror_match_rejects_missing_or_extra_rows_inside_the_producer_horizon() -> None:
    mirror = [
        _mirror("GDP", value=1.0),
        {**_mirror("GDP", value=2.0), "obs_date": "2026-02-01"},
    ]
    first = _vintage("GDP", value=1.0)
    second = {
        **_vintage("GDP", value=2.0),
        "observation_period": "2026-02-01",
    }
    extra = {
        **_vintage("GDP", value=1.5),
        "observation_period": "2026-01-15",
    }

    assert evidence._mirror_matches_vintages(mirror, [first, second]) is True
    assert evidence._mirror_matches_vintages(mirror, [first]) is False
    assert evidence._mirror_matches_vintages(mirror, [first, extra, second]) is False


def test_live_direct_inputs_require_matching_immutable_captures() -> None:
    mirrors = _complete_mirrors()
    vintages = _complete_vintages() + [
        _vintage(series_id, value=float(index + 10))
        for index, series_id in enumerate(MIRROR_IDS)
    ]

    missing = _materialization(vintages=vintages, mirrors=mirrors, captures=[])
    assert all(_item(missing, key)["pit_state"] == "unverified" for key in MIRROR_IDS)

    mismatched = _captures(mirrors)
    mismatched[0] = {**mismatched[0], "series_digest_sha256": "f" * 64}
    materialization = _materialization(
        vintages=vintages, mirrors=mirrors, captures=mismatched)
    assert _item(materialization, MIRROR_IDS[0])["pit_state"] == "unverified"

    inconsistent_identity = _captures(mirrors)
    inconsistent_identity[0] = {
        **inconsistent_identity[0],
        "global_input_digest_sha256": "b" * 64,
    }
    materialization = _materialization(
        vintages=vintages, mirrors=mirrors, captures=inconsistent_identity
    )
    assert all(
        _item(materialization, key)["pit_state"] == "unverified"
        for key in MIRROR_IDS
    )


def test_immutable_capture_survives_later_mutable_decision_digest_rewrite() -> None:
    mirrors = _complete_mirrors()
    vintages = _complete_vintages() + [
        _vintage(series_id, value=float(index + 10))
        for index, series_id in enumerate(MIRROR_IDS)
    ]
    decision = {**_decision(), "input_digest_sha256": "b" * 64}

    materialization = _materialization(
        decision=decision,
        vintages=vintages,
        mirrors=mirrors,
        captures=_captures(mirrors),
    )

    assert all(
        _item(materialization, key)["pit_state"] == "verified"
        for key in MIRROR_IDS
    )
    assert {
        materialization.private_lineage[key]["producer_capture"][
            "global_input_digest_sha256"
        ]
        for key in MIRROR_IDS
    } == {"a" * 64}
    assert all(_item(materialization, key)["pit_state"] == "verified"
               for key in MIRROR_IDS[1:])

    bad_horizon = _captures(mirrors)
    bad_horizon[0] = {**bad_horizon[0], "row_count": 2}
    materialization = _materialization(
        vintages=vintages, mirrors=mirrors, captures=bad_horizon
    )
    assert _item(materialization, MIRROR_IDS[0])["pit_state"] == "unverified"


def test_bootstrap_direct_inputs_cannot_be_verified_even_with_matching_capture() -> None:
    decision = {**_decision(), "decision_basis": "bootstrap_replay"}
    materialization = _materialization(decision=decision)
    assert all(_item(materialization, key)["pit_state"] == "unverified"
               for key in MIRROR_IDS)


def test_carried_chain_uses_its_seed_cutoff_while_v04_uses_decision_creation_time() -> None:
    """The two producer legs keep their distinct private cutoffs and no timing leaks."""
    decision = {
        **_decision(quadrant_source="chain_carry", carry_age=1),
        "decision_basis": "live",
        "input_digest_sha256": "a" * 64,
        "run_id": "open-macro-v04-test",
        "updated_at": "2026-03-03T12:00:00+00:00",
    }
    chain_seed = {
        "as_of": dt.date(2026, 1, 31),
        "status": "valid",
        "basis": "certified_chain",
        "pack_sha256": "b" * 64,
        "loaded_at": "2026-02-27T00:00:00+00:00",
    }
    vintages = [
        _vintage(series_id, available_at="2026-01-31T00:00:00+00:00")
        for series_id in SEED_IDS
    ] + [
        _vintage(series_id, available_at="2026-03-01T00:00:00+00:00")
        for series_id in MIRROR_IDS
    ]

    materialization = evidence.build_materialization(
        decision,
        vintages=vintages,
        spy_rows=[{"ticker": "SPY", "date": "2026-01-30", "adj_close": 490.0}],
        mirror_rows=[_mirror(series_id, value=1.0) for series_id in MIRROR_IDS],
        captures=_captures([_mirror(series_id, value=1.0) for series_id in MIRROR_IDS]),
        chain_seed=chain_seed,
    )

    assert materialization.private_decision["decision_cutoff"] == "2026-03-03T12:00:00+00:00"
    assert materialization.private_decision["carry_seed_as_of"] == "2026-01-31"
    assert materialization.private_decision["carry_seed_cutoff"] == "2026-01-31T00:00:00+00:00"
    for series_key in (*SEED_IDS, "SPY"):
        item = _item(materialization, series_key)
        assert item["evidence_state"] == "carried"
        assert item["freshness_state"] == "stale"
    for series_key in MIRROR_IDS:
        item = _item(materialization, series_key)
        assert item["evidence_state"] == "observed"
        assert item["freshness_state"] == "current"
    private_records = {
        record["series_key"]: record for record in evidence._private_records(materialization)
    }
    for series_key in (*SEED_IDS, "SPY"):
        assert private_records[series_key]["carry_seed_decision_month"] == "2026-01"
        assert private_records[series_key]["carry_seed_fingerprint"] == "b" * 64
    for series_key in MIRROR_IDS:
        assert private_records[series_key]["carry_seed_decision_month"] is None
        assert private_records[series_key]["carry_seed_fingerprint"] is None
    for item in materialization.public_items:
        assert not any("cutoff" in key or "seed" in key for key in item)


def test_publication_persists_private_lineage_before_opening_public_header() -> None:
    """One transaction owns private audit rows, 13 public items, and final open state."""
    conn = _Conn()
    materialization = _materialization()

    evidence.publish(conn, materialization)

    writes = [sql for sql, _ in conn.calls]
    private_indexes = [i for i, sql in enumerate(writes) if sql is evidence.INSERT_PRIVATE_SQL]
    item_indexes = [i for i, sql in enumerate(writes) if sql is evidence.INSERT_ITEM_SQL]
    assert len(private_indexes) == 13
    assert len(item_indexes) == 13
    header_indexes = [i for i, sql in enumerate(writes) if sql is evidence.INSERT_SNAPSHOT_SQL]
    taxonomy_indexes = [i for i, sql in enumerate(writes) if sql is evidence.INSERT_TAXONOMY_SQL]
    assert len(header_indexes) == 1
    assert len(taxonomy_indexes) == 1
    assert max(private_indexes) < min(item_indexes) < taxonomy_indexes[0] < header_indexes[0]
    assert conn.snapshots[materialization.decision_month]["publication_status"] == "open"
    assert conn.taxonomies[materialization.decision_month] == materialization.public_taxonomy


def test_replay_conflicts_when_private_lineage_diverges_even_if_public_status_is_same() -> None:
    """Immutable private lineage, not only the status projection, defines idempotence."""
    conn = _Conn()
    original = _materialization()
    evidence.publish(conn, original)
    changed = _materialization()
    changed.private_lineage["INDPRO"]["selected_value"] = 12345.0

    with pytest.raises(evidence.EvidenceConflictError, match="private lineage"):
        evidence.publish(conn, changed)


def test_worker_run_uses_an_independent_lock_and_never_logs_private_values(monkeypatch) -> None:
    """The dispatcher entry point publishes counts/statuses only, not lineage values."""
    import contextlib

    monkeypatch.setenv("OPEN_MACRO_V04_PIT_EVIDENCE_ENABLED", "1")

    class _RunConn(_Conn):
        def __init__(self):
            super().__init__()
            self.commits = 0

        def commit(self):
            self.commits += 1

        def close(self):
            pass

    conn = _RunConn()

    @contextlib.contextmanager
    def _acquired(actual_conn, lock_id):
        assert lock_id == evidence.LOCK_OPEN_MACRO_V04_PIT_EVIDENCE
        yield True

    monkeypatch.setattr(evidence, "connect", lambda dsn: conn)
    monkeypatch.setattr(evidence, "advisory_lock", _acquired)
    monkeypatch.setattr(evidence, "ensure_schema", lambda actual_conn: None)
    monkeypatch.setattr(evidence, "pin_search_path", lambda actual_conn: None)
    monkeypatch.setattr(evidence, "begin_consistent_read", lambda actual_conn: None)
    monkeypatch.setattr(
        evidence,
        "materialize_from_connection",
        lambda actual_conn, decision_as_of: _materialization(),
    )

    result = evidence.run("postgresql://example", calc_date="2026-02-28")

    assert result == {
        "status": "published",
        "decision_month": "2026-02",
        "coverage_state": "partial",
        "components": 13,
    }
    assert "value" not in str(result).lower()
    assert "cutoff" not in str(result).lower()
    assert conn.commits == 1


def test_worker_default_month_uses_the_producer_new_york_date(monkeypatch) -> None:
    """Just after UTC midnight, the producer is still on the prior New York date."""
    import contextlib
    import importlib
    import types

    monkeypatch.setenv("OPEN_MACRO_V04_PIT_EVIDENCE_ENABLED", "1")
    importlib.import_module("src.workers.open_macro_v04")
    instant = dt.datetime(2026, 3, 1, 0, 30, tzinfo=dt.timezone.utc)

    class _FrozenDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

    @contextlib.contextmanager
    def _acquired(conn, lock_id):
        yield True

    captured: dict[str, dt.date] = {}
    conn = types.SimpleNamespace(commit=lambda: None, close=lambda: None)
    materialization = types.SimpleNamespace(
        decision_month="2026-01",
        header={"coverage_state": "partial"},
        public_items=tuple(range(13)),
    )
    monkeypatch.setattr(evidence.dt, "datetime", _FrozenDateTime)
    monkeypatch.setattr(evidence, "connect", lambda dsn: conn)
    monkeypatch.setattr(evidence, "advisory_lock", _acquired)
    monkeypatch.setattr(evidence, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(evidence, "pin_search_path", lambda conn: None)
    monkeypatch.setattr(evidence, "begin_consistent_read", lambda conn: None)
    monkeypatch.setattr(
        evidence,
        "materialize_from_connection",
        lambda conn, decision_as_of: captured.setdefault("decision_as_of", decision_as_of)
        and materialization,
    )
    monkeypatch.setattr(evidence, "publish", lambda conn, materialization: "published")

    evidence.run("postgresql://example")

    assert captured["decision_as_of"] == dt.date(2026, 1, 31)


def test_worker_holds_its_lock_before_schema_bootstrap(monkeypatch) -> None:
    import contextlib

    monkeypatch.setenv("OPEN_MACRO_V04_PIT_EVIDENCE_ENABLED", "1")

    events: list[str] = []

    class _RunConn(_Conn):
        def commit(self) -> None:
            events.append("commit")

        def close(self) -> None:
            pass

    @contextlib.contextmanager
    def _acquired(conn, lock_id):
        events.append("lock")
        yield True

    monkeypatch.setattr(evidence, "connect", lambda dsn: _RunConn())
    monkeypatch.setattr(evidence, "advisory_lock", _acquired)
    def _schema(conn) -> None:
        events.append("schema")
        conn.commit()

    def _path(conn) -> None:
        events.append("path")
        conn.commit()

    monkeypatch.setattr(evidence, "ensure_schema", _schema)
    monkeypatch.setattr(evidence, "pin_search_path", _path)
    monkeypatch.setattr(
        evidence, "begin_consistent_read", lambda conn: events.append("snapshot")
    )
    monkeypatch.setattr(
        evidence, "materialize_from_connection", lambda conn, date: _materialization()
    )
    monkeypatch.setattr(evidence, "publish", lambda conn, materialization: "published")

    evidence.run("postgresql://example", calc_date="2026-02-28")

    assert events[:6] == ["lock", "path", "commit", "schema", "commit", "snapshot"]


def test_worker_materializer_is_disabled_without_explicit_evidence_enable(monkeypatch) -> None:
    monkeypatch.delenv("OPEN_MACRO_V04_PIT_EVIDENCE_ENABLED", raising=False)
    monkeypatch.setattr(evidence, "connect", lambda dsn: pytest.fail("must not connect while disabled"))

    assert evidence.run("postgresql://example", calc_date="2026-02-28") == {"status": "disabled"}


def test_registered_lock_is_unique_and_dispatcher_names_the_evidence_worker() -> None:
    from pathlib import Path
    from src import db

    lock_ids = [
        value
        for name, value in vars(db).items()
        if name.startswith("LOCK_") and isinstance(value, int)
    ]
    assert db.LOCK_OPEN_MACRO_V04_PIT_EVIDENCE == 900_222
    assert lock_ids.count(db.LOCK_OPEN_MACRO_V04_PIT_EVIDENCE) == 1
    runner = Path("src/run_worker.py").read_text(encoding="utf-8")
    assert "|open_macro_v04_pit_evidence" in runner


def test_dispatcher_passes_calc_date_to_the_evidence_worker(monkeypatch, capsys) -> None:
    """The shared Railway entry point can target one published decision month."""
    import types
    from src import run_worker

    called: dict[str, object] = {}
    fake_worker = types.SimpleNamespace(
        run=lambda dsn, *, calc_date=None: called.update(
            dsn=dsn, calc_date=calc_date
        ) or {"status": "published", "components": 13}
    )
    monkeypatch.setenv("WORKER", "open_macro_v04_pit_evidence")
    monkeypatch.setenv("WORKER_CALC_DATE", "2026-02-28")
    monkeypatch.delenv("WORKER_LIMIT", raising=False)
    monkeypatch.setattr(run_worker.importlib, "import_module", lambda name: fake_worker)
    monkeypatch.setattr(run_worker, "resolve_dsn", lambda: "postgresql://worker")

    run_worker.main()

    assert called == {
        "dsn": "postgresql://worker",
        "calc_date": "2026-02-28",
    }
    assert '"worker": "open_macro_v04_pit_evidence"' in capsys.readouterr().out


def test_dry_run_backfill_builds_without_publishing_and_apply_is_explicit(monkeypatch) -> None:
    """Historical reconstruction is report-only unless the caller opts into apply."""
    import contextlib
    from scripts import backfill_open_macro_v04_pit_evidence as backfill

    class _BackfillConn:
        def __init__(self) -> None:
            self.closed = False
            self.commits = 0

        def commit(self) -> None:
            self.commits += 1

        def close(self) -> None:
            self.closed = True

    conn = _BackfillConn()
    published: list[str] = []
    monkeypatch.setattr(backfill, "connect", lambda dsn: conn)
    events: list[str] = []
    monkeypatch.setattr(
        backfill.evidence, "pin_search_path", lambda actual_conn: events.append("path")
    )
    snapshot_calls: list[object] = []
    monkeypatch.setattr(
        backfill.evidence,
        "begin_consistent_read",
        lambda actual_conn: (events.append("snapshot"), snapshot_calls.append(actual_conn)),
    )
    schema_calls: list[object] = []
    monkeypatch.setattr(
        backfill.evidence,
        "ensure_schema",
        lambda actual_conn: schema_calls.append(actual_conn),
    )

    @contextlib.contextmanager
    def _acquired(actual_conn, lock_id):
        assert lock_id == evidence.LOCK_OPEN_MACRO_V04_PIT_EVIDENCE
        events.append("lock")
        yield True

    monkeypatch.setattr(backfill.evidence, "advisory_lock", _acquired)
    monkeypatch.setattr(
        backfill,
        "decision_months",
        lambda actual_conn, start, end: [
            dt.date(2026, 1, 31),
            dt.date(2026, 2, 28),
        ],
    )
    monkeypatch.setattr(
        backfill.evidence,
        "materialize_from_connection",
        lambda actual_conn, month: _materialization(),
    )
    monkeypatch.setattr(
        backfill.evidence,
        "publication_outcome",
        lambda actual_conn, materialization: "would_publish",
    )
    monkeypatch.setattr(
        backfill.evidence,
        "publish",
        lambda actual_conn, materialization: (
            published.append(materialization.decision_month) or "published"
        ),
    )

    dry = backfill.run(
        "postgresql://example",
        start=dt.date(2026, 1, 31),
        end=dt.date(2026, 2, 28),
    )
    assert dry == {
        "mode": "dry_run",
        "decisions": 2,
        "complete": 0,
        "partial": 2,
        "unavailable": 0,
        "would_publish": 2,
        "no_op": 0,
        "conflict": 0,
    }
    assert published == []
    assert conn.commits == 0
    assert schema_calls == []
    assert snapshot_calls == [conn]
    # pg_try_advisory_lock is a query.  Dry runs set RR first so lock acquisition
    # becomes the snapshot's first statement rather than invalidating SET TRANSACTION.
    assert events[:3] == ["path", "snapshot", "lock"]

    applied = backfill.run(
        "postgresql://example",
        start=dt.date(2026, 1, 31),
        end=dt.date(2026, 2, 28),
        apply=True,
    )
    assert applied["mode"] == "apply"
    assert applied["published"] == 2
    assert published == ["2026-02", "2026-02"]
    # One ends lock acquisition, one ends the month-list transaction, and one
    # commits each immutable month before its next repeatable-read snapshot.
    assert conn.commits == 4
    assert schema_calls == [conn]
    assert snapshot_calls == [conn, conn, conn]
    assert events[3:5] == ["path", "lock"]


def test_dry_run_backfill_classifies_immutable_conflicts_without_writing(monkeypatch) -> None:
    """Planning must expose a divergent immutable month before an apply can reach it."""
    import contextlib
    from scripts import backfill_open_macro_v04_pit_evidence as backfill

    class _DryRunConn(_Conn):
        def close(self) -> None:
            pass

    conn = _DryRunConn()
    evidence.publish(conn, _materialization())
    calls_before_dry_run = len(conn.calls)
    divergent = _materialization()
    divergent.private_lineage["INDPRO"]["selected_value"] = 12345.0

    @contextlib.contextmanager
    def _acquired(actual_conn, lock_id):
        yield True

    monkeypatch.setattr(backfill, "connect", lambda dsn: conn)
    monkeypatch.setattr(backfill.evidence, "pin_search_path", lambda actual_conn: None)
    monkeypatch.setattr(backfill.evidence, "begin_consistent_read", lambda actual_conn: None)
    monkeypatch.setattr(backfill.evidence, "advisory_lock", _acquired)
    monkeypatch.setattr(
        backfill,
        "decision_months",
        lambda actual_conn, start, end: [dt.date(2026, 2, 28)],
    )
    monkeypatch.setattr(
        backfill.evidence,
        "materialize_from_connection",
        lambda actual_conn, month: divergent,
    )

    result = backfill.run(
        "postgresql://example",
        start=dt.date(2026, 2, 28),
        end=dt.date(2026, 2, 28),
    )

    assert result == {
        "mode": "dry_run",
        "decisions": 1,
        "complete": 0,
        "partial": 1,
        "unavailable": 0,
        "would_publish": 0,
        "no_op": 0,
        "conflict": 1,
    }
    assert len(conn.calls) == calls_before_dry_run + 5


def test_backfill_uses_the_same_advisory_lock_as_the_runtime_worker(monkeypatch) -> None:
    """Backfill and cron cannot concurrently race one immutable decision month."""
    import contextlib
    from scripts import backfill_open_macro_v04_pit_evidence as backfill

    class _BackfillConn:
        def close(self) -> None:
            pass

    observed: list[int] = []

    @contextlib.contextmanager
    def _busy(actual_conn, lock_id):
        observed.append(lock_id)
        yield False

    monkeypatch.setattr(backfill, "connect", lambda dsn: _BackfillConn())
    monkeypatch.setattr(backfill.evidence, "pin_search_path", lambda conn: None)
    monkeypatch.setattr(backfill.evidence, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(backfill.evidence, "begin_consistent_read", lambda conn: None)
    monkeypatch.setattr(backfill.evidence, "advisory_lock", _busy)

    assert backfill.run(
        "postgresql://example",
        start=dt.date(2026, 1, 31),
        end=dt.date(2026, 2, 28),
    ) == {"mode": "dry_run", "status": "lock_busy"}
    assert observed == [evidence.LOCK_OPEN_MACRO_V04_PIT_EVIDENCE]


def test_private_publication_refuses_missing_decision_identity() -> None:
    """Private lineage never substitutes sentinel provenance for a real decision."""
    materialization = _materialization()
    materialization.private_decision["decision_created_at"] = None

    with pytest.raises(ValueError, match="decision created_at"):
        evidence.publish(_Conn(), materialization)


@pytest.mark.parametrize("digest", [None, "A" * 64, "a" * 63, "g" * 64])
def test_private_publication_refuses_invalid_input_digest(digest: str | None) -> None:
    materialization = _materialization()
    materialization.private_decision["decision_input_digest_sha256"] = digest

    with pytest.raises(ValueError, match="input digest"):
        evidence.publish(_Conn(), materialization)


@pytest.mark.skipif(
    not os.getenv("TEST_POSTGRES_DSN"),
    reason="set TEST_POSTGRES_DSN for the real PostgreSQL evidence contract gate",
)
def test_real_postgres_atomic_snapshot_and_public_private_grant_boundary() -> None:
    """The DDL and publisher work together, including deferred FK and role grants."""
    import psycopg

    dsn = os.environ["TEST_POSTGRES_DSN"]
    with psycopg.connect(dsn, autocommit=True) as admin:
        with admin.cursor() as cur:
            for relation in (
                "open_macro_v04_categorical_taxonomy",
                "open_macro_v04_evidence_items",
                "open_macro_v04_evidence_snapshots",
                "open_macro_v04_pit_evidence",
            ):
                cur.execute(f"TRUNCATE TABLE {relation} CASCADE")

    with psycopg.connect(dsn) as conn:
        assert evidence.publish(conn, _materialization()) == "published"
        assert evidence.publish(conn, _materialization()) == "no_op"

    with psycopg.connect(dsn, autocommit=True) as public_conn:
        with public_conn.cursor() as cur:
            cur.execute("SET ROLE app_runtime")
            cur.execute(
                "SELECT has_table_privilege(current_user, 'open_macro_v04_evidence_snapshots', 'SELECT'), "
                "has_table_privilege(current_user, 'open_macro_v04_evidence_items', 'SELECT'), "
                "has_table_privilege(current_user, 'open_macro_v04_categorical_taxonomy', 'SELECT'), "
                "has_table_privilege(current_user, 'open_macro_v04_pit_evidence', 'SELECT'), "
                "has_table_privilege(current_user, 'open_macro_v04_decisions', 'SELECT'), "
                "has_table_privilege(current_user, 'open_macro_v04_allocations', 'SELECT')"
            )
            assert cur.fetchone() == (True, True, True, False, False, False)
            cur.execute(
                "SELECT publication_status, coverage_state "
                "FROM open_macro_v04_evidence_snapshots WHERE decision_month = '2026-02'"
            )
            assert cur.fetchone() == ("open", "partial")
            cur.execute(
                "SELECT count(*) FROM open_macro_v04_evidence_items "
                "WHERE decision_month = '2026-02'"
            )
            assert cur.fetchone() == (13,)
            cur.execute(
                "SELECT taxonomy_state, quadrant, cycle_direction, book "
                "FROM open_macro_v04_categorical_taxonomy WHERE decision_month = '2026-02'"
            )
            assert cur.fetchone() == ("cycle_led", "recovery", "up", "center")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cur.execute("SELECT series_key FROM open_macro_v04_pit_evidence")


@pytest.mark.skipif(
    not os.getenv("TEST_POSTGRES_DSN"),
    reason="set TEST_POSTGRES_DSN for the real PostgreSQL evidence contract gate",
)
def test_real_postgres_non_integer_numeric_replay_is_a_no_op() -> None:
    """PostgreSQL NUMERIC returns Decimal; a canonical non-integer replay must not conflict."""
    import psycopg

    dsn = os.environ["TEST_POSTGRES_DSN"]
    with psycopg.connect(dsn, autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("TRUNCATE TABLE open_macro_v04_evidence_snapshots CASCADE")
            cur.execute("TRUNCATE TABLE open_macro_v04_pit_evidence")
    vintages = _complete_vintages()
    vintages[0]["value"] = 4.6
    materialization = _materialization(vintages=vintages)
    with psycopg.connect(dsn) as conn:
        assert evidence.publish(conn, materialization) == "published"
        assert evidence.publish(conn, materialization) == "no_op"


@pytest.mark.skipif(
    not os.getenv("TEST_POSTGRES_DSN"),
    reason="set TEST_POSTGRES_DSN for the real PostgreSQL evidence contract gate",
)
def test_real_postgres_worker_reapply_fails_closed_until_owner_repairs_producer_acl() -> None:
    """A non-owner never blesses broad categorical-cutover grants by skipping them."""
    import psycopg

    dsn = os.environ["TEST_POSTGRES_DSN"]
    ddl = evidence._SCHEMA.read_text(encoding="utf-8")
    with psycopg.connect(dsn, autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute(
                "SELECT bool_and(c.relowner = r.oid) OR bool_or(r.rolsuper) "
                "FROM pg_class c CROSS JOIN pg_roles r "
                "WHERE r.rolname = current_user AND c.oid IN ("
                "'open_macro_v04_decisions'::regclass, "
                "'open_macro_v04_allocations'::regclass)"
            )
            assert cur.fetchone() == (True,)
            # Start from a valid admin bootstrap, then reproduce a broad/default
            # producer-table grant that Railway may have installed later.
            cur.execute(ddl)
            cur.execute(
                "GRANT SELECT ON TABLE open_macro_v04_decisions, "
                "open_macro_v04_allocations TO app_runtime, app_analytics_ro"
            )
            try:
                cur.execute("SET ROLE worker_writer")
                with pytest.raises(
                    psycopg.errors.InsufficientPrivilege,
                    match="producer ACLs are unsafe; owner bootstrap required",
                ):
                    cur.execute(ddl)
            finally:
                # SET ROLE is session state and survives a failed autocommit
                # statement. Always restore the admin role, then restore safe ACLs.
                cur.execute("RESET ROLE")
                cur.execute(ddl)

            cur.execute(
                "SELECT role_name, relation_name, "
                "has_table_privilege(role_name, relation_name, 'SELECT') "
                "FROM unnest(ARRAY['app_runtime', 'app_analytics_ro']) AS roles(role_name) "
                "CROSS JOIN unnest(ARRAY['open_macro_v04_decisions', "
                "'open_macro_v04_allocations']) AS relations(relation_name)"
            )
            assert all(not has_select for _, _, has_select in cur.fetchall())

            cur.execute("SET ROLE worker_writer")
            try:
                cur.execute(ddl)
            finally:
                cur.execute("RESET ROLE")


@pytest.mark.skipif(
    not os.getenv("TEST_POSTGRES_DSN"),
    reason="set TEST_POSTGRES_DSN for the real PostgreSQL evidence contract gate",
)
def test_real_postgres_admin_bootstrap_transfers_evidence_ownership_to_worker_writer() -> None:
    """An admin bootstrap leaves the complete evidence DDL re-runnable by its worker."""
    import psycopg

    dsn = os.environ["TEST_POSTGRES_DSN"]
    ddl = evidence._SCHEMA.read_text(encoding="utf-8")
    relations = (
        "open_macro_v04_categorical_taxonomy",
        "open_macro_v04_evidence_items",
        "open_macro_v04_evidence_snapshots",
        "open_macro_v04_pit_evidence",
    )
    functions = (
        "open_macro_v04_pit_evidence_reject_mutation",
        "open_macro_v04_evidence_snapshots_insert_guard",
        "open_macro_v04_evidence_items_insert_guard",
        "open_macro_v04_evidence_items_reject_mutation",
        "open_macro_v04_evidence_snapshots_reject_mutation",
        "open_macro_v04_categorical_taxonomy_reject_mutation",
    )
    with psycopg.connect(dsn, autocommit=True) as admin:
        notices: list[str] = []
        admin.add_notice_handler(
            lambda diagnostic: notices.append(diagnostic.message_primary or "")
        )
        with admin.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS open_macro_v04_categorical_taxonomy CASCADE")
            cur.execute("DROP TABLE IF EXISTS open_macro_v04_evidence_items CASCADE")
            cur.execute("DROP TABLE IF EXISTS open_macro_v04_evidence_snapshots CASCADE")
            cur.execute("DROP TABLE IF EXISTS open_macro_v04_pit_evidence CASCADE")
            cur.execute(ddl)
            cur.execute(
                "SELECT relname, pg_get_userbyid(relowner) FROM pg_class "
                "WHERE relname = ANY(%s) AND relkind = 'r' ORDER BY relname",
                (list(relations),),
            )
            assert dict(cur.fetchall()) == {relation: "worker_writer" for relation in sorted(relations)}
            cur.execute(
                "SELECT proname, pg_get_userbyid(proowner) FROM pg_proc "
                "WHERE pronamespace = 'public'::regnamespace AND proname = ANY(%s) "
                "ORDER BY proname",
                (list(functions),),
            )
            assert dict(cur.fetchall()) == {function: "worker_writer" for function in sorted(functions)}
            notices.clear()
            cur.execute("SET ROLE worker_writer")
            cur.execute(ddl)
            assert not any(
                "must be owner" in notice.lower() or "permission denied" in notice.lower()
                for notice in notices
            )
            cur.execute("RESET ROLE")
