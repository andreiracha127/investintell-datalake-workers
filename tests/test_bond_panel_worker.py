"""Focused orchestration tests for the daily bond-panel stage."""
from __future__ import annotations

import contextlib
import json
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from src.bonds.panel_materializer import MaterializationResult
from src.bonds.distribution_series import DistributionSeriesError
from src.workers import bond_panel


REG_S_SNAPSHOT_ID = "7d2b63ce-63a0-534b-9741-d10242d399ad"
REG_S_LINEAGE = {
    "distribution_rule": "reg_s",
    "distribution_mapping_snapshot_id": REG_S_SNAPSHOT_ID,
}


def test_panel_refuses_to_bootstrap_a_two_month_history(monkeypatch) -> None:
    """A daily delta needs a validated compatible parent; it never self-bases."""
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: None)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setenv("CODE_REVISION", "test")

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "gate_failed"
    assert outcome["reason"] == "panel_no_parent"
    assert outcome["aborted"] is True


def test_current_parent_reads_declared_and_direct_surface_months() -> None:
    row = (
        "92740098-1571-559d-9fb3-119de8321754",
        None,
        date(2002, 7, 1),
        date(2026, 6, 1),
        None,
        date(2026, 6, 1),
        date(2025, 3, 1),
        REG_S_LINEAGE,
    )

    class Connection:
        def execute(self, _sql, _params=()):
            return type("Result", (), {"fetchone": lambda self: row})()

    parent = bond_panel._current_parent(Connection())

    assert parent == {
        "publication_id": row[0],
        "parent_publication_id": None,
        "first_month": row[2],
        "last_closed_month": row[3],
        "open_month": None,
        "snapshot_max_month": row[5],
        "returns_max_month": row[6],
        "source_lineage": REG_S_LINEAGE,
    }


def test_db_loader_uses_reg_s_execution_sources_and_reference_terms(monkeypatch) -> None:
    sql_seen: list[tuple[str, tuple[object, ...]]] = []
    resolver_calls: list[dict[str, object]] = []

    def frame(_conn, sql, _params=()):
        sql_seen.append((sql, _params))
        if sql.strip().startswith("SELECT upper(btrim(cusip9)) AS reference_cusip9"):
            return pd.DataFrame({"reference_cusip9": ["REFERENCE1", "UNMAPPED1"]})
        if sql.startswith("SELECT DISTINCT source_sha256"):
            return pd.DataFrame({"source_sha256": ["a" * 64]})
        if "FROM bond_rating_static r JOIN mapping m" in sql:
            return pd.DataFrame({
                "cusip9": ["037833100"],
                "rating_bucket": ["A"],
                "rating_as_of_month": [date(2026, 7, 1)],
                "rating_state": ["static_current"],
                "rating_reason": ["static_backfill"],
                "source_sha256": ["a" * 64],
            })
        return pd.DataFrame()

    def resolve(_conn, *, snapshot_id, as_of, reference_cusip9s):
        resolver_calls.append(
            {
                "snapshot_id": snapshot_id,
                "as_of": as_of,
                "reference_cusip9s": list(reference_cusip9s),
            }
        )
        execution_cusip9 = "CLOSEDREG" if as_of == date(2026, 7, 31) else "OPENREGS1"
        return SimpleNamespace(
            resolutions={
                "REFERENCE1": SimpleNamespace(
                    reference_cusip9="REFERENCE1",
                    reg_s_cusip9=execution_cusip9,
                    decision_id=f"decision-{as_of.isoformat()}",
                )
            },
            reason_by_reference={"UNMAPPED1": "no_supported_reg_s_cusip"},
        )

    monkeypatch.setattr(bond_panel, "_frame", frame)
    monkeypatch.setattr(bond_panel, "resolve_reg_s_cusip_map_from_db", resolve)

    _inputs, lineage = bond_panel._load_inputs(
        object(),
        pd.Timestamp("2026-07-01"),
        pd.Timestamp("2026-08-01"),
        date(2026, 8, 8),
        mapping_snapshot_id=REG_S_SNAPSHOT_ID,
        structural_publication_id="92740098-1571-559d-9fb3-119de8321754",
        structural_month=date(2026, 6, 1),
    )

    assert resolver_calls == [
        {
            "snapshot_id": REG_S_SNAPSHOT_ID,
            "as_of": date(2026, 7, 31),
            "reference_cusip9s": ["REFERENCE1", "UNMAPPED1"],
        },
        {
            "snapshot_id": REG_S_SNAPSHOT_ID,
            "as_of": date(2026, 8, 8),
            "reference_cusip9s": ["REFERENCE1", "UNMAPPED1"],
        },
    ]
    mapped_queries = [sql for sql, _params in sql_seen if "jsonb_to_recordset" in sql]
    mapped_payloads = [
        json.loads(_params[0])
        for sql, _params in sql_seen
        if "jsonb_to_recordset" in sql
    ]
    assert len(mapped_queries) == 5
    assert all(
        payload
        == [
            {
                "decision_id": "decision-2026-07-31",
                "execution_cusip9": "CLOSEDREG",
                "month": "2026-07-01",
                "reference_cusip9": "REFERENCE1",
            },
            {
                "decision_id": "decision-2026-08-08",
                "execution_cusip9": "OPENREGS1",
                "month": "2026-08-01",
                "reference_cusip9": "REFERENCE1",
            },
        ]
        for payload in mapped_payloads
    )
    assert all(
        "reference_cusip9 text, execution_cusip9 text, decision_id text, month date" in sql
        for sql in mapped_queries
    )
    assert all("execution_cusip9" in sql for sql in mapped_queries)
    assert any("FROM bond_observation_daily o JOIN mapping m" in sql for sql in mapped_queries)
    assert any("date_trunc('month', o.day)::date = m.month" in sql for sql in mapped_queries)
    assert any("FROM mapping m JOIN bond_reference_terms r ON upper(btrim(r.cusip9)) = m.reference_cusip9" in sql for sql in mapped_queries)
    assert any("FROM mapping m JOIN panel_months pm ON pm.month = m.month" in sql for sql in mapped_queries)
    rating_sql = next(sql for sql in mapped_queries if "FROM bond_rating_static r JOIN mapping m" in sql)
    assert "SELECT DISTINCT m.execution_cusip9 AS cusip9" in rating_sql
    assert "ON upper(btrim(r.cusip9)) = m.reference_cusip9" in rating_sql
    issuer_sql = next(sql for sql in mapped_queries if "sec_cusip_ticker_map" in sql)
    assert "non[-[:space:]]*corporate" in issuer_sql
    assert "~* '(^|[^[:alnum:]])corporate([^[:alnum:]]|$)'" in issuer_sql
    assert "ILIKE '%%corporate%%'" not in issuer_sql
    assert "bond_price_fund_asof_v1(pm.price_as_of)" in issuer_sql
    assert "panel_months(month, price_as_of)" in issuer_sql
    assert "panel_months(month, price_as_of) AS (SELECT DISTINCT" in issuer_sql
    assert "a.valid_from <= pm.price_as_of" in issuer_sql
    assert "FROM bond_price_latest_v1" not in issuer_sql
    assert "db_type_reason" in issuer_sql
    reference_sql = next(sql for sql in mapped_queries if "FROM mapping m JOIN bond_reference_terms" in sql)
    assert "amount_outstanding_vendor" in reference_sql
    assert "prior.amount_outstanding_k" in reference_sql
    assert "JOIN bond_panel_snapshot" in reference_sql
    liquidity_sql = next(sql for sql in mapped_queries if "bond_liquidity_monthly" in sql)
    assert ")), historical AS" in liquidity_sql
    assert "), live AS" in liquidity_sql
    assert "l.month IN (%s, %s)" in liquidity_sql
    assert "AND m.month = %s WHERE t.day >= %s AND t.day <= %s" in liquidity_sql
    assert "all_rows AS (SELECT * FROM live UNION ALL SELECT * FROM historical)" in liquidity_sql
    assert lineage["distribution_rule"] == "reg_s"
    assert lineage["distribution_mapping_snapshot_id"] == REG_S_SNAPSHOT_ID
    assert lineage["distribution_mapping_count"] == "1"
    assert lineage["distribution_mapping_closed_as_of"] == "2026-07-31"
    assert lineage["distribution_mapping_open_as_of"] == "2026-08-08"
    assert lineage["distribution_mapping_closed_count"] == "1"
    assert lineage["distribution_mapping_open_count"] == "1"
    assert lineage["distribution_mapping_omission:no_supported_reg_s_cusip"] == "1"
    assert lineage["distribution_mapping_closed_omission:no_supported_reg_s_cusip"] == "1"
    assert lineage["static_rating_mapping"] == f"bond_rating_static:{'a' * 64}"


def test_initial_stage6_base_requires_revision_bound_authorization(monkeypatch) -> None:
    parent = {
        "publication_id": "base",
        "parent_publication_id": None,
        "first_month": date(2020, 1, 1),
        "last_closed_month": date(2026, 6, 1),
    }
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setenv("CODE_REVISION", "revision-123")
    monkeypatch.delenv("BOND_PANEL_STAGE6_INITIAL_AUTHORIZATION", raising=False)

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "gate_failed"
    assert outcome["input_relation_reasons"] == ["initial_stage6_authorization_absent_or_mismatch"]


def test_initial_stage6_authorization_must_equal_the_exact_code_revision(monkeypatch) -> None:
    monkeypatch.setenv("BOND_PANEL_STAGE6_INITIAL_AUTHORIZATION", "revision-123")
    assert bond_panel._initial_stage6_authorized(
        {"parent_publication_id": None}, "revision-123"
    )
    assert not bond_panel._initial_stage6_authorized(
        {"parent_publication_id": None}, "revision-other"
    )
    assert bond_panel._initial_stage6_authorized(
        {"parent_publication_id": "base"}, "revision-other"
    )


def test_parent_distribution_requires_the_selected_reg_s_snapshot() -> None:
    assert bond_panel._parent_distribution_reasons(
        {"source_lineage": REG_S_LINEAGE}, REG_S_SNAPSHOT_ID
    ) == []
    assert bond_panel._parent_distribution_reasons(
        {"source_lineage": {"distribution_rule": "rule_144a"}}, REG_S_SNAPSHOT_ID
    ) == ["parent_distribution_rule_not_reg_s"]
    assert bond_panel._parent_distribution_reasons(
        {"source_lineage": REG_S_LINEAGE}, "other-snapshot"
    ) == ["parent_distribution_mapping_snapshot_mismatch"]


def test_panel_refuses_legacy_144a_parent_before_loading_inputs(monkeypatch) -> None:
    parent = {
        "publication_id": "legacy",
        "parent_publication_id": "base",
        "first_month": date(2020, 1, 1),
        "last_closed_month": date(2026, 6, 1),
        "open_month": date(2026, 7, 1),
        "snapshot_max_month": date(2026, 7, 1),
        "returns_max_month": date(2026, 6, 1),
        "source_lineage": {"distribution_rule": "rule_144a"},
    }
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setattr(bond_panel, "_load_inputs", lambda *_args, **_kwargs: pytest.fail("must not load"))
    monkeypatch.setenv("CODE_REVISION", "revision-123")
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", REG_S_SNAPSHOT_ID)

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "gate_failed"
    assert outcome["input_relation_reasons"] == ["parent_distribution_rule_not_reg_s"]


def test_panel_same_month_rerun_is_current_without_reloading_inputs(monkeypatch) -> None:
    parent = {
        "publication_id": "current-reg-s",
        "parent_publication_id": "base-reg-s",
        "first_month": date(2020, 1, 1),
        "last_closed_month": date(2026, 7, 1),
        "open_month": date(2026, 8, 1),
        "snapshot_max_month": date(2026, 8, 1),
        "returns_max_month": date(2026, 7, 1),
        "source_lineage": REG_S_LINEAGE,
    }
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setattr(
        bond_panel,
        "_load_inputs",
        lambda *_args, **_kwargs: pytest.fail("same-month rerun must not reload inputs"),
    )
    monkeypatch.setenv("CODE_REVISION", "revision-123")
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", REG_S_SNAPSHOT_ID)

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 9))

    assert outcome == {
        "state": "current",
        "aborted": False,
        "reason": "panel_month_already_current",
        "publication_id": "current-reg-s",
        "config_hash": bond_panel.PANEL_CONFIG_HASH,
        "closed_month": "2026-07-01",
        "open_month": "2026-08-01",
        "distribution_mapping_snapshot_id": REG_S_SNAPSHOT_ID,
    }


def test_panel_classifies_registry_resolution_failures_as_mapping_gates(monkeypatch) -> None:
    parent = {
        "publication_id": "reg-s-parent",
        "parent_publication_id": "base",
        "first_month": date(2020, 1, 1),
        "last_closed_month": date(2026, 6, 1),
        "open_month": date(2026, 7, 1),
        "snapshot_max_month": date(2026, 7, 1),
        "returns_max_month": date(2026, 6, 1),
        "source_lineage": REG_S_LINEAGE,
    }
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setattr(
        bond_panel,
        "_load_inputs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(DistributionSeriesError("snapshot_not_approved")),
    )
    monkeypatch.setenv("CODE_REVISION", "revision-123")
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", REG_S_SNAPSHOT_ID)

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "gate_failed"
    assert outcome["input_relation_reasons"] == ["distribution_mapping:snapshot_not_approved"]


def test_stage6_rejects_parent_whose_returns_do_not_reach_declared_close() -> None:
    parent = {
        "last_closed_month": date(2026, 6, 1),
        "open_month": None,
        "snapshot_max_month": date(2026, 6, 1),
        "returns_max_month": date(2025, 3, 1),
    }

    assert bond_panel._parent_integrity_reasons(parent) == [
        "parent_returns_max_month_mismatch:2025-03-01:2026-06-01"
    ]


def test_panel_run_fails_closed_before_loading_an_incomplete_parent(monkeypatch) -> None:
    parent = {
        "publication_id": "child",
        "parent_publication_id": "base",
        "first_month": date(2020, 1, 1),
        "last_closed_month": date(2026, 6, 1),
        "open_month": date(2026, 7, 1),
        "snapshot_max_month": date(2026, 7, 1),
        "returns_max_month": date(2025, 3, 1),
    }
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setenv("CODE_REVISION", "revision-123")

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "gate_failed"
    assert outcome["input_relation_reasons"] == [
        "parent_returns_max_month_mismatch:2025-03-01:2026-06-01"
    ]


def test_stage6_accepts_a_structurally_complete_parent_partition() -> None:
    parent = {
        "last_closed_month": date(2026, 6, 1),
        "open_month": date(2026, 7, 1),
        "snapshot_max_month": date(2026, 7, 1),
        "returns_max_month": date(2026, 6, 1),
        "source_lineage": REG_S_LINEAGE,
    }

    assert bond_panel._parent_integrity_reasons(parent) == []


def test_parent_return_anchor_normalizes_postgres_month_for_returns(monkeypatch) -> None:
    closed = pd.Timestamp("2026-07-01")
    postgres_anchor = pd.DataFrame(
        {
            "cusip_id": ["AAA"],
            "month": [date(2026, 6, 1)],
            "pr": [100.0],
            "ytm": [0.05],
            "bond_maturity": [5.0],
            "rating_bucket": ["BBB"],
            "eligibility_state": ["included"],
        }
    )
    monkeypatch.setattr(bond_panel, "_frame", lambda *_args, **_kwargs: postgres_anchor.copy())

    anchor = bond_panel._parent_return_anchor(object(), closed)
    current = pd.DataFrame(
        {
            "cusip_id": ["AAA"],
            "month": [closed],
            "pr": [101.0],
            "ytm": [0.05],
            "bond_maturity": [5.0],
        }
    )

    returns = bond_panel.monthly_returns(pd.concat([anchor, current], ignore_index=True))

    assert anchor.loc[0, "month"] == pd.Timestamp("2026-06-01")
    assert returns["month"].tolist() == [closed]


def test_parent_return_anchor_normalizes_postgres_numerics_for_returns(monkeypatch) -> None:
    closed = pd.Timestamp("2026-07-01")
    postgres_anchor = pd.DataFrame(
        {
            "cusip_id": ["AAA"],
            "month": [date(2026, 6, 1)],
            "pr": [Decimal("100.0")],
            "ytm": [Decimal("0.05")],
            "bond_maturity": [Decimal("5.0")],
            "rating_bucket": ["BBB"],
            "eligibility_state": ["included"],
        }
    )
    monkeypatch.setattr(bond_panel, "_frame", lambda *_args, **_kwargs: postgres_anchor.copy())

    anchor = bond_panel._parent_return_anchor(object(), closed)
    current = pd.DataFrame(
        {
            "cusip_id": ["AAA"],
            "month": [closed],
            "pr": [101.0],
            "ytm": [0.05],
            "bond_maturity": [5.0],
        }
    )

    returns = bond_panel.monthly_returns(pd.concat([anchor, current], ignore_index=True))

    assert returns["month"].tolist() == [closed]


def test_closed_returns_use_observed_exclusions_and_tombstone_removed_parent_bonds() -> None:
    closed = pd.Timestamp("2026-07-01")
    anchor = pd.DataFrame(
        {
            "cusip_id": ["NOW_EXCLUDED", "REMOVED"],
            "month": [pd.Timestamp("2026-06-01")] * 2,
            "pr": [100.0, 80.0],
            "ytm": [0.05, 0.08],
            "bond_maturity": [5.0, 4.0],
            "rating_bucket": ["BBB", "BBB"],
            "rating_state": ["static_current", "static_current"],
            "rating_as_of_month": [pd.Timestamp("2026-06-01")] * 2,
            "rating_reason": ["static_rating_current"] * 2,
            "rating_staleness_months": [0, 0],
            "eligibility_state": ["included", "included"],
        }
    )
    current = pd.DataFrame(
        {
            "cusip_id": ["NOW_EXCLUDED"],
            "month": [closed],
            "pr": [101.0],
            "ytm": [0.05],
            "bond_maturity": [5.0],
            "rating_bucket": ["BBB"],
            "eligibility_state": ["excluded"],
            "eligibility_reason": ["illiquid"],
        }
    )

    returns, tombstones = bond_panel._closed_returns_and_tombstones(anchor, current, closed)

    observed = returns.set_index("cusip_id").loc["NOW_EXCLUDED"]
    assert observed["exit_basis"] == "observed"
    assert observed["total_return"] > 0
    terminal = returns.set_index("cusip_id").loc["REMOVED"]
    assert terminal["exit_basis"] == "unexplained"
    assert tombstones[["cusip_id", "month", "eligibility_state", "eligibility_reason"]].to_dict("records") == [
        {
            "cusip_id": "REMOVED",
            "month": closed,
            "eligibility_state": "excluded",
            "eligibility_reason": "terminal_exit_removed",
        }
    ]
    assert pd.isna(tombstones.loc[0, "pr"])
    assert tombstones.loc[0, "flags"] == {"terminal_exit": True, "source": "parent_snapshot"}


def test_panel_publishes_with_missing_execution_ratings_and_closed_month_signals(monkeypatch) -> None:
    closed = pd.Timestamp("2026-07-01")
    open_month = pd.Timestamp("2026-08-01")
    panel = pd.DataFrame(
        {
            "cusip_id": ["AAA", "AAA"],
            "month": [closed, open_month],
            "pr": [99.0, 100.0],
            "ytm": [0.05, 0.05],
            "bond_maturity": [5.0, 5.0],
            "issuer_id": ["issuer-1", "issuer-1"],
            "ff17num": [1, 1],
            "currency": ["USD", "USD"],
            "asset_class": ["corporate", "corporate"],
            "amt_outstanding_k": [500_000, 500_000],
            "traded_days": [10, 5],
            "trade_count": [10, 5],
            "dollar_volume": [1000.0, 500.0],
            "quoted_days": [1, 1],
            "rel_bid_ask_bps": [10.0, 10.0],
            "coupon_pct": [5.0, 5.0],
            "maturity_date": [pd.Timestamp("2031-01-01"), pd.Timestamp("2031-01-01")],
            "spread_final": [0.01, 0.01],
            "spread_final_bps": [100.0, 100.0],
            "spread_definition": ["ytm_minus_interpolated_dgs"] * 2,
                "rating_bucket": ["BBB", "A"],
            "rating_as_of_month": [pd.NaT, pd.NaT],
                "rating_state": ["static_current", "static_carry_forward"],
                "rating_reason": ["static_present", "static_present"],
                "rating_staleness_months": [pd.NA, pd.NA],
                "reason_code": ["live_tick_median_valid_bps", None],
        }
    )
    parent = {
        "publication_id": "parent",
        "parent_publication_id": "base",
        "first_month": date(2020, 1, 1),
        "last_closed_month": date(2026, 6, 1),
        "open_month": date(2026, 7, 1),
        "snapshot_max_month": date(2026, 7, 1),
        "returns_max_month": date(2026, 6, 1),
        "source_lineage": REG_S_LINEAGE,
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setenv("CODE_REVISION", "test")
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", REG_S_SNAPSHOT_ID)
    monkeypatch.setattr(
        bond_panel,
        "_load_inputs",
        lambda _conn, _closed, _open, _as_of, **_kwargs: ({
            "static_rating_mapping": pd.DataFrame(),
        }, {
            "distribution_mapping_count": "1",
            "distribution_mapping_omission:no_supported_reg_s_cusip": "1",
        }),
    )
    monkeypatch.setattr(bond_panel, "build_db_monthly_panel", lambda **_kwargs: panel.copy())
    def snapshots(frame, ratings_pit=None):
        included = frame.iloc[[0]].merge(ratings_pit, on=["cusip_id", "month"], how="left").assign(eligibility_state="included", eligibility_reason="eligible")
        excluded = frame.iloc[[1]].assign(eligibility_state="excluded", eligibility_reason="missing_terms")
        return included, excluded
    monkeypatch.setattr(bond_panel, "build_snapshots", snapshots)
    monkeypatch.setattr(bond_panel, "_parent_return_anchor", lambda _conn, _closed: pd.DataFrame())
    monkeypatch.setattr(bond_panel, "monthly_returns", lambda _panel, terminal_exits=None: pd.DataFrame({"cusip_id": ["AAA"], "month": [closed], "total_return": [0.01], "exit_basis": ["observed"], "exit_reason": [None], "price_return": [0.01], "carry_return": [0.0], "suspect": [False]}))
    monkeypatch.setattr(bond_panel, "fit_all_months", lambda frame, *, as_of: (pd.DataFrame({"cusip_id": ["AAA"], "month": [closed], "rv_signal": [1.0]}), pd.DataFrame()))

    def materialize(_conn, **kwargs):
        captured.update(kwargs)
        return MaterializationResult("published", "fingerprint", "validated", {name: len(rows) for name, rows in kwargs["facts"].items()}, "parent")

    monkeypatch.setattr(bond_panel, "materialize_panel", materialize)

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "published", outcome
    facts = captured["facts"]
    assert {row["month"] for row in facts["rv_signal"]} == {"2026-07-01"}
    assert {row["month"] for row in facts["returns"]} == {"2026-07-01"}
    assert {row["month"] for row in facts["snapshot"]} == {"2026-07-01", "2026-08-01"}
    excluded = next(row for row in facts["snapshot"] if row["eligibility_state"] == "excluded")
    assert (excluded["rating_bucket"], excluded["rating_state"], excluded["rating_reason"]) == ("A", "static_carry_forward", "static_present")
    assert excluded["liquidity_reason"] == "monthly_liquidity_absent"
    included = next(row for row in facts["snapshot"] if row["eligibility_state"] == "included")
    assert included["liquidity_reason"] == "live_tick_median_valid_bps"
    assert included["terms_source"] == "bond_reference_terms"
    assert captured["first_month"] == date(2020, 1, 1)


def test_terminal_exit_rows_are_closed_month_only_and_typed() -> None:
    closed = pd.Timestamp("2026-07-01")
    exits = pd.DataFrame({
        "cusip_id": ["MATURED", "DISTRESS", "UNKNOWN"],
        "month": [closed, closed, closed],
        "pr": [100.0, 60.0, 80.0],
        "ytm": [0.12, 0.10, 0.10],
        "bond_maturity": [1.0, 5.0, 5.0],
        "rating_bucket": ["BBB", "D", "BBB"],
    })

    rows = bond_panel.monthly_returns(
        pd.DataFrame({"cusip_id": ["OLD"], "month": [pd.Timestamp("2026-06-01")], "pr": [100.0], "ytm": [0.05], "bond_maturity": [5.0]}),
        terminal_exits=exits,
    )

    assert set(rows["exit_basis"]) == {"matured", "distressed", "unexplained"}
    assert set(rows["month"]) == {closed}
