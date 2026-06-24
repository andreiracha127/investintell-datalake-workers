from __future__ import annotations

import re
from pathlib import Path

from src.workers import risk_metrics as rm
from src.workers.risk_metric_ownership import (
    ACTIVE_SHARE_COLUMNS,
    FUND_RISK_COLUMN_OWNERS,
    FUND_RISK_LATEST_MV_COLUMNS,
    FUND_RISK_LATEST_MV_STALE_POLICIES,
    FUND_RISK_METRICS_COLUMNS,
    LATENT_MMF_COLUMNS,
    OWNER_ACTIVE_SHARE,
    OWNER_RESERVED,
    OWNER_RISK_METRICS,
    RESERVED_COLUMNS,
    RISK_METRICS_POST_STEP_COLUMNS,
    RISK_METRICS_UPSERT_COLUMNS,
)


def _schema_sql() -> str:
    root = Path(__file__).resolve().parents[1]
    return (root / "schemas" / "risk_metrics.sql").read_text(encoding="utf-8")


def _declared_fund_risk_columns(sql: str) -> set[str]:
    create_match = re.search(
        r"CREATE TABLE IF NOT EXISTS fund_risk_metrics \((.*?)\n\);",
        sql,
        flags=re.S,
    )
    assert create_match, "fund_risk_metrics CREATE TABLE block not found"
    create_cols = {
        m.group(1)
        for m in re.finditer(r"^\s{4}([a-zA-Z_][a-zA-Z0-9_]*)\s+", create_match.group(1), re.M)
        if m.group(1).lower() not in {"constraint"}
    }
    alter_cols = set(
        re.findall(
            r"ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS ([a-zA-Z_][a-zA-Z0-9_]*)",
            sql,
        )
    )
    return create_cols | alter_cols


def _mv_select_columns(sql: str) -> set[str]:
    match = re.search(
        r"CREATE MATERIALIZED VIEW fund_risk_latest_mv AS\s*SELECT DISTINCT ON "
        r"\(instrument_id\)(.*?)\nFROM fund_risk_metrics",
        sql,
        flags=re.S,
    )
    assert match, "fund_risk_latest_mv SELECT block not found"
    return {
        token.strip().rstrip(",")
        for token in match.group(1).splitlines()
        if token.strip() and not token.strip().startswith("--")
    }


def test_schema_declares_every_managed_fund_risk_column():
    declared = _declared_fund_risk_columns(_schema_sql())
    assert set(FUND_RISK_METRICS_COLUMNS) <= declared
    assert len(FUND_RISK_METRICS_COLUMNS) == 102


def test_mmf_columns_are_latent_not_schema_managed_or_owned():
    declared = _declared_fund_risk_columns(_schema_sql())
    for col in LATENT_MMF_COLUMNS:
        assert col not in declared
        assert col not in FUND_RISK_COLUMN_OWNERS
        assert col not in FUND_RISK_LATEST_MV_COLUMNS


def test_every_fund_risk_column_has_exactly_one_owner():
    assert set(FUND_RISK_COLUMN_OWNERS) == set(FUND_RISK_METRICS_COLUMNS)
    assert len(FUND_RISK_COLUMN_OWNERS) == len(FUND_RISK_METRICS_COLUMNS)


def test_metric_columns_match_risk_metrics_upsert_owner():
    assert tuple(rm._METRIC_COLUMNS) == RISK_METRICS_UPSERT_COLUMNS


def test_risk_metrics_post_step_columns_are_declared_as_owned():
    for col in RISK_METRICS_POST_STEP_COLUMNS:
        assert FUND_RISK_COLUMN_OWNERS[col] == OWNER_RISK_METRICS


def test_latest_mv_exposes_only_owned_non_reserved_columns():
    owners = {col: FUND_RISK_COLUMN_OWNERS[col] for col in FUND_RISK_LATEST_MV_COLUMNS}
    assert OWNER_RESERVED not in owners.values()
    for col in FUND_RISK_LATEST_MV_COLUMNS:
        assert col not in RESERVED_COLUMNS


def test_latest_mv_active_share_has_explicit_stale_policy():
    mv_active_share = set(FUND_RISK_LATEST_MV_COLUMNS) & set(ACTIVE_SHARE_COLUMNS)
    assert mv_active_share
    assert "active_share_fund_report_date" in mv_active_share
    assert "active_share_benchmark_report_date" in mv_active_share
    assert "fund_report_age_days" in mv_active_share
    assert "benchmark_report_age_days" in mv_active_share
    assert OWNER_ACTIVE_SHARE in FUND_RISK_LATEST_MV_STALE_POLICIES


def test_schema_mv_matches_registry_contract():
    mv_cols = _mv_select_columns(_schema_sql())
    assert mv_cols == set(FUND_RISK_LATEST_MV_COLUMNS)


def test_schema_rebuilds_dependent_funds_list_without_reserved_risk_source():
    sql = _schema_sql()
    assert (
        sql.index("DROP MATERIALIZED VIEW IF EXISTS funds_list_mv")
        < sql.index("DROP MATERIALIZED VIEW IF EXISTS fund_risk_latest_mv")
    )

    match = re.search(
        r"CREATE MATERIALIZED VIEW funds_list_mv AS(.*?);",
        sql,
        flags=re.S,
    )
    assert match, "funds_list_mv CREATE MATERIALIZED VIEW block not found"
    funds_list_select = match.group(1)
    assert "r.elite_flag" not in funds_list_select
    assert "NULL::boolean AS elite_flag" in funds_list_select
