from __future__ import annotations

from pathlib import Path

import src.workers.matview_refresh as matview_refresh


SCHEMA = Path(__file__).parents[1] / "schemas" / "fund_reveal_holdings_mv.sql"


def test_reveal_holdings_mv_contract_preserves_null_weight_provenance() -> None:
    assert SCHEMA.exists(), "fund reveal holdings MV DDL must be versioned"

    ddl = SCHEMA.read_text(encoding="utf-8").lower()

    for fragment in (
        "create materialized view if not exists fund_reveal_holdings_mv",
        "with no data",
        "max(report_date)",
        "from fund_top_holdings_mv",
        "upper(btrim(h.cusip))",
        "sum(pct_of_nav) / 100.0",
        "count(*) as source_row_count",
        "count(pct_of_nav) as nonnull_weight_count",
        "count(*) - count(pct_of_nav) as null_weight_count",
        "bool_or(weight is null or null_weight_count > 0)",
        "as has_unknown_weight",
        "row_number() over",
        "weight desc nulls last",
        "cusip asc",
        "where rank <= 100",
        "create unique index if not exists fund_reveal_holdings_mv_series_report_rank_uidx",
        "(series_id, report_date, rank)",
    ):
        assert fragment in ddl

    assert "coalesce(sum(pct_of_nav)" not in ddl


def test_unknown_weight_gate_is_computed_before_top_100_truncation() -> None:
    ddl = SCHEMA.read_text(encoding="utf-8").lower()

    quality_gate = ddl.index("bool_or(weight is null or null_weight_count > 0)")
    top_100 = ddl.index("where rank <= 100")

    assert quality_gate < top_100


class _Cursor:
    def __init__(self, sink: dict[str, object]) -> None:
        self.sink = sink
        self.last_params: tuple[str, ...] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> None:
        self.sink.setdefault("sql", []).append(sql)
        self.sink.setdefault("params", []).append(params)
        self.last_params = params

    def fetchone(self) -> tuple[bool]:
        assert self.last_params is not None
        populated = self.sink["populated"]
        assert isinstance(populated, dict)
        return (populated[self.last_params[0]],)


class _Connection:
    def __init__(self, sink: dict[str, object]) -> None:
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def cursor(self) -> _Cursor:
        return _Cursor(self.sink)


def _refresh_sql_for(populated: bool, monkeypatch) -> list[str]:
    sink: dict[str, object] = {
        "populated": {"fund_reveal_holdings_mv": populated},
    }
    monkeypatch.setattr(
        matview_refresh,
        "connect",
        lambda dsn, *, autocommit: _Connection(sink),
    )

    assert matview_refresh._refresh_bootstrap_mvs(
        "postgres://lake", ["fund_reveal_holdings_mv"]
    ) == ["fund_reveal_holdings_mv"]
    sql = sink["sql"]
    assert isinstance(sql, list)
    return sql


def test_first_refresh_of_both_reveal_read_models_is_ordered(monkeypatch) -> None:
    sink: dict[str, object] = {
        "populated": {
            "fund_reveal_holdings_mv": False,
            "fund_reveal_13f_holdings_mv": False,
        },
    }
    monkeypatch.setattr(
        matview_refresh,
        "connect",
        lambda dsn, *, autocommit: _Connection(sink),
    )

    assert matview_refresh._refresh_bootstrap_mvs(
        "postgres://lake",
        ["fund_reveal_holdings_mv", "fund_reveal_13f_holdings_mv"],
    ) == ["fund_reveal_holdings_mv", "fund_reveal_13f_holdings_mv"]

    refreshes = [
        statement
        for statement in sink["sql"]
        if statement.startswith("REFRESH MATERIALIZED VIEW")
    ]
    assert refreshes == [
        "REFRESH MATERIALIZED VIEW fund_reveal_holdings_mv",
        "REFRESH MATERIALIZED VIEW fund_reveal_13f_holdings_mv",
    ]


def test_first_refresh_of_unpopulated_mv_is_not_concurrent(monkeypatch) -> None:
    sql = _refresh_sql_for(False, monkeypatch)

    assert any("relispopulated" in statement for statement in sql)
    assert "REFRESH MATERIALIZED VIEW fund_reveal_holdings_mv" in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_reveal_holdings_mv" not in sql


def test_refresh_of_populated_mv_remains_concurrent(monkeypatch) -> None:
    sql = _refresh_sql_for(True, monkeypatch)

    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_reveal_holdings_mv" in sql
