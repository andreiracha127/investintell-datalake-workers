from __future__ import annotations

from pathlib import Path

import src.workers.fund_institutional_reveal as reveal
import src.workers.matview_refresh as matview_refresh


SCHEMA = Path(__file__).parents[1] / "schemas" / "fund_reveal_13f_holdings_mv.sql"


def test_reveal_13f_mv_keeps_only_each_cusips_latest_rows() -> None:
    assert SCHEMA.exists(), "fund reveal 13F MV DDL must be versioned"

    ddl = SCHEMA.read_text(encoding="utf-8").lower()
    for fragment in (
        "create materialized view if not exists fund_reveal_13f_holdings_mv",
        "upper(cusip) as cusip",
        "max(report_date) as report_date",
        "group by upper(cusip)",
        "from sec_13f_holdings",
        "latest.report_date = h.report_date",
        "coalesce((to_jsonb(h) ->> 'period')::date, h.report_date) as source_period",
        "h.accession_number",
        "h.cusip as source_cusip",
        "to_jsonb(h) ->> 'name'",
        "to_jsonb(h) ->> 'issuer_name'",
        "to_jsonb(h) ->> 'value_usd'",
        "to_jsonb(h) ->> 'market_value'",
        ") as value_usd",
        "with no data",
        "create unique index if not exists fund_reveal_13f_holdings_mv_identity_uidx",
        "report_date, cik, source_period, accession_number, source_cusip",
        "create index if not exists fund_reveal_13f_holdings_mv_cusip_report_idx",
        "(cusip, report_date desc)",
    ):
        assert fragment in ddl

    # Preserve the legacy match exactly: surrounding whitespace was not
    # canonicalized by upper(h.cusip) = ANY(...).
    assert "btrim(" not in ddl


def test_reveal_query_reads_indexed_13f_mv_but_keeps_set_latest_semantics() -> None:
    sql = " ".join(reveal._13F_SQL.lower().split())

    assert "from fund_reveal_13f_holdings_mv h" in sql
    assert "from sec_13f_holdings h" not in sql
    assert "latest as (select max(period) as period from matched)" in sql
    assert "matched join latest on latest.period = matched.period" in sql
    assert "left join lateral" in sql
    assert "from sec_managers" in sql
    assert "where h.cusip = any(%(cusips)s)" in sql
    assert "upper(h.cusip)" not in sql
    assert "order by value_usd desc nulls last, cik asc, cusip asc, source_cusip asc" in sql


def test_reveal_13f_mv_refreshes_after_reveal_holdings_mv() -> None:
    reveal_index = matview_refresh._APP_BOOTSTRAP_MVS.index("fund_reveal_holdings_mv")
    reveal_13f_index = matview_refresh._APP_BOOTSTRAP_MVS.index(
        "fund_reveal_13f_holdings_mv"
    )

    assert reveal_index < reveal_13f_index
