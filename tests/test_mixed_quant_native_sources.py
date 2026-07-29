"""Cobertura do staging NATIVO do publisher (``_populate_native_observations``).

Os testes de `test_mixed_quant_publication.py` inserem direto em
`mixed_quant_identity_observation` — eles nunca exercitam as queries que LEEM as
superfícies de origem. Foi exatamente aí que a publicação `mixed_quant_v1` de
produção ficou 100% fundos: a etapa de single names casava
`sec_cusip_ticker_map.cusip` (CUSIP de 9 caracteres) contra `left(h.cusip, 6)`,
o que não casa nunca — 0 linhas sobre 1.582.121 holdings elegíveis, sem erro.

Estes testes cobrem a origem: dado um holding de ação e um mapa que o resolve,
uma identidade de equity (e seus retornos) TEM de chegar ao staging.

Rodam contra um Postgres isolado (nunca um DSN de produção). Defina
SEC_TEST_DATABASE_URL, ex.: "host=127.0.0.1 port=55432 dbname=postgres user=postgres".
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
from uuid import uuid4

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.types.json import Jsonb  # noqa: E402

from src.quant_data import publication as pub  # noqa: E402
from src.workers import mixed_quant_publication as worker  # noqa: E402

_BASE_DSN = os.getenv("SEC_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not _BASE_DSN, reason="SEC_TEST_DATABASE_URL not set")

AS_OF = date(2026, 7, 23)
# CUSIP real de 9 caracteres, como o holdings do N-PORT e o mapa guardam.
CUSIP = "037833100"
FUND_ID = "11111111-1111-4111-8111-111111111111"


def _install_sources(conn) -> None:
    """Cria as cinco superfícies de origem que o staging nativo exige."""
    conn.execute(
        "CREATE TABLE funds_v ("
        " instrument_id uuid, currency text, series_id text, ticker text,"
        " inception_date date)"
    )
    conn.execute(
        "CREATE TABLE nav_timeseries ("
        " instrument_id uuid, nav_date date, return_1d double precision,"
        " return_type text, source text)"
    )
    conn.execute(
        "CREATE TABLE sec_cusip_ticker_map ("
        " cusip text, ticker text, issuer_cik text, resolved_via text,"
        " last_verified_at timestamptz, is_tradeable boolean, security_type text)"
    )
    conn.execute(
        "CREATE TABLE sec_nport_holdings_v2_current ("
        " holding_id bigint, publication_id uuid, cusip text, isin text,"
        " issuer_name text, issuer_category text, source_series_id text,"
        " report_date date, signed_pct_of_nav double precision,"
        " payoff_profile text, source_run_id uuid,"
        " source_typed_projection jsonb)"
    )
    conn.execute(
        "CREATE TABLE stock_daily_returns ("
        " ticker text, date date, return_1d double precision)"
    )


def _seed(conn) -> None:
    """Um fundo com NAV e uma AÇÃO listada num holding, resolvível pelo mapa."""
    conn.execute(
        "INSERT INTO funds_v VALUES (%s,'USD','S1','FUNDA',%s)",
        (FUND_ID, date(2015, 1, 1)),
    )
    conn.execute(
        "INSERT INTO nav_timeseries VALUES (%s,%s,0.001,'simple','vendor')",
        (FUND_ID, date(2026, 7, 22)),
    )
    conn.execute(
        "INSERT INTO sec_cusip_ticker_map VALUES (%s,'AAPL','0000320193','exact',%s,true,'Common Stock')",
        (CUSIP, datetime(2026, 7, 1, tzinfo=timezone.utc)),
    )
    conn.execute(
        "INSERT INTO sec_nport_holdings_v2_current VALUES "
        "(1,%s,%s,'US0378331005','Apple Inc','Technology','S1',%s,1.5,'Long',%s,%s)",
        (uuid4(), CUSIP, date(2026, 6, 30), uuid4(), Jsonb({"ASSET_CAT": "EC"})),
    )
    conn.execute(
        "INSERT INTO stock_daily_returns VALUES ('AAPL',%s,0.004)",
        (date(2026, 7, 22),),
    )


@pytest.fixture()
def env():
    schema = f"mixed_native_{uuid4().hex}"
    with psycopg.connect(_BASE_DSN, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
        conn.execute(f'SET search_path TO "{schema}"')
        pub.install_schema(conn)
        _install_sources(conn)
        try:
            yield conn
        finally:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_equity_holdings_reach_the_identity_staging(env) -> None:
    """A regressão que deixou a publicação sem single names: o holding de ação
    existe e o mapa o resolve, logo a identidade de equity TEM de ser estagiada."""
    conn = env
    _seed(conn)

    counts = worker._populate_native_observations(conn, AS_OF)

    assert counts["fund_identities"] == 1, "o caminho de fundos é o controle"
    assert counts["equity_cusip_identities"] == 1, (
        "nenhuma identidade de equity foi estagiada a partir de um holding "
        "elegível — a publicação nasceria 100% fundos"
    )
    assert counts["equity_ticker_identities"] == 1
    staged = conn.execute(
        "SELECT instrument_type, alias_type, alias_value FROM "
        "mixed_quant_identity_observation WHERE instrument_type='equity' "
        "ORDER BY alias_type"
    ).fetchall()
    assert staged == [
        ("equity", "cusip", CUSIP),
        ("equity", "ticker", "AAPL"),
    ]


def test_equity_returns_reach_the_return_staging(env) -> None:
    """Identidade sem retorno não vira posição otimizável — o adapter a descarta.
    A CTE ``eligible`` dos retornos usa o mesmo predicado das identidades."""
    conn = env
    _seed(conn)

    counts = worker._populate_native_observations(conn, AS_OF)

    assert counts["equity_returns"] == 1, "a ação chegaria à publicação sem histórico"
    rows = conn.execute(
        "SELECT alias_value, total_return FROM mixed_quant_return_observation "
        "WHERE source_lineage->>'source_surface'='stock_daily_returns'"
    ).fetchall()
    assert rows == [("AAPL", pytest.approx(0.004))]


def test_zero_equities_from_a_populated_source_fails_loud(env) -> None:
    """A falha silenciosa é o que escondeu o bug: contagem 0 e run bem-sucedido.
    Com holdings elegíveis presentes e NENHUMA identidade de equity resolvida,
    o staging tem de falhar em vez de publicar um universo 'misto' só de fundos.
    """
    conn = env
    _seed(conn)
    # O mapa deixa de resolver o holding (o CUSIP some) — a origem continua
    # povoada, mas nada casa. É a forma exata do bug de produção.
    conn.execute("UPDATE sec_cusip_ticker_map SET cusip='999999999'")

    with pytest.raises(worker.MixedQuantSourceError) as exc:
        worker._populate_native_observations(conn, AS_OF)
    assert "equity" in str(exc.value).lower()


def test_source_without_equity_holdings_is_not_an_error(env) -> None:
    """A guarda não pode punir uma origem que legitimamente não tem ações —
    ela só dispara quando há holding elegível e mesmo assim zero identidades."""
    conn = env
    _seed(conn)
    conn.execute("DELETE FROM sec_nport_holdings_v2_current")

    counts = worker._populate_native_observations(conn, AS_OF)

    assert counts["equity_cusip_identities"] == 0
    assert counts["fund_identities"] == 1
