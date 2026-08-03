"""Tests for the eod_prices_warmer worker (Tiingo → eod_prices).

Pure-helper tests (full-bar → row mapping, universe union, watermark filtering,
upsert SQL shape) run anywhere with no network and no DB. The idempotent-upsert
test uses a throwaway schema in a local DB and self-skips if unreachable.

This worker keeps the Investintell-Light API's ``eod_prices`` universe fresh so
the API can serve /stocks/* DB-first (Strategy B) without a synchronous Tiingo
fetch for stale tickers on the request path.
"""

from __future__ import annotations

import datetime as _dt

import psycopg
import pytest

from src.db import LOCK_EOD_PRICES_WARMER, advisory_lock
from src.workers import eod_prices_warmer as w
from src.workers._tiingo import TiingoClient, parse_price_bars

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"local DB unreachable: {exc}")


_FULL_BAR = {
    "date": "2026-06-15T00:00:00.000Z",
    "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "volume": 1000,
    "adjOpen": 9.9, "adjHigh": 10.9, "adjLow": 9.4, "adjClose": 10.4,
    "adjVolume": 1000, "divCash": 0.0, "splitFactor": 1.0,
}


# ──────────────────────────────────────────────────────────────────────────────
# Full-bar → eod_prices row mapping
# ──────────────────────────────────────────────────────────────────────────────
def test_build_eod_rows_maps_all_fourteen_columns():
    rows = w.build_eod_rows("AAPL", [_FULL_BAR])
    assert rows == [
        ("AAPL", _dt.date(2026, 6, 15), 10.0, 11.0, 9.5, 10.5, 1000,
         9.9, 10.9, 9.4, 10.4, 1000, 0.0, 1.0)
    ]


def test_build_eod_rows_skips_bar_missing_any_required_field():
    """eod_prices columns are all NOT NULL → a bar missing/None on any field is dropped."""
    no_adjclose = {k: v for k, v in _FULL_BAR.items() if k != "adjClose"}
    null_volume = {**_FULL_BAR, "date": "2026-06-16T00:00:00.000Z", "volume": None}
    rows = w.build_eod_rows("AAPL", [_FULL_BAR, no_adjclose, null_volume])
    # only the complete bar survives
    assert [r[1] for r in rows] == [_dt.date(2026, 6, 15)]


def test_build_eod_rows_empty():
    assert w.build_eod_rows("AAPL", []) == []


# ──────────────────────────────────────────────────────────────────────────────
# Upsert SQL shape (DB-free contract check)
# ──────────────────────────────────────────────────────────────────────────────
def test_eod_upsert_sql_targets_ticker_date_and_updates_price_columns():
    sql = w.EOD_UPSERT_SQL
    assert "INSERT INTO eod_prices" in sql
    assert "ON CONFLICT (ticker, date) DO UPDATE" in sql
    for col in (
        "open", "high", "low", "close", "volume",
        "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume",
        "div_cash", "split_factor",
    ):
        assert f"{col} = EXCLUDED.{col}" in sql
    # ticker/date are the conflict key — never in the SET clause.
    assert "ticker = EXCLUDED" not in sql
    assert "date = EXCLUDED" not in sql


def test_instrument_seed_sqls_preserve_eod_prices_fk_parent():
    active_sql = w.SEED_ACTIVE_INSTRUMENTS_SQL
    extra_sql = w.SEED_EXTRA_INSTRUMENT_SQL

    assert "INSERT INTO instruments" in active_sql
    assert "FROM universe_constituents" in active_sql
    assert "WHERE status = 'active'" in active_sql
    assert "ON CONFLICT (ticker) DO NOTHING" in active_sql
    assert "INSERT INTO instruments" in extra_sql
    assert "VALUES (%s, %s, 'etf')" in extra_sql
    assert "ON CONFLICT (ticker) DO NOTHING" in extra_sql


# ──────────────────────────────────────────────────────────────────────────────
# Universe + watermarks (fake cursor — no DB)
# ──────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._sql = sql

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)


def test_warming_universe_unions_index_tickers_dedups_and_sorts():
    conn = _FakeConn([("MSFT",), ("AAPL",), ("SPY",)])
    universe = w.warming_universe(conn)
    # SPY already present is not duplicated; remaining index/benchmark ETFs added.
    assert universe == sorted(set(["MSFT", "AAPL", "SPY", *w.INDEX_TICKERS]))
    assert universe == sorted(universe)
    assert len(universe) == len(set(universe))


def test_ticker_watermarks_drops_null_max_date():
    conn = _FakeConn([("AAPL", _dt.date(2026, 6, 15)), ("ZZZ", None)])
    marks = w._ticker_watermarks(conn)
    assert marks == {"AAPL": _dt.date(2026, 6, 15)}


# ──────────────────────────────────────────────────────────────────────────────
# Tiingo client: raw full-bar fetch wiring (no network — monkeypatched _get_bars)
# ──────────────────────────────────────────────────────────────────────────────
def test_fetch_daily_bars_returns_raw_and_prices_stay_parsed():
    client = TiingoClient(key="test")
    try:
        sample = [_FULL_BAR]
        client._get_bars = lambda *a, **k: sample  # type: ignore[method-assign]
        # raw full bars for the warmer
        assert client.fetch_daily_bars("AAPL", _dt.date(2026, 6, 1)) is sample
        # NAV path unchanged: still date+adjClose tuples
        assert client.fetch_daily_prices("AAPL", _dt.date(2026, 6, 1)) == parse_price_bars(sample)
    finally:
        client.close()


# ──────────────────────────────────────────────────────────────────────────────
# Advisory lock id
# ──────────────────────────────────────────────────────────────────────────────
def test_advisory_lock_id_is_distinct():
    assert LOCK_EOD_PRICES_WARMER == 900_335


def test_fetch_rate_stays_within_the_shared_account_budget():
    """Was ``test_fetch_rate_is_fast_lane``, asserting ``>= 25.0 req/s``.

    That pinned the defect in place: 25 req/s is 90k req/h against a ceiling of
    10k req/h on the current key, and lower still on the key live during the
    incident, so the warmer drained the fleet's hourly budget within minutes
    each morning and the regime workers running in the same rolling hour got
    only 429s. The invariant worth holding is the account's, not the sweep's.
    """
    from src.workers._tiingo import TIINGO_MAX_REQUESTS_PER_HOUR

    assert w.FETCH_RATE_PER_S * 3600 <= TIINGO_MAX_REQUESTS_PER_HOUR
    assert w.FETCH_BURST >= 10.0


def test_warmer_cold_lookback_covers_screener_two_year_beta():
    assert w.NEW_TICKER_LOOKBACK_DAYS >= 745
    for ticker in ("SPY", "GLD", "AGG", "TLT", "USO"):
        assert ticker in w.INDEX_TICKERS


# ──────────────────────────────────────────────────────────────────────────────
# Upsert idempotency (throwaway schema; self-skips without a local DB)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_eod_prices_idempotent():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_eodw CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_eodw")
            cur.execute("SET search_path TO _dlw_test_eodw")
            cur.execute(
                """CREATE TABLE eod_prices (
                       ticker varchar(20) NOT NULL,
                       date date NOT NULL,
                       open double precision NOT NULL,
                       high double precision NOT NULL,
                       low double precision NOT NULL,
                       close double precision NOT NULL,
                       volume bigint NOT NULL,
                       adj_open double precision NOT NULL,
                       adj_high double precision NOT NULL,
                       adj_low double precision NOT NULL,
                       adj_close double precision NOT NULL,
                       adj_volume bigint NOT NULL,
                       div_cash double precision NOT NULL,
                       split_factor double precision NOT NULL,
                       PRIMARY KEY (ticker, date))"""
            )
        rows = w.build_eod_rows("AAPL", [_FULL_BAR])
        n1 = w.upsert_eod_prices(conn, rows)
        conn.commit()
        n2 = w.upsert_eod_prices(conn, rows)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM eod_prices")
            count = cur.fetchone()[0]
        assert n1 == n2 == 1
        assert count == 1
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_eodw CASCADE")
        conn.commit()
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Sweep ordering + resume cursor
#
# Regressão de 2026-07-22..28: a varredura era `sorted(tickers)` puro e o abort
# por orçamento não guardava posição, então toda execução refazia o prefixo
# alfabético. SPY/TLT/TIP/SHY/GLD/DBC (S,T,G,D) nunca eram alcançados, ficaram 8
# dias úteis parados em 2026-07-16, e o open_macro_v03 passou a falhar fechado
# com staleness_block — /macro e o builder mostrando "no usable macro signal".
# ──────────────────────────────────────────────────────────────────────────────
def test_macro_sleeves_are_fetched_before_the_long_tail():
    from src.workers.eod_prices_warmer import MACRO_SLEEVE_TICKERS, order_sweep

    # O universo do teste TEM de conter todos os sleeves, porque order_sweep só
    # ordena o que existe: um sleeve ausente daqui passaria despercebido e é
    # exatamente o buraco que o incidente abriu. LQD entrou com o open_macro v4
    # (perna de crédito do barbell B60-LQD no livro de dominância).
    universe = sorted(["AAPL", "AA", "ABBV", *MACRO_SLEEVE_TICKERS, "ZM"])
    assert set(MACRO_SLEEVE_TICKERS) <= set(universe)
    ordered = order_sweep(universe)

    head = ordered[: len(set(MACRO_SLEEVE_TICKERS))]
    for ticker in MACRO_SLEEVE_TICKERS:
        assert ticker in head, f"{ticker} tem de vir antes da cauda"
    # E um corte por orçamento no tamanho do incidente ainda os alcança.
    assert set(MACRO_SLEEVE_TICKERS).issubset(set(ordered[:8]))


def test_sweep_is_a_ring_so_no_ticker_starves():
    from src.workers.eod_prices_warmer import order_sweep

    universe = [f"T{i:03d}" for i in range(10)]
    # Sem cursor, começa do início da cauda.
    assert order_sweep(universe, priority=())[0] == "T000"
    # Com cursor, retoma DEPOIS dele e dá a volta.
    rotated = order_sweep(universe, resume_after="T004", priority=())
    assert rotated[0] == "T005"
    assert rotated[-1] == "T004"
    assert sorted(rotated) == sorted(universe), "a rotação não pode perder ticker"


def test_cursor_tolerates_a_ticker_that_left_the_universe():
    from src.workers.eod_prices_warmer import order_sweep

    universe = ["AAA", "CCC", "DDD"]
    # "BBB" saiu do universo desde a última execução: retoma no próximo que
    # ordena depois dele, em vez de reiniciar do começo.
    assert order_sweep(universe, resume_after="BBB", priority=())[0] == "CCC"
    # Cursor no último elemento dá a volta inteira.
    assert order_sweep(universe, resume_after="DDD", priority=())[0] == "AAA"


def test_priority_head_never_moves_the_cursor():
    """Senão todo abort rebobina a varredura para a posição do head."""
    import inspect

    from src.workers import eod_prices_warmer

    source = inspect.getsource(eod_prices_warmer.run)
    assert "if ticker not in _PRIORITY_SET:" in source
    assert "last_done = ticker" in source


def test_run_actually_consumes_the_ordered_sweep():
    """Prender a fiação, não só a função pura.

    A primeira versão destes testes exercitava order_sweep isoladamente: voltar o
    run() para `sorted(warming_universe(conn))` passava por todos eles. É a
    ordenação NO CAMINHO DE EXECUÇÃO que mantém as sleeves de macro vivas.
    """
    import inspect

    from src.workers import eod_prices_warmer

    source = inspect.getsource(eod_prices_warmer.run)
    assert "order_sweep(" in source, "run() tem de ordenar a varredura"
    assert "resume_after=resume_after" in source, "run() tem de passar o cursor"
    assert "read_cursor(conn)" in source
    assert "write_cursor(conn, last_done)" in source
    # O bug original, explicitamente proibido.
    assert "sorted(warming_universe(" not in source
