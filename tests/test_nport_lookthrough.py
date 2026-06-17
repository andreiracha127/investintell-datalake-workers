"""Tests for the nport_lookthrough worker (Frente C — look-through de holdings).

Pure-engine tests (expansão recursiva, agregações, residual, staleness) run
anywhere — no DB, no network. The integration tests run against the **cloud**
data-lake (DATABASE_URL from the environment / .env) because
``sec_nport_holdings`` only exists there; they self-skip when unreachable.
The worker's output tables belong to this worker, so writing a handful of
series to them from the test is safe and idempotent by design.

Modelo (doc 2026-06-11-lean-research-rebalance-macro-lookthrough.md §4.3 + §6):
  * expansão BFS profundidade máx. 2, guarda de ciclo por cadeia de ancestrais;
  * peso composto w = (pct_parent/100) × pct_child;
  * dimensões: issuer (CUSIP-6), asset_class, sector, currency;
  * direta × indireta separadas; NUNCA renormalizar Σpct>100;
  * residual explícito: fundo não-decomponível + derivativos gross/net;
  * staleness em cadeia: report_date mais antigo da cadeia expandida;
  * chaves sintéticas: IS:<isin> casa via isin (e via CUSIP embutido p/ ISIN US);
    LE:/H:/CIK: nunca casam → seguem como exposição direta e somam no bucket
    ``unidentified_pct``.
"""

from __future__ import annotations

import datetime as _dt
import os
import pathlib

import psycopg
import pytest

from src.db import LOCK_NPORT_LOOKTHROUGH, advisory_lock
from src.workers import nport_lookthrough as lt

D_ROOT = _dt.date(2026, 1, 31)
D_CHILD = _dt.date(2025, 12, 31)
D_GRAND = _dt.date(2025, 9, 30)


def H(cusip=None, isin=None, issuer="Issuer", asset="EC", sector="Tech",
      currency="USD", pct=0.0):
    return {
        "cusip": cusip, "isin": isin, "issuer_name": issuer,
        "asset_class": asset, "sector": sector, "currency": currency,
        "pct_of_nav": pct,
    }


def make_get_holdings(data):
    """data: {series_id: (report_date, [holdings])}"""
    return lambda series_id: data.get(series_id)


EMPTY_MAP = {"cusip": {}, "isin": {}}


# ──────────────────────────────────────────────────────────────────────────────
# sector_label — GICS real via issuer CUSIP-6; fallback issuerCat → bucket legível
# ──────────────────────────────────────────────────────────────────────────────
def test_sector_label_resolves_gics_via_issuer_cusip6():
    # A corporate *bond* inherits its issuer's equity GICS sector (CUSIP-6).
    smap = {"037833": "Information Technology"}
    assert lt.sector_label(H(cusip="037833100", sector="CORP"), smap) == "Information Technology"


def test_sector_label_falls_back_to_readable_issuercat_bucket():
    # Issuers absent from the GICS map (bonds, treasuries, munis) → readable bucket.
    assert lt.sector_label(H(cusip="912828XX1", sector="UST"), {}) == "U.S. Treasury"
    assert lt.sector_label(H(cusip="38259P999", sector="CORP"), {}) == "Corporate"
    assert lt.sector_label(H(cusip="64966M111", sector="MUN"), {}) == "Municipal"
    assert lt.sector_label(H(cusip="X", sector="RF"), {}) == "Registered Fund"


def test_sector_label_unknown_and_unmapped_code_passthrough():
    assert lt.sector_label(H(cusip=None, sector=None), {}) == "UNKNOWN"
    assert lt.sector_label(H(cusip="00000000Z", sector="ZZZ"), {}) == "ZZZ"


def test_expand_series_sector_dimension_prefers_gics_then_bucket():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", sector="CORP", pct=60.0),   # Apple bond → IT
        H(cusip="912828XX1", sector="UST", pct=40.0),    # Treasury → bucket
    ])}
    smap = {"037833": "Information Technology"}
    exposures, _ = lt.expand_series(
        "S1", make_get_holdings(data), EMPTY_MAP, sector_map=smap
    )
    sector_keys = {k for (dim, k) in exposures if dim == "sector"}
    assert "Information Technology" in sector_keys
    assert "U.S. Treasury" in sector_keys
    assert "CORP" not in sector_keys and "UST" not in sector_keys


# ──────────────────────────────────────────────────────────────────────────────
# match_fund — a aresta FoF
# ──────────────────────────────────────────────────────────────────────────────
def test_match_fund_real_cusip():
    fund_map = {"cusip": {"111111111": "S_CHILD"}, "isin": {}}
    assert lt.match_fund(H(cusip="111111111"), fund_map) == "S_CHILD"
    assert lt.match_fund(H(cusip="999999999"), fund_map) is None


def test_match_fund_isin_column_and_embedded_us():
    fund_map = {"cusip": {"111111111": "S_C"}, "isin": {"IE00B4L5Y983": "S_I"}}
    # coluna isin casa direto no mapa de isin
    assert lt.match_fund(H(cusip="999999999", isin="IE00B4L5Y983"), fund_map) == "S_I"
    # ISIN US carrega o CUSIP-9 embutido (posições 3..11) → casa no mapa de cusip
    assert lt.match_fund(H(cusip=None, isin="US1111111119"), fund_map) == "S_C"


def test_match_fund_synthetic_keys():
    fund_map = {"cusip": {"111111111": "S_C"}, "isin": {"IE00B4L5Y983": "S_I"}}
    # IS:<isin> casa via isin…
    assert lt.match_fund(H(cusip="IS:IE00B4L5Y983"), fund_map) == "S_I"
    # …e via CUSIP embutido quando o ISIN é US
    assert lt.match_fund(H(cusip="IS:US1111111119"), fund_map) == "S_C"
    # LE:/H:/CIK: nunca casam
    assert lt.match_fund(H(cusip="LE:529900ABCDEF"), fund_map) is None
    assert lt.match_fund(H(cusip="H:abc"), fund_map) is None
    assert lt.match_fund(H(cusip="CIK:1234"), fund_map) is None


# ──────────────────────────────────────────────────────────────────────────────
# Expansão — agregação direta simples
# ──────────────────────────────────────────────────────────────────────────────
def test_direct_aggregation_dedupes_issuer_by_cusip6():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", issuer="Apple Inc", asset="EC", pct=40.0),
        H(cusip="037833AB1", issuer="Apple Inc (bond)", asset="DBT", pct=10.0),
        H(cusip="594918104", issuer="Microsoft", asset="EC", pct=50.0),
    ])}
    exposures, summary = lt.expand_series("S1", make_get_holdings(data), EMPTY_MAP)

    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    assert issuer["037833"]["direct_pct"] == pytest.approx(50.0)
    assert issuer["594918"]["direct_pct"] == pytest.approx(50.0)
    assert issuer["037833"]["indirect_pct"] == pytest.approx(0.0)

    asset = {k[1]: v for k, v in exposures.items() if k[0] == "asset_class"}
    assert asset["EC"]["direct_pct"] == pytest.approx(90.0)
    assert asset["DBT"]["direct_pct"] == pytest.approx(10.0)

    cur = {k[1]: v for k, v in exposures.items() if k[0] == "currency"}
    assert cur["USD"]["direct_pct"] == pytest.approx(100.0)

    assert summary["direct_pct"] == pytest.approx(100.0)
    assert summary["indirect_pct"] == pytest.approx(0.0)
    assert summary["sum_pct_total"] == pytest.approx(100.0)
    assert summary["expanded_fund_pct"] == pytest.approx(0.0)
    assert summary["n_children_expanded"] == 0
    assert summary["oldest_report_date"] == D_ROOT
    assert summary["report_date"] == D_ROOT
    assert summary["n_holdings"] == 3


# ──────────────────────────────────────────────────────────────────────────────
# Expansão — FoF profundidade 1
# ──────────────────────────────────────────────────────────────────────────────
def test_fof_expansion_depth1_composes_weights_and_staleness():
    fund_map = {"cusip": {"111111111": "S_CHILD"}, "isin": {}}
    data = {
        "S1": (D_ROOT, [
            H(cusip="037833100", issuer="Apple Inc", pct=50.0),
            H(cusip="111111111", issuer="Some Fund", pct=50.0),
        ]),
        "S_CHILD": (D_CHILD, [
            H(cusip="037833100", issuer="Apple Inc", pct=60.0),
            H(cusip="88888XAA1", issuer="Foo Corp", asset="DBT", pct=40.0),
        ]),
    }
    exposures, summary = lt.expand_series("S1", make_get_holdings(data), fund_map)

    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    # Apple: 50 direta + 0.5×60 = 30 indireta
    assert issuer["037833"]["direct_pct"] == pytest.approx(50.0)
    assert issuer["037833"]["indirect_pct"] == pytest.approx(30.0)
    assert issuer["88888X"]["indirect_pct"] == pytest.approx(20.0)
    # a posição do fundo expandido foi SUBSTITUÍDA — não aparece como issuer
    assert "111111" not in issuer

    assert summary["direct_pct"] == pytest.approx(50.0)
    assert summary["indirect_pct"] == pytest.approx(50.0)
    assert summary["expanded_fund_pct"] == pytest.approx(50.0)
    assert summary["sum_pct_total"] == pytest.approx(100.0)
    assert summary["n_children_expanded"] == 1
    assert summary["oldest_report_date"] == D_CHILD  # staleness em cadeia


# ──────────────────────────────────────────────────────────────────────────────
# Expansão — profundidade 2, limite e ciclo
# ──────────────────────────────────────────────────────────────────────────────
def test_depth2_limit_marks_grandchild_fund_nondecomposable():
    fund_map = {"cusip": {"BBBBBBBBB": "S_B", "CCCCCCCCC": "S_C",
                          "DDDDDDDDD": "S_D"}, "isin": {}}
    data = {
        "S_A": (D_ROOT, [H(cusip="BBBBBBBBB", pct=100.0)]),
        "S_B": (D_CHILD, [H(cusip="CCCCCCCCC", pct=100.0)]),
        # S_C ainda tem um fundo (S_D) — mas profundidade 2 já foi atingida
        "S_C": (D_GRAND, [H(cusip="DDDDDDDDD", issuer="Fund D", pct=100.0)]),
        "S_D": (D_GRAND, [H(cusip="037833100", pct=100.0)]),
    }
    exposures, summary = lt.expand_series("S_A", make_get_holdings(data), fund_map)

    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    # S_D NÃO foi expandido (limite de profundidade): fica como exposição
    # indireta no issuer do próprio CUSIP, e soma no não-decomponível.
    assert issuer["DDDDDD"]["indirect_pct"] == pytest.approx(100.0)
    assert "037833" not in issuer
    assert summary["nondecomposable_fund_pct"] == pytest.approx(100.0)
    assert summary["n_children_expanded"] == 2
    assert summary["oldest_report_date"] == D_GRAND


def test_cycle_guard_stops_reexpansion():
    fund_map = {"cusip": {"XXXXXXXXX": "S_X", "YYYYYYYYY": "S_Y"}, "isin": {}}
    data = {
        "S_X": (D_ROOT, [H(cusip="YYYYYYYYY", pct=50.0),
                         H(cusip="037833100", pct=50.0)]),
        "S_Y": (D_CHILD, [H(cusip="XXXXXXXXX", issuer="Fund X", pct=100.0)]),
    }
    exposures, summary = lt.expand_series("S_X", make_get_holdings(data), fund_map)

    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    # Y foi expandido; a posição de Y em X (ciclo) NÃO re-expande → indireta
    # no issuer de X + não-decomponível 50.
    assert issuer["XXXXXX"]["indirect_pct"] == pytest.approx(50.0)
    assert summary["nondecomposable_fund_pct"] == pytest.approx(50.0)
    assert summary["n_children_expanded"] == 1


# ──────────────────────────────────────────────────────────────────────────────
# Chaves sintéticas, shorts, derivativos, residual
# ──────────────────────────────────────────────────────────────────────────────
def test_synthetic_keys_expand_via_isin_and_bucket_unidentified():
    fund_map = {"cusip": {}, "isin": {"IE00B4L5Y983": "S_CHILD"}}
    data = {
        "S1": (D_ROOT, [
            H(cusip="IS:IE00B4L5Y983", issuer="iShares Fund", pct=30.0),
            H(cusip="LE:529900ABCDEF", issuer="Some LEI Co", asset="DBT", pct=20.0),
            H(cusip="H:abc123", issuer="Unknown", asset="LON", pct=10.0),
            H(cusip="999999999", issuer="Real Co", pct=40.0),
        ]),
        "S_CHILD": (D_CHILD, [H(cusip="037833100", issuer="Apple Inc", pct=100.0)]),
    }
    exposures, summary = lt.expand_series("S1", make_get_holdings(data), fund_map)

    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    # IS: casou e expandiu
    assert issuer["037833"]["indirect_pct"] == pytest.approx(30.0)
    # LE:/H: ficam como exposição direta com a própria chave sintética
    assert issuer["LE:529900ABCDEF"]["direct_pct"] == pytest.approx(20.0)
    assert issuer["H:abc123"]["direct_pct"] == pytest.approx(10.0)
    assert issuer["999999"]["direct_pct"] == pytest.approx(40.0)
    # dimensões categóricas cobrem TODAS as posições não-expandidas
    asset = {k[1]: v for k, v in exposures.items() if k[0] == "asset_class"}
    assert asset["DBT"]["direct_pct"] == pytest.approx(20.0)
    assert asset["LON"]["direct_pct"] == pytest.approx(10.0)

    assert summary["unidentified_pct"] == pytest.approx(30.0)  # LE: + H:
    assert summary["expanded_fund_pct"] == pytest.approx(30.0)


def test_shorts_and_derivatives_never_renormalized():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", asset="EC", pct=-20.0),               # short
        H(cusip="111111111", asset="DE", pct=15.0),                # deriv equity
        H(cusip="222222222", asset="DIR", pct=-5.0),               # deriv rates
        H(cusip="333333333", asset="DBT", pct=110.0),              # alavancado
    ])}
    exposures, summary = lt.expand_series("S1", make_get_holdings(data), EMPTY_MAP)

    asset = {k[1]: v for k, v in exposures.items() if k[0] == "asset_class"}
    assert asset["EC"]["direct_pct"] == pytest.approx(-20.0)   # sinal preservado
    assert asset["DBT"]["direct_pct"] == pytest.approx(110.0)  # sem renormalizar
    assert summary["sum_pct_total"] == pytest.approx(100.0)
    assert summary["derivatives_gross_pct"] == pytest.approx(20.0)  # |15|+|−5|
    assert summary["derivatives_net_pct"] == pytest.approx(10.0)    # 15−5


def test_matched_fund_without_data_is_nondecomposable_direct():
    fund_map = {"cusip": {"111111111": "S_NODATA"}, "isin": {}}
    data = {"S1": (D_ROOT, [
        H(cusip="111111111", issuer="Fund w/o N-PORT", pct=25.0),
        H(cusip="037833100", pct=75.0),
    ])}
    exposures, summary = lt.expand_series("S1", make_get_holdings(data), fund_map)

    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    assert issuer["111111"]["direct_pct"] == pytest.approx(25.0)
    assert summary["nondecomposable_fund_pct"] == pytest.approx(25.0)
    assert summary["expanded_fund_pct"] == pytest.approx(0.0)
    assert summary["n_children_expanded"] == 0


def test_null_pct_rows_are_skipped_not_invented():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", pct=100.0),
        H(cusip="594918104", pct=None),
    ])}
    exposures, summary = lt.expand_series("S1", make_get_holdings(data), EMPTY_MAP)
    issuer = {k[1]: v for k, v in exposures.items() if k[0] == "issuer"}
    assert "594918" not in issuer
    assert summary["sum_pct_total"] == pytest.approx(100.0)
    assert summary["n_holdings"] == 2  # contadas, mas sem peso inventado


def test_unknown_categoricals_bucket_explicitly():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", asset=None, sector=None, currency=None, pct=100.0),
    ])}
    exposures, _ = lt.expand_series("S1", make_get_holdings(data), EMPTY_MAP)
    asset = {k[1]: v for k, v in exposures.items() if k[0] == "asset_class"}
    assert asset["UNKNOWN"]["direct_pct"] == pytest.approx(100.0)


# ──────────────────────────────────────────────────────────────────────────────
# Integração — cloud (self-skip)
# ──────────────────────────────────────────────────────────────────────────────
def _cloud_dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        env = pathlib.Path(__file__).resolve().parents[1] / ".env"
        if env.exists():
            for line in env.read_text().splitlines():
                if line.startswith("DATABASE_URL="):
                    dsn = line.split("=", 1)[1].strip().strip('"')
    if not dsn:
        pytest.skip("DATABASE_URL not configured")
    return dsn


def _cloud():
    try:
        return psycopg.connect(_cloud_dsn(), connect_timeout=10)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"cloud unreachable: {exc}")


def test_advisory_lock_is_distinct():
    assert LOCK_NPORT_LOOKTHROUGH == 900_204
    conn = _cloud()
    try:
        with advisory_lock(conn, LOCK_NPORT_LOOKTHROUGH) as got:
            assert got is True
    finally:
        conn.close()


def test_fund_map_built_from_catalog_has_real_edges():
    """O mapa identificador→série construído do catálogo do cloud tem volume."""
    conn = _cloud()
    try:
        fund_map = lt.build_fund_map(conn)
    finally:
        conn.close()
    # Realidade medida (2026-06-12): a aresta dominante são ETFs — ~576 CUSIPs
    # de classe casam ticker em sec_fund_classes/sec_etfs (cobrem 8.261
    # posições só no report 2025-12-31); o mapa isin traz ~5,7k pares do
    # instruments_universe (regra IS:<isin> do ADENDO §6).
    assert len(fund_map["cusip"]) >= 400
    assert len(fund_map["isin"]) >= 3_000
    # nenhum mapeamento para chave sintética
    assert all(not c.startswith(("IS:", "LE:", "H:", "CIK:"))
               for c in list(fund_map["cusip"])[:1000])


def test_run_end_to_end_and_idempotent_on_cloud():
    """run(limit=15) materializa exposições reais no cloud e é idempotente."""
    dsn = _cloud_dsn()
    conn = _cloud()
    conn.close()

    stats1 = lt.run(dsn, limit=15)
    stats2 = lt.run(dsn, limit=15)
    print("\nrun stats (1st):", stats1)
    print("run stats (2nd):", stats2)
    assert stats1["processed"] >= 10
    assert stats1["upserted_series"] >= 10
    assert stats1["upserted_series"] == stats2["upserted_series"]

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT count(*), count(DISTINCT series_id)
            FROM nport_lookthrough_summary
        """)
        n_rows, n_series = cur.fetchone()
        assert n_series >= 10
        # staleness e coverage preenchidos
        cur.execute("""
            SELECT count(*) FROM nport_lookthrough_summary
            WHERE oldest_report_date IS NULL OR sum_pct_total IS NULL
        """)
        assert cur.fetchone()[0] == 0
        # exposições têm as 4 dimensões para pelo menos uma série
        cur.execute("""
            SELECT count(DISTINCT dimension) FROM nport_lookthrough_exposures
        """)
        assert cur.fetchone()[0] >= 4


def test_real_fof_series_expands_on_cloud():
    """Uma série FoF real do cloud expande com indireta > 0 (fixture viva)."""
    conn = _cloud()
    try:
        fund_map = lt.build_fund_map(conn)
        # acha uma série recente cujos holdings casam com o mapa (FoF real)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.series_id, sum(h.pct_of_nav) AS fund_pct
                FROM sec_nport_holdings h
                WHERE h.report_date >= '2025-12-31'
                  AND h.cusip = ANY(%s)
                  AND h.pct_of_nav > 1.0
                GROUP BY h.series_id
                ORDER BY fund_pct DESC
                LIMIT 1
            """, (list(fund_map["cusip"].keys())[:20_000],))
            row = cur.fetchone()
        if not row:
            pytest.skip("nenhuma série FoF encontrada na janela")
        series_id = row[0]
        get_holdings = lt.make_db_get_holdings(conn, _dt.date(2026, 6, 12))
        exposures, summary = lt.expand_series(series_id, get_holdings, fund_map)
        print(f"\nFoF real: {series_id} indirect={summary['indirect_pct']:.2f} "
              f"expanded={summary['expanded_fund_pct']:.2f} "
              f"children={summary['n_children_expanded']}")
        assert summary["n_children_expanded"] >= 1
        assert summary["indirect_pct"] != 0
        assert summary["oldest_report_date"] <= summary["report_date"]
    finally:
        conn.close()
