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
      currency="USD", pct=0.0, payoff_profile=None, investment_country=None):
    return {
        "cusip": cusip, "isin": isin, "issuer_name": issuer,
        "asset_class": asset, "sector": sector, "currency": currency,
        "pct_of_nav": pct, "payoff_profile": payoff_profile,
        "investment_country": investment_country,
    }


def make_get_holdings(data):
    """data: {series_id: (report_date, [holdings])}"""
    return lambda series_id: data.get(series_id)


EMPTY_MAP = {"cusip": {}, "isin": {}}


# ──────────────────────────────────────────────────────────────────────────────
# sector_label — dual-axis por assetCat: ações→GICS/Unclassified, dívida→setor FI
# ──────────────────────────────────────────────────────────────────────────────
def test_sector_label_equity_resolves_gics_else_unclassified():
    smap = {"037833": "Information Technology"}
    # Equity with a mapped issuer → real GICS sector.
    assert lt.sector_label(H(cusip="037833100", asset="EC", sector="CORP"), smap) == "Information Technology"
    # Non-US / unmapped equity → honest "Unclassified", NOT an issuerCat code.
    assert lt.sector_label(H(cusip="700000000", asset="EC", sector="CORP"), smap) == "Unclassified"
    assert lt.sector_label(H(isin="JP1234567890", asset="EP", sector="CORP"), {}) == "Unclassified"


def test_sector_label_debt_splits_by_structure_then_issuer():
    # Plain debt (DBT) split by issuer type.
    assert lt.sector_label(H(asset="DBT", sector="CORP"), {}) == "Corporate Debt"
    assert lt.sector_label(H(asset="DBT", sector="UST"), {}) == "U.S. Treasury"
    assert lt.sector_label(H(asset="DBT", sector="USGSE"), {}) == "U.S. Agency"
    assert lt.sector_label(H(asset="DBT", sector="MUN"), {}) == "Municipal"
    assert lt.sector_label(H(asset="DBT", sector="NUSS"), {}) == "Sovereign (ex-US)"
    # Structured debt → its own FI sector regardless of issuer.
    assert lt.sector_label(H(asset="ABS-MBS", sector="CORP"), {}) == "Mortgage-Backed (MBS)"
    assert lt.sector_label(H(asset="ABS-O", sector="CORP"), {}) == "Asset-Backed (ABS)"
    assert lt.sector_label(H(asset="ABS-CBDO", sector="CORP"), {}) == "CLO/CDO"
    assert lt.sector_label(H(asset="LON", sector="CORP"), {}) == "Bank Loans"
    assert lt.sector_label(H(asset="STIV", sector="CORP"), {}) == "Short-Term / Cash"


def test_sector_label_derivatives_repo_and_ambiguous():
    assert lt.sector_label(H(asset="DE", sector="CORP"), {}) == "Derivatives"
    assert lt.sector_label(H(asset="DFE", sector="OTHER"), {}) == "Derivatives"
    assert lt.sector_label(H(asset="RA", sector="CORP"), {}) == "Repo"
    # Unknown assetCat: only unambiguously-debt issuers map; CORP/None → Other.
    assert lt.sector_label(H(asset=None, sector="UST"), {}) == "U.S. Treasury"
    assert lt.sector_label(H(asset="OTHER", sector="CORP"), {}) == "Other"
    assert lt.sector_label(H(asset=None, sector=None), {}) == "Other"


def test_expand_series_sector_dual_axis():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", asset="EC", sector="CORP", pct=50.0),  # equity → IT
        H(asset="DBT", sector="UST", pct=30.0),                     # treasury debt
        H(asset="ABS-MBS", sector="CORP", pct=20.0),                # MBS
    ])}
    smap = {"037833": "Information Technology"}
    exposures, _ = lt.expand_series(
        "S1", make_get_holdings(data), EMPTY_MAP, sector_map=smap
    )
    sector_keys = {k for (dim, k) in exposures if dim == "sector"}
    assert {"Information Technology", "U.S. Treasury", "Mortgage-Backed (MBS)"} <= sector_keys
    assert "CORP" not in sector_keys and "UST" not in sector_keys


def test_sector_label_equity_falls_back_to_isin_when_cusip6_misses():
    # Foreign equity: the synthetic IS:<isin> cusip never matches a CUSIP-6, but
    # the ISIN is in the enrichment cache (merged into sector_map by ISIN key).
    smap = {"TW0002330008": "Information Technology"}
    h = H(cusip="IS:TW0002330008", isin="TW0002330008", asset="EC", sector="CORP")
    assert lt.sector_label(h, smap) == "Information Technology"
    # No ISIN entry either → honest Unclassified.
    h2 = H(cusip="IS:JP3633400001", isin="JP3633400001", asset="EC", sector="CORP")
    assert lt.sector_label(h2, smap) == "Unclassified"


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


def test_equity_country_dimension_uses_isin_and_keeps_unknown_explicit():
    data = {"S1": (D_ROOT, [
        H(cusip="037833100", isin="US0378331005", asset="EC", pct=60.0),
        H(cusip="G1151C101", isin="GB00B03MLX29", asset="EP", pct=20.0),
        H(cusip="IS:IE00B4L5Y983", isin=None, asset="EC", pct=10.0),
        H(cusip="594918104", isin="XX0000000000", asset="EC", pct=5.0),
        H(cusip="02079K305", isin=None, asset="EC", pct=5.0),
        H(cusip="88888XAA1", isin="US0000000000", asset="DBT", pct=10.0),
    ])}

    exposures, _summary = lt.expand_series("S1", make_get_holdings(data), EMPTY_MAP)

    country = {k[1]: v for k, v in exposures.items() if k[0] == "country"}
    assert country["US"]["direct_pct"] == pytest.approx(60.0)
    assert country["GB"]["direct_pct"] == pytest.approx(20.0)
    assert country["IE"]["direct_pct"] == pytest.approx(10.0)
    assert country["UNKNOWN"]["direct_pct"] == pytest.approx(10.0)
    assert "XX" not in country
    assert sum(cell["direct_pct"] for cell in country.values()) == pytest.approx(100.0)


def test_explicit_country_and_payoff_drive_gross_and_net_equity():
    data = {"S1": (D_ROOT, [
        H(cusip="111111111", isin="US0378331005", pct=80.0,
          payoff_profile="Long", investment_country="GB"),
        H(cusip="222222222", isin="US5949181045", pct=30.0,
          payoff_profile="Short", investment_country="JP"),
    ])}

    exposures, summary = lt.expand_series("S1", make_get_holdings(data), EMPTY_MAP)

    assert exposures[("country", "GB")]["direct_pct"] == pytest.approx(80.0)
    assert exposures[("country", "JP")]["direct_pct"] == pytest.approx(-30.0)
    assert summary["gross_equity_pct"] == pytest.approx(110.0)
    assert summary["net_equity_pct"] == pytest.approx(50.0)


def test_exact_equity_rollup_overrides_collapsed_holding_evidence():
    data = {"S1": (D_ROOT, [
        H(cusip="LE:N/A", isin=None, pct=50.0),
    ])}
    rollup = lt.EquityInputs(
        gross_equity_pct=110.0,
        net_equity_pct=50.0,
        country_exposures_pct={"GB": 80.0, "JP": -30.0},
    )

    exposures, summary = lt.expand_series(
        "S1",
        make_get_holdings(data),
        EMPTY_MAP,
        get_equity_inputs=lambda _series_id, _report_date: rollup,
    )

    assert exposures[("country", "GB")]["direct_pct"] == pytest.approx(80.0)
    assert exposures[("country", "JP")]["direct_pct"] == pytest.approx(-30.0)
    assert ("country", "UNKNOWN") not in exposures
    assert summary["gross_equity_pct"] == pytest.approx(110.0)
    assert summary["net_equity_pct"] == pytest.approx(50.0)


def test_exact_holding_weight_applies_short_sign_to_db_holding_dimensions() -> None:
    data = {
        "S1": (
            D_ROOT,
            [
                H(
                    cusip="30231G102",
                    isin="US30231G1022",
                    asset="EC",
                    pct=30.0,
                )
            ],
        )
    }
    rollup = lt.EquityInputs(
        gross_equity_pct=30.0,
        net_equity_pct=-30.0,
        country_exposures_pct={"US": -30.0},
        signed_holding_weights_pct={"30231G102": -30.0},
    )

    exposures, summary = lt.expand_series(
        "S1",
        make_get_holdings(data),
        EMPTY_MAP,
        get_equity_inputs=lambda _series_id, _report_date: rollup,
    )

    assert exposures[("issuer", "30231G")]["direct_pct"] == pytest.approx(-30.0)
    assert exposures[("asset_class", "EC")]["direct_pct"] == pytest.approx(-30.0)
    assert exposures[("country", "US")]["direct_pct"] == pytest.approx(-30.0)
    assert summary["gross_equity_pct"] == pytest.approx(30.0)
    assert summary["net_equity_pct"] == pytest.approx(-30.0)


def test_exact_rollup_does_not_count_an_expanded_equity_wrapper() -> None:
    fund_map = {"cusip": {"111111111": "S_CHILD"}, "isin": {}}
    data = {
        "S1": (
            D_ROOT,
            [
                H(
                    cusip="111111111",
                    isin="US4642872000",
                    issuer="Equity ETF",
                    asset="EC",
                    pct=40.0,
                ),
                H(
                    cusip="037833100",
                    isin="US0378331005",
                    asset="EC",
                    pct=60.0,
                ),
            ],
        ),
        "S_CHILD": (
            D_CHILD,
            [
                H(
                    cusip="G1151C101",
                    isin="GB00B03MLX29",
                    asset="EC",
                    pct=100.0,
                )
            ],
        ),
    }
    rollups = {
        ("S1", D_ROOT): lt.EquityInputs(100.0, 100.0, {"US": 100.0}),
        ("S_CHILD", D_CHILD): lt.EquityInputs(100.0, 100.0, {"GB": 100.0}),
    }

    exposures, summary = lt.expand_series(
        "S1",
        make_get_holdings(data),
        fund_map,
        get_equity_inputs=lambda sid, report_date: rollups.get((sid, report_date)),
    )

    assert exposures[("country", "US")]["direct_pct"] == pytest.approx(60.0)
    assert exposures[("country", "GB")]["indirect_pct"] == pytest.approx(40.0)
    assert summary["gross_equity_pct"] == pytest.approx(100.0)
    assert summary["net_equity_pct"] == pytest.approx(100.0)


def test_exact_rollup_preserves_collapsed_non_wrapper_equities() -> None:
    fund_map = {"cusip": {"111111111": "S_CHILD"}, "isin": {}}
    data = {
        "S1": (
            D_ROOT,
            [
                H(
                    cusip="111111111",
                    isin="US4642872000",
                    issuer="Equity ETF",
                    asset="EC",
                    pct=40.0,
                ),
                # The cloud PK collapsed a US +70 and CA -10 pair to net +60.
                H(
                    cusip="037833100",
                    isin="US0378331005",
                    asset="EC",
                    pct=60.0,
                ),
            ],
        ),
        "S_CHILD": (
            D_CHILD,
            [
                H(
                    cusip="G1151C101",
                    isin="GB00B03MLX29",
                    asset="EC",
                    pct=100.0,
                )
            ],
        ),
    }
    rollups = {
        ("S1", D_ROOT): lt.EquityInputs(
            120.0,
            100.0,
            {"CA": -10.0, "US": 110.0},
            {"037833100": 60.0, "111111111": 40.0},
            {"037833100": 80.0, "111111111": 40.0},
        ),
        ("S_CHILD", D_CHILD): lt.EquityInputs(100.0, 100.0, {"GB": 100.0}),
    }

    exposures, summary = lt.expand_series(
        "S1",
        make_get_holdings(data),
        fund_map,
        get_equity_inputs=lambda sid, report_date: rollups.get((sid, report_date)),
    )

    assert exposures[("country", "US")]["direct_pct"] == pytest.approx(70.0)
    assert exposures[("country", "CA")]["direct_pct"] == pytest.approx(-10.0)
    assert exposures[("country", "GB")]["indirect_pct"] == pytest.approx(40.0)
    assert summary["gross_equity_pct"] == pytest.approx(120.0)
    assert summary["net_equity_pct"] == pytest.approx(100.0)


def test_exact_rollup_preserves_direct_equities_for_isin_only_wrapper() -> None:
    wrapper_isin = "US4642872000"
    fund_map = {"cusip": {}, "isin": {wrapper_isin: "S_CHILD"}}
    data = {
        "S1": (
            D_ROOT,
            [
                H(isin=wrapper_isin, issuer="Equity ETF", asset="EC", pct=40.0),
                H(
                    cusip="037833100",
                    isin="US0378331005",
                    asset="EC",
                    pct=60.0,
                ),
            ],
        ),
        "S_CHILD": (
            D_CHILD,
            [H(cusip="G1151C101", isin="GB00B03MLX29", asset="EC", pct=100.0)],
        ),
    }
    rollups = {
        ("S1", D_ROOT): lt.EquityInputs(
            120.0,
            20.0,
            {"CA": -10.0, "US": 30.0},
            {"037833100": 60.0, f"IS:{wrapper_isin}": -40.0},
            {"037833100": 80.0, f"IS:{wrapper_isin}": 40.0},
        ),
        ("S_CHILD", D_CHILD): lt.EquityInputs(100.0, 100.0, {"GB": 100.0}),
    }

    exposures, summary = lt.expand_series(
        "S1",
        make_get_holdings(data),
        fund_map,
        get_equity_inputs=lambda sid, report_date: rollups.get((sid, report_date)),
    )

    assert exposures[("country", "US")]["direct_pct"] == pytest.approx(70.0)
    assert exposures[("country", "CA")]["direct_pct"] == pytest.approx(-10.0)
    assert exposures[("country", "GB")]["indirect_pct"] == pytest.approx(-40.0)
    assert summary["gross_equity_pct"] == pytest.approx(120.0)
    assert summary["net_equity_pct"] == pytest.approx(20.0)


def test_exact_rollup_subtracts_offsetting_wrapper_lot_gross() -> None:
    fund_map = {"cusip": {"111111111": "S_CHILD"}, "isin": {}}
    data = {
        "S1": (
            D_ROOT,
            [
                H(cusip="111111111", isin="US4642872000", asset="EC", pct=40.0),
                H(cusip="037833100", isin="US0378331005", asset="EC", pct=60.0),
            ],
        ),
        "S_CHILD": (
            D_CHILD,
            [H(cusip="G1151C101", isin="GB00B03MLX29", asset="EC", pct=100.0)],
        ),
    }
    rollups = {
        # Wrapper lots +60/-20 contribute gross 80 and net 40. The direct
        # equity contributes gross 80 and net 60 after cloud-PK collapse.
        ("S1", D_ROOT): lt.EquityInputs(
            160.0,
            100.0,
            {"CA": -10.0, "US": 110.0},
            {"037833100": 60.0, "111111111": 40.0},
            {"037833100": 80.0, "111111111": 80.0},
        ),
        ("S_CHILD", D_CHILD): lt.EquityInputs(100.0, 100.0, {"GB": 100.0}),
    }

    exposures, summary = lt.expand_series(
        "S1",
        make_get_holdings(data),
        fund_map,
        get_equity_inputs=lambda sid, report_date: rollups.get((sid, report_date)),
    )

    assert exposures[("country", "US")]["direct_pct"] == pytest.approx(70.0)
    assert exposures[("country", "CA")]["direct_pct"] == pytest.approx(-10.0)
    assert exposures[("country", "GB")]["indirect_pct"] == pytest.approx(40.0)
    assert summary["gross_equity_pct"] == pytest.approx(120.0)
    assert summary["net_equity_pct"] == pytest.approx(100.0)


def test_equity_input_getter_reads_summary_and_country_sidecars():
    class Cursor:
        def __init__(self):
            self.query = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, query, _params=None):
            self.query = query

        def fetchone(self):
            if "to_regclass" in self.query:
                return ("nport_equity_holding_weights", True)
            return (110.0, 50.0)

        def fetchall(self):
            if "nport_equity_holding_weights" in self.query:
                return [("30231G102", -30.0, 30.0)]
            return [("GB", 80.0), ("JP", -30.0)]

    class Connection:
        def cursor(self):
            return Cursor()

    getter = lt.make_db_get_equity_inputs(Connection())

    assert getter("S1", D_ROOT) == lt.EquityInputs(
        gross_equity_pct=110.0,
        net_equity_pct=50.0,
        country_exposures_pct={"GB": 80.0, "JP": -30.0},
        signed_holding_weights_pct={"30231G102": -30.0},
        gross_holding_weights_pct={"30231G102": 30.0},
    )


def test_equity_input_getter_tolerates_weight_sidecar_not_yet_provisioned() -> None:
    class Cursor:
        def __init__(self):
            self.query = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, query, _params=None):
            self.query = query
            if "FROM nport_equity_holding_weights" in query:
                raise AssertionError("missing sidecar must not be queried")

        def fetchone(self):
            if "to_regclass" in self.query:
                return (None, False)
            return (30.0, -30.0)

        def fetchall(self):
            return [("US", -30.0)]

    class Connection:
        def cursor(self):
            return Cursor()

    getter = lt.make_db_get_equity_inputs(Connection())

    assert getter("S1", D_ROOT) == lt.EquityInputs(
        gross_equity_pct=30.0,
        net_equity_pct=-30.0,
        country_exposures_pct={"US": -30.0},
    )


def test_summary_contract_persists_gross_and_net_equity():
    schema = (
        pathlib.Path(__file__).resolve().parents[1] / "schemas" / "nport_lookthrough.sql"
    ).read_text(encoding="utf-8")

    assert "gross_equity_pct" in lt._SUMMARY_COLS
    assert "net_equity_pct" in lt._SUMMARY_COLS
    assert "gross_equity_pct" in schema
    assert "net_equity_pct" in schema


def test_equity_country_dimension_preserves_indirect_lookthrough_weight():
    fund_map = {"cusip": {"111111111": "S_CHILD"}, "isin": {}}
    data = {
        "S1": (D_ROOT, [
            H(cusip="037833100", isin="US0378331005", asset="EC", pct=50.0),
            H(cusip="111111111", issuer="Some Fund", asset="RF", pct=50.0),
        ]),
        "S_CHILD": (D_CHILD, [
            H(cusip="G1151C101", isin="GB00B03MLX29", asset="EC", pct=100.0),
        ]),
    }

    exposures, _summary = lt.expand_series("S1", make_get_holdings(data), fund_map)

    country = {k[1]: v for k, v in exposures.items() if k[0] == "country"}
    assert country["US"]["direct_pct"] == pytest.approx(50.0)
    assert country["GB"]["indirect_pct"] == pytest.approx(50.0)


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
