"""nport_lookthrough worker — materialize recursive fund look-through exposures.

Frente C do doc ``2026-06-11-lean-research-rebalance-macro-lookthrough.md``
(§4.3 com o re-escopo do ADENDO §6): lê ``sec_nport_holdings`` (96M linhas) do
data-lake, expande recursivamente as posições que são outros fundos
(profundidade máx. 2, guarda de ciclo) e materializa exposições agregadas por
**emissor (CUSIP-6), asset_class, sector e currency**, separando exposição
direta × indireta, com residual explícito e staleness em cadeia. O Light só
consome as tabelas materializadas (DB-first) — nada disto roda em request path.

Regras do modelo (decisões do doc, não opcionais):
  * peso composto ``w = (pct_parent/100) × pct_child``; sinais preservados
    (shorts negativos); Σpct > 100 (derivativos/alavancagem) NUNCA é
    renormalizado.
  * aresta FoF: identifier do holding → série N-PORT via catálogo
    (``sec_cusip_ticker_map`` × ``sec_fund_classes``/``sec_etfs`` +
    ``instrument_identity``). Chaves sintéticas: ``IS:<isin>`` casa via isin
    (e via CUSIP-9 embutido quando o ISIN é US); ``LE:``/``H:``/``CIK:`` nunca
    casam → permanecem exposição direta e somam em ``unidentified_pct``.
  * fundo casado mas não-expandível (sem dados, ciclo ou limite de
    profundidade) → permanece nas dimensões e soma em
    ``nondecomposable_fund_pct``.
  * derivativos = asset_class N-PORT ``D*`` exceto ``DBT`` → gross (Σ|pct|) e
    net (Σpct) explícitos no summary.
  * ``coverage_pct`` vem COPIADO de ``cagg_nport_series_profile`` — nunca é
    recalculado aqui (ADENDO §6, C2).
  * staleness em cadeia: ``oldest_report_date`` = report mais antigo entre a
    série-mãe e todas as séries efetivamente expandidas.

Contract:  run(dsn, *, calc_date=None, limit=None, serial=False)
           -> {"processed", "upserted_series", "exposure_rows", "calc_date",
               "workers", "identifier_coverage"}

``identifier_coverage`` is the post-verify from
``src.workers.nport_identifier_coverage``: a bounded read of how much of the
recent tail of ``sec_nport_holdings`` still carries an ISIN. It observes, it does
not gate -- see the note at the end of ``run``.
"""

from __future__ import annotations

import datetime as _dt
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

from src.db import LOCK_NPORT_LOOKTHROUGH, advisory_lock, connect
from src.workers import nport_identifier_coverage

MAX_DEPTH = 2
MAX_WORKERS_CAP = 24            # Railway vCPU count; bounds the cloud pool
NUMERIC_14_6_MAX = 99_999_999.999999
INSERT_CHUNK = 1_000

# N-PORT derivative asset categories (DBT is plain debt, NOT a derivative).
DERIVATIVE_CLASSES = {"DE", "DFE", "DFF", "DIR", "DCO", "DCR", "DO"}

SYNTHETIC_PREFIXES = ("IS:", "LE:", "H:", "CIK:")
UNIDENTIFIED_PREFIXES = ("LE:", "H:", "CIK:")

DIMENSIONS = ("issuer", "asset_class", "sector", "currency", "country")

HOLDING_COLS = ("cusip", "isin", "issuer_name", "asset_class", "sector",
                "currency", "pct_of_nav")


@dataclass(frozen=True)
class EquityInputs:
    gross_equity_pct: float
    net_equity_pct: float
    country_exposures_pct: dict[str, float]
    signed_holding_weights_pct: dict[str, float] = field(default_factory=dict)
    gross_holding_weights_pct: dict[str, float] = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Pure engine — no I/O
# ──────────────────────────────────────────────────────────────────────────────
def _clip6(value: float) -> float:
    v = max(-NUMERIC_14_6_MAX, min(NUMERIC_14_6_MAX, float(value)))
    return round(v, 6)


def _embedded_cusip9(isin: str | None) -> str | None:
    """US ISINs embed the 9-char CUSIP at positions 3..11 (US + CUSIP9 + check)."""
    if isin and len(isin) == 12 and isin.startswith("US"):
        return isin[2:11]
    return None


def match_fund(holding: dict, fund_map: dict) -> str | None:
    """Resolve a holding to a child fund series_id, or None.

    Order: real CUSIP-9 → isin map → embedded-US-ISIN CUSIP-9. Synthetic
    ``IS:<isin>`` keys go through the isin paths; ``LE:``/``H:``/``CIK:`` are
    not identifiable as funds by construction (ADENDO §6, C3).
    """
    cusip = holding.get("cusip") or ""
    isin = holding.get("isin")
    if cusip.startswith(UNIDENTIFIED_PREFIXES):
        return None
    if cusip.startswith("IS:"):
        isin = isin or cusip[3:]
        cusip = ""
    if cusip:
        series = fund_map["cusip"].get(cusip)
        if series:
            return series
    if isin:
        series = fund_map["isin"].get(isin)
        if series:
            return series
        embedded = _embedded_cusip9(isin)
        if embedded:
            series = fund_map["cusip"].get(embedded)
            if series:
                return series
    return None


def issuer_key(cusip: str | None, isin: str | None) -> str:
    """Issuer aggregation key: CUSIP-6 when derivable, else the synthetic key.

    Real CUSIPs dedupe at issuer level (first 6 chars). ``IS:US…`` recovers the
    embedded CUSIP-6; other synthetic keys aggregate at security level under
    their own explicit key — never silently merged by issuer_name.
    """
    cusip = cusip or ""
    if cusip and not cusip.startswith(SYNTHETIC_PREFIXES):
        return cusip[:6]
    if cusip.startswith("IS:"):
        embedded = _embedded_cusip9(cusip[3:])
        return embedded[:6] if embedded else cusip
    if cusip:  # LE: / H: / CIK:
        return cusip
    if isin:
        embedded = _embedded_cusip9(isin)
        return embedded[:6] if embedded else f"IS:{isin}"
    return "UNKNOWN"


ISO_3166_ALPHA2 = frozenset("""
AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM
BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX
CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG
GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR
IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV
LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE
NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO
RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF
TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF
WS YE YT ZA ZM ZW
""".split())


def _valid_isin(value: str) -> bool:
    if len(value) != 12 or not value.isalnum() or value[:2] not in ISO_3166_ALPHA2:
        return False
    digits = "".join(char if char.isdigit() else str(ord(char) - 55) for char in value)
    total = 0
    for index, char in enumerate(reversed(digits)):
        digit = int(char)
        if index % 2 == 1:
            digit *= 2
        total += digit // 10 + digit % 10
    return total % 10 == 0


def equity_country_key(
    isin: str | None,
    asset_class: str | None,
    cusip: str | None = None,
    investment_country: str | None = None,
) -> str | None:
    """Return a validated ISIN country prefix for equity holdings only.

    The explicit ``UNKNOWN`` bucket keeps geography coverage measurable by
    consumers. Synthetic ``IS:<isin>`` keys recover a missing ISIN column.
    Non-equity holdings do not participate in this dimension.
    """
    if (asset_class or "") not in _EQUITY_CATS:
        return None
    explicit_country = (investment_country or "").strip().upper()
    if explicit_country:
        return explicit_country if explicit_country in ISO_3166_ALPHA2 else "UNKNOWN"
    candidates = [(isin or "").strip().upper()]
    synthetic = (cusip or "").strip().upper()
    if synthetic.startswith("IS:"):
        candidates.append(synthetic[3:])
    for candidate in candidates:
        if _valid_isin(candidate):
            return candidate[:2]
    return "UNKNOWN"


# The "sector" dimension is DUAL-AXIS, chosen by N-PORT assetCat — a GICS sector
# only makes sense for equities; debt needs a fixed-income taxonomy.
_EQUITY_CATS = frozenset({"EC", "EP"})  # common, preferred
_DERIVATIVE_CATS = frozenset({"DE", "DFE", "DCR", "DIR", "DO"})  # eq/fx/credit/rate/other
# Debt STRUCTURE → FI sector (independent of who issued it).
_FI_STRUCTURE_LABELS = {
    "ABS-MBS": "Mortgage-Backed (MBS)",
    "ABS-O": "Asset-Backed (ABS)",
    "ABS-CBDO": "CLO/CDO",
    "LON": "Bank Loans",
    "STIV": "Short-Term / Cash",
}
# Plain debt (DBT) split by issuer type (the N-PORT ``sector``/issuerCat code).
_DEBT_ISSUER_LABELS = {
    "CORP": "Corporate Debt",
    "UST": "U.S. Treasury",
    "USGA": "U.S. Agency",
    "USGSE": "U.S. Agency",
    "MUN": "Municipal",
    "NUSS": "Sovereign (ex-US)",
}
# issuerCat codes that are unambiguously debt (used when assetCat is unknown).
_DEBT_ISSUER_CODES = frozenset({"UST", "USGA", "USGSE", "MUN", "NUSS"})


def sector_label(holding: dict, sector_map: dict[str, str]) -> str:
    """Dual-axis sector for one holding, chosen by N-PORT assetCat.

    Equities (EC/EP) → the issuer's real GICS sector via ``sector_map`` (issuer
    CUSIP-6), or ``"Unclassified"`` when absent (e.g. non-US names outside the
    US-centric GICS map) — never an issuerCat code, which would misrepresent an
    equity as "Corporate". Debt → a fixed-income sector by structure (MBS / ABS /
    CLO / bank loans / short-term), with plain bonds (DBT) split by issuer type
    (Corporate / Treasury / Agency / Municipal / Sovereign). Derivatives and repo
    get their own buckets. N-PORT's ``sector`` field is the issuerCat code, NOT a
    sector — it is only used to split debt, never as a sector itself.
    """
    asset = (holding.get("asset_class") or "").strip().upper()
    issuer = (holding.get("sector") or "").strip().upper()
    if asset in _EQUITY_CATS:
        # US/large names resolve by issuer CUSIP-6; foreign names (synthetic
        # IS:<isin> cusip) resolve by ISIN via the enrichment cache. Both live
        # in ``sector_map`` (6-char CUSIP-6 keys vs 12-char ISIN keys — no clash).
        gics = sector_map.get(issuer_key(holding.get("cusip"), holding.get("isin")))
        if not gics:
            isin = (holding.get("isin") or "").strip().upper()
            gics = sector_map.get(isin) if isin else None
        return gics or "Unclassified"
    if asset in _FI_STRUCTURE_LABELS:
        return _FI_STRUCTURE_LABELS[asset]
    if asset == "DBT":
        return _DEBT_ISSUER_LABELS.get(issuer, "Debt")
    if asset in _DERIVATIVE_CATS:
        return "Derivatives"
    if asset == "RA":
        return "Repo"
    # Unknown/missing assetCat: only unambiguously-debt issuers map; else Other.
    return _DEBT_ISSUER_LABELS[issuer] if issuer in _DEBT_ISSUER_CODES else "Other"


def expand_series(
    series_id: str,
    get_holdings: Callable[[str], tuple[_dt.date, list[dict]] | None],
    fund_map: dict,
    *,
    sector_map: dict[str, str] | None = None,
    get_equity_inputs: Callable[[str, _dt.date], EquityInputs | None] | None = None,
    max_depth: int = MAX_DEPTH,
) -> tuple[dict, dict]:
    """Recursive look-through of one series. Pure computation over a fetcher.

    ``get_holdings(series_id)`` → ``(report_date, holdings)`` or None.
    Returns ``(exposures, summary)`` where exposures maps
    ``(dimension, key) → {"label", "direct_pct", "indirect_pct"}`` and summary
    carries the explicit residual buckets and chain staleness (docstring above).
    Raises LookupError when the root series has no holdings — fail loud, a
    parent listed for computation must exist.
    """
    sector_map = sector_map or {}
    root = get_holdings(series_id)
    if root is None:
        raise LookupError(f"no holdings for root series {series_id}")
    report_date, root_holdings = root

    exposures: dict[tuple[str, str], dict[str, Any]] = {}
    summary: dict[str, Any] = {
        "report_date": report_date,
        "oldest_report_date": report_date,
        "direct_pct": 0.0,
        "indirect_pct": 0.0,
        "expanded_fund_pct": 0.0,
        "nondecomposable_fund_pct": 0.0,
        "derivatives_gross_pct": 0.0,
        "derivatives_net_pct": 0.0,
        "gross_equity_pct": 0.0,
        "net_equity_pct": 0.0,
        "unidentified_pct": 0.0,
        "n_holdings": len(root_holdings),
        "n_children_expanded": 0,
    }
    expanded_children: set[str] = set()

    def _accumulate(
        holding: dict, pct: float, depth: int, *, has_equity_rollup: bool
    ) -> None:
        side = "direct_pct" if depth == 0 else "indirect_pct"
        keys = [
            ("issuer", issuer_key(holding.get("cusip"), holding.get("isin")),
             holding.get("issuer_name")),
            ("asset_class", holding.get("asset_class") or "UNKNOWN", None),
            ("sector", sector_label(holding, sector_map), None),
            ("currency", holding.get("currency") or "UNKNOWN", None),
        ]
        if not has_equity_rollup:
            country = equity_country_key(
                holding.get("isin"),
                holding.get("asset_class"),
                holding.get("cusip"),
                holding.get("investment_country"),
            )
            if country is not None:
                keys.append(("country", country, None))
        for dimension, key, label in keys:
            cell = exposures.setdefault(
                (dimension, key),
                {"label": None, "direct_pct": 0.0, "indirect_pct": 0.0},
            )
            cell[side] += pct
            if label and not cell["label"]:
                cell["label"] = label
        summary[side] += pct
        if (holding.get("asset_class") or "") in DERIVATIVE_CLASSES:
            summary["derivatives_gross_pct"] += abs(pct)
            summary["derivatives_net_pct"] += pct
        if not has_equity_rollup and (holding.get("asset_class") or "") in _EQUITY_CATS:
            summary["gross_equity_pct"] += abs(pct)
            summary["net_equity_pct"] += pct
        if (holding.get("cusip") or "").startswith(UNIDENTIFIED_PREFIXES):
            summary["unidentified_pct"] += abs(pct)

    def _walk(sid: str, weight: float, depth: int, ancestors: frozenset) -> None:
        rd, holdings = get_holdings(sid)  # guaranteed by caller
        if rd < summary["oldest_report_date"]:
            summary["oldest_report_date"] = rd

        def expandable_child(holding: dict) -> str | None:
            child = match_fund(holding, fund_map)
            if (
                child is not None
                and child not in ancestors
                and depth < max_depth
                and get_holdings(child) is not None
            ):
                return child
            return None

        equity_inputs = get_equity_inputs(sid, rd) if get_equity_inputs else None

        def equity_input_key(holding: dict) -> str:
            holding_key = (holding.get("cusip") or "").strip().upper()
            if (
                holding_key.startswith(SYNTHETIC_PREFIXES)
                or (
                    len(holding_key) == 9
                    and holding_key.isalnum()
                    and set(holding_key) != {"0"}
                )
            ):
                return holding_key
            if holding_isin := (holding.get("isin") or "").strip().upper():
                return f"IS:{holding_isin}"
            return holding_key

        expanded_equity_wrappers = [
            holding
            for holding in holdings
            if (holding.get("asset_class") or "").strip().upper() in _EQUITY_CATS
            and expandable_child(holding) is not None
        ]
        use_equity_rollup = equity_inputs is not None
        rollup_gross = equity_inputs.gross_equity_pct if equity_inputs else 0.0
        rollup_net = equity_inputs.net_equity_pct if equity_inputs else 0.0
        rollup_countries = (
            dict(equity_inputs.country_exposures_pct) if equity_inputs else {}
        )
        seen_wrapper_keys: set[str] = set()
        for wrapper in expanded_equity_wrappers:
            holding_key = equity_input_key(wrapper)
            if holding_key in seen_wrapper_keys:
                continue
            seen_wrapper_keys.add(holding_key)
            exact_pct = (
                equity_inputs.signed_holding_weights_pct.get(holding_key)
                if equity_inputs is not None
                else None
            )
            exact_gross = (
                equity_inputs.gross_holding_weights_pct.get(holding_key)
                if equity_inputs is not None
                else None
            )
            country = equity_country_key(
                wrapper.get("isin"),
                wrapper.get("asset_class"),
                wrapper.get("cusip"),
                wrapper.get("investment_country"),
            )
            if exact_pct is None or exact_gross is None or country not in rollup_countries:
                use_equity_rollup = False
                break
            rollup_gross -= exact_gross
            rollup_net -= exact_pct
            rollup_countries[country] -= exact_pct
        if use_equity_rollup:
            side = "direct_pct" if depth == 0 else "indirect_pct"
            summary["gross_equity_pct"] += rollup_gross * abs(weight)
            summary["net_equity_pct"] += rollup_net * weight
            for country, country_pct in rollup_countries.items():
                if abs(country_pct) < 1e-12:
                    continue
                cell = exposures.setdefault(
                    ("country", country),
                    {"label": None, "direct_pct": 0.0, "indirect_pct": 0.0},
                )
                cell[side] += country_pct * weight
        for holding in holdings:
            raw_pct = holding.get("pct_of_nav")
            if raw_pct is None:
                continue  # sem peso reportado: nada a inventar
            holding_key = equity_input_key(holding)
            exact_pct = (
                equity_inputs.signed_holding_weights_pct.get(holding_key)
                if equity_inputs is not None
                else None
            )
            local_pct = float(exact_pct) if exact_pct is not None else float(raw_pct)
            if exact_pct is None:
                profile = (holding.get("payoff_profile") or "").strip().upper()
                if profile == "SHORT":
                    local_pct = -abs(local_pct)
                elif profile == "LONG":
                    local_pct = abs(local_pct)
            pct = local_pct * weight
            child = match_fund(holding, fund_map)
            expanded_child = expandable_child(holding)
            if expanded_child is not None:
                if depth == 0:
                    summary["expanded_fund_pct"] += pct
                expanded_children.add(expanded_child)
                _walk(expanded_child, weight * local_pct / 100.0, depth + 1,
                      ancestors | {sid})
            else:
                _accumulate(
                    holding,
                    pct,
                    depth,
                    has_equity_rollup=use_equity_rollup,
                )
                if child is not None:
                    summary["nondecomposable_fund_pct"] += abs(pct)

    _walk(series_id, 1.0, 0, frozenset({series_id}))

    summary["n_children_expanded"] = len(expanded_children)
    summary["sum_pct_total"] = summary["direct_pct"] + summary["indirect_pct"]
    return exposures, summary


# ──────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────────────────────────────────────
def build_fund_map(conn) -> dict:
    """identifier → series_id map for the FoF edge, from the cloud catalog.

    CUSIP-9 path: ``sec_cusip_ticker_map`` × tickers of ``sec_fund_classes`` /
    ``sec_etfs`` (the dominant edge: 8k+ matches per quarter), plus the direct
    ``instrument_identity`` pairs. ISIN path: ``sec_etfs.isin`` +
    ``instrument_identity.isin``. ``min(series_id)`` keeps ambiguous
    identifiers deterministic.
    """
    cusip_map: dict[str, str] = {}
    isin_map: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("""
            WITH t2s AS (
                SELECT upper(ticker) AS ticker, series_id
                FROM sec_fund_classes
                WHERE ticker IS NOT NULL AND series_id IS NOT NULL
                UNION
                SELECT upper(ticker), series_id
                FROM sec_etfs
                WHERE ticker IS NOT NULL AND series_id IS NOT NULL
                UNION
                -- SEC company_tickers_mf: authoritative ticker -> series_id for
                -- registered funds/ETFs absent from the N-CEN sec_etfs slice
                -- (e.g. WisdomTree DTD/DEM/DXJ held inside fund-of-funds).
                SELECT upper(ticker), series_id
                FROM sec_company_tickers_mf
                WHERE ticker IS NOT NULL AND series_id IS NOT NULL
            )
            SELECT m.cusip, min(t.series_id)
            FROM sec_cusip_ticker_map m
            JOIN t2s t ON upper(m.ticker) = t.ticker
            GROUP BY m.cusip
        """)
        cusip_map.update(cur.fetchall())
        cur.execute("""
            SELECT cusip_9, min(sec_series_id)
            FROM instrument_identity
            WHERE cusip_9 IS NOT NULL AND sec_series_id IS NOT NULL
            GROUP BY cusip_9
        """)
        cusip_map.update(cur.fetchall())
        cur.execute("""
            SELECT isin, min(series_id) FROM (
                SELECT isin, sec_series_id AS series_id
                FROM instrument_identity
                WHERE isin IS NOT NULL AND sec_series_id IS NOT NULL
                UNION
                SELECT isin, series_id
                FROM sec_etfs
                WHERE isin IS NOT NULL AND series_id IS NOT NULL
                UNION
                SELECT isin, attributes->>'series_id'
                FROM instruments_universe
                WHERE isin IS NOT NULL AND attributes->>'series_id' IS NOT NULL
            ) u GROUP BY isin
        """)
        isin_map.update(cur.fetchall())
    return {"cusip": cusip_map, "isin": isin_map}


def build_sector_map(conn) -> dict[str, str]:
    """Issuer CUSIP-6 → GICS sector, from ``sec_cusip_ticker_map.gics_sector``.

    The map is the GICS-classified (equity) universe; bonds, treasuries and
    munis are absent by construction and fall back to readable issuerCat buckets
    (see ``sector_label``). CUSIP-6 (issuer grain) lets a corporate *bond*
    inherit its issuer's equity sector. First non-null wins per issuer
    (one issuer = one sector).
    """
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cusip, gics_sector FROM sec_cusip_ticker_map "
            "WHERE gics_sector IS NOT NULL AND cusip IS NOT NULL"
        )
        for cusip, gics in cur.fetchall():
            if len(cusip) >= 6:
                out.setdefault(cusip[:6], gics)
        # International equities (synthetic IS:<isin> cusips) resolve by ISIN via
        # the nport_cusip_enrichment cache. 12-char ISIN keys never clash with
        # the 6-char CUSIP-6 keys above. Optional — skip if not yet provisioned.
        if cur.execute("SELECT to_regclass('public.sec_isin_sector')").fetchone()[0]:
            cur.execute(
                "SELECT isin, gics_sector FROM sec_isin_sector "
                "WHERE gics_sector IS NOT NULL AND isin IS NOT NULL"
            )
            for isin, gics in cur.fetchall():
                out.setdefault(isin, gics)
    return out


def make_db_get_holdings(
    conn, calc_date: _dt.date, cache: dict | None = None
) -> Callable[[str], tuple[_dt.date, list[dict]] | None]:
    """Fetcher over sec_nport_holdings: latest report ≤ calc_date per series.

    Memoized — children (popular ETFs) repeat across parents, so each shard
    pays each child series once. Uses the (series_id, report_date DESC) index.
    """
    memo: dict[str, tuple[_dt.date, list[dict]] | None] = (
        cache if cache is not None else {}
    )

    def get_holdings(series_id: str):
        if series_id in memo:
            return memo[series_id]
        with conn.cursor() as cur:
            cur.execute(
                """SELECT max(report_date) FROM sec_nport_holdings
                   WHERE series_id = %s AND report_date <= %s""",
                (series_id, calc_date),
            )
            report_date = cur.fetchone()[0]
            if report_date is None:
                memo[series_id] = None
                return None
            cur.execute(
                f"""SELECT {', '.join(HOLDING_COLS)} FROM sec_nport_holdings
                    WHERE series_id = %s AND report_date = %s""",
                (series_id, report_date),
            )
            holdings = [dict(zip(HOLDING_COLS, row)) for row in cur.fetchall()]
        result = (report_date, holdings)
        memo[series_id] = result
        return result

    return get_holdings


def make_db_get_equity_inputs(
    conn, cache: dict | None = None
) -> Callable[[str, _dt.date], EquityInputs | None]:
    """Memoized reader for exact, uncollapsed equity classification inputs."""

    with conn.cursor() as cur:
        cur.execute(
            """SELECT to_regclass('public.nport_equity_holding_weights'),
                      EXISTS (
                          SELECT 1
                          FROM pg_attribute
                          WHERE attrelid = to_regclass('public.nport_equity_holding_weights')
                            AND attname = 'gross_pct_of_nav'
                            AND NOT attisdropped
                      )"""
        )
        sidecar_state = cur.fetchone()
        holding_weights_available = sidecar_state[0] is not None
        holding_gross_available = len(sidecar_state) > 1 and bool(sidecar_state[1])

    memo: dict[tuple[str, _dt.date], EquityInputs | None] = (
        cache if cache is not None else {}
    )

    def get_equity_inputs(series_id: str, report_date: _dt.date) -> EquityInputs | None:
        key = (series_id, report_date)
        if key in memo:
            return memo[key]
        with conn.cursor() as cur:
            cur.execute(
                """SELECT gross_equity_pct, net_equity_pct
                   FROM nport_equity_exposure_summary
                   WHERE series_id = %s AND report_date = %s""",
                key,
            )
            summary_row = cur.fetchone()
        if summary_row is None:
            memo[key] = None
            return None
        with conn.cursor() as cur:
            cur.execute(
                """SELECT country, direct_pct
                   FROM nport_equity_country_exposures
                   WHERE series_id = %s AND report_date = %s
                   ORDER BY country""",
                key,
            )
            countries = {str(country): float(value) for country, value in cur.fetchall()}
        holding_weights: dict[str, float] = {}
        holding_gross: dict[str, float] = {}
        if holding_weights_available:
            with conn.cursor() as cur:
                if holding_gross_available:
                    cur.execute(
                        """SELECT cusip, signed_pct_of_nav, gross_pct_of_nav
                           FROM nport_equity_holding_weights
                           WHERE series_id = %s AND report_date = %s
                           ORDER BY cusip""",
                        key,
                    )
                    rows = cur.fetchall()
                    holding_weights = {
                        str(cusip): float(signed) for cusip, signed, _gross in rows
                    }
                    holding_gross = {
                        str(cusip): float(gross)
                        for cusip, _signed, gross in rows
                        if gross is not None
                    }
                else:
                    cur.execute(
                        """SELECT cusip, signed_pct_of_nav
                       FROM nport_equity_holding_weights
                       WHERE series_id = %s AND report_date = %s
                       ORDER BY cusip""",
                        key,
                    )
                    holding_weights = {
                        str(cusip): float(value) for cusip, value in cur.fetchall()
                    }
        result = EquityInputs(
            gross_equity_pct=float(summary_row[0]),
            net_equity_pct=float(summary_row[1]),
            country_exposures_pct=countries,
            signed_holding_weights_pct=holding_weights,
            gross_holding_weights_pct=holding_gross,
        )
        memo[key] = result
        return result

    return get_equity_inputs


def _list_parents(conn, calc_date: _dt.date, limit: int | None) -> list[str]:
    """All series with a report ≤ calc_date, biggest (latest n_holdings) first."""
    sql = """
        SELECT series_id FROM (
            SELECT DISTINCT ON (series_id) series_id, n_holdings
            FROM cagg_nport_series_profile
            WHERE report_day <= %s
            ORDER BY series_id, report_day DESC
        ) latest
        ORDER BY n_holdings DESC, series_id
    """
    params: list[Any] = [calc_date]
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def _coverage_pct(conn, series_id: str, report_date: _dt.date) -> float | None:
    """COPY (never recompute) coverage from cagg_nport_series_profile."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT coverage_pct FROM cagg_nport_series_profile
               WHERE series_id = %s AND report_day = %s""",
            (series_id, report_date),
        )
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


_SUMMARY_COLS = (
    "series_id", "report_date", "sum_pct_total", "direct_pct", "indirect_pct",
    "expanded_fund_pct", "nondecomposable_fund_pct", "derivatives_gross_pct",
    "derivatives_net_pct", "gross_equity_pct", "net_equity_pct",
    "unidentified_pct", "coverage_pct", "n_holdings", "n_children_expanded",
    "oldest_report_date",
)


def _upsert_series(conn, series_id: str, exposures: dict, summary: dict,
                   coverage_pct: float | None) -> int:
    """Replace the materialization for (series, report) atomically.

    DELETE + INSERT inside the caller's transaction: recomputation reproduces
    the full row set, so stale keys from a previous run can never linger.
    """
    report_date = summary["report_date"]
    exposure_rows = [
        (series_id, report_date, dimension, key, cell["label"],
         _clip6(cell["direct_pct"]), _clip6(cell["indirect_pct"]))
        for (dimension, key), cell in exposures.items()
    ]
    summary_row = (
        series_id, report_date,
        _clip6(summary["sum_pct_total"]), _clip6(summary["direct_pct"]),
        _clip6(summary["indirect_pct"]), _clip6(summary["expanded_fund_pct"]),
        _clip6(summary["nondecomposable_fund_pct"]),
        _clip6(summary["derivatives_gross_pct"]),
        _clip6(summary["derivatives_net_pct"]),
        _clip6(summary["gross_equity_pct"]),
        _clip6(summary["net_equity_pct"]),
        _clip6(summary["unidentified_pct"]),
        coverage_pct, summary["n_holdings"], summary["n_children_expanded"],
        summary["oldest_report_date"],
    )
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM nport_lookthrough_exposures WHERE series_id = %s AND report_date = %s",
            (series_id, report_date),
        )
        for start in range(0, len(exposure_rows), INSERT_CHUNK):
            cur.executemany(
                """INSERT INTO nport_lookthrough_exposures
                   (series_id, report_date, dimension, key, label,
                    direct_pct, indirect_pct)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                exposure_rows[start:start + INSERT_CHUNK],
            )
        cur.execute(
            "DELETE FROM nport_lookthrough_summary WHERE series_id = %s AND report_date = %s",
            (series_id, report_date),
        )
        cur.execute(
            f"""INSERT INTO nport_lookthrough_summary
                ({', '.join(_SUMMARY_COLS)})
                VALUES ({', '.join(['%s'] * len(_SUMMARY_COLS))})""",
            summary_row,
        )
    return len(exposure_rows)


def ensure_schema(conn) -> None:
    """Apply idempotent look-through and exact-equity input DDL."""
    schema_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "schemas"
    )
    with conn.cursor() as cur:
        for filename in (
            "nport_equity_classification_inputs.sql",
            "nport_lookthrough.sql",
        ):
            with open(os.path.join(schema_dir, filename), encoding="utf-8") as fh:
                cur.execute(fh.read())
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Sharded execution (process-level parallelism — mirrors risk_metrics)
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_max_workers() -> int:
    return min(os.cpu_count() or 4, MAX_WORKERS_CAP)


def _process_shard(
    dsn: str, calc_date_iso: str, fund_map: dict,
    sector_map: dict[str, str], series_ids: list[str]
) -> tuple[int, int, int]:
    """Child-process entrypoint: own connection, per-series commit."""
    calc_date = _dt.date.fromisoformat(calc_date_iso)
    processed = upserted = exposure_rows = 0
    with connect(dsn) as conn:
        get_holdings = make_db_get_holdings(conn, calc_date)
        get_equity_inputs = make_db_get_equity_inputs(conn)
        for series_id in series_ids:
            exposures, summary = expand_series(
                series_id,
                get_holdings,
                fund_map,
                sector_map=sector_map,
                get_equity_inputs=get_equity_inputs,
            )
            coverage = _coverage_pct(conn, series_id, summary["report_date"])
            exposure_rows += _upsert_series(conn, series_id, exposures, summary,
                                            coverage)
            conn.commit()
            processed += 1
            upserted += 1
    return processed, upserted, exposure_rows


def _shard(series_ids: list[str], n_shards: int) -> list[list[str]]:
    """Round-robin (ordered biggest-first) keeps shard workloads balanced."""
    shards: list[list[str]] = [[] for _ in range(n_shards)]
    for i, sid in enumerate(series_ids):
        shards[i % n_shards].append(sid)
    return [s for s in shards if s]


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    serial: bool = False,
) -> dict:
    """Materialize look-through exposures for every series in the data-lake.

    The MAIN process takes LOCK_NPORT_LOOKTHROUGH once, ensures the schema,
    resolves ``calc_date`` (default: max report_date in sec_nport_holdings),
    builds the fund map and the parent list, then dispatches shards to a
    process pool (``min(cpu_count, 24)``); children open their own connections
    and commit per series (DELETE+INSERT atomically per parent).
    """
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_NPORT_LOOKTHROUGH) as got:
            if not got:
                return {"processed": 0, "upserted_series": 0,
                        "skipped": "lock_busy"}

            ensure_schema(conn)
            if calc_date:
                cdate = _dt.date.fromisoformat(calc_date)
            else:
                with conn.cursor() as cur:
                    cur.execute("SELECT max(report_date) FROM sec_nport_holdings")
                    cdate = cur.fetchone()[0]
                if cdate is None:
                    raise RuntimeError("sec_nport_holdings is empty")

            fund_map = build_fund_map(conn)
            sector_map = build_sector_map(conn)
            parents = _list_parents(conn, cdate, limit)
            cdate_iso = cdate.isoformat()

            n_workers = 1 if serial else min(_resolve_max_workers(),
                                             len(parents) or 1)

            if n_workers <= 1:
                processed, upserted, exposure_rows = _process_shard(
                    dsn, cdate_iso, fund_map, sector_map, parents
                )
            else:
                processed = upserted = exposure_rows = 0
                shards = _shard(parents, n_workers)
                with ProcessPoolExecutor(max_workers=n_workers) as pool:
                    futures = [
                        pool.submit(_process_shard, dsn, cdate_iso, fund_map,
                                    sector_map, shard)
                        for shard in shards
                    ]
                    for fut in as_completed(futures):
                        p, u, e = fut.result()
                        processed += p
                        upserted += u
                        exposure_rows += e

            # Post-verify, not a gate. sec_nport_holdings is loaded a DERA
            # package at a time by an operator script outside this repo; a
            # package that lost its ISIN side loads without failing anything,
            # and this worker is the recurring job that reads the column. The
            # verdict rides in the stats so a degraded load is legible in the
            # run log the week it lands. It does NOT raise: the damage is to
            # history already written, so stopping the look-through would cost
            # a week of exposures without repairing a single row.
            coverage = nport_identifier_coverage.probe(conn)

            return {
                "processed": processed,
                "upserted_series": upserted,
                "exposure_rows": exposure_rows,
                "calc_date": cdate_iso,
                "workers": n_workers,
                "identifier_coverage": coverage,
            }
