"""fund_peer_groups worker — EMPIRICAL peer groups from the N-PORT look-through.

WHAT THIS PUBLISHES
-------------------
One row per served fund series per QUARTERLY ANCHOR in ``fund_peer_groups_v1``: the
group the fund's portfolio actually lands in, that group's size and median pairwise
overlap, and — the part the product cannot do without — whether the fund has an
empirical group at all. No label, no name, no description: the surface decides how a
group is presented, this worker decides only what is true about it.

PROVENANCE OF THE RECIPE (ported, not invented)
-----------------------------------------------
Every parameter below is transcribed from a frozen pre-registration and its measured
fact sheet; none of it was chosen here:

  * ``peers_p0``    identifier normalisation (``norm_id``), the served-universe rule
                    and the eligibility floors (>= 10 identified long positions,
                    identified coverage >= 50%).
  * ``peers_p1_5``  the "last report <= anchor, max lag 4 months 15 days" window and
                    the MIXED-granularity ruler (fixed income collapsed to the
                    CUSIP-6 issuer, everything else at CUSIP-9 / ISIN).
  * ``peers_p1_6``  ``step1_partition16.py`` — the partitioner this module ports
                    function for function: hard FI/EQ/MIXED pre-split at 0.70, three
                    disjoint graphs, Louvain (seed 20260805, resolution 1.0,
                    threshold 1e-07), an 8% size cap with recursive re-partition
                    (depth <= 3, resolution ladder [1.0, 2.0, 4.0], seed + depth),
                    and the coherence layer (median intra-group overlap >= 0.05 on
                    the raw mixed ruler, theta = 0.10).
  * ``peers_p1_7``  the eight-anchor stability evidence that ratified the above, and
                    the diagnosis of the identifier hole this worker guards against.

The IDF discount was measured and DISCARDED upstream: the ruler that FORMS the graph
is the ruler that JUDGES it, raw. Do not reintroduce a discount here.

THE ONE PARAMETER THAT IS NOT FROZEN
------------------------------------
The size cap. 8% is what the evidence was cut at, and it is what breaks the US
large-cap complex into four readable groups instead of one — a GRANULARITY DECISION
with a measured price (those four are the least stable groups in the product, and
raising the cap is the lever that would settle them). It is read from
``FUND_PEER_GROUPS_SIZE_CAP`` so a parallel measurement can move it, and it is part of
``params_sha256`` so two anchors computed under different caps can never be silently
compared. Everything else is a module constant on purpose.

THE GUARD THAT EXISTS BECAUSE THE DEFECT ALREADY HAPPENED
----------------------------------------------------------
Filings dated 2024-11-29 .. 2025-01-31 landed with a hole in the identifier
(CUSIP/ISIN) columns: identified rows fell to 83.6%-96.3% where every other report
date in two years sits between 98.5% and 99.7%. The mechanical consequence was that
346 funds — almost all equity — failed the eligibility floors and left the universe,
and the anchor that lost them reported the BEST coverage and the BEST median of the
whole series. A data defect that disguises itself as a quality improvement is exactly
what a pipeline gate has to catch, and the universe-size trigger that was
pre-registered upstream did NOT catch it. So this worker measures identifier coverage
over the anchor's own rows, on rows AND on weight, and REFUSES TO PUBLISH below the
floor, naming both numbers and the worst report dates. That refusal is the point: an
anchor computed on a poisoned quarter must not exist.

FAIL-LOUD ORDER (no side effect precedes any gate)
--------------------------------------------------
 1. resolve the anchor (last CLOSED quarter-end) and the parameters; compute
    ``params_sha256``. No DB.
 2. connect + pin ``search_path`` to public + advisory lock 900_219.
 3. READ-ONLY catalog verification of ``fund_peer_groups_v1``. ``run()`` NEVER
    applies schema — the operator does (``schemas/fund_peer_groups_v1.sql``) — so an
    absent or drifted catalog fails here with zero writes.
 4. the served universe. An empty universe raises NAMING THE RELATION.
 5. the anchor's holdings, streamed; per-series weights, shares and eligibility.
 6. IDENTIFIER COVERAGE GUARD (above). Before the matrix, before any write.
 7. the overlap matrix, the pre-split, Louvain under the cap, the coherence layer.
 8. publish: DELETE the anchor + INSERT, in ONE transaction.
 9. post-write verification: the row count, one re-read row field by field, and the
    invariants re-measured IN THE DATABASE — no group above the cap, no group whose
    stated size disagrees with its member count, no state/NULL inconsistency, one
    single ``computed_at``.

REPRODUCTION OF THE REFERENCE ANCHOR
------------------------------------
``tests/test_fund_peer_groups.py`` pins the partitioner byte for byte against a
committed SYNTHETIC golden partition (three blocks, 42 funds) and pins
``params_sha256`` against a literal digest, so a silent edit to any frozen constant
fails the suite. The FULL 2025-12-31 reference anchor is NOT a repo fixture: it is a
7023 x 7023 float32 matrix (about 197 MB) built from tens of millions of holding
rows, and committing it would put a 200 MB artifact in the repo to re-assert a
number the fact sheet already carries. It is reproduced by an OPERATOR run against
the anchor, compared to the published figures — the procedure is
``docs/fund_peer_groups_runbook.md`` section "Reproducing the reference anchor".
That is a stated trade-off, not an omission.
"""

from __future__ import annotations

import collections
import datetime as _dt
import hashlib
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Iterable

import networkx as nx
import numpy as np
from networkx.algorithms.community import louvain_communities

from src.db import LOCK_FUND_PEER_GROUPS, advisory_lock, connect, resolve_dsn

ROOT = Path(__file__).resolve().parents[2]
TABLE = "fund_peer_groups_v1"

ANCHOR_ENV = "FUND_PEER_GROUPS_ANCHOR"
SIZE_CAP_ENV = "FUND_PEER_GROUPS_SIZE_CAP"
IDENT_FLOOR_ENV = "FUND_PEER_GROUPS_IDENT_FLOOR"
UNIVERSE_DSN_ENV = "FUND_PEER_GROUPS_UNIVERSE_DSN"


class FundPeerGroupsError(RuntimeError):
    """Any refusal of this worker. Every message names the number that caused it."""


# --------------------------------------------------------------------------- #
# The frozen recipe. Do not tune a constant in this block without re-running the
# eight-anchor evidence and recording the amendment: `params_sha256` will change,
# which is the intended alarm, not a nuisance.
# --------------------------------------------------------------------------- #
SEED = 20260805                     # P1.6 §1.2
THETA = 0.10                        # P1.6 §4 — the primary edge threshold
RESOLUTION = 1.0                    # P1.6 §1.2 — first-level resolution, never swept
LOUVAIN_THRESHOLD = 1e-07           # P1.6 §1.2
BLOCK_THRESHOLD = 0.70              # P1.6 §1.1 — the hard FI/EQ/MIXED pre-split
MAX_DEPTH = 3                       # P1.6 §1.3 — recursion budget beyond the block
RESOLUTION_LADDER = (1.0, 2.0, 4.0)  # P1.6 §1.3 — tried in this order, first that fits
COHERENCE_MIN_MEDIAN = 0.05         # P1.6 §2 — a group below this is not a peer group
DEFAULT_SIZE_CAP_FRAC = 0.08        # P1.6 §1.3 — the one product dial (see docstring)
DEFAULT_IDENT_FLOOR = 0.95          # the guard for the known identifier hole

# Universe rule — P0 §0 / P1.5 §1, unchanged across every anchor of the evidence.
MAX_LAG = "4 months 15 days"
MIN_POSITIONS = 10
MIN_COVERAGE = 0.50

# N-PORT asset categories. FI is collapsed to the CUSIP-6 ISSUER on the mixed ruler;
# EQ stays at the security. Everything else (derivatives, "other") keeps its security
# identity and counts toward neither share, so a derivative-heavy book falls to
# 'mixed' rather than being classed by an exposure it does not hold outright.
FI_CODES = frozenset({"DBT", "ABS-MBS", "ABS-CBDO", "ABS-APCP", "ABS-O",
                      "LON", "SN", "RA", "STIV"})
EQ_CODES = frozenset({"EC", "EP"})

# The copy lock, as a mapping. See schemas/fund_peer_groups_v1.sql.
BLOCK_GRANULARITY = {"fi": "issuer", "eq": "security", "mixed": "mixed"}
BLOCK_DB_NAME = {"FI": "fi", "EQ": "eq", "MIXED": "mixed"}

STATE_EMPIRICAL = "empirical"
STATE_NO_GROUP = "no_empirical_group"


# --------------------------------------------------------------------------- #
# Parameters + their digest (Gate 1 — no DB)
# --------------------------------------------------------------------------- #
def _env_fraction(name: str, default: float, *, low: float, high: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise FundPeerGroupsError(f"{name}={raw!r} is not a number") from exc
    if not (low < value <= high):
        raise FundPeerGroupsError(
            f"{name}={value} is outside the admissible range ({low}, {high}]")
    return value


def resolve_size_cap_frac() -> float:
    """The size cap as a fraction of the anchor's universe.

    Bounded below by 1/100 (a cap under 1% would shred every real group) and above by
    1.0 (a cap of 1.0 disables the cap, which is a legitimate — and declared —
    configuration if the owner decides granularity is not worth the instability)."""
    return _env_fraction(SIZE_CAP_ENV, DEFAULT_SIZE_CAP_FRAC, low=0.01, high=1.0)


def resolve_ident_floor() -> float:
    return _env_fraction(IDENT_FLOOR_ENV, DEFAULT_IDENT_FLOOR, low=0.0, high=1.0)


def canonical_params(*, size_cap_frac: float, ident_floor: float) -> dict[str, Any]:
    """Every input that can move the partition, in one canonical dict.

    Anything absent from here is either not an input (the anchor date, which is its
    own column) or not capable of changing the output. Adding a dial without adding it
    here would let two incomparable anchors carry the same digest."""
    return {
        "recipe": "peers_p1_6_partitioner/p1_7_validated",
        "seed": SEED,
        "theta": THETA,
        "resolution": RESOLUTION,
        "louvain_threshold": LOUVAIN_THRESHOLD,
        "block_threshold": BLOCK_THRESHOLD,
        "size_cap_frac": size_cap_frac,
        "max_depth": MAX_DEPTH,
        "resolution_ladder": list(RESOLUTION_LADDER),
        "coherence_min_median": COHERENCE_MIN_MEDIAN,
        "ruler": "mixed:cusip9_equity/cusip6_fixed_income_issuer",
        "idf": False,
        "universe_rule": "last_report<=anchor",
        "max_lag": MAX_LAG,
        "min_positions": MIN_POSITIONS,
        "min_coverage": MIN_COVERAGE,
        "fi_codes": sorted(FI_CODES),
        "eq_codes": sorted(EQ_CODES),
        "identifier_coverage_floor": ident_floor,
        "block_granularity": dict(sorted(BLOCK_GRANULARITY.items())),
    }


def params_sha256(params: dict[str, Any]) -> str:
    canonical = json.dumps(params, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def code_commit() -> str:
    sha = os.environ.get("RAILWAY_GIT_COMMIT_SHA")
    if sha:
        return sha.strip()
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout.strip()


# --------------------------------------------------------------------------- #
# The anchor
# --------------------------------------------------------------------------- #
def last_closed_quarter_end(today: _dt.date) -> _dt.date:
    """The last quarter-end strictly before ``today``'s quarter.

    A quarter in progress has no N-PORT reports to partition, and the quarter that
    just closed needs its filings to have landed — which is why the cron sits on the
    15th of the month AFTER the filing deadline, not on the quarter-end itself."""
    quarter_first_month = 3 * ((today.month - 1) // 3) + 1
    return _dt.date(today.year, quarter_first_month, 1) - _dt.timedelta(days=1)


def resolve_anchor(anchor_arg: str | None = None, *,
                   today: _dt.date | None = None) -> _dt.date:
    """The quarter-end this run publishes.

    An override (argument or ``FUND_PEER_GROUPS_ANCHOR``) must itself be a quarter-end
    and may not be in the future: a mid-quarter partition is a different object, and a
    quarter that has not closed has no reports."""
    if today is None:
        today = _dt.datetime.now(_dt.timezone.utc).date()
    latest = last_closed_quarter_end(today)
    override = anchor_arg or os.environ.get(ANCHOR_ENV)
    if not override:
        return latest
    resolved = _dt.date.fromisoformat(override.strip())
    if resolved != last_closed_quarter_end(resolved + _dt.timedelta(days=1)):
        raise FundPeerGroupsError(
            f"anchor override {resolved.isoformat()} is not a quarter-end; the "
            "partition is quarterly and a mid-quarter anchor does not exist")
    if resolved > latest:
        raise FundPeerGroupsError(
            f"anchor override {resolved.isoformat()} is beyond the last closed "
            f"quarter-end {latest.isoformat()}; a quarter in progress has no reports "
            "to partition")
    return resolved


# --------------------------------------------------------------------------- #
# Gate 2 — search_path
# --------------------------------------------------------------------------- #
def pin_search_path(conn) -> None:
    """Force ``search_path`` to public BEFORE any table access.

    Every relation is referenced bare; a non-public default would resolve them against
    a look-alike schema and this worker would stamp production provenance on it."""
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public")
        cur.execute("SHOW search_path")
        current = (cur.fetchone()[0] or "").replace(" ", "")
    conn.commit()
    if current != "public":
        raise FundPeerGroupsError(
            f"search_path is {current!r} after pinning to public; refusing to run "
            "against a non-public schema")


# --------------------------------------------------------------------------- #
# Gate 3 — READ-ONLY catalog verification
# --------------------------------------------------------------------------- #
# MAINTENANCE: mirrors schemas/fund_peer_groups_v1.sql column for column. It is
# TRANSCRIBED from the committed DDL (not captured from a live catalog), and
# tests/test_fund_peer_groups.py parses that DDL and cross-checks this mapping, so a
# transcription slip fails the suite rather than production.
#   col -> (data_type, character_maximum_length | None, is_nullable, column_default)
EXPECTED_COLUMNS: dict[str, tuple] = {
    "anchor_date": ("date", None, "NO", None),
    "series_id": ("text", None, "NO", None),
    "block": ("text", None, "NO", None),
    "group_id": ("text", None, "YES", None),
    "group_state": ("text", None, "NO", None),
    "group_size": ("integer", None, "YES", None),
    "group_median_overlap": ("numeric", None, "YES", None),
    "granularity": ("text", None, "NO", None),
    "computed_at": ("timestamp with time zone", None, "NO", "now()"),
    "code_commit": ("text", None, "NO", None),
    "params_sha256": ("text", None, "NO", None),
}

CATALOG_COLUMNS_SQL = (
    "SELECT column_name, data_type, character_maximum_length, is_nullable, "
    "column_default FROM information_schema.columns "
    "WHERE table_schema = 'public' AND table_name = %(table)s "
    "ORDER BY ordinal_position")


def verify_schema(conn) -> dict[str, tuple]:
    """Verify the live catalog against the committed DDL (SELECT only).

    Scoped to ``public`` so a scratch look-alike cannot be certified in place of the
    table this worker writes. The full signature is compared — type, length,
    nullability, default — because a dropped DEFAULT or a retyped column is exactly
    the drift a bare name check waves through."""
    with conn.cursor() as cur:
        cur.execute(CATALOG_COLUMNS_SQL, {"table": TABLE})
        rows = cur.fetchall()

    actual = {name: (data_type, char_len, nullable, default)
              for name, data_type, char_len, nullable, default in rows}
    if not actual:
        raise FundPeerGroupsError(
            f"{TABLE}: table missing from the public catalog (apply "
            f"schemas/{TABLE}.sql first — the worker never creates it)")

    problems: list[str] = []
    missing = sorted(set(EXPECTED_COLUMNS) - set(actual))
    unexpected = sorted(set(actual) - set(EXPECTED_COLUMNS))
    if missing or unexpected:
        problems.append(f"{TABLE}: column set diverges from the committed DDL "
                        f"(missing={missing}, unexpected={unexpected})")
    for col in sorted(set(EXPECTED_COLUMNS) & set(actual)):
        if actual[col] != EXPECTED_COLUMNS[col]:
            problems.append(f"{TABLE}.{col}: signature {actual[col]} != expected "
                            f"{EXPECTED_COLUMNS[col]} (type, length, nullable, "
                            "default)")
    if problems:
        raise FundPeerGroupsError(
            "schema catalog verification failed: " + "; ".join(problems))
    return actual


# --------------------------------------------------------------------------- #
# Gate 4 — the served universe
# --------------------------------------------------------------------------- #
# The universe filter is the SERVED set, exactly as the evidence defined it: a series
# the product carries a strategy reading for. Declared limitation, inherited and not
# fixed here: this is TODAY's served set applied to every historical anchor, so a fund
# that died before today enters no anchor at all. In an eight-quarter study that is a
# survivorship bias worth naming; in a forward-looking quarterly publication it is the
# universe the product actually serves.
SERVED_UNIVERSE_SQL = """
    SELECT DISTINCT fp.series_id
    FROM funds_profile_mv fp
    LEFT JOIN fund_risk_latest_mv frl ON frl.instrument_id = fp.instrument_id
    WHERE coalesce(frl.peer_strategy_label, fp.strategy_label) IS NOT NULL
      AND fp.series_id IS NOT NULL
    ORDER BY 1
"""


def read_served_universe(conn) -> list[str]:
    """Every series the product serves a strategy reading for, sorted.

    ``FUND_PEER_GROUPS_UNIVERSE_DSN`` moves this ONE read to another database for the
    deployment where the served read-models and the N-PORT surface do not share a
    logical DB. Unset (the default) it reads through the worker's own connection."""
    dsn = (os.environ.get(UNIVERSE_DSN_ENV) or "").strip()
    if dsn:
        with connect(dsn) as other:
            series = _select_universe(other)
    else:
        series = _select_universe(conn)
    if not series:
        raise FundPeerGroupsError(
            "the served universe is EMPTY: funds_profile_mv / fund_risk_latest_mv "
            "returned no series with a strategy reading. There is nothing to "
            f"partition. If those read-models live in another database, set "
            f"{UNIVERSE_DSN_ENV}")
    return series


def _select_universe(conn) -> list[str]:
    try:
        with conn.cursor() as cur:
            cur.execute(SERVED_UNIVERSE_SQL)
            return [row[0] for row in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001 - re-raised with the relation named
        raise FundPeerGroupsError(
            "could not read the served universe from funds_profile_mv / "
            f"fund_risk_latest_mv: {exc}. If those read-models live in another "
            f"database, set {UNIVERSE_DSN_ENV}") from exc


# --------------------------------------------------------------------------- #
# Gate 5 — the anchor's holdings
# --------------------------------------------------------------------------- #
HOLDINGS_SQL = """
    WITH w AS (
      SELECT series_id, max(report_date) AS r
      FROM sec_nport_holdings
      WHERE report_date <= %(anchor)s
        AND report_date >= (%(anchor)s::date - interval '{lag}')
        AND series_id = ANY(%(series)s)
      GROUP BY series_id
    )
    SELECT h.series_id, h.report_date, h.cusip, h.isin, h.asset_class, h.pct_of_nav
    FROM sec_nport_holdings h
    JOIN w ON w.series_id = h.series_id AND w.r = h.report_date
""".format(lag=MAX_LAG)

# ASCII-only by construction: ``str.isalnum()`` would accept full-width digits and
# other unicode alphanumerics, which are not identifiers. These are the frozen
# recipe's patterns, character for character.
CUSIP_RE = re.compile(r"^[0-9A-Za-z]{9}$")
ISIN_RE = re.compile(r"^[A-Z]{2}[0-9A-Za-z]{10}$")


def _isin_to_id(isin: str) -> str:
    """A US/CA ISIN carries its CUSIP-9 in positions 2..11; fold it onto the CUSIP so
    the same paper reported two ways is ONE identifier, not two."""
    if isin[:2] in ("US", "CA"):
        body = isin[2:11]
        if CUSIP_RE.match(body):
            return "C:" + body.upper()
    return "I:" + isin


def norm_id(cusip: str | None, isin: str | None) -> str | None:
    """The identifier of a holding row, or None when it has none.

    Ported verbatim from ``peers_p0/step1_load.py``. ``IS:<isin>`` is the synthetic
    key the ingest writes into ``cusip`` when only an ISIN was filed; ``LE:``, ``H:``
    and ``CIK:`` are legal-entity / issuer stand-ins that identify no security and
    therefore return None — which is what the identifier-coverage guard measures."""
    c = (cusip or "").strip()
    i = (isin or "").strip().upper()
    if c.upper().startswith("IS:"):
        candidate = c[3:].strip().upper()
        if ISIN_RE.match(candidate):
            return _isin_to_id(candidate)
        c = ""
    if CUSIP_RE.match(c):
        return "C:" + c.upper()
    if ISIN_RE.match(i):
        return _isin_to_id(i)
    return None


def to_mixed(nid: str, asset_class: str | None) -> str:
    """The MIXED ruler: fixed income to the CUSIP-6 issuer, everything else as is."""
    if asset_class in FI_CODES and nid.startswith("C:"):
        return "C6:" + nid[2:8]
    return nid


class IdentifierCoverage:
    """Identifier coverage over the anchor's own long rows, pooled and per report date.

    Two readings, because the defect showed up differently in each: 2024-11-30 passed
    a 95% ROW floor (96.34%) and failed on WEIGHT (93.87%), while 2025-01-31 failed
    both. Guarding on one alone would have let a poisoned date through."""

    def __init__(self) -> None:
        self.rows = 0
        self.rows_identified = 0
        self.weight = 0.0
        self.weight_identified = 0.0
        self.by_date: dict[str, dict[str, float]] = {}
        self.series_by_date: dict[str, int] = {}

    def set_series_counts(self, counts) -> None:
        """How many series each report date represents — the size of the blast radius
        when that date is the poisoned one."""
        self.series_by_date = dict(counts)

    def observe(self, report_date: str, weight: float, identified: bool) -> None:
        bucket = self.by_date.setdefault(
            report_date, {"rows": 0, "rows_identified": 0,
                          "weight": 0.0, "weight_identified": 0.0})
        self.rows += 1
        self.weight += weight
        bucket["rows"] += 1
        bucket["weight"] += weight
        if identified:
            self.rows_identified += 1
            self.weight_identified += weight
            bucket["rows_identified"] += 1
            bucket["weight_identified"] += weight

    @property
    def row_coverage(self) -> float:
        return self.rows_identified / self.rows if self.rows else 0.0

    @property
    def weight_coverage(self) -> float:
        return self.weight_identified / self.weight if self.weight else 0.0

    def per_date(self) -> list[dict[str, Any]]:
        out = []
        for date, b in sorted(self.by_date.items()):
            out.append({
                "report_date": date,
                "series": int(self.series_by_date.get(date, 0)),
                "rows": int(b["rows"]),
                "row_coverage": round(b["rows_identified"] / b["rows"], 6)
                if b["rows"] else 0.0,
                "weight_coverage": round(b["weight_identified"] / b["weight"], 6)
                if b["weight"] else 0.0,
            })
        return sorted(out, key=lambda r: (r["row_coverage"], r["report_date"]))

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "row_coverage": round(self.row_coverage, 6),
            "weight_coverage": round(self.weight_coverage, 6),
            "by_report_date": self.per_date(),
        }


def check_identifier_coverage(coverage: IdentifierCoverage, floor: float,
                              anchor: _dt.date) -> None:
    """Refuse the anchor when identifiers are thin. See the module docstring.

    The refusal names both numbers and the three worst report dates, because the
    operator's next question is always "which filings?" and the answer is what tells
    an ingestion hole apart from a genuinely different quarter."""
    if coverage.rows == 0:
        raise FundPeerGroupsError(
            f"anchor {anchor.isoformat()}: not one long holding row was read for the "
            "served universe; there is no portfolio to compare")
    row_cov, weight_cov = coverage.row_coverage, coverage.weight_coverage
    if row_cov >= floor and weight_cov >= floor:
        return
    worst = coverage.per_date()[:3]
    detail = "; ".join(
        f"{d['report_date']} rows={d['row_coverage']:.4f} "
        f"weight={d['weight_coverage']:.4f} ({d['rows']:,} rows)" for d in worst)
    raise FundPeerGroupsError(
        f"anchor {anchor.isoformat()}: identifier coverage is BELOW the floor "
        f"{floor:.4f} — rows {row_cov:.4f}, weight {weight_cov:.4f} over "
        f"{coverage.rows:,} long holding rows. Worst report dates: {detail}. "
        "Refusing to publish: a filing window with missing CUSIP/ISIN drops exactly "
        "the funds whose books are hardest to identify, which SHRINKS the universe "
        "and INFLATES every quality number of the anchor. Re-ingest the affected "
        f"report dates, then re-run. ({IDENT_FLOOR_ENV} moves the floor, and is part "
        "of params_sha256.)")


def load_anchor(conn, anchor: _dt.date, series: list[str]) -> dict[str, Any]:
    """Stream the anchor's holdings into per-series weight vectors + eligibility.

    The window rule is the frozen one: each series is represented by its LAST report
    at or before the anchor, within ``MAX_LAG``. Shorts are dropped from the book (a
    negative weight is not a shared holding), zero rows are ignored, and an
    unidentified row counts against coverage but never enters a vector."""
    mixed: dict[str, dict[str, float]] = collections.defaultdict(
        lambda: collections.defaultdict(float))
    # The position floor is counted on the SECURITY-level book, not the collapsed one
    # — that is what the frozen recipe counts. A bond fund holding twelve papers from
    # five issuers has twelve positions; counting the five would reject it for a
    # thinness that is an artefact of the ruler.
    securities: dict[str, set[str]] = collections.defaultdict(set)
    report_date: dict[str, str] = {}
    diag: dict[str, dict[str, float]] = collections.defaultdict(
        lambda: {"long_gross": 0.0, "long_ident": 0.0, "fi_w": 0.0, "eq_w": 0.0})
    coverage = IdentifierCoverage()
    n_rows = 0

    with conn.cursor(name="fund_peer_groups_holdings") as cur:
        cur.itersize = 200_000
        cur.execute(HOLDINGS_SQL, {"anchor": anchor, "series": series})
        for sid, rdate, cusip, isin, asset_class, pct in cur:
            n_rows += 1
            report_date[sid] = rdate.isoformat()
            if pct is None:
                continue
            weight = float(pct)
            if weight <= 0:
                continue                    # shorts and empty lines hold nothing
            nid = norm_id(cusip, isin)
            coverage.observe(report_date[sid], weight, nid is not None)
            d = diag[sid]
            d["long_gross"] += weight
            if nid is None:
                continue
            d["long_ident"] += weight
            if asset_class in FI_CODES:
                d["fi_w"] += weight
            elif asset_class in EQ_CODES:
                d["eq_w"] += weight
            securities[sid].add(nid)
            mixed[sid][to_mixed(nid, asset_class)] += weight

    coverage.set_series_counts(collections.Counter(report_date.values()))

    weights: dict[str, dict[str, float]] = {}
    meta: dict[str, dict[str, Any]] = {}
    reject: collections.Counter = collections.Counter()
    reject["no_report_in_window"] = sum(1 for s in series if s not in report_date)

    for sid, book in mixed.items():
        positions = {k: v for k, v in book.items() if v > 0}
        d = diag[sid]
        if len(securities[sid]) < MIN_POSITIONS:
            reject["lt_min_positions"] += 1
            continue
        cov = d["long_ident"] / d["long_gross"] if d["long_gross"] > 0 else 0.0
        if cov < MIN_COVERAGE:
            reject["lt_min_coverage"] += 1
            continue
        total = sum(positions.values())
        weights[sid] = {k: v / total for k, v in positions.items()}
        identified = d["long_ident"] or 1.0
        meta[sid] = {
            "report_date": report_date[sid],
            "fi_share": d["fi_w"] / identified,
            "eq_share": d["eq_w"] / identified,
            "coverage": cov,
            "n_securities": len(securities[sid]),
            "n_positions": len(positions),
        }
    return {"weights": weights, "meta": meta, "coverage": coverage,
            "reject": dict(reject), "holding_rows": n_rows}


# --------------------------------------------------------------------------- #
# Gate 7 — the partitioner (ported from peers_p1_6/step1_partition16.py)
# --------------------------------------------------------------------------- #
def block_of(meta_row: dict[str, Any]) -> str:
    """The hard class pre-split at 70% of identified long weight (P1.6 §1.1)."""
    if meta_row["fi_share"] >= BLOCK_THRESHOLD:
        return "FI"
    if meta_row["eq_share"] >= BLOCK_THRESHOLD:
        return "EQ"
    return "MIXED"


def overlap_matrix(sids: list[str],
                   weights: dict[str, dict[str, float]]) -> np.ndarray:
    """The RAW pairwise overlap ``sum_s min(w_i(s), w_j(s))`` on the mixed ruler.

    Accumulated per identifier rather than per pair: a security held by k funds
    contributes one k x k outer minimum, which is why a 7000-fund anchor builds in
    seconds instead of hours. No IDF, no normalisation — this matrix both FORMS the
    graph and JUDGES the groups it produces."""
    n = len(sids)
    index = {s: i for i, s in enumerate(sids)}
    holders: dict[str, list[tuple[int, float]]] = collections.defaultdict(list)
    for sid in sids:
        i = index[sid]
        for security, w in weights[sid].items():
            holders[security].append((i, w))

    M = np.zeros((n, n), dtype=np.float32)
    for hs in holders.values():
        if len(hs) < 2:
            continue
        ii = np.fromiter((h[0] for h in hs), dtype=np.int64, count=len(hs))
        ww = np.fromiter((h[1] for h in hs), dtype=np.float32, count=len(hs))
        M[np.ix_(ii, ii)] += np.minimum.outer(ww, ww)
    np.fill_diagonal(M, 1.0)
    np.clip(M, 0.0, 1.0, out=M)
    return M


def triangle_values(M: np.ndarray, members: Iterable[int]) -> np.ndarray:
    """Every distinct intra-group pair's overlap, once each."""
    sel = np.asarray(sorted(members), dtype=np.int64)
    k = sel.size
    if k < 2:
        return np.array([], dtype=np.float32)
    sub = M[np.ix_(sel, sel)]
    return np.concatenate([sub[i, i + 1:] for i in range(k - 1)])


def build_block_graph(M: np.ndarray, theta: float,
                      nodes: Iterable[int]) -> nx.Graph:
    """A weighted graph over ``nodes`` only. NO EDGE CROSSES A BLOCK — that is what
    makes bond x equity contamination zero by construction rather than by hope."""
    nodes_arr = np.asarray(sorted(nodes), dtype=np.int64)
    G = nx.Graph()
    G.add_nodes_from(nodes_arr.tolist())
    if nodes_arr.size < 2:
        return G
    sub = M[np.ix_(nodes_arr, nodes_arr)]
    k = nodes_arr.size
    ei, ej, ew = [], [], []
    for i in range(k - 1):
        row = sub[i, i + 1:]
        j = np.nonzero(row >= theta)[0]
        if j.size == 0:
            continue
        ei.append(np.full(j.size, nodes_arr[i], dtype=np.int64))
        ej.append(nodes_arr[j + i + 1])
        ew.append(row[j])
    if ei:
        ei_c = np.concatenate(ei)
        ej_c = np.concatenate(ej)
        ew_c = np.concatenate(ew)
        G.add_weighted_edges_from(zip(ei_c.tolist(), ej_c.tolist(),
                                      ew_c.astype(float).tolist()))
    del sub
    return G


def partition_with_cap(G: nx.Graph, cap: float) -> list[tuple[set, int, str]]:
    """Louvain, then re-partition anything above the cap, recursively (P1.6 §1.2-1.3).

    The stop rule is declared in full and never widened: at most ``MAX_DEPTH`` levels,
    and at each level the FIRST resolution on the fixed ladder that yields >= 2 parts
    whose largest is STRICTLY smaller than the input. If none does, the community is
    marked ``irreducible``, stays whole and stays above the cap — an honest failure
    with a name, not a fourth attempt or a hand-tuned sweep."""
    final: list[tuple[set, int, str]] = []
    stack: list[tuple[set, int]] = [
        (set(part), 0) for part in louvain_communities(
            G, weight="weight", resolution=RESOLUTION,
            threshold=LOUVAIN_THRESHOLD, seed=SEED)]
    while stack:
        part, depth = stack.pop()
        if len(part) <= cap:
            final.append((part, depth, "ok"))
            continue
        if depth >= MAX_DEPTH:
            final.append((part, depth, "depth_exhausted"))
            continue
        sub = G.subgraph(part)
        new_parts = None
        for res in RESOLUTION_LADDER:
            candidate = [set(x) for x in louvain_communities(
                sub, weight="weight", resolution=res,
                threshold=LOUVAIN_THRESHOLD, seed=SEED + depth + 1)]
            if len(candidate) >= 2 and max(len(x) for x in candidate) < len(part):
                new_parts = candidate
                break
        if new_parts is None:
            final.append((part, depth, "irreducible"))
            continue
        for q in new_parts:
            stack.append((q, depth + 1))
    return final


def partition_anchor(sids: list[str], meta: dict[str, dict[str, Any]],
                     M: np.ndarray, *, size_cap_frac: float,
                     theta: float = THETA) -> dict[str, Any]:
    """Pre-split, partition under the cap, then apply the coherence layer.

    Community ids are assigned by SIZE DESCENDING, ties broken by the smallest member
    position — a total order over disjoint sets, so the same anchor always yields the
    same ids whatever order Louvain happened to return its parts in."""
    n = len(sids)
    cap = size_cap_frac * n
    blocks: dict[str, list[int]] = collections.defaultdict(list)
    for i, sid in enumerate(sids):
        blocks[block_of(meta[sid])].append(i)

    raw: list[tuple[list[int], str, int, str]] = []
    modularity: dict[str, float | None] = {}
    edges = 0
    for name in ("FI", "EQ", "MIXED"):
        nodes = blocks.get(name, [])
        if not nodes:
            continue
        G = build_block_graph(M, theta, nodes)
        edges += G.number_of_edges()
        parts = partition_with_cap(G, cap)
        try:
            modularity[name] = round(float(nx.community.modularity(
                G, [frozenset(p) for p, _, _ in parts], weight="weight")), 6)
        except Exception:  # noqa: BLE001 - a block with no edge has no modularity
            modularity[name] = None
        for part, depth, status in parts:
            raw.append((sorted(part), name, depth, status))
        del G

    raw.sort(key=lambda item: (-len(item[0]), item[0][0]))

    communities, community_blocks, statuses = [], [], []
    medians: list[float | None] = []
    coherent: list[bool] = []
    for members, name, depth, status in raw:
        communities.append(members)
        community_blocks.append(name)
        statuses.append({"depth": depth, "status": status})
        values = triangle_values(M, members)
        if values.size == 0:
            medians.append(None)
            coherent.append(False)
            continue
        median = float(np.median(values))
        medians.append(median)
        coherent.append(median >= COHERENCE_MIN_MEDIAN)

    labels = [-1] * n
    for k, members in enumerate(communities):
        for i in members:
            labels[i] = k

    n_coherent_funds = sum(len(communities[k]) for k in range(len(communities))
                           if coherent[k])
    return {
        "communities": communities,
        "blocks": community_blocks,
        "medians": medians,
        "coherent": coherent,
        "statuses": statuses,
        "labels": labels,
        "cap": cap,
        "edges": edges,
        "modularity_by_block": modularity,
        "block_sizes": {b: len(v) for b, v in sorted(blocks.items())},
        "n_communities": len(communities),
        "n_coherent_communities": int(sum(coherent)),
        "n_singletons": int(sum(1 for c in communities if len(c) == 1)),
        "largest_community": max((len(c) for c in communities), default=0),
        "coherent_coverage_pct": round(100.0 * n_coherent_funds / n, 4) if n else 0.0,
        "n_oversized_after_cap": int(sum(1 for c in communities if len(c) > cap)),
        "status_counts": dict(collections.Counter(s["status"] for s in statuses)),
    }


# --------------------------------------------------------------------------- #
# Rows
# --------------------------------------------------------------------------- #
def build_rows(anchor: _dt.date, sids: list[str], partition: dict[str, Any], *,
               computed_at: _dt.datetime, commit: str,
               params_digest: str) -> list[dict[str, Any]]:
    """One row per series. A fund without a coherent community gets the state and
    THREE NULLS — see the DDL header for why the absence is a NULL."""
    rows: list[dict[str, Any]] = []
    for i, sid in enumerate(sids):
        k = partition["labels"][i]
        if k < 0:                       # unreachable: every node is in exactly one part
            raise FundPeerGroupsError(
                f"series {sid} was not assigned to any community at anchor "
                f"{anchor.isoformat()}; the partition does not cover its own universe")
        block = BLOCK_DB_NAME[partition["blocks"][k]]
        is_coherent = partition["coherent"][k]
        rows.append({
            "anchor_date": anchor,
            "series_id": sid,
            "block": block,
            "group_id": f"{anchor.isoformat()}:{block}:{k}" if is_coherent else None,
            "group_state": STATE_EMPIRICAL if is_coherent else STATE_NO_GROUP,
            "group_size": len(partition["communities"][k]) if is_coherent else None,
            "group_median_overlap": partition["medians"][k] if is_coherent else None,
            "granularity": BLOCK_GRANULARITY[block],
            "computed_at": computed_at,
            "code_commit": commit,
            "params_sha256": params_digest,
        })
    return rows


# --------------------------------------------------------------------------- #
# Gate 8 — publish
# --------------------------------------------------------------------------- #
ROW_COLUMNS = ("anchor_date", "series_id", "block", "group_id", "group_state",
               "group_size", "group_median_overlap", "granularity", "computed_at",
               "code_commit", "params_sha256")

DELETE_ANCHOR_SQL = f"DELETE FROM {TABLE} WHERE anchor_date = %(anchor)s"
INSERT_SQL = (
    f"INSERT INTO {TABLE} (" + ", ".join(ROW_COLUMNS) + ") VALUES ("
    + ", ".join(f"%({c})s" for c in ROW_COLUMNS) + ")")
INSERT_CHUNK = 2_000


def publish(conn, anchor: _dt.date, rows: list[dict[str, Any]]) -> int:
    """Replace the anchor: DELETE then INSERT, in ONE transaction.

    Replace rather than upsert because the partition is a WHOLE: a fund can move
    between groups and a group can cease to exist, so merging a new run into an old
    row set would leave orphan members of a group that no longer has a median. Either
    the anchor is entirely this run's, or it is entirely the previous one's."""
    with conn.cursor() as cur:
        cur.execute(DELETE_ANCHOR_SQL, {"anchor": anchor})
        for start in range(0, len(rows), INSERT_CHUNK):
            cur.executemany(INSERT_SQL, rows[start:start + INSERT_CHUNK])
    conn.commit()
    return len(rows)


# --------------------------------------------------------------------------- #
# Gate 9 — post-write verification
# --------------------------------------------------------------------------- #
VERIFY_COUNTS_SQL = f"""
    SELECT count(*),
           count(*) FILTER (WHERE group_state = 'empirical'),
           count(DISTINCT computed_at),
           count(*) FILTER (WHERE (group_state = 'empirical')
                                  <> (group_id IS NOT NULL)),
           coalesce(max(group_size), 0)
    FROM {TABLE} WHERE anchor_date = %(anchor)s
"""
VERIFY_GROUP_SIZES_SQL = f"""
    SELECT group_id, group_size, count(*)
    FROM {TABLE}
    WHERE anchor_date = %(anchor)s AND group_id IS NOT NULL
    GROUP BY group_id, group_size
    HAVING count(*) <> group_size
    LIMIT 5
"""
READBACK_SQL = (
    "SELECT " + ", ".join(ROW_COLUMNS) + f" FROM {TABLE} "
    "WHERE anchor_date = %(anchor)s AND series_id = %(series_id)s")


def _equal(read: Any, computed: Any) -> bool:
    """Compare a re-read value to the computed one across the driver's type shifts.

    NUMERIC comes back as Decimal, DATE as date. The tolerance on the float is ZERO:
    the overlap median is a float32-derived float64 and a mismatch here would be real
    corruption, not rounding."""
    if computed is None or read is None:
        return computed is None and read is None
    if isinstance(computed, float):
        return float(read) == computed
    return read == computed


def post_write_verify(conn, anchor: _dt.date, rows: list[dict[str, Any]],
                      cap: float) -> dict[str, Any]:
    """Re-measure the invariants IN THE DATABASE, not in the objects just built.

    A verification that inspected the Python rows would only prove the worker agrees
    with itself. These read back what the table now holds."""
    expected = len(rows)
    expected_empirical = sum(1 for r in rows if r["group_state"] == STATE_EMPIRICAL)
    with conn.cursor() as cur:
        cur.execute(VERIFY_COUNTS_SQL, {"anchor": anchor})
        total, empirical, distinct_stamps, inconsistent, max_size = cur.fetchone()
        cur.execute(VERIFY_GROUP_SIZES_SQL, {"anchor": anchor})
        size_mismatches = cur.fetchall()

    problems: list[str] = []
    if int(total) != expected:
        problems.append(f"row count {int(total)} != {expected} computed")
    if int(empirical) != expected_empirical:
        problems.append(f"empirical rows {int(empirical)} != {expected_empirical} "
                        "computed")
    if int(distinct_stamps) != 1:
        problems.append(f"{int(distinct_stamps)} distinct computed_at values for one "
                        "anchor; the publication was not atomic")
    if int(inconsistent) != 0:
        problems.append(f"{int(inconsistent)} rows disagree between group_state and "
                        "group_id")
    if size_mismatches:
        detail = ", ".join(f"{g}: stated {s} vs {c} members"
                           for g, s, c in size_mismatches)
        problems.append(f"group_size disagrees with the member count ({detail})")
    if int(max_size) > cap:
        problems.append(f"largest published group is {int(max_size)} members, above "
                        f"the size cap of {cap:.2f}")
    if problems:
        raise FundPeerGroupsError(
            f"post-write verification failed for anchor {anchor.isoformat()}: "
            + "; ".join(problems))

    probe = rows[0]
    with conn.cursor() as cur:
        cur.execute(READBACK_SQL, {"anchor": anchor,
                                   "series_id": probe["series_id"]})
        read = cur.fetchone()
    if read is None:
        raise FundPeerGroupsError(
            f"post-write verification failed: series {probe['series_id']} is not in "
            f"{TABLE} at anchor {anchor.isoformat()} after publishing it")
    for column, value in zip(ROW_COLUMNS, read):
        if not _equal(value, probe[column]):
            raise FundPeerGroupsError(
                f"post-write verification failed: {TABLE}.{column} re-read as "
                f"{value!r}, computed {probe[column]!r}")
    return {"verified_rows": int(total), "verified_empirical": int(empirical),
            "verified_max_group_size": int(max_size)}


# --------------------------------------------------------------------------- #
# run()
# --------------------------------------------------------------------------- #
def run(dsn: str, *, anchor: str | None = None,
        today: _dt.date | None = None) -> dict[str, Any]:
    """Compute and publish one quarterly anchor. See the module docstring for the
    gate ordering; no side effect precedes any gate."""
    t0 = time.monotonic()

    # Gate 1 — parameters, the anchor and the provenance stamp (NO DB, NO compute).
    # The commit is resolved HERE and not at publication time: it can only fail when
    # neither the platform variable nor a git checkout is available, and finding that
    # out after a minute of matrix work would waste the whole run to learn something
    # knowable before the first connection.
    anchor_date = resolve_anchor(anchor, today=today)
    size_cap_frac = resolve_size_cap_frac()
    ident_floor = resolve_ident_floor()
    params = canonical_params(size_cap_frac=size_cap_frac, ident_floor=ident_floor)
    params_digest = params_sha256(params)
    commit = code_commit()

    # Gate 2 — connect + search_path + advisory lock.
    conn = connect(dsn)
    try:
        pin_search_path(conn)
        with advisory_lock(conn, LOCK_FUND_PEER_GROUPS) as got:
            if not got:
                return {"status": "lock_busy"}

            # Gate 3 — READ-ONLY catalog verification.
            verify_schema(conn)

            # Gate 4 — the served universe.
            universe = read_served_universe(conn)

            # Gate 5 — the anchor's holdings.
            loaded = load_anchor(conn, anchor_date, universe)
            weights, meta = loaded["weights"], loaded["meta"]

            # Gate 6 — the identifier guard, BEFORE the matrix and before any write.
            check_identifier_coverage(loaded["coverage"], ident_floor, anchor_date)

            # The reads are done. END THE READ TRANSACTION before the compute: the
            # matrix and the partition take about a minute at production size, and a
            # snapshot held open across them pins backend_xmin and stalls VACUUM on
            # a hypertable this database cannot afford to leave un-vacuumed. The
            # publication below opens its own transaction and is atomic on its own.
            conn.commit()

            if not weights:
                raise FundPeerGroupsError(
                    f"anchor {anchor_date.isoformat()}: not one of the "
                    f"{len(universe):,} served series passed the eligibility floors "
                    f"(>= {MIN_POSITIONS} identified long positions, identified "
                    f"coverage >= {MIN_COVERAGE:.0%}). Rejections: "
                    f"{loaded['reject']}")

            # Gate 7 — the matrix, the pre-split, the partition, the coherence layer.
            sids = sorted(weights)
            M = overlap_matrix(sids, weights)
            partition = partition_anchor(sids, meta, M,
                                         size_cap_frac=size_cap_frac)
            del M

            computed_at = _dt.datetime.now(_dt.timezone.utc)
            rows = build_rows(anchor_date, sids, partition,
                              computed_at=computed_at, commit=commit,
                              params_digest=params_digest)

            # Gate 8 — publish (atomic).
            published = publish(conn, anchor_date, rows)

            # Gate 9 — post-write verification.
            verified = post_write_verify(conn, anchor_date, rows, partition["cap"])

            return {
                "status": "published",
                "anchor_date": anchor_date.isoformat(),
                "n_served_universe": len(universe),
                "n_universe": len(sids),
                "n_published": published,
                "reject": loaded["reject"],
                "holding_rows": loaded["holding_rows"],
                "identifier_coverage": loaded["coverage"].as_dict(),
                "block_sizes": partition["block_sizes"],
                "edges": partition["edges"],
                "modularity_by_block": partition["modularity_by_block"],
                "n_communities": partition["n_communities"],
                "n_coherent_communities": partition["n_coherent_communities"],
                "n_singletons": partition["n_singletons"],
                "largest_community": partition["largest_community"],
                "size_cap_nodes": round(partition["cap"], 4),
                "n_oversized_after_cap": partition["n_oversized_after_cap"],
                "community_status_counts": partition["status_counts"],
                "coherent_coverage_pct": partition["coherent_coverage_pct"],
                "size_cap_frac": size_cap_frac,
                "identifier_coverage_floor": ident_floor,
                "params_sha256": params_digest,
                "computed_at": computed_at.isoformat(),
                **verified,
                "wall_ms": int((time.monotonic() - t0) * 1000),
            }
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(prog="python -m src.workers.fund_peer_groups")
    parser.add_argument("--anchor", default=None,
                        help="quarter-end to publish (default: the last closed "
                             "quarter-end)")
    args = parser.parse_args(argv)
    print(json.dumps(run(resolve_dsn(), anchor=args.anchor), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["run", "main", "FundPeerGroupsError", "block_of", "build_block_graph",
           "build_rows", "canonical_params", "check_identifier_coverage",
           "code_commit", "last_closed_quarter_end", "load_anchor", "norm_id",
           "overlap_matrix", "params_sha256", "partition_anchor",
           "partition_with_cap", "pin_search_path", "post_write_verify", "publish",
           "read_served_universe", "resolve_anchor", "resolve_ident_floor",
           "resolve_size_cap_frac", "to_mixed", "triangle_values", "verify_schema",
           "EXPECTED_COLUMNS", "ROW_COLUMNS", "TABLE"]
