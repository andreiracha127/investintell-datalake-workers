"""Versioned composer for the app-owned ``fund_regulatory_serving_*`` layer.

WHY THIS EXISTS
---------------
The app-owned composition layer pins one immutable worker publication and maps
every app instrument onto the ``(series_id, class_id)`` grain the serving facts
are published at.  Until this module existed the layer had no composer at all:
app publication ``bcee45f9-0b64-5903-8dcd-f3067ab2fb80`` (v1) was written by an
unversioned ops one-off which resolved ``ncen_fund_id`` for all 7 375
instruments but never attempted ``class_id`` -- every row landed with
``class_id=''`` and ``mapping_state='class_context_ambiguous'``.  That single
omission silently disabled the fee sidecar (``f.class_id IN (m.class_id,'')``
collapses to the series fallback) and the mandate/operating sidecars (their
views filter ``mapping_state='resolved'``).

WHAT IT DOES
------------
Composes a NEW app publication (the layer is append-only: publications are
immutable once validated, and mappings/artifacts only accept writes while the
parent is ``prepared``) that carries:

  * the SAME worker artifact pins as the base publication -- one variable
    changes, so the flip is A/B-comparable against the base; and
  * mappings whose ``class_id`` is resolved by the cascade below.

CLASS RESOLUTION CASCADE (measured on the mirror, 2026-08-01/02)
---------------------------------------------------------------
  1. ``instrument_identity.sec_class_id``            -> 4 590 / 7 375
  2. ``sec_company_tickers_mf`` by ``(series_id, upper(ticker))``  -> +2 783
  3. ``sec_series_class_catalog`` by ``(series_id, upper(class_ticker))`` -> +1
  => 7 374 / 7 375 resolved, with ZERO disagreement between sources.

Every source is filtered to ``class_id LIKE 'C%'`` and to single-valued
``(series_id, ticker)`` groups.  A ticker that maps to more than one class in a
source is NOT resolved from that source: that is the legitimate use of
``class_context_ambiguous``.

POISONED SOURCES -- NEVER USE
-----------------------------
``sec_fund_classes`` / ``fund_classes_latest_mv`` carry 11 773 rows whose
``class_id`` column holds the SERIES id (``S000000362`` where a ``C000...`` is
expected).  A composer trusting them writes a series id into ``class_id``; the
join against the ``rr1_fee`` facts then yields exactly zero hits (measured).
These relations are named here only so the guard test can assert they never
appear in the composition SQL.

IDENTITY / IDEMPOTENCE
----------------------
``app_publication_id`` is a UUIDv5 over (recipe revision, artifact pins,
content fingerprint of the resolved mappings) -- the same identity discipline
the worker side uses in ``sec_serving.materializer.publication_id_for``.  Same
inputs re-run => same id => explicit no-op.  Changed sources => different id =>
a new publication, never an in-place mutation.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Sequence
from uuid import UUID, uuid5

import psycopg

# Distinct from the worker-side serving namespace: this mints APP publication
# ids, not worker publication ids.
_APP_NAMESPACE = UUID("6f1c0f5a-2d47-5c8e-9a31-7c0d5b8e4a12")

# Bump when the cascade recipe changes semantics; it is part of the identity so
# a recipe change can never be mistaken for a source refresh.
MAPPING_RECIPE_REVISION = "class_cascade_v1"

# Relations that must never be consulted (see module docstring).
POISONED_CLASS_SOURCES: tuple[str, ...] = ("sec_fund_classes", "fund_classes_latest_mv")

# The four worker products the app manifest pins.
ARTIFACT_PRODUCTS: tuple[str, ...] = (
    "ncen_operating_profile_v1",
    "regulatory_mandate",
    "rr1_fee_profile_v1",
    "sec_regulatory_serving_v1",
)

CLASS_SOURCE_IDENTITY = "instrument_identity"
CLASS_SOURCE_TICKERS = "sec_company_tickers_mf"
CLASS_SOURCE_CATALOG = "sec_series_class_catalog"
CLASS_SOURCE_UNRESOLVED = "unresolved"


class CompositionError(RuntimeError):
    """Raised when the composition cannot proceed or its gate fails."""


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------

# One shared cascade projection.  Kept as a single string so the guard test can
# assert, on the exact text the composer executes, that no poisoned relation is
# consulted.  ``%%`` escapes are literal ``%`` under pyformat parameters.
CASCADE_SQL = """
WITH base AS (
    SELECT m.instrument_id,
           m.series_id,
           m.ncen_fund_id,
           upper(btrim(coalesce(ii.ticker, ''))) AS ticker,
           btrim(coalesce(ii.sec_class_id, '')) AS identity_class_raw,
           CASE WHEN btrim(coalesce(ii.sec_class_id, '')) LIKE 'C%%'
                THEN btrim(ii.sec_class_id) END AS identity_class_id
      FROM fund_regulatory_serving_instrument_mappings m
      JOIN instrument_identity ii ON ii.instrument_id = m.instrument_id
     WHERE m.app_publication_id = %(base_app)s
), ticker_classes AS (
    SELECT series_id,
           upper(btrim(ticker)) AS ticker,
           min(class_id) AS class_id,
           count(DISTINCT class_id) AS variants
      FROM sec_company_tickers_mf
     WHERE class_id LIKE 'C%%' AND btrim(coalesce(ticker, '')) <> ''
     GROUP BY 1, 2
), catalog_classes AS (
    SELECT series_id,
           upper(btrim(class_ticker)) AS ticker,
           min(class_id) AS class_id,
           count(DISTINCT class_id) AS variants
      FROM sec_series_class_catalog
     WHERE class_id LIKE 'C%%' AND btrim(coalesce(class_ticker, '')) <> ''
     GROUP BY 1, 2
), joined AS (
    SELECT b.instrument_id,
           b.series_id,
           b.ncen_fund_id,
           b.ticker,
           b.identity_class_raw,
           b.identity_class_id,
           CASE WHEN t.variants = 1 THEN t.class_id END AS ticker_class_id,
           CASE WHEN c.variants = 1 THEN c.class_id END AS catalog_class_id,
           coalesce(t.variants, 0) AS ticker_variants,
           coalesce(c.variants, 0) AS catalog_variants
      FROM base b
      LEFT JOIN ticker_classes t
        ON t.series_id = b.series_id AND t.ticker = b.ticker
      LEFT JOIN catalog_classes c
        ON c.series_id = b.series_id AND c.ticker = b.ticker
)
SELECT instrument_id,
       series_id,
       ncen_fund_id,
       ticker,
       identity_class_raw,
       identity_class_id,
       ticker_class_id,
       catalog_class_id,
       ticker_variants,
       catalog_variants,
       coalesce(identity_class_id, ticker_class_id, catalog_class_id) AS class_id
  FROM joined
 ORDER BY instrument_id
"""

_INSERT_MAPPING_SQL = """
INSERT INTO fund_regulatory_serving_instrument_mappings
    (app_publication_id, instrument_id, series_id, class_id, ncen_fund_id, mapping_state)
VALUES (%s, %s, %s, %s, %s, %s)
"""

_MAPPING_METRICS_SQL = """
SELECT count(*) AS mappings,
       count(*) FILTER (WHERE mapping_state = 'resolved') AS resolved,
       count(*) FILTER (WHERE class_id LIKE 'C%%') AS class_grain,
       count(*) FILTER (WHERE class_id = series_id) AS class_equals_series
  FROM fund_regulatory_serving_instrument_mappings
 WHERE app_publication_id = %(app)s
"""

# Mirrors the ``rr1_fee`` class-grain leg of ``fund_regulatory_serving_facts_v``
# but bound to an EXPLICIT app publication, because the view reads the current
# pointer and the gate must run BEFORE the pointer moves.
_FEE_MATCH_SQL = """
SELECT count(DISTINCT m.instrument_id)
  FROM fund_regulatory_serving_instrument_mappings m
  JOIN fund_regulatory_serving_artifacts a
    ON a.app_publication_id = m.app_publication_id
   AND a.product = 'sec_regulatory_serving_v1'
  JOIN sec_regulatory_serving_facts f
    ON f.publication_id = a.worker_publication_id
   AND f.family = 'rr1_fee'
   AND f.grain_origin = 'class'
   AND f.series_id = m.series_id
   AND f.class_id IN (m.class_id, '')
 WHERE m.app_publication_id = %(app)s
   AND m.mapping_state = 'resolved'
"""

# Mirrors ``fund_regulatory_mandate_profiles_v``'s selection_state='published'
# arm, likewise bound to an explicit app publication.
_MANDATE_PUBLISHED_SQL = """
WITH candidate_pool AS (
    SELECT m.instrument_id,
           CASE WHEN x.class_id = m.class_id THEN 0 ELSE 1 END AS priority,
           x.document_id
      FROM fund_regulatory_serving_instrument_mappings m
      JOIN fund_regulatory_serving_artifacts a
        ON a.app_publication_id = m.app_publication_id
       AND a.product = 'regulatory_mandate'
      JOIN sec_regulatory_mandate_profiles x
        ON x.publication_id = a.worker_publication_id
       AND x.series_id = m.series_id
       AND x.class_id IN (m.class_id, '')
     WHERE m.app_publication_id = %(app)s
       AND m.mapping_state = 'resolved'
), ranked AS (
    SELECT *, min(priority) OVER (PARTITION BY instrument_id) AS best_priority
      FROM candidate_pool
), selected AS (
    SELECT instrument_id,
           count(*) OVER (PARTITION BY instrument_id) AS candidate_count,
           row_number() OVER (PARTITION BY instrument_id ORDER BY document_id) AS candidate_number
      FROM ranked
     WHERE priority = best_priority
)
SELECT count(*)
  FROM fund_regulatory_serving_instrument_mappings m
  JOIN selected s ON s.instrument_id = m.instrument_id AND s.candidate_number = 1
 WHERE m.app_publication_id = %(app)s
   AND m.mapping_state = 'resolved'
   AND s.candidate_count = 1
"""


# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class BasePublication:
    """The app publication the composition copies its worker pins from."""

    app_publication_id: UUID
    app_publication_version: int
    worker_publication_id: UUID
    worker_publication_version: int
    artifacts: tuple[tuple[str, UUID, int], ...]

    def pin_key(self) -> str:
        parts = [f"{p}:{wid}:{ver}" for p, wid, ver in sorted(self.artifacts, key=lambda a: a[0])]
        return "|".join([f"root:{self.worker_publication_id}:{self.worker_publication_version}", *parts])


@dataclass(frozen=True)
class CascadeRow:
    """One instrument with every class candidate the cascade found."""

    instrument_id: UUID
    series_id: str
    ncen_fund_id: str | None
    ticker: str
    identity_class_raw: str
    identity_class_id: str | None
    ticker_class_id: str | None
    catalog_class_id: str | None
    ticker_variants: int
    catalog_variants: int
    class_id: str | None

    @property
    def class_source(self) -> str:
        if self.identity_class_id is not None:
            return CLASS_SOURCE_IDENTITY
        if self.ticker_class_id is not None:
            return CLASS_SOURCE_TICKERS
        if self.catalog_class_id is not None:
            return CLASS_SOURCE_CATALOG
        return CLASS_SOURCE_UNRESOLVED

    @property
    def mapping_state(self) -> str:
        """The only confidence expression the mapping table carries.

        ``fund_regulatory_serving_instrument_mappings`` has no confidence
        column: its vocabulary is exactly ``resolved`` /
        ``class_context_ambiguous``.  A resolved class means every consulted
        source agreed and the ``(series, ticker)`` group was single-valued; the
        irreducible tail keeps the honest ambiguous state.
        """
        return "resolved" if self.class_id else "class_context_ambiguous"

    @property
    def disagrees(self) -> bool:
        found = [c for c in (self.identity_class_id, self.ticker_class_id, self.catalog_class_id) if c]
        return len(set(found)) > 1


@dataclass(frozen=True)
class CascadeReport:
    """Source-level quality of one cascade run (computed off the fetched rows)."""

    instruments: int
    resolved: int
    from_identity: int
    from_tickers: int
    from_catalog: int
    unresolved: int
    disagreements: int
    multi_class_tickers: int
    identity_non_class_values: int
    unresolved_instruments: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "instruments": self.instruments,
            "resolved": self.resolved,
            "from_identity": self.from_identity,
            "from_tickers": self.from_tickers,
            "from_catalog": self.from_catalog,
            "unresolved": self.unresolved,
            "disagreements": self.disagreements,
            "multi_class_tickers": self.multi_class_tickers,
            "identity_non_class_values": self.identity_non_class_values,
            "unresolved_instruments": list(self.unresolved_instruments),
        }


@dataclass(frozen=True)
class GateThresholds:
    """Pre-flip acceptance gate.  Defaults are the measured v1 baseline."""

    mappings: int = 7375
    resolved: int = 7374
    class_grain: int = 7374
    fee_matched_instruments: int = 7273
    mandate_published: int = 7370
    max_disagreements: int = 0


@dataclass
class GateResult:
    metrics: dict[str, Any] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


@dataclass
class CompositionResult:
    app_publication_id: UUID
    app_publication_version: int
    base: BasePublication
    fingerprint: str
    created: bool
    cascade: CascadeReport
    gate: GateResult
    validated: bool = False
    promoted: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "app_publication_id": str(self.app_publication_id),
            "app_publication_version": self.app_publication_version,
            "base_app_publication_id": str(self.base.app_publication_id),
            "base_app_publication_version": self.base.app_publication_version,
            "recipe_revision": MAPPING_RECIPE_REVISION,
            "mapping_fingerprint": self.fingerprint,
            "created": self.created,
            "cascade": self.cascade.as_dict(),
            "gate_metrics": self.gate.metrics,
            "gate_failures": self.gate.failures,
            "gate_ok": self.gate.ok,
            "validated": self.validated,
            "promoted": self.promoted,
            "rollback_sql": rollback_sql(self.base.app_publication_id),
        }


# --------------------------------------------------------------------------
# Composition steps
# --------------------------------------------------------------------------


def rollback_sql(app_publication_id: UUID) -> str:
    """The one statement that reverts the flip (no data is touched)."""
    return (
        "SELECT fund_set_current_regulatory_serving_publication_v2("
        f"'{app_publication_id}'::uuid);"
    )


def read_base_publication(conn: psycopg.Connection, base_app_id: UUID | None = None) -> BasePublication:
    """Read the publication whose worker pins the new composition inherits.

    Default: the publication the app pointer currently serves; if no pointer is
    set, the highest ``app_publication_version``.
    """
    with conn.cursor() as cur:
        if base_app_id is None:
            cur.execute("SELECT app_publication_id FROM fund_regulatory_serving_app_current_pointer")
            row = cur.fetchone()
            if row is not None:
                base_app_id = row[0]
        if base_app_id is None:
            cur.execute(
                "SELECT app_publication_id FROM fund_regulatory_serving_publications"
                " ORDER BY app_publication_version DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                raise CompositionError("no base app publication to compose from")
            base_app_id = row[0]

        cur.execute(
            "SELECT app_publication_id, app_publication_version, worker_publication_id,"
            " worker_publication_version, lifecycle_state"
            " FROM fund_regulatory_serving_publications WHERE app_publication_id = %s",
            (base_app_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise CompositionError(f"base app publication {base_app_id} does not exist")
        if row[4] != "validated":
            raise CompositionError(
                f"base app publication {base_app_id} is {row[4]!r}; only a validated base may be inherited"
            )
        cur.execute(
            "SELECT product, worker_publication_id, worker_publication_version"
            " FROM fund_regulatory_serving_artifacts WHERE app_publication_id = %s ORDER BY product",
            (base_app_id,),
        )
        artifacts = tuple((p, w, v) for p, w, v in cur.fetchall())
    if not artifacts:
        raise CompositionError(f"base app publication {base_app_id} pins no worker artifacts")
    return BasePublication(row[0], row[1], row[2], row[3], artifacts)


def resolve_class_cascade(conn: psycopg.Connection, base_app_id: UUID) -> list[CascadeRow]:
    """Run the cascade and return one row per instrument of the base publication."""
    with conn.cursor() as cur:
        cur.execute(CASCADE_SQL, {"base_app": base_app_id})
        rows = cur.fetchall()
    return [CascadeRow(*row) for row in rows]


def summarize_cascade(rows: Sequence[CascadeRow]) -> CascadeReport:
    unresolved = [str(r.instrument_id) for r in rows if r.class_id is None]
    return CascadeReport(
        instruments=len(rows),
        resolved=sum(1 for r in rows if r.class_id),
        from_identity=sum(1 for r in rows if r.class_source == CLASS_SOURCE_IDENTITY),
        from_tickers=sum(1 for r in rows if r.class_source == CLASS_SOURCE_TICKERS),
        from_catalog=sum(1 for r in rows if r.class_source == CLASS_SOURCE_CATALOG),
        unresolved=len(unresolved),
        disagreements=sum(1 for r in rows if r.disagrees),
        multi_class_tickers=sum(1 for r in rows if r.ticker_variants > 1 or r.catalog_variants > 1),
        identity_non_class_values=sum(
            1 for r in rows if r.identity_class_id is None and r.identity_class_raw
        ),
        unresolved_instruments=tuple(sorted(unresolved)),
    )


def mapping_fingerprint(rows: Sequence[CascadeRow]) -> str:
    """Content address of exactly what the composer will write."""
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda r: str(r.instrument_id)):
        digest.update(
            "|".join(
                (
                    str(row.instrument_id),
                    row.series_id,
                    row.class_id or "",
                    row.ncen_fund_id or "",
                    row.mapping_state,
                )
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def app_publication_id_for(base: BasePublication, fingerprint: str) -> UUID:
    """Deterministic UUIDv5 identity over recipe + worker pins + written content."""
    return uuid5(
        _APP_NAMESPACE,
        f"fund_regulatory_serving|{MAPPING_RECIPE_REVISION}|{base.pin_key()}|{fingerprint}",
    )


def _next_version(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(app_publication_version), 0) + 1 FROM fund_regulatory_serving_publications")
        return int(cur.fetchone()[0])


def _publication_state(conn: psycopg.Connection, app_id: UUID) -> tuple[int, str] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT app_publication_version, lifecycle_state"
            " FROM fund_regulatory_serving_publications WHERE app_publication_id = %s",
            (app_id,),
        )
        row = cur.fetchone()
    return (int(row[0]), row[1]) if row else None


def write_publication(
    conn: psycopg.Connection,
    base: BasePublication,
    rows: Sequence[CascadeRow],
    app_publication_id: UUID,
) -> tuple[int, bool]:
    """Insert the prepared publication, its artifact pins and its mappings.

    Returns ``(app_publication_version, created)``.  When the deterministic id
    already exists the write is an explicit no-op (``created=False``) -- the
    layer is append-only and a validated publication must never be mutated.
    """
    existing = _publication_state(conn, app_publication_id)
    if existing is not None:
        return existing[0], False

    version = _next_version(conn)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fund_regulatory_serving_publications"
            " (app_publication_id, app_publication_version, worker_publication_id,"
            "  worker_publication_version, lifecycle_state)"
            " VALUES (%s, %s, %s, %s, 'prepared')",
            (app_publication_id, version, base.worker_publication_id, base.worker_publication_version),
        )
        cur.executemany(
            "INSERT INTO fund_regulatory_serving_artifacts"
            " (app_publication_id, product, worker_publication_id, worker_publication_version)"
            " VALUES (%s, %s, %s, %s)",
            [(app_publication_id, product, wid, ver) for product, wid, ver in base.artifacts],
        )
        cur.executemany(
            _INSERT_MAPPING_SQL,
            [
                (
                    app_publication_id,
                    row.instrument_id,
                    row.series_id,
                    row.class_id or "",
                    row.ncen_fund_id,
                    row.mapping_state,
                )
                for row in rows
            ],
        )
    return version, True


def run_gate(
    conn: psycopg.Connection,
    app_publication_id: UUID,
    cascade: CascadeReport,
    thresholds: GateThresholds = GateThresholds(),
) -> GateResult:
    """Pre-flip acceptance gate.  Every metric is read off the NEW publication."""
    with conn.cursor() as cur:
        cur.execute(_MAPPING_METRICS_SQL, {"app": app_publication_id})
        mappings, resolved, class_grain, class_equals_series = cur.fetchone()
        cur.execute(_FEE_MATCH_SQL, {"app": app_publication_id})
        fee_matched = int(cur.fetchone()[0])
        cur.execute(_MANDATE_PUBLISHED_SQL, {"app": app_publication_id})
        mandate_published = int(cur.fetchone()[0])

    metrics = {
        "mappings": int(mappings),
        "resolved": int(resolved),
        "class_grain": int(class_grain),
        "class_equals_series": int(class_equals_series),
        "fee_matched_instruments": fee_matched,
        "mandate_published": mandate_published,
        "source_disagreements": cascade.disagreements,
        "multi_class_tickers": cascade.multi_class_tickers,
    }
    failures: list[str] = []
    if metrics["mappings"] != thresholds.mappings:
        failures.append(f"mappings {metrics['mappings']} != expected {thresholds.mappings}")
    if metrics["resolved"] < thresholds.resolved:
        failures.append(f"resolved {metrics['resolved']} < required {thresholds.resolved}")
    if metrics["class_grain"] < thresholds.class_grain:
        failures.append(f"class_id LIKE 'C%' {metrics['class_grain']} < required {thresholds.class_grain}")
    if metrics["class_equals_series"] != 0:
        failures.append(
            f"{metrics['class_equals_series']} mappings carry the series id in class_id"
            " (poisoned-source signature)"
        )
    if metrics["fee_matched_instruments"] < thresholds.fee_matched_instruments:
        failures.append(
            f"instruments matching >=1 rr1_fee fact {metrics['fee_matched_instruments']}"
            f" < required {thresholds.fee_matched_instruments}"
        )
    if metrics["mandate_published"] < thresholds.mandate_published:
        failures.append(
            f"mandate published {metrics['mandate_published']} < required {thresholds.mandate_published}"
        )
    if metrics["source_disagreements"] > thresholds.max_disagreements:
        failures.append(
            f"cascade source disagreements {metrics['source_disagreements']}"
            f" > allowed {thresholds.max_disagreements}"
        )
    return GateResult(metrics=metrics, failures=failures)


def validate_publication(conn: psycopg.Connection, app_publication_id: UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fund_validate_regulatory_serving_publication_v2(%s)", (app_publication_id,)
        )


def promote_publication(conn: psycopg.Connection, app_publication_id: UUID) -> None:
    """Move the single app pointer.  Also the rollback path (pass the old id)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fund_set_current_regulatory_serving_publication_v2(%s)", (app_publication_id,)
        )


def current_pointer(conn: psycopg.Connection) -> UUID | None:
    with conn.cursor() as cur:
        cur.execute("SELECT app_publication_id FROM fund_regulatory_serving_app_current_pointer")
        row = cur.fetchone()
    return row[0] if row else None


def compose(
    conn: psycopg.Connection,
    *,
    base_app_id: UUID | None = None,
    thresholds: GateThresholds = GateThresholds(),
    promote: bool = False,
) -> CompositionResult:
    """Compose, gate, validate and (optionally) promote a new app publication.

    The composition + validation run in ONE transaction; promotion is a second
    transaction so a failed flip never loses a composed publication.  A failing
    gate rolls the composition back and raises: nothing is validated, nothing is
    promoted.
    """
    base = read_base_publication(conn, base_app_id)
    rows = resolve_class_cascade(conn, base.app_publication_id)
    if not rows:
        raise CompositionError(f"base publication {base.app_publication_id} produced no mappings")
    cascade = summarize_cascade(rows)
    fingerprint = mapping_fingerprint(rows)
    app_publication_id = app_publication_id_for(base, fingerprint)

    version, created = write_publication(conn, base, rows, app_publication_id)
    gate = run_gate(conn, app_publication_id, cascade, thresholds)
    result = CompositionResult(
        app_publication_id=app_publication_id,
        app_publication_version=version,
        base=base,
        fingerprint=fingerprint,
        created=created,
        cascade=cascade,
        gate=gate,
    )
    if not gate.ok:
        conn.rollback()
        return result

    state = _publication_state(conn, app_publication_id)
    if state is not None and state[1] == "prepared":
        validate_publication(conn, app_publication_id)
    conn.commit()
    result.validated = True

    if promote:
        promote_publication(conn, app_publication_id)
        conn.commit()
        result.promoted = current_pointer(conn) == app_publication_id
    return result
