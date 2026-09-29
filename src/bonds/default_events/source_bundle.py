"""Coverage-only ``CreditBundle`` composition from a sanitized source-frontier manifest.

First-delivery diagnostic slice (``bond_credit_evidence_v1`` bundle v2, ``quality_state=partial``,
``build_scope=limited``). It is a **manifest-inventory diagnostic**: the source packages listed
in the frontier manifest are inventoried but *not ingested*, so every evidence frame is empty and
no ``SourcePackage``/provenance row is fabricated for a payload that was never transferred.

What the builder does:

* validates a canonical, SHA-pinned frontier manifest (complete hashes, ``observed_at <= K``,
  ``inventory_only``/``unavailable`` states, ``not_ingested``, no local paths, no pickle/C1);
* reads the panel grid ``(cusip_id, month)`` for the 61 snapshot months ending at the target month
  from ``public.bond_panel_app_pointer`` / ``bond_panel_publications`` / ``bond_panel_snapshot`` in
  one READ ONLY REPEATABLE READ transaction, resolving ancestry exactly like
  ``bond_panel_current_snapshot_v1`` (nearest depth wins) from the *pinned* publication, rejecting
  a broken/cyclic/incompatible chain and applying **no eligibility filter**;
* counts the grid first and refuses to assemble when it exceeds the manifest bounds;
* emits exactly two ``missing`` rating rows per grid key and 60 x 6 ``unavailable`` coverage cells
  whose rationale embeds the canonical frontier records, then calls :func:`contracts.assemble_bundle`.

The module never opens a file, socket or pickle: the manifest arrives as a mapping and the only
I/O is the caller-supplied database connection (read-only panel tables).
"""

from __future__ import annotations

import datetime as dt
import re
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from . import contracts as c
from . import diagnostic_publication as dp
from . import public_ratings as pr

PANEL_PRODUCT = "bond_panel_v1"
PANEL_SCHEMA = "public"
PANEL_CONFIG_HASHES = ("0c0d78a866bc1090", "1863d3d5fa3a0edf")
PANEL_LEGACY_CONFIG = "0c0d78a866bc1090"
PANEL_CURRENT_CONFIG = "1863d3d5fa3a0edf"
PANEL_MONTHS = 61
MAX_ANCESTRY_DEPTH = 256
PANEL_STATEMENT_TIMEOUT_MS = 900_000
PANEL_FETCH_BATCH = 50_000

SOURCE_MANIFEST_VERSION = "bond_default_events_source_frontier_manifest_v1"
MANIFEST_MODE = "inventory_only"

SOURCES = ("agency_rocr", "sec_edgar", "sec_ncen", "sec_nport")
INVENTORY_KINDS = (
    "agency_history",
    "census",
    "dera_packages",
    "form_index",
    "submissions",
)
FRONTIER_STATES = ("inventory_only", "unavailable")
INGESTION = "not_ingested"
#: Closed initial limitation codes (design section 9.4), owned by the diagnostic layer.
LIMITATION_CODES = dp.LIMITATION_CODES
#: The exact frontier inventory a first-delivery manifest must carry.
REQUIRED_FRONTIER_KEYS = (
    "agency_rocr.agency_history",
    "sec_edgar.census",
    "sec_edgar.submissions",
    "sec_ncen.dera_packages",
    "sec_ncen.form_index",
    "sec_nport.dera_packages",
)
#: (source, event_type, frontier records embedded in the rationale, record providing ``source_frontier``).
COVERAGE_SOURCE_CELLS: tuple[tuple[str, str, tuple[str, ...], str | None], ...] = (
    (
        "sec_nport",
        "default_state",
        ("sec_nport.dera_packages",),
        "sec_nport.dera_packages",
    ),
    (
        "sec_edgar",
        "bankruptcy",
        ("sec_edgar.submissions", "sec_edgar.census"),
        "sec_edgar.census",
    ),
    (
        "sec_edgar",
        "payment_default",
        ("sec_edgar.submissions", "sec_edgar.census"),
        "sec_edgar.census",
    ),
    (
        "sec_edgar",
        "distressed_exchange",
        ("sec_edgar.submissions", "sec_edgar.census"),
        "sec_edgar.census",
    ),
    ("agency_rocr", "agency_issue_default", ("agency_rocr.agency_history",), None),
)
RATING_VIEWS = ("effective_audit", "public_pit")
MAX_LIMIT_VALUE = 1 << 40

_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_LABEL = re.compile(r"[A-Za-z0-9_.\-]{1,64}")
#: Local filesystem paths, URLs to files, pickle/C1 payload references are refused anywhere in the manifest.
_FORBIDDEN_TEXT = re.compile(
    r"(?i)(^[a-z]:[\\/])|\\|(^/)|(file://)|(/users/)|(/home/)|(/tmp/)|(appdata)|\.pkl\b|\.pickle\b|pickle|(\bc1\b)|bond_default_reference"
)


class SourceBundleError(RuntimeError):
    """Refusal with a stable ``code`` (text after the colon in ``str(exc)`` is bounded detail)."""

    def __init__(self, code: str, detail: object = "") -> None:
        text = str(detail)[:300]
        super().__init__(f"{code}:{text}" if text else code)
        self.code = code
        self.detail = text


class SourceManifestError(SourceBundleError):
    """The frontier manifest is malformed, unpinned, late, unsanitized or inconsistent."""


class PanelReadError(SourceBundleError):
    """Pointer/publication/ancestry/coverage of the panel does not match the pinned expectation."""


class BoundsExceeded(SourceBundleError):
    """The measured grid or the estimated bundle exceeds the manifest resource bounds."""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def add_months(day: dt.date, months: int) -> dt.date:
    index = day.year * 12 + (day.month - 1) + months
    return dt.date(index // 12, index % 12 + 1, 1)


def expected_grid_months(target_month: dt.date) -> tuple[dt.date, ...]:
    """The 61 snapshot months ``T-60 .. T`` (inclusive)."""
    return tuple(
        add_months(target_month, offset) for offset in range(-(PANEL_MONTHS - 1), 1)
    )


def _aware_utc(value: dt.datetime, name: str) -> dt.datetime:
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SourceBundleError(f"{name}_timezone_required")
    return value.astimezone(dt.timezone.utc)


def _exact_keys(obj: Any, required: Sequence[str], where: str) -> Mapping[str, Any]:
    if not isinstance(obj, Mapping):
        raise SourceManifestError("manifest_object_expected", where)
    missing = sorted(set(required) - set(obj))
    extra = sorted(set(obj) - set(required))
    if missing or extra:
        raise SourceManifestError(
            "manifest_fields_mismatch", f"{where}:missing={missing}:extra={extra}"
        )
    return obj


def _canonical_date(value: Any, where: str) -> dt.date:
    try:
        parsed = dt.date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise SourceManifestError("manifest_date_invalid", where) from exc
    if parsed.isoformat() != value:
        raise SourceManifestError("manifest_date_not_canonical", where)
    return parsed


def _canonical_ts(value: Any, where: str) -> dt.datetime:
    if not isinstance(value, str) or not _TS_RE.fullmatch(value):
        raise SourceManifestError("manifest_timestamp_not_canonical", where)
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=dt.timezone.utc
        )
    except ValueError as exc:
        raise SourceManifestError("manifest_timestamp_invalid", where) from exc


def _plain_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _scan_evidence(value: Any, where: str, depth: int = 0) -> None:
    """Recursively bound the untrusted manifest tree: no floats, no local paths, full hashes only."""
    if depth > 8:
        raise SourceManifestError("manifest_too_deep", where)
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not _LABEL.fullmatch(key):
                raise SourceManifestError("manifest_key_invalid", where)
            if (key == "sha256" or key.endswith("_sha256")) and not (
                isinstance(item, str) and _HEX64.fullmatch(item)
            ):
                raise SourceManifestError(
                    "manifest_sha256_not_complete", f"{where}.{key}"
                )
            _scan_evidence(item, f"{where}.{key}", depth + 1)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _scan_evidence(item, f"{where}[{index}]", depth + 1)
    elif isinstance(value, str):
        if _FORBIDDEN_TEXT.search(value):
            raise SourceManifestError("manifest_forbidden_text", where)
    elif value is None or isinstance(value, bool) or _plain_int(value):
        return
    else:
        raise SourceManifestError(
            "manifest_value_type_invalid", f"{where}:{type(value).__name__}"
        )


# ---------------------------------------------------------------------------
# Frontier manifest
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FrontierRecord:
    source: str
    inventory_kind: str
    source_key: str
    frontier: dt.date | None
    observed_at: dt.datetime
    state: str
    filing_count: int | None
    reason_codes: tuple[str, ...]
    basis: str
    code: str
    record: Mapping[
        str, Any
    ]  # canonical diagnostic-layer frontier record (embedded in rationale)

    def rationale_record(self) -> dict[str, Any]:
        """The canonical record embedded in coverage rationale (built by ``dp.frontier_record``)."""
        return dict(self.record)


@dataclass(frozen=True)
class SourceManifest:
    """A validated frontier manifest (immutable view; ``document`` is the parsed JSON)."""

    digest: (
        str  # ``sha256:<hex>`` over the canonical document minus its own ``digest`` key
    )
    target_month: dt.date
    panel_publication_id: uuid.UUID
    max_grid_rows: int
    max_bundle_bytes: int
    frontiers: Mapping[str, FrontierRecord]
    document: Mapping[str, Any]

    @property
    def digest_hex(self) -> str:
        return self.digest.removeprefix("sha256:")

    def latest_observation(self) -> dt.datetime:
        return max(record.observed_at for record in self.frontiers.values())


_TOP_KEYS = (
    "version",
    "product",
    "mode",
    "target_month",
    "panel",
    "limits",
    "frontiers",
    "digest",
)
_PANEL_KEYS = (
    "product",
    "expected_publication_id",
    "first_month",
    "last_month",
    "months",
)
_LIMIT_KEYS = ("max_grid_rows", "max_bundle_bytes")
_FRONTIER_KEYS = (
    "source",
    "inventory_kind",
    "source_key",
    "frontier",
    "observed_at",
    "state",
    "ingestion",
    "filing_count",
    "reason_codes",
    "basis",
    "code",
    "evidence",
)


def validate_source_manifest(
    document: Mapping[str, Any],
    *,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    expected_panel_publication_id: uuid.UUID,
) -> SourceManifest:
    """Strictly validate the frontier manifest against the pinned run identity (pure, no I/O)."""
    cutoff = _aware_utc(knowledge_cutoff, "knowledge_cutoff")
    top = _exact_keys(document, _TOP_KEYS, "manifest")
    try:
        c.canonical_json_bytes(dict(top))  # rejects floats / non-string keys
        computed = c.document_digest(top, "digest")
    except c.ContractError as exc:
        raise SourceManifestError("manifest_not_canonical", str(exc)) from exc
    if not isinstance(top["digest"], str) or not _DIGEST.fullmatch(top["digest"]):
        raise SourceManifestError("manifest_digest_invalid")
    if top["digest"] != computed:
        raise SourceManifestError("manifest_digest_mismatch")
    if top["version"] != SOURCE_MANIFEST_VERSION:
        raise SourceManifestError(
            "manifest_version_unsupported", str(top["version"])[:60]
        )
    if top["product"] != c.PRODUCT or top["mode"] != MANIFEST_MODE:
        raise SourceManifestError("manifest_product_or_mode_mismatch")
    if (
        _canonical_date(top["target_month"], "target_month") != target_month
        or target_month.day != 1
    ):
        raise SourceManifestError("manifest_target_month_mismatch")

    panel = _exact_keys(top["panel"], _PANEL_KEYS, "panel")
    months = expected_grid_months(target_month)
    if (
        panel["product"] != PANEL_PRODUCT
        or panel["months"] != PANEL_MONTHS
        or isinstance(panel["months"], bool)
    ):
        raise SourceManifestError("manifest_panel_mismatch", "product/months")
    if (
        _canonical_date(panel["first_month"], "panel.first_month") != months[0]
        or _canonical_date(panel["last_month"], "panel.last_month") != months[-1]
    ):
        raise SourceManifestError("manifest_panel_mismatch", "window")
    try:
        pinned = uuid.UUID(str(panel["expected_publication_id"]))
    except ValueError as exc:
        raise SourceManifestError("manifest_panel_id_invalid") from exc
    if (
        str(pinned) != panel["expected_publication_id"]
        or pinned != expected_panel_publication_id
    ):
        raise SourceManifestError("manifest_panel_publication_mismatch")

    limits = _exact_keys(top["limits"], _LIMIT_KEYS, "limits")
    for name in _LIMIT_KEYS:
        value = limits[name]
        if not _plain_int(value) or not 0 < value <= MAX_LIMIT_VALUE:
            raise SourceManifestError("manifest_limit_invalid", name)

    raw_frontiers = top["frontiers"]
    if not isinstance(raw_frontiers, list) or not raw_frontiers:
        raise SourceManifestError("manifest_frontiers_expected")
    records: dict[str, FrontierRecord] = {}
    for index, raw in enumerate(raw_frontiers):
        where = f"frontiers[{index}]"
        item = _exact_keys(raw, _FRONTIER_KEYS, where)
        if (
            item["source"] not in SOURCES
            or item["inventory_kind"] not in INVENTORY_KINDS
        ):
            raise SourceManifestError("manifest_frontier_enum_invalid", where)
        if item["source_key"] != f"{item['source']}.{item['inventory_kind']}":
            raise SourceManifestError("manifest_source_key_mismatch", where)
        if item["source_key"] in records:
            raise SourceManifestError(
                "manifest_source_key_duplicate", item["source_key"]
            )
        if item["state"] not in FRONTIER_STATES:
            raise SourceManifestError("manifest_state_invalid", where)
        if item["ingestion"] != INGESTION:
            raise SourceManifestError("manifest_ingestion_invalid", where)
        frontier = (
            None
            if item["frontier"] is None
            else _canonical_date(item["frontier"], f"{where}.frontier")
        )
        if frontier is None and item["state"] != "unavailable":
            raise SourceManifestError("manifest_frontier_required", where)
        observed = _canonical_ts(item["observed_at"], f"{where}.observed_at")
        if observed > cutoff:
            raise SourceManifestError(
                "manifest_observed_after_cutoff", item["source_key"]
            )
        count = item["filing_count"]
        if count is not None and (not _plain_int(count) or count < 0):
            raise SourceManifestError("manifest_filing_count_invalid", where)
        codes = item["reason_codes"]
        if (
            not isinstance(codes, list)
            or codes != sorted(set(codes))
            or not all(
                isinstance(code, str) and code in LIMITATION_CODES for code in codes
            )
        ):
            raise SourceManifestError("manifest_reason_codes_invalid", where)
        basis = item["basis"]
        code = item["code"]
        if not isinstance(basis, str) or not isinstance(code, str):
            raise SourceManifestError("manifest_basis_invalid", where)
        if not isinstance(item["evidence"], Mapping) or not item["evidence"]:
            raise SourceManifestError("manifest_evidence_required", where)
        try:  # the diagnostic layer owns the record shape (charset, lengths, hashes)
            record = dp.frontier_record(
                source=item["source"],
                inventory_kind=item["inventory_kind"],
                frontier=frontier,
                observed_at=observed,
                state=item["state"],
                filing_count=count,
                reason_codes=codes,
                basis=basis,
                code=code,
                manifest_sha256=top["digest"].removeprefix("sha256:"),
                source_key=item["source_key"],
            )
        except dp.DiagnosticError as exc:
            raise SourceManifestError(
                "manifest_frontier_record_invalid", f"{where}:{exc.reason}:{exc.detail}"
            ) from exc
        records[item["source_key"]] = FrontierRecord(
            source=item["source"],
            inventory_kind=item["inventory_kind"],
            source_key=item["source_key"],
            frontier=frontier,
            observed_at=observed,
            state=item["state"],
            filing_count=count,
            reason_codes=tuple(codes),
            basis=basis,
            code=code,
            record=record,
        )
    if sorted(records) != list(REQUIRED_FRONTIER_KEYS):
        raise SourceManifestError("manifest_frontier_set_mismatch", sorted(records))
    _scan_evidence(top, "manifest")
    return SourceManifest(
        digest=top["digest"],
        target_month=target_month,
        panel_publication_id=pinned,
        max_grid_rows=limits["max_grid_rows"],
        max_bundle_bytes=limits["max_bundle_bytes"],
        frontiers=records,
        document=top,
    )


# ---------------------------------------------------------------------------
# Panel read (READ ONLY, REPEATABLE READ, pinned publication)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PanelPublication:
    publication_id: uuid.UUID
    parent_publication_id: uuid.UUID | None
    config_hash: str
    first_month: dt.date
    last_closed_month: dt.date


@dataclass(frozen=True)
class PanelCount:
    """Result of the count-first phase (no key has been fetched yet)."""

    chain: tuple[PanelPublication, ...]
    month_counts: tuple[tuple[dt.date, int], ...]
    winning_depth_counts: tuple[int, ...]  # index = ancestry depth (0 = pinned head)
    eligibility_counts: tuple[tuple[str, int], ...]

    @property
    def grid_count(self) -> int:
        return sum(count for _month, count in self.month_counts)


@dataclass(frozen=True)
class PanelRead:
    publication_id: uuid.UUID
    counts: PanelCount
    grid: tuple[tuple[str, dt.date], ...]
    grid_digest: str

    @property
    def month_counts(self) -> Mapping[dt.date, int]:
        return dict(self.counts.month_counts)


def _ancestry_compatible(child_config: str, parent_config: str) -> bool:
    return parent_config == child_config or (
        child_config == PANEL_CURRENT_CONFIG and parent_config == PANEL_LEGACY_CONFIG
    )


def walk_ancestry(
    fetch_publication: Any, head_id: uuid.UUID
) -> tuple[PanelPublication, ...]:
    """Walk ``head -> root`` exactly like ``bond_panel_current_snapshot_v1`` but fail loudly.

    ``fetch_publication(uuid) -> mapping | None`` returns ``publication_id``, ``parent_publication_id``,
    ``publication_status``, ``config_hash``, ``first_month``, ``last_closed_month`` (pure, unit-testable).
    The SQL view stops silently at an unvalidated/incompatible/missing parent; that would hide
    months, so here it is a :class:`PanelReadError`.
    """
    chain: list[PanelPublication] = []
    seen: set[uuid.UUID] = set()
    current: uuid.UUID | None = head_id
    child_config: str | None = None
    while current is not None:
        if current in seen:
            raise PanelReadError("panel_ancestry_cycle", current)
        if len(chain) >= MAX_ANCESTRY_DEPTH:
            raise PanelReadError("panel_ancestry_too_deep", len(chain))
        seen.add(current)
        row = fetch_publication(current)
        if row is None:
            raise PanelReadError("panel_ancestry_broken", f"missing:{current}")
        if row["publication_status"] != "validated":
            raise PanelReadError("panel_ancestry_unvalidated", current)
        config = str(row["config_hash"]).strip()
        if child_config is None:
            if config not in PANEL_CONFIG_HASHES:
                raise PanelReadError("panel_config_unsupported", current)
        elif not _ancestry_compatible(child_config, config):
            raise PanelReadError("panel_ancestry_config_incompatible", current)
        chain.append(
            PanelPublication(
                publication_id=row["publication_id"],
                parent_publication_id=row["parent_publication_id"],
                config_hash=config,
                first_month=row["first_month"],
                last_closed_month=row["last_closed_month"],
            )
        )
        child_config = config
        current = row["parent_publication_id"]
    return tuple(chain)


_RANGE_SQL = "f.month >= %(first)s AND f.month <= %(last)s"


def _begin_read_only(conn: Any) -> Any:
    """Return a context manager for a READ ONLY REPEATABLE READ transaction on ``conn``.

    Restores the connection attributes afterwards. Refuses a connection that is mid-transaction.
    """
    from contextlib import contextmanager

    from psycopg import IsolationLevel, pq

    @contextmanager
    def manager() -> Iterator[None]:
        if conn.info.transaction_status != pq.TransactionStatus.IDLE:
            raise PanelReadError("panel_connection_not_idle")
        previous = (conn.isolation_level, conn.read_only)
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        try:
            with conn.transaction():
                conn.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    [str(PANEL_STATEMENT_TIMEOUT_MS)],
                )
                conn.execute("SELECT set_config('lock_timeout', '5000', true)")
                read_only = conn.execute("SHOW transaction_read_only").fetchone()[0]
                isolation = conn.execute("SHOW transaction_isolation").fetchone()[0]
                if read_only != "on" or isolation != "repeatable read":
                    raise PanelReadError(
                        "panel_transaction_not_read_only_repeatable_read",
                        f"{read_only}/{isolation}",
                    )
                yield
        finally:
            conn.isolation_level, conn.read_only = previous

    return manager()


def count_panel(
    conn: Any,
    *,
    expected_panel_publication_id: uuid.UUID,
    target_month: dt.date,
    max_grid_rows: int,
) -> PanelCount:
    """Count-first phase, **inside the caller's read-only transaction**: pointer, ancestry, per-month counts."""
    months = expected_grid_months(target_month)
    pointer = conn.execute(
        f"SELECT publication_id FROM {PANEL_SCHEMA}.bond_panel_app_pointer WHERE product = %s",
        [PANEL_PRODUCT],
    ).fetchall()
    if len(pointer) != 1 or pointer[0][0] != expected_panel_publication_id:
        raise PanelReadError("panel_pointer_mismatch", f"rows={len(pointer)}")

    def fetch(pid: uuid.UUID) -> Mapping[str, Any] | None:
        row = conn.execute(
            "SELECT publication_id, parent_publication_id, publication_status, btrim(config_hash::text), "
            f"first_month, last_closed_month FROM {PANEL_SCHEMA}.bond_panel_publications WHERE publication_id = %s",
            [pid],
        ).fetchone()
        if row is None:
            return None
        return {
            "publication_id": row[0],
            "parent_publication_id": row[1],
            "publication_status": row[2],
            "config_hash": row[3],
            "first_month": row[4],
            "last_closed_month": row[5],
        }

    chain = walk_ancestry(fetch, expected_panel_publication_id)
    head = chain[0]
    if head.last_closed_month < months[-1]:
        raise PanelReadError(
            "panel_target_month_not_closed", f"last_closed={head.last_closed_month}"
        )
    ids = [item.publication_id for item in chain]
    params = {"ids": ids, "first": months[0], "last": months[-1]}
    rows = conn.execute(
        "SELECT month, count(*) FROM (SELECT DISTINCT f.month, f.cusip_id "
        f"FROM {PANEL_SCHEMA}.bond_panel_snapshot f WHERE f.publication_id = ANY(%(ids)s) AND {_RANGE_SQL}) s "
        "GROUP BY month ORDER BY month",
        params,
    ).fetchall()
    counts = tuple((month, int(n)) for month, n in rows)
    if tuple(month for month, _n in counts) != months:
        missing = sorted(set(months) - {m for m, _n in counts})
        raise PanelReadError(
            "panel_month_missing",
            [m.isoformat() for m in missing][:6] or "unexpected_months",
        )
    total = sum(n for _m, n in counts)
    if total > max_grid_rows:
        raise BoundsExceeded(
            "grid_rows_exceed_bound", f"grid={total} max={max_grid_rows}"
        )
    won = conn.execute(
        "SELECT s.depth, s.eligibility_state, count(*) FROM ("
        "SELECT DISTINCT ON (f.month, f.cusip_id) a.depth AS depth, f.eligibility_state "
        f"FROM unnest(%(ids)s::uuid[]) WITH ORDINALITY AS a(publication_id, depth) "
        f"JOIN {PANEL_SCHEMA}.bond_panel_snapshot f ON f.publication_id = a.publication_id "
        f"WHERE {_RANGE_SQL} ORDER BY f.month, f.cusip_id, a.depth) s GROUP BY 1, 2 ORDER BY 1, 2",
        params,
    ).fetchall()
    by_depth = [0] * len(chain)
    by_state: dict[str, int] = {}
    for depth, state, n in won:
        by_depth[int(depth) - 1] += int(n)
        by_state[str(state)] = by_state.get(str(state), 0) + int(n)
    if sum(by_depth) != total:
        raise PanelReadError(
            "panel_resolution_inconsistent", f"{sum(by_depth)}!={total}"
        )
    return PanelCount(
        chain=chain,
        month_counts=counts,
        winning_depth_counts=tuple(by_depth),
        eligibility_counts=tuple(sorted(by_state.items())),
    )


def _fetch_grid(
    conn: Any, counts: PanelCount, target_month: dt.date
) -> tuple[tuple[str, dt.date], ...]:
    months = expected_grid_months(target_month)
    ids = [item.publication_id for item in counts.chain]
    interned: dict[str, str] = {}
    grid: list[tuple[str, dt.date]] = []
    with conn.cursor(name="bond_default_events_panel_keys") as cur:
        cur.itersize = PANEL_FETCH_BATCH
        cur.execute(
            "SELECT DISTINCT f.cusip_id, f.month "
            f"FROM {PANEL_SCHEMA}.bond_panel_snapshot f WHERE f.publication_id = ANY(%(ids)s) AND {_RANGE_SQL}",
            {"ids": ids, "first": months[0], "last": months[-1]},
        )
        for cusip, month in cur:
            grid.append((interned.setdefault(cusip, cusip), month))
    if len(grid) != counts.grid_count:
        raise PanelReadError(
            "panel_grid_count_drift", f"{len(grid)}!={counts.grid_count}"
        )
    bad = sorted({cusip for cusip in interned if not c.is_valid_cusip9(cusip)})
    if bad:
        raise PanelReadError("panel_invalid_cusip9", f"distinct_invalid={len(bad)}")
    grid.sort(key=lambda item: f"{item[0]}|{item[1].isoformat()}")
    tally: dict[dt.date, int] = {}
    for _cusip, month in grid:
        tally[month] = tally.get(month, 0) + 1
    if tuple(sorted(tally.items())) != counts.month_counts:
        raise PanelReadError("panel_grid_month_counts_drift")
    return tuple(grid)


def read_panel(
    conn: Any,
    *,
    expected_panel_publication_id: uuid.UUID,
    target_month: dt.date,
    max_grid_rows: int,
    max_bundle_bytes: int | None = None,
    coverage_bytes: int = 0,
) -> PanelRead:
    """Read the pinned panel grid in one READ ONLY REPEATABLE READ transaction (count first)."""
    with _begin_read_only(conn):
        counts = count_panel(
            conn,
            expected_panel_publication_id=expected_panel_publication_id,
            target_month=target_month,
            max_grid_rows=max_grid_rows,
        )
        if max_bundle_bytes is not None:
            enforce_bundle_bound(counts.grid_count, coverage_bytes, max_bundle_bytes)
        grid = _fetch_grid(conn, counts, target_month)
    return PanelRead(
        publication_id=expected_panel_publication_id,
        counts=counts,
        grid=grid,
        grid_digest=c.grid_digest(grid),
    )


# ---------------------------------------------------------------------------
# Composition (pure)
# ---------------------------------------------------------------------------
def _missing_rating(cusip: str, month: dt.date, view: str) -> c.RatingGridRow:
    return c.RatingGridRow(
        cusip_id=cusip,
        month=month,
        view_kind=view,
        bucket=None,
        state="missing",
        action_date=None,
        public_known_at=None,
        agency_source_ids=(),
        binding_link_ids=(),
        coverage_frontier=None,
        action_input_digest=None,
        default_overlay_episode_id=None,
    )


def iter_missing_ratings(
    grid: Sequence[tuple[str, dt.date]],
) -> Iterator[c.RatingGridRow]:
    """Exactly two ``missing`` rows per grid key (no agency data is inspected)."""
    for cusip, month in grid:
        for view in RATING_VIEWS:
            yield _missing_rating(cusip, month, view)


CELL_BASE_REASON_CODES = ("coverage_only", "outcomes_unascertained")


def _rationale(manifest: SourceManifest, keys: Sequence[str]) -> str:
    """Canonical coverage rationale from the diagnostic layer (frontier records + cell reason codes)."""
    records = [manifest.frontiers[key].rationale_record() for key in sorted(keys)]
    codes = set(CELL_BASE_REASON_CODES)
    for record in records:
        codes.update(record["reason_codes"])
    return dp.coverage_rationale(frontiers=records, reason_codes=sorted(codes))


def outcome_months(target_month: dt.date) -> tuple[dt.date, ...]:
    """Outcome months ``T-59 .. T`` (each one's exposure is the panel start at ``m-1``)."""
    return expected_grid_months(target_month)[1:]


def build_coverage(
    month_counts: Mapping[dt.date, int], manifest: SourceManifest, target_month: dt.date
) -> tuple[c.CoverageCell, ...]:
    """60 outcome months x (1 ``all/all`` + 5 source) cells, all ``unavailable`` (design section 9.3)."""
    specs: list[tuple[str, str, str, dt.date | None]] = [
        ("all", "all", _rationale(manifest, REQUIRED_FRONTIER_KEYS), None)
    ]
    for source, event_type, keys, frontier_key in COVERAGE_SOURCE_CELLS:
        frontier = (
            None if frontier_key is None else manifest.frontiers[frontier_key].frontier
        )
        specs.append((source, event_type, _rationale(manifest, keys), frontier))
    cells: list[c.CoverageCell] = []
    for month in outcome_months(target_month):
        starts = month_counts[add_months(month, -1)]
        for source, event_type, rationale, frontier in specs:
            cells.append(
                c.CoverageCell(
                    period_label=month.strftime("%Y-%m"),
                    source=source,
                    event_type=event_type,
                    rating_stratum="unknown",
                    exposure_cohort="all",
                    state="unavailable",
                    denominator_basis="panel_exposure",
                    denominator_count=starts,
                    exposed_issue_months=starts,
                    event_count=0,
                    unlinked_count=0,
                    date_uncertain_count=0,
                    unknown_outcome_issue_months=starts,
                    source_frontier=frontier,
                    lag_p50_days=None,
                    lag_p90_days=None,
                    lag_max_days=None,
                    rationale=rationale,
                    validation_receipt_digest=None,
                )
            )
    return tuple(cells)


def _rating_bytes_per_key() -> int:
    """Exact canonical bytes per grid key: two ``missing`` rating rows plus the grid entry.

    Rating rows have a fixed-width shape (valid CUSIP9, ISO month, one of two views), so the size is
    exact; every list separator is counted.
    """
    sample = "S0000000"
    sample = sample + c.cusip_check_digit(sample)
    month = dt.date(2021, 8, 1)
    total = sum(
        len(c.canonical_json_bytes(_missing_rating(sample, month, view).to_record()))
        + 1
        for view in RATING_VIEWS
    )
    return total + len(c.canonical_json_bytes([sample, month.isoformat()])) + 1


def estimate_bundle_bytes(grid_count: int, coverage_bytes: int) -> int:
    """Estimate of ``CreditBundle.canonical_bytes()``: ratings + grid (exact) + coverage + manifest allowance."""
    return grid_count * _rating_bytes_per_key() + coverage_bytes + 16_384


def enforce_bundle_bound(
    grid_count: int, coverage_bytes: int, max_bundle_bytes: int
) -> int:
    """Refuse before assembly when the estimated canonical bundle exceeds the manifest bound."""
    estimate = estimate_bundle_bytes(grid_count, coverage_bytes)
    if estimate > max_bundle_bytes:
        raise BoundsExceeded(
            "bundle_bytes_exceed_bound", f"estimate={estimate} max={max_bundle_bytes}"
        )
    return estimate


def coverage_canonical_bytes(coverage: Sequence[c.CoverageCell]) -> int:
    return sum(len(c.canonical_json_bytes(cell.to_record())) + 1 for cell in coverage)


def compose_bundle(
    panel: PanelRead,
    manifest: SourceManifest,
    coverage: Sequence[c.CoverageCell],
    *,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
    code_digest: str,
) -> c.CreditBundle:
    """Assemble the coverage-only bundle with the real pins (no synthetic fixtures)."""
    declarations = pr.rating_declarations_record((), ())
    return c.assemble_bundle(
        target_month=target_month,
        knowledge_cutoff=knowledge_cutoff,
        knowledge_mode=knowledge_mode,
        build_scope="limited",
        quality_state="partial",
        code_digest=code_digest,
        panel_publication_id=panel.publication_id,
        panel_grid=panel.grid,
        issuer_mapping_digest=None,
        rating_declarations=declarations,
        rating_input_digest=pr.rating_input_manifest_digest((), (), ()),
        validation_receipt=None,
        source_packages=(),
        observations=(),
        event_links=(),
        adjudications=(),
        events=(),
        followups=(),
        exit_evidence=(),
        coverage=coverage,
        ratings=iter_missing_ratings(panel.grid),
        ncen_filings=(),
        family_contexts=(),
        family_evidence=(),
        proposal_evidence=(),
        exchange_relations=(),
    )


def build_bundle_from_sources(
    source_manifest: Mapping[str, Any],
    *,
    conn: Any,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: Literal["current_run"] = "current_run",
    expected_panel_publication_id: uuid.UUID,
    code_digest: str,
) -> c.CreditBundle:
    """Frontier manifest + pinned panel -> coverage-only :class:`~contracts.CreditBundle`.

    ``knowledge_cutoff`` is the frozen UTC run K: an identical manifest, K, panel and code digest
    give the identical logical build. Only ``current_run`` exists in this slice.
    """
    if knowledge_mode != "current_run":
        raise SourceBundleError("knowledge_mode_unsupported", knowledge_mode)
    if (
        not isinstance(target_month, dt.date)
        or isinstance(target_month, dt.datetime)
        or target_month.day != 1
    ):
        raise SourceBundleError("target_month_invalid")
    cutoff = _aware_utc(knowledge_cutoff, "knowledge_cutoff")
    if not isinstance(code_digest, str) or not _DIGEST.fullmatch(code_digest):
        raise SourceBundleError("code_digest_invalid")
    manifest = validate_source_manifest(
        source_manifest,
        target_month=target_month,
        knowledge_cutoff=cutoff,
        expected_panel_publication_id=expected_panel_publication_id,
    )
    coverage_probe = build_coverage(
        {m: 0 for m in expected_grid_months(target_month)}, manifest, target_month
    )
    panel = read_panel(
        conn,
        expected_panel_publication_id=expected_panel_publication_id,
        target_month=target_month,
        max_grid_rows=manifest.max_grid_rows,
        max_bundle_bytes=manifest.max_bundle_bytes,
        coverage_bytes=coverage_canonical_bytes(coverage_probe)
        + 8 * len(coverage_probe),
    )
    coverage = build_coverage(panel.month_counts, manifest, target_month)
    return compose_bundle(
        panel,
        manifest,
        coverage,
        target_month=target_month,
        knowledge_cutoff=cutoff,
        knowledge_mode=knowledge_mode,
        code_digest=code_digest,
    )


# ---------------------------------------------------------------------------
# Deterministic code digest (pins the exact producer tree)
# ---------------------------------------------------------------------------
CODE_DIGEST_VERSION = "bond_default_events_code_digest_v1"
#: Producer modules (relative to the repository root) covered by :func:`code_digest`.
_CODE_GLOBS = ("src/bonds/default_events/*.py",)
_CODE_FILES = ("src/workers/bond_default_events.py",)
_CODE_TREES = ("contracts/bonds",)


def code_digest_files(root: Path | None = None) -> tuple[str, ...]:
    """Sorted POSIX relative paths hashed by :func:`code_digest`.

    Exact set: ``src/bonds/default_events/*.py`` (non-recursive), ``src/workers/bond_default_events.py``,
    the five SQL files (the four ``contracts.SQL_FILES`` plus the diagnostic release file) and every
    regular file under ``contracts/bonds/**``. ``__pycache__`` directories and ``*.pyc`` files are
    excluded. Invocation manifests must live outside this set (they carry the digest itself).
    """
    base = c.ROOT if root is None else Path(root)
    found: set[str] = set()
    for pattern in _CODE_GLOBS:
        found.update(
            p.relative_to(base).as_posix() for p in base.glob(pattern) if p.is_file()
        )
    found.update(_CODE_FILES)
    found.update(f"schemas/{name}" for name in c.SQL_FILES)
    found.add(f"schemas/{dp.SQL_PATH.name}")
    for tree in _CODE_TREES:
        top = base / tree
        found.update(
            p.relative_to(base).as_posix()
            for p in top.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"
        )
    return tuple(sorted(found))


def code_digest(root: Path | None = None) -> str:
    """``sha256:`` digest of the producer tree: sorted relative paths + LF-normalized file bytes.

    ``digest_of({"version", "files": [[path, sha256(lf_bytes)], ...]})``; the path list is the exact
    set of :func:`code_digest_files`. Deterministic across checkouts (CRLF/LF) and platforms; a missing
    file is a :class:`SourceBundleError`, never silently skipped.
    """
    base = c.ROOT if root is None else Path(root)
    entries: list[list[str]] = []
    for rel in code_digest_files(base):
        path = base / rel
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise SourceBundleError("code_digest_file_unreadable", rel) from exc
        entries.append([rel, c.sha256_hex(data.replace(b"\r\n", b"\n"))])
    if not any(rel.startswith("contracts/bonds/") for rel, _ in entries):
        raise SourceBundleError("code_digest_contracts_missing")
    return c.digest_of({"version": CODE_DIGEST_VERSION, "files": entries})


# ---------------------------------------------------------------------------
# Diagnostic projection (derived by the diagnostic layer from coverage alone)
# ---------------------------------------------------------------------------
def build_diagnostic_projection(
    bundle: c.CreditBundle, source_manifest: Mapping[str, Any]
) -> dp.DiagnosticProjection:
    """Allowlisted coverage-only projection for the diagnostic release layer.

    Validates the frontier manifest again, requires that the frontier records embedded in the bundle's
    coverage rationale are exactly the manifest's records, and lets
    :func:`diagnostic_publication.derive_projection` derive the display from the coverage rows (SQL
    re-derives it and requires equality). Exposures are cross-checked against the panel grid's own
    per-month key counts; the rating rows are never iterated.
    """
    m = bundle.manifest
    manifest = validate_source_manifest(
        source_manifest,
        target_month=m["target_month"],
        knowledge_cutoff=m["knowledge_cutoff"],
        expected_panel_publication_id=m["panel_publication_id"],
    )
    coverage = bundle.frames["coverage"]
    embedded = dp.frontier_records_of(coverage, knowledge_cutoff=m["knowledge_cutoff"])  # type: ignore[arg-type]
    expected = sorted(
        (record.rationale_record() for record in manifest.frontiers.values()),
        key=lambda r: (r["source"], r["inventory_kind"]),
    )
    if list(embedded) != expected:
        raise SourceManifestError("coverage_manifest_records_mismatch")
    starts = Counter(month for _cusip, month in bundle.panel_grid)
    try:
        return dp.derive_projection(
            target_month=m["target_month"],
            knowledge_cutoff=m["knowledge_cutoff"],
            panel_grid_count=m["panel_grid_count"],
            coverage=coverage,  # type: ignore[arg-type]
            start_counts=starts,
        )
    except dp.DiagnosticError as exc:
        raise SourceBundleError(
            "projection_derivation_failed", f"{exc.reason}:{exc.detail}"
        ) from exc


def source_frontier_manifest_digest(bundle: c.CreditBundle) -> str:
    """The ``source_frontier_manifest_digest`` argument of ``prepare_diagnostic``.

    It is the diagnostic layer's digest of the deduplicated frontier records embedded in the
    coverage rationale (not the digest of the frontier manifest file).
    """
    records = dp.frontier_records_of(
        bundle.frames["coverage"],  # type: ignore[arg-type]
        knowledge_cutoff=bundle.manifest["knowledge_cutoff"],
    )
    return dp.frontier_manifest_digest(records)
