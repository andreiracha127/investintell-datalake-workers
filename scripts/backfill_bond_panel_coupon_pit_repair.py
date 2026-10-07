"""Offline, DSN-less emitter for the coupon-PIT repair child of ``bond_panel_v1`` (audit A2-01).

The child ``C`` extends the CURRENT validated head ``H`` with H's exact window
and config.  Three surfaces (snapshot, rv_signal, rating_pit) and every returns
row AFTER the fallback cutoff are copied verbatim from the four
``bond_panel_current_*_v1`` views inside PostgreSQL; the returns rows AT OR
BEFORE the cutoff come from the pinned coupon-PIT artifact produced by
``scripts/build_bond_panel_coupon_pit_returns.py`` (same keys, same
``price_return``, same typed exits; only ``carry_return``/``total_return``/
``suspect`` re-priced under the one coupon convention).  Like the T3 base and
the unit repair, this program never opens a database connection: it verifies
the pinned artifact and emits ``psql`` transactions on stdout.

Differences from the unit repair, and why:

* the head is NOT a frozen constant.  Stage 6 advances the pointer monthly,
  while the history at or before the cutoff is invariant across heads (a
  Stage 6 child only adds its own closed month; the rows at or before the
  cutoff always resolve to the unit-repair child).  The head is therefore bound
  at plan time (``--from-head``) and verified LIVE by the emitted SQL (current
  pointer, validated, dual-series config, window shape, ancestry through the
  unit-repair child and the frozen root); the frozen identity is the
  artifact's (sha256 of the three files, the terms export, the per-year carry
  digest, the counts);
* the child's row counts for the verbatim surfaces are read from the current
  views inside the prepare transaction and attested in the publication row;
  the artifact pins the count at or before the cutoff;
* the finalize gates are structural: keys identical to the head projection,
  ``price_return``/``exit_basis``/``exit_reason`` identical per row, rows after
  the cutoff identical in full, ``total_return = price_return + carry_return``
  and ``suspect = |total| > 0.5`` on every repriced row, and the per-year
  carry sums (before AND after) equal to the artifact manifest;
* a pinned dropped key (no coupon basis at or before its month, not in
  default) is not merely omitted: the served view overlays the ancestry by
  nearest depth and would serve the head's look-ahead row. Prepare writes one
  ``bond_panel_returns_tombstone`` row per dropped key, finalize gates the
  tombstone set and, after the CAS, that the served view holds none of them
  and exactly the child's declared row count;
* the artifact identity includes the default-flat source (contract v2): the
  default-events sha256 is pinned and enters the child fingerprint.

``COUPON_PIT_EXPECTED_ARTIFACT`` is ``None`` until the owner's artifact exists:
the CLI refuses (``coupon_pit_artifact_unpinned``) rather than emitting SQL
bound to placeholder digests.  The follow-up commit that pins it is the
authorization; see ``docs/runbooks/bond-panel-coupon-pit-republication.md``.
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_bond_panel_history import (  # noqa: E402
    CONFIG_HASH,
    EXPECTED_SHA256,
    EXPECTED_SHA256_UNIT_REPAIR_V2,
    PRODUCT,
    REPAIR_CODE_REVISION,
    SURFACES,
    UNIT_REPAIR_CONFIG_HASH,
    UNIT_REPAIR_CONTRACT,
    UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY,
    UNIT_REPAIR_EXPECTED_PUBLICATION_ID,
    UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID,
    ArtifactPinError,
    CursorError,
    PlanError,
    Surface,
    _canonical_digest,
    _clean,
    _connect,
    _COPY_COLUMNS,
    _COPY_NULLABLE,
    _COPY_TYPES,
    _scalar_month,
    _sha256,
    _sql_json,
    _sql_string,
    _TABLES,
)
from scripts.backfill_psql_transport import render_immutable_batch  # noqa: E402
from scripts.build_bond_panel_coupon_pit_returns import (  # noqa: E402
    CONTRACT as COUPON_PIT_CONTRACT,
    COUPON_CONVENTION,
    DEFAULT_FLAT_RULE,
    FALLBACK_CUTOFF as COUPON_PIT_CUTOFF,
    INPUT_FILES as COUPON_PIT_V2_INPUTS,
    MANIFEST_VERSION as COUPON_PIT_MANIFEST_VERSION,
    OUTPUT_BASIS,
    OUTPUT_MANIFEST,
    OUTPUT_RETURNS,
    SUSPECT_ABS_RETURN,
)

COUPON_PIT_CODE_REVISION = "t3_returns_coupon_pit_repair_v2"
# Contractual coupon, PIT fallback, and the default-flat rule (contract v2):
# every other builder mode is a preview the emitter refuses.
COUPON_PIT_REQUIRED_MODE = "contractual_then_pit_default_flat"
# Every key the authorization (``COUPON_PIT_EXPECTED_ARTIFACT``) must carry; a
# missing one is refused by name instead of surfacing as a KeyError.
COUPON_PIT_PIN_KEYS = ("artifact_sha256", "terms_export_sha256", "default_events_sha256", "per_year_digest", "dropped_keys_digest", "counts")
# A dropped key (no coupon basis at or before its month, not in default) has no
# return row under the resolver. Omitting it from the child is not enough: the
# served view overlays the ancestry by nearest depth and would serve the head's
# look-ahead row. The child writes a tombstone the view honors instead.
COUPON_PIT_TOMBSTONE_REASON = "coupon_pit_no_coupon_basis"
COUPON_PIT_AFFECTED_SURFACES: tuple[Surface, ...] = ("returns",)
COUPON_PIT_ARTIFACT_FILES = (OUTPUT_RETURNS, OUTPUT_BASIS, OUTPUT_MANIFEST)
COUPON_PIT_PINNED_COUNT_KEYS = ("returns_rows_out", "rows_at_or_before_cutoff", "rows_after_cutoff", "scope_rows", "repriced_rows", "exit_rows_at_or_before_cutoff")
# Rows with no inversion at or before their month and no contractual coupon get
# no return row (the resolver's own outcome).  Their keys are pinned and the
# finalize gate admits exactly those absences; more than this many means the
# inputs are not the history this contract describes.
COUPON_PIT_DROPPED_ROWS_BOUND = 1_000
COUPON_PIT_FIRST_MONTH = "2002-07-01"
COUPON_PIT_RETURNS_FIRST_MONTH = "2002-08-01"
COUPON_PIT_CARRY_SUM_TOLERANCE = 1e-6
COUPON_PIT_IDENTITY_TOLERANCE = 1e-12
COUPON_PIT_FINALIZE_LOCK_TIMEOUT = "5s"
COUPON_PIT_FINALIZE_STATEMENT_TIMEOUT = "55min"
COUPON_PIT_FINALIZE_REFRESH_STATEMENT_TIMEOUT = "20min"
COUPON_PIT_DEFAULT_ARTIFACT_DIRECTORY = UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY / "coupon_pit_v3"
# Frozen artifact authorization.  ``None`` until the owner's artifact exists; the
# follow-up commit that fills it (sha256 of the three artifact files, the terms
# export sha256, the per-year digest and the pinned counts, all copied from the
# artifact manifest) is the authorization for the republication.
COUPON_PIT_EXPECTED_ARTIFACT: dict[str, Any] | None = None

_IDENTITY_COLUMNS = ("distribution_rule", "reference_cusip9", "distribution_decision_id")
_RETURNS_REPRICED_COLUMNS = ("total_return", "carry_return", "suspect", "payload")


@dataclass(frozen=True)
class CouponPitArtifacts:
    """The pinned coupon-PIT artifact: three files whose bytes and manifest claims agree."""

    directory: Path
    paths: dict[str, Path]
    sha256: dict[str, str]
    manifest: dict[str, Any]
    expected: dict[str, Any]

    @classmethod
    def open(cls, directory: Path, *, expected: dict[str, Any] | None = None) -> CouponPitArtifacts:
        pins = COUPON_PIT_EXPECTED_ARTIFACT if expected is None else expected
        if pins is None:
            raise ArtifactPinError("coupon_pit_artifact_unpinned")
        missing_pins = [key for key in COUPON_PIT_PIN_KEYS if key not in pins]
        if missing_pins:
            raise ArtifactPinError(f"coupon_pit_pin_keys_missing:{','.join(missing_pins)}")
        manifest_path = directory / OUTPUT_MANIFEST
        if not manifest_path.is_file():
            raise ArtifactPinError("coupon_pit_manifest_unavailable")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ArtifactPinError("coupon_pit_manifest_unreadable") from exc
        if not isinstance(manifest, dict):
            raise ArtifactPinError("coupon_pit_manifest_unreadable")
        if manifest.get("manifest_version") != COUPON_PIT_MANIFEST_VERSION:
            raise PlanError("coupon_pit_manifest_version_mismatch")
        if manifest.get("contract") != COUPON_PIT_CONTRACT:
            raise PlanError("coupon_pit_contract_mismatch")
        if manifest.get("mode") != COUPON_PIT_REQUIRED_MODE:
            raise PlanError("coupon_pit_preview_artifact_refused")
        if manifest.get("fallback_cutoff") != COUPON_PIT_CUTOFF:
            raise PlanError("coupon_pit_cutoff_mismatch")
        inputs = manifest.get("inputs") or {}
        v2_expected = {name: EXPECTED_SHA256_UNIT_REPAIR_V2[name] for name in COUPON_PIT_V2_INPUTS}
        if inputs.get("artifact_sha256") != v2_expected:
            raise PlanError("coupon_pit_v2_input_identity_mismatch")
        terms = inputs.get("terms_export") or {}
        if terms.get("sha256") != pins["terms_export_sha256"]:
            raise PlanError("coupon_pit_terms_export_sha256_mismatch")
        default_events = inputs.get("default_events") or {}
        if default_events.get("sha256") != pins["default_events_sha256"]:
            raise PlanError("coupon_pit_default_events_sha256_mismatch")
        counts = manifest.get("counts") or {}
        dropped = int(counts.get("dropped_rows_no_pit_basis", -1))
        dropped_keys = manifest.get("dropped_keys")
        if dropped < 0 or not isinstance(dropped_keys, list) or len(dropped_keys) != dropped:
            raise PlanError("coupon_pit_dropped_keys_inconsistent")
        if dropped > COUPON_PIT_DROPPED_ROWS_BOUND:
            raise PlanError("coupon_pit_dropped_rows_exceed_bound")
        if any(not isinstance(key, dict) or set(key) != {"cusip_id", "month"} for key in dropped_keys):
            raise PlanError("coupon_pit_dropped_keys_malformed")
        if manifest.get("dropped_keys_digest") != _canonical_digest(dropped_keys) or manifest.get("dropped_keys_digest") != pins["dropped_keys_digest"]:
            raise PlanError("coupon_pit_dropped_keys_digest_mismatch")
        pinned_counts = {key: int(counts[key]) for key in COUPON_PIT_PINNED_COUNT_KEYS if key in counts}
        if pinned_counts != {key: int(value) for key, value in pins["counts"].items()}:
            raise PlanError("coupon_pit_counts_mismatch")
        per_year = manifest.get("per_year")
        if not isinstance(per_year, list) or not per_year:
            raise PlanError("coupon_pit_per_year_absent")
        if manifest.get("per_year_digest") != _canonical_digest({"per_year": per_year}) or manifest.get("per_year_digest") != pins["per_year_digest"]:
            raise PlanError("coupon_pit_per_year_digest_mismatch")
        if set(pins["artifact_sha256"]) != set(COUPON_PIT_ARTIFACT_FILES):
            raise ArtifactPinError("coupon_pit_artifact_map_not_three_files")
        paths: dict[str, Path] = {}
        actual: dict[str, str] = {}
        for name in COUPON_PIT_ARTIFACT_FILES:
            path = directory / name
            if not path.is_file():
                raise ArtifactPinError(f"artifact_unavailable:{name}")
            digest = _sha256(path)
            if digest != pins["artifact_sha256"][name]:
                raise ArtifactPinError(f"artifact_sha256_mismatch:{name}")
            paths[name] = path
            actual[name] = digest
        outputs = manifest.get("outputs") or {}
        for name in (OUTPUT_RETURNS, OUTPUT_BASIS):
            if (outputs.get(name) or {}).get("sha256") != actual[name]:
                raise ArtifactPinError(f"coupon_pit_manifest_output_sha256_mismatch:{name}")
        rows = int(pq.ParquetFile(paths[OUTPUT_RETURNS]).metadata.num_rows)
        if rows != pinned_counts["returns_rows_out"]:
            raise ArtifactPinError("coupon_pit_returns_rows_mismatch")
        return cls(directory=directory, paths=paths, sha256=actual, manifest=manifest, expected=copy.deepcopy(pins))

    def path(self, name: str) -> str:
        return self.paths[name].as_posix()


@dataclass(frozen=True)
class CouponPitPlan:
    publication_id: str
    input_fingerprint: str
    from_head_publication_id: str
    config_hash: str
    cutoff: str
    counts: dict[str, int]
    artifact_sha256: dict[str, str]
    terms_export_sha256: str
    default_events_sha256: str
    per_year: tuple[dict[str, Any], ...]
    per_year_digest: str
    resolver_sha256: str
    dropped_rows: int
    dropped_keys_digest: str
    contract: str = COUPON_PIT_CONTRACT
    code_revision: str = COUPON_PIT_CODE_REVISION
    root_base_publication_id: str = UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID
    unit_repair_child_publication_id: str = UNIT_REPAIR_EXPECTED_PUBLICATION_ID

    def evidence(self) -> dict[str, Any]:
        return {
            "publication_id": self.publication_id,
            "input_fingerprint": self.input_fingerprint,
            "contract": self.contract,
            "code_revision": self.code_revision,
            "from_head_publication_id": self.from_head_publication_id,
            "root_base_publication_id": self.root_base_publication_id,
            "unit_repair_child_publication_id": self.unit_repair_child_publication_id,
            "config_hash": self.config_hash,
            "cutoff": self.cutoff,
            "affected_surfaces": list(COUPON_PIT_AFFECTED_SURFACES),
            "counts": dict(sorted(self.counts.items())),
            "artifact_sha256": dict(sorted(self.artifact_sha256.items())),
            "terms_export_sha256": self.terms_export_sha256,
            "default_events_sha256": self.default_events_sha256,
            "per_year_carry_digest": self.per_year_digest,
            "per_year": [dict(item) for item in self.per_year],
            "resolver_sha256": self.resolver_sha256,
            "dropped_rows": self.dropped_rows,
            "dropped_keys_digest": self.dropped_keys_digest,
            "coupon_convention": COUPON_CONVENTION,
            "default_flat_rule": DEFAULT_FLAT_RULE,
        }


def _fingerprint_payload(plan: CouponPitPlan) -> dict[str, Any]:
    return {
        "contract": plan.contract,
        "from_head_publication_id": plan.from_head_publication_id,
        "root_base_publication_id": plan.root_base_publication_id,
        "unit_repair_child_publication_id": plan.unit_repair_child_publication_id,
        "config_hash": plan.config_hash,
        "cutoff": plan.cutoff,
        "affected_surfaces": list(COUPON_PIT_AFFECTED_SURFACES),
        "artifact_sha256": dict(sorted(plan.artifact_sha256.items())),
        "terms_export_sha256": plan.terms_export_sha256,
        "default_events_sha256": plan.default_events_sha256,
        "per_year_carry_digest": plan.per_year_digest,
        "counts": dict(sorted(plan.counts.items())),
        "resolver_sha256": plan.resolver_sha256,
        "dropped_rows": plan.dropped_rows,
        "dropped_keys_digest": plan.dropped_keys_digest,
    }


def _validate_plan(plan: CouponPitPlan, artifacts: CouponPitArtifacts) -> None:
    """Fail closed unless the plan is the pinned artifact's deterministic identity for this head."""
    try:
        head = uuid.UUID(plan.from_head_publication_id)
    except ValueError as exc:
        raise PlanError("coupon_pit_from_head_not_a_uuid") from exc
    fingerprint = _canonical_digest(_fingerprint_payload(plan))
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:coupon-pit-repair:{fingerprint}"))
    pins = artifacts.expected
    if (
        plan.input_fingerprint != fingerprint
        or plan.publication_id != publication_id
        or str(head) != plan.from_head_publication_id
        or plan.contract != COUPON_PIT_CONTRACT
        or plan.code_revision != COUPON_PIT_CODE_REVISION
        or plan.config_hash != UNIT_REPAIR_CONFIG_HASH
        or plan.cutoff != COUPON_PIT_CUTOFF
        or plan.artifact_sha256 != dict(sorted(pins["artifact_sha256"].items()))
        or plan.artifact_sha256 != dict(sorted(artifacts.sha256.items()))
        or plan.terms_export_sha256 != pins["terms_export_sha256"]
        or plan.default_events_sha256 != pins["default_events_sha256"]
        or plan.per_year_digest != pins["per_year_digest"]
        or plan.per_year_digest != _canonical_digest({"per_year": list(plan.per_year)})
        or plan.counts != {key: int(value) for key, value in pins["counts"].items()}
        or plan.dropped_rows != int(artifacts.manifest["counts"]["dropped_rows_no_pit_basis"])
        or plan.dropped_rows > COUPON_PIT_DROPPED_ROWS_BOUND
        or plan.dropped_keys_digest != pins["dropped_keys_digest"]
        or plan.dropped_keys_digest != _canonical_digest(artifacts.manifest["dropped_keys"])
        or plan.root_base_publication_id != UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID
        or plan.unit_repair_child_publication_id != UNIT_REPAIR_EXPECTED_PUBLICATION_ID
    ):
        raise PlanError("coupon_pit_plan_not_authorized")


def build_coupon_pit_plan(artifacts: CouponPitArtifacts, *, from_head_publication_id: str) -> CouponPitPlan:
    manifest = artifacts.manifest
    counts = {key: int(manifest["counts"][key]) for key in COUPON_PIT_PINNED_COUNT_KEYS if key in manifest["counts"]}
    plan = CouponPitPlan(
        publication_id="",
        input_fingerprint="",
        from_head_publication_id=from_head_publication_id,
        config_hash=UNIT_REPAIR_CONFIG_HASH,
        cutoff=COUPON_PIT_CUTOFF,
        counts=counts,
        artifact_sha256=dict(sorted(artifacts.sha256.items())),
        terms_export_sha256=str(manifest["inputs"]["terms_export"]["sha256"]),
        default_events_sha256=str(manifest["inputs"]["default_events"]["sha256"]),
        per_year=tuple(dict(item) for item in manifest["per_year"]),
        per_year_digest=str(manifest["per_year_digest"]),
        resolver_sha256=str(manifest["inputs"]["resolver_sha256"]),
        dropped_rows=int(manifest["counts"]["dropped_rows_no_pit_basis"]),
        dropped_keys_digest=str(manifest["dropped_keys_digest"]),
    )
    fingerprint = _canonical_digest(_fingerprint_payload(plan))
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:coupon-pit-repair:{fingerprint}"))
    resolved = replace(plan, input_fingerprint=fingerprint, publication_id=publication_id)
    _validate_plan(resolved, artifacts)
    return resolved


def _marker(plan: CouponPitPlan) -> dict[str, Any]:
    return {
        "contract": plan.contract,
        "from_head_publication_id": plan.from_head_publication_id,
        "root_base_publication_id": plan.root_base_publication_id,
        "unit_repair_child_publication_id": plan.unit_repair_child_publication_id,
        "cutoff": plan.cutoff,
        "affected_surfaces": list(COUPON_PIT_AFFECTED_SURFACES),
        "authorized_code_revision": plan.code_revision,
        "artifact_sha256": dict(sorted(plan.artifact_sha256.items())),
        "terms_export_sha256": plan.terms_export_sha256,
        "default_events_sha256": plan.default_events_sha256,
        "per_year_carry_digest": plan.per_year_digest,
        "counts": dict(sorted(plan.counts.items())),
        "resolver_sha256": plan.resolver_sha256,
        "dropped_rows": plan.dropped_rows,
        "dropped_keys_digest": plan.dropped_keys_digest,
    }


def _marker_containment(plan: CouponPitPlan, alias: str = "candidate") -> str:
    return (
        f"{alias}.gate_evidence @> jsonb_build_object('coupon_pit_repair', jsonb_build_object("
        f"'contract', {_sql_string(plan.contract)}, 'from_head_publication_id', {_sql_string(plan.from_head_publication_id)}))"
    )


def _child_identity_check(plan: CouponPitPlan) -> str:
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    return f"""EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.publication_status IN ('prepared', 'validated')
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.config_hash = {_sql_string(plan.config_hash)}
          AND candidate.input_fingerprint = {_sql_string(plan.input_fingerprint)}
          AND candidate.code_revision = {_sql_string(plan.code_revision)}
          AND {_marker_containment(plan)}
    )"""


def _pointer_check(plan: CouponPitPlan, phase: str) -> str:
    return f"""    IF (SELECT publication_id FROM bond_panel_app_pointer WHERE product = {_sql_string(PRODUCT)}) IS DISTINCT FROM {_sql_string(plan.from_head_publication_id)}::uuid THEN
        RAISE EXCEPTION 'coupon pit {phase} requires the expected head pointer';
    END IF;"""


def _head_window_check(plan: CouponPitPlan) -> str:
    head = _sql_string(plan.from_head_publication_id)
    return f"""    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications head
        WHERE head.publication_id = {head}::uuid
          AND head.publication_status = 'validated'
          AND head.config_hash = {_sql_string(plan.config_hash)}
          AND head.first_month = {_sql_string(COUPON_PIT_FIRST_MONTH)}::date
          AND head.last_closed_month > {_sql_string(plan.cutoff)}::date
          AND head.open_month = (head.last_closed_month + INTERVAL '1 month')::date
    ) THEN RAISE EXCEPTION 'coupon pit repair requires a validated dual-series head extending past the cutoff'; END IF;
    IF NOT EXISTS (
        WITH RECURSIVE ancestry(publication_id, parent_publication_id, path) AS (
            SELECT p.publication_id, p.parent_publication_id, ARRAY[p.publication_id]
            FROM bond_panel_publications p WHERE p.publication_id = {head}::uuid
            UNION ALL
            SELECT p.publication_id, p.parent_publication_id, a.path || p.publication_id
            FROM bond_panel_publications p JOIN ancestry a ON p.publication_id = a.parent_publication_id
            WHERE NOT p.publication_id = ANY(a.path)
        )
        SELECT 1
        WHERE EXISTS (
            SELECT 1 FROM ancestry a JOIN bond_panel_publications child ON child.publication_id = a.publication_id
            WHERE a.publication_id = {_sql_string(plan.unit_repair_child_publication_id)}::uuid
              AND child.publication_status = 'validated'
              AND child.gate_evidence @> jsonb_build_object('unit_repair', jsonb_build_object('contract', {_sql_string(UNIT_REPAIR_CONTRACT)}))
        ) AND EXISTS (
            SELECT 1 FROM ancestry a JOIN bond_panel_publications root ON root.publication_id = a.publication_id
            WHERE a.publication_id = {_sql_string(plan.root_base_publication_id)}::uuid
              AND a.parent_publication_id IS NULL
              AND root.publication_status = 'validated'
              AND root.config_hash = {_sql_string(CONFIG_HASH)}
              AND root.code_revision = {_sql_string(REPAIR_CODE_REVISION)}
              AND root.source_lineage->'source_sha256' = {_sql_json(dict(sorted(EXPECTED_SHA256.items())))}::jsonb
        )
    ) THEN RAISE EXCEPTION 'coupon pit repair requires the head to descend from the unit-repair child and the frozen root'; END IF;"""


def _dropped_keys_json(artifacts: CouponPitArtifacts) -> str:
    return _sql_string(json.dumps(list(artifacts.manifest["dropped_keys"]), sort_keys=True, separators=(",", ":")))


def _tombstone_ddl_check(phase: str) -> str:
    """The served returns view must honor ``bond_panel_returns_tombstone`` before a child relies on it."""
    return f"""    IF pg_catalog.to_regclass('bond_panel_returns_tombstone') IS NULL
       OR pg_catalog.strpos(pg_catalog.pg_get_viewdef('bond_panel_current_returns_v1'::regclass), 'bond_panel_returns_tombstone') = 0
    THEN RAISE EXCEPTION 'coupon pit {phase} requires the returns tombstone DDL (apply schemas/bond_panel_v1.sql first)'; END IF;"""


def render_coupon_pit_prepare_sql(plan: CouponPitPlan, artifacts: CouponPitArtifacts) -> str:
    """Create or attest the deterministic prepared child and its tombstones; never move the pointer.

    The child's tombstones are exactly the pinned dropped keys: without them the
    served view would fall back to the head's row for each dropped key.
    """
    _validate_plan(plan, artifacts)
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    marker = _sql_json(_marker(plan))
    cutoff = _sql_string(plan.cutoff)
    before = plan.counts["rows_at_or_before_cutoff"]
    loaded = before - plan.dropped_rows
    dropped_keys = _dropped_keys_json(artifacts)
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $coupon_pit_prepare$
DECLARE
    v_snapshot bigint;
    v_rv_signal bigint;
    v_rating_pit bigint;
    v_returns_before bigint;
    v_returns_after bigint;
BEGIN
{_pointer_check(plan, "prepare")}
{_head_window_check(plan)}
{_tombstone_ddl_check("prepare")}
    IF EXISTS (
        SELECT 1 FROM bond_panel_publications prior
        WHERE prior.publication_status = 'validated' AND {_marker_containment(plan, "prior")}
    ) THEN RAISE EXCEPTION 'coupon pit child already validated for this head'; END IF;
    SELECT count(*) INTO v_returns_before FROM bond_panel_current_returns_v1 WHERE month <= {cutoff}::date;
    IF v_returns_before <> {before} THEN
        RAISE EXCEPTION 'coupon pit prepare requires the pinned returns count at or before the cutoff (% <> {before})', v_returns_before;
    END IF;
    SELECT count(*) INTO v_returns_after FROM bond_panel_current_returns_v1 WHERE month > {cutoff}::date;
    SELECT count(*) INTO v_snapshot FROM bond_panel_current_snapshot_v1;
    SELECT count(*) INTO v_rv_signal FROM bond_panel_current_rv_signal_v1;
    SELECT count(*) INTO v_rating_pit FROM bond_panel_current_rating_pit_v1;
    INSERT INTO bond_panel_publications (publication_id, parent_publication_id, publication_status, config_hash, input_fingerprint, code_revision, first_month, last_closed_month, open_month, snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence)
    SELECT {child}::uuid, {head}::uuid, 'prepared', {_sql_string(plan.config_hash)}, {_sql_string(plan.input_fingerprint)}, {_sql_string(plan.code_revision)}, head.first_month, head.last_closed_month, head.open_month,
           v_snapshot, v_rv_signal, {loaded} + v_returns_after, v_rating_pit,
           head.source_lineage || jsonb_build_object('coupon_pit_repair', {marker}::jsonb),
           jsonb_build_object('coupon_pit_repair', {marker}::jsonb, 'live_counts', jsonb_build_object('snapshot', v_snapshot, 'rv_signal', v_rv_signal, 'returns_after_cutoff', v_returns_after, 'rating_pit', v_rating_pit))
    FROM bond_panel_publications head
    WHERE head.publication_id = {head}::uuid
    ON CONFLICT (publication_id) DO NOTHING;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        JOIN bond_panel_publications head ON head.publication_id = {head}::uuid
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.publication_status IN ('prepared', 'validated')
          AND candidate.config_hash = {_sql_string(plan.config_hash)}
          AND candidate.input_fingerprint = {_sql_string(plan.input_fingerprint)}
          AND candidate.code_revision = {_sql_string(plan.code_revision)}
          AND candidate.first_month = head.first_month
          AND candidate.last_closed_month = head.last_closed_month
          AND candidate.open_month = head.open_month
          AND candidate.snapshot_rows = v_snapshot
          AND candidate.rv_signal_rows = v_rv_signal
          AND candidate.returns_rows = {loaded} + v_returns_after
          AND candidate.ratings_pit_rows = v_rating_pit
          AND candidate.source_lineage @> jsonb_build_object('coupon_pit_repair', {marker}::jsonb)
          AND candidate.gate_evidence @> jsonb_build_object('coupon_pit_repair', {marker}::jsonb)
    ) THEN RAISE EXCEPTION 'non-identical or non-resumable coupon-pit publication'; END IF;
    INSERT INTO bond_panel_returns_tombstone (publication_id, month, cusip_id, reason, payload)
    SELECT {child}::uuid, dropped.month, dropped.cusip_id, {_sql_string(COUPON_PIT_TOMBSTONE_REASON)},
           jsonb_build_object('coupon_pit_repair', {marker}::jsonb)
    FROM jsonb_to_recordset({dropped_keys}::jsonb) AS dropped(month date, cusip_id text)
    ON CONFLICT (publication_id, month, cusip_id) DO NOTHING;
    IF (SELECT count(*) FROM bond_panel_returns_tombstone WHERE publication_id = {child}::uuid) <> {plan.dropped_rows}
       OR EXISTS (
           SELECT month, cusip_id FROM bond_panel_returns_tombstone WHERE publication_id = {child}::uuid
           EXCEPT SELECT month, cusip_id FROM jsonb_to_recordset({dropped_keys}::jsonb) AS dropped(month date, cusip_id text)
       )
    THEN RAISE EXCEPTION 'coupon pit tombstones must equal the pinned dropped keys'; END IF;
END
$coupon_pit_prepare$;
COMMIT;
SELECT jsonb_build_object('publication_id', {child}, 'phase', 'prepared', 'contract', {_sql_string(plan.contract)}, 'from_head_publication_id', {head}, 'input_fingerprint', {_sql_string(plan.input_fingerprint)}, 'artifact_sha256', {_sql_json(plan.artifact_sha256)}::jsonb) AS coupon_pit_evidence;
"""


def _copy_expressions(plan: CouponPitPlan, surface: Surface) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for column in _COPY_COLUMNS[surface]:
        if column == "publication_id":
            items.append((column, f"{_sql_string(plan.publication_id)}::uuid"))
        elif column == "distribution_rule":
            items.append((column, "COALESCE(source.distribution_rule, 'rule_144a')"))
        elif column == "reference_cusip9":
            items.append((column, "COALESCE(source.reference_cusip9, source.cusip_id)"))
        elif column == "distribution_decision_id":
            items.append((column, "CASE WHEN source.distribution_rule IS NULL THEN NULL ELSE source.distribution_decision_id END"))
        else:
            items.append((column, f"source.{column}"))
    return items


def _verbatim_columns(surface: Surface) -> tuple[str, ...]:
    excluded = {"publication_id", *_IDENTITY_COLUMNS}
    return tuple(column for column in _COPY_COLUMNS[surface] if column not in excluded)


def render_coupon_pit_copy_sql(plan: CouponPitPlan, artifacts: CouponPitArtifacts, surface: Surface) -> str:
    """Copy one current view verbatim into the prepared child (returns: months after the cutoff only)."""
    if surface not in SURFACES:
        raise ValueError(f"unknown_surface:{surface}")
    _validate_plan(plan, artifacts)
    child = _sql_string(plan.publication_id)
    table = _TABLES[surface]
    view = f"bond_panel_current_{surface}_v1"
    items = _copy_expressions(plan, surface)
    target_columns = ", ".join(column for column, _expression in items)
    source_expressions = ", ".join(expression for _column, expression in items)
    verbatim = _verbatim_columns(surface)
    candidate_row = ", ".join(f"candidate.{column}" for column in verbatim)
    source_row = ", ".join(f"source.{column}" for column in verbatim)
    scope = f"source.month > {_sql_string(plan.cutoff)}::date" if surface == "returns" else "TRUE"
    expected_count = (
        f"(SELECT count(*) FROM {view} source WHERE {scope})"
    )
    declared = {
        "snapshot": "candidate.snapshot_rows", "rv_signal": "candidate.rv_signal_rows",
        "rating_pit": "candidate.ratings_pit_rows",
        "returns": f"candidate.returns_rows - {plan.counts['rows_at_or_before_cutoff'] - plan.dropped_rows}",
    }[surface]
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $coupon_pit_copy_order$
BEGIN
{_pointer_check(plan, "copy")}
    IF NOT {_child_identity_check(plan)} THEN
        RAISE EXCEPTION 'coupon pit copy requires the prepared coupon-pit child';
    END IF;
END
$coupon_pit_copy_order$;
LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO {table} ({target_columns})
SELECT {source_expressions}
FROM {view} source
WHERE {scope}
  AND EXISTS (
    SELECT 1 FROM bond_panel_publications candidate
    WHERE candidate.publication_id = {child}::uuid
      AND candidate.publication_status = 'prepared'
)
ON CONFLICT (publication_id, month, cusip_id) DO NOTHING;
DO $coupon_pit_copy_gates$
BEGIN
    IF (SELECT count(*) FROM {table} candidate_rows WHERE candidate_rows.publication_id = {child}::uuid{" AND candidate_rows.month > " + _sql_string(plan.cutoff) + "::date" if surface == "returns" else ""}) <> {expected_count} THEN
        RAISE EXCEPTION 'coupon pit copy count mismatch:{surface}';
    END IF;
    IF (SELECT {declared} FROM bond_panel_publications candidate WHERE candidate.publication_id = {child}::uuid) <> {expected_count} THEN
        RAISE EXCEPTION 'coupon pit copy count differs from the prepared declaration:{surface}';
    END IF;
    IF EXISTS (
        SELECT 1 FROM {table} candidate
        JOIN {view} source USING (month, cusip_id)
        WHERE candidate.publication_id = {child}::uuid
          AND {scope}
          AND ROW({candidate_row}) IS DISTINCT FROM ROW({source_row})
    ) THEN RAISE EXCEPTION 'coupon pit verbatim conflict:{surface}'; END IF;
END
$coupon_pit_copy_gates$;
COMMIT;
"""


def _artifact_rows(artifacts: CouponPitArtifacts, plan: CouponPitPlan, *, start_after: int, limit: int) -> tuple[list[dict[str, Any]], int]:
    if start_after < 0 or limit <= 0:
        raise CursorError("invalid_cursor_or_limit")
    conn, state = _connect()
    try:
        total = int(conn.execute("SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)", [artifacts.path(OUTPUT_RETURNS), plan.cutoff]).fetchone()[0])
        raw = conn.execute(
            """SELECT CAST(month AS DATE), cusip_id, distribution_rule, reference_cusip9, distribution_decision_id,
                      total_return, price_return, carry_return, exit_basis, exit_reason, suspect, payload
               FROM read_parquet(?)
               WHERE CAST(month AS DATE) <= CAST(? AS DATE)
               ORDER BY CAST(month AS DATE), cusip_id LIMIT ? OFFSET ?""",
            [artifacts.path(OUTPUT_RETURNS), plan.cutoff, limit, start_after],
        ).fetchall()
    finally:
        conn.close()
        state.cleanup()
    if total != plan.counts["rows_at_or_before_cutoff"] - plan.dropped_rows:
        raise PlanError("coupon_pit_artifact_rows_at_or_before_cutoff_mismatch")
    marker = _marker(plan)
    rows: list[dict[str, Any]] = []
    for month, cusip, rule, reference, decision, total_return, price_return, carry_return, exit_basis, exit_reason, suspect, payload in raw:
        try:
            payload_object = json.loads(payload) if isinstance(payload, str) and payload else {}
        except ValueError:
            payload_object = {"raw_payload": payload}
        if not isinstance(payload_object, dict):
            payload_object = {"raw_payload": payload_object}
        payload_object = {**payload_object, "coupon_pit_repair": marker}
        rows.append({
            "publication_id": plan.publication_id,
            "month": _scalar_month(month),
            "cusip_id": cusip,
            "distribution_rule": rule or "rule_144a",
            "reference_cusip9": reference or cusip,
            "distribution_decision_id": None if rule is None else decision,
            "total_return": _clean(total_return),
            "price_return": _clean(price_return),
            "carry_return": _clean(carry_return),
            "exit_basis": exit_basis or "observed",
            "exit_reason": exit_reason,
            "suspect": bool(suspect),
            "payload": payload_object,
        })
    return rows, total


def render_coupon_pit_batch_sql(plan: CouponPitPlan, artifacts: CouponPitArtifacts, *, start_after: int, limit: int) -> str:
    """One bounded, idempotent COPY batch of artifact returns rows at or before the cutoff."""
    _validate_plan(plan, artifacts)
    rows, total = _artifact_rows(artifacts, plan, start_after=start_after, limit=limit)
    committed_through = start_after + len(rows)
    columns = _COPY_COLUMNS["returns"]
    values = [tuple(row[column] for column in columns) for row in rows]
    evidence = "jsonb_build_object(" + ",".join((
        "'publication_id'," + _sql_string(plan.publication_id), "'surface','returns_coupon_pit'",
        "'contract'," + _sql_string(plan.contract), "'artifact_sha256'," + _sql_json(plan.artifact_sha256) + "::jsonb",
        "'cursor'," + str(start_after), "'selected'," + str(len(rows)), "'committed_through'," + str(committed_through),
        "'remaining'," + str(total - committed_through), "'done'," + ("true" if committed_through == total else "false"),
    )) + ")"
    emitted = render_immutable_batch(
        target=_TABLES["returns"], columns=columns, column_types=_COPY_TYPES["returns"],
        key_columns=("publication_id", "month", "cusip_id"), rows=values,
        artifact_sha256=plan.artifact_sha256[OUTPUT_RETURNS], start_after=start_after,
        committed_through=committed_through, skipped=0, target_evidence_sql=evidence,
        nullable_columns=_COPY_NULLABLE["returns"],
    )
    select_values = ", ".join(f's."{column}"' for column in columns)
    insert_select = f"SELECT {select_values} FROM _backfill_stage s"
    replay_safe = insert_select + f" WHERE EXISTS (SELECT 1 FROM bond_panel_publications p WHERE p.publication_id={_sql_string(plan.publication_id)}::uuid AND p.publication_status='prepared')"
    preamble = "SET LOCAL ROLE worker_writer;\n"
    precondition = f"""DO $coupon_pit_batch_order$
BEGIN
{_pointer_check(plan, "batch")}
    IF NOT {_child_identity_check(plan)} THEN
        RAISE EXCEPTION 'coupon pit batch requires the prepared coupon-pit child';
    END IF;
END
$coupon_pit_batch_order$;
"""
    if insert_select not in emitted or emitted.count(preamble) != 1:  # pragma: no cover - guards transport drift
        raise RuntimeError("psql_transport_shape_changed")
    return emitted.replace(preamble, preamble + precondition, 1).replace(insert_select, replay_safe, 1)


def _expected_per_year_json(plan: CouponPitPlan) -> str:
    payload = [
        {"year": int(item["year"]), "rows": int(item["rows"]), "sum_carry_before": float(item["sum_carry_before"]), "sum_carry_after": float(item["sum_carry_after"])}
        for item in plan.per_year
    ]
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def render_coupon_pit_finalize_sql(plan: CouponPitPlan, artifacts: CouponPitArtifacts) -> str:
    """Gate the loaded child structurally and against the artifact, then validate and CAS."""
    _validate_plan(plan, artifacts)
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    marker = _sql_json(_marker(plan))
    cutoff = _sql_string(plan.cutoff)
    aggregates = _sql_string(_expected_per_year_json(plan))
    dropped_keys = _dropped_keys_json(artifacts)
    invalid_identity = (
        "f.distribution_rule IS NULL"
        " OR f.reference_cusip9 IS NULL OR btrim(f.reference_cusip9) = ''"
        " OR (f.distribution_rule = 'rule_144a' AND"
        " (f.cusip_id <> f.reference_cusip9 OR f.distribution_decision_id IS NOT NULL))"
        " OR (f.distribution_rule = 'reg_s' AND nullif(f.distribution_decision_id, '') IS NULL)"
    )
    metadata = f"""          AND candidate.config_hash = {_sql_string(plan.config_hash)}
          AND candidate.input_fingerprint = {_sql_string(plan.input_fingerprint)}
          AND candidate.code_revision = {_sql_string(plan.code_revision)}
          AND candidate.first_month = head.first_month
          AND candidate.last_closed_month = head.last_closed_month
          AND candidate.open_month = head.open_month
          AND candidate.source_lineage @> jsonb_build_object('coupon_pit_repair', {marker}::jsonb)
          AND candidate.gate_evidence @> jsonb_build_object('coupon_pit_repair', {marker}::jsonb)"""
    summaries = "\n".join(
        f"""INSERT INTO pg_temp.coupon_pit_month_stats (surface, month, rows, bad_identity)
SELECT {_sql_string(surface)}, f.month, count(*), bool_or({invalid_identity})
FROM {_TABLES[surface]} f
WHERE f.publication_id = {child}::uuid
GROUP BY f.month;"""
        for surface in SURFACES
    )
    declared = {"snapshot": "snapshot_rows", "rv_signal": "rv_signal_rows", "returns": "returns_rows", "rating_pit": "ratings_pit_rows"}
    counts_block = "\n".join(
        f"""    SELECT coalesce(sum(rows), 0), min(month), max(month) INTO v_rows, v_min, v_max
    FROM pg_temp.coupon_pit_month_stats WHERE surface = {_sql_string(surface)};
    IF v_rows <> (SELECT {declared[surface]} FROM bond_panel_publications WHERE publication_id = {child}::uuid) THEN
        RAISE EXCEPTION 'coupon pit final count mismatch:{surface}';
    END IF;{chr(10) + "    v_returns_min := v_min;" + chr(10) + "    v_returns_max := v_max;" if surface == "returns" else ""}
    RAISE NOTICE 'coupon pit finalize: {surface} rows=% min=% max=% elapsed_ms=%', v_rows, v_min, v_max, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);"""
        for surface in SURFACES
    )
    identity_block = "\n".join(
        f"""    IF EXISTS (SELECT 1 FROM pg_temp.coupon_pit_month_stats WHERE surface = {_sql_string(surface)} AND bad_identity IS TRUE LIMIT 1) THEN
        RAISE EXCEPTION 'coupon pit identity coverage invalid:{surface}';
    END IF;"""
        for surface in SURFACES
    )
    coverage_specs = (
        ("rv_signal", "snapshot", " AND s.eligibility_state = 'included'", "coupon pit rv_signal coverage mismatch"),
        ("returns", "snapshot", "", "coupon pit returns coverage mismatch"),
        ("snapshot", "rating_pit", "", "coupon pit rating coverage mismatch"),
        ("rating_pit", "snapshot", "", "coupon pit rating coverage mismatch"),
    )
    coverage_blocks = [
        f"""        IF EXISTS (
            SELECT 1 FROM bond_panel_{forward} f
            WHERE f.publication_id = {child}::uuid AND f.month = v_month
              AND NOT EXISTS (
                  SELECT 1 FROM bond_panel_{probe} s
                  WHERE s.publication_id = {child}::uuid AND s.month = v_month AND s.cusip_id = f.cusip_id{extra}
              ) LIMIT 1
        ) THEN RAISE EXCEPTION '{message}'; END IF;"""
        for forward, probe, extra, message in coverage_specs
    ]
    for surface in ("rv_signal", "returns", "rating_pit"):
        coverage_blocks.append(
            f"""        IF EXISTS (
            SELECT 1 FROM bond_panel_{surface} f
            JOIN bond_panel_snapshot s ON s.publication_id = {child}::uuid AND s.month = v_month AND s.month = f.month AND s.cusip_id = f.cusip_id
            WHERE f.publication_id = {child}::uuid AND f.month = v_month
              AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id)
                  IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
            LIMIT 1
        ) THEN RAISE EXCEPTION 'coupon pit cross-surface identity mismatch:{surface}'; END IF;"""
        )
    source_projection = """SELECT DISTINCT ON (source_fact.month, source_fact.cusip_id)
                       source_fact.month, source_fact.cusip_id, source_fact.total_return, source_fact.price_return,
                       source_fact.carry_return, source_fact.exit_basis, source_fact.exit_reason, source_fact.suspect, source_fact.payload
                FROM bond_panel_returns source_fact
                JOIN pg_temp.coupon_pit_source_ancestry ancestry USING (publication_id)
                WHERE source_fact.month = v_month
                  AND NOT EXISTS (
                      SELECT 1 FROM bond_panel_returns_tombstone tomb
                      JOIN pg_temp.coupon_pit_source_ancestry tomb_ancestry USING (publication_id)
                      WHERE tomb.month = source_fact.month AND tomb.cusip_id = source_fact.cusip_id
                        AND tomb_ancestry.depth <= ancestry.depth)
                ORDER BY source_fact.month, source_fact.cusip_id, ancestry.depth"""
    coverage_blocks.append(
        f"""        -- Keys identical to the head projection; price_return and the typed exit
        -- identical per row; rows after the cutoff identical in full; repriced rows
        -- carry the marker and satisfy the carry identities.
        IF EXISTS (
            SELECT 1
            FROM (SELECT month, cusip_id, total_return, price_return, carry_return, exit_basis, exit_reason, suspect, payload
                  FROM bond_panel_returns WHERE publication_id = {child}::uuid AND month = v_month) candidate
            FULL JOIN ({source_projection}) source USING (month, cusip_id)
            WHERE (candidate.month IS NULL AND NOT EXISTS (
                       SELECT 1 FROM pg_temp.coupon_pit_dropped_keys dropped
                       WHERE dropped.month = source.month AND dropped.cusip_id = source.cusip_id))
               OR source.month IS NULL
               OR candidate.price_return IS DISTINCT FROM source.price_return
               OR candidate.exit_basis IS DISTINCT FROM source.exit_basis
               OR candidate.exit_reason IS DISTINCT FROM source.exit_reason
               OR (v_month > {cutoff}::date AND (
                       candidate.total_return IS DISTINCT FROM source.total_return
                    OR candidate.carry_return IS DISTINCT FROM source.carry_return
                    OR candidate.suspect IS DISTINCT FROM source.suspect
                    OR candidate.payload IS DISTINCT FROM source.payload))
               OR (v_month <= {cutoff}::date AND (
                       NOT candidate.payload @> jsonb_build_object('coupon_pit_repair', {marker}::jsonb)
                    OR NOT candidate.payload @> (source.payload - 'coupon_pit_repair')
                    OR (candidate.exit_basis <> 'observed' AND (
                            candidate.total_return IS DISTINCT FROM source.total_return
                         OR candidate.carry_return IS DISTINCT FROM source.carry_return
                         OR candidate.suspect IS DISTINCT FROM source.suspect))
                    OR (candidate.exit_basis = 'observed' AND (
                            candidate.carry_return IS NULL OR candidate.price_return IS NULL
                         OR abs(candidate.total_return - (candidate.price_return + candidate.carry_return)) > {COUPON_PIT_IDENTITY_TOLERANCE}
                         OR candidate.suspect IS DISTINCT FROM (abs(candidate.total_return) > {SUSPECT_ABS_RETURN})))))
            LIMIT 1
        ) THEN RAISE EXCEPTION 'coupon pit returns row gate failed at month %', v_month; END IF;
        INSERT INTO pg_temp.coupon_pit_year_carry (year, rows, sum_carry_before, sum_carry_after)
        SELECT extract(year FROM v_month)::int, count(*), sum(source.carry_return), sum(candidate.carry_return)
        FROM (SELECT month, cusip_id, carry_return FROM bond_panel_returns WHERE publication_id = {child}::uuid AND month = v_month AND exit_basis = 'observed') candidate
        JOIN ({source_projection}) source USING (month, cusip_id)
        WHERE v_month <= {cutoff}::date
        HAVING count(*) > 0
        ON CONFLICT (year) DO UPDATE SET rows = pg_temp.coupon_pit_year_carry.rows + EXCLUDED.rows,
            sum_carry_before = pg_temp.coupon_pit_year_carry.sum_carry_before + EXCLUDED.sum_carry_before,
            sum_carry_after = pg_temp.coupon_pit_year_carry.sum_carry_after + EXCLUDED.sum_carry_after;"""
    )
    coverage_block = "\n".join(coverage_blocks)
    returns_first = COUPON_PIT_RETURNS_FIRST_MONTH
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
SET LOCAL lock_timeout = {_sql_string(COUPON_PIT_FINALIZE_LOCK_TIMEOUT)};
SET LOCAL statement_timeout = {_sql_string(COUPON_PIT_FINALIZE_STATEMENT_TIMEOUT)};
-- Freeze the four fact tables for the whole validation window (SHARE blocks
-- writers, allows readers); fixed order; an operator-confirmed quiet window and
-- a 5s lock timeout fail instead of queueing an incident.
LOCK TABLE bond_panel_snapshot, bond_panel_rv_signal, bond_panel_returns, bond_panel_rating_pit, bond_panel_returns_tombstone IN SHARE MODE;
DO $coupon_pit_finalize$
DECLARE
    v_month date;
    v_rows bigint;
    v_min date;
    v_max date;
    v_returns_min date;
    v_returns_max date;
    v_months integer;
    v_checked integer := 0;
    v_mismatches integer;
    v_status_rows integer;
    v_cas_rows integer;
    v_last_closed date;
    v_served bigint;
    v_started timestamptz := clock_timestamp();
BEGIN
{_tombstone_ddl_check("finalize")}
    PERFORM 1 FROM bond_panel_publications WHERE publication_id = {child}::uuid FOR UPDATE;
    PERFORM 1 FROM bond_panel_app_pointer WHERE product = {_sql_string(PRODUCT)} FOR UPDATE;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_app_pointer
        WHERE product = {_sql_string(PRODUCT)} AND publication_id IN ({head}::uuid, {child}::uuid)
    ) THEN RAISE EXCEPTION 'coupon pit finalize requires the expected head pointer or this child'; END IF;
    IF EXISTS (
        SELECT 1 FROM bond_panel_app_pointer WHERE product = {_sql_string(PRODUCT)} AND publication_id = {child}::uuid
    ) AND NOT EXISTS (
        SELECT 1 FROM bond_panel_publications WHERE publication_id = {child}::uuid AND publication_status = 'validated'
    ) THEN RAISE EXCEPTION 'coupon pit finalize cannot point at an unvalidated child'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        JOIN bond_panel_publications head ON head.publication_id = {head}::uuid
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.publication_status IN ('prepared', 'validated')
{metadata}
    ) THEN RAISE EXCEPTION 'non-identical coupon-pit publication finalization'; END IF;
    SELECT head.last_closed_month INTO v_last_closed FROM bond_panel_publications head WHERE head.publication_id = {head}::uuid;
    CREATE TEMP TABLE coupon_pit_source_ancestry (publication_id uuid PRIMARY KEY, depth integer NOT NULL) ON COMMIT DROP;
    WITH RECURSIVE source_ancestry(publication_id, parent_publication_id, config_hash, depth, path) AS (
        SELECT p.publication_id, p.parent_publication_id, p.config_hash, 0, ARRAY[p.publication_id]
        FROM bond_panel_publications p WHERE p.publication_id = {head}::uuid
        UNION ALL
        SELECT p.publication_id, p.parent_publication_id, p.config_hash, ancestry.depth + 1, ancestry.path || p.publication_id
        FROM bond_panel_publications p JOIN source_ancestry ancestry ON p.publication_id = ancestry.parent_publication_id
        WHERE NOT p.publication_id = ANY(ancestry.path)
          AND (p.config_hash = ancestry.config_hash
               OR (btrim(ancestry.config_hash::text) = {_sql_string(UNIT_REPAIR_CONFIG_HASH)} AND btrim(p.config_hash::text) = {_sql_string(CONFIG_HASH)}))
    )
    INSERT INTO pg_temp.coupon_pit_source_ancestry (publication_id, depth) SELECT publication_id, depth FROM source_ancestry;
    IF NOT EXISTS (SELECT 1 FROM pg_temp.coupon_pit_source_ancestry WHERE publication_id = {head}::uuid AND depth = 0)
       OR NOT EXISTS (SELECT 1 FROM pg_temp.coupon_pit_source_ancestry WHERE publication_id = {_sql_string(plan.unit_repair_child_publication_id)}::uuid)
       OR NOT EXISTS (SELECT 1 FROM pg_temp.coupon_pit_source_ancestry WHERE publication_id = {_sql_string(plan.root_base_publication_id)}::uuid)
    THEN RAISE EXCEPTION 'coupon pit source ancestry does not reach the unit-repair child and the frozen root'; END IF;
    -- The pinned keys the artifact dropped (no inversion at or before the month,
    -- no contractual coupon): absent from the child by contract, and nowhere else.
    CREATE TEMP TABLE coupon_pit_dropped_keys (month date NOT NULL, cusip_id text NOT NULL, PRIMARY KEY (month, cusip_id)) ON COMMIT DROP;
    INSERT INTO pg_temp.coupon_pit_dropped_keys (month, cusip_id)
    SELECT month, cusip_id FROM jsonb_to_recordset({dropped_keys}::jsonb) AS dropped(month date, cusip_id text);
    IF (SELECT count(*) FROM pg_temp.coupon_pit_dropped_keys) <> {plan.dropped_rows} THEN
        RAISE EXCEPTION 'coupon pit dropped key set does not match the pinned count';
    END IF;
    IF EXISTS (
        SELECT 1 FROM bond_panel_returns f JOIN pg_temp.coupon_pit_dropped_keys dropped USING (month, cusip_id)
        WHERE f.publication_id = {child}::uuid
    ) THEN RAISE EXCEPTION 'coupon pit dropped key present in the child'; END IF;
    -- ... and tombstoned in it, so the served view cannot fall back to the head's row.
    IF EXISTS (
        SELECT month, cusip_id FROM bond_panel_returns_tombstone WHERE publication_id = {child}::uuid
        EXCEPT SELECT month, cusip_id FROM pg_temp.coupon_pit_dropped_keys
    ) OR EXISTS (
        SELECT month, cusip_id FROM pg_temp.coupon_pit_dropped_keys
        EXCEPT SELECT month, cusip_id FROM bond_panel_returns_tombstone WHERE publication_id = {child}::uuid
    ) THEN RAISE EXCEPTION 'coupon pit tombstones must equal the pinned dropped keys'; END IF;
    CREATE TEMP TABLE coupon_pit_month_stats (surface text NOT NULL, month date NOT NULL, rows bigint NOT NULL, bad_identity boolean, PRIMARY KEY (surface, month)) ON COMMIT DROP;
{summaries}
    RAISE NOTICE 'coupon pit finalize: summaries filled months=% elapsed_ms=%', (SELECT count(*) FROM pg_temp.coupon_pit_month_stats), round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
{counts_block}
    IF v_returns_min IS DISTINCT FROM {_sql_string(returns_first)}::date THEN
        RAISE EXCEPTION 'coupon pit returns must start one month after the first snapshot month';
    END IF;
    IF v_returns_max IS DISTINCT FROM v_last_closed THEN
        RAISE EXCEPTION 'coupon pit returns must end at the head closed month';
    END IF;
    SELECT count(*) INTO v_months FROM pg_temp.coupon_pit_month_stats
    WHERE surface = 'returns' AND month BETWEEN {_sql_string(returns_first)}::date AND v_last_closed AND month = date_trunc('month', month)::date;
    IF v_months <> (SELECT count(*) FROM generate_series({_sql_string(returns_first)}::date, v_last_closed, INTERVAL '1 month')) THEN
        RAISE EXCEPTION 'coupon pit returns history is not contiguous through the head closed month';
    END IF;
{identity_block}
    CREATE TEMP TABLE coupon_pit_year_carry (year integer PRIMARY KEY, rows bigint NOT NULL, sum_carry_before numeric, sum_carry_after numeric) ON COMMIT DROP;
    FOR v_month IN SELECT DISTINCT month FROM pg_temp.coupon_pit_month_stats ORDER BY month LOOP
{coverage_block}
        v_checked := v_checked + 1;
        IF v_checked % 25 = 0 THEN
            RAISE NOTICE 'coupon pit finalize: coverage months=% elapsed_ms=%', v_checked, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
        END IF;
    END LOOP;
    RAISE NOTICE 'coupon pit finalize: coverage months=% elapsed_ms=%', v_checked, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
    -- Artifact/DB per-year carry gate: the repriced rows per year, the carry the
    -- head projection stored (before) and the carry loaded (after) must equal the
    -- artifact manifest; float64 artifact sums vs numeric sums are compared
    -- within {COUPON_PIT_CARRY_SUM_TOLERANCE}.
    CREATE TEMP TABLE coupon_pit_expected_year (year integer PRIMARY KEY, rows bigint, sum_carry_before numeric, sum_carry_after numeric) ON COMMIT DROP;
    INSERT INTO pg_temp.coupon_pit_expected_year (year, rows, sum_carry_before, sum_carry_after)
    SELECT "year", rows, sum_carry_before, sum_carry_after
    FROM jsonb_to_recordset({aggregates}::jsonb) AS expected("year" int, rows bigint, sum_carry_before numeric, sum_carry_after numeric);
    SELECT count(*) INTO v_mismatches
    FROM pg_temp.coupon_pit_expected_year e
    FULL JOIN pg_temp.coupon_pit_year_carry a USING (year)
    WHERE e.rows IS DISTINCT FROM a.rows
       OR abs(coalesce(e.sum_carry_before, 0) - coalesce(a.sum_carry_before, 0)) > {COUPON_PIT_CARRY_SUM_TOLERANCE}
       OR abs(coalesce(e.sum_carry_after, 0) - coalesce(a.sum_carry_after, 0)) > {COUPON_PIT_CARRY_SUM_TOLERANCE};
    IF v_mismatches > 0 THEN RAISE EXCEPTION 'coupon pit artifact/DB per-year carry mismatch'; END IF;
    RAISE NOTICE 'coupon pit finalize: per-year carry gate mismatches=% elapsed_ms=%', v_mismatches, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
    UPDATE bond_panel_publications
    SET publication_status = 'validated',
        validated_at = COALESCE(validated_at, now()),
        gate_evidence = gate_evidence || jsonb_build_object('coupon_pit_repair_validated', {marker}::jsonb)
    WHERE publication_id = {child}::uuid AND publication_status = 'prepared';
    GET DIAGNOSTICS v_status_rows = ROW_COUNT;
    IF v_status_rows > 1 THEN RAISE EXCEPTION 'coupon pit finalize updated more than one publication row'; END IF;
    IF v_status_rows = 0 AND NOT EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        JOIN bond_panel_publications head ON head.publication_id = {head}::uuid
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.publication_status = 'validated'
{metadata}
    ) THEN RAISE EXCEPTION 'coupon pit finalize requires one prepared-to-validated transition or an identical validated child'; END IF;
    UPDATE bond_panel_app_pointer
    SET publication_id = {child}::uuid, changed_at = now()
    WHERE product = {_sql_string(PRODUCT)} AND publication_id = {head}::uuid;
    GET DIAGNOSTICS v_cas_rows = ROW_COUNT;
    IF v_cas_rows > 1 THEN RAISE EXCEPTION 'coupon pit pointer compare-and-swap updated more than one row'; END IF;
    IF v_cas_rows = 0 AND NOT EXISTS (
        SELECT 1 FROM bond_panel_app_pointer pointer
        JOIN bond_panel_publications candidate ON candidate.publication_id = pointer.publication_id
        WHERE pointer.product = {_sql_string(PRODUCT)} AND pointer.publication_id = {child}::uuid AND candidate.publication_status = 'validated'
    ) THEN RAISE EXCEPTION 'coupon pit pointer compare-and-swap lost'; END IF;
    RAISE NOTICE 'coupon pit finalize: status_rows=% cas_rows=% elapsed_ms=%', v_status_rows, v_cas_rows, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
    -- The served surface after the switch: no dropped key falls back to an
    -- ancestor, and the view holds exactly the rows the child declares.
    IF EXISTS (
        SELECT 1 FROM bond_panel_current_returns_v1 served
        JOIN pg_temp.coupon_pit_dropped_keys dropped USING (month, cusip_id)
    ) THEN RAISE EXCEPTION 'coupon pit dropped key still served after the pointer switch'; END IF;
    SELECT count(*) INTO v_served FROM bond_panel_current_returns_v1;
    IF v_served <> (SELECT returns_rows FROM bond_panel_publications WHERE publication_id = {child}::uuid) THEN
        RAISE EXCEPTION 'coupon pit served returns count differs from the child declaration (%)', v_served;
    END IF;
    RAISE NOTICE 'coupon pit finalize: served returns=% elapsed_ms=%', v_served, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
END
$coupon_pit_finalize$;
COMMIT;
SELECT jsonb_build_object('publication_id', {child}, 'phase', 'validated_and_pointed', 'contract', {_sql_string(plan.contract)}, 'from_head_publication_id', {head}, 'artifact_sha256', {_sql_json(plan.artifact_sha256)}::jsonb) AS coupon_pit_evidence;
-- Pointer moved: refresh the *_mat mirrors the Light app reads, AFTER the
-- COMMIT, in the same order as the frozen base and unit-repair finalize; the
-- refresh phase carries its own session-level timeout and cannot undo the CAS.
\\echo coupon_pit_finalize_phase=refresh_start
SET ROLE worker_writer;
SET statement_timeout = {_sql_string(COUPON_PIT_FINALIZE_REFRESH_STATEMENT_TIMEOUT)};
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;
RESET statement_timeout;
RESET ROLE;
\\echo coupon_pit_finalize_phase=refresh_done
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--artifact-dir", type=Path, default=COUPON_PIT_DEFAULT_ARTIFACT_DIRECTORY)
    parser.add_argument("--from-head", required=True, help="the current validated head publication id the child extends")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="emit verified planning evidence as JSON")
    mode.add_argument("--emit-prepare", action="store_true")
    mode.add_argument("--emit-copy", choices=SURFACES, help="verbatim database-to-database copy of one current surface (returns: months after the cutoff)")
    mode.add_argument("--emit-batch", action="store_true", help="one bounded COPY batch of artifact returns rows at or before the cutoff")
    mode.add_argument("--emit-finalize", action="store_true")
    parser.add_argument("--start-after", type=int, default=0)
    parser.add_argument("--limit", type=int, help="bounded row count for one --emit-batch transaction")
    args = parser.parse_args(argv)
    if args.emit_batch and args.limit is None:
        parser.error("--limit is required with --emit-batch")
    try:
        artifacts = CouponPitArtifacts.open(args.artifact_dir)
        plan = build_coupon_pit_plan(artifacts, from_head_publication_id=args.from_head)
        if args.plan:
            print(json.dumps(plan.evidence(), sort_keys=True))
        elif args.emit_prepare:
            print(render_coupon_pit_prepare_sql(plan, artifacts), end="")
        elif args.emit_copy:
            print(render_coupon_pit_copy_sql(plan, artifacts, args.emit_copy), end="")
        elif args.emit_batch:
            print(render_coupon_pit_batch_sql(plan, artifacts, start_after=args.start_after, limit=args.limit), end="")
        else:
            print(render_coupon_pit_finalize_sql(plan, artifacts), end="")
    except (ArtifactPinError, PlanError, CursorError, ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
