"""Immutable row contracts, validators and canonical hashing for bond default evidence.

This module is the single Python authority for the cross-repo bundle
``contracts/bonds/default_event_bundle_v2.schema.json`` (rendered from the row
specs below) and binds the frozen ``contracts/bonds/default_event_policy_v1.json``
(policy version 1.1.0, W0 amendment 1). It performs no I/O beyond reading those
contract files and never contacts a provider or database.

Wire version ``bond_default_event_bundle_v2`` (W0 amendment 1) adds the persisted
dependency closure: the N-CEN filing ledger (``ncen_filings``), full-universe family
contexts and memberships, persisted state-proposal evidence and directional exchange
relations, evidence-only adjudication subjects and rating-row binding links. The v1
schema file is retained unchanged only as a legacy rejection artifact; v1 payloads are
refused by :meth:`CreditBundle.from_json_obj` even with ``schema_check=False``.

Conventions (frozen with the bundle contract):

* ``*_sha256`` columns are bare 64-hex SHA-256 of raw bytes or of one canonical
  row; ``*_digest`` values are ``sha256:<64hex>`` digests of canonical
  structures.
* Row encoding ``bond_credit_row_v1`` (:func:`row_encoding`): every row hash
  (``row_sha256``), the validation-receipt digest and the fingerprint are SHA-256 of
  the fields in fixed column order, each length-prefixed and type-tagged with an
  explicit null marker. ``bond_credit_validate`` recomputes all of them from the
  stored columns with the byte-identical SQL encoding, so supplied hashes are never
  trusted.
* Canonical JSON: keys sorted recursively, compact separators, UTF-8,
  ``ensure_ascii=False``, NaN/Infinity and floats rejected, duplicate keys
  rejected on load, array order preserved (arrays that are sets are stored
  sorted and unique).
* Timestamps are timezone-aware and serialized as UTC
  ``YYYY-MM-DDTHH:MM:SS.ffffffZ``; dates as ``YYYY-MM-DD``; month keys are
  first-of-month dates.
* Row identifiers are UUIDv5 under :data:`NAMESPACE` over the canonical JSON array
  of their components (:func:`identity_name`; delimiter joins are ambiguous).
* The publication id is an RFC 9562 UUIDv8 built from the first 128 bits of the
  SHA-256 input fingerprint (:func:`publication_id_for`), never from
  ``prepared_at``. This replaces the earlier UUIDv5 publication id (plan
  amendment recorded by the coordinator) so SQL can verify it with the built-in
  ``sha256()`` alone.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
POLICY_PATH = ROOT / "contracts" / "bonds" / "default_event_policy_v1.json"
SCHEMA_PATH = ROOT / "contracts" / "bonds" / "default_event_bundle_v2.schema.json"
#: Retained, unchanged v1 schema: a legacy rejection/reference artifact, never a decoder fallback.
LEGACY_V1_SCHEMA_PATH = ROOT / "contracts" / "bonds" / "default_event_bundle_v1.schema.json"
LEGACY_V1_CONTRACT_VERSION = "bond_default_event_bundle_v1"
SQL_FILES: tuple[str, ...] = (
    "bond_default_sources_v1.sql",
    "bond_credit_publications_v1.sql",
    "bond_default_events_v1.sql",
    "bond_rating_history_public_v1.sql",
)
SQL_PATHS: tuple[Path, ...] = tuple(ROOT / "schemas" / name for name in SQL_FILES)

PRODUCT = "bond_credit_evidence_v1"
CONTRACT_VERSION = "bond_default_event_bundle_v2"
POLICY_ID = "bond_default_event_policy_v1"
POLICY_VERSION = "1.1.0"
#: Canonical digests of the frozen contract documents (see ``hash_procedure``).
POLICY_DIGEST = "sha256:f0aea3d0d86daa874c237adf38de634151f18c0ef2cff5ab69e7d67588bc5662"
SCHEMA_DIGEST = "sha256:7b48e744ecb5e75e8865c6284d285462dcb9f11ca7ffa15dda1a375cd27f337f"
#: Unchanged by the v2 wire version on purpose: raw package/observation/link identities are
#: not renumbered by a contract amendment (W0 amendment 1, section 3).
NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL,
    "https://investintell.local/contracts/bonds/default_event_bundle_v1",
)
EASTERN = ZoneInfo("America/New_York")
UTC = dt.timezone.utc


class ContractError(ValueError):
    """A value violates the frozen bundle contract."""


# ---------------------------------------------------------------------------
# Canonical hashing
# ---------------------------------------------------------------------------
def _reject_floats(value: Any, path: str = "$") -> None:
    if isinstance(value, float):
        raise ContractError(f"float_not_canonical:{path}")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ContractError(f"non_string_key:{path}")
            _reject_floats(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_floats(item, f"{path}[{index}]")


def canonical_json_bytes(payload: Any) -> bytes:
    """Serialize ``payload`` in the contract's canonical digest form."""
    _reject_floats(payload)
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_of(payload: Any) -> str:
    """``sha256:<hex>`` digest of the canonical serialization of ``payload``."""
    return "sha256:" + sha256_hex(canonical_json_bytes(payload))


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise ContractError(f"duplicate_json_key:{key}")
        out[key] = value
    return out


def load_json_strict(text: str | bytes) -> Any:
    """Parse JSON rejecting duplicate keys and non-finite numbers."""

    def _bad_constant(name: str) -> Any:
        raise ContractError(f"non_finite_json_number:{name}")

    return json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_bad_constant)


def document_digest(document: Mapping[str, Any], digest_key: str) -> str:
    """Digest of a contract document with its own top-level digest key removed."""
    body = {key: value for key, value in document.items() if key != digest_key}
    return digest_of(body)


def load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    document = load_json_strict(path.read_bytes())
    computed = document_digest(document, "digest")
    if document.get("digest") != computed:
        raise ContractError("policy_embedded_digest_mismatch")
    if computed != POLICY_DIGEST:
        raise ContractError("policy_digest_not_pinned")
    return document


def load_bundle_schema(path: Path = SCHEMA_PATH) -> dict[str, Any]:
    document = load_json_strict(path.read_bytes())
    computed = document_digest(document, "x-digest")
    if document.get("x-digest") != computed:
        raise ContractError("schema_embedded_digest_mismatch")
    if computed != SCHEMA_DIGEST:
        raise ContractError("schema_digest_not_pinned")
    return document


def sql_digest(paths: Iterable[Path] = SQL_PATHS) -> str:
    """Digest of the four additive SQL files (line endings normalized to LF)."""
    entries = []
    for path in paths:
        text = path.read_bytes().replace(b"\r\n", b"\n")
        entries.append({"name": path.name, "sha256": sha256_hex(text)})
    return digest_of(entries)


def frame_digest(row_hashes: Iterable[str]) -> str:
    """Order-independent digest of a frame from its per-row ``row_sha256`` values.

    ``sha256`` over the sorted hex hashes joined by ``\\n`` (empty frame: the
    empty string). Mirrored exactly by ``bond_credit_frame_digest`` in SQL.
    """
    hashes = sorted(row_hashes)
    for value in hashes:
        if not _HEX64.fullmatch(value):
            raise ContractError("row_sha256_not_hex")
    return "sha256:" + sha256_hex("\n".join(hashes).encode("utf-8"))


#: Frozen description of the row encoding (rendered into the bundle schema).
ROW_ENCODING = (
    "bond_credit_row_v1: sha256 over UTF-8 of the fields in x-columns order; "
    "null=N; string=S<utf8 bytes>:<text>; integer=I<len>:<decimal>; boolean=B<len>:true|false; "
    "array=A<count>:<elements>; object=M<count>:<S-key><value> keys in UTF-8 byte order; "
    "ts=S of UTC YYYY-MM-DDTHH:MM:SS.ffffffZ; date=S of YYYY-MM-DD; uuid=S of lowercase text"
)


def _encoded_scalar(tag: str, text: str) -> str:
    return f"{tag}{len(text.encode('utf-8'))}:{text}"


def encode_value(value: Any) -> str:
    """Canonical row-encoding text of one encoded (``to_record``) value.

    Mirrored exactly by ``bond_credit_value_encoding`` in SQL:

    * ``None`` -> ``N``;
    * string -> ``S<utf8-length>:<text>``; integer -> ``I<length>:<decimal>``;
      boolean -> ``B<length>:true|false``;
    * array -> ``A<count>:`` + the element encodings in order;
    * object -> ``M<count>:`` + ``S``-encoded key then value encoding, keys ordered by
      their UTF-8 bytes (PostgreSQL ``COLLATE "C"``).

    Every component is length-prefixed and type-tagged, so the encoding is
    unambiguous without escaping.
    """
    if value is None:
        return "N"
    if isinstance(value, bool):
        return _encoded_scalar("B", "true" if value else "false")
    if isinstance(value, int):
        return _encoded_scalar("I", str(value))
    if isinstance(value, str):
        return _encoded_scalar("S", value)
    if isinstance(value, (list, tuple)):
        return f"A{len(value)}:" + "".join(encode_value(item) for item in value)
    if isinstance(value, Mapping):
        keys = list(value)
        if not all(isinstance(key, str) for key in keys):
            raise ContractError("row_encoding:non_string_key")
        ordered = sorted(keys, key=lambda key: key.encode("utf-8"))
        return f"M{len(ordered)}:" + "".join(
            _encoded_scalar("S", key) + encode_value(value[key]) for key in ordered
        )
    raise ContractError(f"row_encoding:unsupported_value:{type(value).__name__}")


def row_encoding(record: Mapping[str, Any], names: Iterable[str]) -> bytes:
    """UTF-8 bytes of the fixed-column-order encoding of ``record`` (``bond_credit_row_v1``).

    Timestamps are encoded as their canonical UTC text (``ts_text``), dates as
    ``YYYY-MM-DD``; SQL normalizes ``timestamptz`` columns to the same text.
    """
    return "".join(encode_value(record[name]) for name in names).encode("utf-8")


def row_encoding_sha256(record: Mapping[str, Any], names: Iterable[str]) -> str:
    return sha256_hex(row_encoding(record, names))


def encoded_digest(value: Any) -> str:
    """``sha256:<hex>`` of the ``bond_credit_row_v1`` value encoding of ``value``.

    Used for derived structural digests that SQL recomputes (``bond_credit_encoded_digest``):
    the N-CEN projection digest and the family-context universe digest.
    """
    return "sha256:" + sha256_hex(encode_value(value).encode("utf-8"))


def sql_row_spec(spec: Iterable[tuple[str, str]]) -> tuple[str, ...]:
    """SQL column spec for ``bond_credit_row_sha256``: ``t:<name>`` for timestamps, else ``j:<name>``."""
    return tuple(("t:" if kind.rstrip("?") == "ts" else "j:") + name for name, kind in spec)


SQL_FRAME_SPEC_BEGIN = "-- BEGIN GENERATED FRAME SPECS"
SQL_FRAME_SPEC_END = "-- END GENERATED FRAME SPECS"
SQL_PINS_BEGIN = "-- BEGIN GENERATED CONTRACT PINS"
SQL_PINS_END = "-- END GENERATED CONTRACT PINS"


def render_sql_pins() -> str:
    """Generated body of ``bond_credit_expected_pins``: the contract identity SQL enforces.

    ``bond_credit_validate`` refuses a publication whose contract version, policy digest or
    contract (schema) digest differ from these literals, so SQL never accepts a
    writer-selected contract identity. The SQL files' own digest is not embedded (no cycle);
    it is bound through the fingerprint.
    """
    return "\n".join((
        SQL_PINS_BEGIN,
        f"    SELECT '{CONTRACT_VERSION}'::text, '{POLICY_DIGEST}'::text, '{SCHEMA_DIGEST}'::text,",
        f"           '{FAMILY_RULE_VERSION}'::text, '{RATING_RESOLVER_ID}'::text",
        SQL_PINS_END,
    ))


def sql_frame_specs() -> dict[str, tuple[str, ...]]:
    """Every row encoding bound in SQL: all frames, the receipt and the fingerprint."""
    kinds = dict(MANIFEST_SPEC)
    specs = {cls.FRAME: sql_row_spec(cls.SPEC) for cls in ROW_TYPES}
    specs["validation_receipt"] = sql_row_spec(ValidationReceipt.SPEC)
    specs["fingerprint"] = sql_row_spec((name, kinds[name]) for name in FINGERPRINT_FIELDS)
    return specs


def render_sql_frame_specs() -> str:
    """The generated ``WHEN`` block of ``bond_credit_frame_spec`` (tested equal to the SQL file)."""
    lines = [SQL_FRAME_SPEC_BEGIN]
    for frame, spec in sorted(sql_frame_specs().items()):
        items = ", ".join(f"'{entry}'" for entry in spec)
        lines.append(f"        WHEN '{frame}' THEN ARRAY[{items}]::text[]")
    lines.append(SQL_FRAME_SPEC_END)
    return "\n".join(lines)


def id_inventory_digest(ids: Iterable[uuid.UUID]) -> str:
    """Digest of a set of row ids (sorted canonical text joined by ``\\n``)."""
    return "sha256:" + sha256_hex("\n".join(sorted(str(item) for item in ids)).encode("utf-8"))


def grid_digest(keys: Iterable[tuple[str, dt.date]]) -> str:
    """Digest of full-grid keys ``CUSIP9|YYYY-MM-DD`` sorted and joined by ``\\n``."""
    lines = sorted(f"{cusip}|{month.isoformat()}" for cusip, month in keys)
    return "sha256:" + sha256_hex("\n".join(lines).encode("utf-8"))


# ---------------------------------------------------------------------------
# Identifiers and time helpers
# ---------------------------------------------------------------------------
_HEX64 = re.compile(r"[0-9a-f]{64}")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_CUSIP = re.compile(r"[0-9A-Z*@#]{8}[0-9]")
_CIK = re.compile(r"[0-9]{10}")
_PERIOD = re.compile(r"all|[0-9]{4}|[0-9]{4}-(0[1-9]|1[0-2])")
_EDGAR_ACCEPTANCE = re.compile(r"[0-9]{14}")

UUID_PATTERN = "^" + _UUID.pattern + "$"
HEX64_PATTERN = "^" + _HEX64.pattern + "$"
DIGEST_PATTERN = "^" + _DIGEST.pattern + "$"
DATE_PATTERN = "^" + _DATE.pattern + "$"
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])-01$"
TS_PATTERN = "^" + _TS.pattern + "$"
CUSIP_PATTERN = "^" + _CUSIP.pattern + "$"
CIK_PATTERN = "^" + _CIK.pattern + "$"
PERIOD_PATTERN = "^(" + _PERIOD.pattern + ")$"
TEXT_PATTERN = r"\S"


def _char_value(char: str) -> int:
    if char.isdigit():
        return int(char)
    if "A" <= char <= "Z":
        return ord(char) - ord("A") + 10
    return {"*": 36, "@": 37, "#": 38}[char]


def cusip_check_digit(base8: str) -> str:
    """Standard CUSIP modulus-10 double-add-double check digit."""
    if len(base8) != 8 or not re.fullmatch(r"[0-9A-Z*@#]{8}", base8):
        raise ContractError("cusip_base_invalid")
    total = 0
    for index, char in enumerate(base8):
        value = _char_value(char)
        if index % 2 == 1:
            value *= 2
        total += value // 10 + value % 10
    return str((10 - total % 10) % 10)


def is_valid_cusip9(value: object) -> bool:
    return (
        isinstance(value, str)
        and _CUSIP.fullmatch(value) is not None
        and cusip_check_digit(value[:8]) == value[8]
    )


def is_valid_isin(value: object) -> bool:
    """ISO 6166 structure and Luhn check digit."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z]{2}[0-9A-Z]{9}[0-9]", value):
        return False
    digits = "".join(str(int(ch, 36)) for ch in value[:-1])
    total = 0
    for index, char in enumerate(reversed(digits)):
        number = int(char)
        if index % 2 == 0:
            number *= 2
        total += number // 10 + number % 10
    return (10 - total % 10) % 10 == int(value[-1])


def cusip_from_isin(isin: str) -> str | None:
    """CUSIP9 embedded in a validated US/CA ISIN, else ``None``."""
    if not is_valid_isin(isin) or isin[:2] not in ("US", "CA"):
        return None
    cusip = isin[2:11]
    return cusip if is_valid_cusip9(cusip) else None


def month_key(value: dt.date) -> dt.date:
    return dt.date(value.year, value.month, 1)


def add_months(month: dt.date, count: int) -> dt.date:
    index = month.year * 12 + month.month - 1 + count
    return dt.date(index // 12, index % 12 + 1, 1)


def month_end(month: dt.date) -> dt.date:
    return add_months(month_key(month), 1) - dt.timedelta(days=1)


def next_month_boundary_utc(month: dt.date) -> dt.datetime:
    """Exclusive UTC instant at which ``month`` ends (first instant of next month)."""
    nxt = add_months(month_key(month), 1)
    return dt.datetime(nxt.year, nxt.month, 1, tzinfo=UTC)


def derive_timing_class(lower_exclusive: dt.date | None, upper_inclusive: dt.date) -> str:
    """Policy ``timing.classes``: prevalent / incident / interval_uncertain."""
    if lower_exclusive is None:
        return "prevalent"
    if lower_exclusive >= upper_inclusive:
        raise ContractError("onset_bounds_not_increasing")
    first_day = lower_exclusive + dt.timedelta(days=1)
    if (first_day.year, first_day.month) == (upper_inclusive.year, upper_inclusive.month):
        return "incident"
    return "interval_uncertain"


def edgar_acceptance_to_utc(raw: str) -> dt.datetime:
    """Convert EDGAR ``ACCEPTANCE-DATETIME`` (America/New_York wall time) to UTC.

    Ambiguous fall-back wall times resolve to the later instant (conservative
    knowledge time); nonexistent spring-forward wall times are rejected.
    """
    if not isinstance(raw, str) or not _EDGAR_ACCEPTANCE.fullmatch(raw):
        raise ContractError("edgar_acceptance_raw_invalid")
    parts = tuple(int(raw[a:b]) for a, b in ((0, 4), (4, 6), (6, 8), (8, 10), (10, 12), (12, 14)))
    try:
        early = dt.datetime(*parts, tzinfo=EASTERN, fold=0)
        late = dt.datetime(*parts, tzinfo=EASTERN, fold=1)
    except ValueError as exc:
        raise ContractError("edgar_acceptance_raw_invalid") from exc
    for candidate in (early, late):
        if candidate.astimezone(UTC).astimezone(EASTERN).timetuple()[:6] != parts:
            raise ContractError("edgar_acceptance_nonexistent_local_time")
    return max(early.astimezone(UTC), late.astimezone(UTC))


def date_only_public_available_at(day: dt.date, timezone_name: str) -> dt.datetime:
    """Next-day boundary in the evidenced timezone, as UTC (policy rule)."""
    zone = ZoneInfo(timezone_name)
    nxt = day + dt.timedelta(days=1)
    local = dt.datetime(nxt.year, nxt.month, nxt.day, tzinfo=zone)
    if local.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != local.replace(tzinfo=None):
        raise ContractError("date_only_boundary_nonexistent_local_time")
    return local.astimezone(UTC)


def identity_name(kind: str, *parts: str) -> str:
    """Unambiguous UUIDv5 name: the canonical JSON array ``[kind, *parts]``.

    A delimiter join is ambiguous when a component contains the delimiter
    (``("a|b", "c")`` vs ``("a", "b|c")``); the JSON array encoding escapes
    every component, so distinct component tuples always yield distinct names.
    """
    components = (kind, *parts)
    for component in components:
        if not isinstance(component, str):
            raise ContractError("identity_component_not_text")
    return canonical_json_bytes(list(components)).decode("utf-8")


def uuid5_of(kind: str, *parts: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, identity_name(kind, *parts))


# ---------------------------------------------------------------------------
# Enumerations (mirrors of the frozen policy; tested for equality)
# ---------------------------------------------------------------------------
ENUMS: dict[str, tuple[str, ...]] = {
    "source_family": (
        "adjudication_batch", "agency_rocr_xbrl", "court_public_document",
        "issuer_public_document", "link_batch", "sec_edgar_document", "sec_edgar_index",
        "sec_ncen_acceptance_header", "sec_ncen_dera", "sec_ncen_public_xml",
        "sec_nport_dera", "sec_nport_public_xml",
    ),
    "rights_state": (
        "approved", "denied", "internal_work_product", "public_document_internal_use",
        "public_government_record", "unverified",
    ),
    "public_time_basis": (
        "archived_release_metadata", "date_only_next_day_boundary", "edgar_acceptance_datetime",
        "first_verified_retrieval", "internal_record", "rocr_file_creation",
    ),
    "observation_kind": (
        "agency_action", "court_document_passage", "edgar_passage",
        "issuer_document_passage", "nport_holding",
    ),
    "date_precision": ("day", "interval", "month", "unknown"),
    "agency_subject_kind": ("instrument", "issuer"),
    "revision_kind": ("amendment", "correction", "original", "retraction"),
    "affected_scope": ("exchange_new", "exchange_old", "issue", "issuer_affected_obligation"),
    "link_status": ("admitted", "quarantined", "rejected"),
    "subject_kind": (
        "candidate", "corroboration", "exchange_pairing", "followup_continuity", "issue_episode",
        "issue_scope",
    ),
    "adjudication_status": (
        "accepted_event", "accepted_evidence", "accepted_state", "candidate", "disputed",
        "nonqualifying", "retracted",
    ),
    "reviewer_role": ("extraction_proposer", "human_reviewer", "policy_rule_engine"),
    "source_role": (
        "adjudication_inventory", "agency_evidence", "document_evidence", "edgar_evidence",
        "link_inventory", "ncen_family_evidence", "nport_evidence",
    ),
    "ncen_form_type": ("N-CEN", "N-CEN/A"),
    "ncen_parse_status": ("index_only", "parsed", "quarantined", "retracted"),
    "ncen_public_time_basis": (
        "archived_release_metadata", "date_only_next_day_boundary", "edgar_acceptance_datetime",
        "first_verified_retrieval",
    ),
    "family_answer": ("N", "Y"),
    "adviser_role": ("adviser", "sub_adviser", "terminated_adviser", "terminated_sub_adviser"),
    "membership_state": ("complete", "incomplete"),
    "proposal_status": ("accepted_state", "candidate"),
    "primary_type": (
        "agency_issue_default", "bankruptcy", "default_state", "distressed_exchange",
        "payment_default",
    ),
    "corroboration_flag": (
        "agency_issue_default", "agency_rac_wd", "bankruptcy", "court_document",
        "distressed_exchange", "edgar_document", "issuer_agency_default_linked",
        "issuer_document", "nport_consensus_state", "payment_default",
    ),
    "admission_status": ("accepted_event", "accepted_state"),
    "timing_class": ("incident", "interval_uncertain", "prevalent"),
    "followup_status": ("nondefault_continuous", "repaid", "resolved", "unknown"),
    "completeness_basis": (
        "continuous_document", "issuer_trustee_confirmation", "none", "surveillance_receipt",
    ),
    "exit_reason": ("matured", "maturity_floor", "scope_change", "source_handoff", "unknown"),
    "exit_flag": (
        "distressed_candidate", "matured", "maturity_floor", "observed_gap_reentry",
        "scope_change", "source_handoff", "target_end_censored", "unknown",
    ),
    "coverage_state": ("not_applicable", "partial", "qualified", "unavailable"),
    "coverage_source": (
        "agency_rocr", "all", "independent_reference", "sec_edgar", "sec_nport", "unknown",
    ),
    "coverage_event_type": (
        "agency_issue_default", "all", "bankruptcy", "default_state", "distressed_exchange",
        "payment_default", "unknown",
    ),
    "rating_stratum": ("HY", "IG", "all", "unknown"),
    "exposure_cohort": ("all", "exited", "gap", "retained", "unknown"),
    "denominator_basis": ("independent_reference_enumeration", "none", "panel_exposure"),
    "view_kind": ("effective_audit", "public_pit"),
    "rating_state": (
        "carried_verified", "missing", "observed", "pit_unverified", "rights_unverified",
        "stale", "withdrawn",
    ),
    "rating_bucket": ("A", "AA", "AAA", "B", "BB", "BBB", "CCC", "D"),
    "knowledge_mode": ("current_run", "historical_reconstruction"),
    "build_scope": ("complete", "limited"),
    "quality_state": ("partial", "qualified", "unavailable"),
    "validation_verdict": ("partial", "qualified", "unavailable"),
    "field_presence_state": ("absent", "invalid", "null", "present"),
}

#: Source family -> (allowed rights states, publication role, requires rights_ref).
SOURCE_FAMILY_POLICY: dict[str, tuple[frozenset[str], str, bool]] = {
    "sec_nport_dera": (frozenset({"public_government_record"}), "nport_evidence", False),
    "sec_nport_public_xml": (frozenset({"public_government_record"}), "nport_evidence", False),
    "sec_edgar_index": (frozenset({"public_government_record"}), "edgar_evidence", False),
    "sec_edgar_document": (frozenset({"public_government_record"}), "edgar_evidence", False),
    "issuer_public_document": (
        frozenset({"public_document_internal_use"}), "document_evidence", False,
    ),
    "court_public_document": (
        frozenset({"public_document_internal_use"}), "document_evidence", False,
    ),
    "agency_rocr_xbrl": (frozenset({"approved"}), "agency_evidence", True),
    "link_batch": (frozenset({"internal_work_product"}), "link_inventory", False),
    "adjudication_batch": (frozenset({"internal_work_product"}), "adjudication_inventory", False),
    # N-CEN is fund-family provenance, never credit evidence (no observation kind exists).
    "sec_ncen_dera": (frozenset({"public_government_record"}), "ncen_family_evidence", False),
    "sec_ncen_public_xml": (frozenset({"public_government_record"}), "ncen_family_evidence", False),
    "sec_ncen_acceptance_header": (
        frozenset({"public_government_record"}), "ncen_family_evidence", False,
    ),
}
#: Source families whose packages may own ``ncen_filings`` rows (``index_only`` rows come
#: only from ``sec_edgar_index``; parsed/quarantined rows only from the N-CEN families).
NCEN_FILING_FAMILIES = frozenset({"sec_ncen_dera", "sec_ncen_public_xml", "sec_edgar_index"})
NCEN_HEADER_FAMILY = "sec_ncen_acceptance_header"
#: Final W3 FE-1a/b rule version (plan v1_8 FE-1, B0 record); pinned into contexts and SQL.
FAMILY_RULE_VERSION = "bond_default_ncen_family_fe1ab_v3"
#: N-CEN/A schema versions evidenced as complete replacements (``ncen_amendment_semantics_v1``).
NCEN_AMENDMENT_COMPLETE_SCHEMAS = frozenset({"X0505"})
NCEN_EFFECTIVE_WINDOW_MONTHS = 15
#: W2b public-rating resolver identity bound into every ``action_input_digest``.
RATING_RESOLVER_ID = "bond_public_ratings_v1"
RATING_DECLARATIONS_VERSION = "bond_rating_declarations_v1"
RATING_INPUT_MANIFEST_VERSION = "rating_input_manifest_v2"
OBSERVATION_FAMILIES: dict[str, frozenset[str]] = {
    "nport_holding": frozenset({"sec_nport_dera", "sec_nport_public_xml"}),
    "edgar_passage": frozenset({"sec_edgar_document", "sec_edgar_index"}),
    "issuer_document_passage": frozenset({"issuer_public_document"}),
    "court_document_passage": frozenset({"court_public_document"}),
    "agency_action": frozenset({"agency_rocr_xbrl"}),
}
REVIEWER_ROLE_STATUSES: dict[str, frozenset[str]] = {
    "extraction_proposer": frozenset({"candidate"}),
    "policy_rule_engine": frozenset(
        {"candidate", "accepted_state", "nonqualifying", "disputed", "retracted"}
    ),
    "human_reviewer": frozenset(ENUMS["adjudication_status"]),
}
NPORT_FLAG_FIELDS = ("nport_arrears_or_deferral", "nport_is_default", "nport_paid_in_kind")
RATED_STATES = frozenset({"observed", "carried_verified"})
#: Event-admitting statuses (only ``issue_episode`` subjects, exactly one event each).
ADMITTING_STATUSES = frozenset({"accepted_state", "accepted_event"})
#: Evidence-only subjects (W0 amendment 1 section 4.6 and coordinator decision): never
#: event-admitting, exempt from event completeness, admitted only by a human reviewer's
#: ``accepted_evidence``.
EVIDENCE_SUBJECT_KINDS = frozenset({"corroboration", "exchange_pairing", "followup_continuity", "issue_scope"})
EVIDENCE_STATUS = "accepted_evidence"
#: Subjects whose accepted evidence carries a support validity window.
SUPPORT_WINDOW_SUBJECT_KINDS = frozenset({"corroboration", "exchange_pairing"})
#: Structured-array caps (bounded provenance, never truncated: longer arrays are refused).
MAX_ADVISER_RECORDS = 20000
MAX_UNDERWRITER_RECORDS = 1000
MAX_SERIES_IDS = 5000
MAX_RAW_TEXT = 1000


# ---------------------------------------------------------------------------
# Typed field codec
# ---------------------------------------------------------------------------
def _fail(name: str, reason: str) -> ContractError:
    return ContractError(f"{name}:{reason}")


def _as_uuid(name: str, value: Any) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str) and _UUID.fullmatch(value):
        return uuid.UUID(value)
    raise _fail(name, "uuid_expected")


def _as_date(name: str, value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        raise _fail(name, "date_expected")
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str) and _DATE.fullmatch(value):
        try:
            return dt.date.fromisoformat(value)
        except ValueError as exc:
            raise _fail(name, "date_expected") from exc
    raise _fail(name, "date_expected")


def _as_ts(name: str, value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise _fail(name, "aware_timestamp_expected")
        return value.astimezone(UTC)
    if isinstance(value, str) and _TS.fullmatch(value):
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    raise _fail(name, "timestamp_expected")


def ts_text(value: dt.datetime) -> str:
    return _as_ts("timestamp", value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


#: Structured provenance records: field order (= canonical sort order) and the enum field.
STRUCTURED_KINDS: dict[str, tuple[tuple[str, ...], str | None, int]] = {
    "advisers": (("series_id", "role", "file_number_raw", "crd_raw", "lei_raw"), "role", MAX_ADVISER_RECORDS),
    "underwriters": (("file_number_raw", "crd_raw", "lei_raw"), None, MAX_UNDERWRITER_RECORDS),
}

RATING_SCOPE_FIELDS = ("agency_name", "rating_type", "scale")
UNCLEARED_RATING_SOURCE_FIELDS = ("source_ref", "rights_state", "coverage_start", "coverage_end")
RATING_DECLARATION_FIELDS = ("version", "rating_scopes", "uncleared_rating_sources")


def _null_first(value: str | None) -> tuple[int, str]:
    return (0, "") if value is None else (1, value)


def structured_sort_key(fields: tuple[str, ...], record: Mapping[str, str | None]) -> tuple[tuple[int, str], ...]:
    """Canonical order of a structured record: declared fields, null before text."""
    return tuple(_null_first(record[name]) for name in fields)


def _check_structured(name: str, base: str, value: Any) -> tuple[Mapping[str, str | None], ...]:
    fields, enum_field, cap = STRUCTURED_KINDS[base]
    if isinstance(value, (str, bytes, Mapping)) or not isinstance(value, Iterable):
        raise _fail(name, "array_expected")
    records: list[Mapping[str, str | None]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != set(fields):
            raise _fail(name, "structured_record_fields_invalid")
        checked: dict[str, str | None] = {}
        for field_name in fields:
            raw = item[field_name]
            if raw is not None and (not isinstance(raw, str) or not re.search(TEXT_PATTERN, raw)
                                    or len(raw) > MAX_RAW_TEXT):
                raise _fail(name, f"structured_value_invalid:{field_name}")
            checked[field_name] = raw
        if enum_field is not None and checked[enum_field] not in ENUMS["adviser_role"]:
            raise _fail(name, "structured_enum_invalid")
        records.append(MappingProxyType(checked))
    if len(records) > cap:
        raise _fail(name, "structured_array_too_long")
    keys = [structured_sort_key(fields, r) for r in records]
    if keys != sorted(set(keys)) or len(set(keys)) != len(keys):
        raise _fail(name, "array_must_be_sorted_unique")
    return tuple(records)


def normalize_rating_declarations_record(value: Any) -> dict[str, Any]:
    """Validate and copy the canonical W2b rating declaration record.

    The declaration constructors and canonical serializer remain in ``public_ratings``;
    this is the persisted wire validator shared by the bundle codec and publication checks.
    """
    name = "rating_declarations"
    if not isinstance(value, Mapping) or set(value) != set(RATING_DECLARATION_FIELDS):
        raise _fail(name, "fields_invalid")
    if value["version"] != RATING_DECLARATIONS_VERSION:
        raise _fail(name, "version_invalid")
    raw_scopes = value["rating_scopes"]
    raw_sources = value["uncleared_rating_sources"]
    if any(isinstance(rows, (str, bytes, Mapping)) or not isinstance(rows, Iterable)
           for rows in (raw_scopes, raw_sources)):
        raise _fail(name, "arrays_expected")

    scopes: list[dict[str, str | None]] = []
    for item in raw_scopes:
        if not isinstance(item, Mapping) or set(item) != set(RATING_SCOPE_FIELDS):
            raise _fail(name, "scope_fields_invalid")
        agency = item["agency_name"]
        if not isinstance(agency, str) or not re.search(TEXT_PATTERN, agency):
            raise _fail(name, "scope_agency_invalid")
        if any(value is not None and not isinstance(value, str)
               for value in (item["rating_type"], item["scale"])):
            raise _fail(name, "scope_values_invalid")
        scopes.append({field: item[field] for field in RATING_SCOPE_FIELDS})

    sources: list[dict[str, str | None]] = []
    for item in raw_sources:
        if not isinstance(item, Mapping) or set(item) != set(UNCLEARED_RATING_SOURCE_FIELDS):
            raise _fail(name, "uncleared_source_fields_invalid")
        source_ref = item["source_ref"]
        rights_state = item["rights_state"]
        if not isinstance(source_ref, str) or not re.search(TEXT_PATTERN, source_ref):
            raise _fail(name, "uncleared_source_ref_invalid")
        if rights_state not in ENUMS["rights_state"] or rights_state == "approved":
            raise _fail(name, "uncleared_rights_state_invalid")
        bounds: list[str | None] = []
        for field in ("coverage_start", "coverage_end"):
            bound = item[field]
            if bound is not None:
                if not isinstance(bound, str) or not _DATE.fullmatch(bound):
                    raise _fail(name, "uncleared_coverage_invalid")
                try:
                    dt.date.fromisoformat(bound)
                except ValueError as exc:
                    raise _fail(name, "uncleared_coverage_invalid") from exc
            bounds.append(bound)
        start, end = bounds
        if (start is None) != (end is None) or (start is not None and end is not None and start > end):
            raise _fail(name, "uncleared_coverage_invalid")
        sources.append({field: item[field] for field in UNCLEARED_RATING_SOURCE_FIELDS})

    for label, records in (("rating_scopes", scopes), ("uncleared_rating_sources", sources)):
        keys = [canonical_json_bytes(record) for record in records]
        if keys != sorted(set(keys)):
            raise _fail(name, f"{label}_must_be_sorted_unique")
    refs = [record["source_ref"] for record in sources]
    if len(refs) != len(set(refs)):
        raise _fail(name, "uncleared_source_refs_must_be_unique")
    return {
        "version": RATING_DECLARATIONS_VERSION,
        "rating_scopes": scopes,
        "uncleared_rating_sources": sources,
    }


def _check_rating_declarations(value: Any) -> Mapping[str, Any]:
    record = normalize_rating_declarations_record(value)
    return MappingProxyType({
        "version": record["version"],
        "rating_scopes": tuple(MappingProxyType(item) for item in record["rating_scopes"]),
        "uncleared_rating_sources": tuple(
            MappingProxyType(item) for item in record["uncleared_rating_sources"]
        ),
    })


def _check_value(name: str, kind: str, value: Any, enums: Mapping[str, tuple[str, ...]]) -> Any:
    """Validate and normalize one field value; returns the Python-native value."""
    optional = kind.endswith("?")
    base = kind[:-1] if optional else kind
    if value is None:
        if optional:
            return None
        raise _fail(name, "required")
    if base == "uuid":
        return _as_uuid(name, value)
    if base == "text":
        if not isinstance(value, str) or not re.search(TEXT_PATTERN, value):
            raise _fail(name, "nonblank_text_expected")
        return value
    if base == "raw":
        if not isinstance(value, str):
            raise _fail(name, "text_expected")
        return value
    if base == "date":
        return _as_date(name, value)
    if base == "month":
        day = _as_date(name, value)
        if day.day != 1:
            raise _fail(name, "month_key_expected")
        return day
    if base == "ts":
        return _as_ts(name, value)
    if base == "int":
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise _fail(name, "nonnegative_int_expected")
        return value
    if base == "bool":
        if not isinstance(value, bool):
            raise _fail(name, "bool_expected")
        return value
    if base == "hex":
        if not isinstance(value, str) or not _HEX64.fullmatch(value):
            raise _fail(name, "sha256_hex_expected")
        return value
    if base == "digest":
        if not isinstance(value, str) or not _DIGEST.fullmatch(value):
            raise _fail(name, "sha256_digest_expected")
        return value
    if base == "cusip":
        if not is_valid_cusip9(value):
            raise _fail(name, "valid_cusip9_expected")
        return value
    if base == "cik":
        if not isinstance(value, str) or not _CIK.fullmatch(value):
            raise _fail(name, "cik10_expected")
        return value
    if base == "period":
        if not isinstance(value, str) or not _PERIOD.fullmatch(value):
            raise _fail(name, "period_label_expected")
        return value
    if base == "members":
        if not isinstance(value, Mapping):
            raise _fail(name, "object_expected")
        for member, member_sha in value.items():
            if not isinstance(member, str) or not re.search(TEXT_PATTERN, member):
                raise _fail(name, "member_name_invalid")
            if not isinstance(member_sha, str) or not _HEX64.fullmatch(member_sha):
                raise _fail(name, "member_sha256_invalid")
        return MappingProxyType(dict(sorted(value.items())))  # private frozen copy
    if base == "presence":
        if not isinstance(value, Mapping):
            raise _fail(name, "object_expected")
        allowed = set(NPORT_FLAG_FIELDS)
        states = set(enums["field_presence_state"])
        if not set(value) <= allowed or not all(v in states for v in value.values()):
            raise _fail(name, "field_presence_invalid")
        return MappingProxyType(dict(sorted(value.items())))  # private frozen copy
    if base == "rating_declarations":
        return _check_rating_declarations(value)
    if base in STRUCTURED_KINDS:
        return _check_structured(name, base, value)
    if base == "boundedtext":
        if not isinstance(value, str) or not re.search(TEXT_PATTERN, value) or len(value) > MAX_RAW_TEXT:
            raise _fail(name, "bounded_text_expected")
        return value
    if base == "series_ids":
        parsed_series = _check_value(name, "texts", value, enums)
        if len(parsed_series) > MAX_SERIES_IDS or any(len(s) > MAX_RAW_TEXT for s in parsed_series):
            raise _fail(name, "series_ids_too_long")
        return parsed_series
    if base in ("uuids", "texts") or base.startswith("enums:"):
        if isinstance(value, (str, bytes, Mapping)) or not isinstance(value, Iterable):
            raise _fail(name, "array_expected")
        items = list(value)
        if base == "uuids":
            parsed: list[Any] = [_as_uuid(name, item) for item in items]
            keys = [str(item) for item in parsed]
        else:
            parsed = [_check_value(name, "text", item, enums) for item in items]
            keys = parsed
            if base.startswith("enums:"):
                allowed_items = enums[base[6:]]
                if any(item not in allowed_items for item in parsed):
                    raise _fail(name, "enum_item_invalid")
        if keys != sorted(set(keys)):
            raise _fail(name, "array_must_be_sorted_unique")
        return tuple(parsed)
    if base.startswith("enum:"):
        if value not in enums[base[5:]]:
            raise _fail(name, "enum_invalid")
        return value
    raise ContractError(f"unknown_field_kind:{kind}")


def _encode(kind: str, value: Any) -> Any:
    if value is None:
        return None
    base = kind.rstrip("?")
    if base == "uuid":
        return str(value)
    if base in ("date", "month"):
        return value.isoformat()
    if base == "ts":
        return ts_text(value)
    if base == "uuids":
        return [str(item) for item in value]
    if base in ("texts", "series_ids") or base.startswith("enums:"):
        return list(value)
    if base in ("presence", "members"):
        return dict(value)
    if base == "rating_declarations":
        return {
            "version": value["version"],
            "rating_scopes": [dict(item) for item in value["rating_scopes"]],
            "uncleared_rating_sources": [dict(item) for item in value["uncleared_rating_sources"]],
        }
    if base in STRUCTURED_KINDS:
        return [dict(record) for record in value]
    return value


def sorted_uuids(values: Iterable[uuid.UUID | str]) -> tuple[uuid.UUID, ...]:
    """Canonical (sorted, unique) tuple for ``uuids`` fields."""
    unique = {str(_as_uuid("uuids", item)) for item in values}
    return tuple(uuid.UUID(item) for item in sorted(unique))


def sorted_texts(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


class _Row:
    """Mixin for frozen row dataclasses described by ``SPEC``."""

    SPEC: ClassVar[tuple[tuple[str, str], ...]]
    FRAME: ClassVar[str]
    KEY: ClassVar[tuple[str, ...]]

    def __post_init__(self) -> None:
        for name, kind in self.SPEC:
            object.__setattr__(self, name, _check_value(name, kind, getattr(self, name), ENUMS))
        self._rules()

    def _rules(self) -> None:  # pragma: no cover - overridden where needed
        return None

    def to_record(self) -> dict[str, Any]:
        return {name: _encode(kind, getattr(self, name)) for name, kind in self.SPEC}

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> Any:
        expected = [name for name, _ in cls.SPEC]
        extra = sorted(set(record) - set(expected))
        missing = sorted(set(expected) - set(record))
        if extra or missing:
            raise ContractError(f"{cls.FRAME}:fields_mismatch:extra={extra}:missing={missing}")
        return cls(**{name: record[name] for name, _ in cls.SPEC})

    @classmethod
    def encoded_without(cls, values: Mapping[str, Any], skip: str) -> dict[str, Any]:
        return {
            name: _encode(kind, _check_value(name, kind, values[name], ENUMS))
            for name, kind in cls.SPEC
            if name != skip
        }

    def row_sha256(self) -> str:
        return row_encoding_sha256(self.to_record(), (name for name, _ in self.SPEC))

    def key(self) -> tuple[str, ...]:
        record = self.to_record()
        return tuple(str(record[name]) for name in self.KEY)


# ---------------------------------------------------------------------------
# Input ledger rows
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SourcePackage(_Row):
    """Immutable acquired source (or internal link/adjudication batch) package."""

    package_id: uuid.UUID
    source_family: str
    external_id: str
    content_sha256: str
    raw_sha256: str
    header_sha256: str | None
    member_sha256s: Mapping[str, str]
    official_url: str | None
    accession_number: str | None
    rights_state: str
    rights_ref: str | None
    parser_version: str
    schema_version: str
    retrieved_at: dt.datetime
    first_verified_public_at: dt.datetime
    public_time_basis: str
    public_time_evidence: str
    source_coverage_start: dt.date | None
    source_coverage_end: dt.date | None
    public_coverage_start: dt.date | None
    public_coverage_end: dt.date | None
    effective_coverage_start: dt.date | None
    effective_coverage_end: dt.date | None
    raw_locator: str
    revision_of_package_id: uuid.UUID | None
    sec_run_id: uuid.UUID | None
    sec_package_id: uuid.UUID | None

    FRAME: ClassVar[str] = "source_packages"
    KEY: ClassVar[tuple[str, ...]] = ("package_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("package_id", "uuid"),
        ("source_family", "enum:source_family"),
        ("external_id", "text"),
        ("content_sha256", "hex"),
        ("raw_sha256", "hex"),
        ("header_sha256", "hex?"),
        ("member_sha256s", "members"),
        ("official_url", "text?"),
        ("accession_number", "text?"),
        ("rights_state", "enum:rights_state"),
        ("rights_ref", "text?"),
        ("parser_version", "text"),
        ("schema_version", "text"),
        ("retrieved_at", "ts"),
        ("first_verified_public_at", "ts"),
        ("public_time_basis", "enum:public_time_basis"),
        ("public_time_evidence", "text"),
        ("source_coverage_start", "date?"),
        ("source_coverage_end", "date?"),
        ("public_coverage_start", "date?"),
        ("public_coverage_end", "date?"),
        ("effective_coverage_start", "date?"),
        ("effective_coverage_end", "date?"),
        ("raw_locator", "text"),
        ("revision_of_package_id", "uuid?"),
        ("sec_run_id", "uuid?"),
        ("sec_package_id", "uuid?"),
    )

    @staticmethod
    def derive_id(source_family: str, external_id: str, content_sha256: str) -> uuid.UUID:
        return uuid5_of("package", source_family, external_id, content_sha256)

    @classmethod
    def create(cls, **values: Any) -> SourcePackage:
        package_id = cls.derive_id(
            values["source_family"], values["external_id"], values["content_sha256"]
        )
        return cls(package_id=package_id, **values)

    def _rules(self) -> None:
        if self.package_id != self.derive_id(
            self.source_family, self.external_id, self.content_sha256
        ):
            raise ContractError("package_id:not_derived")
        allowed, _role, needs_ref = SOURCE_FAMILY_POLICY[self.source_family]
        if self.rights_state not in allowed:
            raise ContractError("rights_state:not_permitted_for_family")
        if needs_ref and self.rights_ref is None:
            raise ContractError("rights_ref:required_for_family")
        if self.first_verified_public_at > self.retrieved_at:
            raise ContractError("first_verified_public_at:after_retrieval")
        for start, end in (
            (self.source_coverage_start, self.source_coverage_end),
            (self.public_coverage_start, self.public_coverage_end),
            (self.effective_coverage_start, self.effective_coverage_end),
        ):
            if (start is None) != (end is None) or (start is not None and start > end):
                raise ContractError("coverage_bounds:invalid")
        if self.revision_of_package_id == self.package_id:
            raise ContractError("revision_of_package_id:self")
        if self.raw_locator.startswith(("/", "\\")) or re.match(r"[A-Za-z]:", self.raw_locator):
            raise ContractError("raw_locator:must_be_relative")
        if ".." in re.split(r"[\\/]", self.raw_locator):
            raise ContractError("raw_locator:path_traversal")


@dataclass(frozen=True)
class CreditObservation(_Row):
    """One lexical source observation (N-PORT holding, agency action or passage)."""

    observation_id: uuid.UUID
    package_id: uuid.UUID
    member_name: str
    row_locator: str
    observation_kind: str
    semantic_key: str
    accession_number: str | None
    holding_id: str | None
    cusip_raw: str | None
    cusip9: str | None
    isin_raw: str | None
    security_id: uuid.UUID | None
    issuer_cik: str | None
    registrant_cik: str | None
    series_id: str | None
    fund_family_id: str | None
    issuer_type_raw: str | None
    asset_category_raw: str | None
    report_date: dt.date | None
    effective_date: dt.date | None
    date_precision: str
    effective_lower_exclusive: dt.date | None
    effective_upper_inclusive: dt.date | None
    acceptance_raw: str | None
    acceptance_at: dt.datetime | None
    public_available_at: dt.datetime
    public_time_basis: str
    first_seen_at: dt.datetime
    nport_is_default: str | None
    nport_arrears_or_deferral: str | None
    nport_paid_in_kind: str | None
    field_presence: Mapping[str, str]
    agency_name: str | None
    agency_subject_kind: str | None
    agency_rating_type: str | None
    agency_scale: str | None
    agency_currency: str | None
    agency_rating_symbol: str | None
    agency_action_classification: str | None
    agency_action_date: dt.date | None
    agency_file_creation_at: dt.datetime | None
    document_quote: str | None
    document_location: str | None
    document_sha256: str | None
    revision_kind: str
    supersedes_observation_id: uuid.UUID | None

    FRAME: ClassVar[str] = "observations"
    KEY: ClassVar[tuple[str, ...]] = ("observation_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("observation_id", "uuid"),
        ("package_id", "uuid"),
        ("member_name", "text"),
        ("row_locator", "text"),
        ("observation_kind", "enum:observation_kind"),
        ("semantic_key", "text"),
        ("accession_number", "text?"),
        ("holding_id", "text?"),
        ("cusip_raw", "raw?"),
        ("cusip9", "cusip?"),
        ("isin_raw", "raw?"),
        ("security_id", "uuid?"),
        ("issuer_cik", "cik?"),
        ("registrant_cik", "cik?"),
        ("series_id", "text?"),
        ("fund_family_id", "text?"),
        ("issuer_type_raw", "raw?"),
        ("asset_category_raw", "raw?"),
        ("report_date", "date?"),
        ("effective_date", "date?"),
        ("date_precision", "enum:date_precision"),
        ("effective_lower_exclusive", "date?"),
        ("effective_upper_inclusive", "date?"),
        ("acceptance_raw", "raw?"),
        ("acceptance_at", "ts?"),
        ("public_available_at", "ts"),
        ("public_time_basis", "enum:public_time_basis"),
        ("first_seen_at", "ts"),
        ("nport_is_default", "raw?"),
        ("nport_arrears_or_deferral", "raw?"),
        ("nport_paid_in_kind", "raw?"),
        ("field_presence", "presence"),
        ("agency_name", "text?"),
        ("agency_subject_kind", "enum:agency_subject_kind?"),
        ("agency_rating_type", "raw?"),
        ("agency_scale", "raw?"),
        ("agency_currency", "raw?"),
        ("agency_rating_symbol", "raw?"),
        ("agency_action_classification", "raw?"),
        ("agency_action_date", "date?"),
        ("agency_file_creation_at", "ts?"),
        ("document_quote", "raw?"),
        ("document_location", "text?"),
        ("document_sha256", "hex?"),
        ("revision_kind", "enum:revision_kind"),
        ("supersedes_observation_id", "uuid?"),
    )

    @staticmethod
    def derive_id(package_id: uuid.UUID, member_name: str, row_locator: str) -> uuid.UUID:
        return uuid5_of("observation", str(package_id), member_name, row_locator)

    @classmethod
    def create(cls, **values: Any) -> CreditObservation:
        observation_id = cls.derive_id(
            _as_uuid("package_id", values["package_id"]),
            values["member_name"],
            values["row_locator"],
        )
        return cls(observation_id=observation_id, **values)

    def _rules(self) -> None:
        if self.observation_id != self.derive_id(self.package_id, self.member_name, self.row_locator):
            raise ContractError("observation_id:not_derived")
        if self.cusip9 is not None:
            from_raw = self.cusip_raw is not None and self.cusip_raw.strip().upper() == self.cusip9
            from_isin = self.isin_raw is not None and cusip_from_isin(
                self.isin_raw.strip().upper()
            ) == self.cusip9
            if not (from_raw or from_isin):
                raise ContractError("cusip9:not_supported_by_raw_identifier")
        if (self.revision_kind == "original") != (self.supersedes_observation_id is None):
            raise ContractError("supersedes_observation_id:revision_mismatch")
        if self.date_precision == "day" and self.effective_date is None:
            raise ContractError("effective_date:required_for_day_precision")
        has_bounds = self.effective_upper_inclusive is not None
        if self.date_precision == "interval":
            if not has_bounds:
                raise ContractError("effective_bounds:required_for_interval")
            lower = self.effective_lower_exclusive
            if lower is not None and lower >= self.effective_upper_inclusive:
                raise ContractError("effective_bounds:not_increasing")
        elif has_bounds or self.effective_lower_exclusive is not None:
            raise ContractError("effective_bounds:only_for_interval")
        if self.acceptance_at is not None and self.acceptance_raw is None:
            raise ContractError("acceptance_raw:required")
        if self.acceptance_raw is not None:
            converted = edgar_acceptance_to_utc(self.acceptance_raw)
            if self.acceptance_at != converted:
                raise ContractError("acceptance_at:not_converted_from_raw")
        if self.public_time_basis == "edgar_acceptance_datetime" and (
            self.acceptance_at is None or self.public_available_at != self.acceptance_at
        ):
            raise ContractError("public_available_at:must_equal_acceptance")
        if self.public_available_at > self.first_seen_at and self.public_time_basis in (
            "first_verified_retrieval",
        ):
            raise ContractError("public_available_at:after_first_seen")
        if self.observation_kind == "nport_holding":
            if self.accession_number is None or self.holding_id is None or self.report_date is None:
                raise ContractError("nport_holding:accession_holding_report_required")
            if set(self.field_presence) != set(NPORT_FLAG_FIELDS):
                raise ContractError("field_presence:all_nport_flags_required")
            for flag in NPORT_FLAG_FIELDS:
                raw = getattr(self, flag)
                state = self.field_presence[flag]
                ok = (
                    (state == "present" and raw in ("Y", "N"))
                    or (state in ("null", "absent") and raw is None)
                    or (state == "invalid" and raw is not None and raw not in ("Y", "N"))
                )
                if not ok:
                    raise ContractError(f"field_presence:{flag}_inconsistent")
        else:
            if dict(self.field_presence) or any(getattr(self, f) is not None for f in NPORT_FLAG_FIELDS):
                raise ContractError("nport_flags:only_for_nport_holding")
        if self.observation_kind == "agency_action":
            if (
                self.agency_name is None
                or self.agency_subject_kind is None
                or self.agency_action_date is None
            ):
                raise ContractError("agency_action:name_subject_action_date_required")
        elif any(
            getattr(self, name) is not None
            for name in (
                "agency_name", "agency_subject_kind", "agency_rating_type", "agency_scale",
                "agency_currency", "agency_rating_symbol", "agency_action_classification",
                "agency_action_date", "agency_file_creation_at",
            )
        ):
            raise ContractError("agency_fields:only_for_agency_action")
        if self.observation_kind.endswith("_passage") and (
            self.document_sha256 is None or self.document_location is None
        ):
            raise ContractError("document_passage:sha_and_location_required")


@dataclass(frozen=True)
class EventLink(_Row):
    """Immutable observation -> security link revision."""

    link_id: uuid.UUID
    package_id: uuid.UUID
    observation_id: uuid.UUID
    security_id: uuid.UUID
    cusip9: str
    obligor_id: str
    affected_scope: str
    valid_from: dt.date
    valid_to: dt.date | None
    link_known_at: dt.datetime
    identity_evidence_refs: tuple[str, ...]
    identity_evidence_digest: str
    status: str
    rationale: str
    supersedes_link_id: uuid.UUID | None

    FRAME: ClassVar[str] = "event_links"
    KEY: ClassVar[tuple[str, ...]] = ("link_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("link_id", "uuid"),
        ("package_id", "uuid"),
        ("observation_id", "uuid"),
        ("security_id", "uuid"),
        ("cusip9", "cusip"),
        ("obligor_id", "text"),
        ("affected_scope", "enum:affected_scope"),
        ("valid_from", "date"),
        ("valid_to", "date?"),
        ("link_known_at", "ts"),
        ("identity_evidence_refs", "texts"),
        ("identity_evidence_digest", "digest"),
        ("status", "enum:link_status"),
        ("rationale", "text"),
        ("supersedes_link_id", "uuid?"),
    )

    @classmethod
    def derive_id(cls, record: Mapping[str, Any]) -> uuid.UUID:
        body = {key: value for key, value in record.items() if key != "link_id"}
        return uuid5_of("link", digest_of(body))

    @classmethod
    def create(cls, **values: Any) -> EventLink:
        return cls(link_id=cls.derive_id(cls.encoded_without(values, "link_id")), **values)

    def _rules(self) -> None:
        if self.link_id != self.derive_id(self.to_record()):
            raise ContractError("link_id:not_derived")
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ContractError("valid_to:before_valid_from")
        if self.status == "admitted" and not self.identity_evidence_refs:
            raise ContractError("identity_evidence_refs:required_for_admitted")


@dataclass(frozen=True)
class Adjudication(_Row):
    """Append-only adjudication record (policy ``adjudication``)."""

    adjudication_id: uuid.UUID
    package_id: uuid.UUID
    subject_kind: str
    subject_id: uuid.UUID
    status: str
    supersedes_adjudication_id: uuid.UUID | None
    policy_digest: str
    reviewer_id: str
    reviewer_role: str
    adjudicated_at: dt.datetime
    rationale: str
    evidence_observation_ids: tuple[uuid.UUID, ...]
    link_ids: tuple[uuid.UUID, ...]
    proposal_evidence_ids: tuple[uuid.UUID, ...]
    support_valid_from: dt.date | None
    support_valid_to: dt.date | None

    FRAME: ClassVar[str] = "adjudications"
    KEY: ClassVar[tuple[str, ...]] = ("adjudication_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("adjudication_id", "uuid"),
        ("package_id", "uuid"),
        ("subject_kind", "enum:subject_kind"),
        ("subject_id", "uuid"),
        ("status", "enum:adjudication_status"),
        ("supersedes_adjudication_id", "uuid?"),
        ("policy_digest", "digest"),
        ("reviewer_id", "text"),
        ("reviewer_role", "enum:reviewer_role"),
        ("adjudicated_at", "ts"),
        ("rationale", "text"),
        ("evidence_observation_ids", "uuids"),
        ("link_ids", "uuids"),
        ("proposal_evidence_ids", "uuids"),
        ("support_valid_from", "date?"),
        ("support_valid_to", "date?"),
    )

    @classmethod
    def derive_id(cls, record: Mapping[str, Any]) -> uuid.UUID:
        body = {key: value for key, value in record.items() if key != "adjudication_id"}
        return uuid5_of("adjudication", digest_of(body))

    @classmethod
    def create(cls, **values: Any) -> Adjudication:
        return cls(
            adjudication_id=cls.derive_id(cls.encoded_without(values, "adjudication_id")),
            **values,
        )

    def _rules(self) -> None:
        if self.adjudication_id != self.derive_id(self.to_record()):
            raise ContractError("adjudication_id:not_derived")
        if self.status not in REVIEWER_ROLE_STATUSES[self.reviewer_role]:
            raise ContractError("status:not_permitted_for_reviewer_role")
        if self.status in ADMITTING_STATUSES:
            if self.subject_kind != "issue_episode":
                raise ContractError("subject_kind:admission_requires_issue_episode")
            if not self.evidence_observation_ids or not self.link_ids:
                raise ContractError("admission:evidence_and_links_required")
        if self.status == EVIDENCE_STATUS:
            # Evidence-only acceptance: human reviewer, evidence-only subject, cited support.
            if self.subject_kind not in EVIDENCE_SUBJECT_KINDS:
                raise ContractError("subject_kind:accepted_evidence_requires_evidence_subject")
            if self.reviewer_role != "human_reviewer":
                raise ContractError("status:accepted_evidence_requires_human_reviewer")
            if not self.evidence_observation_ids:
                raise ContractError("accepted_evidence:evidence_required")
            if self.subject_kind in ("corroboration", "exchange_pairing", "issue_scope") and not self.link_ids:
                raise ContractError("accepted_evidence:links_required")
            if self.subject_kind == "issue_scope" and self.subject_id not in self.link_ids:
                raise ContractError("issue_scope:subject_link_not_cited")
        if self.proposal_evidence_ids and self.subject_kind != "issue_episode":
            raise ContractError("proposal_evidence_ids:only_for_issue_episode")
        windowed = self.subject_kind in SUPPORT_WINDOW_SUBJECT_KINDS and self.status == EVIDENCE_STATUS
        if windowed != (self.support_valid_from is not None):
            raise ContractError("support_valid_from:required_iff_windowed_accepted_evidence")
        if self.support_valid_to is not None and (
            self.support_valid_from is None or self.support_valid_to <= self.support_valid_from
        ):
            raise ContractError("support_valid_to:not_after_support_valid_from")


# ---------------------------------------------------------------------------
# N-CEN lexical normalization (frozen W3 FE-1 rule; ASCII case folding, mirrored in SQL)
# ---------------------------------------------------------------------------
_ASCII_UPPER = str.maketrans("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
_ASCII_WS = " \t\n\r\f\v"
NCEN_FAMILY_SUFFIXES = ("FAMILY", "COMPLEX", "GROUP", "FUNDS", "FUND", "TRUST")
NCEN_MISSING_SENTINELS = frozenset({
    "N/A", "N.A.", "N.A", "NA", "N / A", "NONE", "NULL", "NIL", "-", "--", "---", "0",
    "NOT APPLICABLE", "NOT AVAILABLE", "UNKNOWN", "TBD",
})
_NCEN_FILE_NUMBER = re.compile(r"([0-9]{1,3})-([0-9]{1,9})")
_NCEN_CRD = re.compile(r"[0-9]{1,12}")
_NCEN_LEI = re.compile(r"[A-Z0-9]{20}")
_NCEN_NON_ALNUM = re.compile(r"[^A-Z0-9]")
_NCEN_SERIES = re.compile(r"S[0-9]{9}")
_NCEN_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_NCEN_COMPONENT = re.compile(r"ncenfam:[0-9a-f]{32}")


def _ncen_clean(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = raw.strip(_ASCII_WS)
    return text or None


ncen_clean = _ncen_clean
NCEN_SERIES_PATTERN = _NCEN_SERIES


def ncen_file_number(raw: str | None) -> str | None:
    """SEC file number ``NNN-N+`` canonicalized (``801-00856`` == ``801-856``); else ``None``."""
    text = _ncen_clean(raw)
    if text is None:
        return None
    match = _NCEN_FILE_NUMBER.fullmatch(text.replace(" ", ""))
    if match is None or int(match.group(2)) == 0:
        return None
    return f"{int(match.group(1))}-{int(match.group(2))}"


def ncen_crd(raw: str | None) -> str | None:
    text = _ncen_clean(raw)
    if text is None or _NCEN_CRD.fullmatch(text) is None or int(text) == 0:
        return None
    return str(int(text))


def ncen_lei(raw: str | None) -> str | None:
    text = _ncen_clean(raw)
    if text is None:
        return None
    text = text.translate(_ASCII_UPPER)
    if _NCEN_LEI.fullmatch(text) is None or set(text) == {"0"}:
        return None
    return text


def ncen_is_sentinel(raw: str | None) -> bool:
    text = _ncen_clean(raw)
    if text is None:
        return True
    collapsed = " ".join(part for part in re.split(r"[ \t\n\r\f\v]+", text.translate(_ASCII_UPPER)) if part)
    return collapsed in NCEN_MISSING_SENTINELS


def ncen_family_name_key(raw: str | None) -> str | None:
    text = _ncen_clean(raw)
    if text is None:
        return None
    key = _NCEN_NON_ALNUM.sub("", text.translate(_ASCII_UPPER))
    stripped = True
    while stripped and key:
        stripped = False
        for suffix in NCEN_FAMILY_SUFFIXES:
            if key.endswith(suffix):
                key = key[: -len(suffix)]
                stripped = True
                break
    return key or None


def ncen_family_key(answer: str | None, raw: str | None) -> str | None:
    """Normalized B.5 key of a ``Y`` answer; sentinels and blanks have no key."""
    if answer != "Y" or ncen_is_sentinel(raw):
        return None
    return ncen_family_name_key(raw)


def ncen_adviser_tokens(record: Mapping[str, str | None]) -> tuple[str, ...]:
    out = []
    for prefix, value in (("FN", ncen_file_number(record["file_number_raw"])),
                          ("CRD", ncen_crd(record["crd_raw"])),
                          ("LEI", ncen_lei(record["lei_raw"]))):
        if value is not None:
            out.append(f"{prefix}:{value}")
    return tuple(out)


def ncen_underwriter_tokens(record: Mapping[str, str | None]) -> tuple[str, ...]:
    """Underwriter disjointness uses file number and CRD only (LEI is not compared)."""
    out = []
    for prefix, value in (("FN", ncen_file_number(record["file_number_raw"])),
                          ("CRD", ncen_crd(record["crd_raw"]))):
        if value is not None:
            out.append(f"{prefix}:{value}")
    return tuple(out)


def ncen_projection(
    accession_number: str, registrant_cik: str | None, report_period_end: dt.date | None,
    family_answer: str | None, family_name_raw: str | None, reported_series_ids: Iterable[str],
    adviser_records: Iterable[Mapping[str, str | None]], underwriter_records: Iterable[Mapping[str, str | None]],
) -> list[Any]:
    """Canonical normalized family-relevant projection of one accession (no raw bytes)."""
    advisers = sorted(
        [a["series_id"] or "", a["role"] or "", ncen_file_number(a["file_number_raw"]) or "",
         ncen_crd(a["crd_raw"]) or "", ncen_lei(a["lei_raw"]) or ""]
        for a in adviser_records
    )
    underwriters = sorted(
        [ncen_file_number(u["file_number_raw"]) or "", ncen_crd(u["crd_raw"]) or "", ncen_lei(u["lei_raw"]) or ""]
        for u in underwriter_records
    )
    return [
        accession_number, registrant_cik or "",
        "" if report_period_end is None else report_period_end.isoformat(),
        family_answer or "", ncen_family_key(family_answer, family_name_raw) or "",
        sorted(reported_series_ids), advisers, underwriters,
    ]


def months_before(day: dt.date, months: int) -> dt.date:
    """Calendar date ``months`` months before ``day`` (day-of-month clamped)."""
    index = day.year * 12 + (day.month - 1) - months
    year, month = divmod(index, 12)
    month += 1
    last = month_end(dt.date(year, month, 1)).day
    return dt.date(year, month, min(day.day, last))


@dataclass(frozen=True)
class NcenFilingEvidence(_Row):
    """``ncen_filings``: one projection of one N-CEN accession from one source artifact.

    Fund-family provenance only (never a credit observation). ``public_available_at`` is the
    availability of this relied data version; ``version_evidence_filing_ids`` may cite only
    equal normalized projections of the same accession (validated at bundle level).
    """

    filing_evidence_id: uuid.UUID
    package_id: uuid.UUID
    accession_number: str
    row_locator: str
    registrant_cik: str | None
    form_type: str | None
    report_period_end: dt.date | None
    filing_date: dt.date | None
    header_package_id: uuid.UUID | None
    acceptance_raw: str | None
    acceptance_at: dt.datetime | None
    public_available_at: dt.datetime
    first_seen_at: dt.datetime
    public_time_basis: str
    version_evidence_filing_ids: tuple[uuid.UUID, ...]
    parse_status: str
    reasons: tuple[str, ...]
    family_answer: str | None
    family_name_raw: str | None
    reported_series_ids: tuple[str, ...]
    adviser_records: tuple[Mapping[str, str | None], ...]
    underwriter_records: tuple[Mapping[str, str | None], ...]
    projection_digest: str
    supersedes_filing_evidence_id: uuid.UUID | None

    FRAME: ClassVar[str] = "ncen_filings"
    KEY: ClassVar[tuple[str, ...]] = ("filing_evidence_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("filing_evidence_id", "uuid"),
        ("package_id", "uuid"),
        ("accession_number", "text"),
        ("row_locator", "text"),
        ("registrant_cik", "cik?"),
        ("form_type", "enum:ncen_form_type?"),
        ("report_period_end", "date?"),
        ("filing_date", "date?"),
        ("header_package_id", "uuid?"),
        ("acceptance_raw", "raw?"),
        ("acceptance_at", "ts?"),
        ("public_available_at", "ts"),
        ("first_seen_at", "ts"),
        ("public_time_basis", "enum:ncen_public_time_basis"),
        ("version_evidence_filing_ids", "uuids"),
        ("parse_status", "enum:ncen_parse_status"),
        ("reasons", "texts"),
        ("family_answer", "enum:family_answer?"),
        ("family_name_raw", "boundedtext?"),
        ("reported_series_ids", "series_ids"),
        ("adviser_records", "advisers"),
        ("underwriter_records", "underwriters"),
        ("projection_digest", "digest"),
        ("supersedes_filing_evidence_id", "uuid?"),
    )

    @staticmethod
    def derive_id(package_id: uuid.UUID, accession_number: str, row_locator: str) -> uuid.UUID:
        return uuid5_of("ncen_filing", str(package_id), accession_number, row_locator)

    def projection(self) -> list[Any]:
        return ncen_projection(
            self.accession_number, self.registrant_cik, self.report_period_end, self.family_answer,
            self.family_name_raw, self.reported_series_ids, self.adviser_records, self.underwriter_records,
        )

    @staticmethod
    def derive_projection_digest(**values: Any) -> str:
        return encoded_digest(ncen_projection(
            values["accession_number"], values["registrant_cik"], values["report_period_end"],
            values["family_answer"], values["family_name_raw"], values["reported_series_ids"],
            values["adviser_records"], values["underwriter_records"],
        ))

    @classmethod
    def create(cls, **values: Any) -> NcenFilingEvidence:
        package_id = _as_uuid("package_id", values["package_id"])
        filing_id = cls.derive_id(package_id, values["accession_number"], values["row_locator"])
        digest = cls.derive_projection_digest(**values)
        return cls(filing_evidence_id=filing_id, projection_digest=digest, **values)

    def _rules(self) -> None:
        if self.filing_evidence_id != self.derive_id(self.package_id, self.accession_number, self.row_locator):
            raise ContractError("filing_evidence_id:not_derived")
        if _NCEN_ACCESSION.fullmatch(self.accession_number) is None:
            raise ContractError("accession_number:invalid")
        if self.projection_digest != encoded_digest(self.projection()):
            raise ContractError("projection_digest:not_derived")
        if self.acceptance_at is not None and self.acceptance_raw is None:
            raise ContractError("acceptance_raw:required")
        if self.acceptance_raw is not None:
            if self.acceptance_at != edgar_acceptance_to_utc(self.acceptance_raw):
                raise ContractError("acceptance_at:not_converted_from_raw")
            if self.header_package_id is None:
                raise ContractError("header_package_id:required_for_acceptance")
        if self.public_time_basis == "edgar_acceptance_datetime" and (
            self.acceptance_at is None or self.public_available_at != self.acceptance_at
        ):
            raise ContractError("public_available_at:must_equal_acceptance")
        if self.public_time_basis == "date_only_next_day_boundary" and (
            self.filing_date is None
            or self.public_available_at != date_only_public_available_at(self.filing_date, "America/New_York")
        ):
            raise ContractError("public_available_at:not_date_only_boundary")
        if self.public_time_basis == "first_verified_retrieval" and self.public_available_at > self.first_seen_at:
            raise ContractError("public_available_at:after_first_seen")
        if self.filing_evidence_id in self.version_evidence_filing_ids:
            raise ContractError("version_evidence_filing_ids:self")
        if self.supersedes_filing_evidence_id == self.filing_evidence_id:
            raise ContractError("supersedes_filing_evidence_id:self")
        has_family = bool(self.adviser_records or self.underwriter_records or self.reported_series_ids
                          or self.family_answer is not None or self.family_name_raw is not None)
        if self.parse_status == "parsed":
            if (self.registrant_cik is None or self.form_type is None or self.report_period_end is None
                    or self.filing_date is None or self.acceptance_at is None):
                raise ContractError("parsed:registrant_form_period_filing_date_acceptance_required")
            if self.reasons:
                raise ContractError("parsed:reasons_must_be_empty")
        elif self.parse_status == "index_only":
            if has_family or self.version_evidence_filing_ids:
                raise ContractError("index_only:family_fields_must_be_empty")
            if self.registrant_cik is None:
                raise ContractError("index_only:registrant_required")
        else:
            if not self.reasons:
                raise ContractError(f"{self.parse_status}:reasons_required")
        if self.parse_status == "retracted" and self.supersedes_filing_evidence_id is None:
            raise ContractError("retracted:supersedes_required")


# ---------------------------------------------------------------------------
# Publication rows
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PublicationSource(_Row):
    """Many-source lineage row: one package consumed by the publication."""

    package_id: uuid.UUID
    role: str
    package_content_sha256: str
    row_count: int
    row_inventory_digest: str

    FRAME: ClassVar[str] = "publication_sources"
    KEY: ClassVar[tuple[str, ...]] = ("package_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("package_id", "uuid"),
        ("role", "enum:source_role"),
        ("package_content_sha256", "hex"),
        ("row_count", "int"),
        ("row_inventory_digest", "digest"),
    )


@dataclass(frozen=True)
class DefaultEpisode(_Row):
    """``bond_default_event_v1`` row: one issue-episode of one security."""

    security_id: uuid.UUID
    episode_id: uuid.UUID
    cusip9: str
    obligor_id: str
    issuer_episode_id: uuid.UUID
    primary_type: str
    corroboration_flags: tuple[str, ...]
    admission_status: str
    timing_class: str
    onset_date: dt.date | None
    onset_lower_exclusive: dt.date | None
    onset_upper_inclusive: dt.date
    onset_lower_evidence_ids: tuple[uuid.UUID, ...]
    recognition_date: dt.date | None
    evidence_known_at: dt.datetime
    link_known_at: dt.datetime
    evidence_observation_ids: tuple[uuid.UUID, ...]
    link_ids: tuple[uuid.UUID, ...]
    adjudication_ids: tuple[uuid.UUID, ...]
    resolution_date: dt.date | None
    resolution_refs: tuple[uuid.UUID, ...]
    resolution_known_at: dt.datetime | None
    alias_spell_id: uuid.UUID | None
    proposal_evidence_ids: tuple[uuid.UUID, ...]
    exchange_relation_ids: tuple[uuid.UUID, ...]
    dependency_digest: str
    event_input_digest: str

    FRAME: ClassVar[str] = "events"
    KEY: ClassVar[tuple[str, ...]] = ("security_id", "episode_id")
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("security_id", "uuid"),
        ("episode_id", "uuid"),
        ("cusip9", "cusip"),
        ("obligor_id", "text"),
        ("issuer_episode_id", "uuid"),
        ("primary_type", "enum:primary_type"),
        ("corroboration_flags", "enums:corroboration_flag"),
        ("admission_status", "enum:admission_status"),
        ("timing_class", "enum:timing_class"),
        ("onset_date", "date?"),
        ("onset_lower_exclusive", "date?"),
        ("onset_upper_inclusive", "date"),
        ("onset_lower_evidence_ids", "uuids"),
        ("recognition_date", "date?"),
        ("evidence_known_at", "ts"),
        ("link_known_at", "ts"),
        ("evidence_observation_ids", "uuids"),
        ("link_ids", "uuids"),
        ("adjudication_ids", "uuids"),
        ("resolution_date", "date?"),
        ("resolution_refs", "uuids"),
        ("resolution_known_at", "ts?"),
        ("alias_spell_id", "uuid?"),
        ("proposal_evidence_ids", "uuids"),
        ("exchange_relation_ids", "uuids"),
        ("dependency_digest", "digest"),
        ("event_input_digest", "digest"),
    )

    @staticmethod
    def derive_input_digest(
        evidence_observation_ids: Iterable[uuid.UUID],
        link_ids: Iterable[uuid.UUID],
        adjudication_ids: Iterable[uuid.UUID],
        proposal_evidence_ids: Iterable[uuid.UUID],
        exchange_relation_ids: Iterable[uuid.UUID],
        dependency_digest: str,
    ) -> str:
        """v2 event input digest: the three direct arrays, the two dependency edges and the
        typed dependency-closure digest (canonical ``digest_of``; mirrored in SQL)."""
        return digest_of(
            {
                "adjudication_ids": [str(x) for x in sorted_uuids(adjudication_ids)],
                "dependency_digest": dependency_digest,
                "evidence_observation_ids": [str(x) for x in sorted_uuids(evidence_observation_ids)],
                "exchange_relation_ids": [str(x) for x in sorted_uuids(exchange_relation_ids)],
                "link_ids": [str(x) for x in sorted_uuids(link_ids)],
                "proposal_evidence_ids": [str(x) for x in sorted_uuids(proposal_evidence_ids)],
            }
        )

    @staticmethod
    def derive_alias_spell_id(episode_id: uuid.UUID) -> uuid.UUID:
        """W2a's exchange alias spell identity (one spell per old issue episode)."""
        return uuid5_of("bond_default_exchange_alias_spell", str(episode_id))

    @property
    def is_exchange(self) -> bool:
        return self.primary_type == "distressed_exchange" or "distressed_exchange" in self.corroboration_flags

    def _rules(self) -> None:
        derived = derive_timing_class(self.onset_lower_exclusive, self.onset_upper_inclusive)
        if self.timing_class != derived:
            raise ContractError("timing_class:not_derived_from_bounds")
        if self.onset_date is not None and (
            self.onset_upper_inclusive != self.onset_date
            or self.onset_lower_exclusive != self.onset_date - dt.timedelta(days=1)
        ):
            raise ContractError("onset_date:bounds_mismatch")
        if (self.admission_status == "accepted_state") != (self.primary_type == "default_state"):
            raise ContractError("admission_status:primary_type_mismatch")
        if not (self.evidence_observation_ids and self.link_ids and self.adjudication_ids):
            raise ContractError("event:evidence_links_adjudications_required")
        # A lower onset bound is never invented: it needs explicit dating provenance
        # (validated against the accepting adjudication by check_bundle/bond_credit_validate).
        if (self.onset_lower_exclusive is None) != (not self.onset_lower_evidence_ids):
            raise ContractError("onset_lower_evidence_ids:required_iff_lower_bound")
        if not set(self.onset_lower_evidence_ids) <= set(self.evidence_observation_ids):
            raise ContractError("onset_lower_evidence_ids:not_in_event_evidence")
        if self.link_known_at > self.evidence_known_at:
            raise ContractError("evidence_known_at:before_link_known_at")
        if (self.resolution_date is None) != (not self.resolution_refs) or (
            (self.resolution_date is None) != (self.resolution_known_at is None)
        ):
            raise ContractError("resolution:date_refs_known_at_together")
        if self.resolution_date is not None and self.resolution_date < self.onset_upper_inclusive:
            raise ContractError("resolution_date:before_onset")
        # A completed exchange (primary or corroborating) needs its persisted relation(s); an
        # alias spell never exists without them, and non-exchange events carry neither.
        if self.is_exchange != bool(self.exchange_relation_ids):
            raise ContractError("exchange_relation_ids:required_iff_distressed_exchange")
        expected_spell = self.derive_alias_spell_id(self.episode_id) if self.exchange_relation_ids else None
        if self.alias_spell_id != expected_spell:
            raise ContractError("alias_spell_id:requires_relation_and_derivation")
        # Proposal-based N-PORT consensus is distinguishable from an explicit human state claim.
        if ("nport_consensus_state" in self.corroboration_flags) != bool(self.proposal_evidence_ids):
            raise ContractError("proposal_evidence_ids:required_iff_nport_consensus_state")
        if self.event_input_digest != self.derive_input_digest(
            self.evidence_observation_ids, self.link_ids, self.adjudication_ids,
            self.proposal_evidence_ids, self.exchange_relation_ids, self.dependency_digest,
        ):
            raise ContractError("event_input_digest:not_derived")


@dataclass(frozen=True)
class FollowUp(_Row):
    """Outcome follow-up segment ``(interval_start_exclusive, interval_end_inclusive]``."""

    security_id: uuid.UUID
    spell_id: uuid.UUID
    segment_id: uuid.UUID
    cusip9: str
    interval_start_exclusive: dt.date
    interval_end_inclusive: dt.date
    status: str
    completeness_basis: str
    evidence_observation_ids: tuple[uuid.UUID, ...]
    adjudication_ids: tuple[uuid.UUID, ...]
    known_at: dt.datetime

    FRAME: ClassVar[str] = "followups"
    KEY: ClassVar[tuple[str, ...]] = ("security_id", "spell_id", "segment_id")
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("security_id", "uuid"),
        ("spell_id", "uuid"),
        ("segment_id", "uuid"),
        ("cusip9", "cusip"),
        ("interval_start_exclusive", "date"),
        ("interval_end_inclusive", "date"),
        ("status", "enum:followup_status"),
        ("completeness_basis", "enum:completeness_basis"),
        ("evidence_observation_ids", "uuids"),
        ("adjudication_ids", "uuids"),
        ("known_at", "ts"),
    )

    def _rules(self) -> None:
        if self.interval_start_exclusive >= self.interval_end_inclusive:
            raise ContractError("followup_interval:not_increasing")
        if (self.status == "unknown") != (self.completeness_basis == "none"):
            raise ContractError("completeness_basis:unknown_iff_none")
        if self.status != "unknown" and not self.evidence_observation_ids:
            raise ContractError("followup:evidence_required")


@dataclass(frozen=True)
class ExitEvidence(_Row):
    """Typed exit reason evidence for the last panel month of a spell."""

    security_id: uuid.UUID
    last_panel_month: dt.date
    cusip9: str
    primary_reason: str
    flags: tuple[str, ...]
    next_observed_month: dt.date | None
    gap_months: int | None
    scheduled_maturity: dt.date | None
    proven_repayment_date: dt.date | None
    evidence_observation_ids: tuple[uuid.UUID, ...]
    known_at: dt.datetime

    FRAME: ClassVar[str] = "exit_evidence"
    KEY: ClassVar[tuple[str, ...]] = ("security_id", "last_panel_month")
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("security_id", "uuid"),
        ("last_panel_month", "month"),
        ("cusip9", "cusip"),
        ("primary_reason", "enum:exit_reason"),
        ("flags", "enums:exit_flag"),
        ("next_observed_month", "month?"),
        ("gap_months", "int?"),
        ("scheduled_maturity", "date?"),
        ("proven_repayment_date", "date?"),
        ("evidence_observation_ids", "uuids"),
        ("known_at", "ts"),
    )

    def _rules(self) -> None:
        reentry = "observed_gap_reentry" in self.flags
        if reentry != (self.next_observed_month is not None):
            raise ContractError("next_observed_month:reentry_flag_mismatch")
        if self.next_observed_month is not None:
            if self.next_observed_month <= self.last_panel_month:
                raise ContractError("next_observed_month:not_after_exit")
            months = (self.next_observed_month.year - self.last_panel_month.year) * 12 + (
                self.next_observed_month.month - self.last_panel_month.month
            )
            if self.gap_months != months - 1:
                raise ContractError("gap_months:inconsistent")
        elif self.gap_months is not None:
            raise ContractError("gap_months:requires_next_observed_month")
        if self.primary_reason == "matured" and self.scheduled_maturity is None:
            raise ContractError("scheduled_maturity:required_for_matured")
        if self.proven_repayment_date is not None and not self.evidence_observation_ids:
            raise ContractError("proven_repayment_date:evidence_required")


@dataclass(frozen=True)
class CoverageCell(_Row):
    """Year/month x source x event type x rating stratum x cohort coverage cell."""

    period_label: str
    source: str
    event_type: str
    rating_stratum: str
    exposure_cohort: str
    state: str
    denominator_basis: str
    denominator_count: int | None
    exposed_issue_months: int
    event_count: int
    unlinked_count: int
    date_uncertain_count: int
    unknown_outcome_issue_months: int
    source_frontier: dt.date | None
    lag_p50_days: int | None
    lag_p90_days: int | None
    lag_max_days: int | None
    rationale: str
    validation_receipt_digest: str | None

    FRAME: ClassVar[str] = "coverage"
    KEY: ClassVar[tuple[str, ...]] = (
        "period_label", "source", "event_type", "rating_stratum", "exposure_cohort",
    )
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("period_label", "period"),
        ("source", "enum:coverage_source"),
        ("event_type", "enum:coverage_event_type"),
        ("rating_stratum", "enum:rating_stratum"),
        ("exposure_cohort", "enum:exposure_cohort"),
        ("state", "enum:coverage_state"),
        ("denominator_basis", "enum:denominator_basis"),
        ("denominator_count", "int?"),
        ("exposed_issue_months", "int"),
        ("event_count", "int"),
        ("unlinked_count", "int"),
        ("date_uncertain_count", "int"),
        ("unknown_outcome_issue_months", "int"),
        ("source_frontier", "date?"),
        ("lag_p50_days", "int?"),
        ("lag_p90_days", "int?"),
        ("lag_max_days", "int?"),
        ("rationale", "text"),
        ("validation_receipt_digest", "digest?"),
    )

    def _rules(self) -> None:
        if (self.denominator_basis == "none") != (self.denominator_count is None):
            raise ContractError("denominator_count:basis_mismatch")
        if self.state == "qualified" and (
            self.validation_receipt_digest is None or self.denominator_basis == "none"
        ):
            raise ContractError("coverage:qualified_requires_receipt_and_denominator")
        lags = (self.lag_p50_days, self.lag_p90_days, self.lag_max_days)
        if any(v is None for v in lags) and not all(v is None for v in lags):
            raise ContractError("lag_summary:all_or_none")
        if lags[0] is not None and not (lags[0] <= lags[1] <= lags[2]):  # type: ignore[operator]
            raise ContractError("lag_summary:not_ordered")


@dataclass(frozen=True)
class RatingGridRow(_Row):
    """Full-grid public rating resolution for one ``(cusip, month, view)``."""

    cusip_id: str
    month: dt.date
    view_kind: str
    bucket: str | None
    state: str
    action_date: dt.date | None
    public_known_at: dt.datetime | None
    agency_source_ids: tuple[uuid.UUID, ...]
    binding_link_ids: tuple[uuid.UUID, ...]
    coverage_frontier: dt.date | None
    action_input_digest: str | None
    default_overlay_episode_id: uuid.UUID | None

    FRAME: ClassVar[str] = "ratings"
    KEY: ClassVar[tuple[str, ...]] = ("cusip_id", "month", "view_kind")
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("cusip_id", "cusip"),
        ("month", "month"),
        ("view_kind", "enum:view_kind"),
        ("bucket", "enum:rating_bucket?"),
        ("state", "enum:rating_state"),
        ("action_date", "date?"),
        ("public_known_at", "ts?"),
        ("agency_source_ids", "uuids"),
        ("binding_link_ids", "uuids"),
        ("coverage_frontier", "date?"),
        ("action_input_digest", "digest?"),
        ("default_overlay_episode_id", "uuid?"),
    )

    def _rules(self) -> None:
        if self.binding_link_ids and not self.agency_source_ids:
            raise ContractError("binding_link_ids:require_agency_source_ids")
        rated = self.state in RATED_STATES
        if (self.bucket is not None) != rated:
            raise ContractError("bucket:only_for_observed_or_carried_verified")
        if rated and (
            self.action_date is None
            or self.public_known_at is None
            or not self.agency_source_ids
            or self.action_input_digest is None
        ):
            raise ContractError("rated_row:action_known_sources_digest_required")
        if self.action_date is not None and self.action_date > month_end(self.month):
            raise ContractError("action_date:after_month_end")
        if (
            self.view_kind == "public_pit"
            and rated
            and self.public_known_at is not None
            and self.public_known_at >= next_month_boundary_utc(self.month)
        ):
            raise ContractError("public_pit:not_known_by_month_end")


@dataclass(frozen=True)
class ValidationReceipt:
    """Completeness/validation receipt; qualified requires positive evidence.

    ``rating_input_digest``/``rating_package_digest`` (both or neither) record that the
    receipt positively qualifies the rating input: the manifest ``rating_input_digest``
    and :func:`rating_package_digest` of the approved agency packages it reviewed. A
    receipt without them does not qualify any rating input, so the bundle cannot be
    ``qualified``.

    ``surveillance_start_exclusive``/``surveillance_end_inclusive`` (both or neither) are
    the structured surveillance scope: the receipt attests default surveillance of every
    CUSIP of the pinned panel grid over ``(start, end]``. Only a ``qualified`` receipt
    whose window covers a follow-up segment supports a ``surveillance_receipt`` basis.
    """

    receipt_id: uuid.UUID
    verdict: str
    scope: str
    evidence_digest: str
    reviewer_id: str
    issued_at: dt.datetime
    positive_evidence_count: int
    rating_input_digest: str | None = None
    rating_package_digest: str | None = None
    surveillance_start_exclusive: dt.date | None = None
    surveillance_end_inclusive: dt.date | None = None

    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("receipt_id", "uuid"),
        ("verdict", "enum:validation_verdict"),
        ("scope", "text"),
        ("evidence_digest", "digest"),
        ("reviewer_id", "text"),
        ("issued_at", "ts"),
        ("positive_evidence_count", "int"),
        ("rating_input_digest", "digest?"),
        ("rating_package_digest", "digest?"),
        ("surveillance_start_exclusive", "date?"),
        ("surveillance_end_inclusive", "date?"),
    )

    def __post_init__(self) -> None:
        for name, kind in self.SPEC:
            object.__setattr__(self, name, _check_value(name, kind, getattr(self, name), ENUMS))
        if self.verdict == "qualified" and self.positive_evidence_count <= 0:
            raise ContractError("validation_receipt:qualified_requires_positive_evidence")
        if (self.rating_input_digest is None) != (self.rating_package_digest is None):
            raise ContractError("validation_receipt:rating_digests_both_or_none")
        start, end = self.surveillance_start_exclusive, self.surveillance_end_inclusive
        if (start is None) != (end is None):
            raise ContractError("validation_receipt:surveillance_window_both_or_none")
        if start is not None and end is not None and start >= end:
            raise ContractError("validation_receipt:surveillance_window_not_increasing")

    def surveils(self, cusip_in_grid: bool, start_exclusive: dt.date, end_inclusive: dt.date) -> bool:
        """Qualified surveillance scope covers a panel-grid issue over ``(start, end]``."""
        return (
            self.verdict == "qualified"
            and cusip_in_grid
            and self.surveillance_start_exclusive is not None
            and self.surveillance_end_inclusive is not None
            and self.surveillance_start_exclusive <= start_exclusive
            and end_inclusive <= self.surveillance_end_inclusive
        )

    def to_record(self) -> dict[str, Any]:
        return {name: _encode(kind, getattr(self, name)) for name, kind in self.SPEC}

    def digest(self) -> str:
        """Row encoding of the receipt fields (recomputed by ``bond_credit_validate``)."""
        return "sha256:" + row_encoding_sha256(self.to_record(), (name for name, _ in self.SPEC))


RATING_WITHDRAWAL_RACS = frozenset({"WD", "WE", "WO", "WR"})
RATING_WITHDRAWAL_SYMBOLS = frozenset({"WD", "WR", "NR"})
_HIGH_LOW = r" ?\((?:high|low)\)"
#: Long-term global symbol -> bucket (W2b ``bond_public_ratings_v1``; full match; SQL mirror).
RATING_SYMBOL_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"AAA|Aaa", "AAA"),
    (rf"AA[+-]?|AA{_HIGH_LOW}|Aa[1-3]?", "AA"),
    (rf"A[+-]?|A{_HIGH_LOW}|A[1-3]", "A"),
    (rf"BBB[+-]?|BBB{_HIGH_LOW}|Baa[1-3]?", "BBB"),
    (rf"BB[+-]?|BB{_HIGH_LOW}|Ba[1-3]?", "BB"),
    (rf"B[+-]?|B{_HIGH_LOW}|B[1-3]", "B"),
    (rf"CCC[+-]?|CCC{_HIGH_LOW}|CC|C|Caa[1-3]?|Ca", "CCC"),
    (r"D", "D"),
)
_RATING_SYMBOL_RES = tuple((re.compile(pattern), bucket) for pattern, bucket in RATING_SYMBOL_PATTERNS)


def classify_rating_action(o: CreditObservation) -> tuple[str, str | None]:
    """``(kind, bucket)`` of an agency action: ``rated``/``withdrawn``/``unmapped``.

    Mirrors W2b ``classify_agency_action`` (withdrawal RAC/symbol wins; exact long-term
    global symbols only) with ASCII whitespace trimming; ``bond_credit_rating_bucket`` in SQL.
    """
    rac = (o.agency_action_classification or "").strip(_ASCII_WS).translate(_ASCII_UPPER)
    symbol = (o.agency_rating_symbol or "").strip(_ASCII_WS)
    if rac in RATING_WITHDRAWAL_RACS or symbol.translate(_ASCII_UPPER) in RATING_WITHDRAWAL_SYMBOLS:
        return "withdrawn", None
    for pattern, bucket in _RATING_SYMBOL_RES:
        if pattern.fullmatch(symbol):
            return "rated", bucket
    return "unmapped", None


def rating_action_input_digest(
    view_kind: str,
    observations: Iterable[CreditObservation],
    packages: Iterable[SourcePackage],
    links: Iterable[EventLink],
) -> str:
    """W2b ``action_input_digest``: resolver, view and the relied rows' hashes (SQL mirror)."""
    return digest_of({
        "resolver": RATING_RESOLVER_ID,
        "view_kind": view_kind,
        "observation_row_sha256": sorted({o.row_sha256() for o in observations}),
        "package_row_sha256": sorted({p.row_sha256() for p in packages}),
        "link_row_sha256": sorted({x.row_sha256() for x in links}),
    })


RATING_SOURCE_FAMILY = "agency_rocr_xbrl"


def is_approved_rating_package(package: SourcePackage) -> bool:
    """Rating input source: an agency package whose rights are ``approved``."""
    return package.source_family == RATING_SOURCE_FAMILY and package.rights_state == "approved"


def rating_package_digest(packages: Iterable[SourcePackage]) -> str | None:
    """Frame digest of the approved rating packages' row hashes; ``None`` when there are none.

    Mirrored in ``bond_credit_validate``; a qualifying receipt must carry this value.
    """
    hashes = [p.row_sha256() for p in packages if is_approved_rating_package(p)]
    return frame_digest(hashes) if hashes else None


def rating_input_manifest_record(
    declarations: Mapping[str, Any], packages: Iterable[SourcePackage],
) -> dict[str, Any]:
    """Versioned governed rating-input manifest bound by ``rating_input_digest``."""
    record = normalize_rating_declarations_record(declarations)
    return {
        "version": RATING_INPUT_MANIFEST_VERSION,
        "resolver_id": RATING_RESOLVER_ID,
        "rating_declarations": record,
        "rating_declarations_digest": digest_of(record),
        "rating_package_digest": rating_package_digest(packages),
    }


def rating_input_manifest_digest(
    declarations: Mapping[str, Any], packages: Iterable[SourcePackage],
) -> str:
    """Canonical digest of :func:`rating_input_manifest_record`."""
    return digest_of(rating_input_manifest_record(declarations, packages))


def rating_coverage(package: SourcePackage, view_kind: str) -> tuple[dt.date | None, dt.date | None]:
    """Verified coverage ``(start, frontier)`` of a rating package for one view.

    ``public_pit`` uses the public-availability coverage; ``effective_audit`` the
    effective-date coverage.
    """
    if view_kind == "public_pit":
        return package.public_coverage_start, package.public_coverage_end
    return package.effective_coverage_start, package.effective_coverage_end


# ---------------------------------------------------------------------------
# Persisted dependency closure (W0 amendment 1): family contexts/memberships, proposal
# evidence and directional exchange relations (publication-scoped output frames)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FamilyContext(_Row):
    """``family_contexts``: the full FE-1a voter universe of one report date at K.

    The id derives from rule version, R, K and the sorted vote, selection-filing and index
    package ids; it excludes the membership digest and derived times (no identity cycle).
    """

    context_id: uuid.UUID
    report_date: dt.date
    knowledge_cutoff: dt.datetime
    rule_version: str
    vote_observation_ids: tuple[uuid.UUID, ...]
    selection_filing_ids: tuple[uuid.UUID, ...]
    index_package_ids: tuple[uuid.UUID, ...]
    universe_digest: str
    membership_digest: str
    evidence_known_at: dt.datetime

    FRAME: ClassVar[str] = "family_contexts"
    KEY: ClassVar[tuple[str, ...]] = ("context_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("context_id", "uuid"),
        ("report_date", "date"),
        ("knowledge_cutoff", "ts"),
        ("rule_version", "text"),
        ("vote_observation_ids", "uuids"),
        ("selection_filing_ids", "uuids"),
        ("index_package_ids", "uuids"),
        ("universe_digest", "digest"),
        ("membership_digest", "digest"),
        ("evidence_known_at", "ts"),
    )

    @staticmethod
    def derive_id(rule_version: str, report_date: dt.date, knowledge_cutoff: dt.datetime,
                  vote_observation_ids: Iterable[uuid.UUID], selection_filing_ids: Iterable[uuid.UUID],
                  index_package_ids: Iterable[uuid.UUID]) -> uuid.UUID:
        return uuid5_of("family_context", rule_version, report_date.isoformat(), ts_text(knowledge_cutoff), digest_of({
            "index_package_ids": [str(x) for x in sorted_uuids(index_package_ids)],
            "selection_filing_ids": [str(x) for x in sorted_uuids(selection_filing_ids)],
            "vote_observation_ids": [str(x) for x in sorted_uuids(vote_observation_ids)],
        }))

    def _rules(self) -> None:
        if self.rule_version != FAMILY_RULE_VERSION:
            raise ContractError("rule_version:not_current")
        if self.context_id != self.derive_id(self.rule_version, self.report_date, self.knowledge_cutoff,
                                             self.vote_observation_ids, self.selection_filing_ids,
                                             self.index_package_ids):
            raise ContractError("context_id:not_derived")


@dataclass(frozen=True)
class FamilyMembership(_Row):
    """``family_evidence``: one voting registrant of a context, complete or incomplete."""

    family_evidence_id: uuid.UUID
    context_id: uuid.UUID
    registrant_cik: str
    voting_series_ids: tuple[str, ...]
    vote_observation_ids: tuple[uuid.UUID, ...]
    selected_filing_id: uuid.UUID | None
    blocking_filing_ids: tuple[uuid.UUID, ...]
    state: str
    reasons: tuple[str, ...]
    component_id: str | None
    valid_from: dt.date
    valid_to: dt.date

    FRAME: ClassVar[str] = "family_evidence"
    KEY: ClassVar[tuple[str, ...]] = ("context_id", "registrant_cik")
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("family_evidence_id", "uuid"),
        ("context_id", "uuid"),
        ("registrant_cik", "cik"),
        ("voting_series_ids", "series_ids"),
        ("vote_observation_ids", "uuids"),
        ("selected_filing_id", "uuid?"),
        ("blocking_filing_ids", "uuids"),
        ("state", "enum:membership_state"),
        ("reasons", "texts"),
        ("component_id", "text?"),
        ("valid_from", "date"),
        ("valid_to", "date"),
    )

    @staticmethod
    def derive_id(context_id: uuid.UUID, registrant_cik: str) -> uuid.UUID:
        return uuid5_of("family_evidence", str(context_id), registrant_cik)

    def _rules(self) -> None:
        if self.family_evidence_id != self.derive_id(self.context_id, self.registrant_cik):
            raise ContractError("family_evidence_id:not_derived")
        if self.valid_to != self.valid_from + dt.timedelta(days=1):
            raise ContractError("valid_to:must_be_report_date_plus_one_day")
        if (self.state == "complete") != (not self.reasons):
            raise ContractError("reasons:empty_iff_complete")
        if self.state == "complete" and (self.selected_filing_id is None or self.component_id is None):
            raise ContractError("complete:selected_filing_and_component_required")
        if self.state == "incomplete" and self.component_id is not None:
            raise ContractError("incomplete:component_must_be_null")
        if self.component_id is not None and _NCEN_COMPONENT.fullmatch(self.component_id) is None:
            raise ContractError("component_id:invalid")
        if not self.vote_observation_ids:
            raise ContractError("vote_observation_ids:required")


@dataclass(frozen=True)
class ProposalEvidence(_Row):
    """``proposal_evidence``: one relied W1 state-proposal revision (never self-admitting)."""

    proposal_evidence_id: uuid.UUID
    cusip9: str
    proposed_status: str
    basis: str
    onset_lower_exclusive: dt.date | None
    onset_upper_inclusive: dt.date | None
    onset_lower_evidence_ids: tuple[uuid.UUID, ...]
    onset_upper_evidence_ids: tuple[uuid.UUID, ...]
    evidence_observation_ids: tuple[uuid.UUID, ...]
    family_evidence_ids: tuple[uuid.UUID, ...]
    corroboration_adjudication_ids: tuple[uuid.UUID, ...]
    evidence_known_at: dt.datetime
    policy_digest: str

    FRAME: ClassVar[str] = "proposal_evidence"
    KEY: ClassVar[tuple[str, ...]] = ("proposal_evidence_id",)
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("proposal_evidence_id", "uuid"),
        ("cusip9", "cusip"),
        ("proposed_status", "enum:proposal_status"),
        ("basis", "text"),
        ("onset_lower_exclusive", "date?"),
        ("onset_upper_inclusive", "date?"),
        ("onset_lower_evidence_ids", "uuids"),
        ("onset_upper_evidence_ids", "uuids"),
        ("evidence_observation_ids", "uuids"),
        ("family_evidence_ids", "uuids"),
        ("corroboration_adjudication_ids", "uuids"),
        ("evidence_known_at", "ts"),
        ("policy_digest", "digest"),
    )

    @classmethod
    def derive_id(cls, record: Mapping[str, Any]) -> uuid.UUID:
        body = {k: v for k, v in record.items() if k not in ("proposal_evidence_id", "evidence_known_at")}
        return uuid5_of("proposal_evidence", digest_of(body))

    @classmethod
    def create(cls, **values: Any) -> ProposalEvidence:
        encoded = cls.encoded_without({**values, "proposal_evidence_id": None}, "proposal_evidence_id")
        return cls(proposal_evidence_id=cls.derive_id(encoded), **values)

    def _rules(self) -> None:
        if self.proposal_evidence_id != self.derive_id(self.to_record()):
            raise ContractError("proposal_evidence_id:not_derived")
        lower, upper = self.onset_lower_exclusive, self.onset_upper_inclusive
        if self.proposed_status == "accepted_state":
            if upper is None or not self.onset_upper_evidence_ids:
                raise ContractError("accepted_state:upper_bound_and_evidence_required")
            if lower is not None and lower >= upper:
                raise ContractError("onset_bounds:not_increasing")
        elif lower is not None or upper is not None or self.onset_upper_evidence_ids:
            raise ContractError("candidate:bounds_must_be_null")
        if (lower is None) != (not self.onset_lower_evidence_ids):
            raise ContractError("onset_lower_evidence_ids:required_iff_lower_bound")
        bound = set(self.onset_lower_evidence_ids) | set(self.onset_upper_evidence_ids)
        if not bound <= set(self.evidence_observation_ids) or not self.evidence_observation_ids:
            raise ContractError("evidence_observation_ids:must_cover_bound_evidence")


@dataclass(frozen=True)
class ExchangeRelation(_Row):
    """``exchange_relations``: reviewed directional old -> new continuity of one exchange.

    Not security equivalence, successor default, cure or repayment. ``valid_to`` is
    **exclusive** (null = no evidenced end), unlike the inclusive ``EventLink.valid_to``.
    """

    relation_id: uuid.UUID
    old_security_id: uuid.UUID
    old_cusip9: str
    new_security_id: uuid.UUID
    new_cusip9: str
    episode_id: uuid.UUID
    alias_spell_id: uuid.UUID
    old_link_id: uuid.UUID
    new_link_id: uuid.UUID
    exchange_document_observation_ids: tuple[uuid.UUID, ...]
    pairing_adjudication_id: uuid.UUID
    exchange_effective_date: dt.date
    valid_from: dt.date
    valid_to: dt.date | None
    evidence_known_at: dt.datetime

    FRAME: ClassVar[str] = "exchange_relations"
    KEY: ClassVar[tuple[str, ...]] = ("old_security_id", "new_security_id", "episode_id")
    SPEC: ClassVar[tuple[tuple[str, str], ...]] = (
        ("relation_id", "uuid"),
        ("old_security_id", "uuid"),
        ("old_cusip9", "cusip"),
        ("new_security_id", "uuid"),
        ("new_cusip9", "cusip"),
        ("episode_id", "uuid"),
        ("alias_spell_id", "uuid"),
        ("old_link_id", "uuid"),
        ("new_link_id", "uuid"),
        ("exchange_document_observation_ids", "uuids"),
        ("pairing_adjudication_id", "uuid"),
        ("exchange_effective_date", "date"),
        ("valid_from", "date"),
        ("valid_to", "date?"),
        ("evidence_known_at", "ts"),
    )

    @staticmethod
    def derive_id(old_security_id: uuid.UUID, new_security_id: uuid.UUID, episode_id: uuid.UUID) -> uuid.UUID:
        return uuid5_of("exchange_relation", str(old_security_id), str(new_security_id), str(episode_id))

    def _rules(self) -> None:
        if self.relation_id != self.derive_id(self.old_security_id, self.new_security_id, self.episode_id):
            raise ContractError("relation_id:not_derived")
        if self.alias_spell_id != DefaultEpisode.derive_alias_spell_id(self.episode_id):
            raise ContractError("alias_spell_id:not_derived")
        if self.old_security_id == self.new_security_id or self.old_cusip9 == self.new_cusip9:
            raise ContractError("exchange_relation:self_link")
        if self.valid_from != self.exchange_effective_date:
            raise ContractError("valid_from:must_equal_exchange_effective_date")
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ContractError("valid_to:not_after_valid_from")
        if not self.exchange_document_observation_ids:
            raise ContractError("exchange_document_observation_ids:required")


ROW_TYPES: tuple[type[_Row], ...] = (
    SourcePackage, CreditObservation, EventLink, Adjudication, NcenFilingEvidence, PublicationSource,
    DefaultEpisode, FollowUp, ExitEvidence, CoverageCell, RatingGridRow,
    FamilyContext, FamilyMembership, ProposalEvidence, ExchangeRelation,
)
INPUT_FRAMES = ("source_packages", "observations", "event_links", "adjudications", "ncen_filings")
OUTPUT_FRAMES = (
    "publication_sources", "events", "followups", "exit_evidence", "coverage", "ratings",
    "family_contexts", "family_evidence", "proposal_evidence", "exchange_relations",
)
FRAME_TYPES: dict[str, type[_Row]] = {cls.FRAME: cls for cls in ROW_TYPES}
#: Manifest (count, digest) column prefixes for output frames.
FRAME_MANIFEST_PREFIX = {
    "publication_sources": "sources",
    "events": "events",
    "followups": "followups",
    "exit_evidence": "exits",
    "coverage": "coverage",
    "ratings": "ratings",
    "family_contexts": "family_contexts",
    "family_evidence": "family_evidence",
    "proposal_evidence": "proposal_evidence",
    "exchange_relations": "exchange_relations",
}
#: Input frame -> manifest inventory digest column.
INPUT_INVENTORY_FIELD = {
    "source_packages": "package_inventory_digest",
    "observations": "observation_inventory_digest",
    "event_links": "link_inventory_digest",
    "adjudications": "adjudication_inventory_digest",
    "ncen_filings": "ncen_filing_inventory_digest",
}
#: Input frames whose rows are owned by a package (counted in ``PublicationSource``).
OWNED_INPUT_FRAMES = ("observations", "event_links", "adjudications", "ncen_filings")


# ---------------------------------------------------------------------------
# Manifest and bundle
# ---------------------------------------------------------------------------
MANIFEST_SPEC: tuple[tuple[str, str], ...] = (
    ("product", "text"),
    ("contract_version", "text"),
    ("publication_id", "uuid"),
    ("fingerprint_digest", "digest"),
    ("target_month", "month"),
    ("knowledge_cutoff", "ts"),
    ("knowledge_mode", "enum:knowledge_mode"),
    ("build_scope", "enum:build_scope"),
    ("quality_state", "enum:quality_state"),
    ("policy_digest", "digest"),
    ("contract_digest", "digest"),
    ("sql_digest", "digest"),
    ("code_digest", "digest"),
    ("panel_publication_id", "uuid"),
    ("panel_grid_digest", "digest"),
    ("panel_grid_count", "int"),
    ("issuer_mapping_digest", "digest?"),
    ("rating_declarations", "rating_declarations"),
    ("rating_declarations_digest", "digest"),
    ("rating_input_digest", "digest"),
    ("source_manifest_digest", "digest"),
    ("package_inventory_digest", "digest"),
    ("observation_inventory_digest", "digest"),
    ("link_inventory_digest", "digest"),
    ("adjudication_inventory_digest", "digest"),
    ("ncen_filing_inventory_digest", "digest"),
    ("validation_receipt_id", "uuid?"),
    ("validation_verdict", "enum:validation_verdict?"),
    ("validation_scope", "text?"),
    ("validation_evidence_digest", "digest?"),
    ("validation_reviewer_id", "text?"),
    ("validation_issued_at", "ts?"),
    ("validation_positive_evidence_count", "int?"),
    ("validation_rating_input_digest", "digest?"),
    ("validation_rating_package_digest", "digest?"),
    ("validation_surveillance_start_exclusive", "date?"),
    ("validation_surveillance_end_inclusive", "date?"),
    ("validation_digest", "digest?"),
    ("sources_count", "int"),
    ("sources_digest", "digest"),
    ("events_count", "int"),
    ("events_digest", "digest"),
    ("followups_count", "int"),
    ("followups_digest", "digest"),
    ("exits_count", "int"),
    ("exits_digest", "digest"),
    ("coverage_count", "int"),
    ("coverage_digest", "digest"),
    ("ratings_count", "int"),
    ("ratings_digest", "digest"),
    ("family_contexts_count", "int"),
    ("family_contexts_digest", "digest"),
    ("family_evidence_count", "int"),
    ("family_evidence_digest", "digest"),
    ("proposal_evidence_count", "int"),
    ("proposal_evidence_digest", "digest"),
    ("exchange_relations_count", "int"),
    ("exchange_relations_digest", "digest"),
)
#: Publication identity: every input inventory plus the new dependency-closure frames, so a
#: changed membership, proposal or pairing never leaves the publication id unchanged.
FINGERPRINT_FIELDS: tuple[str, ...] = (
    "product", "contract_version", "target_month", "knowledge_cutoff", "knowledge_mode",
    "build_scope", "policy_digest", "contract_digest", "sql_digest", "code_digest",
    "panel_publication_id", "panel_grid_digest", "panel_grid_count", "issuer_mapping_digest",
    "rating_declarations", "rating_declarations_digest", "rating_input_digest",
    "source_manifest_digest", "package_inventory_digest",
    "observation_inventory_digest", "link_inventory_digest", "adjudication_inventory_digest",
    "ncen_filing_inventory_digest", "validation_digest", "family_contexts_digest",
    "family_evidence_digest", "proposal_evidence_digest", "exchange_relations_digest",
)
VALIDATION_FIELDS: tuple[str, ...] = (
    "validation_receipt_id", "validation_verdict", "validation_scope",
    "validation_evidence_digest", "validation_reviewer_id", "validation_issued_at",
    "validation_positive_evidence_count",
)
#: Optional receipt fields (both or neither, only with a receipt): rating-input qualification.
VALIDATION_RATING_FIELDS: tuple[str, ...] = (
    "validation_rating_input_digest", "validation_rating_package_digest",
)
#: Optional receipt fields (both or neither, only with a receipt): surveillance window.
VALIDATION_SURVEILLANCE_FIELDS: tuple[str, ...] = (
    "validation_surveillance_start_exclusive", "validation_surveillance_end_inclusive",
)


@dataclass(frozen=True)
class BundleManifest:
    """Publication row (``bond_credit_publications`` minus lifecycle columns)."""

    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        names = [name for name, _ in MANIFEST_SPEC]
        extra = sorted(set(self.values) - set(names))
        missing = sorted(set(names) - set(self.values))
        if extra or missing:
            raise ContractError(f"manifest:fields_mismatch:extra={extra}:missing={missing}")
        checked = {
            name: _check_value(name, kind, self.values[name], ENUMS) for name, kind in MANIFEST_SPEC
        }
        # Private copy behind a read-only view: the caller's mapping cannot alter the manifest.
        object.__setattr__(self, "values", MappingProxyType(checked))
        if checked["product"] != PRODUCT or checked["contract_version"] != CONTRACT_VERSION:
            raise ContractError("manifest:product_or_contract_version")
        present = [checked[name] is not None for name in VALIDATION_FIELDS]
        if any(present) and not all(present):
            raise ContractError("manifest:validation_receipt_all_or_none")
        if not all(present) and any(checked[name] is not None for name in VALIDATION_RATING_FIELDS):
            raise ContractError("manifest:rating_qualification_without_receipt")
        if not all(present) and any(checked[name] is not None for name in VALIDATION_SURVEILLANCE_FIELDS):
            raise ContractError("manifest:surveillance_window_without_receipt")
        receipt = self.receipt()
        expected_digest = receipt.digest() if receipt is not None else None
        if checked["validation_digest"] != expected_digest:
            raise ContractError("manifest:validation_digest_not_derived")
        declarations = _encode("rating_declarations", checked["rating_declarations"])
        if checked["rating_declarations_digest"] != digest_of(declarations):
            raise ContractError("manifest:rating_declarations_digest_not_derived")
        if checked["fingerprint_digest"] != fingerprint_digest(checked):
            raise ContractError("manifest:fingerprint_not_derived")
        if checked["publication_id"] != publication_id_for(checked["fingerprint_digest"]):
            raise ContractError("manifest:publication_id_not_derived")

    def __getitem__(self, name: str) -> Any:
        return self.values[name]

    def receipt(self) -> ValidationReceipt | None:
        if self.values["validation_receipt_id"] is None:
            return None
        return ValidationReceipt(
            receipt_id=self.values["validation_receipt_id"],
            verdict=self.values["validation_verdict"],
            scope=self.values["validation_scope"],
            evidence_digest=self.values["validation_evidence_digest"],
            reviewer_id=self.values["validation_reviewer_id"],
            issued_at=self.values["validation_issued_at"],
            positive_evidence_count=self.values["validation_positive_evidence_count"],
            rating_input_digest=self.values["validation_rating_input_digest"],
            rating_package_digest=self.values["validation_rating_package_digest"],
            surveillance_start_exclusive=self.values["validation_surveillance_start_exclusive"],
            surveillance_end_inclusive=self.values["validation_surveillance_end_inclusive"],
        )

    def to_record(self) -> dict[str, Any]:
        return {name: _encode(kind, self.values[name]) for name, kind in MANIFEST_SPEC}

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> BundleManifest:
        return cls(dict(record))


def fingerprint_payload(values: Mapping[str, Any]) -> dict[str, Any]:
    kinds = dict(MANIFEST_SPEC)
    return {name: _encode(kinds[name], values[name]) for name in FINGERPRINT_FIELDS}


def fingerprint_digest(values: Mapping[str, Any]) -> str:
    """Row encoding of :data:`FINGERPRINT_FIELDS` (recomputed by ``bond_credit_validate``)."""
    return "sha256:" + row_encoding_sha256(fingerprint_payload(values), FINGERPRINT_FIELDS)


def publication_id_for(fingerprint: str) -> uuid.UUID:
    """RFC 9562 UUIDv8 from the first 128 bits of the SHA-256 fingerprint.

    Version nibble ``8`` and variant ``10xx`` are set; the remaining 122 bits are
    the fingerprint's. Mirrored by ``bond_credit_publication_id_for`` in SQL.
    """
    if not isinstance(fingerprint, str) or not _DIGEST.fullmatch(fingerprint):
        raise ContractError("fingerprint_digest_expected")
    raw = bytearray(bytes.fromhex(fingerprint[7:39]))
    raw[6] = (raw[6] & 0x0F) | 0x80
    raw[8] = (raw[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(raw))


@dataclass(frozen=True)
class CreditBundle:
    """A complete, canonically ordered bond-credit evidence bundle."""

    manifest: BundleManifest
    panel_grid: tuple[tuple[str, dt.date], ...]
    frames: Mapping[str, tuple[_Row, ...]]

    def __post_init__(self) -> None:
        if set(self.frames) != set(FRAME_TYPES):
            raise ContractError("bundle:frame_set_mismatch")
        for name, rows in self.frames.items():
            cls = FRAME_TYPES[name]
            if not all(type(row) is cls for row in rows):
                raise ContractError(f"bundle:{name}:row_type_mismatch")
            keys = [row.key() for row in rows]
            if keys != sorted(set(keys)):
                raise ContractError(f"bundle:{name}:rows_must_be_sorted_unique_by_key")
        grid = [(cusip, _check_value("panel_grid", "month", month, ENUMS)) for cusip, month in self.panel_grid]
        for cusip, _month in grid:
            if not is_valid_cusip9(cusip):
                raise ContractError("panel_grid:invalid_cusip9")
        texts = [f"{c}|{m.isoformat()}" for c, m in grid]
        if texts != sorted(set(texts)):
            raise ContractError("panel_grid:must_be_sorted_unique")
        object.__setattr__(self, "panel_grid", tuple(grid))
        # Private copy behind a read-only view; rows are frozen with frozen nested values.
        object.__setattr__(
            self, "frames", MappingProxyType({name: tuple(self.frames[name]) for name in sorted(self.frames)})
        )

    def verify_frames_against_manifest(self) -> None:
        """Recompute frame counts/digests, input inventories and the grid digest against the manifest."""
        for frame, prefix in FRAME_MANIFEST_PREFIX.items():
            rows = self.frames[frame]
            if len(rows) != self.manifest[f"{prefix}_count"]:
                raise ContractError(f"bundle:{frame}:count_does_not_match_manifest")
            if frame_digest(row.row_sha256() for row in rows) != self.manifest[f"{prefix}_digest"]:
                raise ContractError(f"bundle:{frame}:digest_does_not_match_manifest")
        for frame, field_name in (("publication_sources", "source_manifest_digest"), *INPUT_INVENTORY_FIELD.items()):
            if frame_digest(row.row_sha256() for row in self.frames[frame]) != self.manifest[field_name]:
                raise ContractError(f"bundle:{frame}:inventory_does_not_match_manifest")
        if (len(self.panel_grid) != self.manifest["panel_grid_count"]
                or grid_digest(self.panel_grid) != self.manifest["panel_grid_digest"]):
            raise ContractError("bundle:panel_grid_does_not_match_manifest")

    @property
    def publication_id(self) -> uuid.UUID:
        return self.manifest["publication_id"]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "manifest": self.manifest.to_record(),
            "panel_grid": [[cusip, month.isoformat()] for cusip, month in self.panel_grid],
            "frames": {name: [row.to_record() for row in rows] for name, rows in self.frames.items()},
        }

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_json_obj())

    def bundle_digest(self) -> str:
        return "sha256:" + sha256_hex(self.canonical_bytes())

    @classmethod
    def from_json_obj(cls, payload: Mapping[str, Any], *, schema_check: bool = True) -> CreditBundle:
        # Explicit wire-version gate before anything else (also with schema_check=False): a v1
        # payload is never decoded as v2, and no field is silently dropped or defaulted.
        if not isinstance(payload, Mapping):
            raise ContractError("bundle:object_expected")
        version = payload.get("contract_version")
        manifest = payload.get("manifest")
        manifest_version = manifest.get("contract_version") if isinstance(manifest, Mapping) else None
        if version != CONTRACT_VERSION or manifest_version != CONTRACT_VERSION:
            raise ContractError(f"bundle:unsupported_contract_version:{version}/{manifest_version}")
        if set(payload) != {"contract_version", "manifest", "panel_grid", "frames"}:
            raise ContractError("bundle:top_level_fields_mismatch")
        if not isinstance(payload["frames"], Mapping) or set(payload["frames"]) != set(FRAME_TYPES):
            raise ContractError("bundle:frame_set_mismatch")
        if schema_check:
            validate_against_schema(payload)
        frames = {
            name: tuple(FRAME_TYPES[name].from_record(record) for record in payload["frames"][name])
            for name in FRAME_TYPES
        }
        grid = tuple((item[0], _as_date("panel_grid", item[1])) for item in payload["panel_grid"])
        return cls(BundleManifest.from_record(payload["manifest"]), grid, frames)

    @classmethod
    def from_json_bytes(cls, data: bytes) -> CreditBundle:
        return cls.from_json_obj(load_json_strict(data))


#: Embedded digest of the retained (unchanged) v1 schema; it is never an active decoder.
LEGACY_V1_SCHEMA_DIGEST = "sha256:c93de581b501d2d3a02ae257da7afb359e25a705d4955611f8b47b5419b507b9"


def load_legacy_v1_schema(path: Path = LEGACY_V1_SCHEMA_PATH) -> dict[str, Any]:
    document = load_json_strict(path.read_bytes())
    if document.get("x-digest") != document_digest(document, "x-digest") or (
        document["x-digest"] != LEGACY_V1_SCHEMA_DIGEST
    ):
        raise ContractError("legacy_v1_schema_not_pinned")
    return document


def validate_against_schema(payload: Any, *, schema: Mapping[str, Any] | None = None) -> None:
    """Strict JSON Schema validation against the pinned bundle schema (or ``schema``)."""
    import jsonschema

    schema = load_bundle_schema() if schema is None else schema
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path))
    if errors:
        first = errors[0]
        where = "/".join(str(part) for part in first.absolute_path)
        raise ContractError(f"schema_violation:{where}:{first.message[:200]}")


def assemble_bundle(
    *,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
    build_scope: str,
    quality_state: str,
    code_digest: str,
    panel_publication_id: uuid.UUID,
    panel_grid: Iterable[tuple[str, dt.date]],
    issuer_mapping_digest: str | None,
    rating_declarations: Mapping[str, Any],
    rating_input_digest: str,
    validation_receipt: ValidationReceipt | None,
    source_packages: Iterable[SourcePackage],
    observations: Iterable[CreditObservation],
    event_links: Iterable[EventLink],
    adjudications: Iterable[Adjudication],
    events: Iterable[DefaultEpisode],
    followups: Iterable[FollowUp],
    exit_evidence: Iterable[ExitEvidence],
    coverage: Iterable[CoverageCell],
    ratings: Iterable[RatingGridRow],
    ncen_filings: Iterable[NcenFilingEvidence],
    family_contexts: Iterable[FamilyContext],
    family_evidence: Iterable[FamilyMembership],
    proposal_evidence: Iterable[ProposalEvidence],
    exchange_relations: Iterable[ExchangeRelation],
    policy_digest: str = POLICY_DIGEST,
    contract_digest: str = SCHEMA_DIGEST,
    sql_digest_value: str | None = None,
) -> CreditBundle:
    """Derive lineage, inventories, digests and the publication id; order all frames.

    The five v2 dependency frames are required keyword arguments: an empty iterable is an
    explicit statement that no output depends on them, never a decoder default.
    """

    def ordered(rows: Iterable[_Row]) -> tuple[_Row, ...]:
        return tuple(sorted(rows, key=lambda row: row.key()))

    packages = ordered(source_packages)
    obs = ordered(observations)
    links = ordered(event_links)
    adjs = ordered(adjudications)
    filings = ordered(ncen_filings)
    rows_by_package: dict[uuid.UUID, list[uuid.UUID]] = {p.package_id: [] for p in packages}  # type: ignore[attr-defined]
    for row in (*obs, *links, *adjs, *filings):
        owner = row.package_id  # type: ignore[attr-defined]
        if owner not in rows_by_package:
            raise ContractError(f"{row.FRAME}:package_not_in_bundle")
        rows_by_package[owner].append(getattr(row, row.KEY[0]))
    sources = ordered(
        PublicationSource(
            package_id=p.package_id,  # type: ignore[attr-defined]
            role=SOURCE_FAMILY_POLICY[p.source_family][1],  # type: ignore[attr-defined]
            package_content_sha256=p.content_sha256,  # type: ignore[attr-defined]
            row_count=len(rows_by_package[p.package_id]),  # type: ignore[attr-defined]
            row_inventory_digest=id_inventory_digest(rows_by_package[p.package_id]),  # type: ignore[attr-defined]
        )
        for p in packages
    )
    frames: dict[str, tuple[_Row, ...]] = {
        "source_packages": packages,
        "observations": obs,
        "event_links": links,
        "adjudications": adjs,
        "ncen_filings": filings,
        "publication_sources": sources,
        "events": ordered(events),
        "followups": ordered(followups),
        "exit_evidence": ordered(exit_evidence),
        "coverage": ordered(coverage),
        "ratings": ordered(ratings),
        "family_contexts": ordered(family_contexts),
        "family_evidence": ordered(family_evidence),
        "proposal_evidence": ordered(proposal_evidence),
        "exchange_relations": ordered(exchange_relations),
    }
    grid = tuple(sorted(set(panel_grid), key=lambda item: f"{item[0]}|{item[1].isoformat()}"))
    values: dict[str, Any] = {
        "product": PRODUCT,
        "contract_version": CONTRACT_VERSION,
        "target_month": target_month,
        "knowledge_cutoff": knowledge_cutoff,
        "knowledge_mode": knowledge_mode,
        "build_scope": build_scope,
        "quality_state": quality_state,
        "policy_digest": policy_digest,
        "contract_digest": contract_digest,
        "sql_digest": sql_digest_value if sql_digest_value is not None else sql_digest(),
        "code_digest": code_digest,
        "panel_publication_id": panel_publication_id,
        "panel_grid_digest": grid_digest(grid),
        "panel_grid_count": len(grid),
        "issuer_mapping_digest": issuer_mapping_digest,
        "rating_declarations": rating_declarations,
        "rating_declarations_digest": digest_of(normalize_rating_declarations_record(rating_declarations)),
        "rating_input_digest": rating_input_digest,
        "source_manifest_digest": frame_digest(r.row_sha256() for r in sources),
    }
    for frame, field_name in INPUT_INVENTORY_FIELD.items():
        values[field_name] = frame_digest(r.row_sha256() for r in frames[frame])
    for field_name in (*VALIDATION_FIELDS, *VALIDATION_RATING_FIELDS, *VALIDATION_SURVEILLANCE_FIELDS):
        short = field_name.removeprefix("validation_")
        values[field_name] = None if validation_receipt is None else getattr(validation_receipt, short)
    values["validation_digest"] = validation_receipt.digest() if validation_receipt else None
    for frame, prefix in FRAME_MANIFEST_PREFIX.items():
        rows = frames[frame]
        values[f"{prefix}_count"] = len(rows)
        values[f"{prefix}_digest"] = frame_digest(r.row_sha256() for r in rows)
    values["fingerprint_digest"] = fingerprint_digest(values)
    values["publication_id"] = publication_id_for(values["fingerprint_digest"])
    return CreditBundle(BundleManifest(values), grid, frames)


# ---------------------------------------------------------------------------
# Schema rendering (the committed schema file is this rendering, tested equal)
# ---------------------------------------------------------------------------
def _kind_schema(kind: str) -> dict[str, Any]:
    optional = kind.endswith("?")
    base = kind[:-1] if optional else kind
    simple: dict[str, dict[str, Any]] = {
        "uuid": {"type": "string", "pattern": UUID_PATTERN},
        "text": {"type": "string", "pattern": TEXT_PATTERN},
        "raw": {"type": "string"},
        "date": {"type": "string", "pattern": DATE_PATTERN},
        "month": {"type": "string", "pattern": MONTH_PATTERN},
        "ts": {"type": "string", "pattern": TS_PATTERN},
        "int": {"type": "integer", "minimum": 0},
        "bool": {"type": "boolean"},
        "hex": {"type": "string", "pattern": HEX64_PATTERN},
        "digest": {"type": "string", "pattern": DIGEST_PATTERN},
        "cusip": {"type": "string", "pattern": CUSIP_PATTERN},
        "cik": {"type": "string", "pattern": CIK_PATTERN},
        "period": {"type": "string", "pattern": PERIOD_PATTERN},
    }
    if base in simple:
        schema = simple[base]
    elif base == "members":
        schema = {
            "type": "object",
            "additionalProperties": {"type": "string", "pattern": HEX64_PATTERN},
        }
    elif base == "presence":
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                name: {"enum": list(ENUMS["field_presence_state"])} for name in NPORT_FLAG_FIELDS
            },
        }
    elif base == "rating_declarations":
        nullable_raw = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        nullable_date = {"anyOf": [{"type": "string", "pattern": DATE_PATTERN}, {"type": "null"}]}
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": list(RATING_DECLARATION_FIELDS),
            "properties": {
                "version": {"const": RATING_DECLARATIONS_VERSION},
                "rating_scopes": {
                    "type": "array",
                    "uniqueItems": True,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": list(RATING_SCOPE_FIELDS),
                        "properties": {
                            "agency_name": {"type": "string", "pattern": TEXT_PATTERN},
                            "rating_type": nullable_raw,
                            "scale": nullable_raw,
                        },
                    },
                },
                "uncleared_rating_sources": {
                    "type": "array",
                    "uniqueItems": True,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": list(UNCLEARED_RATING_SOURCE_FIELDS),
                        "properties": {
                            "source_ref": {"type": "string", "pattern": TEXT_PATTERN},
                            "rights_state": {"enum": [
                                state for state in ENUMS["rights_state"] if state != "approved"
                            ]},
                            "coverage_start": nullable_date,
                            "coverage_end": nullable_date,
                        },
                    },
                },
            },
        }
    elif base in STRUCTURED_KINDS:
        fields, enum_field, cap = STRUCTURED_KINDS[base]
        bounded = {"anyOf": [{"type": "string", "pattern": TEXT_PATTERN, "maxLength": MAX_RAW_TEXT},
                             {"type": "null"}]}
        properties = {name: dict(bounded) for name in fields}
        if enum_field is not None:
            properties[enum_field] = {"enum": list(ENUMS["adviser_role"])}
        schema = {
            "type": "array", "uniqueItems": True, "maxItems": cap,
            "items": {"type": "object", "additionalProperties": False, "required": list(fields),
                      "properties": properties},
        }
    elif base == "boundedtext":
        schema = {"type": "string", "pattern": TEXT_PATTERN, "maxLength": MAX_RAW_TEXT}
    elif base == "series_ids":
        schema = {"type": "array", "uniqueItems": True, "maxItems": MAX_SERIES_IDS,
                  "items": {"type": "string", "pattern": TEXT_PATTERN, "maxLength": MAX_RAW_TEXT}}
    elif base == "uuids":
        schema = {"type": "array", "uniqueItems": True, "items": simple["uuid"]}
    elif base == "texts":
        schema = {"type": "array", "uniqueItems": True, "items": simple["text"]}
    elif base.startswith("enums:"):
        schema = {"type": "array", "uniqueItems": True, "items": {"enum": list(ENUMS[base[6:]])}}
    elif base.startswith("enum:"):
        schema = {"enum": list(ENUMS[base[5:]])}
    else:  # pragma: no cover - guarded by tests
        raise ContractError(f"unknown_field_kind:{kind}")
    return {"anyOf": [schema, {"type": "null"}]} if optional else schema


def _object_schema(spec: tuple[tuple[str, str], ...], extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": [name for name, _ in spec],
        "properties": {name: _kind_schema(kind) for name, kind in spec},
    }
    if extra:
        schema.update(extra)
    return schema


def render_bundle_schema() -> dict[str, Any]:
    """Render the strict bundle JSON Schema (without its ``x-digest``)."""
    defs: dict[str, Any] = {}
    for cls in ROW_TYPES:
        defs[cls.FRAME] = _object_schema(
            cls.SPEC,
            {
                "x-row-key": list(cls.KEY),
                "x-columns": [{"name": n, "kind": k} for n, k in cls.SPEC],
            },
        )
    defs["manifest"] = _object_schema(
        MANIFEST_SPEC,
        {"x-fingerprint-fields": list(FINGERPRINT_FIELDS)},
    )
    defs["manifest"]["properties"]["product"] = {"const": PRODUCT}
    defs["manifest"]["properties"]["contract_version"] = {"const": CONTRACT_VERSION}
    frames = {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(FRAME_TYPES),
        "properties": {
            name: {"type": "array", "items": {"$ref": f"#/$defs/{name}"}} for name in sorted(FRAME_TYPES)
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://investintell.local/contracts/bonds/default_event_bundle_v2.schema.json",
        "title": "BondDefaultEventBundleV2",
        "$comment": (
            "Rendered from src/bonds/default_events/contracts.py; do not hand-edit. "
            "x-digest = sha256 of the canonical JSON of this document without x-digest. "
            "Frames are arrays sorted and unique by x-row-key; set-valued arrays are sorted. "
            "Cross-frame, temporal, identity and derivation rules are enforced by "
            "validate_bundle (Python) and bond_credit_validate (SQL); this schema is the "
            "structural layer."
        ),
        "x-contract-version": CONTRACT_VERSION,
        "x-product": PRODUCT,
        "x-policy-digest": POLICY_DIGEST,
        "x-uuid-namespace": str(NAMESPACE),
        "x-identity-name": "UUIDv5 name = canonical JSON array [kind, *components]",
        "x-publication-id": (
            "RFC 9562 UUIDv8 from fingerprint sha256 bits 0..127; version nibble 8, variant 10xx"
        ),
        "x-row-encoding": ROW_ENCODING,
        "x-sql-files": list(SQL_FILES),
        "x-input-frames": list(INPUT_FRAMES),
        "x-output-frames": list(OUTPUT_FRAMES),
        "x-frame-manifest-prefix": dict(FRAME_MANIFEST_PREFIX),
        "x-legacy-contract-versions": [LEGACY_V1_CONTRACT_VERSION],
        "x-family-rule-version": FAMILY_RULE_VERSION,
        "x-rating-resolver-id": RATING_RESOLVER_ID,
        "x-structured-sort": "structured records sort by declared fields in order, null before text",
        "type": "object",
        "additionalProperties": False,
        "required": ["contract_version", "manifest", "panel_grid", "frames"],
        "properties": {
            "contract_version": {"const": CONTRACT_VERSION},
            "manifest": {"$ref": "#/$defs/manifest"},
            "panel_grid": {
                "type": "array",
                "uniqueItems": True,
                "items": {
                    "type": "array",
                    "prefixItems": [
                        {"type": "string", "pattern": CUSIP_PATTERN},
                        {"type": "string", "pattern": MONTH_PATTERN},
                    ],
                    "items": False,
                    "minItems": 2,
                },
            },
            "frames": frames,
        },
        "$defs": defs,
    }
