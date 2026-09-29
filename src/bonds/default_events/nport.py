"""SEC Form N-PORT credit-state evidence: DERA packages, public XML, revisions, consensus.

Implementation contract §4.1 items 1-8. Public functions:

* :func:`parse_dera_package` - one quarterly DERA ZIP, streamed through DuckDB with
  disk spill; six pinned tables; debt->holding join with uniqueness/100 % match
  assertions; IDENTIFIERS aggregated before the join; CORP+DBT eligibility with
  exclusion counts; persists a small Parquet credit projection, never all holdings.
* :func:`iter_package_observations` - lazily build contract ``CreditObservation`` rows
  from a projection (one lexical row each; package copies stay distinct rows that
  share a ``semantic_key``).
* :func:`parse_public_accession` - one public ``primary_doc.xml`` (DTD/entities refused)
  into the same observation contract.
* :func:`resolve_accession_revisions` - filing families and complete-replacement choice.
* :func:`build_votes` / :func:`build_consensus_states` - vote unit and the frozen
  consensus/onset policy. This module never admits events: it emits typed state
  proposals and an adjudication queue for W2/human review.

No value interpretation erases the lexical source value: flags keep their raw text and a
``field_presence`` state (present/null/absent/invalid).
"""

from __future__ import annotations

import ctypes
import datetime as dt
import hashlib
import json
import re
import shutil
import sys
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any
from xml.etree import ElementTree

from . import publication
from .contracts import (
    FRAME_TYPES,
    POLICY_DIGEST,
    ContractError,
    CreditObservation,
    ProposalEvidence,
    SourcePackage,
    cusip_from_isin,
    derive_timing_class,
    is_valid_cusip9,
    is_valid_isin,
    sorted_uuids,
)
from .sec_acquisition import AcceptanceHeader, normalize_cik

PARSER_VERSION = "bond_default_nport_dera_v1"
DERA_SCHEMA_VERSION = "sec_nport_dera_quarterly_tsv_v1"
XML_PARSER_VERSION = "bond_default_nport_xml_v1"
XML_MAPPING_VERSION = "nport_primary_doc_xml_mapping_v1"
PROJECTION_SCHEMA_VERSION = "bond_default_nport_projection_v1"

PINNED_TABLES = (
    "SUBMISSION",
    "REGISTRANT",
    "FUND_REPORTED_INFO",
    "FUND_REPORTED_HOLDING",
    "IDENTIFIERS",
    "DEBT_SECURITY",
)
DEBT_REQUIRED_COLUMNS = (
    "HOLDING_ID",
    "MATURITY_DATE",
    "COUPON_TYPE",
    "ANNUALIZED_RATE",
    "IS_DEFAULT",
    "ARE_ANY_INTEREST_PAYMENT",
    "IS_ANY_PORTION_INTEREST_PAID",
    "IS_CONVTIBLE_MANDATORY",
    "IS_CONVTIBLE_CONTINGENT",
)
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "SUBMISSION": ("ACCESSION_NUMBER", "FILING_DATE", "SUB_TYPE", "REPORT_DATE"),
    "REGISTRANT": ("ACCESSION_NUMBER", "CIK"),
    "FUND_REPORTED_INFO": ("ACCESSION_NUMBER", "SERIES_ID"),
    "FUND_REPORTED_HOLDING": (
        "ACCESSION_NUMBER", "HOLDING_ID", "ISSUER_NAME", "ISSUER_LEI", "ISSUER_TITLE",
        "ISSUER_CUSIP", "BALANCE", "UNIT", "CURRENCY_CODE", "CURRENCY_VALUE", "ASSET_CAT",
        "ISSUER_TYPE",
    ),
    "IDENTIFIERS": ("HOLDING_ID", "IDENTIFIERS_ID", "IDENTIFIER_ISIN"),
    "DEBT_SECURITY": DEBT_REQUIRED_COLUMNS,
}
#: DERA column -> contract observation column (policy ``nport.flag_mapping``).
#: C.9.e is ``IS_ANY_PORTION_INTEREST_PAID`` (paid in kind), never ``IS_PAID_KIND``.
FLAG_COLUMNS: dict[str, str] = {
    "IS_DEFAULT": "nport_is_default",
    "ARE_ANY_INTEREST_PAYMENT": "nport_arrears_or_deferral",
    "IS_ANY_PORTION_INTEREST_PAID": "nport_paid_in_kind",
}
PUBLIC_SUB_TYPES = frozenset({"NPORT-P", "NPORT-P/A"})
ORIGINAL_SUB_TYPE = "NPORT-P"
AMENDMENT_SUB_TYPE = "NPORT-P/A"
ELIGIBLE_ISSUER_TYPE = "CORP"
ELIGIBLE_ASSET_CAT = "DBT"
DEBT_MEMBER = "DEBT_SECURITY.tsv"
XML_MEMBER = "primary_doc.xml"

_MONTHS = {
    name: index
    for index, name in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1
    )
}
_ACCESSION = re.compile(r"\d{10}-\d{2}-\d{6}")
_SERIES = re.compile(r"S\d{9}")
_CUSIP_PLACEHOLDER = re.compile(r"N/?A|NONE|0+|-+|9{9}")
_COLUMN_NAME = re.compile(r"[A-Z][A-Z0-9_]*")


class NportError(RuntimeError):
    """Fatal N-PORT processing error (never used for data-quality quarantine)."""


class PackageIntegrityError(NportError):
    """The package bytes do not match the pinned SHA-256."""


class ZipSafetyError(NportError):
    """ZIP layout violates the bounded-expansion/traversal policy."""


class XmlSafetyError(NportError):
    """XML declares a DTD/entities or exceeds the bounded size."""


# ---------------------------------------------------------------------------
# Bounded ZIP handling
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ZipLimits:
    """Expansion bounds; the largest DERA member seen (2022q3 holdings) is ~1.6 GB."""

    max_members: int = 64
    max_member_bytes: int = 8 * 1024**3
    max_total_bytes: int = 64 * 1024**3
    max_compression_ratio: float = 200.0
    max_header_bytes: int = 1024 * 1024


def sha256_file(path: Path, *, buffer_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    buffer = bytearray(buffer_size)
    view = memoryview(buffer)
    with open(path, "rb") as handle:
        while True:
            read = handle.readinto(buffer)
            if not read:
                break
            digest.update(view[:read])
    return digest.hexdigest()


def _unsafe_member_name(name: str) -> bool:
    if not name or name.startswith(("/", "\\")) or "\\" in name or "\x00" in name:
        return True
    if re.match(r"[A-Za-z]:", name):
        return True
    return any(part in ("", ".", "..") for part in name.rstrip("/").split("/"))


def inspect_zip(archive: zipfile.ZipFile, limits: ZipLimits) -> dict[str, zipfile.ZipInfo]:
    """Validate every member name and declared size; returns name -> info (files only)."""
    infos = archive.infolist()
    if len(infos) > limits.max_members:
        raise ZipSafetyError(f"zip_member_count:{len(infos)}")
    members: dict[str, zipfile.ZipInfo] = {}
    total = 0
    for info in infos:
        if _unsafe_member_name(info.filename):
            raise ZipSafetyError(f"zip_member_name_unsafe:{info.filename!r}")
        if info.flag_bits & 0x1:
            raise ZipSafetyError(f"zip_member_encrypted:{info.filename}")
        if info.is_dir():
            continue
        if info.filename in members:
            raise ZipSafetyError(f"zip_member_duplicate:{info.filename}")
        if info.file_size > limits.max_member_bytes:
            raise ZipSafetyError(f"zip_member_too_large:{info.filename}:{info.file_size}")
        if info.file_size > 0 and info.file_size / max(info.compress_size, 1) > limits.max_compression_ratio:
            raise ZipSafetyError(f"zip_member_ratio:{info.filename}")
        total += info.file_size
        members[info.filename] = info
    if total > limits.max_total_bytes:
        raise ZipSafetyError(f"zip_total_too_large:{total}")
    return members


def _extract_member(
    archive: zipfile.ZipFile, info: zipfile.ZipInfo, destination: Path, *, buffer_size: int = 1 << 20
) -> tuple[str, int]:
    """Stream one member to disk; enforce the declared size; return (sha256, bytes)."""
    digest = hashlib.sha256()
    written = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with archive.open(info) as source, open(destination, "wb") as target:
        while True:
            chunk = source.read(buffer_size)
            if not chunk:
                break
            written += len(chunk)
            if written > info.file_size:
                raise ZipSafetyError(f"zip_member_exceeds_declared_size:{info.filename}")
            digest.update(chunk)
            target.write(chunk)
    if written != info.file_size:
        raise ZipSafetyError(f"zip_member_size_mismatch:{info.filename}")
    return digest.hexdigest(), written


def verify_member_hashes(
    zip_path: Path, *, expected_sha256: str, expected_members: Mapping[str, str], limits: ZipLimits | None = None
) -> None:
    """Re-verify a pinned package: whole-file SHA-256, then each pinned member's SHA-256."""
    if sha256_file(Path(zip_path)) != expected_sha256:
        raise PackageIntegrityError("package_sha256_mismatch")
    with zipfile.ZipFile(zip_path) as archive:
        members = inspect_zip(archive, limits or ZipLimits())
        for name, digest in sorted(expected_members.items()):
            info = members.get(name)
            if info is None:
                raise PackageIntegrityError(f"member_missing:{name}")
            if _member_sha(archive, info) != digest:
                raise PackageIntegrityError(f"member_sha256_mismatch:{name}")


def _read_header_line(archive: zipfile.ZipFile, info: zipfile.ZipInfo, limits: ZipLimits) -> str:
    with archive.open(info) as source:
        line = source.readline(limits.max_header_bytes + 1)
    if len(line) > limits.max_header_bytes or not line.endswith(b"\n"):
        raise ZipSafetyError(f"tsv_header_unbounded:{info.filename}")
    return line.decode("utf-8").rstrip("\n").rstrip("\r")


# ---------------------------------------------------------------------------
# Lexical helpers
# ---------------------------------------------------------------------------
def parse_dera_date(raw: str | None) -> dt.date | None:
    """Strict ``DD-MON-YYYY`` (DERA) parser; anything else is ``None`` (quarantined)."""
    if raw is None:
        return None
    match = re.fullmatch(r"(\d{2})-([A-Z]{3})-(\d{4})", raw)
    if match is None or match.group(2) not in _MONTHS:
        return None
    try:
        return dt.date(int(match.group(3)), _MONTHS[match.group(2)], int(match.group(1)))
    except ValueError:
        return None


def flag_presence(raw: str | None, *, column_present: bool = True) -> tuple[str | None, str]:
    """Contract ``field_presence`` state and retained raw value of one Y/N flag."""
    if not column_present:
        return None, "absent"
    if raw is None:
        return None, "null"
    if raw in ("Y", "N"):
        return raw, "present"
    return raw, "invalid"


@dataclass(frozen=True)
class IdentityResolution:
    """CUSIP9 identity of one holding; ``cusip9`` is ``None`` unless unambiguous."""

    cusip9: str | None
    status: str
    isin_raw: str | None


def resolve_identity(cusip_raw: str | None, isin_raws: Sequence[str]) -> IdentityResolution:
    """Validated CUSIP9 without truncation; US/CA ISIN used only with instrument agreement.

    * a checksum-valid reported CUSIP wins when no ISIN contradicts it;
    * an ISIN-derived CUSIP is used only when the reported CUSIP is blank or an explicit
      placeholder (``N/A``, zeros, ...); a nonblank malformed CUSIP is never "repaired";
    * several distinct ISINs, or ISIN/CUSIP disagreement, leave the holding unlinked.
    """
    normalized = sorted({value.strip().upper() for value in isin_raws if value and value.strip()})
    isin_raw = None
    if len(normalized) == 1:
        isin_raw = min(v for v in isin_raws if v and v.strip().upper() == normalized[0])
    valid_isins = [value for value in normalized if is_valid_isin(value)]
    derived = sorted({c for c in (cusip_from_isin(value) for value in valid_isins) if c is not None})
    text = None if cusip_raw is None else cusip_raw.strip().upper()
    placeholder = text is None or text == "" or _CUSIP_PLACEHOLDER.fullmatch(text) is not None
    reported = None if placeholder else (text if is_valid_cusip9(text) else None)
    if len(normalized) > 1:
        return IdentityResolution(None, "conflict_multiple_isins", None)
    if reported is not None:
        if derived and derived != [reported]:
            return IdentityResolution(None, "conflict_cusip_isin", isin_raw)
        if normalized and not valid_isins:
            return IdentityResolution(reported, "valid_cusip_isin_invalid", isin_raw)
        return IdentityResolution(reported, "valid_cusip", isin_raw)
    if not placeholder:
        return IdentityResolution(None, "invalid_cusip", isin_raw)
    if len(derived) == 1:
        return IdentityResolution(derived[0], "isin_derived", isin_raw)
    if normalized and not valid_isins:
        return IdentityResolution(None, "invalid_isin", isin_raw)
    if normalized:
        return IdentityResolution(None, "non_us_ca_isin", isin_raw)
    return IdentityResolution(None, "missing", None)


def is_series_key(fund_key: str | None) -> bool:
    """True only for a valid EDGAR series ID (``S`` + 9 digits), never a ``cik:`` fallback."""
    return fund_key is not None and _SERIES.fullmatch(fund_key) is not None


def fund_key_for(series_id: str | None, registrant_cik: str | None) -> str | None:
    """Fund (vote grouping) identity: EDGAR series ID, else ``cik:<registrant>``.

    The ``cik:`` fallback groups lots of a filing without a (valid) series ID; it never
    counts as a distinct series for consensus (see :func:`is_series_key`).
    """
    if series_id is not None:
        return series_id
    if registrant_cik is not None:
        return f"cik:{registrant_cik}"
    return None


def process_peak_memory_bytes() -> int | None:
    """Peak resident set (Windows peak working set / POSIX ru_maxrss) of this process."""
    if sys.platform == "win32":
        class _Counters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = _Counters()
        counters.cb = ctypes.sizeof(_Counters)
        try:
            kernel32 = ctypes.WinDLL("kernel32")
            psapi = ctypes.WinDLL("psapi")
        except OSError:  # pragma: no cover
            return None
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(_Counters), ctypes.c_ulong]
        psapi.GetProcessMemoryInfo.restype = ctypes.c_int
        if not psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            return None
        return int(counters.PeakWorkingSetSize)
    try:
        import resource
    except ImportError:  # pragma: no cover
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def _ts(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_ts(text: str) -> dt.datetime:
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------------
# DERA package parsing
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DeraPackageResult:
    """Outcome of one DERA package; ``quarantined`` packages carry no projection."""

    package_label: str
    status: str
    quarantine_reasons: tuple[str, ...]
    zip_sha256: str
    source_package: SourcePackage | None
    projection_path: Path | None
    accessions_path: Path | None
    stats: Mapping[str, Any]

    def to_record(self) -> dict[str, Any]:
        return {
            "accessions_path": None if self.accessions_path is None else self.accessions_path.name,
            "package_label": self.package_label,
            "projection_path": None if self.projection_path is None else self.projection_path.name,
            "quarantine_reasons": list(self.quarantine_reasons),
            "source_package": None if self.source_package is None else self.source_package.to_record(),
            "stats": dict(self.stats),
            "status": self.status,
            "zip_sha256": self.zip_sha256,
        }

    @classmethod
    def load(cls, directory: Path) -> DeraPackageResult:
        """Reload a persisted result. The package is rebuilt with the *current* W0
        constructor (its ID is derived from family/external_id/content_sha256), so a
        projection stays usable if the contract's ID encoding changes."""
        record = json.loads((Path(directory) / "package_result.json").read_bytes())
        package = record["source_package"]
        if package is not None:
            package = SourcePackage.create(**{k: v for k, v in package.items() if k != "package_id"})
            if package.content_sha256 != record["zip_sha256"]:
                raise NportError("package_result_inconsistent")
        return cls(
            package_label=record["package_label"],
            status=record["status"],
            quarantine_reasons=tuple(record["quarantine_reasons"]),
            zip_sha256=record["zip_sha256"],
            source_package=package,
            projection_path=None if record["projection_path"] is None else Path(directory) / record["projection_path"],
            accessions_path=None if record["accessions_path"] is None else Path(directory) / record["accessions_path"],
            stats=record["stats"],
        )


#: Timing/memory keys excluded from deterministic comparisons of ``stats``.
NONDETERMINISTIC_STATS = frozenset({"timings_seconds", "peak_memory_bytes"})

#: The projection deliberately stores no contract-derived IDs (package/observation IDs are
#: derived by the W0 contract at observation time), so it stays valid across ID encodings.
_PROJECTION_FIELDS: tuple[tuple[str, str], ...] = (
    ("accession_number", "string"), ("holding_id", "string"),
    ("member_name", "string"), ("row_locator", "string"), ("semantic_key", "string"),
    ("content_sha256", "string"), ("semantic_sha256", "string"), ("lot_ordinal", "int32"), ("sub_type", "string"),
    ("report_date", "date32"), ("filing_date", "date32"), ("registrant_cik", "string"),
    ("series_id", "string"), ("fund_key", "string"), ("issuer_name", "string"),
    ("issuer_lei", "string"), ("issuer_title", "string"), ("cusip_raw", "string"),
    ("isin_raw", "string"), ("isins_json", "string"), ("identifier_rows", "int32"),
    ("cusip9", "string"), ("identity_status", "string"), ("issuer_type_raw", "string"),
    ("asset_category_raw", "string"), ("balance_raw", "string"), ("unit_raw", "string"),
    ("currency_code_raw", "string"), ("currency_value_raw", "string"),
    ("maturity_date_raw", "string"), ("coupon_type_raw", "string"),
    ("annualized_rate_raw", "string"), ("is_default_raw", "string"),
    ("is_default_presence", "string"), ("arrears_raw", "string"), ("arrears_presence", "string"),
    ("pik_raw", "string"), ("pik_presence", "string"),
)
_CONTENT_FIELDS = (
    "issuer_name", "issuer_lei", "issuer_title", "cusip_raw", "isins", "balance_raw", "unit_raw",
    "currency_code_raw", "currency_value_raw", "issuer_type_raw", "asset_category_raw",
    "maturity_date_raw", "coupon_type_raw", "annualized_rate_raw", "is_default_raw",
    "arrears_raw", "pik_raw",
)


def _projection_schema() -> Any:
    import pyarrow as pa

    kinds = {"string": pa.string(), "int32": pa.int32(), "date32": pa.date32()}
    return pa.schema([(name, kinds[kind]) for name, kind in _PROJECTION_FIELDS])


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _count_lines(path: Path) -> int:
    count = 0
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            count += block.count(b"\n")
    return count


def dera_official_url(package_label: str) -> str:
    return f"https://www.sec.gov/files/dera/data/form-n-port-data-sets/{package_label}"


def parse_dera_package(
    zip_path: Path,
    *,
    expected_sha256: str,
    package_label: str,
    output_dir: Path,
    work_dir: Path,
    retrieved_at: dt.datetime,
    first_verified_public_at: dt.datetime | None = None,
    official_url: str | None = None,
    limits: ZipLimits | None = None,
    duckdb_memory_limit: str = "3GB",
    duckdb_threads: int = 4,
    batch_rows: int = 50_000,
    keep_work: bool = False,
) -> DeraPackageResult:
    """Parse one DERA quarterly N-PORT package into a CORP+DBT credit projection.

    The ZIP is hashed before it is opened; a mismatch raises. Layout problems that
    make the package unusable (missing pinned member/required column, TSV parse error,
    join/cardinality violation) *quarantine* it: the result carries reasons and
    diagnostics but no projection. Only one package is processed per call; members are
    streamed to ``work_dir`` and joined by DuckDB with a disk spill directory.
    """
    limits = limits or ZipLimits()
    started = time.perf_counter()
    timings: dict[str, float] = {}
    output_dir = Path(output_dir)
    work = Path(work_dir) / f"dera_{package_label}_{uuid.uuid4().hex[:8]}"
    stats: dict[str, Any] = {"package_label": package_label}
    reasons: list[str] = []

    zip_sha = sha256_file(Path(zip_path))
    timings["sha256"] = time.perf_counter() - started
    if zip_sha != expected_sha256:
        raise PackageIntegrityError(f"package_sha256_mismatch:{package_label}:{zip_sha}")
    retrieved = retrieved_at.astimezone(dt.timezone.utc)
    first_public = (first_verified_public_at or retrieved_at).astimezone(dt.timezone.utc)

    member_shas: dict[str, str] = {}
    headers: dict[str, list[str]] = {}
    extracted: dict[str, Path] = {}
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = inspect_zip(archive, limits)
            stats["members"] = sorted(members)
            stats["optional_members_present"] = {
                "FUND_VAR_INFO.tsv": "FUND_VAR_INFO.tsv" in members,
            }
            for table in PINNED_TABLES:
                name = f"{table}.tsv"
                info = members.get(name)
                if info is None:
                    reasons.append(f"member_missing:{name}")
                    continue
                columns = _read_header_line(archive, info, limits).split("\t")
                headers[table] = columns
                if len(set(columns)) != len(columns) or not all(_COLUMN_NAME.fullmatch(c) for c in columns):
                    reasons.append(f"header_invalid:{name}")
                missing = [c for c in REQUIRED_COLUMNS[table] if c not in columns]
                if missing:
                    reasons.append(f"required_columns_missing:{name}:{','.join(missing)}")
            stats["headers"] = {table: "\t".join(cols) for table, cols in sorted(headers.items())}
            stats["extra_columns"] = {
                table: [c for c in cols if c not in REQUIRED_COLUMNS[table]]
                for table, cols in sorted(headers.items())
            }
            if not reasons:
                phase = time.perf_counter()
                for table in PINNED_TABLES:
                    name = f"{table}.tsv"
                    destination = work / name
                    member_shas[name], _ = _extract_member(archive, members[name], destination)
                    extracted[table] = destination
                timings["extract"] = time.perf_counter() - phase
            else:
                for table in PINNED_TABLES:
                    name = f"{table}.tsv"
                    if name in members:
                        member_shas[name] = _member_sha(archive, members[name])
    except zipfile.BadZipFile as exc:
        raise ZipSafetyError(f"zip_invalid:{package_label}") from exc

    header_sha = hashlib.sha256(
        json.dumps(stats["headers"], sort_keys=True).encode("utf-8")
    ).hexdigest()
    try:
        if reasons:
            source_package = _dera_source_package(
                package_label, zip_sha, header_sha, member_shas, retrieved, first_public,
                official_url, filing_range=None, report_range=None,
            )
            return _finish(
                output_dir, package_label, "quarantined", reasons, zip_sha, source_package,
                None, None, stats, timings, started,
            )
        return _parse_extracted(
            extracted,
            headers=headers,
            package_label=package_label,
            zip_sha=zip_sha,
            header_sha=header_sha,
            member_shas=member_shas,
            retrieved=retrieved,
            first_public=first_public,
            official_url=official_url,
            output_dir=output_dir,
            work=work,
            stats=stats,
            timings=timings,
            started=started,
            duckdb_memory_limit=duckdb_memory_limit,
            duckdb_threads=duckdb_threads,
            batch_rows=batch_rows,
        )
    finally:
        if not keep_work:
            shutil.rmtree(work, ignore_errors=True)


def _member_sha(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> str:
    digest = hashlib.sha256()
    with archive.open(info) as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dera_source_package(
    package_label: str,
    zip_sha: str,
    header_sha: str,
    member_shas: Mapping[str, str],
    retrieved: dt.datetime,
    first_public: dt.datetime,
    official_url: str | None,
    *,
    filing_range: tuple[dt.date, dt.date] | None,
    report_range: tuple[dt.date, dt.date] | None,
) -> SourcePackage:
    return SourcePackage.create(
        source_family="sec_nport_dera",
        external_id=package_label,
        content_sha256=zip_sha,
        raw_sha256=zip_sha,
        header_sha256=header_sha,
        member_sha256s=dict(member_shas),
        official_url=official_url or dera_official_url(package_label),
        accession_number=None,
        rights_state="public_government_record",
        rights_ref=None,
        parser_version=PARSER_VERSION,
        schema_version=DERA_SCHEMA_VERSION,
        retrieved_at=retrieved,
        first_verified_public_at=first_public,
        public_time_basis="first_verified_retrieval",
        public_time_evidence=(
            f"DERA package {package_label} sha256:{zip_sha} verified at retrieval; package "
            "Last-Modified/regeneration dates are not historical publication evidence"
        ),
        source_coverage_start=None if filing_range is None else filing_range[0],
        source_coverage_end=None if filing_range is None else filing_range[1],
        public_coverage_start=None,
        public_coverage_end=None,
        effective_coverage_start=None if report_range is None else report_range[0],
        effective_coverage_end=None if report_range is None else report_range[1],
        raw_locator=f"sec_nport_dera/{package_label}",
        revision_of_package_id=None,
        sec_run_id=None,
        sec_package_id=None,
    )


def _finish(
    output_dir: Path,
    package_label: str,
    status: str,
    reasons: Sequence[str],
    zip_sha: str,
    source_package: SourcePackage | None,
    projection: Path | None,
    accessions: Path | None,
    stats: dict[str, Any],
    timings: dict[str, float],
    started: float,
) -> DeraPackageResult:
    timings["total"] = time.perf_counter() - started
    stats["timings_seconds"] = {k: round(v, 3) for k, v in sorted(timings.items())}
    stats["peak_memory_bytes"] = process_peak_memory_bytes()
    result = DeraPackageResult(
        package_label=package_label,
        status=status,
        quarantine_reasons=tuple(reasons),
        zip_sha256=zip_sha,
        source_package=source_package,
        projection_path=projection,
        accessions_path=accessions,
        stats=stats,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "package_result.json").write_text(
        json.dumps(result.to_record(), sort_keys=True, indent=1, default=str), encoding="utf-8"
    )
    return result


def _scalar(con: Any, sql: str) -> Any:
    row = con.execute(sql).fetchone()
    return None if row is None else row[0]


def _parse_extracted(
    extracted: Mapping[str, Path],
    *,
    headers: Mapping[str, list[str]],
    package_label: str,
    zip_sha: str,
    header_sha: str,
    member_shas: Mapping[str, str],
    retrieved: dt.datetime,
    first_public: dt.datetime,
    official_url: str | None,
    output_dir: Path,
    work: Path,
    stats: dict[str, Any],
    timings: dict[str, float],
    started: float,
    duckdb_memory_limit: str,
    duckdb_threads: int,
    batch_rows: int,
) -> DeraPackageResult:
    import duckdb

    reasons: list[str] = []
    spill = work / "spill"
    spill.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        str(work / "nport.duckdb"),
        config={
            "memory_limit": duckdb_memory_limit,
            "threads": duckdb_threads,
            "temp_directory": str(spill),
            "preserve_insertion_order": False,
        },
    )
    try:
        phase = time.perf_counter()
        physical: dict[str, int] = {}
        for table in PINNED_TABLES:
            columns = headers[table]
            struct = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in columns) + "}"
            path = _sql_literal(extracted[table].as_posix())
            try:
                con.execute(
                    f"CREATE TABLE t_{table} AS SELECT * FROM read_csv({path}, delim='\t', "
                    f"header=true, quote='\"', escape='\"', columns={struct}, auto_detect=false, "
                    "strict_mode=true, null_padding=false, encoding='utf-8')"
                )
            except duckdb.Error as exc:
                reasons.append(f"tsv_parse_error:{table}.tsv:{type(exc).__name__}")
                continue
            physical[table] = _count_lines(extracted[table]) - 1
        timings["load"] = time.perf_counter() - phase
        if reasons:
            source_package = _dera_source_package(
                package_label, zip_sha, header_sha, member_shas, retrieved, first_public,
                official_url, filing_range=None, report_range=None,
            )
            return _finish(output_dir, package_label, "quarantined", reasons, zip_sha,
                           source_package, None, None, stats, timings, started)

        rows = {t: int(_scalar(con, f"SELECT count(*) FROM t_{t}")) for t in PINNED_TABLES}
        stats["rows_per_table"] = rows
        stats["physical_data_lines"] = physical
        stats["quoted_multiline_rows"] = {t: physical[t] - rows[t] for t in PINNED_TABLES if physical[t] != rows[t]}

        phase = time.perf_counter()
        cardinality = _cardinality_checks(con)
        stats["cardinality"] = cardinality
        for key in (
            "submission_duplicate_accessions", "registrant_duplicate_accessions",
            "fund_info_duplicate_accessions", "holding_null_ids", "holding_duplicate_ids",
            "debt_null_ids", "debt_duplicate_ids", "debt_orphans", "debt_rows_matching_duplicate_holdings",
            "holding_accessions_without_submission", "holding_accessions_without_registrant",
            "holding_accessions_without_fund_info", "holding_null_accessions",
        ):
            if cardinality[key]:
                reasons.append(f"cardinality:{key}={cardinality[key]}")
        timings["cardinality"] = time.perf_counter() - phase
        if reasons:
            source_package = _dera_source_package(
                package_label, zip_sha, header_sha, member_shas, retrieved, first_public,
                official_url, filing_range=None, report_range=None,
            )
            return _finish(output_dir, package_label, "quarantined", reasons, zip_sha,
                           source_package, None, None, stats, timings, started)

        phase = time.perf_counter()
        accessions = _accession_metadata(con, headers)
        stats["debt_by_category"] = _debt_category_counts(con)
        stats["identifiers"] = _identifier_aggregate(con)
        timings["aggregate"] = time.perf_counter() - phase

        phase = time.perf_counter()
        output_dir.mkdir(parents=True, exist_ok=True)
        projection_path = output_dir / "nport_credit_projection.parquet"
        projection_stats, digests = _write_projection(con, accessions, projection_path, batch_rows)
        stats.update(projection_stats)
        timings["projection"] = time.perf_counter() - phase

        accessions_path = output_dir / "accessions.jsonl"
        with open(accessions_path, "w", encoding="utf-8", newline="\n") as handle:
            for accession in sorted(accessions):
                record = dict(accessions[accession])
                record["eligible_content_digest"] = digests.get(accession, _EMPTY_DIGEST)
                handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        filing_dates = [a["filing_date"] for a in accessions.values() if a["filing_date"]]
        report_dates = [a["report_date"] for a in accessions.values() if a["report_date"]]
        source_package = _dera_source_package(
            package_label, zip_sha, header_sha, member_shas, retrieved, first_public, official_url,
            filing_range=(dt.date.fromisoformat(min(filing_dates)), dt.date.fromisoformat(max(filing_dates)))
            if filing_dates else None,
            report_range=(dt.date.fromisoformat(min(report_dates)), dt.date.fromisoformat(max(report_dates)))
            if report_dates else None,
        )
        return _finish(output_dir, package_label, "parsed", (), zip_sha, source_package,
                       projection_path, accessions_path, stats, timings, started)
    finally:
        con.close()


_EMPTY_DIGEST = hashlib.sha256(b"").hexdigest()


def _cardinality_checks(con: Any) -> dict[str, int]:
    """Join/cardinality diagnostics; every nonzero key quarantines the package."""
    def one(sql: str) -> int:
        return int(_scalar(con, sql) or 0)

    return {
        "submission_duplicate_accessions": one(
            "SELECT count(*) - count(DISTINCT ACCESSION_NUMBER) FROM t_SUBMISSION"
        ),
        "registrant_duplicate_accessions": one(
            "SELECT count(*) - count(DISTINCT ACCESSION_NUMBER) FROM t_REGISTRANT"
        ),
        "fund_info_duplicate_accessions": one(
            "SELECT count(*) - count(DISTINCT ACCESSION_NUMBER) FROM t_FUND_REPORTED_INFO"
        ),
        "holding_null_ids": one("SELECT count(*) FROM t_FUND_REPORTED_HOLDING WHERE HOLDING_ID IS NULL"),
        "holding_null_accessions": one(
            "SELECT count(*) FROM t_FUND_REPORTED_HOLDING WHERE ACCESSION_NUMBER IS NULL"
        ),
        "holding_duplicate_ids": one(
            "SELECT count(HOLDING_ID) - count(DISTINCT HOLDING_ID) FROM t_FUND_REPORTED_HOLDING"
        ),
        "debt_null_ids": one("SELECT count(*) FROM t_DEBT_SECURITY WHERE HOLDING_ID IS NULL"),
        "debt_duplicate_ids": one(
            "SELECT count(HOLDING_ID) - count(DISTINCT HOLDING_ID) FROM t_DEBT_SECURITY"
        ),
        "debt_orphans": one(
            "SELECT count(*) FROM t_DEBT_SECURITY d WHERE d.HOLDING_ID IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM t_FUND_REPORTED_HOLDING h WHERE h.HOLDING_ID = d.HOLDING_ID)"
        ),
        "debt_rows_matching_duplicate_holdings": one(
            "SELECT count(*) FROM t_DEBT_SECURITY d JOIN (SELECT HOLDING_ID FROM t_FUND_REPORTED_HOLDING "
            "GROUP BY HOLDING_ID HAVING count(*) > 1) h ON h.HOLDING_ID = d.HOLDING_ID"
        ),
        "debt_matched_rows": one(
            "SELECT count(*) FROM t_DEBT_SECURITY d JOIN t_FUND_REPORTED_HOLDING h ON h.HOLDING_ID = d.HOLDING_ID"
        ),
        "holding_accessions": one(
            "SELECT count(DISTINCT ACCESSION_NUMBER) FROM t_FUND_REPORTED_HOLDING"
        ),
        "holding_accessions_without_submission": one(
            "SELECT count(DISTINCT h.ACCESSION_NUMBER) FROM t_FUND_REPORTED_HOLDING h WHERE NOT EXISTS "
            "(SELECT 1 FROM t_SUBMISSION s WHERE s.ACCESSION_NUMBER = h.ACCESSION_NUMBER)"
        ),
        "holding_accessions_without_registrant": one(
            "SELECT count(DISTINCT h.ACCESSION_NUMBER) FROM t_FUND_REPORTED_HOLDING h WHERE NOT EXISTS "
            "(SELECT 1 FROM t_REGISTRANT r WHERE r.ACCESSION_NUMBER = h.ACCESSION_NUMBER)"
        ),
        "holding_accessions_without_fund_info": one(
            "SELECT count(DISTINCT h.ACCESSION_NUMBER) FROM t_FUND_REPORTED_HOLDING h WHERE NOT EXISTS "
            "(SELECT 1 FROM t_FUND_REPORTED_INFO f WHERE f.ACCESSION_NUMBER = h.ACCESSION_NUMBER)"
        ),
        "submission_accessions_without_holdings": one(
            "SELECT count(*) FROM t_SUBMISSION s WHERE NOT EXISTS "
            "(SELECT 1 FROM t_FUND_REPORTED_HOLDING h WHERE h.ACCESSION_NUMBER = s.ACCESSION_NUMBER)"
        ),
    }


def _accession_metadata(con: Any, headers: Mapping[str, list[str]]) -> dict[str, dict[str, Any]]:
    """Per-accession filing identity (submission/registrant/series) plus holding counts."""
    rows = con.execute(
        """
        WITH counts AS (
            SELECT h.ACCESSION_NUMBER,
                   count(*) AS n_holdings,
                   count(d.HOLDING_ID) AS n_debt,
                   count(d.HOLDING_ID) FILTER (
                       WHERE h.ISSUER_TYPE = 'CORP' AND h.ASSET_CAT = 'DBT') AS n_eligible
            FROM t_FUND_REPORTED_HOLDING h
            LEFT JOIN t_DEBT_SECURITY d ON d.HOLDING_ID = h.HOLDING_ID
            GROUP BY h.ACCESSION_NUMBER
        )
        SELECT s.ACCESSION_NUMBER, s.SUB_TYPE, s.FILING_DATE, s.REPORT_DATE, r.CIK, f.SERIES_ID,
               coalesce(c.n_holdings, 0), coalesce(c.n_debt, 0), coalesce(c.n_eligible, 0)
        FROM t_SUBMISSION s
        LEFT JOIN t_REGISTRANT r ON r.ACCESSION_NUMBER = s.ACCESSION_NUMBER
        LEFT JOIN t_FUND_REPORTED_INFO f ON f.ACCESSION_NUMBER = s.ACCESSION_NUMBER
        LEFT JOIN counts c ON c.ACCESSION_NUMBER = s.ACCESSION_NUMBER
        ORDER BY s.ACCESSION_NUMBER
        """
    ).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for accession, sub_type, filing_raw, report_raw, cik_raw, series_raw, n_hold, n_debt, n_elig in rows:
        registrant = normalize_cik(cik_raw)
        series = series_raw if series_raw is not None and _SERIES.fullmatch(series_raw) else None
        filing = parse_dera_date(filing_raw)
        report = parse_dera_date(report_raw)
        result[accession] = {
            "accession_number": accession,
            "accession_valid": bool(accession and _ACCESSION.fullmatch(accession)),
            "sub_type": sub_type,
            "filing_date_raw": filing_raw,
            "filing_date": None if filing is None else filing.isoformat(),
            "report_date_raw": report_raw,
            "report_date": None if report is None else report.isoformat(),
            "registrant_cik_raw": cik_raw,
            "registrant_cik": registrant,
            "series_id_raw": series_raw,
            "series_id": series,
            "fund_key": fund_key_for(series, registrant),
            "n_holdings": int(n_hold),
            "n_debt": int(n_debt),
            "n_eligible": int(n_elig),
        }
    return result


def _debt_category_counts(con: Any) -> dict[str, Any]:
    """Debt rows by (ISSUER_TYPE, ASSET_CAT) and the IS_DEFAULT lexical distribution."""
    rows = con.execute(
        """
        SELECT coalesce(h.ISSUER_TYPE, '<null>'), coalesce(h.ASSET_CAT, '<null>'), count(*),
               count(*) FILTER (WHERE d.IS_DEFAULT = 'Y')
        FROM t_DEBT_SECURITY d JOIN t_FUND_REPORTED_HOLDING h ON h.HOLDING_ID = d.HOLDING_ID
        GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2
        """
    ).fetchall()
    eligible = [r for r in rows if r[0] == ELIGIBLE_ISSUER_TYPE and r[1] == ELIGIBLE_ASSET_CAT]
    return {
        "debt_rows_total": sum(r[2] for r in rows),
        "eligible_corp_dbt_rows": sum(r[2] for r in eligible),
        "excluded_rows": sum(r[2] for r in rows) - sum(r[2] for r in eligible),
        "excluded_is_default_y_rows": sum(r[3] for r in rows) - sum(r[3] for r in eligible),
        "by_issuer_type_asset_cat": [
            {"issuer_type": r[0], "asset_cat": r[1], "rows": r[2], "is_default_y": r[3]} for r in rows
        ],
    }


def _identifier_aggregate(con: Any) -> dict[str, int]:
    """Aggregate IDENTIFIERS per debt holding *before* joining (no fan-out)."""
    con.execute(
        """
        CREATE TABLE idagg AS
        SELECT i.HOLDING_ID,
               count(*) AS identifier_rows,
               list_sort(list_distinct(list(i.IDENTIFIER_ISIN))) AS isins,
               count(i.IDENTIFIER_ISIN) - count(DISTINCT i.IDENTIFIER_ISIN) AS duplicate_isin_rows
        FROM t_IDENTIFIERS i
        WHERE i.HOLDING_ID IN (SELECT HOLDING_ID FROM t_DEBT_SECURITY)
        GROUP BY i.HOLDING_ID
        """
    )
    row = con.execute(
        """
        SELECT count(*), coalesce(sum(identifier_rows), 0),
               count(*) FILTER (WHERE identifier_rows > 1),
               count(*) FILTER (WHERE len(isins) > 1),
               coalesce(sum(duplicate_isin_rows), 0)
        FROM idagg
        """
    ).fetchone()
    return {
        "debt_holdings_with_identifiers": int(row[0]),
        "identifier_rows_for_debt_holdings": int(row[1]),
        "debt_holdings_with_multiple_identifier_rows": int(row[2]),
        "debt_holdings_with_multiple_distinct_isins": int(row[3]),
        "duplicate_isin_rows": int(row[4]),
    }


def _content_sha(values: Mapping[str, Any]) -> str:
    """Raw lexical provenance hash (exact source strings; differs between DERA and XML)."""
    payload = [values[name] for name in _CONTENT_FIELDS]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


#: Versioned canonical semantic identity of one holding lot (lot matching, revision
#: comparison). Raw lexical values are preserved separately (``raw_content_sha256``).
SEMANTIC_VERSION = "nport_semantic_v1"
_SEMANTIC_TEXT = ("issuer_name", "issuer_lei", "issuer_title", "unit_raw", "currency_code_raw", "issuer_type_raw",
                  "asset_category_raw", "coupon_type_raw")
_SEMANTIC_DECIMAL = ("balance_raw", "currency_value_raw", "annualized_rate_raw")
_SEMANTIC_FLAGS = ("is_default_raw", "arrears_raw", "pik_raw")
SEMANTIC_FIELDS = (*_SEMANTIC_TEXT, "cusip_raw", "isins", *_SEMANTIC_DECIMAL, "maturity_date_raw", *_SEMANTIC_FLAGS)


def canonical_decimal(raw: str | None) -> str | None:
    """Canonical decimal text (``5.000`` -> ``5``, ``-0`` -> ``0``); unparseable -> ``raw:<text>``."""
    if raw is None:
        return None
    text = raw.strip()
    if text == "":
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        return f"raw:{text}"
    if not value.is_finite():
        return f"raw:{text}"
    if value == 0:
        return "0"
    normalized = format(value.normalize(), "f")
    return normalized


def canonical_date(raw: str | None) -> str | None:
    """ISO date from DERA ``DD-MON-YYYY`` or XML ``YYYY-MM-DD``; otherwise ``raw:<text>``."""
    if raw is None:
        return None
    text = raw.strip()
    if text == "":
        return None
    parsed = parse_dera_date(text)
    if parsed is None and re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            parsed = dt.date.fromisoformat(text)
        except ValueError:
            parsed = None
    return f"raw:{text}" if parsed is None else parsed.isoformat()


def _canonical_text(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = raw.strip()
    return text or None


def semantic_sha(values: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical semantic lot identity (``SEMANTIC_VERSION``)."""
    canonical: dict[str, Any] = {"version": SEMANTIC_VERSION}
    for name in _SEMANTIC_TEXT:
        canonical[name] = _canonical_text(values.get(name))
    cusip = _canonical_text(values.get("cusip_raw"))
    canonical["cusip_raw"] = None if cusip is None else cusip.upper()
    canonical["isins"] = sorted({i.strip().upper() for i in (values.get("isins") or ()) if i and i.strip()})
    for name in _SEMANTIC_DECIMAL:
        canonical[name] = canonical_decimal(values.get(name))
    canonical["maturity_date_raw"] = canonical_date(values.get("maturity_date_raw"))
    for name in _SEMANTIC_FLAGS:  # value only: DERA cannot express an absent element
        canonical[name] = _canonical_text(values.get(name))
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def semantic_key_for(accession: str, semantic: str, ordinal: int) -> str:
    return f"sec_nport:{accession}:{semantic}:{ordinal}"


def accession_semantic_digest(semantic_shas: Iterable[str]) -> str:
    """Order-independent digest of an accession's eligible lots (multiset of semantic shas)."""
    return hashlib.sha256(",".join(sorted(semantic_shas)).encode("ascii")).hexdigest()


def _write_projection(
    con: Any,
    accessions: Mapping[str, Mapping[str, Any]],
    projection_path: Path,
    batch_rows: int,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Stream eligible debt rows (Arrow batches) into the Parquet credit projection."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    reader = con.execute(
        """
        SELECT h.ACCESSION_NUMBER AS accession, d.HOLDING_ID AS holding_id,
               h.ISSUER_NAME, h.ISSUER_LEI, h.ISSUER_TITLE, h.ISSUER_CUSIP,
               h.BALANCE, h.UNIT, h.CURRENCY_CODE, h.CURRENCY_VALUE, h.ISSUER_TYPE, h.ASSET_CAT,
               d.MATURITY_DATE, d.COUPON_TYPE, d.ANNUALIZED_RATE, d.IS_DEFAULT,
               d.ARE_ANY_INTEREST_PAYMENT, d.IS_ANY_PORTION_INTEREST_PAID,
               coalesce(ia.isins, []) AS isins, coalesce(ia.identifier_rows, 0) AS identifier_rows
        FROM t_DEBT_SECURITY d
        JOIN t_FUND_REPORTED_HOLDING h ON h.HOLDING_ID = d.HOLDING_ID
        LEFT JOIN idagg ia ON ia.HOLDING_ID = d.HOLDING_ID
        WHERE h.ISSUER_TYPE = 'CORP' AND h.ASSET_CAT = 'DBT'
        ORDER BY h.ACCESSION_NUMBER, length(d.HOLDING_ID), d.HOLDING_ID
        """
    ).to_arrow_reader(batch_rows)
    schema = _projection_schema()
    flags: dict[str, Counter[str]] = {column: Counter() for column in FLAG_COLUMNS}
    identity: Counter[str] = Counter()
    excluded: Counter[str] = Counter()
    by_sub_type: Counter[str] = Counter()
    y_cusips: set[str] = set()
    y_unlinked = 0
    digests: dict[str, str] = {}
    current: str | None = None
    current_hashes: list[str] = []
    ordinals: Counter[str] = Counter()
    identity_cache: dict[tuple[str | None, tuple[str, ...]], IdentityResolution] = {}
    written = 0

    def close_accession() -> None:
        if current is not None:
            digests[current] = accession_semantic_digest(current_hashes)

    with pq.ParquetWriter(projection_path, schema, compression="zstd") as writer:
        for batch in reader:
            out: dict[str, list[Any]] = {name: [] for name, _ in _PROJECTION_FIELDS}
            for row in batch.to_pylist():
                accession = row["accession"]
                meta = accessions.get(accession)
                if accession != current:
                    close_accession()
                    current, current_hashes, ordinals = accession, [], Counter()
                if meta is None or not meta["accession_valid"]:
                    excluded["accession_invalid"] += 1
                    continue
                if meta["sub_type"] not in PUBLIC_SUB_TYPES:
                    excluded["sub_type_not_public_nport"] += 1
                    continue
                if meta["report_date"] is None:
                    excluded["report_date_invalid"] += 1
                    continue
                isins = tuple(row["isins"])
                key = (row["ISSUER_CUSIP"], isins)
                resolved = identity_cache.get(key)
                if resolved is None:
                    if len(identity_cache) > 2_000_000:
                        identity_cache.clear()
                    resolved = identity_cache[key] = resolve_identity(row["ISSUER_CUSIP"], isins)
                values = {
                    "issuer_name": row["ISSUER_NAME"], "issuer_lei": row["ISSUER_LEI"],
                    "issuer_title": row["ISSUER_TITLE"], "cusip_raw": row["ISSUER_CUSIP"],
                    "isins": list(isins), "balance_raw": row["BALANCE"], "unit_raw": row["UNIT"],
                    "currency_code_raw": row["CURRENCY_CODE"], "currency_value_raw": row["CURRENCY_VALUE"],
                    "issuer_type_raw": row["ISSUER_TYPE"], "asset_category_raw": row["ASSET_CAT"],
                    "maturity_date_raw": row["MATURITY_DATE"], "coupon_type_raw": row["COUPON_TYPE"],
                    "annualized_rate_raw": row["ANNUALIZED_RATE"], "is_default_raw": row["IS_DEFAULT"],
                    "arrears_raw": row["ARE_ANY_INTEREST_PAYMENT"],
                    "pik_raw": row["IS_ANY_PORTION_INTEREST_PAID"],
                }
                content = _content_sha(values)
                semantic = semantic_sha(values)
                current_hashes.append(semantic)
                ordinal = ordinals[semantic]
                ordinals[semantic] += 1
                presences = {}
                for column, raw_key in (
                    ("IS_DEFAULT", "is_default"), ("ARE_ANY_INTEREST_PAYMENT", "arrears"),
                    ("IS_ANY_PORTION_INTEREST_PAID", "pik"),
                ):
                    raw_value, state = flag_presence(row[column])
                    presences[raw_key] = state
                    flags[column][raw_value if state in ("present",) else state] += 1
                identity[resolved.status] += 1
                by_sub_type[meta["sub_type"]] += 1
                if row["IS_DEFAULT"] == "Y":
                    if resolved.cusip9 is not None:
                        y_cusips.add(resolved.cusip9)
                    else:
                        y_unlinked += 1
                holding_id = row["holding_id"]
                record = {
                    "accession_number": accession,
                    "holding_id": holding_id,
                    "member_name": DEBT_MEMBER,
                    "row_locator": f"HOLDING_ID={holding_id}",
                    "semantic_key": semantic_key_for(accession, semantic, ordinal),
                    "content_sha256": content,
                    "semantic_sha256": semantic,
                    "lot_ordinal": ordinal,
                    "sub_type": meta["sub_type"],
                    "report_date": dt.date.fromisoformat(meta["report_date"]),
                    "filing_date": None if meta["filing_date"] is None else dt.date.fromisoformat(meta["filing_date"]),
                    "registrant_cik": meta["registrant_cik"],
                    "series_id": meta["series_id"],
                    "fund_key": meta["fund_key"],
                    "isin_raw": resolved.isin_raw,
                    "isins_json": json.dumps(list(isins), ensure_ascii=False),
                    "identifier_rows": int(row["identifier_rows"]),
                    "cusip9": resolved.cusip9,
                    "identity_status": resolved.status,
                    "is_default_presence": presences["is_default"],
                    "arrears_presence": presences["arrears"],
                    "pik_presence": presences["pik"],
                }
                for name in _CONTENT_FIELDS:
                    if name != "isins":
                        record[name] = values[name]
                for name, _ in _PROJECTION_FIELDS:
                    out[name].append(record[name])
                written += 1
            if out["accession_number"]:
                writer.write_table(pa.Table.from_pydict(out, schema=schema))
        close_accession()
    return (
        {
            "eligible_rows_projected": written,
            "eligible_rows_excluded": dict(sorted(excluded.items())),
            "eligible_rows_by_sub_type": dict(sorted(by_sub_type.items())),
            "eligible_flag_counts": {k: dict(sorted(v.items())) for k, v in flags.items()},
            "eligible_identity_status": dict(sorted(identity.items())),
            "eligible_is_default_y_distinct_cusip9": len(y_cusips),
            "eligible_is_default_y_unlinked_rows": y_unlinked,
        },
        digests,
    )


# ---------------------------------------------------------------------------
# Public knowledge time and observations
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PublicTime:
    public_available_at: dt.datetime
    basis: str
    acceptance_raw: str | None
    acceptance_at: dt.datetime | None
    note: str


def select_public_time(
    *,
    accession_number: str,
    sub_type: str | None,
    registrant_cik: str | None,
    fallback_at: dt.datetime,
    header: AcceptanceHeader | None,
) -> PublicTime:
    """EDGAR acceptance (America/New_York -> UTC) for a verified public P/P-A accession.

    Anything unverifiable falls back to the package's first verified public availability
    (``first_verified_retrieval``): never FILING_DATE midnight, never report date + 60 days.
    """
    def fallback(note: str) -> PublicTime:
        return PublicTime(fallback_at, "first_verified_retrieval", None, None, note)

    if header is None:
        return fallback("acceptance_header_missing")
    if header.accession_number != accession_number:
        return fallback("acceptance_header_accession_mismatch")
    if not header.is_public_nport:
        return fallback("acceptance_header_not_public_nport")
    if sub_type is not None and header.submission_type != sub_type:
        return fallback("acceptance_header_type_mismatch")
    if registrant_cik is not None and registrant_cik not in header.filer_ciks:
        return fallback("acceptance_header_filer_mismatch")
    if header.acceptance_at > fallback_at:
        return fallback("acceptance_after_first_verified_availability")
    return PublicTime(
        header.acceptance_at, "edgar_acceptance_datetime", header.acceptance_raw,
        header.acceptance_at, "edgar_acceptance",
    )


def _flag_value(raw: str | None, presence: str) -> str | None:
    return raw if presence in ("present", "invalid") else None


def observation_from_projection(
    row: Mapping[str, Any],
    *,
    package: SourcePackage,
    public_time: PublicTime,
    first_seen_at: dt.datetime,
) -> CreditObservation:
    """One contract observation from one projection row (lexical values preserved)."""
    report_date = row["report_date"]
    if isinstance(report_date, str):
        report_date = dt.date.fromisoformat(report_date)
    presence = {
        "nport_is_default": row["is_default_presence"],
        "nport_arrears_or_deferral": row["arrears_presence"],
        "nport_paid_in_kind": row["pik_presence"],
    }
    return CreditObservation.create(
        package_id=package.package_id,
        member_name=row["member_name"],
        row_locator=row["row_locator"],
        observation_kind="nport_holding",
        semantic_key=row["semantic_key"],
        accession_number=row["accession_number"],
        holding_id=row["holding_id"],
        cusip_raw=row["cusip_raw"],
        cusip9=row["cusip9"],
        isin_raw=row["isin_raw"],
        security_id=None,
        issuer_cik=None,
        registrant_cik=row["registrant_cik"],
        series_id=row["series_id"],
        fund_family_id=None,
        issuer_type_raw=row["issuer_type_raw"],
        asset_category_raw=row["asset_category_raw"],
        report_date=report_date,
        effective_date=report_date,
        date_precision="day",
        effective_lower_exclusive=None,
        effective_upper_inclusive=None,
        acceptance_raw=public_time.acceptance_raw,
        acceptance_at=public_time.acceptance_at,
        public_available_at=public_time.public_available_at,
        public_time_basis=public_time.basis,
        first_seen_at=first_seen_at,
        nport_is_default=_flag_value(row["is_default_raw"], presence["nport_is_default"]),
        nport_arrears_or_deferral=_flag_value(row["arrears_raw"], presence["nport_arrears_or_deferral"]),
        nport_paid_in_kind=_flag_value(row["pik_raw"], presence["nport_paid_in_kind"]),
        field_presence=presence,
        agency_name=None,
        agency_subject_kind=None,
        agency_rating_type=None,
        agency_scale=None,
        agency_currency=None,
        agency_rating_symbol=None,
        agency_action_classification=None,
        agency_action_date=None,
        agency_file_creation_at=None,
        document_quote=None,
        document_location=None,
        document_sha256=None,
        revision_kind="original",
        supersedes_observation_id=None,
    )


def iter_projection_rows(
    projection_path: Path, *, cusips: Iterable[str] | None = None, batch_rows: int = 50_000
) -> Iterator[dict[str, Any]]:
    """Projection rows in file order (deterministic), optionally restricted to CUSIP9s."""
    import pyarrow.parquet as pq

    wanted = None if cusips is None else frozenset(cusips)
    parquet = pq.ParquetFile(projection_path)
    for batch in parquet.iter_batches(batch_size=batch_rows):
        for row in batch.to_pylist():
            if wanted is None or row["cusip9"] in wanted:
                yield row


class ReconciliationError(NportError):
    """DERA rows cannot be (or are no longer) proven equal to the public accession content."""


RECONCILIATION_VERSION = "nport_dera_public_reconciliation_v1"


@dataclass(frozen=True)
class DeraReconciliation:
    """Persisted, hash-bound proof that a DERA package's eligible rows of one accession equal
    the public ``primary_doc.xml`` content (canonical semantic identity, ``SEMANTIC_VERSION``).

    Only with such a proof may regenerated DERA rows inherit the EDGAR acceptance time.
    """

    accession_number: str
    dera_package_id: str
    dera_zip_sha256: str
    dera_semantic_digest: str
    xml_package_id: str
    xml_sha256: str
    xml_semantic_digest: str
    header_sha256: str
    header_document_sha256: str
    acceptance_raw: str
    semantic_version: str = SEMANTIC_VERSION
    version: str = RECONCILIATION_VERSION

    def to_record(self) -> dict[str, str]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> DeraReconciliation:
        if set(record) != set(cls.__dataclass_fields__):
            raise ReconciliationError("reconciliation_record_fields")
        rec = cls(**{name: str(record[name]) for name in cls.__dataclass_fields__})
        if rec.version != RECONCILIATION_VERSION or rec.semantic_version != SEMANTIC_VERSION:
            raise ReconciliationError("reconciliation_version_unsupported")
        if rec.dera_semantic_digest != rec.xml_semantic_digest:
            raise ReconciliationError("reconciliation_digests_differ")
        return rec


def _accession_record(result: DeraPackageResult, accession: str) -> dict[str, Any]:
    if result.accessions_path is None:
        raise NportError(f"package_not_parsed:{result.package_label}")
    with open(result.accessions_path, encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            if record["accession_number"] == accession:
                return record
    raise ReconciliationError(f"accession_not_in_dera_package:{accession}")


def reconcile_dera_with_public(result: DeraPackageResult, public: PublicAccessionResult) -> DeraReconciliation:
    """Build the reconciliation proof, or raise :class:`ReconciliationError` if they differ."""
    if result.status != "parsed" or result.source_package is None:
        raise ReconciliationError(f"package_not_parsed:{result.package_label}")
    if public.status != "parsed" or public.header is None:
        raise ReconciliationError("public_accession_not_parsed")
    header = public.header
    record = _accession_record(result, header.accession_number)
    public_fund = fund_key_for(public.series_id, public.registrant_cik)
    mismatches = [
        name for name, dera_value, xml_value in (
            ("sub_type", record["sub_type"], public.sub_type),
            ("registrant_cik", record["registrant_cik"], public.registrant_cik),
            ("fund_key", record["fund_key"], public_fund),
            ("report_date", record["report_date"], None if public.report_date is None else public.report_date.isoformat()),
        ) if dera_value != xml_value
    ]
    if mismatches:
        raise ReconciliationError(f"filing_identity_mismatch:{','.join(mismatches)}")
    xml_digest = accession_semantic_digest(public.semantic_shas)
    if record["eligible_content_digest"] != xml_digest:
        raise ReconciliationError(f"semantic_digest_mismatch:{header.accession_number}")
    return DeraReconciliation(
        accession_number=header.accession_number,
        dera_package_id=str(result.source_package.package_id),
        dera_zip_sha256=result.zip_sha256,
        dera_semantic_digest=record["eligible_content_digest"],
        xml_package_id=str(public.source_package.package_id),
        xml_sha256=public.source_package.content_sha256,
        xml_semantic_digest=xml_digest,
        header_sha256=header.header_sha256,
        header_document_sha256=header.document_sha256,
        acceptance_raw=header.acceptance_raw,
    )


def _verified_reconciliations(
    result: DeraPackageResult, reconciliations: Iterable[DeraReconciliation]
) -> dict[str, DeraReconciliation]:
    """Reconciliations for this package, each re-verified against its persisted accession digest."""
    package = result.source_package
    assert package is not None
    verified: dict[str, DeraReconciliation] = {}
    for rec in reconciliations:
        if rec.dera_package_id != str(package.package_id):
            continue
        if rec.dera_zip_sha256 != result.zip_sha256 or rec.dera_semantic_digest != rec.xml_semantic_digest:
            raise ReconciliationError(f"reconciliation_binding_invalid:{rec.accession_number}")
        record = _accession_record(result, rec.accession_number)
        if record["eligible_content_digest"] != rec.dera_semantic_digest:
            raise ReconciliationError(f"reconciliation_digest_stale:{rec.accession_number}")
        if rec.accession_number in verified and verified[rec.accession_number] != rec:
            raise ReconciliationError(f"reconciliation_duplicate:{rec.accession_number}")
        verified[rec.accession_number] = rec
    return verified


def dera_public_time(
    *,
    accession_number: str,
    sub_type: str | None,
    registrant_cik: str | None,
    package: SourcePackage,
    header: AcceptanceHeader | None,
    reconciliation: DeraReconciliation | None,
) -> PublicTime:
    """Knowledge time of DERA-derived content.

    Regenerated DERA content is public only from the package's verified availability. A
    matching acceptance header is kept as metadata (``acceptance_raw``/``acceptance_at``);
    the acceptance time becomes the availability only when a hash-bound reconciliation
    proves the DERA rows equal the public accession content under that same header.
    """
    selected = select_public_time(
        accession_number=accession_number, sub_type=sub_type, registrant_cik=registrant_cik,
        fallback_at=package.first_verified_public_at, header=header,
    )
    if selected.basis != "edgar_acceptance_datetime":
        return selected
    assert header is not None
    if (
        reconciliation is not None
        and reconciliation.header_sha256 == header.header_sha256
        and reconciliation.acceptance_raw == header.acceptance_raw
    ):
        return PublicTime(selected.public_available_at, "edgar_acceptance_datetime", selected.acceptance_raw,
                          selected.acceptance_at, "reconciled_with_public_xml")
    return PublicTime(package.first_verified_public_at, "first_verified_retrieval", selected.acceptance_raw,
                      selected.acceptance_at, "acceptance_metadata_only")


def iter_package_observations(
    result: DeraPackageResult,
    *,
    acceptance_headers: Mapping[str, AcceptanceHeader] | None = None,
    reconciliations: Iterable[DeraReconciliation] = (),
    cusips: Iterable[str] | None = None,
    first_seen_at: dt.datetime | None = None,
    time_notes: Counter[str] | None = None,
    batch_rows: int = 50_000,
) -> Iterator[CreditObservation]:
    """Lazily build contract observations for a parsed package (bounded memory).

    Availability stays at the package's verified availability unless a verified
    :class:`DeraReconciliation` binds the accession to the matching header (see
    :func:`dera_public_time`).
    """
    if result.status != "parsed" or result.projection_path is None or result.source_package is None:
        raise NportError(f"package_not_parsed:{result.package_label}:{result.status}")
    package = result.source_package
    seen_at = first_seen_at or package.retrieved_at
    headers = acceptance_headers or {}
    proofs = _verified_reconciliations(result, reconciliations)
    cache: dict[str, PublicTime] = {}
    for row in iter_projection_rows(result.projection_path, cusips=cusips, batch_rows=batch_rows):
        accession = row["accession_number"]
        public_time = cache.get(accession)
        if public_time is None:
            public_time = cache[accession] = dera_public_time(
                accession_number=accession,
                sub_type=row["sub_type"],
                registrant_cik=row["registrant_cik"],
                package=package,
                header=headers.get(accession),
                reconciliation=proofs.get(accession),
            )
            if time_notes is not None:
                time_notes[public_time.note] += 1
        yield observation_from_projection(row, package=package, public_time=public_time, first_seen_at=seen_at)


# ---------------------------------------------------------------------------
# Public primary_doc.xml (tail after the latest DERA package)
# ---------------------------------------------------------------------------
#: Versioned mapping of the public N-PORT XML (EDGAR Form N-PORT XML technical
#: specification). Elements are matched by namespace URI + local name; an unknown root
#: namespace quarantines the document instead of guessing.
XML_MAPPING: dict[str, Any] = {
    "version": XML_MAPPING_VERSION,
    "root": "edgarSubmission",
    "namespaces": {
        "nport": "http://www.sec.gov/edgar/nport",
        "com": "http://www.sec.gov/edgar/common",
        "ncom": "http://www.sec.gov/edgar/nportcommon",
    },
    "submission_type": ("headerData", "submissionType"),
    "filer_cik": ("headerData", "filerInfo", "filer", "issuerCredentials", "cik"),
    "registrant_cik": ("formData", "genInfo", "regCik"),
    "series_id": ("formData", "genInfo", "seriesId"),
    "report_date": ("formData", "genInfo", "repPdDate"),
    "holdings": ("formData", "invstOrSecs", "invstOrSec"),
    "holding_fields": {
        "issuer_name": "name", "issuer_lei": "lei", "issuer_title": "title", "cusip_raw": "cusip",
        "balance_raw": "balance", "unit_raw": "units", "currency_value_raw": "valUSD",
    },
    "currency_code": ("curCd", ("currencyConditional", "@curCd")),
    "isin": ("identifiers", "isin", "@value"),
    "asset_category": ("assetCat", ("assetConditional", "@assetCat")),
    "issuer_type": ("issuerCat", ("issuerConditional", "@issuerCat")),
    "debt": "debtSec",
    "debt_fields": {
        "maturity_date_raw": "maturityDt", "coupon_type_raw": "couponKind",
        "annualized_rate_raw": "annualizedRt",
    },
    #: XML element -> DERA flag column (C.9.c / C.9.d / C.9.e).
    "flags": {"isDefault": "IS_DEFAULT", "areIntrstPmntsInArrs": "ARE_ANY_INTEREST_PAYMENT",
              "isPaidKind": "IS_ANY_PORTION_INTEREST_PAID"},
}
MAX_XML_BYTES = 256 * 1024 * 1024
_UTF8_BOM = b"\xef\xbb\xbf"
_FORBIDDEN_XML = re.compile(r"<!\s*(DOCTYPE|ENTITY|ELEMENT|ATTLIST|NOTATION)", re.IGNORECASE)
_XML_DECLARATION = re.compile(r"<\?xml\s[^>]*?encoding\s*=\s*[\"']([^\"']+)[\"']")
_PERMITTED_ENCODINGS = frozenset({"utf-8", "utf8", "us-ascii"})


def _refuse(reason: str) -> Any:
    def handler(*_args: Any) -> None:
        raise XmlSafetyError(reason)

    return handler


def _expat_guard(data: bytes) -> None:
    """Independent expat pass (forced UTF-8) that raises on any DTD/entity construct."""
    from xml.parsers import expat

    parser = expat.ParserCreate(encoding="UTF-8")
    parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)
    parser.StartDoctypeDeclHandler = _refuse("xml_dtd_refused")
    parser.EntityDeclHandler = _refuse("xml_entity_declaration_refused")
    parser.UnparsedEntityDeclHandler = _refuse("xml_entity_declaration_refused")
    parser.NotationDeclHandler = _refuse("xml_notation_refused")
    parser.ExternalEntityRefHandler = _refuse("xml_external_entity_refused")
    parser.SkippedEntityHandler = _refuse("xml_entity_reference_refused")
    try:
        parser.Parse(data, True)
    except expat.ExpatError as exc:
        raise XmlSafetyError(f"xml_malformed:{exc}") from exc


def safe_xml_root(data: bytes, *, max_bytes: int = MAX_XML_BYTES) -> ElementTree.Element:
    """Parse XML that is strictly UTF-8 (optional BOM) with no DTD/entity/external resolution.

    Encoding is enforced *before* any parser sees the bytes: NUL bytes (BOM-less
    UTF-16/32), invalid UTF-8 and any declared encoding other than UTF-8/US-ASCII are
    refused. DTD/entity constructs are refused twice, lexically on the decoded text and
    by an expat pass whose handlers raise, and the tree is then built with the encoding
    forced to UTF-8 so a declaration cannot re-route decoding.
    """
    if len(data) > max_bytes:
        raise XmlSafetyError(f"xml_too_large:{len(data)}")
    body = data.removeprefix(_UTF8_BOM)
    if b"\x00" in body:
        raise XmlSafetyError("xml_encoding_not_utf8:nul_bytes")
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise XmlSafetyError("xml_encoding_not_utf8:invalid_utf8") from exc
    stripped = text.lstrip()
    if stripped.startswith("<?xml"):
        declaration = _XML_DECLARATION.match(stripped)
        if declaration is not None and declaration.group(1).lower() not in _PERMITTED_ENCODINGS:
            raise XmlSafetyError(f"xml_encoding_not_utf8:declared_{declaration.group(1)}")
    if _FORBIDDEN_XML.search(text):
        raise XmlSafetyError("xml_dtd_or_entity_declaration_refused")
    _expat_guard(body)
    try:
        return ElementTree.fromstring(body, parser=ElementTree.XMLParser(encoding="utf-8"))
    except ElementTree.ParseError as exc:
        raise XmlSafetyError(f"xml_malformed:{exc}") from exc


def _local(tag: str) -> tuple[str | None, str]:
    if tag.startswith("{"):
        namespace, _, name = tag[1:].partition("}")
        return namespace, name
    return None, tag


class XmlNamespaceError(XmlSafetyError):
    """A mapped element name appears in a namespace other than the official one."""


def _children(element: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    """Children with the exact QName ``{nport}name``.

    Every mapped path step lives in the official N-PORT namespace
    (``XML_MAPPING['namespaces']['nport']``). A child carrying the mapped local name in
    any other namespace (or none) is never read and raises :class:`XmlNamespaceError`,
    which quarantines the filing.
    """
    official = XML_MAPPING["namespaces"]["nport"]
    matched: list[ElementTree.Element] = []
    for child in element:
        if not isinstance(child.tag, str):
            continue
        namespace, local = _local(child.tag)
        if local != name:
            continue
        if namespace != official:
            raise XmlNamespaceError(f"xml_namespace_mismatch:{name}:{namespace}")
        matched.append(child)
    return matched


def _path(element: ElementTree.Element, steps: Sequence[str]) -> list[ElementTree.Element]:
    nodes = [element]
    for step in steps:
        nodes = [child for node in nodes for child in _children(node, step)]
    return nodes


def _single_text(element: ElementTree.Element, steps: Sequence[str]) -> tuple[bool, str | None]:
    """(element present, text) for a path expected to occur at most once."""
    nodes = _path(element, steps)
    if len(nodes) > 1:
        raise XmlSafetyError(f"xml_repeated_element:{'/'.join(steps)}")
    if not nodes:
        return False, None
    text = nodes[0].text
    return True, (None if text is None or text == "" else text)


def _check_holding_namespaces(holdings: Sequence[ElementTree.Element]) -> None:
    """Probe every mapped holding/debt path so a wrong-namespace field quarantines the filing
    before any holding is read (holding-level parse problems stay holding-local)."""
    plain = list(XML_MAPPING["holding_fields"].values()) + [
        XML_MAPPING["currency_code"][0], XML_MAPPING["currency_code"][1][0],
        XML_MAPPING["asset_category"][0], XML_MAPPING["asset_category"][1][0],
        XML_MAPPING["issuer_type"][0], XML_MAPPING["issuer_type"][1][0], XML_MAPPING["debt"],
    ]
    for holding in holdings:
        for name in plain:
            _children(holding, name)
        _path(holding, XML_MAPPING["isin"][:2])
        for debt in _children(holding, XML_MAPPING["debt"]):
            for name in list(XML_MAPPING["debt_fields"].values()) + list(XML_MAPPING["flags"]):
                _children(debt, name)


def _conditional(element: ElementTree.Element, spec: Sequence[Any]) -> str | None:
    plain, (conditional, attribute) = spec
    present, value = _single_text(element, (plain,))
    if present:
        return value
    nodes = _children(element, conditional)
    if len(nodes) == 1:
        return nodes[0].get(attribute[1:])
    return None


@dataclass(frozen=True)
class PublicAccessionResult:
    status: str
    quarantine_reasons: tuple[str, ...]
    source_package: SourcePackage
    observations: tuple[CreditObservation, ...]
    stats: Mapping[str, Any]
    header: AcceptanceHeader | None = None
    sub_type: str | None = None
    registrant_cik: str | None = None
    series_id: str | None = None
    report_date: dt.date | None = None
    holding_count: int = 0
    #: Canonical semantic identity of every eligible lot (multiset, file order).
    semantic_shas: tuple[str, ...] = ()


def parse_public_accession(
    xml_bytes: bytes,
    *,
    header: AcceptanceHeader,
    retrieved_at: dt.datetime,
    official_url: str,
    first_seen_at: dt.datetime | None = None,
    max_bytes: int = MAX_XML_BYTES,
) -> PublicAccessionResult:
    """Parse one public NPORT-P/P-A ``primary_doc.xml`` into CORP+DBT observations.

    The accession header (fetched once per accession) must be a public P/P-A submission;
    its acceptance time is the knowledge time. A flag element that is not reported is
    ``absent``; an empty element is ``null``; any other lexical value is ``invalid``.
    """
    retrieved = retrieved_at.astimezone(dt.timezone.utc)
    xml_sha = hashlib.sha256(xml_bytes).hexdigest()
    package = SourcePackage.create(
        source_family="sec_nport_public_xml",
        external_id=header.accession_number,
        content_sha256=xml_sha,
        raw_sha256=xml_sha,
        header_sha256=header.header_sha256,
        member_sha256s={XML_MEMBER: xml_sha},
        official_url=official_url,
        accession_number=header.accession_number,
        rights_state="public_government_record",
        rights_ref=None,
        parser_version=XML_PARSER_VERSION,
        schema_version=XML_MAPPING_VERSION,
        retrieved_at=retrieved,
        first_verified_public_at=retrieved,
        public_time_basis="first_verified_retrieval",
        public_time_evidence=(
            f"primary_doc.xml sha256:{xml_sha} retrieved from EDGAR; acceptance header "
            f"sha256:{header.header_sha256} ACCEPTANCE-DATETIME {header.acceptance_raw}"
        ),
        source_coverage_start=None,
        source_coverage_end=None,
        public_coverage_start=None,
        public_coverage_end=None,
        effective_coverage_start=None,
        effective_coverage_end=None,
        raw_locator=f"sec_nport_public_xml/{header.accession_number}/{XML_MEMBER}",
        revision_of_package_id=None,
        sec_run_id=None,
        sec_package_id=None,
    )
    reasons: list[str] = []
    stats: dict[str, Any] = {"accession_number": header.accession_number}
    try:
        root = safe_xml_root(xml_bytes, max_bytes=max_bytes)
    except XmlSafetyError as exc:
        return PublicAccessionResult("quarantined", (str(exc),), package, (), stats)
    namespace, name = _local(root.tag)
    stats["root_namespace"] = namespace
    if name != XML_MAPPING["root"] or namespace != XML_MAPPING["namespaces"]["nport"]:
        return PublicAccessionResult("quarantined", (f"xml_root_unknown:{root.tag}",), package, (), stats)
    try:
        _, submission_type = _single_text(root, XML_MAPPING["submission_type"])
        _, filer_cik = _single_text(root, XML_MAPPING["filer_cik"])
        _, reg_cik = _single_text(root, XML_MAPPING["registrant_cik"])
        _, series_raw = _single_text(root, XML_MAPPING["series_id"])
        _, report_raw = _single_text(root, XML_MAPPING["report_date"])
    except XmlSafetyError as exc:
        return PublicAccessionResult("quarantined", (str(exc),), package, (), stats)
    registrant = normalize_cik(reg_cik) or normalize_cik(filer_cik)
    if submission_type not in PUBLIC_SUB_TYPES:
        reasons.append(f"submission_type_not_public_nport:{submission_type}")
    if submission_type != header.submission_type:
        reasons.append("submission_type_header_mismatch")
    if registrant is None or registrant not in header.filer_ciks:
        reasons.append("registrant_not_header_filer")
    try:
        report_date = None if report_raw is None else dt.date.fromisoformat(report_raw)
    except ValueError:
        report_date = None
    if report_date is None:
        reasons.append("report_date_invalid")
    if reasons:
        return PublicAccessionResult("quarantined", tuple(reasons), package, (), stats)
    assert report_date is not None
    series = series_raw if series_raw is not None and _SERIES.fullmatch(series_raw) else None
    public_time = select_public_time(
        accession_number=header.accession_number,
        sub_type=submission_type,
        registrant_cik=registrant,
        fallback_at=retrieved,
        header=header,
    )
    seen = first_seen_at or retrieved
    observations: list[CreditObservation] = []
    counts: Counter[str] = Counter()
    ordinals: Counter[str] = Counter()
    semantic_shas: list[str] = []
    try:
        holdings = _path(root, XML_MAPPING["holdings"])
        _check_holding_namespaces(holdings)
    except XmlNamespaceError as exc:
        return PublicAccessionResult("quarantined", (str(exc),), package, (), stats)
    for index, holding in enumerate(holdings, start=1):
        counts["holdings"] += 1
        try:
            values: dict[str, Any] = {}
            for key, element in XML_MAPPING["holding_fields"].items():
                values[key] = _single_text(holding, (element,))[1]
            isins = sorted(
                node.get("value") or ""
                for node in _path(holding, XML_MAPPING["isin"][:2])
                if node.get("value")
            )
            values["currency_code_raw"] = _conditional(holding, XML_MAPPING["currency_code"])
            values["issuer_type_raw"] = _conditional(holding, XML_MAPPING["issuer_type"])
            values["asset_category_raw"] = _conditional(holding, XML_MAPPING["asset_category"])
            debts = _children(holding, XML_MAPPING["debt"])
        except XmlSafetyError:
            counts["holding_malformed"] += 1
            continue
        if values["issuer_type_raw"] != ELIGIBLE_ISSUER_TYPE or values["asset_category_raw"] != ELIGIBLE_ASSET_CAT:
            counts["excluded_not_corp_dbt"] += 1
            continue
        if len(debts) != 1:
            counts["eligible_without_single_debt_section"] += 1
            continue
        debt = debts[0]
        presence_by_column: dict[str, tuple[str | None, str]] = {}
        try:
            for key, element in XML_MAPPING["debt_fields"].items():
                values[key] = _single_text(debt, (element,))[1]
            for element, column in XML_MAPPING["flags"].items():
                present, raw = _single_text(debt, (element,))
                presence_by_column[column] = flag_presence(raw, column_present=present)
        except XmlSafetyError:
            counts["holding_malformed"] += 1
            continue
        values["is_default_raw"], is_default_presence = presence_by_column["IS_DEFAULT"]
        values["arrears_raw"], arrears_presence = presence_by_column["ARE_ANY_INTEREST_PAYMENT"]
        values["pik_raw"], pik_presence = presence_by_column["IS_ANY_PORTION_INTEREST_PAID"]
        values["isins"] = isins
        resolved = resolve_identity(values["cusip_raw"], isins)
        semantic = semantic_sha(values)
        semantic_shas.append(semantic)
        ordinal = ordinals[semantic]
        ordinals[semantic] += 1
        row = dict(values)
        row.update(
            {
                "member_name": XML_MEMBER,
                "row_locator": f"invstOrSec[{index}]",
                "semantic_key": semantic_key_for(header.accession_number, semantic, ordinal),
                "content_sha256": _content_sha(values),
                "semantic_sha256": semantic,
                "accession_number": header.accession_number,
                "holding_id": f"xml:{index}",
                "isin_raw": resolved.isin_raw,
                "cusip9": resolved.cusip9,
                "registrant_cik": registrant,
                "series_id": series,
                "report_date": report_date,
                "is_default_presence": is_default_presence,
                "arrears_presence": arrears_presence,
                "pik_presence": pik_presence,
            }
        )
        observations.append(
            observation_from_projection(row, package=package, public_time=public_time, first_seen_at=seen)
        )
        counts["eligible_observations"] += 1
    stats.update(dict(sorted(counts.items())))
    stats["public_time_note"] = public_time.note
    return PublicAccessionResult(
        "parsed", (), package, tuple(observations), stats,
        header=header, sub_type=submission_type, registrant_cik=registrant, series_id=series,
        report_date=report_date, holding_count=counts["holdings"], semantic_shas=tuple(semantic_shas),
    )


# ---------------------------------------------------------------------------
# Accession revisions (§4.1.5)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AccessionFiling:
    """One copy of one accession as attested by one source package."""

    accession_number: str
    registrant_cik: str | None
    fund_key: str | None
    report_date: dt.date | None
    sub_type: str | None
    filing_date: dt.date | None
    public_available_at: dt.datetime
    public_time_basis: str
    holding_count: int
    content_digest: str
    package_id: str
    #: When this copy entered our possession (package retrieval); ``current_run`` mode
    #: requires it <= K in addition to public availability. ``None`` = public time.
    retrieved_at: dt.datetime | None = None


REVISION_MODES = ("historical", "current_run")


@dataclass(frozen=True)
class FilingFamily:
    registrant_cik: str
    fund_key: str
    report_date: dt.date
    status: str
    selected_accession: str | None
    superseded_accessions: tuple[str, ...]
    after_cutoff_accessions: tuple[str, ...]
    ordering_basis: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class RevisionResolution:
    knowledge_cutoff: dt.datetime
    families: tuple[FilingFamily, ...]
    unplaceable_accessions: tuple[str, ...]
    copy_conflict_accessions: tuple[str, ...]
    #: Packages whose copy of the accession was eligible at K (the only copies used).
    attestations: Mapping[str, tuple[str, ...]]
    mode: str = "historical"
    #: Audit only: packages whose copy became eligible after K (never used at K).
    after_cutoff_copies: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def selected_accessions(self) -> frozenset[str]:
        return frozenset(f.selected_accession for f in self.families if f.selected_accession)

    def accession_status(self) -> dict[str, str]:
        status: dict[str, str] = {}
        for family in self.families:
            for accession in family.after_cutoff_accessions:
                status[accession] = "after_cutoff"
            for accession in family.superseded_accessions:
                status[accession] = "superseded" if family.status == "selected" else "disputed_family"
            if family.selected_accession is not None:
                status[family.selected_accession] = "selected"
        for accession in self.unplaceable_accessions:
            status[accession] = "unplaceable"
        return status


def accession_filings_from_result(
    result: DeraPackageResult,
    *,
    acceptance_headers: Mapping[str, AcceptanceHeader] | None = None,
    reconciliations: Iterable[DeraReconciliation] = (),
) -> list[AccessionFiling]:
    """Accession copies attested by one parsed package (public P/P-A only)."""
    if result.status != "parsed" or result.accessions_path is None or result.source_package is None:
        raise NportError(f"package_not_parsed:{result.package_label}")
    package = result.source_package
    headers = acceptance_headers or {}
    proofs = _verified_reconciliations(result, reconciliations)
    filings: list[AccessionFiling] = []
    with open(result.accessions_path, encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            if record["sub_type"] not in PUBLIC_SUB_TYPES or not record["accession_valid"]:
                continue
            public = dera_public_time(
                accession_number=record["accession_number"],
                sub_type=record["sub_type"],
                registrant_cik=record["registrant_cik"],
                package=package,
                header=headers.get(record["accession_number"]),
                reconciliation=proofs.get(record["accession_number"]),
            )
            filings.append(
                AccessionFiling(
                    accession_number=record["accession_number"],
                    registrant_cik=record["registrant_cik"],
                    fund_key=record["fund_key"],
                    report_date=None if record["report_date"] is None else dt.date.fromisoformat(record["report_date"]),
                    sub_type=record["sub_type"],
                    filing_date=None if record["filing_date"] is None else dt.date.fromisoformat(record["filing_date"]),
                    public_available_at=public.public_available_at,
                    public_time_basis=public.basis,
                    holding_count=int(record["n_holdings"]),
                    content_digest=record["eligible_content_digest"],
                    package_id=str(package.package_id),
                    retrieved_at=package.retrieved_at,
                )
            )
    return filings


def accession_filing_from_public(result: PublicAccessionResult) -> AccessionFiling:
    """The accession copy attested by one parsed public ``primary_doc.xml``."""
    if result.status != "parsed" or result.header is None:
        raise NportError(f"public_accession_not_parsed:{result.status}")
    package = result.source_package
    time = select_public_time(
        accession_number=result.header.accession_number, sub_type=result.sub_type,
        registrant_cik=result.registrant_cik, fallback_at=package.first_verified_public_at, header=result.header,
    )
    return AccessionFiling(
        accession_number=result.header.accession_number,
        registrant_cik=result.registrant_cik,
        fund_key=fund_key_for(result.series_id, result.registrant_cik),
        report_date=result.report_date,
        sub_type=result.sub_type,
        filing_date=result.header.filing_date,
        public_available_at=time.public_available_at,
        public_time_basis=time.basis,
        holding_count=result.holding_count,
        content_digest=accession_semantic_digest(result.semantic_shas),
        package_id=str(package.package_id),
        retrieved_at=package.retrieved_at,
    )


def _identities_agree(copies: Sequence[AccessionFiling]) -> bool:
    """Filing identity fields agree across copies; a field a copy does not attest (None) is
    not a disagreement (e.g. an XML copy whose header lacks a filing date)."""
    for name in ("registrant_cik", "fund_key", "report_date", "sub_type", "filing_date"):
        if len({getattr(c, name) for c in copies} - {None}) > 1:
            return False
    return True


def _copy_eligible(copy: AccessionFiling, cutoff: dt.datetime, mode: str) -> bool:
    if copy.public_available_at > cutoff:
        return False
    if mode == "current_run":
        possessed = copy.retrieved_at if copy.retrieved_at is not None else copy.public_available_at
        return possessed <= cutoff
    return True


def resolve_accession_revisions(
    filings: Iterable[AccessionFiling], *, knowledge_cutoff: dt.datetime, mode: str = "historical"
) -> RevisionResolution:
    """Choose, per filing family ``(registrant, series, report_date)``, the latest complete
    replacement public by ``knowledge_cutoff``; never blend original and amendment rows.

    Form N-PORT General Instruction: an amendment must answer *all* items, so an
    NPORT-P/A with a holdings schedule is a complete replacement. An amendment without
    any holding row, several originals, unordered revisions or conflicting package copies
    of one accession leave the family ``disputed``. Revisions accepted after the cutoff
    are listed but never replace facts in the frozen build.

    Only copies eligible at K take part: public by K (and, in ``current_run`` mode, also
    retrieved by K). A later copy of the same accession - even a changed one - cannot
    alter the prior-K selection; it is kept in ``after_cutoff_copies`` for audit.
    """
    if mode not in REVISION_MODES:
        raise NportError(f"revision_mode_invalid:{mode}")
    cutoff = knowledge_cutoff.astimezone(dt.timezone.utc)
    by_accession: dict[str, list[AccessionFiling]] = defaultdict(list)
    late_copies: dict[str, set[str]] = defaultdict(set)
    late_only: dict[str, AccessionFiling] = {}
    for filing in filings:
        if _copy_eligible(filing, cutoff, mode):
            by_accession[filing.accession_number].append(filing)
        else:
            late_copies[filing.accession_number].add(filing.package_id)
            late_only.setdefault(filing.accession_number, filing)
    merged: dict[str, AccessionFiling] = {}
    conflicts: set[str] = set()
    attestations: dict[str, tuple[str, ...]] = {}
    for accession, copies in by_accession.items():
        digests = {c.content_digest for c in copies}
        if not _identities_agree(copies) or len(digests) > 1:
            conflicts.add(accession)
        attestations[accession] = tuple(sorted({c.package_id for c in copies}))
        earliest = min(copies, key=lambda c: (c.public_available_at, c.public_time_basis != "edgar_acceptance_datetime"))
        merged[accession] = earliest
    families: dict[tuple[str, str, dt.date], list[AccessionFiling]] = defaultdict(list)
    late_by_family: dict[tuple[str, str, dt.date], set[str]] = defaultdict(set)
    unplaceable: list[str] = []
    for accession, filing in merged.items():
        if filing.registrant_cik is None or filing.fund_key is None or filing.report_date is None:
            unplaceable.append(accession)
            continue
        families[(filing.registrant_cik, filing.fund_key, filing.report_date)].append(filing)
    for accession, filing in late_only.items():
        if accession in merged:
            continue
        if filing.registrant_cik is None or filing.fund_key is None or filing.report_date is None:
            continue
        key = (filing.registrant_cik, filing.fund_key, filing.report_date)
        late_by_family[key].add(accession)
        families.setdefault(key, [])
    resolved: list[FilingFamily] = []
    for (registrant, fund, report_date), available in sorted(families.items()):
        late = tuple(sorted(late_by_family.get((registrant, fund, report_date), ())))
        if not available:
            resolved.append(FilingFamily(registrant, fund, report_date, "unavailable", None, (), late, "none", ()))
            continue
        reasons: list[str] = []
        if any(m.accession_number in conflicts for m in available):
            reasons.append("accession_copy_conflict")
        if any(m.sub_type not in PUBLIC_SUB_TYPES for m in available):
            reasons.append("unexpected_sub_type")
        originals = [m for m in available if m.sub_type == ORIGINAL_SUB_TYPE]
        amendments = [m for m in available if m.sub_type == AMENDMENT_SUB_TYPE]
        if len(originals) > 1:
            reasons.append("multiple_originals")
        ordering, ordered = _order_revisions(available)
        if ordered is None:
            if len(available) > 1:
                reasons.append("revision_order_ambiguous")
            latest = available[0] if len(available) == 1 else None
        else:
            latest = ordered[-1]
        if amendments and latest is not None and latest.sub_type == ORIGINAL_SUB_TYPE:
            reasons.append("original_after_amendment")
        if latest is not None and latest.sub_type == AMENDMENT_SUB_TYPE and latest.holding_count == 0:
            reasons.append("ambiguous_partial_amendment")
        accessions = sorted(m.accession_number for m in available)
        if reasons or latest is None:
            resolved.append(
                FilingFamily(registrant, fund, report_date, "disputed", None, tuple(accessions), late, ordering,
                             tuple(sorted(set(reasons))))
            )
            continue
        superseded = tuple(a for a in accessions if a != latest.accession_number)
        resolved.append(
            FilingFamily(registrant, fund, report_date, "selected", latest.accession_number, superseded, late,
                         ordering, ())
        )
    return RevisionResolution(
        knowledge_cutoff=cutoff,
        families=tuple(resolved),
        unplaceable_accessions=tuple(sorted(unplaceable)),
        copy_conflict_accessions=tuple(sorted(conflicts)),
        attestations=attestations,
        mode=mode,
        after_cutoff_copies={a: tuple(sorted(p)) for a, p in sorted(late_copies.items())},
    )


def _order_revisions(members: Sequence[AccessionFiling]) -> tuple[str, list[AccessionFiling] | None]:
    """Total order by EDGAR acceptance when every member has it, else by filing date."""
    if len(members) == 1:
        return "single", list(members)
    if all(m.public_time_basis == "edgar_acceptance_datetime" for m in members):
        keys = [m.public_available_at for m in members]
        basis = "edgar_acceptance"
        if len(set(keys)) == len(keys):
            return basis, sorted(members, key=lambda m: m.public_available_at)
    dates = [m.filing_date for m in members]
    if all(d is not None for d in dates) and len(set(dates)) == len(dates):
        return "filing_date", sorted(members, key=lambda m: m.filing_date or dt.date.min)
    return "ambiguous", None


# ---------------------------------------------------------------------------
# Votes and consensus (§4.1.6-7)
# ---------------------------------------------------------------------------
VoteKey = tuple[str, str, str, dt.date]


@dataclass(frozen=True)
class Vote:
    """One fund's vote: ``(accession, series, cusip9, report_date)``; lots are grouped."""

    accession_number: str
    fund_key: str
    registrant_cik: str | None
    cusip9: str
    report_date: dt.date
    value: str
    y_lots: int
    n_lots: int
    nonvote_lots: int
    invalid_lots: int
    arrears_or_deferral_y: bool
    paid_in_kind_y: bool
    observation_ids: tuple[uuid.UUID, ...]
    y_observation_ids: tuple[uuid.UUID, ...]
    n_observation_ids: tuple[uuid.UUID, ...]
    public_available_at: dt.datetime

    @property
    def key(self) -> VoteKey:
        return (self.accession_number, self.fund_key, self.cusip9, self.report_date)


@dataclass(frozen=True)
class VoteSet:
    votes: tuple[Vote, ...]
    excluded_observations: Mapping[str, int]
    disputed_families: tuple[FilingFamily, ...]


class _PreparedVoteBuilder:
    """Operation-local immutable preparation for repeated vote-building chunks."""

    __slots__ = ("_accession_status", "_disputed_families", "_resolution")

    def __init__(self, resolution: RevisionResolution) -> None:
        self._resolution = resolution
        self._accession_status: Mapping[str, str] = MappingProxyType(resolution.accession_status())
        self._disputed_families = tuple(f for f in resolution.families if f.status == "disputed")

    @property
    def disputed_families(self) -> tuple[FilingFamily, ...]:
        return self._disputed_families

    def build(self, observations: Iterable[CreditObservation]) -> VoteSet:
        """Group observations of *selected* accessions into votes.

        Package copies of one semantic row count once; duplicated lots are one vote;
        contradictory lots make the vote ``disputed``; lots without a Y/N IS_DEFAULT
        (null, absent, invalid) cast no vote.

        Every observation must itself be available at the resolution's K
        (``public_available_at <= K``; ``current_run`` mode also
        ``first_seen_at <= K``) and come from a package whose copy of the accession
        was eligible at K.
        """
        resolution = self._resolution
        status = self._accession_status
        cutoff = resolution.knowledge_cutoff
        excluded: Counter[str] = Counter()
        lots: dict[VoteKey, dict[str, list[CreditObservation]]] = defaultdict(lambda: defaultdict(list))
        registrants: dict[VoteKey, str | None] = {}
        for observation in observations:
            if observation.observation_kind != "nport_holding":
                excluded["not_nport_holding"] += 1
                continue
            if observation.public_available_at > cutoff:
                excluded["observation_after_cutoff"] += 1
                continue
            if resolution.mode == "current_run" and observation.first_seen_at > cutoff:
                excluded["observation_not_ingested_by_cutoff"] += 1
                continue
            if observation.cusip9 is None:
                excluded["cusip9_unlinked"] += 1
                continue
            accession = observation.accession_number or ""
            state = status.get(accession, "not_in_resolution")
            if state != "selected":
                excluded[f"accession_{state}"] += 1
                continue
            if str(observation.package_id) not in resolution.attestations.get(accession, ()):
                excluded["package_copy_not_eligible_at_cutoff"] += 1
                continue
            fund = fund_key_for(observation.series_id, observation.registrant_cik)
            if fund is None or observation.report_date is None:
                excluded["fund_or_report_date_missing"] += 1
                continue
            key = (accession, fund, observation.cusip9, observation.report_date)
            lots[key][observation.semantic_key].append(observation)
            registrants[key] = observation.registrant_cik
        votes: list[Vote] = []
        for key in sorted(lots, key=lambda k: (k[2], k[3], k[1], k[0])):
            semantic = lots[key]
            y = n = none = invalid = 0
            arrears = pik = False
            ids: list[uuid.UUID] = []
            y_ids: list[uuid.UUID] = []
            n_ids: list[uuid.UUID] = []
            known: list[dt.datetime] = []
            for copies in semantic.values():
                first = copies[0]
                copy_ids = [c.observation_id for c in copies]
                ids.extend(copy_ids)
                known.append(min(c.public_available_at for c in copies))
                presence = first.field_presence["nport_is_default"]
                if presence == "present" and first.nport_is_default == "Y":
                    y += 1
                    y_ids.extend(copy_ids)
                elif presence == "present" and first.nport_is_default == "N":
                    n += 1
                    n_ids.extend(copy_ids)
                else:
                    none += 1
                    invalid += presence == "invalid"
                arrears = arrears or first.nport_arrears_or_deferral == "Y"
                pik = pik or first.nport_paid_in_kind == "Y"
            value = "disputed" if y and n else "Y" if y else "N" if n else "none"
            votes.append(
                Vote(
                    accession_number=key[0], fund_key=key[1], registrant_cik=registrants[key], cusip9=key[2],
                    report_date=key[3], value=value, y_lots=y, n_lots=n, nonvote_lots=none, invalid_lots=invalid,
                    arrears_or_deferral_y=arrears, paid_in_kind_y=pik,
                    observation_ids=tuple(sorted(ids, key=str)), y_observation_ids=tuple(sorted(y_ids, key=str)),
                    n_observation_ids=tuple(sorted(n_ids, key=str)), public_available_at=max(known),
                )
            )
        return VoteSet(tuple(votes), dict(sorted(excluded.items())), self._disputed_families)


def build_votes(
    observations: Iterable[CreditObservation], *, resolution: RevisionResolution
) -> VoteSet:
    """Build one vote set using an operation-local prepared resolution snapshot."""
    return _PreparedVoteBuilder(resolution).build(observations)


@dataclass(frozen=True)
class FamilyEvidence:
    """Evidenced fund-family identity of one registrant (e.g. an N-CEN adviser relationship).

    Usable for a report date ``d`` at cutoff ``K`` only if ``valid_from <= d < valid_to``
    (open ends allowed) and both the evidence publication and the mapping's own knowledge
    time are ``<= K``.
    """

    registrant_cik: str
    family_id: str
    evidence_ref: str
    evidence_digest: str
    valid_from: dt.date | None
    valid_to: dt.date | None
    public_available_at: dt.datetime
    known_at: dt.datetime

    @property
    def available_at(self) -> dt.datetime:
        return max(self.public_available_at, self.known_at)

    def applies(self, report_date: dt.date, cutoff: dt.datetime) -> bool:
        if self.valid_from is not None and report_date < self.valid_from:
            return False
        if self.valid_to is not None and report_date >= self.valid_to:
            return False
        return self.available_at <= cutoff


@dataclass(frozen=True)
class IndependentCorroboration:
    """Reviewed EDGAR or authorized-agency evidence that ``cusip9`` was in default by
    ``default_evidenced_by`` (economic date).

    Usable at ``K`` only when the evidence was public, the issue link known and the
    adjudication made by ``K``; it then supports report dates ``>= default_evidenced_by``
    (and ``< applies_until`` when a later cure/emergence bounds it).
    """

    cusip9: str
    source_kind: str
    default_evidenced_by: dt.date
    evidence_observation_ids: tuple[uuid.UUID, ...]
    adjudication_id: uuid.UUID
    evidence_refs: tuple[str, ...]
    evidence_digests: tuple[str, ...]
    evidence_public_at: dt.datetime
    link_known_at: dt.datetime
    adjudication_known_at: dt.datetime
    applies_until: dt.date | None = None

    @property
    def available_at(self) -> dt.datetime:
        return max(self.evidence_public_at, self.link_known_at, self.adjudication_known_at)

    def applies(self, report_date: dt.date) -> bool:
        if report_date < self.default_evidenced_by:
            return False
        return self.applies_until is None or report_date < self.applies_until


CORROBORATION_SOURCES = frozenset({"sec_edgar", "agency_rocr"})


@dataclass(frozen=True)
class DateState:
    cusip9: str
    report_date: dt.date
    status: str
    basis: str
    y_funds: tuple[str, ...]
    n_funds: tuple[str, ...]
    disputed_funds: tuple[str, ...]
    y_families: tuple[str, ...]
    n_families: tuple[str, ...]
    corroboration_ids: tuple[uuid.UUID, ...]
    arrears_or_legal_deferral_unknown: bool
    y_observation_ids: tuple[uuid.UUID, ...]
    n_observation_ids: tuple[uuid.UUID, ...]
    #: max knowledge time over every dependency relied on (votes, family evidence, corroboration).
    known_at: dt.datetime
    family_evidence: tuple[FamilyEvidence, ...] = ()
    corroborations: tuple[IndependentCorroboration, ...] = ()


@dataclass(frozen=True)
class StateProposal:
    """Policy-rule proposal for one CUSIP9 (never an admission; W2/human adjudicate).

    ``evidence_observation_ids`` is every relied-on observation (Y votes, prior-N lower
    bound, corroborating observations); ``evidence_known_at`` is the max knowledge time
    over all dependencies. ``onset_lower_evidence_ids`` is non-empty exactly when the lower
    bound is non-null (prior N observations with report date == lower bound).
    """

    cusip9: str
    proposed_status: str
    reviewer_role: str
    basis: str
    onset_lower_exclusive: dt.date | None
    onset_upper_inclusive: dt.date | None
    timing_class: str | None
    left_censored: bool
    evidence_observation_ids: tuple[uuid.UUID, ...]
    evidence_known_at: dt.datetime
    subsequent_y_dates: tuple[dt.date, ...]
    credible_n_after_first_y: tuple[dt.date, ...]
    conflict_dates: tuple[dt.date, ...]
    candidate_y_dates: tuple[dt.date, ...]
    onset_lower_evidence_ids: tuple[uuid.UUID, ...] = ()
    onset_upper_evidence_ids: tuple[uuid.UUID, ...] = ()
    family_evidence: tuple[FamilyEvidence, ...] = ()
    corroboration_adjudication_ids: tuple[uuid.UUID, ...] = ()
    corroboration_evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class QueueItem:
    kind: str
    cusip9: str | None
    report_date: dt.date | None
    reason: str
    family_key: tuple[str, str, str] | None
    accessions: tuple[str, ...]
    evidence_observation_ids: tuple[uuid.UUID, ...]


@dataclass(frozen=True)
class ConsensusResult:
    date_states: tuple[DateState, ...]
    proposals: tuple[StateProposal, ...]
    adjudication_queue: tuple[QueueItem, ...]
    stats: Mapping[str, Any] = field(default_factory=dict)


FamilyEvidenceInput = Mapping[str, "FamilyEvidence | Sequence[FamilyEvidence]"]


def _family_for(
    history: Sequence[FamilyEvidence], report_date: dt.date, cutoff: dt.datetime, stats: Counter[str]
) -> FamilyEvidence | None:
    """The single family mapping applicable at (report date, K); ambiguity -> None."""
    usable = [f for f in history if f.applies(report_date, cutoff)]
    if any(f.available_at > cutoff and f.applies(report_date, f.available_at) for f in history):
        stats["family_evidence_unavailable_at_cutoff"] += 1
    if not usable:
        return None
    if len({f.family_id for f in usable}) > 1:
        stats["family_evidence_ambiguous"] += 1
        return None
    return min(usable, key=lambda f: (f.available_at, f.evidence_ref))


def _support(
    votes: Sequence[Vote],
    *,
    side: str,
    families: Mapping[str, Sequence[FamilyEvidence]],
    corroborations: Sequence[IndependentCorroboration],
    report_date: dt.date,
    cutoff: dt.datetime,
    stats: Counter[str],
) -> tuple[bool, str, tuple[str, ...], tuple[FamilyEvidence, ...], tuple[IndependentCorroboration, ...]]:
    """(supported, basis, families, family evidence relied on, corroborations relied on)."""
    # Only votes carrying a valid EDGAR series ID count toward the >=2 distinct-series and
    # family tests; ``cik:`` fallback keys are grouping/diagnostic identities only.
    series_votes = [v for v in votes if is_series_key(v.fund_key)]
    funds = sorted({v.fund_key for v in series_votes})
    mapped = [
        _family_for(families.get(v.registrant_cik or "", ()), report_date, cutoff, stats) for v in series_votes
    ]
    used = tuple(sorted({m for m in mapped if m is not None}, key=lambda f: (f.registrant_cik, f.evidence_ref)))
    evidenced = tuple(sorted({m.family_id for m in mapped if m is not None}))
    if len(funds) < 2:
        missing = any(not is_series_key(v.fund_key) for v in votes)
        return False, "series_id_missing" if missing else "single_series", evidenced, (), ()
    if len(evidenced) >= 2:
        return True, "independent_families", evidenced, used, ()
    if any(m is None for m in mapped):
        return False, "family_independence_unknown", evidenced, (), ()
    if side == "Y" and corroborations:
        return True, "same_family_corroborated", evidenced, used, tuple(corroborations)
    return False, "same_family_uncorroborated" if side == "Y" else "same_family", evidenced, (), ()


def _as_history(value: FamilyEvidence | Sequence[FamilyEvidence]) -> tuple[FamilyEvidence, ...]:
    return (value,) if isinstance(value, FamilyEvidence) else tuple(value)


def build_consensus_states(
    votes: Iterable[Vote],
    *,
    knowledge_cutoff: dt.datetime,
    family_evidence: FamilyEvidenceInput | None = None,
    corroborations: Iterable[IndependentCorroboration] = (),
    resolved_votes: Iterable[VoteKey] = (),
    disputed_families: Iterable[FilingFamily] = (),
) -> ConsensusResult:
    """State consensus per ``(cusip9, report_date)`` as of ``knowledge_cutoff`` (K), and
    onset-bounded state proposals.

    Consensus (Y or N, identical tests): >= 2 distinct series AND >= 2 independently
    evidenced registrant/fund families, and no unresolved opposite or disputed vote.
    Unknown family identity never supplies independent support; same-family series need
    reviewed independent EDGAR/agency corroboration (Y side only). Votes, family evidence
    and corroboration not available at K are rejected. Any material Y/N conflict is queued
    for human adjudication - never a majority vote. Accepted consensus Y proposes
    ``accepted_state`` with onset ``(last credible N, first credible Y]``; with no prior
    credible N the lower bound is null (prevalent / left-censored). Later N does not prove
    cure; later Y corroborates. C.9.d is only ``arrears_or_legal_deferral_unknown`` and
    C.9.e/PIK never counts.
    """
    cutoff = knowledge_cutoff.astimezone(dt.timezone.utc)
    families = {cik: _as_history(value) for cik, value in (family_evidence or {}).items()}
    for cik, history in families.items():
        if any(f.registrant_cik != cik for f in history):
            raise NportError(f"family_evidence_key_mismatch:{cik}")
    resolved = frozenset(resolved_votes)
    stats: Counter[str] = Counter()
    corroboration_by_cusip: dict[str, list[IndependentCorroboration]] = defaultdict(list)
    for item in corroborations:
        if item.source_kind not in CORROBORATION_SOURCES:
            raise NportError(f"corroboration_source_invalid:{item.source_kind}")
        if item.available_at > cutoff:
            stats["corroborations_unavailable_at_cutoff"] += 1
            continue
        corroboration_by_cusip[item.cusip9].append(item)
    grouped: dict[tuple[str, dt.date], list[Vote]] = defaultdict(list)
    for vote in votes:
        if vote.public_available_at > cutoff:
            stats["votes_after_cutoff"] += 1
            continue
        grouped[(vote.cusip9, vote.report_date)].append(vote)

    states: list[DateState] = []
    queue: list[QueueItem] = []
    for (cusip, report_date), members in sorted(grouped.items()):
        active = [v for v in members if v.key not in resolved and v.value != "none"]
        y_votes = [v for v in active if v.value == "Y"]
        n_votes = [v for v in active if v.value == "N"]
        disputed = [v for v in active if v.value == "disputed"]
        applicable = [c for c in corroboration_by_cusip.get(cusip, ()) if c.applies(report_date)]
        arrears = any(v.arrears_or_deferral_y for v in members)
        y_ids = tuple(sorted({i for v in y_votes + disputed for i in v.y_observation_ids}, key=str))
        n_ids = tuple(sorted({i for v in n_votes + disputed for i in v.n_observation_ids}, key=str))
        y_fam: tuple[str, ...] = ()
        n_fam: tuple[str, ...] = ()
        used_families: tuple[FamilyEvidence, ...] = ()
        used_corroborations: tuple[IndependentCorroboration, ...] = ()
        support = {"families": families, "report_date": report_date, "cutoff": cutoff, "stats": stats}
        if disputed or (y_votes and n_votes):
            status = "conflict"
            basis = "contradictory_lots" if disputed else "material_y_n_conflict"
            queue.append(
                QueueItem(
                    kind="nport_state_conflict", cusip9=cusip, report_date=report_date, reason=basis,
                    family_key=None, accessions=tuple(sorted({v.accession_number for v in active})),
                    evidence_observation_ids=tuple(sorted(set(y_ids) | set(n_ids), key=str)),
                )
            )
        elif y_votes:
            ok, basis, y_fam, used_families, used_corroborations = _support(
                y_votes, side="Y", corroborations=applicable, **support)
            status = "consensus_y" if ok else "candidate_y"
        elif n_votes:
            ok, basis, n_fam, used_families, used_corroborations = _support(
                n_votes, side="N", corroborations=(), **support)
            status = "consensus_n" if ok else "candidate_n"
        else:
            status, basis = "no_informative_vote", "no_y_or_n_vote"
        relied = {"consensus_y": y_votes, "candidate_y": y_votes, "consensus_n": n_votes,
                  "candidate_n": n_votes}.get(status) or members
        known = [v.public_available_at for v in relied]
        known += [f.available_at for f in used_families] + [c.available_at for c in used_corroborations]
        stats[f"date_states_{status}"] += 1
        states.append(
            DateState(
                cusip9=cusip, report_date=report_date, status=status, basis=basis,
                y_funds=tuple(sorted({v.fund_key for v in y_votes})),
                n_funds=tuple(sorted({v.fund_key for v in n_votes})),
                disputed_funds=tuple(sorted({v.fund_key for v in disputed})),
                y_families=y_fam, n_families=n_fam,
                corroboration_ids=tuple(sorted({c.adjudication_id for c in used_corroborations}, key=str)),
                arrears_or_legal_deferral_unknown=arrears,
                y_observation_ids=y_ids, n_observation_ids=n_ids,
                known_at=max(known),
                family_evidence=used_families,
                corroborations=used_corroborations,
            )
        )

    by_cusip: dict[str, list[DateState]] = defaultdict(list)
    for state in states:
        by_cusip[state.cusip9].append(state)
    proposals: list[StateProposal] = []
    for cusip, cusip_states in sorted(by_cusip.items()):
        proposal = _proposal(cusip, cusip_states)
        if proposal is None:
            continue
        proposals.append(proposal)
        stats[f"proposals_{proposal.proposed_status}"] += 1
        for later in proposal.credible_n_after_first_y:
            queue.append(
                QueueItem(
                    kind="nport_credible_n_after_y", cusip9=cusip, report_date=later,
                    reason="n_after_y_is_not_cure_evidence", family_key=None, accessions=(),
                    evidence_observation_ids=next(
                        s.n_observation_ids for s in cusip_states if s.report_date == later
                    ),
                )
            )
    for family in disputed_families:
        queue.append(
            QueueItem(
                kind="nport_filing_family_disputed", cusip9=None, report_date=family.report_date,
                reason=",".join(family.reasons), family_key=(family.registrant_cik, family.fund_key,
                                                             family.report_date.isoformat()),
                accessions=family.superseded_accessions, evidence_observation_ids=(),
            )
        )
    queue.sort(key=lambda q: (q.kind, q.cusip9 or "", q.report_date or dt.date.min, q.family_key or ()))
    stats["queue_items"] = len(queue)
    return ConsensusResult(tuple(states), tuple(proposals), tuple(queue), dict(sorted(stats.items())))


def _lineage(states: Sequence[DateState]) -> dict[str, Any]:
    families = {(f.registrant_cik, f.evidence_ref, f.evidence_digest): f for s in states for f in s.family_evidence}
    corroborations = {c.adjudication_id: c for s in states for c in s.corroborations}
    return {
        "family_evidence": tuple(families[k] for k in sorted(families)),
        "corroboration_adjudication_ids": tuple(sorted(corroborations, key=str)),
        "corroboration_evidence_refs": tuple(sorted({r for c in corroborations.values() for r in c.evidence_refs})),
        "corroboration_observation_ids": {i for c in corroborations.values() for i in c.evidence_observation_ids},
    }


def _proposal(cusip: str, states: Sequence[DateState]) -> StateProposal | None:
    ordered = sorted(states, key=lambda s: s.report_date)
    consensus_y = [s for s in ordered if s.status == "consensus_y"]
    candidates = tuple(s.report_date for s in ordered if s.status == "candidate_y")
    conflicts = tuple(s.report_date for s in ordered if s.status == "conflict")
    if not consensus_y:
        if not candidates:
            return None
        relied = [s for s in ordered if s.status == "candidate_y"]
        lineage = _lineage(relied)
        return StateProposal(
            cusip9=cusip, proposed_status="candidate", reviewer_role="policy_rule_engine",
            basis=relied[0].basis, onset_lower_exclusive=None, onset_upper_inclusive=None, timing_class=None,
            left_censored=False,
            evidence_observation_ids=tuple(sorted({i for s in relied for i in s.y_observation_ids}, key=str)),
            evidence_known_at=max(s.known_at for s in relied),
            subsequent_y_dates=(), credible_n_after_first_y=(), conflict_dates=conflicts,
            candidate_y_dates=candidates,
            family_evidence=lineage["family_evidence"],
            corroboration_adjudication_ids=lineage["corroboration_adjudication_ids"],
            corroboration_evidence_refs=lineage["corroboration_evidence_refs"],
        )
    first_y = consensus_y[0]
    prior_n = [s for s in ordered if s.status == "consensus_n" and s.report_date < first_y.report_date]
    lower = prior_n[-1] if prior_n else None
    relied = [first_y] if lower is None else [lower, first_y]
    lineage = _lineage(relied)
    lower_ids = () if lower is None else lower.n_observation_ids
    if lower is not None and not lower_ids:
        raise NportError(f"onset_lower_bound_without_evidence:{cusip}")
    evidence = set(first_y.y_observation_ids) | set(lower_ids) | lineage["corroboration_observation_ids"]
    lower_date = None if lower is None else lower.report_date
    return StateProposal(
        cusip9=cusip,
        proposed_status="accepted_state",
        reviewer_role="policy_rule_engine",
        basis=first_y.basis,
        onset_lower_exclusive=lower_date,
        onset_upper_inclusive=first_y.report_date,
        timing_class=derive_timing_class(lower_date, first_y.report_date),
        left_censored=lower is None,
        evidence_observation_ids=tuple(sorted(evidence, key=str)),
        evidence_known_at=max(s.known_at for s in relied),
        subsequent_y_dates=tuple(
            s.report_date for s in ordered
            if s.report_date > first_y.report_date and s.status in ("consensus_y", "candidate_y")
        ),
        credible_n_after_first_y=tuple(
            s.report_date for s in ordered if s.report_date > first_y.report_date and s.status == "consensus_n"
        ),
        conflict_dates=conflicts,
        candidate_y_dates=candidates,
        onset_lower_evidence_ids=tuple(lower_ids),
        onset_upper_evidence_ids=first_y.y_observation_ids,
        family_evidence=lineage["family_evidence"],
        corroboration_adjudication_ids=lineage["corroboration_adjudication_ids"],
        corroboration_evidence_refs=lineage["corroboration_evidence_refs"],
    )


# ---------------------------------------------------------------------------
# Offline candidate-only W0 persistence adapter
# ---------------------------------------------------------------------------
_NPORT_INVENTORY_SEAL = object()
_NPORT_DEPENDENCY_FRAMES = (
    "source_packages",
    "observations",
    "event_links",
    "adjudications",
    "ncen_filings",
    "family_contexts",
    "family_evidence",
    "proposal_evidence",
    "exchange_relations",
)


def _adapter_text(value: object) -> str:
    text = " ".join(str(value).replace("\x00", "").split())
    return re.sub(r"(?i)(?:[A-Z]:[\\/]|/)[^\s:]+", "<path>", text)


class NportAdapterError(NportError):
    """Typed refusal from the offline candidate adapter."""

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = _adapter_text(reason)
        self.detail = _adapter_text(detail)
        super().__init__(f"{self.reason}:{self.detail}" if self.detail else self.reason)


def _adapter_contract_error(exc: ContractError) -> NportAdapterError:
    reason, separator, detail = str(exc).partition(":")
    return NportAdapterError(reason, detail if separator else "")


@dataclass(frozen=True)
class NportAdapterIssue:
    reason: str
    report_date: dt.date | None
    accession_numbers: tuple[str, ...] = ()
    observation_ids: tuple[uuid.UUID, ...] = ()

    def __post_init__(self) -> None:
        if not self.reason or (self.report_date is not None and (
            isinstance(self.report_date, dt.datetime) or not isinstance(self.report_date, dt.date)
        )):
            raise NportAdapterError("nport_adapter_arguments_invalid", "issue")
        object.__setattr__(self, "reason", _adapter_text(self.reason))
        object.__setattr__(self, "accession_numbers", tuple(sorted(set(self.accession_numbers))))
        try:
            object.__setattr__(self, "observation_ids", sorted_uuids(self.observation_ids))
        except ContractError as exc:
            raise NportAdapterError("nport_adapter_arguments_invalid", "issue_observation_ids") from exc


@dataclass(frozen=True)
class NportSourceInput:
    """One already-parsed N-PORT source and its DERA-only public-time proofs."""

    result: DeraPackageResult | PublicAccessionResult
    acceptance_headers: Mapping[str, AcceptanceHeader] = field(default_factory=dict)
    reconciliations: tuple[DeraReconciliation, ...] = ()

    def __post_init__(self) -> None:
        if type(self.result) not in (DeraPackageResult, PublicAccessionResult):
            raise NportAdapterError("nport_adapter_arguments_invalid", "source_result_type")
        if not isinstance(self.acceptance_headers, Mapping):
            raise NportAdapterError("nport_adapter_arguments_invalid", "acceptance_headers")
        headers: dict[str, AcceptanceHeader] = {}
        for accession, header in self.acceptance_headers.items():
            if not isinstance(accession, str) or type(header) is not AcceptanceHeader:
                raise NportAdapterError("nport_adapter_arguments_invalid", "acceptance_headers")
            headers[accession] = header
        proofs = tuple(self.reconciliations)
        if not all(type(proof) is DeraReconciliation for proof in proofs):
            raise NportAdapterError("nport_adapter_arguments_invalid", "reconciliations")
        if type(self.result) is PublicAccessionResult and (headers or proofs):
            raise NportAdapterError("nport_adapter_arguments_invalid", "public_source_proofs")
        object.__setattr__(self, "acceptance_headers", MappingProxyType(dict(sorted(headers.items()))))
        object.__setattr__(
            self,
            "reconciliations",
            tuple(sorted(proofs, key=lambda proof: (
                proof.accession_number,
                proof.dera_package_id,
                proof.xml_package_id,
                proof.header_sha256,
            ))),
        )


@dataclass(frozen=True, eq=False)
class PersistedNportInventory:
    """Sealed process-local inventory over an explicit caller-supplied source corpus."""

    report_dates: tuple[dt.date, ...]
    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    source_scope_package_ids: tuple[uuid.UUID, ...]
    source_packages: tuple[SourcePackage, ...]
    observations: tuple[CreditObservation, ...]
    resolution: RevisionResolution
    issues: tuple[NportAdapterIssue, ...]
    excluded_counts: Mapping[str, int]
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _NPORT_INVENTORY_SEAL:
            raise NportAdapterError("nport_inventory_sealed", "use_materialize_nport_inventory")
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(sorted(self.excluded_counts.items()))))


def _inventory_resolution(resolution: RevisionResolution) -> RevisionResolution:
    return RevisionResolution(
        knowledge_cutoff=resolution.knowledge_cutoff,
        families=tuple(resolution.families),
        unplaceable_accessions=tuple(resolution.unplaceable_accessions),
        copy_conflict_accessions=tuple(resolution.copy_conflict_accessions),
        attestations=MappingProxyType({
            accession: tuple(packages)
            for accession, packages in sorted(resolution.attestations.items())
        }),
        mode=resolution.mode,
        after_cutoff_copies=MappingProxyType({
            accession: tuple(packages)
            for accession, packages in sorted(resolution.after_cutoff_copies.items())
        }),
    )


def _adapter_limit(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise NportAdapterError("nport_adapter_arguments_invalid", name)
    return value


def _adapter_cutoff(value: object) -> dt.datetime:
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise NportAdapterError("nport_adapter_arguments_invalid", "knowledge_cutoff")
    return value.astimezone(dt.timezone.utc)


def _adapter_report_dates(values: Iterable[dt.date], *, max_items: int) -> tuple[dt.date, ...]:
    dates: list[dt.date] = []
    try:
        for index, value in enumerate(values, start=1):
            if index > max_items:
                raise NportAdapterError("nport_inventory_limit_exceeded", "report_dates")
            if isinstance(value, dt.datetime) or not isinstance(value, dt.date):
                raise NportAdapterError("nport_adapter_arguments_invalid", "report_dates")
            dates.append(value)
    except TypeError as exc:
        raise NportAdapterError("nport_adapter_arguments_invalid", "report_dates") from exc
    if not dates or len(dates) != len(set(dates)):
        raise NportAdapterError("nport_adapter_arguments_invalid", "report_dates")
    return tuple(sorted(dates))


def _package_checked(package: SourcePackage | None) -> SourcePackage:
    if type(package) is not SourcePackage:
        raise NportAdapterError("nport_source_not_persistable", "source_package")
    try:
        rebuilt = SourcePackage.from_record(package.to_record())
    except ContractError as exc:
        raise NportAdapterError("nport_source_not_persistable", "source_package_record") from exc
    if rebuilt.row_sha256() != package.row_sha256():
        raise NportAdapterError("nport_source_not_persistable", "source_package_bytes")
    return package


def _source_fingerprint(source: NportSourceInput, *, max_scan_rows: int) -> tuple[object, ...]:
    result = source.result
    package = _package_checked(result.source_package)
    proof = tuple(json.dumps(item.to_record(), sort_keys=True, separators=(",", ":"))
                  for item in source.reconciliations)
    headers = tuple(
        (accession, json.dumps(header.to_record(), sort_keys=True, separators=(",", ":")))
        for accession, header in source.acceptance_headers.items()
    )
    if type(result) is DeraPackageResult:
        if (
            result.status != "parsed"
            or result.quarantine_reasons
            or result.projection_path is None
            or result.accessions_path is None
            or not result.projection_path.is_file()
            or not result.accessions_path.is_file()
            or result.zip_sha256 != package.content_sha256
        ):
            raise NportAdapterError("nport_source_not_persistable", result.package_label)
        count = result.stats.get("eligible_rows_projected")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise NportAdapterError("nport_source_projection_mismatch", result.package_label)
        return (
            "dera",
            package.row_sha256(),
            result.package_label,
            result.zip_sha256,
            sha256_file(result.projection_path),
            sha256_file(result.accessions_path),
            count,
            headers,
            proof,
        )
    if (
        result.status != "parsed"
        or result.quarantine_reasons
        or result.header is None
        or result.sub_type not in PUBLIC_SUB_TYPES
        or result.registrant_cik is None
        or result.report_date is None
    ):
        raise NportAdapterError("nport_source_not_persistable", "public_accession")
    if len(result.observations) > max_scan_rows or len(result.semantic_shas) > max_scan_rows:
        raise NportAdapterError("nport_inventory_limit_exceeded", "max_scan_rows")
    unique_observations: dict[uuid.UUID, CreditObservation] = {}
    for row in result.observations:
        if type(row) is not CreditObservation:
            raise NportAdapterError("nport_source_not_persistable", "public_observation_type")
        existing = unique_observations.get(row.observation_id)
        if existing is not None and existing.row_sha256() != row.row_sha256():
            raise NportAdapterError("nport_duplicate_identity_conflict", str(row.observation_id))
        unique_observations[row.observation_id] = row
    ordinals: Counter[str] = Counter()
    semantic_keys: list[str] = []
    for semantic in result.semantic_shas:
        semantic_keys.append(semantic_key_for(result.header.accession_number, semantic, ordinals[semantic]))
        ordinals[semantic] += 1
    holdings_stat = result.stats.get("holdings", 0)
    eligible_stat = result.stats.get("eligible_observations", 0)
    if (
        isinstance(holdings_stat, bool)
        or not isinstance(holdings_stat, int)
        or isinstance(eligible_stat, bool)
        or not isinstance(eligible_stat, int)
        or result.holding_count < len(unique_observations)
        or holdings_stat != result.holding_count
        or eligible_stat != len(unique_observations)
        or sorted(semantic_keys) != sorted(row.semantic_key for row in unique_observations.values())
    ):
        raise NportAdapterError("nport_source_not_persistable", "public_projection_provenance")
    return (
        "public",
        package.row_sha256(),
        result.header.accession_number,
        result.header.header_sha256,
        result.sub_type,
        result.registrant_cik,
        result.series_id,
        result.report_date.isoformat(),
        result.holding_count,
        json.dumps(result.stats, sort_keys=True, separators=(",", ":"), default=str),
        tuple(result.semantic_shas),
        tuple(sorted(row.row_sha256() for row in unique_observations.values())),
    )


def _filing_key(filing: AccessionFiling) -> tuple[object, ...]:
    return (
        filing.accession_number,
        filing.registrant_cik or "",
        filing.fund_key or "",
        filing.report_date or dt.date.min,
        filing.sub_type or "",
        filing.filing_date or dt.date.min,
        filing.public_available_at,
        filing.public_time_basis,
        filing.holding_count,
        filing.content_digest,
        filing.package_id,
        filing.retrieved_at,
    )


def _adapter_issue_key(issue: NportAdapterIssue) -> tuple[object, ...]:
    return (
        issue.report_date or dt.date.min,
        issue.reason,
        issue.accession_numbers,
        tuple(str(item) for item in issue.observation_ids),
    )


def _observation_eligible(
    row: CreditObservation,
    *,
    package: SourcePackage,
    cutoff: dt.datetime,
    mode: str,
    resolution: RevisionResolution,
) -> str | None:
    if row.public_available_at > cutoff:
        return "observation_after_cutoff"
    if mode == "current_run" and package.retrieved_at > cutoff:
        return "package_not_retrieved_by_cutoff"
    if mode == "current_run" and row.first_seen_at > cutoff:
        return "observation_not_ingested_by_cutoff"
    accession = row.accession_number or ""
    if str(row.package_id) not in resolution.attestations.get(accession, ()):
        return "package_copy_not_eligible_at_cutoff"
    return None


def materialize_nport_inventory(
    sources: Iterable[NportSourceInput],
    *,
    report_dates: Iterable[dt.date],
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
    max_scan_rows: int = 100_000,
    max_output_rows: int = 100_000,
) -> PersistedNportInventory:
    """Materialize every eligible N-PORT row for requested dates from an explicit corpus."""
    scan_limit = _adapter_limit(max_scan_rows, "max_scan_rows")
    output_limit = _adapter_limit(max_output_rows, "max_output_rows")
    dates = _adapter_report_dates(report_dates, max_items=scan_limit)
    cutoff = _adapter_cutoff(knowledge_cutoff)
    if knowledge_mode not in ("current_run", "historical_reconstruction"):
        raise NportAdapterError("nport_adapter_arguments_invalid", "knowledge_mode")

    supplied: dict[uuid.UUID, tuple[NportSourceInput, tuple[object, ...]]] = {}
    source_metadata_items = 0
    accession_metadata_rows = 0
    public_fingerprint_rows = 0
    try:
        for index, source in enumerate(sources, start=1):
            if index > scan_limit or type(source) is not NportSourceInput:
                reason = "nport_inventory_limit_exceeded" if index > scan_limit else "nport_adapter_arguments_invalid"
                raise NportAdapterError(reason, "sources")
            source_metadata_items += len(source.acceptance_headers) + len(source.reconciliations)
            if source_metadata_items > scan_limit:
                raise NportAdapterError("nport_inventory_limit_exceeded", "source_metadata")
            fingerprint = _source_fingerprint(source, max_scan_rows=scan_limit)
            if type(source.result) is DeraPackageResult:
                accession_metadata_rows += _count_lines(source.result.accessions_path)
                if accession_metadata_rows > scan_limit:
                    raise NportAdapterError("nport_inventory_limit_exceeded", "accession_rows")
            else:
                public_fingerprint_rows += len(source.result.observations)
                if public_fingerprint_rows > scan_limit:
                    raise NportAdapterError("nport_inventory_limit_exceeded", "public_rows")
            package = _package_checked(source.result.source_package)
            existing = supplied.get(package.package_id)
            if existing is not None:
                if existing[1] != fingerprint:
                    raise NportAdapterError(
                        "nport_duplicate_identity_conflict",
                        str(package.package_id),
                    )
                continue
            supplied[package.package_id] = (source, fingerprint)
    except TypeError as exc:
        raise NportAdapterError("nport_adapter_arguments_invalid", "sources") from exc
    if not supplied:
        raise NportAdapterError("nport_adapter_arguments_invalid", "sources")

    ordered_sources = [supplied[key][0] for key in sorted(supplied, key=str)]
    packages = {
        source.result.source_package.package_id: source.result.source_package
        for source in ordered_sources
    }
    filings: list[AccessionFiling] = []
    for source in ordered_sources:
        result = source.result
        try:
            current = (
                accession_filings_from_result(
                    result,
                    acceptance_headers=source.acceptance_headers,
                    reconciliations=source.reconciliations,
                )
                if type(result) is DeraPackageResult
                else [accession_filing_from_public(result)]
            )
        except (NportError, ReconciliationError) as exc:
            raise NportAdapterError("nport_source_not_persistable", _adapter_text(exc)) from exc
        current = sorted(current, key=_filing_key)
        filings.extend(current)
    filings.sort(key=_filing_key)
    revision_mode = "historical" if knowledge_mode == "historical_reconstruction" else "current_run"
    try:
        resolution = resolve_accession_revisions(
            filings,
            knowledge_cutoff=cutoff,
            mode=revision_mode,
        )
    except NportError as exc:
        raise NportAdapterError("nport_source_not_persistable", _adapter_text(exc)) from exc

    filing_by_accession_package = {
        (filing.accession_number, uuid.UUID(filing.package_id)): filing for filing in filings
    }
    all_observations: dict[uuid.UUID, CreditObservation] = {}
    selected: dict[uuid.UUID, CreditObservation] = {}
    excluded: Counter[str] = Counter()
    scanned = 0
    unplaceable_observations: set[uuid.UUID] = set()
    for source in ordered_sources:
        result = source.result
        rows = (
            iter_package_observations(
                result,
                acceptance_headers=source.acceptance_headers,
                reconciliations=source.reconciliations,
                cusips=None,
            )
            if type(result) is DeraPackageResult
            else iter(result.observations)
        )
        source_count = 0
        for row in rows:
            scanned += 1
            source_count += 1
            if scanned > scan_limit:
                raise NportAdapterError("nport_inventory_limit_exceeded", "max_scan_rows")
            if type(row) is not CreditObservation:
                raise NportAdapterError("nport_source_not_persistable", "observation_type")
            try:
                rebuilt = CreditObservation.from_record(row.to_record())
            except ContractError as exc:
                raise NportAdapterError("nport_source_not_persistable", "observation_record") from exc
            if rebuilt.row_sha256() != row.row_sha256() or row.package_id != result.source_package.package_id:
                raise NportAdapterError("nport_source_not_persistable", "observation_owner")
            filing = filing_by_accession_package.get((row.accession_number or "", row.package_id))
            if filing is None:
                raise NportAdapterError("nport_source_not_persistable", "observation_accession")
            if (
                row.registrant_cik != filing.registrant_cik
                or fund_key_for(row.series_id, row.registrant_cik) != filing.fund_key
                or row.report_date != filing.report_date
            ):
                raise NportAdapterError("nport_source_not_persistable", "observation_identity")
            if type(result) is PublicAccessionResult and (
                row.accession_number != result.header.accession_number
                or row.registrant_cik != result.registrant_cik
                or row.series_id != result.series_id
                or row.report_date != result.report_date
            ):
                raise NportAdapterError("nport_source_not_persistable", "public_observation_identity")
            existing = all_observations.get(row.observation_id)
            if existing is not None and existing.row_sha256() != row.row_sha256():
                raise NportAdapterError("nport_duplicate_identity_conflict", str(row.observation_id))
            all_observations[row.observation_id] = row
            if row.report_date is None:
                eligibility = _observation_eligible(
                    row,
                    package=packages[row.package_id],
                    cutoff=cutoff,
                    mode=knowledge_mode,
                    resolution=resolution,
                )
                if eligibility is None:
                    unplaceable_observations.add(row.observation_id)
                else:
                    excluded[eligibility] += 1
                continue
            if row.report_date not in dates:
                excluded["report_date_not_requested"] += 1
                continue
            eligibility = _observation_eligible(
                row,
                package=packages[row.package_id],
                cutoff=cutoff,
                mode=knowledge_mode,
                resolution=resolution,
            )
            if eligibility is not None:
                excluded[eligibility] += 1
                continue
            prior = selected.get(row.observation_id)
            if prior is not None and prior.row_sha256() != row.row_sha256():
                raise NportAdapterError("nport_duplicate_identity_conflict", str(row.observation_id))
            selected[row.observation_id] = row
        if type(result) is DeraPackageResult and source_count != result.stats["eligible_rows_projected"]:
            raise NportAdapterError(
                "nport_source_projection_mismatch",
                f"{result.package_label}:{source_count}:{result.stats['eligible_rows_projected']}",
            )

    pending = list(selected.values())
    for row in all_observations.values():
        if row.supersedes_observation_id is not None and row.supersedes_observation_id not in all_observations:
            raise NportAdapterError(
                "nport_inventory_lineage_missing",
                str(row.supersedes_observation_id),
            )
    for package in packages.values():
        if package.revision_of_package_id is not None and package.revision_of_package_id not in packages:
            raise NportAdapterError(
                "nport_inventory_lineage_missing",
                str(package.revision_of_package_id),
            )
    try:
        publication.dependency_index(
            {
                "source_packages": tuple(packages.values()),
                "observations": tuple(all_observations.values()),
            },
            knowledge_cutoff=cutoff,
            knowledge_mode=knowledge_mode,
        )
    except ContractError as exc:
        raise _adapter_contract_error(exc) from exc
    while pending:
        row = pending.pop()
        ancestor_id = row.supersedes_observation_id
        if ancestor_id is None:
            continue
        ancestor = all_observations.get(ancestor_id)
        if ancestor is None:
            raise NportAdapterError("nport_inventory_lineage_missing", str(ancestor_id))
        reason = _observation_eligible(
            ancestor,
            package=packages[ancestor.package_id],
            cutoff=cutoff,
            mode=knowledge_mode,
            resolution=resolution,
        )
        if ancestor.report_date not in dates or reason is not None:
            raise NportAdapterError("nport_inventory_lineage_missing", str(ancestor_id))
        if ancestor_id not in selected:
            selected[ancestor_id] = ancestor
            pending.append(ancestor)

    active_package_ids: set[uuid.UUID] = {row.package_id for row in selected.values()}
    for filing in filings:
        package_id = uuid.UUID(filing.package_id)
        if (
            filing.report_date in dates
            and str(package_id) in resolution.attestations.get(filing.accession_number, ())
        ):
            active_package_ids.add(package_id)
    package_pending = list(active_package_ids)
    while package_pending:
        package = packages.get(package_pending.pop())
        if package is None:
            raise NportAdapterError("nport_inventory_lineage_missing", "source_package")
        ancestor_id = package.revision_of_package_id
        if ancestor_id is None:
            continue
        ancestor = packages.get(ancestor_id)
        if ancestor is None or (knowledge_mode == "current_run" and ancestor.retrieved_at > cutoff):
            raise NportAdapterError("nport_inventory_lineage_missing", str(ancestor_id))
        if ancestor_id not in active_package_ids:
            active_package_ids.add(ancestor_id)
            package_pending.append(ancestor_id)

    issues: list[NportAdapterIssue] = []
    if resolution.unplaceable_accessions or unplaceable_observations:
        for report_date in dates:
            issues.append(NportAdapterIssue(
                "nport_inventory_unplaceable",
                report_date,
                resolution.unplaceable_accessions,
                tuple(unplaceable_observations),
            ))
    for family in resolution.families:
        if family.report_date not in dates:
            continue
        accessions = tuple(sorted({
            *family.superseded_accessions,
            *((family.selected_accession,) if family.selected_accession else ()),
        }))
        if len(accessions) > 1:
            issues.append(NportAdapterIssue(
                "nport_revision_context_unrepresentable",
                family.report_date,
                accessions,
                (),
            ))
        if family.status == "disputed":
            issues.append(NportAdapterIssue(
                "nport_revision_selection_ambiguous",
                family.report_date,
                accessions,
                (),
            ))
    for accession in resolution.copy_conflict_accessions:
        copies = [
            filing for filing in filings
            if filing.accession_number == accession
            and filing.package_id in resolution.attestations.get(accession, ())
        ]
        affected = {filing.report_date for filing in copies if filing.report_date in dates}
        if any(filing.report_date is None for filing in copies):
            affected = set(dates)
        for report_date in affected:
            issues.append(NportAdapterIssue(
                "nport_revision_selection_ambiguous",
                report_date,
                (accession,),
                (),
            ))
    for report_date in dates:
        if not any(row.report_date == report_date for row in selected.values()):
            issues.append(NportAdapterIssue("nport_report_date_without_evidence", report_date))

    output_packages = tuple(packages[key] for key in sorted(active_package_ids, key=str))
    output_observations = tuple(selected[key] for key in sorted(selected, key=str))
    if len(output_packages) + len(output_observations) > output_limit:
        raise NportAdapterError("nport_inventory_limit_exceeded", "max_output_rows")
    return PersistedNportInventory(
        report_dates=dates,
        knowledge_cutoff=cutoff,
        knowledge_mode=knowledge_mode,
        source_scope_package_ids=tuple(sorted(packages, key=str)),
        source_packages=output_packages,
        observations=output_observations,
        resolution=_inventory_resolution(resolution),
        issues=tuple(sorted(set(issues), key=_adapter_issue_key)),
        excluded_counts=MappingProxyType(dict(sorted(excluded.items()))),
        _seal=_NPORT_INVENTORY_SEAL,
    )


def _adapter_frames(frames: Mapping[str, Iterable[object]]) -> dict[str, tuple[object, ...]]:
    if not isinstance(frames, Mapping) or not set(_NPORT_DEPENDENCY_FRAMES) <= set(frames):
        raise NportAdapterError("nport_dependency_frames_invalid", "frame_keys")
    typed: dict[str, tuple[object, ...]] = {}
    for frame in _NPORT_DEPENDENCY_FRAMES:
        expected = FRAME_TYPES[frame]
        found: dict[tuple[str, ...], object] = {}
        try:
            rows = iter(frames[frame])
        except TypeError as exc:
            raise NportAdapterError("nport_dependency_frames_invalid", frame) from exc
        for row in rows:
            if type(row) is not expected:
                raise NportAdapterError("nport_dependency_frames_invalid", f"{frame}:row_type")
            try:
                rebuilt = expected.from_record(row.to_record())
            except ContractError as exc:
                raise NportAdapterError("nport_dependency_frames_invalid", f"{frame}:record") from exc
            key = row.key()
            existing = found.get(key)
            if existing is not None and existing.row_sha256() != rebuilt.row_sha256():
                raise NportAdapterError(
                    "nport_dependency_frames_invalid",
                    f"{frame}:duplicate_identity:{'|'.join(key)}",
                )
            found[key] = rebuilt
        typed[frame] = tuple(found[key] for key in sorted(found))
    return typed


def _frame_rows_by_id(rows: Iterable[object]) -> dict[object, object]:
    return {getattr(row, row.KEY[0]): row for row in rows}


def _validate_adapter_lineage(
    frames: Mapping[str, tuple[object, ...]],
) -> tuple[dict[uuid.UUID, SourcePackage], dict[uuid.UUID, CreditObservation]]:
    packages = _frame_rows_by_id(frames["source_packages"])
    observations = _frame_rows_by_id(frames["observations"])
    for package in packages.values():
        if package.revision_of_package_id is not None and package.revision_of_package_id not in packages:
            raise NportAdapterError("dependency_missing", f"source_packages:{package.revision_of_package_id}")
    for row in observations.values():
        if row.package_id not in packages:
            raise NportAdapterError("dependency_missing", f"source_packages:{row.package_id}")
        if row.supersedes_observation_id is not None and row.supersedes_observation_id not in observations:
            raise NportAdapterError("dependency_missing", f"observations:{row.supersedes_observation_id}")
    return packages, observations


def _validate_current_run_possession(
    packages: Mapping[uuid.UUID, SourcePackage],
    observations: Mapping[uuid.UUID, CreditObservation],
    *,
    knowledge_cutoff: dt.datetime,
    deferred_observation_ids: frozenset[uuid.UUID],
) -> None:
    deferred_packages = {
        observations[observation_id].package_id
        for observation_id in deferred_observation_ids
        if observation_id in observations
    }
    for package in packages.values():
        if package.package_id not in deferred_packages and package.retrieved_at > knowledge_cutoff:
            raise NportAdapterError("proposal_evidence_after_cutoff", str(package.package_id))
    for row in observations.values():
        if row.observation_id not in deferred_observation_ids and row.first_seen_at > knowledge_cutoff:
            raise NportAdapterError("proposal_evidence_after_cutoff", str(row.observation_id))


def _inventory_frames_equal(
    inventory: PersistedNportInventory,
    packages: Mapping[uuid.UUID, SourcePackage],
    observations: Mapping[uuid.UUID, CreditObservation],
) -> bool:
    for package in inventory.source_packages:
        other = packages.get(package.package_id)
        if other is None or other.row_sha256() != package.row_sha256():
            return False
    inventory_ids = {row.observation_id for row in inventory.observations}
    for row in inventory.observations:
        other = observations.get(row.observation_id)
        if other is None or other.row_sha256() != row.row_sha256():
            return False
    return not any(
        row.observation_kind == "nport_holding"
        and row.report_date in inventory.report_dates
        and row.observation_id not in inventory_ids
        for row in observations.values()
    )


def candidate_proposal_evidence(
    proposal: StateProposal,
    *,
    inventory: PersistedNportInventory,
    frames: Mapping[str, Iterable[object]],
) -> ProposalEvidence:
    """Persist one verified lineage-free candidate; no accepting path exists here."""
    if type(proposal) is not StateProposal:
        raise NportAdapterError("nport_adapter_arguments_invalid", "proposal")
    if type(inventory) is not PersistedNportInventory or inventory._seal is not _NPORT_INVENTORY_SEAL:
        raise NportAdapterError("nport_adapter_arguments_invalid", "inventory")
    typed = _adapter_frames(frames)
    packages, observations = _validate_adapter_lineage(typed)
    try:
        evidence_ids = sorted_uuids(proposal.evidence_observation_ids)
    except ContractError as exc:
        raise NportAdapterError("proposal_evidence_not_closed", proposal.cusip9) from exc
    if inventory.knowledge_mode == "current_run":
        _validate_current_run_possession(
            packages,
            observations,
            knowledge_cutoff=inventory.knowledge_cutoff,
            deferred_observation_ids=frozenset(evidence_ids),
        )
    try:
        ix = publication.dependency_index(
            typed,
            knowledge_cutoff=inventory.knowledge_cutoff,
            knowledge_mode=inventory.knowledge_mode,
        )
    except ContractError as exc:
        raise _adapter_contract_error(exc) from exc

    for observation_id in evidence_ids:
        row = observations.get(observation_id)
        if row is None:
            raise NportAdapterError("proposal_evidence_not_closed", str(observation_id))
        if observation_id in ix.stale_obs:
            raise NportAdapterError("proposal_evidence_stale", str(observation_id))
        package = packages[row.package_id]
        if (
            row.public_available_at > inventory.knowledge_cutoff
            or (
                inventory.knowledge_mode == "current_run"
                and (
                    row.first_seen_at > inventory.knowledge_cutoff
                    or package.retrieved_at > inventory.knowledge_cutoff
                )
            )
        ):
            raise NportAdapterError("proposal_evidence_after_cutoff", str(observation_id))
    if inventory.knowledge_mode == "current_run":
        for observation_id in evidence_ids:
            package = packages[observations[observation_id].package_id]
            if package.retrieved_at > inventory.knowledge_cutoff:
                raise NportAdapterError("proposal_evidence_after_cutoff", str(observation_id))

    if not _inventory_frames_equal(inventory, packages, observations):
        raise NportAdapterError("nport_inventory_frame_mismatch")
    if proposal.proposed_status != "candidate":
        raise NportAdapterError("proposal_status_not_candidate", proposal.proposed_status)
    if (
        proposal.onset_lower_exclusive is not None
        or proposal.onset_upper_inclusive is not None
        or proposal.onset_lower_evidence_ids
        or proposal.onset_upper_evidence_ids
        or proposal.timing_class is not None
        or proposal.left_censored
        or proposal.subsequent_y_dates
        or proposal.credible_n_after_first_y
    ):
        raise NportAdapterError("candidate_bounds_not_empty", proposal.cusip9)
    if (
        proposal.family_evidence
        or proposal.corroboration_adjudication_ids
        or proposal.corroboration_evidence_refs
    ):
        raise NportAdapterError("proposal_lineage_not_persistable", proposal.cusip9)
    if proposal.reviewer_role != "policy_rule_engine":
        raise NportAdapterError("proposal_candidate_evidence_mismatch", "reviewer_role")
    candidate_dates = tuple(sorted(set(proposal.candidate_y_dates)))
    if not evidence_ids or not candidate_dates:
        raise NportAdapterError("proposal_evidence_not_closed", proposal.cusip9)
    for report_date in candidate_dates:
        blocking = next(
            (
                issue for issue in inventory.issues
                if issue.report_date in (None, report_date)
            ),
            None,
        )
        if blocking is not None:
            raise NportAdapterError(
                "nport_context_blocked",
                f"{report_date.isoformat()}:{blocking.reason}",
            )
    inventory_observation_ids = {item.observation_id for item in inventory.observations}
    for observation_id in evidence_ids:
        row = observations[observation_id]
        if row.observation_id not in inventory_observation_ids:
            raise NportAdapterError("proposal_evidence_not_closed", str(observation_id))
        if (
            row.observation_kind != "nport_holding"
            or row.cusip9 != proposal.cusip9
            or row.report_date not in candidate_dates
            or row.report_date not in inventory.report_dates
            or row.field_presence.get("nport_is_default") != "present"
            or row.nport_is_default != "Y"
        ):
            raise NportAdapterError("proposal_candidate_evidence_mismatch", str(observation_id))

    vote_set = build_votes(inventory.observations, resolution=inventory.resolution)
    recomputed = build_consensus_states(
        vote_set.votes,
        knowledge_cutoff=inventory.knowledge_cutoff,
        family_evidence=None,
        corroborations=(),
        resolved_votes=(),
        disputed_families=vote_set.disputed_families,
    )
    candidate = next((item for item in recomputed.proposals if item.cusip9 == proposal.cusip9), None)
    if candidate is None or (
        candidate.proposed_status != "candidate"
        or sorted_uuids(candidate.evidence_observation_ids) != evidence_ids
        or candidate.basis != proposal.basis
        or tuple(sorted(set(candidate.candidate_y_dates))) != candidate_dates
        or tuple(sorted(set(candidate.conflict_dates))) != tuple(sorted(set(proposal.conflict_dates)))
    ):
        raise NportAdapterError("proposal_candidate_evidence_mismatch", proposal.cusip9)

    descriptor = {
        "cusip9": proposal.cusip9,
        "proposed_status": "candidate",
        "basis": candidate.basis,
        "onset_lower_exclusive": None,
        "onset_upper_inclusive": None,
        "onset_lower_evidence_ids": (),
        "onset_upper_evidence_ids": (),
        "evidence_observation_ids": evidence_ids,
        "family_evidence_ids": (),
        "corroboration_adjudication_ids": (),
    }
    try:
        known = publication.proposal_evidence_known_at(
            typed,
            knowledge_cutoff=inventory.knowledge_cutoff,
            knowledge_mode=inventory.knowledge_mode,
            **descriptor,
        )
        if known is None:
            raise NportAdapterError("proposal_evidence_not_closed", proposal.cusip9)
        if known > inventory.knowledge_cutoff:
            raise NportAdapterError("proposal_evidence_after_cutoff", proposal.cusip9)
        return ProposalEvidence.create(
            **descriptor,
            evidence_known_at=known,
            policy_digest=POLICY_DIGEST,
        )
    except ContractError as exc:
        raise _adapter_contract_error(exc) from exc
