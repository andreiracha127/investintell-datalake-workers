"""EDGAR candidate enumeration and typed corroboration queue (implementation contract §4.2).

:func:`enumerate_candidates` scans a pinned ``submissions.zip`` (every ``recent`` block and
*all* historical supplemental ``CIK##########-submissions-###.json`` files, including
delisted/former registrants). It validates array alignment, supplemental completeness
(``files[].filingCount``) and accession uniqueness per CIK, parses 8-K item codes as
exact tokens (never substrings) and keeps amendments. :func:`cross_check_full_index`
compares the result with an EDGAR quarterly ``form.idx`` for archival completeness.

:func:`build_corroboration_queue` turns candidates into typed *proposals* only. An item
code or keyword never admits anything: every proposal is ``candidate`` for an
``extraction_proposer`` and lists the evidence still missing. Document/exhibit retrieval is
disabled unless an explicit held-out custody filter is supplied.

Knowledge time: ``acceptanceDateTime`` in the submissions JSON carries a ``Z`` suffix but
its digits are EDGAR's America/New_York wall time (Stage 1A sample 0001193125-23-005670:
header ``ACCEPTANCE-DATETIME 20230110165959`` vs JSON ``2023-01-10T16:59:59.000Z``). It is
kept raw and converted only as a *provisional* Eastern wall time; the accession header
(:class:`sec_acquisition.AcceptanceHeader`) is the knowledge-time authority.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import zipfile
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .contracts import ContractError, edgar_acceptance_to_utc
from .sec_acquisition import AcceptanceHeader, FormIndexEntry, normalize_cik

ENUMERATOR_VERSION = "bond_default_edgar_enumerator_v1"
QUEUE_VERSION = "bond_default_edgar_queue_v1"
REQUIRED_ARRAY_KEYS = ("accessionNumber", "filingDate", "acceptanceDateTime", "form", "items")
OPTIONAL_ARRAY_KEYS = ("reportDate", "primaryDocument")
DEFAULT_ITEMS = ("1.03", "2.04")
CANDIDATE_FORMS = ("8-K", "8-K/A")
DISCOVERY_FORMS = ("10-Q", "10-Q/A", "10-K", "10-K/A", "6-K", "6-K/A")
DISCOVERY_8K_ITEMS = ("8.01",)
MAX_MEMBER_BYTES = 64 * 1024 * 1024
MAX_MEMBERS = 5_000_000

_MAIN_MEMBER = re.compile(r"CIK(\d{10})\.json")
_SUPPLEMENTAL_MEMBER = re.compile(r"CIK(\d{10})-submissions-(\d{3})\.json")
_ACCESSION = re.compile(r"\d{10}-\d{2}-\d{6}")
_ITEM_TOKEN = re.compile(r"\d{1,2}\.\d{2}")
_JSON_ACCEPTANCE = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?Z?")

ITEM_POLICY: dict[str, dict[str, Any]] = {
    "1.03": {
        "reason": "item_1_03_bankruptcy_or_receivership_disclosed",
        "event_type": "bankruptcy",
        "required": (
            "actual_bankruptcy_or_receivership", "debtor_identity", "affected_obligations",
            "petition_or_appointment_date", "not_plan_confirmation_only",
        ),
        "note": "Item 1.03 also covers plan confirmations; confirmation is corroboration, not a new default.",
    },
    "2.04": {
        "reason": "item_2_04_triggering_event_acceleration_disclosed",
        "event_type": "payment_default",
        "required": (
            "payment_default_facts", "due_date", "grace_period_end_uncured", "affected_obligations",
            "economic_date",
        ),
        "note": "Technical/covenant acceleration is nonqualifying absent payment/default facts.",
    },
    "8.01": {
        "reason": "item_8_01_other_event_discovery_only",
        "event_type": "unknown",
        "required": ("document_text_review", "event_type_evidence", "affected_obligations", "economic_date"),
        "note": "Discovery only: Item 8.01 carries no default semantics by itself.",
    },
}
DISCOVERY_POLICY: dict[str, Any] = {
    "reason": "periodic_or_foreign_report_discovery_only",
    "event_type": "unknown",
    "required": ("document_text_review", "event_type_evidence", "affected_obligations", "economic_date"),
    "note": "10-Q/10-K/6-K discovery: missed payments, grace/cure and exchanges need text review; "
            "a 6-K is outside the 8-K item taxonomy.",
}


class EdgarError(RuntimeError):
    """Fatal enumeration error (integrity or layout)."""


def parse_item_tokens(raw: object) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(valid item tokens, invalid tokens) from the comma-separated ``items`` field."""
    if raw is None or raw == "":
        return (), ()
    if not isinstance(raw, str):
        return (), (repr(raw),)
    valid: list[str] = []
    invalid: list[str] = []
    for token in (part.strip() for part in raw.split(",")):
        if not token:
            continue
        (valid if _ITEM_TOKEN.fullmatch(token) else invalid).append(token)
    return tuple(dict.fromkeys(valid)), tuple(invalid)


def provisional_acceptance_utc(raw: object) -> dt.datetime | None:
    """JSON ``acceptanceDateTime`` digits read as America/New_York wall time (provisional)."""
    if not isinstance(raw, str):
        return None
    match = _JSON_ACCEPTANCE.fullmatch(raw)
    if match is None:
        return None
    try:
        return edgar_acceptance_to_utc("".join(match.groups()))
    except ContractError:
        return None


def _date(raw: object) -> dt.date | None:
    if not isinstance(raw, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return None
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        return None


@dataclass(frozen=True)
class FilingRow:
    """One raw submissions-JSON row of an accession (one CIK file, one array index)."""

    cik: str
    accession_number: str
    form: str | None
    filing_date_raw: str | None
    acceptance_json_raw: str | None
    report_date_raw: str | None
    items_raw: str | None
    primary_document: str | None
    source_member: str

    def metadata(self) -> tuple[Any, ...]:
        """Every consumed metadata field, items normalized to (valid tokens, invalid tokens)."""
        valid, invalid = parse_item_tokens(self.items_raw)
        return (self.form, self.filing_date_raw, self.acceptance_json_raw, self.report_date_raw,
                (tuple(sorted(valid)), tuple(sorted(invalid))), self.primary_document)


#: Names of :meth:`FilingRow.metadata` positions, used for conflict reporting.
METADATA_FIELDS = ("form", "filing_date", "acceptance", "report_date", "items", "primary_document")


@dataclass(frozen=True)
class FilingVariant:
    """One distinct metadata version of an accession and every copy that attests it."""

    ciks: tuple[str, ...]
    source_members: tuple[str, ...]
    copies: int
    form: str | None
    filing_date_raw: str | None
    filing_date: dt.date | None
    acceptance_json_raw: str | None
    report_date_raw: str | None
    report_date: dt.date | None
    items_raw: str | None
    items: tuple[str, ...]
    invalid_item_tokens: tuple[str, ...]
    primary_document: str | None


@dataclass(frozen=True)
class EdgarCandidate:
    """An accession reconciled across every copy in the bulk file.

    Scalar metadata is set only when all variants agree; a disagreeing field is ``None``,
    listed in ``conflict_fields`` and preserved per variant (never resolved by lowest CIK).
    """

    accession_number: str
    ciks: tuple[str, ...]
    form: str | None
    filing_date: dt.date | None
    items: tuple[str, ...]
    candidate_items: tuple[str, ...]
    acceptance_json_raw: str | None
    acceptance_provisional_utc: dt.datetime | None
    report_date: dt.date | None
    primary_document: str | None
    source_members: tuple[str, ...]
    discovery_only: bool
    variants: tuple[FilingVariant, ...]
    conflict_fields: tuple[str, ...]
    conflict_scopes: tuple[str, ...]

    @property
    def metadata_conflict(self) -> bool:
        return bool(self.conflict_fields)

    @property
    def is_amendment(self) -> bool:
        return self.form is not None and self.form.endswith("/A")


@dataclass(frozen=True)
class EdgarEnumeration:
    submissions_sha256: str
    window: tuple[dt.date | None, dt.date | None]
    items: tuple[str, ...]
    forms: tuple[str, ...]
    candidates: tuple[EdgarCandidate, ...]
    quarter_accessions: Mapping[str, frozenset[str]]
    stats: Mapping[str, Any]
    anomalies: tuple[str, ...]

    def count(self, item: str, form: str, start: dt.date | None = None, end: dt.date | None = None) -> int:
        """Unconflicted-form/date candidates only; conflicted ones are in ``stats``."""
        return sum(
            1 for c in self.candidates
            if item in c.candidate_items and c.form == form and not c.discovery_only and c.filing_date is not None
            and (start is None or c.filing_date >= start) and (end is None or c.filing_date <= end)
        )


def _quarter_label(day: dt.date) -> str:
    return f"{day.year}Q{(day.month - 1) // 3 + 1}"


def _unsafe(name: str) -> bool:
    return name.startswith(("/", "\\")) or "\\" in name or ".." in name.split("/") or ":" in name


_ACCESSION_BYTES = re.compile(rb"\d{10}-\d{2}-\d{6}")


def _member_arrays(document: Any, member: str) -> Mapping[str, Any] | None:
    if _MAIN_MEMBER.fullmatch(member):
        filings = document.get("filings") if isinstance(document, Mapping) else None
        arrays = filings.get("recent") if isinstance(filings, Mapping) else None
    else:
        arrays = document
    return arrays if isinstance(arrays, Mapping) else None


def _aligned(arrays: Mapping[str, Any]) -> tuple[int, list[str], set[int]] | None:
    lengths = {k: len(v) for k, v in arrays.items() if isinstance(v, list)}
    missing = [k for k in REQUIRED_ARRAY_KEYS if k not in lengths]
    if missing or len(set(lengths.values())) > 1:
        return None
    return next(iter(lengths.values()), 0), missing, set(lengths)


def _raw_row(cik: str, member: str, arrays: Mapping[str, Any], keys: set[str], index: int) -> FilingRow:
    def text(key: str) -> str | None:
        if key not in keys:
            return None
        value = arrays[key][index]
        return value if isinstance(value, str) else None

    return FilingRow(
        cik=cik, accession_number=arrays["accessionNumber"][index], form=text("form"),
        filing_date_raw=text("filingDate"), acceptance_json_raw=text("acceptanceDateTime"),
        report_date_raw=text("reportDate"), items_raw=text("items"), primary_document=text("primaryDocument"),
        source_member=member,
    )


def _scan_group(zip_path: str, group: Sequence[tuple[str, Sequence[str]]], params: Mapping[str, Any]) -> dict[str, Any]:
    """Pass 1 (process-pool entry point): integrity checks and the set of accessions for which
    ANY copy is a candidate (form/item/window) or a discovery hit."""
    items = frozenset(params["items"])
    forms = frozenset(params["forms"])
    start = params["start"]
    end = params["end"]
    discovery_ciks = frozenset(params["discovery_ciks"] or ())
    crosscheck = frozenset(params["crosscheck_quarters"] or ())
    stats: Counter[str] = Counter()
    anomalies: list[str] = []
    hits: dict[str, str] = {}
    quarters: dict[str, set[str]] = defaultdict(set)
    with zipfile.ZipFile(zip_path) as archive:
        for cik, members in group:
            seen: dict[str, tuple[Any, ...]] = {}
            expected_supplemental: dict[str, int] = {}
            present_supplemental = {m for m in members if _SUPPLEMENTAL_MEMBER.fullmatch(m)}
            # Main file first: it declares the supplemental files and their filing counts.
            for member in sorted(members, key=lambda name: (name in present_supplemental, name)):
                info = archive.getinfo(member)
                if info.file_size > MAX_MEMBER_BYTES:
                    anomalies.append(f"member_too_large:{member}")
                    stats["members_too_large"] += 1
                    continue
                try:
                    document = json.loads(archive.read(member))
                except (ValueError, UnicodeDecodeError):
                    anomalies.append(f"json_invalid:{member}")
                    stats["members_json_invalid"] += 1
                    continue
                if _MAIN_MEMBER.fullmatch(member):
                    stats["main_files"] += 1
                    declared = normalize_cik(document.get("cik")) if isinstance(document, Mapping) else None
                    if declared != cik:
                        anomalies.append(f"cik_mismatch:{member}")
                        stats["main_cik_mismatch"] += 1
                    filings = document.get("filings") if isinstance(document, Mapping) else None
                    for entry in (filings.get("files") if isinstance(filings, Mapping) else None) or []:
                        name = entry.get("name") if isinstance(entry, Mapping) else None
                        if not isinstance(name, str) or _unsafe(name):
                            anomalies.append(f"supplemental_reference_invalid:{member}")
                            stats["supplemental_reference_invalid"] += 1
                            continue
                        expected_supplemental[name] = int(entry.get("filingCount") or -1)
                else:
                    stats["supplemental_files"] += 1
                arrays = _member_arrays(document, member)
                if arrays is None:
                    anomalies.append(f"arrays_missing:{member}")
                    stats["arrays_missing"] += 1
                    continue
                shape = _aligned(arrays)
                if shape is None:
                    anomalies.append(f"array_misaligned:{member}")
                    stats["files_array_misaligned"] += 1
                    continue
                count, _, keys = shape
                if member in expected_supplemental and expected_supplemental[member] != count:
                    anomalies.append(f"filing_count_mismatch:{member}:{expected_supplemental[member]}:{count}")
                    stats["supplemental_filing_count_mismatch"] += 1
                if member in present_supplemental:
                    stats["supplemental_rows"] += count
                stats["filing_rows"] += count
                for index in range(count):
                    accession = arrays["accessionNumber"][index]
                    if not isinstance(accession, str) or not _ACCESSION.fullmatch(accession):
                        stats["accession_invalid"] += 1
                        continue
                    row = _raw_row(cik, member, arrays, keys, index)
                    signature = row.metadata()
                    previous = seen.get(accession)
                    if previous is not None:
                        if previous == signature:
                            stats["duplicate_accession_identical"] += 1
                            continue
                        stats["duplicate_accession_conflicting"] += 1
                        anomalies.append(f"duplicate_accession_conflicting:{cik}:{accession}")
                    else:
                        seen[accession] = signature
                    filing_date = _date(row.filing_date_raw)
                    if filing_date is None:
                        stats["filing_date_invalid"] += 1
                        continue
                    form = row.form
                    is_8k = form in forms
                    discovery_form = form in DISCOVERY_FORMS
                    if not (is_8k or discovery_form):
                        continue
                    if is_8k and previous is None:
                        stats["candidate_form_rows"] += 1
                        quarter = _quarter_label(filing_date)
                        if quarter in crosscheck:
                            quarters[quarter].add(accession)
                    if (start is not None and filing_date < start) or (end is not None and filing_date > end):
                        continue
                    valid, invalid = parse_item_tokens(row.items_raw)
                    if invalid:
                        stats["invalid_item_tokens"] += len(invalid)
                    if is_8k:
                        stats["candidate_form_rows_in_window"] += 1
                        if items.intersection(valid):
                            hits[accession] = "candidate"
                        elif DISCOVERY_8K_ITEMS[0] in valid:
                            stats["window_8k_item_8_01"] += 1
                            if cik in discovery_ciks:
                                hits.setdefault(accession, "discovery")
                    elif discovery_form:
                        stats[f"window_discovery_form_{form}"] += 1
                        if cik in discovery_ciks:
                            hits.setdefault(accession, "discovery")
            for name in expected_supplemental:
                if name not in present_supplemental:
                    anomalies.append(f"supplemental_missing:{cik}:{name}")
                    stats["supplemental_missing"] += 1
            for name in present_supplemental - set(expected_supplemental):
                anomalies.append(f"supplemental_unreferenced:{name}")
                stats["supplemental_unreferenced"] += 1
    return {"hits": hits, "stats": stats, "anomalies": anomalies,
            "quarters": {k: sorted(v) for k, v in quarters.items()}}


def _collect_group(zip_path: str, group: Sequence[tuple[str, Sequence[str]]], wanted: frozenset[str]) -> list[FilingRow]:
    """Pass 2: every copy (any CIK, any form/date/items) of the wanted accessions."""
    rows: list[FilingRow] = []
    with zipfile.ZipFile(zip_path) as archive:
        for cik, members in group:
            for member in sorted(members):
                info = archive.getinfo(member)
                if info.file_size > MAX_MEMBER_BYTES:
                    continue
                raw = archive.read(member)
                if not wanted.intersection(m.decode("ascii") for m in _ACCESSION_BYTES.findall(raw)):
                    continue
                try:
                    document = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    continue
                arrays = _member_arrays(document, member)
                shape = None if arrays is None else _aligned(arrays)
                if arrays is None or shape is None:
                    continue
                count, _, keys = shape
                for index in range(count):
                    if arrays["accessionNumber"][index] in wanted:
                        rows.append(_raw_row(cik, member, arrays, keys, index))
    return rows


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _run(
    func: Callable[..., Any], zip_path: Path, tasks: Sequence[Any], extra: Any, workers: int
) -> list[Any]:
    if workers <= 1:
        return [func(str(zip_path), task, extra) for task in tasks]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(func, [str(zip_path)] * len(tasks), tasks, [extra] * len(tasks)))


def enumerate_candidates(
    submissions_zip: Path,
    *,
    expected_sha256: str,
    start: dt.date | None = None,
    end: dt.date | None = None,
    items: Iterable[str] = DEFAULT_ITEMS,
    forms: Iterable[str] = CANDIDATE_FORMS,
    discovery_ciks: Collection[str] | None = None,
    crosscheck_quarters: Collection[str] = (),
    workers: int = 1,
    groups_per_task: int = 20_000,
) -> EdgarEnumeration:
    """Enumerate 8-K/8-K/A item candidates from a pinned bulk ``submissions.zip``.

    Pass 1 selects accessions where any copy matches; pass 2 collects every copy of those
    accessions so metadata is reconciled *before* candidate filtering (co-registrant copies
    that would not match the filters on their own are still compared).
    """
    items_t = tuple(sorted(set(items)))
    forms_t = tuple(sorted(set(forms)))
    for item in items_t:
        if not _ITEM_TOKEN.fullmatch(item):
            raise EdgarError(f"item_token_invalid:{item}")
    digest = _sha256_file(Path(submissions_zip))
    if digest != expected_sha256:
        raise EdgarError(f"submissions_sha256_mismatch:{digest}")
    groups: dict[str, list[str]] = defaultdict(list)
    stats: Counter[str] = Counter()
    anomalies: list[str] = []
    with zipfile.ZipFile(submissions_zip) as archive:
        infos = archive.infolist()
        if len(infos) > MAX_MEMBERS:
            raise EdgarError(f"submissions_member_count:{len(infos)}")
        for info in infos:
            name = info.filename
            if info.is_dir():
                continue
            if _unsafe(name):
                raise EdgarError(f"submissions_member_name_unsafe:{name!r}")
            match = _MAIN_MEMBER.fullmatch(name) or _SUPPLEMENTAL_MEMBER.fullmatch(name)
            if match is None:
                stats["members_unrecognized"] += 1
                anomalies.append(f"member_unrecognized:{name}")
                continue
            groups[match.group(1)].append(name)
    stats["members_total"] = sum(len(v) for v in groups.values())
    stats["ciks"] = len(groups)
    params = {
        "items": items_t, "forms": forms_t, "start": start, "end": end,
        "discovery_ciks": sorted(discovery_ciks or ()), "crosscheck_quarters": sorted(crosscheck_quarters),
    }
    ordered = sorted(groups.items())
    tasks = [ordered[i:i + groups_per_task] for i in range(0, len(ordered), groups_per_task)]
    hits: dict[str, str] = {}
    quarters: dict[str, set[str]] = defaultdict(set)
    for part in _run(_scan_group, Path(submissions_zip), tasks, params, workers):
        for accession, kind in part["hits"].items():
            if kind == "candidate" or accession not in hits:
                hits[accession] = kind
        stats.update(part["stats"])
        anomalies.extend(part["anomalies"])
        for quarter, accessions in part["quarters"].items():
            quarters[quarter].update(accessions)
    wanted = frozenset(hits)
    rows: list[FilingRow] = []
    if wanted:
        for part in _run(_collect_group, Path(submissions_zip), tasks, wanted, workers):
            rows.extend(part)
    stats["reconciled_copies"] = len(rows)
    candidates = _reconcile(rows, hits, frozenset(items_t), stats=stats, anomalies=anomalies)
    candidates.sort(key=lambda c: (c.filing_date or dt.date.max, c.accession_number, c.discovery_only))
    stats["candidates"] = sum(1 for c in candidates if not c.discovery_only)
    stats["discovery_candidates"] = sum(1 for c in candidates if c.discovery_only)
    return EdgarEnumeration(
        submissions_sha256=digest,
        window=(start, end),
        items=items_t,
        forms=forms_t,
        candidates=tuple(candidates),
        quarter_accessions={k: frozenset(v) for k, v in sorted(quarters.items())},
        stats=dict(sorted(stats.items())),
        anomalies=tuple(sorted(anomalies)),
    )


def _reconcile(
    rows: Iterable[FilingRow],
    hits: Mapping[str, str],
    wanted: frozenset[str],
    *,
    stats: Counter[str],
    anomalies: list[str],
) -> list[EdgarCandidate]:
    """Group every copy of each accession into metadata variants; flag, never resolve, conflicts."""
    by_accession: dict[str, list[FilingRow]] = defaultdict(list)
    for row in rows:
        by_accession[row.accession_number].append(row)
    out: list[EdgarCandidate] = []
    for accession in sorted(by_accession):
        copies = by_accession[accession]
        grouped: dict[tuple[Any, ...], list[FilingRow]] = defaultdict(list)
        for row in copies:
            grouped[row.metadata()].append(row)
        variants: list[FilingVariant] = []
        for signature in sorted(grouped, key=lambda s: json.dumps(s, sort_keys=True, default=str)):
            members = grouped[signature]
            first = members[0]
            valid, invalid = parse_item_tokens(first.items_raw)
            variants.append(
                FilingVariant(
                    ciks=tuple(sorted({r.cik for r in members})),
                    source_members=tuple(sorted({r.source_member for r in members})),
                    copies=len(members), form=first.form, filing_date_raw=first.filing_date_raw,
                    filing_date=_date(first.filing_date_raw), acceptance_json_raw=first.acceptance_json_raw,
                    report_date_raw=first.report_date_raw, report_date=_date(first.report_date_raw),
                    items_raw=first.items_raw, items=valid, invalid_item_tokens=invalid,
                    primary_document=first.primary_document,
                )
            )
        signatures = list(grouped)
        conflict_fields = tuple(
            name for position, name in enumerate(METADATA_FIELDS)
            if len({signature[position] for signature in signatures}) > 1
        )
        cik_variants: dict[str, set[int]] = defaultdict(set)
        for position, variant in enumerate(variants):
            for cik in variant.ciks:
                cik_variants[cik].add(position)
        scopes: list[str] = []
        if any(len(v) > 1 for v in cik_variants.values()):
            scopes.append("same_cik")
        if len(variants) > 1 and len({frozenset(v) for v in cik_variants.values()}) > 1:
            scopes.append("cross_cik")
        if conflict_fields:
            stats["accession_conflicts"] += 1
            for name in conflict_fields:
                stats[f"accession_conflict_field_{name}"] += 1
            for scope in scopes:
                stats[f"accession_conflict_scope_{scope}"] += 1
            anomalies.append(f"accession_metadata_conflict:{'+'.join(scopes)}:{accession}:{','.join(conflict_fields)}")
        base = variants[0]
        agreed = {name: name not in conflict_fields for name in METADATA_FIELDS}
        discovery_only = hits.get(accession) != "candidate"
        union_items = tuple(sorted({i for v in variants for i in v.items}))
        selector = frozenset(DISCOVERY_8K_ITEMS) if discovery_only else wanted
        form = base.form if agreed["form"] else None
        filing_date = base.filing_date if agreed["filing_date"] else None
        if (form is None or filing_date is None) and not discovery_only:
            stats["candidates_conflicted_form_or_date"] += 1
        acceptance = base.acceptance_json_raw if agreed["acceptance"] else None
        out.append(
            EdgarCandidate(
                accession_number=accession,
                ciks=tuple(sorted({r.cik for r in copies})),
                form=form,
                filing_date=filing_date,
                items=union_items,
                candidate_items=tuple(i for i in union_items if i in selector),
                acceptance_json_raw=acceptance,
                acceptance_provisional_utc=provisional_acceptance_utc(acceptance),
                report_date=base.report_date if agreed["report_date"] else None,
                primary_document=base.primary_document if agreed["primary_document"] else None,
                source_members=tuple(sorted({r.source_member for r in copies})),
                discovery_only=discovery_only,
                variants=tuple(variants),
                conflict_fields=conflict_fields,
                conflict_scopes=tuple(scopes),
            )
        )
    return out


def cross_check_full_index(
    enumeration: EdgarEnumeration, quarter: str, entries: Iterable[FormIndexEntry]
) -> dict[str, Any]:
    """Compare submissions-derived candidate-form accessions with a quarterly ``form.idx``."""
    if quarter not in enumeration.quarter_accessions:
        raise EdgarError(f"quarter_not_collected:{quarter}")
    forms = set(enumeration.forms)
    index = {e.accession_number for e in entries if e.form_type in forms}
    bulk = set(enumeration.quarter_accessions[quarter])
    only_index = sorted(index - bulk)
    only_bulk = sorted(bulk - index)
    return {
        "quarter": quarter,
        "forms": sorted(forms),
        "index_accessions": len(index),
        "submissions_accessions": len(bulk),
        "in_both": len(index & bulk),
        "only_in_index": len(only_index),
        "only_in_submissions": len(only_bulk),
        "only_in_index_sample": only_index[:20],
        "only_in_submissions_sample": only_bulk[:20],
    }


# ---------------------------------------------------------------------------
# Typed corroboration queue
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CorroborationProposal:
    """Typed extraction proposal; never self-approving and never an admission."""

    proposal_key: str
    accession_number: str
    ciks: tuple[str, ...]
    form: str
    item: str | None
    is_amendment: bool
    filing_date: dt.date | None
    candidate_reason: str
    candidate_event_type: str
    proposed_status: str
    reviewer_role: str
    auto_admissible: bool
    public_known_at: dt.datetime | None
    public_time_basis: str
    acceptance_provisional_utc: dt.datetime | None
    header_sha256: str | None
    document_sha256s: tuple[str, ...]
    quote_locations: tuple[str, ...]
    economic_date_fields: Mapping[str, dt.date | None]
    scope_evidence: Mapping[str, str | None]
    linked_issue_hints: tuple[str, ...]
    required_evidence: tuple[str, ...]
    missing_evidence: tuple[str, ...]
    review_status: str
    custody_filter_applied: bool
    document_retrieval_allowed: bool
    notes: tuple[str, ...] = field(default=())


CustodyFilter = Callable[[str, Sequence[str]], bool]


def build_corroboration_queue(
    candidates: Iterable[EdgarCandidate],
    *,
    acceptance_headers: Mapping[str, AcceptanceHeader] | None = None,
    custody_filter: CustodyFilter | None = None,
    linked_issue_hints: Mapping[str, Sequence[str]] | None = None,
) -> tuple[CorroborationProposal, ...]:
    """One typed ``candidate`` proposal per (accession, item) - no auto-admission.

    ``acceptance_headers`` supply the knowledge time only when the header matches the
    accession and filer; otherwise ``public_known_at`` stays ``None``. ``custody_filter``
    (accession, ciks) -> allowed must be supplied for document retrieval to be permitted;
    without it every proposal records that the held-out custody filter is not in place.
    ``linked_issue_hints`` maps CIK -> CUSIP9 hints (nominations, never link proof).
    """
    headers = acceptance_headers or {}
    hints = linked_issue_hints or {}
    proposals: list[CorroborationProposal] = []
    for candidate in candidates:
        header = headers.get(candidate.accession_number)
        header_ok = (
            header is not None
            and header.accession_number == candidate.accession_number
            and bool(set(header.filer_ciks) & set(candidate.ciks))
            and candidate.form is not None
            and header.submission_type == candidate.form
        )
        allowed = custody_filter is not None and bool(custody_filter(candidate.accession_number, candidate.ciks))
        notes: list[str] = []
        if custody_filter is None:
            notes.append("holdout_custody_filter_not_in_place:document_retrieval_disabled")
        if candidate.metadata_conflict:
            notes.append(
                f"accession_metadata_conflict:{'+'.join(candidate.conflict_scopes)}:"
                f"{','.join(candidate.conflict_fields)}:{len(candidate.variants)}_variants"
            )
        if candidate.is_amendment:
            notes.append("amendment:link_to_original_accession_required")
        if header is not None and not header_ok:
            notes.append("acceptance_header_mismatch")
        issue_hints = tuple(sorted({h for cik in candidate.ciks for h in hints.get(cik, ())}))
        targets: list[tuple[str | None, Mapping[str, Any]]]
        if candidate.discovery_only:
            item = next((i for i in candidate.items if i in ITEM_POLICY), None)
            targets = [(item, ITEM_POLICY[item] if item else DISCOVERY_POLICY)]
        else:
            targets = [(item, ITEM_POLICY[item]) for item in candidate.candidate_items if item in ITEM_POLICY]
            targets += [
                (item, {"reason": f"item_{item.replace('.', '_')}_requested", "event_type": "unknown",
                        "required": ("document_text_review",), "note": "Requested item without policy entry."})
                for item in candidate.candidate_items if item not in ITEM_POLICY
            ]
        for item, policy in targets:
            required = tuple(policy["required"])
            missing = list(required) + ["document_text"]
            if not header_ok:
                missing.append("acceptance_header")
            if not issue_hints:
                missing.append("linked_issue_candidates")
            proposals.append(
                CorroborationProposal(
                    proposal_key=f"edgar:{candidate.accession_number}:{item or candidate.form}",
                    accession_number=candidate.accession_number,
                    ciks=candidate.ciks,
                    form=candidate.form,
                    item=item,
                    is_amendment=candidate.is_amendment,
                    filing_date=candidate.filing_date,
                    candidate_reason=policy["reason"],
                    candidate_event_type=policy["event_type"],
                    proposed_status="candidate",
                    reviewer_role="extraction_proposer",
                    auto_admissible=False,
                    public_known_at=header.acceptance_at if header_ok and header is not None else None,
                    public_time_basis="edgar_acceptance_datetime" if header_ok else "unverified",
                    acceptance_provisional_utc=candidate.acceptance_provisional_utc,
                    header_sha256=header.header_sha256 if header_ok and header is not None else None,
                    document_sha256s=(),
                    quote_locations=(),
                    economic_date_fields={"due_date": None, "grace_period_end": None, "petition_date": None,
                                          "exchange_completion_date": None, "event_date": None},
                    scope_evidence={"debtor": None, "affected_obligations": None, "guarantor_scope": None,
                                    "indenture_scope": None, "cure_evidence": None},
                    linked_issue_hints=issue_hints,
                    required_evidence=required,
                    missing_evidence=tuple(missing),
                    review_status="unreviewed",
                    custody_filter_applied=custody_filter is not None,
                    document_retrieval_allowed=allowed,
                    notes=tuple(notes + [policy["note"]]),
                )
            )
    proposals.sort(key=lambda p: (p.filing_date or dt.date.max, p.accession_number, p.item or ""))
    return tuple(proposals)
