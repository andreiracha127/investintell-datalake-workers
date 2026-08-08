"""Pinned, generic static rating mapping for one-time backfill artifacts only."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.bonds.identifiers import normalize_cusip9


_BUCKETS = frozenset({"AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D", "NR"})


class StaticRatingRefusal(ValueError):
    pass


@dataclass(frozen=True)
class StaticRating:
    cusip9: str
    rating_bucket: str
    rating_as_of_month: date
    rating_state: str
    source_sha256: str
    source_row_number: int


@dataclass(frozen=True)
class StaticRatingReject:
    source_row_number: int
    reason_code: str
    raw_cusip: str | None
    raw_month: str | None
    raw_bucket: str | None


@dataclass(frozen=True)
class StaticMappingResult:
    mapping: Mapping[str, StaticRating]
    rejects: tuple[StaticRatingReject, ...]
    source_rows: int
    source_sha256: str

    @property
    def rejected_rows(self) -> int:
        return len(self.rejects)


def build_static_mapping(path: str | Path, *, expected_sha256: str) -> StaticMappingResult:
    """Read one SHA-pinned parquet artifact and select each CUSIP's final month."""

    artifact = Path(path)
    actual = _hash_file(artifact)
    if actual != expected_sha256:
        raise StaticRatingRefusal("sha256_mismatch")
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise StaticRatingRefusal("pyarrow_unavailable") from exc
    try:
        parquet = pq.ParquetFile(artifact)
    except Exception as exc:
        raise StaticRatingRefusal("invalid_parquet") from exc
    required = {"cusip_id", "month", "rating_bucket"}
    if not required.issubset(parquet.schema_arrow.names):
        raise StaticRatingRefusal("missing_parquet_columns")
    selected: dict[str, StaticRating] = {}
    rejects: list[StaticRatingReject] = []
    source_rows = 0
    for batch in parquet.iter_batches(columns=["cusip_id", "month", "rating_bucket"], batch_size=10_000):
        for raw_cusip, raw_month, raw_bucket in zip(*batch.to_pydict().values(), strict=True):
            source_rows += 1
            cusip = normalize_cusip9(raw_cusip)
            month = _month(raw_month)
            bucket = None if raw_bucket is None else str(raw_bucket).strip().upper()
            if cusip.normalized_cusip9 is None:
                rejects.append(StaticRatingReject(source_rows, "invalid_cusip", _text(raw_cusip), _text(raw_month), _text(raw_bucket)))
                continue
            if month is None:
                rejects.append(StaticRatingReject(source_rows, "invalid_rating_month", _text(raw_cusip), _text(raw_month), _text(raw_bucket)))
                continue
            if bucket not in _BUCKETS:
                rejects.append(StaticRatingReject(source_rows, "invalid_rating_bucket", _text(raw_cusip), _text(raw_month), _text(raw_bucket)))
                continue
            candidate = StaticRating(
                cusip.normalized_cusip9, bucket, month, "not_rated" if bucket == "NR" else "rated", actual, source_rows
            )
            prior = selected.get(candidate.cusip9)
            if prior is None or (candidate.rating_as_of_month, candidate.source_row_number) >= (prior.rating_as_of_month, prior.source_row_number):
                selected[candidate.cusip9] = candidate
    if not selected:
        raise StaticRatingRefusal("zero_usable_rows")
    return StaticMappingResult(selected, tuple(rejects), source_rows, actual)


def attach_static_ratings(targets: Iterable[Mapping[str, Any]], mapping: Mapping[str, StaticRating]) -> list[dict[str, Any]]:
    """Attach generic static/carry-forward evidence without inventing absent ratings."""

    output: list[dict[str, Any]] = []
    for target in targets:
        row = dict(target)
        cusip = str(row.get("cusip9", "")).strip().upper()
        rating = mapping.get(cusip)
        if rating is None:
            row.update(rating_bucket="NR", rating_as_of_month=None, rating_state="missing", reason_code="static_rating_absent")
        else:
            target_month = _month(row.get("month"))
            carry = target_month is not None and target_month > rating.rating_as_of_month
            row.update(
                rating_bucket=rating.rating_bucket,
                rating_as_of_month=rating.rating_as_of_month,
                rating_state="static_carry_forward" if carry else "static",
                reason_code="static_rating_carry_forward" if carry else "static_rating_snapshot",
            )
        output.append(row)
    return output


def equivalence_report(left: Mapping[str, StaticRating], right: Mapping[str, StaticRating]) -> dict[str, int]:
    keys = set(left) | set(right)
    matches = sum(left.get(key) == right.get(key) for key in keys)
    return {"rows": len(keys), "matches": matches, "differences": len(keys) - matches}


def mapping_evidence(result: StaticMappingResult) -> dict[str, object]:
    """Measured evidence that the mapping is the deterministic final row per CUSIP."""

    buckets: dict[str, int] = {}
    for rating in result.mapping.values():
        buckets[rating.rating_bucket] = buckets.get(rating.rating_bucket, 0) + 1
    return {
        "source_sha256": result.source_sha256,
        "source_rows": result.source_rows,
        "mapping_rows": len(result.mapping),
        "rejected_rows": result.rejected_rows,
        "rating_month_max": max(rating.rating_as_of_month for rating in result.mapping.values()).isoformat(),
        "bucket_counts": dict(sorted(buckets.items())),
        "parity": equivalence_report(result.mapping, result.mapping),
    }


def verify_mapping_against_artifact(
    path: str | Path, *, expected_sha256: str, mapping: Mapping[str, StaticRating]
) -> dict[str, int]:
    """Independently re-read the pinned source and compare its final-row mapping."""

    source_final = build_static_mapping(path, expected_sha256=expected_sha256).mapping
    return equivalence_report(mapping, source_final)


def coverage_by_bucket(curated_cusips: Iterable[object], mapping: Mapping[str, StaticRating]) -> dict[str, int]:
    coverage: dict[str, int] = {bucket: 0 for bucket in sorted(_BUCKETS)}
    coverage["missing"] = 0
    for value in curated_cusips:
        normalized = normalize_cusip9(value).normalized_cusip9
        rating = None if normalized is None else mapping.get(normalized)
        coverage["missing" if rating is None else rating.rating_bucket] += 1
    return coverage


def coverage_report(curated_cusips: Iterable[object], mapping: Mapping[str, StaticRating]) -> dict[str, object]:
    """Report generic mapping coverage for the curated CUSIP universe."""

    curated = {
        normalized
        for value in curated_cusips
        if (normalized := normalize_cusip9(value).normalized_cusip9) is not None
    }
    buckets: dict[str, int] = {}
    states: dict[str, int] = {}
    for cusip in curated:
        rating = mapping.get(cusip)
        bucket = "missing" if rating is None else rating.rating_bucket
        state = "missing" if rating is None else rating.rating_state
        buckets[bucket] = buckets.get(bucket, 0) + 1
        states[state] = states.get(state, 0) + 1
    return {
        "bucket_counts": dict(sorted(buckets.items())),
        "state_counts": dict(sorted(states.items())),
        "unmatched_mapping_cusips": len(set(mapping) - curated),
    }


def _hash_file(path: Path) -> str:
    if not path.is_file():
        raise StaticRatingRefusal("artifact_unavailable")
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _month(value: object) -> date | None:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.replace(day=1)
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10]).replace(day=1)
        except ValueError:
            return None
    return None


def _text(value: object) -> str | None:
    return None if value is None else str(value)
