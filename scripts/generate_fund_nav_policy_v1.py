"""Generate and verify a governed XNYS current-daily NAV policy without applying it."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.metadata
import json
import os
import re
import secrets
import stat
import sys
import tempfile
import uuid
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from scripts.fund_nav_readiness_schema import _policy
from src.workers._nav_policy import (
    ADJUSTED_OVERLAP_ABS_TOL,
    ADJUSTED_OVERLAP_REL_TOL,
    CATALOG_EVIDENCE_REFERENCE,
    CURRENT_CATALOG_QUERY_VERSION,
    FUNDS_QUERY,
    GENERATOR_VERSION,
    INSTRUMENTS_QUERY,
    PROVIDER_CONTRACT_VERSION,
    SOURCE_QUERY_SHA256,
    calendar_digest,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_VERSION = "4.13.2"
CALENDAR_NAME = "XNYS"
CALENDAR_SOURCE = "exchange_calendars/XNYS"
NY = ZoneInfo("America/New_York")
UTC = dt.timezone.utc
ICI_REFERENCE_URL = "https://www.ici.org/faqs/faq/mfs/faqs_navs"
SOURCE_REFERENCE = (
    f"exchange_calendars=={PACKAGE_VERSION}/XNYS; "
    f"ICI {ICI_REFERENCE_URL} (6:05 p.m. ET Nasdaq delivery context); "
    "18:05 ET is this policy's operational due time, not a fund-specific legal deadline"
)
PROVIDER_CONTRACT = PROVIDER_CONTRACT_VERSION
EVIDENCE_REFERENCE = CATALOG_EVIDENCE_REFERENCE
SUPPORTED_DAILY_FUND_TYPES = frozenset({"etf", "mutual_fund"})
KNOWN_FUND_TYPES = SUPPORTED_DAILY_FUND_TYPES | {"mmf"}
MAX_SOURCE_ROWS = 100_000
ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]*\Z")
EVIDENCE_FIELDS = frozenset(
    {
        "instrument_id",
        "known_at",
        "effective_at",
        "fund_status",
        "valuation_frequency",
        "identity_verified",
        "return_basis_verified",
        "currency_verified",
        "evidence_reference",
    }
)


class PolicyGenerationError(ValueError):
    """A static, sanitized reason; provider responses and DSNs never enter it."""


def canonical_json(value: dict) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("ascii")


def _calendar():
    try:
        installed_version = importlib.metadata.version("exchange_calendars")
    except importlib.metadata.PackageNotFoundError as exc:
        raise PolicyGenerationError("calendar_package_unavailable") from exc
    if installed_version != PACKAGE_VERSION:
        raise PolicyGenerationError("calendar_package_version_mismatch")
    try:
        import exchange_calendars as xcals
    except ImportError as exc:
        raise PolicyGenerationError("calendar_package_unavailable") from exc

    if xcals.__version__ != PACKAGE_VERSION:
        raise PolicyGenerationError("calendar_package_version_mismatch")
    return xcals


@lru_cache(maxsize=8)
def _sessions_for_range(
    requested_start: dt.date, requested_end: dt.date
) -> tuple[tuple[dt.date, dt.datetime, dt.datetime, str], ...]:
    import exchange_calendars as xcals

    try:
        calendar = xcals.get_calendar(
            CALENDAR_NAME,
            start=requested_start.isoformat(),
            end=requested_end.isoformat(),
        )
        # sessions_in_range rejects a requested holiday before first_session.
        sessions = calendar.sessions
    except (ValueError, OverflowError) as exc:
        raise PolicyGenerationError("calendar_coverage_unavailable") from exc
    if len(sessions) < 401:
        raise PolicyGenerationError("calendar_window_too_short")
    prepared = []
    for session in sessions:
        day = session.date()
        close = calendar.session_close(session).to_pydatetime()
        if close.tzinfo is None:
            raise PolicyGenerationError("calendar_close_naive")
        close_utc = close.astimezone(UTC)
        local_close = close_utc.astimezone(NY)
        due = dt.datetime.combine(day, dt.time(18, 5), NY)
        if (
            day < requested_start
            or day > requested_end
            or local_close.date() != day
            or due.date() != day
            or due < local_close
        ):
            raise PolicyGenerationError("calendar_close_or_deadline_invalid")
        prepared.append((day, close_utc, due.astimezone(UTC), SOURCE_REFERENCE))
    return tuple(prepared)


def build_calendar(requested_start: dt.date, requested_end: dt.date) -> dict:
    if requested_start >= requested_end:
        raise PolicyGenerationError("calendar_coverage_invalid")
    _calendar()  # Check the installed distribution on every call, even with cached sessions.
    prepared = _sessions_for_range(requested_start, requested_end)
    digest = calendar_digest(prepared)
    result = {
        "kind": "xnys_nav_calendar_v1",
        "generator_version": GENERATOR_VERSION,
        "publication_state": "unpublished",
        "calendar_id": CALENDAR_NAME,
        "calendar_version": f"exchange_calendars-{PACKAGE_VERSION}-{digest[:16]}-v1",
        "calendar_source": CALENDAR_SOURCE,
        "timezone": "America/New_York",
        "source_reference": SOURCE_REFERENCE,
        "requested_coverage_start": requested_start.isoformat(),
        "requested_coverage_end": requested_end.isoformat(),
        "coverage_start": prepared[0][0].isoformat(),
        "coverage_end": prepared[-1][0].isoformat(),
        "valid_through": prepared[-1][2].isoformat(),
        "calendar_session_count": len(prepared),
        "calendar_digest": digest,
        "sessions": [
            {
                "session_date": day.isoformat(),
                "valuation_close_at": close.isoformat(),
                "nav_due_at": due.isoformat(),
                "source_reference": reference,
            }
            for day, close, due, reference in prepared
        ],
    }
    return result


def _catalog_rows(cursor) -> tuple[list[dict], list[dict]]:
    cursor.execute(INSTRUMENTS_QUERY)
    instruments = cursor.fetchall()
    cursor.execute(FUNDS_QUERY)
    funds = cursor.fetchall()
    if len(instruments) > MAX_SOURCE_ROWS or len(funds) > MAX_SOURCE_ROWS:
        raise PolicyGenerationError("catalog_row_limit_exceeded")
    return instruments, funds


def read_catalog_snapshot(dsn: str) -> tuple[dt.datetime, list[dict], list[dict]]:
    """Pin both current catalog sources to one transaction snapshot, never assign an xid."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            try:
                cursor.execute("SET LOCAL statement_timeout='30s'")
                cursor.execute("SET LOCAL lock_timeout='1s'")
                cursor.execute(
                    "SELECT clock_timestamp() AS decision_at, "
                    "current_setting('transaction_read_only') AS read_only, "
                    "txid_current_if_assigned() AS xid"
                )
                meta = cursor.fetchone()
                if meta["read_only"] != "on" or meta["xid"] is not None:
                    raise PolicyGenerationError("catalog_transaction_not_read_only")
                instruments, funds = _catalog_rows(cursor)
                cursor.execute("SELECT txid_current_if_assigned() AS xid")
                if cursor.fetchone()["xid"] is not None:
                    raise PolicyGenerationError("catalog_transaction_assigned_xid")
                return meta["decision_at"], instruments, funds
            finally:
                cursor.execute("ROLLBACK")


def _text(value: object) -> str | None:
    return str(value).strip() if value is not None and str(value).strip() else None


def _ticker(value: object) -> str | None:
    text = _text(value)
    return text.upper() if text else None


def _normalize_source(rows: list[dict], fields: tuple[str, ...]) -> list[dict]:
    return sorted(
        (
            {
                key: str(row[key]) if isinstance(row[key], uuid.UUID) else row[key]
                for key in fields
            }
            for row in rows
        ),
        key=lambda row: json.dumps(row, sort_keys=True, default=str),
    )


def classify_catalog(
    instruments: list[dict], funds: list[dict], observed_at: dt.datetime
) -> tuple[list[dict], dict]:
    if observed_at.tzinfo is None or not instruments or not funds:
        raise PolicyGenerationError("catalog_snapshot_unavailable")
    by_instrument: dict[str, list[dict]] = defaultdict(list)
    by_fund: dict[str, list[dict]] = defaultdict(list)
    instrument_tickers: Counter[str] = Counter()
    fund_tickers: Counter[str] = Counter()
    instrument_isins: Counter[str] = Counter()
    fund_isins: Counter[str] = Counter()
    series_tickers: Counter[tuple[str, str]] = Counter()
    for row in instruments:
        key = str(row["instrument_id"])
        by_instrument[key].append(row)
        ticker = _ticker(row.get("ticker"))
        if ticker:
            instrument_tickers[ticker] += 1
        if _text(row.get("isin")):
            instrument_isins[_text(row["isin"]).upper()] += 1
    for row in funds:
        key = str(row["instrument_id"])
        by_fund[key].append(row)
        ticker = _ticker(row.get("ticker"))
        series = _text(row.get("series_id"))
        if ticker:
            fund_tickers[ticker] += 1
        if _text(row.get("isin")):
            fund_isins[_text(row["isin"]).upper()] += 1
        if series and ticker:
            series_tickers[(series, ticker)] += 1
    instant = observed_at.astimezone(UTC).isoformat()
    result = []
    statuses: Counter[str] = Counter()
    frequency: Counter[str] = Counter()
    for instrument_id in sorted(
        set(by_fund)
        | {
            key
            for key, rows in by_instrument.items()
            if any(_text(row.get("instrument_type")) == "fund" for row in rows)
        }
    ):
        uuid.UUID(instrument_id)
        iu_rows, fund_rows = by_instrument[instrument_id], by_fund[instrument_id]
        iu = iu_rows[0] if len(iu_rows) == 1 else None
        fund = fund_rows[0] if len(fund_rows) == 1 else None
        iu_ticker = _ticker(iu.get("ticker")) if iu else None
        fund_ticker = _ticker(fund.get("ticker")) if fund else None
        series = _text(fund.get("series_id")) if fund else None
        fund_type = _text(fund.get("fund_type")) if fund else None
        iu_isin = (
            _text(iu.get("isin")).upper() if iu and _text(iu.get("isin")) else None
        )
        fund_isin = (
            _text(fund.get("isin")).upper()
            if fund and _text(fund.get("isin"))
            else None
        )
        consistent_isin = bool(
            iu_isin
            and fund_isin
            and iu_isin == fund_isin
            and instrument_isins[iu_isin] == fund_isins[iu_isin] == 1
        )
        identity = bool(
            iu
            and fund
            and _text(iu.get("instrument_type")) == "fund"
            and iu_ticker
            and iu_ticker == fund_ticker
            and series
            and instrument_tickers[iu_ticker] == fund_tickers[iu_ticker] == 1
            and series_tickers[(series, iu_ticker)] == 1
            and consistent_isin
        )
        native_usd = bool(
            identity
            and _text(iu.get("currency")) == "USD"
            and _text(fund.get("currency")) == "USD"
        )
        if iu and iu.get("is_active") is False and not fund_rows:
            status = "INACTIVE"
        elif (
            iu
            and iu.get("is_active") is True
            and identity
            and native_usd
            and fund_type in KNOWN_FUND_TYPES
        ):
            status = "ACTIVE"
        else:
            status = "UNKNOWN"
        supported = status == "ACTIVE" and fund_type in SUPPORTED_DAILY_FUND_TYPES
        cadence = "daily" if supported else "unknown"
        result.append(
            {
                "instrument_id": instrument_id,
                "known_at": instant,
                "effective_at": instant,
                "fund_status": status,
                "valuation_frequency": cadence,
                "identity_verified": identity and status == "ACTIVE",
                "return_basis_verified": supported,
                "currency_verified": native_usd and status == "ACTIVE",
                "evidence_reference": EVIDENCE_REFERENCE,
            }
        )
        statuses[status] += 1
        frequency[cadence] += 1
    counts = {
        "instruments_universe": len(instruments),
        "funds_v": len(funds),
        "instrument_evidence": len(result),
        "fund_status": dict(sorted(statuses.items())),
        "valuation_frequency": dict(sorted(frequency.items())),
        "duplicate_catalog_tickers": sum(n > 1 for n in instrument_tickers.values()),
        "duplicate_fund_tickers": sum(n > 1 for n in fund_tickers.values()),
        "duplicate_instrument_isins": sum(n > 1 for n in instrument_isins.values()),
        "duplicate_fund_isins": sum(n > 1 for n in fund_isins.values()),
    }
    return result, counts


def build_policy(
    calendar: dict,
    instruments: list[dict],
    funds: list[dict],
    observed_at: dt.datetime,
    policy_id: str,
    policy_version: str,
) -> dict:
    if (
        not policy_id
        or not policy_version
        or len(policy_id) > 128
        or len(policy_version) > 64
    ):
        raise PolicyGenerationError("policy_identity_invalid")
    evidence, counts = classify_catalog(instruments, funds, observed_at)
    if not evidence:
        raise PolicyGenerationError("catalog_snapshot_empty")
    policy = {
        "readiness_profile": "current_daily_nav_v1",
        "readiness_version": 1,
        "generator_version": GENERATOR_VERSION,
        "provider_contract": PROVIDER_CONTRACT,
        "publication_state": "approved",
        "policy_id": policy_id,
        "policy_version": policy_version,
        "valuation_frequency": "daily",
        "timezone": calendar["timezone"],
        "calendar_id": calendar["calendar_id"],
        "calendar_version": calendar["calendar_version"],
        "calendar_source": calendar["calendar_source"],
        "source_reference": calendar["source_reference"],
        "coverage_start": calendar["coverage_start"],
        "coverage_end": calendar["coverage_end"],
        "valid_through": calendar["valid_through"],
        "calendar_session_count": calendar["calendar_session_count"],
        "calendar_digest": calendar["calendar_digest"],
        "sample_intervals": 400,
        "required_endpoints": 401,
        "annualization_sessions": 252,
        "required_nav_kind": "adjusted",
        "required_return_semantics": "observed_interval_log_ratio",
        "modeling_currency": "USD",
        "currency_treatment": "native_only",
        "repaired_nav_kinds": sorted(REPAIRED_NAV_KINDS),
        "adjusted_overlap_absolute_tolerance": ADJUSTED_OVERLAP_ABS_TOL,
        "adjusted_overlap_relative_tolerance": ADJUSTED_OVERLAP_REL_TOL,
        "sessions": calendar["sessions"],
        "instrument_evidence": evidence,
    }
    source_content = {
        "instruments": _normalize_source(
            instruments,
            (
                "instrument_id",
                "instrument_type",
                "ticker",
                "isin",
                "currency",
                "is_active",
            ),
        ),
        "funds": _normalize_source(
            funds,
            ("instrument_id", "series_id", "ticker", "isin", "currency", "fund_type"),
        ),
    }
    snapshot_digest = hashlib.sha256(canonical_json(source_content)).hexdigest()
    policy["generation"] = {
        "generator_version": GENERATOR_VERSION,
        "generated_at": observed_at.astimezone(UTC).isoformat(),
        "requested_coverage_start": calendar["requested_coverage_start"],
        "requested_coverage_end": calendar["requested_coverage_end"],
        "calendar_package": f"exchange_calendars=={PACKAGE_VERSION}",
        "calendar_digest": calendar["calendar_digest"],
        "source_query_version": CURRENT_CATALOG_QUERY_VERSION,
        "source_query_sha256": SOURCE_QUERY_SHA256,
        "source_snapshot_sha256": snapshot_digest,
        "instrument_evidence_digest": instrument_evidence_digest(evidence),
        "policy_hash": policy_content_digest(policy),
        "provider_contract": PROVIDER_CONTRACT,
        "counts": counts,
    }
    policy["generation"]["generation_sha256"] = generation_metadata_digest(
        policy["generation"]
    )
    _policy(policy)
    return policy


def verify_artifact(artifact: dict, *, raw: bytes | None = None) -> dict:
    if raw is not None and raw != canonical_json(artifact):
        raise PolicyGenerationError("artifact_not_canonical")
    generation = artifact.get("generation", artifact)
    requested_start = dt.date.fromisoformat(generation["requested_coverage_start"])
    requested_end = dt.date.fromisoformat(generation["requested_coverage_end"])
    expected = build_calendar(requested_start, requested_end)
    fields = (
        "calendar_id",
        "calendar_version",
        "calendar_source",
        "timezone",
        "source_reference",
        "coverage_start",
        "coverage_end",
        "valid_through",
        "calendar_session_count",
        "calendar_digest",
        "sessions",
    )
    if any(artifact.get(field) != expected[field] for field in fields):
        raise PolicyGenerationError("calendar_artifact_mismatch")
    if artifact.get("kind") == "xnys_nav_calendar_v1":
        if (
            artifact.get("publication_state") != "unpublished"
            or "instrument_evidence" in artifact
            or artifact.get("generator_version") != GENERATOR_VERSION
        ):
            raise PolicyGenerationError("calendar_reference_not_unpublished")
        return {
            "mode": "calendar",
            "calendar_digest": expected["calendar_digest"],
            "calendar_session_count": expected["calendar_session_count"],
            "coverage_end": expected["coverage_end"],
        }
    if artifact.get("publication_state") != "approved":
        raise PolicyGenerationError("policy_not_approved")
    _policy(artifact)
    evidence = artifact["instrument_evidence"]
    if any(set(row) != EVIDENCE_FIELDS for row in evidence):
        raise PolicyGenerationError("instrument_evidence_shape_invalid")
    if [row["instrument_id"] for row in evidence] != sorted(
        row["instrument_id"] for row in evidence
    ):
        raise PolicyGenerationError("instrument_evidence_order_invalid")
    counts = artifact["generation"]["counts"]
    for key in ("fund_status", "valuation_frequency"):
        field = "fund_status" if key == "fund_status" else "valuation_frequency"
        actual = dict(sorted(Counter(row[field] for row in evidence).items()))
        if counts[key] != actual:
            raise PolicyGenerationError("instrument_evidence_counts_invalid")
    return {
        "mode": "build",
        "calendar_digest": expected["calendar_digest"],
        "calendar_session_count": expected["calendar_session_count"],
        "coverage_end": expected["coverage_end"],
        "policy_hash": artifact["generation"]["policy_hash"],
        "instrument_evidence_digest": artifact["generation"][
            "instrument_evidence_digest"
        ],
        "counts": counts,
    }


def _platform_name() -> str:
    return os.name


def _reject_git_checkout(destination: Path) -> None:
    for ancestor in destination.parents:
        try:
            (ancestor / ".git").lstat()  # Presence only; never read its contents.
        except FileNotFoundError:
            continue
        raise PolicyGenerationError("artifact_inside_git_checkout")


def _custody_destination(path: Path, custody_root: Path | None) -> Path:
    if custody_root is None:
        raise PolicyGenerationError("custody_root_required")
    raw_path = path.expanduser().absolute()
    resolved_path = raw_path.resolve(strict=False)
    _reject_git_checkout(raw_path)
    _reject_git_checkout(resolved_path)
    if raw_path != resolved_path or path.is_symlink():
        raise PolicyGenerationError("artifact_symlink_path_invalid")
    raw_root = custody_root.expanduser().absolute()
    try:
        root = raw_root.resolve(strict=True)
        root_stat = root.stat()
    except FileNotFoundError as exc:
        raise PolicyGenerationError("custody_root_missing") from exc
    _reject_git_checkout(root)
    if raw_root != root or not root.is_dir():
        raise PolicyGenerationError("custody_root_invalid")
    if root_stat.st_uid != os.geteuid() or stat.S_IMODE(root_stat.st_mode) & 0o077:
        raise PolicyGenerationError("custody_root_not_private")
    if not resolved_path.is_relative_to(root) or not resolved_path.parent.is_dir():
        raise PolicyGenerationError("artifact_outside_custody_root")
    return resolved_path


def _pinned_parent(path: Path) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parts = path.parent.parts
    fd = os.open(parts[0], flags)
    try:
        for component in parts[1:]:
            following = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = following
        current = os.fstat(fd)
        original = os.stat(path.parent, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
            raise PolicyGenerationError("artifact_parent_changed")
        _reject_git_checkout(path)
        return fd
    except OSError as exc:
        os.close(fd)
        raise PolicyGenerationError("artifact_parent_changed") from exc
    except Exception:
        os.close(fd)
        raise


def _check_parent_and_target(path: Path, parent_fd: int, *, force: bool) -> None:
    pinned = os.fstat(parent_fd)
    try:
        current = os.stat(path.parent, follow_symlinks=False)
        still_resolved = path.parent.resolve(strict=True) == path.parent
    except FileNotFoundError as exc:
        raise PolicyGenerationError("artifact_parent_changed") from exc
    if not still_resolved or (pinned.st_dev, pinned.st_ino) != (
        current.st_dev,
        current.st_ino,
    ):
        raise PolicyGenerationError("artifact_parent_changed")
    _reject_git_checkout(path)
    try:
        target = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(target.st_mode) or (force and not stat.S_ISREG(target.st_mode)):
        raise PolicyGenerationError("artifact_target_not_regular")


def _write_posix(path: Path, content: bytes, *, force: bool) -> None:
    parent_fd = _pinned_parent(path)
    temporary = None
    published = False
    try:
        for _ in range(4):
            candidate = f".nav-policy-{secrets.token_hex(16)}.tmp"
            try:
                file_fd = os.open(
                    candidate,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=parent_fd,
                )
                temporary = candidate
                break
            except FileExistsError:
                continue
        if temporary is None:
            raise PolicyGenerationError("artifact_private_temp_unavailable")
        with os.fdopen(file_fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        _check_parent_and_target(path, parent_fd, force=force)
        if force:
            os.replace(temporary, path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            temporary = None
            published = True
        else:
            try:
                os.link(
                    temporary,
                    path.name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise PolicyGenerationError("artifact_already_exists") from exc
            published = True
        os.fsync(parent_fd)
    finally:
        try:
            if temporary is not None:
                try:
                    os.unlink(temporary, dir_fd=parent_fd)
                    if published:
                        os.fsync(parent_fd)
                except FileNotFoundError:
                    pass
        finally:
            os.close(parent_fd)


def _write_calendar_nonposix(path: Path, content: bytes, *, force: bool) -> None:
    # No identity data; Windows has no portable directory fsync/dir_fd custody.
    fd, temporary = tempfile.mkstemp(prefix=".nav-calendar-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if force:
            os.replace(temporary, path)
        else:
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                raise PolicyGenerationError("artifact_already_exists") from exc
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_artifact(
    path: Path,
    content: bytes,
    *,
    force: bool,
    build: bool,
    custody_root: Path | None = None,
) -> None:
    if build and _platform_name() != "posix":
        raise PolicyGenerationError("productive_artifact_requires_posix")
    if build:
        path = _custody_destination(path, custody_root)
    else:
        path = path.expanduser().absolute()
        if not path.parent.is_dir() or path.is_symlink():
            raise PolicyGenerationError("artifact_output_path_invalid")
    if _platform_name() == "posix":
        _write_posix(path, content, force=force)
    else:
        _write_calendar_nonposix(path, content, force=force)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    for mode in ("calendar", "build"):
        cmd = sub.add_parser(mode)
        cmd.add_argument("--coverage-start", type=dt.date.fromisoformat, required=True)
        cmd.add_argument("--coverage-end", type=dt.date.fromisoformat, required=True)
        cmd.add_argument("--output", type=Path, required=True)
        cmd.add_argument("--force", action="store_true")
        if mode == "build":
            cmd.add_argument("--dsn-env", default="NAV_READINESS_DATABASE_URL")
            cmd.add_argument("--custody-root", type=Path, required=True)
            cmd.add_argument("--policy-id", required=True)
            cmd.add_argument("--policy-version", required=True)
    sub.add_parser("verify").add_argument("--policy-file", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.mode == "verify":
            raw = args.policy_file.read_bytes()
            report = verify_artifact(json.loads(raw), raw=raw)
            report["artifact_sha256"] = hashlib.sha256(raw).hexdigest()
        else:
            calendar = build_calendar(args.coverage_start, args.coverage_end)
            if args.mode == "calendar":
                artifact = calendar
            else:
                if not ENV_NAME.fullmatch(args.dsn_env) or not os.environ.get(
                    args.dsn_env
                ):
                    raise PolicyGenerationError("dsn_environment_missing")
                instant, instruments, funds = read_catalog_snapshot(
                    os.environ[args.dsn_env]
                )
                artifact = build_policy(
                    calendar,
                    instruments,
                    funds,
                    instant,
                    args.policy_id,
                    args.policy_version,
                )
            report = verify_artifact(artifact)
            content = canonical_json(artifact)
            write_artifact(
                args.output,
                content,
                force=args.force,
                build=args.mode == "build",
                custody_root=args.custody_root if args.mode == "build" else None,
            )
            report["artifact_sha256"] = hashlib.sha256(content).hexdigest()
        print(json.dumps({"status": "ok", **report}, sort_keys=True))
        return 0
    except (
        PolicyGenerationError,
        ValueError,
        KeyError,
        TypeError,
        OSError,
        psycopg.Error,
    ) as exc:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "reason": type(exc).__name__,
                    "sqlstate": exc.sqlstate
                    if isinstance(exc, psycopg.Error)
                    else None,
                }
            )
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
