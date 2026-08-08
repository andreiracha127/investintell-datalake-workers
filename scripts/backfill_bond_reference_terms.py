"""Resumable direct Finnhub bond-reference-terms backfill.

Reads the curated CUSIP9 universe from PostgreSQL, calls ``/bond/profile``
through the shared paced/retrying client, and writes terms plus an attempt
ledger directly to PostgreSQL. It has no profile-file, parquet, JSON cache, or
local-output path.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any, Iterator
from uuid import UUID, NAMESPACE_URL, uuid5

from psycopg.types.json import Jsonb

from src import db
from src.workers import _finnhub


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "bond_reference_terms.sql"
RUN_NAMESPACE = uuid5(NAMESPACE_URL, "investintell/bond-reference-terms/finnhub")
MAX_LIMIT = 1_000
_TERM_COLUMNS = (
    "cusip9", "isin", "coupon_rate", "coupon_type", "maturity_date", "issue_date",
    "seniority", "secured", "day_count", "payment_frequency", "callable", "amount_outstanding_mm",
    "amount_outstanding_vendor", "asset", "asset_type", "bond_type", "dated_date",
    "debt_type", "figi", "first_coupon_date", "industry_group", "industry_sub_group",
    "offering_price_vendor", "original_offering_vendor",
)

_UPSERT_SQL = """
INSERT INTO bond_reference_terms (
    cusip9, isin, coupon_rate, coupon_type, maturity_date, issue_date, seniority,
    secured, day_count, payment_frequency, callable, amount_outstanding_mm,
    amount_outstanding_vendor, asset, asset_type, bond_type, dated_date, debt_type,
    figi, first_coupon_date, industry_group, industry_sub_group, offering_price_vendor,
    original_offering_vendor, batch_label, finnhub_run_id, finnhub_profile_state,
    finnhub_reason_code, finnhub_fetched_at, finnhub_source_lineage
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
ON CONFLICT (cusip9) DO UPDATE SET
    isin = COALESCE(bond_reference_terms.isin, EXCLUDED.isin),
    coupon_rate = COALESCE(bond_reference_terms.coupon_rate, EXCLUDED.coupon_rate),
    coupon_type = COALESCE(bond_reference_terms.coupon_type, EXCLUDED.coupon_type),
    maturity_date = COALESCE(bond_reference_terms.maturity_date, EXCLUDED.maturity_date),
    issue_date = COALESCE(bond_reference_terms.issue_date, EXCLUDED.issue_date),
    seniority = COALESCE(bond_reference_terms.seniority, EXCLUDED.seniority),
    secured = COALESCE(bond_reference_terms.secured, EXCLUDED.secured),
    day_count = COALESCE(bond_reference_terms.day_count, EXCLUDED.day_count),
    payment_frequency = COALESCE(bond_reference_terms.payment_frequency, EXCLUDED.payment_frequency),
    callable = COALESCE(bond_reference_terms.callable, EXCLUDED.callable),
    amount_outstanding_mm = COALESCE(bond_reference_terms.amount_outstanding_mm, EXCLUDED.amount_outstanding_mm),
    amount_outstanding_vendor = COALESCE(bond_reference_terms.amount_outstanding_vendor, EXCLUDED.amount_outstanding_vendor),
    asset = COALESCE(bond_reference_terms.asset, EXCLUDED.asset),
    asset_type = COALESCE(bond_reference_terms.asset_type, EXCLUDED.asset_type),
    bond_type = COALESCE(bond_reference_terms.bond_type, EXCLUDED.bond_type),
    dated_date = COALESCE(bond_reference_terms.dated_date, EXCLUDED.dated_date),
    debt_type = COALESCE(bond_reference_terms.debt_type, EXCLUDED.debt_type),
    figi = COALESCE(bond_reference_terms.figi, EXCLUDED.figi),
    first_coupon_date = COALESCE(bond_reference_terms.first_coupon_date, EXCLUDED.first_coupon_date),
    industry_group = COALESCE(bond_reference_terms.industry_group, EXCLUDED.industry_group),
    industry_sub_group = COALESCE(bond_reference_terms.industry_sub_group, EXCLUDED.industry_sub_group),
    offering_price_vendor = COALESCE(bond_reference_terms.offering_price_vendor, EXCLUDED.offering_price_vendor),
    original_offering_vendor = COALESCE(bond_reference_terms.original_offering_vendor, EXCLUDED.original_offering_vendor),
    finnhub_run_id = EXCLUDED.finnhub_run_id,
    finnhub_profile_state = EXCLUDED.finnhub_profile_state,
    finnhub_reason_code = EXCLUDED.finnhub_reason_code,
    finnhub_fetched_at = EXCLUDED.finnhub_fetched_at,
    finnhub_source_lineage = EXCLUDED.finnhub_source_lineage
"""


def _text(value: object) -> str | None:
    value = str(value).strip() if value is not None else ""
    return value or None


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _date(value: object) -> str | None:
    raw = _text(value)
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


def _boolean(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    return None


def _secured(debt_type: object) -> str | None:
    text = (_text(debt_type) or "").lower()
    if "unsecured" in text:
        return "unsecured"
    if "secured" in text:
        return "secured"
    return None


def profile_identity_basis(cusip9: str, profile: dict[str, Any]) -> str:
    """Return the fail-closed identity proof accepted for one profile response."""
    if not isinstance(profile, dict) or not profile:
        raise _finnhub.FinnhubProfileError("empty_profile")
    requested = cusip9.strip().upper()
    if not re.fullmatch(r"[0-9A-Z]{9}", requested):
        raise ValueError("invalid_requested_cusip")
    returned = (_text(profile.get("cusip")) or "").upper()
    if returned:
        if returned != requested:
            raise ValueError("cusip_mismatch")
        return "returned_cusip"
    isin = (_text(profile.get("isin")) or "").upper()
    if len(isin) >= 11 and isin[:2] in {"US", "CA"}:
        if isin[2:11] != requested:
            raise ValueError("isin_cusip_mismatch")
        return "isin_embedded_cusip9"
    raise ValueError("missing_identity_evidence")


def profile_to_terms(cusip9: str, profile: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Map only documented profile fields; do not invent an amount unit or factor."""
    identity_basis = profile_identity_basis(cusip9, profile)
    requested = cusip9.strip().upper()
    if not requested:
        raise ValueError("cusip_mismatch")
    debt_type = _text(profile.get("debtType"))
    return {
        "cusip9": requested, "isin": _text(profile.get("isin")),
        "coupon_rate": _number(profile.get("coupon")),
        "coupon_type": _text(profile.get("couponType")),
        "maturity_date": _date(profile.get("maturityDate")),
        "issue_date": _date(profile.get("issueDate")),
        "seniority": _text(profile.get("securityLevel")), "secured": _secured(debt_type),
        "day_count": _text(profile.get("dayCount")),
        "payment_frequency": _text(profile.get("paymentFrequency")),
        "callable": _boolean(profile.get("callable")),
        # Match the established August loader convention; also preserve the raw
        # vendor magnitude in the neutral column for future unit provenance.
        "amount_outstanding_mm": _number(profile.get("amountOutstanding")),
        "amount_outstanding_vendor": _number(profile.get("amountOutstanding")),
        "asset": _text(profile.get("asset")), "asset_type": _text(profile.get("assetType")),
        "bond_type": _text(profile.get("bondType")), "dated_date": _date(profile.get("datedDate")),
        "debt_type": debt_type, "figi": _text(profile.get("figi")),
        "first_coupon_date": _date(profile.get("firstCouponDate")),
        # Vendor taxonomy only; this is never FF17 or a model input.
        "industry_group": _text(profile.get("industryGroup")),
        "industry_sub_group": _text(profile.get("industrySubGroup")),
        "offering_price_vendor": _number(profile.get("offeringPrice")),
        "original_offering_vendor": _number(profile.get("originalOffering")),
    }, identity_basis


def install_schema(conn: Any) -> None:
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _lineage(run_id: UUID, identity_basis: str | None = None) -> Jsonb:
    lineage: dict[str, str] = {
        "vendor": "finnhub", "endpoint": "/bond/profile", "run_id": str(run_id),
    }
    if identity_basis is not None:
        lineage["identity_basis"] = identity_basis
    return Jsonb(lineage)


def _ensure_run(conn: Any, run_id: UUID, batch_label: str) -> str | None:
    conn.execute(
        "INSERT INTO bond_reference_terms_finnhub_run "
        "(run_id,batch_label,source_lineage) VALUES (%s,%s,%s) ON CONFLICT (run_id) DO NOTHING",
        (run_id, batch_label, _lineage(run_id)),
    )
    row = conn.execute(
        "SELECT resume_cursor FROM bond_reference_terms_finnhub_run WHERE run_id=%s", (run_id,)
    ).fetchone()
    return row[0] if row else None


def _window(conn: Any, cursor: str | None, limit: int,
            stale_after_days: int) -> list[tuple[str, bool]]:
    rows = conn.execute(
        """
        WITH cursor_window AS (
            SELECT c.cusip9 FROM bond_curated_universe c
            WHERE c.cusip9 > %s ORDER BY c.cusip9 LIMIT %s
        )
        SELECT w.cusip9,
               (r.cusip9 IS NOT NULL AND r.finnhub_profile_state = 'success'
                AND r.finnhub_fetched_at >= now() - %s::interval) AS already_complete
        FROM cursor_window w
        LEFT JOIN bond_reference_terms r ON r.cusip9=w.cusip9
        ORDER BY w.cusip9
        """,
        (cursor or "", limit, f"{stale_after_days} days"),
    ).fetchall()
    return [(row[0], bool(row[1]) if len(row) > 1 else False) for row in rows]


def _record_attempt(conn: Any, run_id: UUID, cusip9: str, fetched_at: datetime,
                    state: str, reason: str, identity_basis: str | None = None) -> None:
    conn.execute(
        "INSERT INTO bond_reference_terms_finnhub_attempt "
        "(run_id,cusip9,fetched_at,profile_state,reason_code,source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id,cusip9,fetched_at) DO NOTHING",
        (run_id, cusip9, fetched_at, state, reason, _lineage(run_id, identity_basis)),
    )


def _store_terms(conn: Any, terms: dict[str, Any], batch_label: str, run_id: UUID,
                 fetched_at: datetime, identity_basis: str) -> None:
    values = tuple(terms[column] for column in _TERM_COLUMNS) + (
        batch_label, run_id, "success", identity_basis, fetched_at,
        _lineage(run_id, identity_basis),
    )
    conn.execute(_UPSERT_SQL, values)


def _advance_cursor(conn: Any, run_id: UUID, cusip9: str) -> None:
    conn.execute(
        "UPDATE bond_reference_terms_finnhub_run SET resume_cursor=%s, updated_at=now() WHERE run_id=%s",
        (cusip9, run_id),
    )


def run_batch(conn: Any, client: Any, *, batch_label: str, limit: int,
              stale_after_days: int = 30, run_id: UUID | None = None) -> dict[str, Any]:
    """Fetch bounded unresolved/stale CUSIPs; commit each cursor update for resume."""
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    if stale_after_days < 1:
        raise ValueError("stale_after_days must be positive")
    resolved_run_id = run_id or uuid5(RUN_NAMESPACE, batch_label)
    cursor_before = _ensure_run(conn, resolved_run_id, batch_label)
    counters: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    cursor_after = cursor_before
    for cusip9, already_complete in _window(conn, cursor_before, limit, stale_after_days):
        if already_complete:
            counters["already_complete"] += 1
            _advance_cursor(conn, resolved_run_id, cusip9)
            conn.commit()
            cursor_after = cusip9
            continue
        counters["attempted"] += 1
        fetched_at = datetime.now(timezone.utc)
        state, reason = "success", "success"
        identity_basis: str | None = None
        try:
            profile = client.profile_by_cusip(cusip9)
            terms, identity_basis = profile_to_terms(cusip9, profile)
        except _finnhub.FinnhubProfileError:
            state, reason = "empty", "empty_profile"
            counters["empty"] += 1
        except ValueError as exc:
            state, reason = "refused", str(exc)
            counters["mismatch"] += 1
        except _finnhub.FinnhubTransientError:
            state, reason = "transient", "transient_error"
            counters["transient"] += 1
        except _finnhub.FinnhubConfigError:
            state, reason = "config_error", "config_error"
            counters["config_error"] += 1
        except Exception:
            # The shared client wraps normal network/provider failures, but an
            # adapter regression must still leave a typed DB checkpoint rather
            # than making the run look like a successful no-data response.
            state, reason = "transient", "unexpected_error"
            counters["transient"] += 1
        else:
            _store_terms(conn, terms, batch_label, resolved_run_id, fetched_at, identity_basis)
            counters["loaded"] += 1
            reason = identity_basis
        _record_attempt(conn, resolved_run_id, cusip9, fetched_at, state, reason, identity_basis)
        reasons[reason] += 1
        if state in {"transient", "config_error"}:
            conn.commit()
            break
        _advance_cursor(conn, resolved_run_id, cusip9)
        conn.commit()
        cursor_after = cusip9
    return {
        "run_id": str(resolved_run_id), "batch_label": batch_label,
        "attempted": counters["attempted"], "loaded": counters["loaded"],
        "already_complete": counters["already_complete"], "empty": counters["empty"], "mismatch": counters["mismatch"],
        "transient": counters["transient"], "config_error": counters["config_error"],
        "reason_counts": dict(sorted(reasons.items())), "cursor_before": cursor_before,
        "cursor_after": cursor_after, "resume_cursor": cursor_after, "limit": limit,
    }


def run(client: Any, *, dsn: str | None = None, batch_label: str, limit: int,
        stale_after_days: int = 30, run_id: UUID | None = None) -> dict[str, Any]:
    """Install idempotent DDL and execute a direct DB batch through ``src.db``."""
    with db.connect(dsn) as conn:
        install_schema(conn)
        summary = run_batch(conn, client, batch_label=batch_label, limit=limit,
                            stale_after_days=stale_after_days, run_id=run_id)
        conn.commit()
        return summary


def _env_file_key(path: Path) -> str | None:
    """Read only FINNHUB_API_KEY from an explicitly named dotenv-style file."""
    for raw in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"\s*(?:export\s+)?FINNHUB_API_KEY\s*=\s*(.*?)\s*$", raw)
        if match:
            return match.group(1).strip().strip('"').strip("'") or None
    return None


@contextmanager
def _client_from_optional_env_file(env_file: Path | None) -> Iterator[_finnhub.FinnhubClient]:
    previous = os.environ.get("FINNHUB_API_KEY")
    injected = False
    if not previous and env_file is not None:
        value = _env_file_key(env_file)
        if value:
            os.environ["FINNHUB_API_KEY"] = value
            injected = True
    try:
        yield _finnhub.client_from_env()
    finally:
        if injected:
            os.environ.pop("FINNHUB_API_KEY", None)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="optional target DSN; defaults to DATABASE_URL")
    parser.add_argument("--env-file", type=Path, default=None,
                        help="optional dotenv file; only FINNHUB_API_KEY is read")
    parser.add_argument("--batch-label", default=date.today().isoformat())
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--stale-after-days", type=int, default=30)
    parser.add_argument("--run-id", type=UUID, default=None)
    args = parser.parse_args(argv)
    try:
        with _client_from_optional_env_file(args.env_file) as client:
            summary = run(client, dsn=args.dsn, batch_label=args.batch_label, limit=args.limit,
                          stale_after_days=args.stale_after_days, run_id=args.run_id)
    except _finnhub.FinnhubConfigError:
        summary = {
            "run_id": str(args.run_id or uuid5(RUN_NAMESPACE, args.batch_label)),
            "batch_label": args.batch_label,
            "attempted": 0, "loaded": 0, "already_complete": 0, "empty": 0,
            "mismatch": 0, "transient": 0, "config_error": 1,
            "reason_counts": {"config_error": 1}, "cursor_before": None,
            "cursor_after": None, "resume_cursor": None,
        }
        print(json.dumps(summary, sort_keys=True))
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 2 if any(summary.get(key, 0) for key in ("empty", "mismatch", "transient", "config_error")) else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
