"""Bounded, accession-exact SEC API recovery for N-PORT fund-level facts.

This worker is intentionally not a publisher.  It only writes the isolated
recovery tables and never reads N-PORT holdings/raw payloads beyond the compact
identity relation used to determine its expected accession set.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
from collections.abc import Iterator
from typing import Any, Callable

from src.db import connect
from src.nport import secapi_fixed_income as parser

LOGGER = logging.getLogger(__name__)
PRODUCT = "nport_fixed_income_secapi_recovery_v1"
_FUND_TABLE = "nport_fixed_income_secapi_fund_info_v1"
_RATE_TABLE = "nport_fixed_income_secapi_rate_risk_v1"
_MANIFEST_TABLE = "nport_fixed_income_secapi_recovery_v1"
ENV_PUBLICATION_ID = "NPORT_SECAPI_SOURCE_HOLDINGS_PUBLICATION_ID"
ENV_SOURCE_RUN_ID = "NPORT_SECAPI_SOURCE_RUN_ID"
ENV_MAX_ACCESSIONS = "NPORT_SECAPI_MAX_ACCESSIONS"
ENV_MAX_API_CALLS = "NPORT_SECAPI_MAX_API_CALLS"
ENV_REQUEST_INTERVAL_SECONDS = "NPORT_SECAPI_REQUEST_INTERVAL_SECONDS"
SCHEMA_VERSION = "v2"


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be set explicitly for SEC API recovery")
    return value


def _positive_env(name: str) -> int:
    raw = _required_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _positive_float_env(name: str) -> float:
    raw = _required_env(name)
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive number") from exc
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(f"{name} must be a positive number")
    return value


def _default_client() -> Any:
    key = os.getenv("SEC_API_IO_KEY")
    if not key:
        raise RuntimeError("SEC API credential is not configured")
    from sec_api import FormNportApi  # dependency is supplied by the deployment owner

    return FormNportApi(key)


def _status_code(exc: Exception) -> int | None:
    for attr in ("status_code", "status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


def _transient(exc: Exception) -> bool:
    status = _status_code(exc)
    if status is not None:
        return status == 429 or 500 <= status <= 599
    name = type(exc).__name__.lower()
    return "timeout" in name or "connection" in name


def _safe_error(exc: Exception) -> str:
    """A useful but credential-safe terminal reason for structured state."""
    status = _status_code(exc)
    return f"provider_status_{status}" if status is not None else type(exc).__name__


class PostgresRecoveryDb:
    """Narrow SQL boundary; table/column strings live here for schema ownership."""

    def __init__(self, conn: Any):
        self.conn = conn

    def install_schema(self) -> None:
        """Verify the release migration; a recurring job never runs DDL."""
        row = self.conn.execute(
            "SELECT to_regclass('nport_fixed_income_secapi_recovery_v1'), "
            "to_regclass('nport_fixed_income_secapi_fund_info_v1'), "
            "to_regclass('nport_fixed_income_secapi_rate_risk_v1'), "
            "to_regprocedure('nport_fixed_income_secapi_scope_ready(uuid,uuid,text)'), "
            "(SELECT schema_version FROM nport_fixed_income_secapi_schema_versions "
            "WHERE schema_name='nport_fixed_income_secapi_sidecars')"
        ).fetchone()
        if row is None or any(value is None for value in row[:-1]) or row[-1] != SCHEMA_VERSION:
            raise RuntimeError("SEC API sidecar schema migration is not installed")

    def expected_accessions(self, publication_id: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT accession_number FROM nport_holdings_snapshot_identity_v1 "
            "WHERE publication_id=%s AND accession_number IS NOT NULL ORDER BY accession_number",
            (publication_id,),
        ).fetchall()
        return [row[0] for row in rows]

    def successful_accessions(
        self, publication_id: str, source_run_id: str
    ) -> dict[str, tuple[str, str]]:
        rows = self.conn.execute(
            f"SELECT accession_number, provider_response_sha256, extractor_version FROM {_MANIFEST_TABLE} "
            "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND status='success' "
            "AND extractor_version=%s",
            (publication_id, source_run_id, parser.EXTRACTOR_VERSION),
        ).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}

    def terminal_accessions(self, publication_id: str, source_run_id: str) -> dict[str, str]:
        rows = self.conn.execute(
            f"SELECT accession_number, status FROM {_MANIFEST_TABLE} "
            "WHERE source_holdings_publication_id=%s AND source_run_id=%s "
            "AND status IN ('terminal_error','conflict')",
            (publication_id, source_run_id),
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def initialize_manifests(
        self, publication_id: str, source_run_id: str, accessions: list[str]
    ) -> None:
        """Record the deterministic recovery plan before the first HTTP request."""
        document_ids = [parser.source_document_id(publication_id, accession) for accession in accessions]
        self.conn.execute(
            f"INSERT INTO {_MANIFEST_TABLE} "
            "(source_holdings_publication_id,source_run_id,accession_number,source_document_id,"
            " source_row_number,extractor_version,status) "
            "SELECT %s,%s,planned.accession_number,planned.source_document_id,0,%s,'pending' "
            "FROM unnest(%s::text[],%s::uuid[]) "
            "AS planned(accession_number,source_document_id) "
            "ON CONFLICT (source_holdings_publication_id,source_run_id,accession_number) DO NOTHING",
            (
                publication_id,
                source_run_id,
                parser.EXTRACTOR_VERSION,
                accessions,
                document_ids,
            ),
        )

    @contextlib.contextmanager
    def advisory_lock(self, publication_id: str, source_run_id: str) -> Iterator[bool]:
        # Session lock, intentionally outside an SQL transaction that includes HTTP.
        key = f"{PRODUCT}|{publication_id}|{source_run_id}"
        acquired = bool(
            self.conn.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (key,)
            ).fetchone()[0]
        )
        try:
            yield acquired
        finally:
            if acquired:
                self.conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (key,)
                )

    def transaction(self):
        return self.conn.transaction()

    def existing_hash(
        self, publication_id: str, source_run_id: str, accession_number: str
    ) -> tuple[str, str] | None:
        row = self.conn.execute(
            f"SELECT provider_response_sha256, extractor_version FROM {_MANIFEST_TABLE} "
            "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number=%s AND status='success'",
            (publication_id, source_run_id, accession_number),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def record_failure(
        self,
        publication_id: str,
        source_run_id: str,
        accession_number: str,
        *,
        status: str,
        attempt_increment: int,
        provider_http_status: int | None,
    ) -> None:
        if status not in {"retryable_error", "terminal_error"}:
            raise ValueError("invalid persisted recovery failure status")
        self.conn.execute(
            f"UPDATE {_MANIFEST_TABLE} SET status=%s, "
            "attempt_count=attempt_count+%s, provider_http_status=%s "
            "WHERE source_holdings_publication_id=%s AND source_run_id=%s "
            "AND accession_number=%s AND status IN ('pending','retryable_error')",
            (
                status,
                attempt_increment,
                provider_http_status,
                publication_id,
                source_run_id,
                accession_number,
            ),
        )

    def write(self, projection: parser.FilingProjection) -> None:
        # The parent manifest is the idempotency record.  Projection tables retain
        # no provider response and, particularly, no invstOrSecs position payload.
        source = projection.fund
        metadata = json.dumps(
            {"form_type": json.loads(projection.compact_json).get("formType")}
        )
        self.conn.execute(
            f"INSERT INTO {_MANIFEST_TABLE} "
            "(source_holdings_publication_id,source_run_id,accession_number,source_document_id,"
            " source_row_number,extractor_version,status,payload_sha256,provider_response_sha256,provider_metadata) "
            "VALUES(%s,%s,%s,%s,0,%s,'success',%s,%s,%s::jsonb) "
            "ON CONFLICT (source_holdings_publication_id,source_run_id,accession_number) DO UPDATE "
            "SET source_document_id=EXCLUDED.source_document_id,source_row_number=0,"
            f"attempt_count={_MANIFEST_TABLE}.attempt_count+1,"
            "extractor_version=EXCLUDED.extractor_version,status='success',payload_sha256=EXCLUDED.payload_sha256,"
            "provider_response_sha256=EXCLUDED.provider_response_sha256,provider_metadata=EXCLUDED.provider_metadata "
            f"WHERE {_MANIFEST_TABLE}.status <> 'success'",
            (
                source["source_holdings_publication_id"],
                source["source_run_id"],
                projection.accession_number,
                projection.source_document_id,
                projection.extractor_version,
                projection.response_sha256,
                projection.response_sha256,
                metadata,
            ),
        )
        fund = {
            "source_holdings_publication_id": source["source_holdings_publication_id"],
            "source_run_id": source["source_run_id"],
            "accession_number": projection.accession_number,
            "source_document_id": projection.source_document_id,
            "source_row_number": 0,
            "extractor_version": projection.extractor_version,
            "payload_sha256": projection.response_sha256,
            "projection_sha256": parser.response_sha256(
                {
                    "compact": projection.compact_json,
                    "presence": projection.fund_presence,
                }
            ),
            "compact_payload": projection.compact_json,
            "presence_map": json.dumps(projection.fund_presence),
            "cur_metric_state": source["cur_metric_state"],
            "cur_metric_count": source["cur_metric_count"],
            "total_assets": source["tot_assets"],
            "total_liabilities": source["tot_liabs"],
            "net_assets": source["net_assets"],
            "borrowing_pay_within_1yr": source["amt_pay_one_yr_banks_borr"],
            "ctrld_companies_pay_within_1yr": source["amt_pay_one_yr_ctrld_comp"],
            "other_affilia_pay_within_1yr": source["amt_pay_one_yr_oth_affil"],
            "other_pay_within_1yr": source["amt_pay_one_yr_other"],
            "borrowing_pay_after_1yr": source["amt_pay_aft_one_yr_banks_borr"],
            "ctrld_companies_pay_after_1yr": source["amt_pay_aft_one_yr_ctrld_comp"],
            "other_affilia_pay_after_1yr": source["amt_pay_aft_one_yr_oth_affil"],
            "other_pay_after_1yr": source["amt_pay_aft_one_yr_other"],
            "delayed_delivery": source["delay_deliv"],
            "standby_commitment": source["stand_by_commit"],
            "cash_not_rptd_in_c_or_d": source["csh_not_rptd_in_cor_d"],
        }
        for source_prefix, target in (
            ("credit_sprd_risk_invst_grade", "invest"),
            ("credit_sprd_risk_non_invst_grade", "noninvest"),
        ):
            for suffix in ("3mon", "1yr", "5yr", "10yr", "30yr"):
                fund[f"credit_spread_{suffix}_{target}"] = source[
                    f"{source_prefix}_{suffix}"
                ]
        columns = tuple(fund)
        self.conn.execute(
            f"INSERT INTO {_FUND_TABLE} ({','.join(columns)}) VALUES ({','.join('%s' for _ in columns)}) "
            "ON CONFLICT DO NOTHING",
            tuple(fund[column] for column in columns),
        )
        for rate in projection.rates:
            payload = {
                "source_holdings_publication_id": rate[
                    "source_holdings_publication_id"
                ],
                "source_run_id": rate["source_run_id"],
                "accession_number": projection.accession_number,
                "source_document_id": projection.source_document_id,
                "source_row_number": rate["source_row_number"],
                "provider_ordinal": rate["provider_ordinal"],
                "provider_rate_risk_id": rate["provider_rate_risk_id"],
                "extractor_version": projection.extractor_version,
                "currency_code": rate["currency_code"],
                "payload_sha256": projection.response_sha256,
                "projection_sha256": parser.response_sha256(rate),
                "compact_payload": json.dumps({"currency_code": rate["currency_code"]}),
                "presence_map": json.dumps(rate["presence"]),
            }
            for prefix in ("dv01", "dv100"):
                for suffix in ("3mon", "1yr", "5yr", "10yr", "30yr"):
                    payload[f"{prefix}_{suffix}"] = rate[f"{prefix}_{suffix}"]
            columns = tuple(payload)
            self.conn.execute(
                f"INSERT INTO {_RATE_TABLE} ({','.join(columns)}) VALUES ({','.join('%s' for _ in columns)}) "
                "ON CONFLICT DO NOTHING",
                tuple(payload[column] for column in columns),
            )


def run(
    dsn: str | None = None,
    *,
    publication_id: str | None = None,
    source_run_id: str | None = None,
    max_accessions: int | None = None,
    max_api_calls: int | None = None,
    request_interval_seconds: float | None = None,
    dry_run: bool = False,
    calc_date: str | None = None,
    limit: int | None = None,
    db: Any | None = None,
    client_factory: Callable[[], Any] | None = None,
    sleeper: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> dict[str, Any]:
    """Recover a bounded prefix, one exact accession per provider call.

    ``max_accessions`` and ``max_api_calls`` must be positive explicit safety
    ceilings.  A dry-run initializes/reads no provider client and reports only
    deterministic identity-set progress.
    """
    if calc_date is not None or limit is not None:
        raise ValueError(
            "SEC API recovery requires explicit NPORT_SECAPI_* scope and budget parameters; "
            "--calc-date/--limit are not valid"
        )
    service_invocation = publication_id is None
    publication_id = publication_id or _required_env(ENV_PUBLICATION_ID)
    source_run_id = source_run_id or _required_env(ENV_SOURCE_RUN_ID)
    max_accessions = (
        max_accessions
        if max_accessions is not None
        else (_positive_env(ENV_MAX_ACCESSIONS) if service_invocation else 1)
    )
    max_api_calls = (
        max_api_calls
        if max_api_calls is not None
        else (_positive_env(ENV_MAX_API_CALLS) if service_invocation else 1)
    )
    request_interval_seconds = (
        request_interval_seconds
        if request_interval_seconds is not None
        else (
            _positive_float_env(ENV_REQUEST_INTERVAL_SECONDS)
            if service_invocation
            else 1.0
        )
    )
    if max_accessions < 1 or max_api_calls < 1:
        raise ValueError("max_accessions and max_api_calls must be positive")
    if not math.isfinite(request_interval_seconds) or request_interval_seconds <= 0:
        raise ValueError("request_interval_seconds must be positive")
    if db is None:
        conn = connect(dsn, autocommit=True)
        with conn:
            return _run_with_db(
                conn,
                publication_id,
                source_run_id,
                max_accessions,
                max_api_calls,
                request_interval_seconds,
                dry_run,
                client_factory,
                sleeper,
                clock,
            )
    return _run_with_db(
        db,
        publication_id,
        source_run_id,
        max_accessions,
        max_api_calls,
        request_interval_seconds,
        dry_run,
        client_factory,
        sleeper,
        clock,
    )


def _run_with_db(
    db_or_conn: Any,
    publication_id: str,
    source_run_id: str,
    max_accessions: int,
    max_api_calls: int,
    request_interval_seconds: float,
    dry_run: bool,
    client_factory: Callable[[], Any] | None,
    sleeper: Callable[[float], None] | None,
    clock: Callable[[], float] | None,
) -> dict[str, Any]:
    db = (
        db_or_conn
        if hasattr(db_or_conn, "expected_accessions")
        else PostgresRecoveryDb(db_or_conn)
    )
    installer = getattr(db, "install_schema", None)
    if installer is not None:
        installer()
    expected = db.expected_accessions(publication_id)
    # Relation is an identity set. Duplication signals an upstream integrity error;
    # doing two provider calls for the same filing would violate the contract.
    if len(expected) != len(set(expected)):
        raise RuntimeError("expected accession identity set contains duplicates")
    success = db.successful_accessions(publication_id, source_run_id)
    terminal_reader = getattr(db, "terminal_accessions", None)
    terminal = terminal_reader(publication_id, source_run_id) if terminal_reader else {}
    pending = [
        accession
        for accession in expected
        if accession not in success and accession not in terminal
    ]
    base = {
        "expected": len(expected),
        "success": len(success),
        "pending": len(pending),
        "remaining": len(pending),
        "max_accessions": max_accessions,
        "max_api_calls": max_api_calls,
        "request_interval_seconds": request_interval_seconds,
    }
    if terminal and not pending:
        return {
            "state": "blocked",
            **base,
            "terminal": len(terminal),
            "accession_number": sorted(terminal)[0],
        }
    if dry_run:
        return {"state": "dry_run", **base}
    with db.advisory_lock(publication_id, source_run_id) as acquired:
        if not acquired:
            return {"state": "locked", **base}
        initializer = getattr(db, "initialize_manifests", None)
        if initializer is not None:
            initializer(publication_id, source_run_id, expected)
        try:
            client = (client_factory or _default_client)()
        except Exception as exc:
            return {
                "state": "failed",
                "reason": _safe_error(exc),
                **base,
                "processed": 0,
                "api_calls": 0,
            }
        sleep = sleeper or __import__("time").sleep
        now = clock or __import__("time").monotonic
        api_calls = processed = successful_processed = 0
        previous_request_at: float | None = None
        for accession in pending:
            if processed >= max_accessions or api_calls >= max_api_calls:
                break
            if previous_request_at is not None:
                sleep(
                    max(
                        0.0,
                        request_interval_seconds - (now() - previous_request_at),
                    )
                )
            previous_request_at = now()
            existing = db.existing_hash(publication_id, source_run_id, accession)
            if existing is not None:
                # A successful result from a different extractor is immutable.
                # Do not spend another API call trying to overwrite its provenance.
                if not isinstance(existing, tuple) or existing[1] != parser.EXTRACTOR_VERSION:
                    return {
                        "state": "conflict",
                        "accession_number": accession,
                        **base,
                        "processed": processed,
                        "api_calls": api_calls,
                    }
            try:
                calls_before = api_calls

                def record_call() -> None:
                    nonlocal api_calls
                    api_calls += 1

                response = _fetch_with_retry(
                    client,
                    accession,
                    sleep,
                    max_calls=max_api_calls - api_calls,
                    on_call=record_call,
                )
                projection = parser.extract_filing(
                    response, publication_id=publication_id, source_run_id=source_run_id
                )
            except Exception as exc:
                # Never include provider exception text: SDK errors can echo URLs/keys.
                recorder = getattr(db, "record_failure", None)
                if recorder is not None:
                    failure_status = (
                        "retryable_error" if _transient(exc) else "terminal_error"
                    )
                    with db.transaction():
                        recorder(
                            publication_id,
                            source_run_id,
                            accession,
                            status=failure_status,
                            attempt_increment=max(1, api_calls - calls_before),
                            provider_http_status=_status_code(exc),
                        )
                    if failure_status == "terminal_error":
                        terminal[accession] = failure_status
                        processed += 1
                        continue
                return {
                    "state": "failed",
                    "accession_number": accession,
                    "reason": _safe_error(exc),
                    **base,
                    "processed": processed,
                    "api_calls": api_calls,
                }
            fingerprint = (projection.response_sha256, projection.extractor_version)
            if existing is not None:
                if existing == fingerprint:
                    processed += 1
                    successful_processed += 1
                    continue
                return {
                    "state": "conflict",
                    "accession_number": accession,
                    **base,
                    "processed": processed,
                    "api_calls": api_calls,
                }
            with db.transaction():
                db.write(projection)
            processed += 1
            successful_processed += 1
        remaining = max(0, len(pending) - processed)
        return {
            "state": (
                "blocked"
                if terminal
                else ("complete" if not remaining else "partial")
            ),
            "expected": len(expected),
            "success": len(success) + successful_processed,
            "pending": len(pending),
            "remaining": remaining,
            "max_accessions": max_accessions,
            "max_api_calls": max_api_calls,
            "processed": processed,
            "api_calls": api_calls,
            "terminal": len(terminal),
        }


def _fetch_with_retry(
    client: Any,
    accession: str,
    sleeper: Callable[[float], None],
    *,
    max_calls: int = 3,
    on_call: Callable[[], None] | None = None,
) -> Any:
    provider_calls = 0

    def record_provider_call() -> None:
        nonlocal provider_calls
        if provider_calls >= max_calls:
            raise RuntimeError("SEC API call budget exhausted")
        provider_calls += 1
        if on_call is not None:
            on_call()

    def call() -> Any:
        return parser.fetch_exact_filing(
            client, accession, on_provider_call=record_provider_call
        )

    # This local loop deliberately calls only the one exact search expression;
    # no generic SDK retry can alter query scope or paginate on our behalf.
    if max_calls < 1:
        raise RuntimeError("SEC API call budget exhausted")
    attempts = 3
    for attempt in range(attempts):
        try:
            return call()
        except (parser.AccessionMismatchError, parser.PayloadError):
            raise
        except Exception as exc:
            if (
                not _transient(exc)
                or attempt + 1 == attempts
                or provider_calls >= max_calls
            ):
                raise
            retry_after = parser._retry_after(exc)
            sleeper(retry_after if retry_after is not None else float(2**attempt))
    raise AssertionError("unreachable")
