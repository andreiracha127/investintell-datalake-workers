"""Append-only v2 Render fallback for immutable v1 N-PORT terminal gaps.

This worker never alters v1 evidence or activates serving.  It can only add a
v2 overlay after the Form API has returned an exact zero result and the same
accession is resolved through the verified Query -> Render chain.
"""

from __future__ import annotations

import contextlib
import hashlib
import math
import os
import time
from collections.abc import Iterator
from typing import Any, Callable

from src.db import connect
from src.nport import secapi_fixed_income as parser


PRODUCT = "nport_fixed_income_secapi_fallback_v2"
PARSER_VERSION = "nport-secapi-fixed-income/v2"
RESOLVER_VERSION = "secapi-query-render/v1"
SCHEMA_VERSION = "v2"
_MANIFEST_TABLE = "nport_fixed_income_secapi_fallback_manifest_v2"
_FUND_TABLE = "nport_fixed_income_secapi_fallback_fund_info_v2"
_RATE_TABLE = "nport_fixed_income_secapi_fallback_rate_risk_v2"
ENV_PUBLICATION_ID = "NPORT_SECAPI_SOURCE_HOLDINGS_PUBLICATION_ID"
ENV_SOURCE_RUN_ID = "NPORT_SECAPI_SOURCE_RUN_ID"
ENV_MAX_ACCESSIONS = "NPORT_SECAPI_FALLBACK_MAX_ACCESSIONS"
ENV_MAX_API_CALLS = "NPORT_SECAPI_FALLBACK_MAX_API_CALLS"
ENV_REQUEST_INTERVAL_SECONDS = "NPORT_SECAPI_FALLBACK_REQUEST_INTERVAL_SECONDS"


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be set explicitly for the SEC API fallback")
    return value


def _positive_env(name: str) -> int:
    try:
        value = int(_required_env(name))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _positive_float_env(name: str) -> float:
    try:
        value = float(_required_env(name))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive number") from exc
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(f"{name} must be a positive number")
    return value


def _default_client() -> parser.ExactNportClient:
    key = os.getenv("SEC_API_IO_KEY")
    if not key:
        raise RuntimeError("SEC API credential is not configured")
    return parser.build_exact_nport_client(key)


def _safe_error(exc: Exception) -> str:
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    return f"provider_status_{status}" if isinstance(status, int) else type(exc).__name__


def _raw_sha256(raw: str | bytes) -> str:
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if not isinstance(raw, bytes):
        raise parser.PayloadError("Render API raw response is not text or bytes")
    return hashlib.sha256(raw).hexdigest()


def evidence_hashes(
    evidence: parser.RenderFallbackEvidence, projection: parser.FilingProjection
) -> dict[str, str]:
    """Return independently attributable evidence hashes; never return raw XML."""
    return {
        "form_nport_response_sha256": parser.response_sha256(evidence.form_response),
        "query_response_sha256": parser.response_sha256(evidence.query_response),
        "render_raw_sha256": _raw_sha256(evidence.render_raw),
        "compact_payload_sha256": hashlib.sha256(
            projection.compact_json.encode("utf-8")
        ).hexdigest(),
    }


class PostgresFallbackDb:
    """Narrow SQL boundary for the v2 overlay; this worker never installs DDL."""

    def __init__(self, conn: Any):
        self.conn = conn

    def install_schema(self) -> None:
        row = self.conn.execute(
            "SELECT to_regclass('nport_fixed_income_secapi_recovery_v1'), "
            "to_regclass('nport_fixed_income_secapi_fallback_manifest_v2'), "
            "to_regclass('nport_fixed_income_secapi_fallback_fund_info_v2'), "
            "to_regclass('nport_fixed_income_secapi_fallback_rate_risk_v2'), "
            "to_regprocedure('nport_fixed_income_secapi_fallback_document_id_v2(uuid,uuid,text)'), "
            "(SELECT schema_version FROM nport_fixed_income_secapi_fallback_schema_versions "
            "WHERE schema_name='nport_fixed_income_secapi_fallback')"
        ).fetchone()
        if row is None or any(value is None for value in row[:-1]) or row[-1] != SCHEMA_VERSION:
            raise RuntimeError("SEC API fallback v2 schema migration is not installed")

    def terminal_accessions(self, publication_id: str, source_run_id: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT r.accession_number FROM nport_fixed_income_secapi_recovery_v1 r "
            "WHERE r.source_holdings_publication_id=%s AND r.source_run_id=%s "
            "AND r.status='terminal_error' AND NOT EXISTS ("
            f"SELECT 1 FROM {_MANIFEST_TABLE} m WHERE "
            "(m.source_holdings_publication_id,m.source_run_id,m.accession_number)="
            "(r.source_holdings_publication_id,r.source_run_id,r.accession_number)) "
            "ORDER BY r.accession_number",
            (publication_id, source_run_id),
        ).fetchall()
        return [row[0] for row in rows]

    @contextlib.contextmanager
    def advisory_lock(self, publication_id: str, source_run_id: str) -> Iterator[bool]:
        key = f"{PRODUCT}|{publication_id}|{source_run_id}"
        acquired = bool(self.conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (key,)
        ).fetchone()[0])
        try:
            yield acquired
        finally:
            if acquired:
                self.conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (key,)
                )

    def transaction(self):
        return self.conn.transaction()

    def existing_hashes(
        self, publication_id: str, source_run_id: str, accession_number: str
    ) -> dict[str, str] | None:
        row = self.conn.execute(
            f"SELECT form_nport_response_sha256,query_response_sha256,render_raw_sha256,"
            f"compact_payload_sha256,parser_version,resolver_version FROM {_MANIFEST_TABLE} "
            "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number=%s",
            (publication_id, source_run_id, accession_number),
        ).fetchone()
        if row is None:
            return None
        return dict(zip((
            "form_nport_response_sha256", "query_response_sha256", "render_raw_sha256",
            "compact_payload_sha256", "parser_version", "resolver_version",
        ), row, strict=True))

    def write(
        self, projection: parser.FilingProjection, evidence: parser.RenderFallbackEvidence
    ) -> None:
        hashes = evidence_hashes(evidence, projection)
        source = projection.fund
        manifest = self.conn.execute(
            f"INSERT INTO {_MANIFEST_TABLE} (source_holdings_publication_id,source_run_id,accession_number,"
            "source_document_id,parser_version,resolver_version,document_url,"
            "form_nport_response_sha256,query_response_sha256,render_raw_sha256,compact_payload_sha256) "
            "VALUES(%s,%s,%s,nport_fixed_income_secapi_fallback_document_id_v2(%s,%s,%s),%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING RETURNING source_document_id",
            (source["source_holdings_publication_id"], source["source_run_id"], projection.accession_number,
             source["source_holdings_publication_id"], source["source_run_id"], projection.accession_number,
             PARSER_VERSION, RESOLVER_VERSION, evidence.document_url,
             hashes["form_nport_response_sha256"], hashes["query_response_sha256"],
             hashes["render_raw_sha256"], hashes["compact_payload_sha256"]),
        ).fetchone()
        if manifest is None:
            existing = self.existing_hashes(
                str(source["source_holdings_publication_id"]), str(source["source_run_id"]), projection.accession_number
            )
            expected = {**hashes, "parser_version": PARSER_VERSION, "resolver_version": RESOLVER_VERSION}
            if existing != expected:
                raise RuntimeError("immutable SEC API fallback overlay hash conflict")
            return
        document_id = manifest[0]
        fund = {
            "source_holdings_publication_id": source["source_holdings_publication_id"],
            "source_run_id": source["source_run_id"], "accession_number": projection.accession_number,
            "source_document_id": document_id, "parser_version": PARSER_VERSION,
            "resolver_version": RESOLVER_VERSION, "compact_payload_sha256": hashes["compact_payload_sha256"],
            "projection_sha256": parser.response_sha256({"compact": projection.compact_json, "presence": projection.fund_presence}),
            "compact_payload": projection.compact_json, "presence_map": parser.canonical_json(projection.fund_presence),
            "cur_metric_state": source["cur_metric_state"], "cur_metric_count": source["cur_metric_count"],
            "total_assets": source["tot_assets"], "total_liabilities": source["tot_liabs"], "net_assets": source["net_assets"],
            "borrowing_pay_within_1yr": source["amt_pay_one_yr_banks_borr"], "ctrld_companies_pay_within_1yr": source["amt_pay_one_yr_ctrld_comp"],
            "other_affilia_pay_within_1yr": source["amt_pay_one_yr_oth_affil"], "other_pay_within_1yr": source["amt_pay_one_yr_other"],
            "borrowing_pay_after_1yr": source["amt_pay_aft_one_yr_banks_borr"], "ctrld_companies_pay_after_1yr": source["amt_pay_aft_one_yr_ctrld_comp"],
            "other_affilia_pay_after_1yr": source["amt_pay_aft_one_yr_oth_affil"], "other_pay_after_1yr": source["amt_pay_aft_one_yr_other"],
            "delayed_delivery": source["delay_deliv"], "standby_commitment": source["stand_by_commit"], "cash_not_rptd_in_c_or_d": source["csh_not_rptd_in_cor_d"],
        }
        for source_prefix, target in (("credit_sprd_risk_invst_grade", "invest"), ("credit_sprd_risk_non_invst_grade", "noninvest")):
            for suffix in ("3mon", "1yr", "5yr", "10yr", "30yr"):
                fund[f"credit_spread_{suffix}_{target}"] = source[f"{source_prefix}_{suffix}"]
        columns = tuple(fund)
        placeholders = ",".join(
            "%s::jsonb" if column in {"compact_payload", "presence_map"} else "%s"
            for column in columns
        )
        self.conn.execute(
            f"INSERT INTO {_FUND_TABLE} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
            tuple(fund[column] for column in columns),
        )
        for rate in projection.rates:
            payload: dict[str, Any] = {
                "source_holdings_publication_id": source["source_holdings_publication_id"], "source_run_id": source["source_run_id"],
                "accession_number": projection.accession_number, "source_document_id": document_id,
                "parser_version": PARSER_VERSION, "resolver_version": RESOLVER_VERSION,
                "provider_ordinal": rate["provider_ordinal"], "provider_rate_risk_id": rate["provider_rate_risk_id"],
                "currency_code": rate["currency_code"], "compact_payload_sha256": hashes["compact_payload_sha256"],
                "projection_sha256": parser.response_sha256(rate),
                "compact_payload": parser.canonical_json({"currency_code": rate["currency_code"]}),
                "presence_map": parser.canonical_json(rate["presence"]),
            }
            for prefix in ("dv01", "dv100"):
                for suffix in ("3mon", "1yr", "5yr", "10yr", "30yr"):
                    payload[f"{prefix}_{suffix}"] = rate[f"{prefix}_{suffix}"]
            columns = tuple(payload)
            placeholders = ",".join(
                "%s::jsonb" if column in {"compact_payload", "presence_map"} else "%s"
                for column in columns
            )
            self.conn.execute(
                f"INSERT INTO {_RATE_TABLE} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                tuple(payload[column] for column in columns),
            )


def run(
    dsn: str | None = None, *, publication_id: str | None = None, source_run_id: str | None = None,
    max_accessions: int | None = None, max_api_calls: int | None = None,
    request_interval_seconds: float | None = None, dry_run: bool = False,
    calc_date: str | None = None, limit: int | None = None, db: Any | None = None,
    client_factory: Callable[[], Any] | None = None, sleeper: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> dict[str, Any]:
    """Recover a bounded terminal-error scope; one complete chain costs three calls."""
    if calc_date is not None or limit is not None:
        raise ValueError("SEC API fallback requires its explicit scope and budget variables")
    service = publication_id is None
    publication_id = publication_id or _required_env(ENV_PUBLICATION_ID)
    source_run_id = source_run_id or _required_env(ENV_SOURCE_RUN_ID)
    max_accessions = max_accessions if max_accessions is not None else (_positive_env(ENV_MAX_ACCESSIONS) if service else 1)
    max_api_calls = max_api_calls if max_api_calls is not None else (_positive_env(ENV_MAX_API_CALLS) if service else 3)
    request_interval_seconds = request_interval_seconds if request_interval_seconds is not None else (_positive_float_env(ENV_REQUEST_INTERVAL_SECONDS) if service else 1.0)
    if max_accessions < 1 or max_api_calls < 1 or not math.isfinite(request_interval_seconds) or request_interval_seconds <= 0:
        raise ValueError("fallback budgets and request interval must be positive")
    if db is None:
        conn = connect(dsn, autocommit=True)
        with conn:
            return _run_with_db(conn, publication_id, source_run_id, max_accessions, max_api_calls, request_interval_seconds, dry_run, client_factory, sleeper, clock)
    return _run_with_db(db, publication_id, source_run_id, max_accessions, max_api_calls, request_interval_seconds, dry_run, client_factory, sleeper, clock)


def _run_with_db(db_or_conn: Any, publication_id: str, source_run_id: str, max_accessions: int,
                 max_api_calls: int, request_interval_seconds: float, dry_run: bool,
                 client_factory: Callable[[], Any] | None, sleeper: Callable[[float], None] | None,
                 clock: Callable[[], float] | None) -> dict[str, Any]:
    db = db_or_conn if hasattr(db_or_conn, "terminal_accessions") else PostgresFallbackDb(db_or_conn)
    db.install_schema()
    candidates = db.terminal_accessions(publication_id, source_run_id)
    if len(candidates) != len(set(candidates)):
        raise RuntimeError("terminal fallback accession set contains duplicates")
    base = {"terminal": len(candidates), "max_accessions": max_accessions, "max_api_calls": max_api_calls,
            "request_interval_seconds": request_interval_seconds}
    if dry_run:
        return {"state": "dry_run", **base, "remaining": len(candidates)}
    with db.advisory_lock(publication_id, source_run_id) as acquired:
        if not acquired:
            return {"state": "locked", **base, "remaining": len(candidates)}
        candidates = db.terminal_accessions(publication_id, source_run_id)
        if not candidates:
            return {"state": "complete", **base, "success": 0, "processed": 0, "api_calls": 0, "remaining": 0}
        try:
            client = (client_factory or _default_client)()
        except Exception as exc:
            return {"state": "failed", "reason": _safe_error(exc), **base, "processed": 0, "api_calls": 0, "remaining": len(candidates)}
        sleep, now = sleeper or time.sleep, clock or time.monotonic
        api_calls = processed = success = 0
        previous_request_at: float | None = None
        for accession in candidates:
            existing = db.existing_hashes(publication_id, source_run_id, accession)
            if existing is not None:
                if existing.get("parser_version") != PARSER_VERSION or existing.get("resolver_version") != RESOLVER_VERSION:
                    return {"state": "conflict", "accession_number": accession, **base, "processed": processed, "success": success, "api_calls": api_calls, "remaining": len(candidates) - processed}
                processed += 1
                success += 1
                continue
            if processed >= max_accessions or api_calls + 3 > max_api_calls:
                break

            def record_call() -> None:
                nonlocal api_calls, previous_request_at
                if api_calls >= max_api_calls:
                    raise RuntimeError("SEC API fallback call budget exhausted")
                if previous_request_at is not None:
                    sleep(max(0.0, request_interval_seconds - (now() - previous_request_at)))
                previous_request_at = now()
                api_calls += 1

            try:
                evidence = parser.fetch_render_fallback_evidence(client, accession, on_provider_call=record_call)
                projection = parser.extract_filing(evidence.filing, publication_id=publication_id, source_run_id=source_run_id, extractor_version=PARSER_VERSION)
                with db.transaction():
                    db.write(projection, evidence)
            except Exception as exc:
                return {"state": "failed", "accession_number": accession, "reason": _safe_error(exc), **base,
                        "processed": processed, "success": success, "api_calls": api_calls, "remaining": len(candidates) - processed}
            processed += 1
            success += 1
        remaining = len(candidates) - processed
        return {"state": "complete" if not remaining else "partial", **base, "processed": processed,
                "success": success, "api_calls": api_calls, "remaining": remaining}
