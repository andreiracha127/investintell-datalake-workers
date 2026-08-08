"""Small, fail-closed parser for SEC API N-PORT fund-level recovery data.

The provider response is deliberately kept in memory only.  The returned
projection cannot contain ``invstOrSecs`` (the position-level payload).
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping

PROVIDER = "sec-api.io"
EXTRACTOR_VERSION = "nport-secapi-fixed-income/v1"
_NAMESPACE = uuid.UUID("3d266eaa-1baa-552c-8e73-ed3676b1ed7f")
_POSITION_KEYS = frozenset(("invstOrSecs", "invstOrSec", "investments", "holdings"))
_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_PERIODS = (
    ("period3Mon", "3mon"),
    ("period1Yr", "1yr"),
    ("period5Yr", "5yr"),
    ("period10Yr", "10yr"),
    ("period30Yr", "30yr"),
)
_FUND_NUMBERS = {
    "totAssets": "tot_assets",
    "totLiabs": "tot_liabs",
    "netAssets": "net_assets",
    "amtPayOneYrBanksBorr": "amt_pay_one_yr_banks_borr",
    "amtPayOneYrCtrldComp": "amt_pay_one_yr_ctrld_comp",
    "amtPayOneYrOthAffil": "amt_pay_one_yr_oth_affil",
    "amtPayOneYrOther": "amt_pay_one_yr_other",
    "amtPayAftOneYrBanksBorr": "amt_pay_aft_one_yr_banks_borr",
    "amtPayAftOneYrCtrldComp": "amt_pay_aft_one_yr_ctrld_comp",
    "amtPayAftOneYrOthAffil": "amt_pay_aft_one_yr_oth_affil",
    "amtPayAftOneYrOther": "amt_pay_aft_one_yr_other",
    "delayDeliv": "delay_deliv",
    "standByCommit": "stand_by_commit",
    "cshNotRptdInCorD": "csh_not_rptd_in_cor_d",
}


class PayloadError(ValueError):
    """The provider returned a shape that cannot be safely projected."""


class AccessionMismatchError(PayloadError):
    """A search response was not exactly the requested accession."""


@dataclass(frozen=True)
class FilingProjection:
    accession_number: str
    response_sha256: str
    source_document_id: str
    extractor_version: str
    compact_json: str
    fund: dict[str, Any]
    fund_presence: dict[str, str]
    rates: list[dict[str, Any]]


def canonical_json(value: Any) -> str:
    """Canonical JSON used only for in-memory hash calculation."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )


def response_sha256(response: Any) -> str:
    return hashlib.sha256(canonical_json(response).encode("utf-8")).hexdigest()


def source_document_id(publication_id: str, accession_number: str) -> str:
    return str(
        uuid.uuid5(_NAMESPACE, f"{PROVIDER}|{publication_id}|{accession_number}")
    )


def _presence(data: Mapping[str, Any], key: str) -> str:
    return "missing" if key not in data else "null" if data[key] is None else "present"


def parse_decimal(value: Any, *, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise PayloadError(f"{field}: boolean is not numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise PayloadError(f"{field}: non-finite numeric")
    if not isinstance(value, (int, float, Decimal, str)):
        raise PayloadError(f"{field}: invalid numeric type")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PayloadError(f"{field}: invalid numeric") from exc
    if not number.is_finite():
        raise PayloadError(f"{field}: non-finite numeric")
    return number


def _period_values(
    container: Mapping[str, Any], *, prefix: str
) -> tuple[dict[str, Decimal | None], dict[str, str]]:
    values: dict[str, Decimal | None] = {}
    presence: dict[str, str] = {}
    for provider_key, suffix in _PERIODS:
        dest = f"{prefix}_{suffix}"
        presence[dest] = _presence(container, provider_key)
        values[dest] = parse_decimal(container.get(provider_key), field=provider_key)
    return values, presence


def _mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PayloadError(f"{field}: expected object")
    return value


def _compact(response: Mapping[str, Any]) -> dict[str, Any]:
    if any(key in response for key in _POSITION_KEYS):
        # Do not copy a response then delete positions: recursively bounded output
        # makes accidental future position persistence materially harder.
        response = {
            key: value for key, value in response.items() if key not in _POSITION_KEYS
        }
    fund = response.get("fundInfo")
    if not isinstance(fund, Mapping):
        raise PayloadError("fundInfo: expected object")
    return {
        "accessionNo": response.get("accessionNo"),
        "formType": response.get("formType"),
        "fundInfo": fund,
    }


def extract_filing(
    response: Mapping[str, Any], *, publication_id: str, source_run_id: str
) -> FilingProjection:
    if not isinstance(response, Mapping):
        raise PayloadError("response: expected object")
    accession = response.get("accessionNo")
    if not isinstance(accession, str) or not accession:
        raise PayloadError("accessionNo: required string")
    fund_info = _mapping(response.get("fundInfo"), field="fundInfo")
    fund: dict[str, Any] = {
        "source_holdings_publication_id": publication_id,
        "source_run_id": source_run_id,
        "accession_number": accession,
        "source_row_number": 0,
    }
    presence: dict[str, str] = {}
    for raw, dest in _FUND_NUMBERS.items():
        fund[dest] = parse_decimal(fund_info.get(raw), field=raw)
        presence[dest] = _presence(fund_info, raw)
    for raw, dest in (
        ("creditSprdRiskInvstGrade", "credit_sprd_risk_invst_grade"),
        ("creditSprdRiskNonInvstGrade", "credit_sprd_risk_non_invst_grade"),
    ):
        presence[dest] = _presence(fund_info, raw)
        child = fund_info.get(raw)
        if child is None:
            for _key, suffix in _PERIODS:
                fund[f"{dest}_{suffix}"] = None
                presence[f"{dest}_{suffix}"] = (
                    "missing" if raw not in fund_info else "null"
                )
        else:
            child_map = _mapping(child, field=raw)
            values, subpresence = _period_values(child_map, prefix=dest)
            fund.update(values)
            presence.update(subpresence)
    metrics_presence = _presence(fund_info, "curMetrics")
    presence["cur_metrics"] = metrics_presence
    metrics = fund_info.get("curMetrics")
    rows: list[Any]
    if metrics is None:
        rows = []
        metric_state = "missing" if "curMetrics" not in fund_info else "null"
    else:
        metric_map = _mapping(metrics, field="curMetrics")
        if "curMetric" not in metric_map:
            rows = []
            metric_state = "empty"
        else:
            rows = metric_map["curMetric"]
            if not isinstance(rows, list):
                raise PayloadError("curMetrics.curMetric: expected array")
            metric_state = "present" if rows else "empty"
    fund["cur_metric_state"] = metric_state
    fund["cur_metric_count"] = len(rows)
    rates: list[dict[str, Any]] = []
    for ordinal, entry in enumerate(rows, start=1):
        metric = _mapping(entry, field="curMetrics.curMetric")
        currency = metric.get("curCd")
        if not isinstance(currency, str) or not currency:
            raise PayloadError("curMetrics.curMetric.curCd: required string")
        rate: dict[str, Any] = {
            "source_holdings_publication_id": publication_id,
            "source_run_id": source_run_id,
            "accession_number": accession,
            "source_row_number": ordinal,
            "currency_code": currency,
            "provider_ordinal": ordinal - 1,
            "provider_rate_risk_id": f"{accession}:{ordinal}",
        }
        rate_presence: dict[str, str] = {}
        for raw, prefix in (
            ("intrstRtRiskdv01", "dv01"),
            ("intrstRtRiskdv100", "dv100"),
        ):
            child = metric.get(raw)
            if child is None:
                for _key, suffix in _PERIODS:
                    rate[f"{prefix}_{suffix}"] = None
                    rate_presence[f"{prefix}_{suffix}"] = (
                        "missing" if raw not in metric else "null"
                    )
            else:
                values, subpresence = _period_values(
                    _mapping(child, field=raw), prefix=prefix
                )
                rate.update(values)
                rate_presence.update(subpresence)
        rate["presence"] = rate_presence
        rates.append(rate)
    document_id = source_document_id(publication_id, accession)
    compact = _compact(response)
    # A final explicit guard ensures no future compact projection silently leaks holdings.
    encoded = canonical_json(compact)
    if any(re.search(rf'"{re.escape(key)}"', encoded) for key in _POSITION_KEYS):
        raise PayloadError("compact projection contains position data")
    fund["source_document_id"] = document_id
    for rate in rates:
        rate["source_document_id"] = document_id
    return FilingProjection(
        accession,
        response_sha256(response),
        document_id,
        EXTRACTOR_VERSION,
        encoded,
        fund,
        presence,
        rates,
    )


def fetch_exact_filing(client: Any, accession_number: str) -> Mapping[str, Any]:
    if not _ACCESSION_RE.fullmatch(accession_number):
        raise PayloadError("accession number has an invalid SEC format")
    query = f'accessionNo:"{accession_number}"'
    response = client.get_data(
        {
            "query": query,
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "asc"}}],
        }
    )
    if not isinstance(response, Mapping):
        raise AccessionMismatchError("SEC API response is not an object")
    records = response.get("filings") or response.get("data") or []
    if not isinstance(records, list) or len(records) != 1:
        raise AccessionMismatchError("SEC API did not return exactly one filing")
    record = records[0]
    if not isinstance(record, Mapping) or record.get("accessionNo") != accession_number:
        raise AccessionMismatchError("SEC API accession mismatch")
    return record


def _retry_after(exc: Exception) -> float | None:
    value = getattr(exc, "retry_after", None)
    if value is None and hasattr(exc, "response"):
        headers = getattr(exc.response, "headers", {})
        value = headers.get("Retry-After") if headers else None
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def retry_transient(
    operation: Callable[[], Any],
    *,
    sleeper: Callable[[float], None],
    max_attempts: int = 3,
    transient: Iterable[type[Exception]] = (),
) -> Any:
    retryable = tuple(transient)
    for attempt in range(max_attempts):
        try:
            return operation()
        except retryable as exc:
            if attempt + 1 >= max_attempts:
                raise
            sleeper(
                _retry_after(exc)
                if _retry_after(exc) is not None
                else float(2**attempt)
            )
    raise AssertionError("unreachable")
