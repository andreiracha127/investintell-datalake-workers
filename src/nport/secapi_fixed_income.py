"""Small, fail-closed parser for SEC API N-PORT fund-level recovery data.

The provider response is deliberately kept in memory only.  The returned
projection cannot contain ``invstOrSecs`` (the position-level payload).
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlsplit, urlunsplit

PROVIDER = "sec-api.io"
EXTRACTOR_VERSION = "nport-secapi-fixed-income/v1"
_NAMESPACE = uuid.UUID("3d266eaa-1baa-552c-8e73-ed3676b1ed7f")
_POSITION_KEYS = frozenset(("invstOrSecs", "invstOrSec", "investments", "holdings"))
_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_DETAIL_PATH_RE = re.compile(
    r"^/Archives/edgar/data/\d+/\d{18}/xslFormNPORT-P_X01/primary_doc\.xml$"
)
MAX_RENDER_XML_BYTES = 64 * 1024 * 1024
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


@dataclass(frozen=True)
class RenderFallbackEvidence:
    """Ephemeral Form -> Query -> Render evidence for the v2 overlay.

    The raw Render document is deliberately available only while the worker
    computes its digest and compact projection.  Database writers must persist
    its digest, never this field.
    """

    filing: Mapping[str, Any]
    form_response: Mapping[str, Any]
    query_response: Mapping[str, Any]
    render_raw: str | bytes
    document_url: str


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_value(element: ET.Element) -> Any:
    children = list(element)
    if not children:
        value = (element.text or "").strip()
        return value or None
    grouped: dict[str, list[Any]] = {}
    for child in children:
        grouped.setdefault(_local_name(child.tag), []).append(_xml_value(child))
    return {
        name: values if name == "curMetric" or len(values) > 1 else values[0]
        for name, values in grouped.items()
    }


def _fund_info_from_render_xml(document: str | bytes) -> tuple[str, Mapping[str, Any]]:
    raw = document.encode("utf-8") if isinstance(document, str) else document
    if not isinstance(raw, bytes):
        raise PayloadError("Render API response is not text or bytes")
    if len(raw) > MAX_RENDER_XML_BYTES:
        raise PayloadError("Render API XML exceeds the bounded payload size")
    upper_prefix = raw[:4096].upper()
    if b"<!DOCTYPE" in upper_prefix or b"<!ENTITY" in upper_prefix:
        raise PayloadError("Render API XML contains a forbidden declaration")
    submission_type: str | None = None
    fund_info: Mapping[str, Any] | None = None
    try:
        for _event, element in ET.iterparse(io.BytesIO(raw), events=("end",)):
            name = _local_name(element.tag)
            if name == "submissionType":
                if submission_type is not None:
                    raise PayloadError("Render API XML has duplicate submissionType")
                submission_type = (element.text or "").strip()
            elif name == "fundInfo":
                if fund_info is not None:
                    raise PayloadError("Render API XML has duplicate fundInfo")
                value = _xml_value(element)
                if not isinstance(value, Mapping):
                    raise PayloadError("Render API fundInfo is not an object")
                fund_info = value
                break
    except ET.ParseError as exc:
        raise PayloadError("Render API XML is malformed") from exc
    if submission_type != "NPORT-P":
        raise AccessionMismatchError("Render API form type mismatch")
    if fund_info is None:
        raise PayloadError("Render API XML has no fundInfo")
    return submission_type, fund_info


class ExactNportClient:
    """Exact N-PORT retrieval with a bounded Render fallback for dataset gaps."""

    def __init__(self, form_client: Any, query_client: Any, render_client: Any):
        self._form = form_client
        self._query = query_client
        self._render = render_client

    def fetch_exact_filing(
        self,
        accession_number: str,
        *,
        on_provider_call: Callable[[], None] | None = None,
        invoke_provider_call: Callable[[Callable[[], Any]], Any] | None = None,
    ) -> Mapping[str, Any]:
        def invoke(operation: Callable[[], Any]) -> Any:
            if invoke_provider_call is not None:
                return invoke_provider_call(operation)
            if on_provider_call is not None:
                on_provider_call()
            return operation()

        payload = {
            "query": f'accessionNo:"{accession_number}"',
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "asc"}}],
        }
        response = invoke(lambda: self._form.get_data(payload))
        if not isinstance(response, Mapping):
            raise AccessionMismatchError("SEC API response is not an object")
        records = response.get("filings") or response.get("data") or []
        if not isinstance(records, list):
            raise AccessionMismatchError("SEC API filings is not an array")
        if records:
            return _validate_exact_record(records, accession_number)

        resolved = invoke(lambda: self._query.get_filings({
                "query": {
                    "query_string": {
                        "query": f'accessionNo:"{accession_number}"'
                    }
                },
                "from": "0",
                "size": "2",
                "sort": [{"filedAt": {"order": "asc"}}],
            }))
        if not isinstance(resolved, Mapping):
            raise AccessionMismatchError("SEC API resolver response is not an object")
        filing = _validate_exact_record(
            resolved.get("filings") or [], accession_number
        )
        detail_url = filing.get("linkToFilingDetails")
        if not isinstance(detail_url, str):
            raise AccessionMismatchError("SEC API resolver has no filing URL")
        parts = urlsplit(detail_url)
        if (
            parts.scheme != "https"
            or parts.netloc != "www.sec.gov"
            or not _DETAIL_PATH_RE.fullmatch(parts.path)
            or parts.query
            or parts.fragment
        ):
            raise AccessionMismatchError("SEC API resolver filing URL is not canonical")
        raw_path = parts.path.replace("/xslFormNPORT-P_X01/", "/")
        raw_url = urlunsplit((parts.scheme, parts.netloc, raw_path, "", ""))
        document = invoke(lambda: self._render.get_file(raw_url))
        form_type, fund_info = _fund_info_from_render_xml(document)
        return {
            "accessionNo": accession_number,
            "formType": form_type,
            "fundInfo": fund_info,
            "retrievalMode": "query-render-xml",
        }

    def fetch_render_fallback_evidence(
        self,
        accession_number: str,
        *,
        on_provider_call: Callable[[], None] | None = None,
        invoke_provider_call: Callable[[Callable[[], Any]], Any] | None = None,
    ) -> RenderFallbackEvidence:
        """Resolve only a verified Form API exact-zero gap through Render.

        This is intentionally separate from ``fetch_exact_filing``: callers
        that write the v2 overlay need the three independent response hashes
        and canonical SEC document URL, while callers of v1 must not gain an
        alternate persistence path.
        """
        def invoke(operation: Callable[[], Any]) -> Any:
            if invoke_provider_call is not None:
                return invoke_provider_call(operation)
            if on_provider_call is not None:
                on_provider_call()
            return operation()

        payload = {
            "query": f'accessionNo:"{accession_number}"',
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "asc"}}],
        }
        response = invoke(lambda: self._form.get_data(payload))
        if not isinstance(response, Mapping):
            raise AccessionMismatchError("SEC API response is not an object")
        records = response.get("filings") or response.get("data") or []
        if not isinstance(records, list):
            raise AccessionMismatchError("SEC API filings is not an array")
        if records:
            raise AccessionMismatchError("Form API exact search was not zero")

        resolved = invoke(lambda: self._query.get_filings({
                "query": {"query_string": {"query": f'accessionNo:"{accession_number}"'}},
                "from": "0",
                "size": "2",
                "sort": [{"filedAt": {"order": "asc"}}],
            }))
        if not isinstance(resolved, Mapping):
            raise AccessionMismatchError("SEC API resolver response is not an object")
        filing = _validate_exact_record(resolved.get("filings") or [], accession_number)
        detail_url = filing.get("linkToFilingDetails")
        if not isinstance(detail_url, str):
            raise AccessionMismatchError("SEC API resolver has no filing URL")
        parts = urlsplit(detail_url)
        if (
            parts.scheme != "https"
            or parts.netloc != "www.sec.gov"
            or not _DETAIL_PATH_RE.fullmatch(parts.path)
            or parts.query
            or parts.fragment
        ):
            raise AccessionMismatchError("SEC API resolver filing URL is not canonical")
        raw_path = parts.path.replace("/xslFormNPORT-P_X01/", "/")
        raw_url = urlunsplit((parts.scheme, parts.netloc, raw_path, "", ""))
        document = invoke(lambda: self._render.get_file(raw_url))
        form_type, fund_info = _fund_info_from_render_xml(document)
        return RenderFallbackEvidence(
            filing={"accessionNo": accession_number, "formType": form_type, "fundInfo": fund_info},
            form_response=response,
            query_response=resolved,
            render_raw=document,
            document_url=raw_url,
        )


def build_exact_nport_client(api_key: str) -> ExactNportClient:
    from sec_api import FormNportApi, QueryApi, RenderApi

    return ExactNportClient(
        FormNportApi(api_key), QueryApi(api_key=api_key), RenderApi(api_key)
    )


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
    response: Mapping[str, Any], *, publication_id: str, source_run_id: str,
    extractor_version: str = EXTRACTOR_VERSION,
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
        extractor_version,
        encoded,
        fund,
        presence,
        rates,
    )


def _validate_exact_record(records: Any, accession_number: str) -> Mapping[str, Any]:
    if not isinstance(records, list) or len(records) != 1:
        raise AccessionMismatchError("SEC API did not return exactly one filing")
    record = records[0]
    if not isinstance(record, Mapping) or record.get("accessionNo") != accession_number:
        raise AccessionMismatchError("SEC API accession mismatch")
    if record.get("formType") != "NPORT-P":
        raise AccessionMismatchError("SEC API form type mismatch")
    return record


def fetch_exact_filing(
    client: Any,
    accession_number: str,
    *,
    on_provider_call: Callable[[], None] | None = None,
) -> Mapping[str, Any]:
    if not _ACCESSION_RE.fullmatch(accession_number):
        raise PayloadError("accession number has an invalid SEC format")
    exact_fetch = getattr(client, "fetch_exact_filing", None)
    if exact_fetch is not None:
        return exact_fetch(
            accession_number, on_provider_call=on_provider_call
        )
    query = f'accessionNo:"{accession_number}"'
    if on_provider_call is not None:
        on_provider_call()
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
    return _validate_exact_record(records, accession_number)


def fetch_render_fallback_evidence(
    client: Any,
    accession_number: str,
    *,
    on_provider_call: Callable[[], None] | None = None,
    invoke_provider_call: Callable[[Callable[[], Any]], Any] | None = None,
) -> RenderFallbackEvidence:
    """Fetch v2-only fallback evidence without permitting a generic client path."""
    if not _ACCESSION_RE.fullmatch(accession_number):
        raise PayloadError("accession number has an invalid SEC format")
    fetch = getattr(client, "fetch_render_fallback_evidence", None)
    if fetch is None:
        raise PayloadError("SEC API client does not support the verified Render fallback")
    evidence = fetch(
        accession_number,
        on_provider_call=on_provider_call,
        invoke_provider_call=invoke_provider_call,
    )
    if not isinstance(evidence, RenderFallbackEvidence):
        raise PayloadError("SEC API fallback client returned invalid evidence")
    return evidence


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
