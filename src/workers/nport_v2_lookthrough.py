"""Governed, artifact-only runner for V2 N-PORT look-through anchors.

This module deliberately has no database import or SQL surface.  It consumes
immutable JSON artifacts and writes only explicitly selected external output.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Callable


class ArtifactValidationError(ValueError):
    """An input artifact did not meet the governed runner contract."""


_FORM_COUNTS = {"nport": 26, "ncen": 17, "rr1": 39}
SHADOW_TABLE_ALLOWLIST = (
    "nport_v2_lookthrough_exposures_shadow",
    "nport_v2_lookthrough_summary_shadow",
)
_PHASE4_SCHEMA = "phase4_completion_manifest/v1"
_INPUT_SCHEMA = "nport_v2_input_artifact/v1"
_ANCHOR_SCHEMA = "nport_v2_lookthrough_anchor/v1"
_RUN_SCHEMA = "nport_v2_lookthrough_run/v1"
MAX_DEPTH = 2
DERIVATIVE_CLASSES = {"DE", "DFE", "DFF", "DIR", "DCO", "DCR", "DO"}
SYNTHETIC_PREFIXES = ("IS:", "LE:", "H:", "CIK:")
UNIDENTIFIED_PREFIXES = ("LE:", "H:", "CIK:")
_EQUITY_CATS = frozenset({"EC", "EP"})
_DERIVATIVE_CATS = frozenset({"DE", "DFE", "DCR", "DIR", "DO"})
_FI_STRUCTURE_LABELS = {"ABS-MBS": "Mortgage-Backed (MBS)", "ABS-O": "Asset-Backed (ABS)", "ABS-CBDO": "CLO/CDO", "LON": "Bank Loans", "STIV": "Short-Term / Cash"}
_DEBT_ISSUER_LABELS = {"CORP": "Corporate Debt", "UST": "U.S. Treasury", "USGA": "U.S. Agency", "USGSE": "U.S. Agency", "MUN": "Municipal", "NUSS": "Sovereign (ex-US)"}
_DEBT_ISSUER_CODES = frozenset({"UST", "USGA", "USGSE", "MUN", "NUSS"})
_ISO_ALPHA2 = frozenset("AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW".split())


@dataclass(frozen=True)
class EquityInputs:
    gross_equity_pct: float
    net_equity_pct: float
    country_exposures_pct: dict[str, float]
    signed_holding_weights_pct: dict[str, float] = field(default_factory=dict)
    gross_holding_weights_pct: dict[str, float] = field(default_factory=dict)


def _embedded_cusip9(isin: str | None) -> str | None:
    return isin[2:11] if isin and len(isin) == 12 and isin.startswith("US") else None


def match_fund(holding: dict[str, Any], fund_map: dict[str, dict[str, str]]) -> str | None:
    cusip, isin = holding.get("cusip") or "", holding.get("isin")
    if cusip.startswith(UNIDENTIFIED_PREFIXES):
        return None
    if cusip.startswith("IS:"):
        isin, cusip = isin or cusip[3:], ""
    if cusip and (series := fund_map["cusip"].get(cusip)):
        return series
    if isin and (series := fund_map["isin"].get(isin)):
        return series
    if isin and (embedded := _embedded_cusip9(isin)):
        return fund_map["cusip"].get(embedded)
    return None


def issuer_key(cusip: str | None, isin: str | None) -> str:
    value = cusip or ""
    if value and not value.startswith(SYNTHETIC_PREFIXES):
        return value[:6]
    if value.startswith("IS:"):
        embedded = _embedded_cusip9(value[3:])
        return embedded[:6] if embedded else value
    if value:
        return value
    embedded = _embedded_cusip9(isin)
    return embedded[:6] if embedded else (f"IS:{isin}" if isin else "UNKNOWN")


def _valid_isin(value: str) -> bool:
    if len(value) != 12 or not value.isalnum() or value[:2] not in _ISO_ALPHA2:
        return False
    digits = "".join(char if char.isdigit() else str(ord(char) - 55) for char in value)
    total = 0
    for index, char in enumerate(reversed(digits)):
        digit = int(char) * (2 if index % 2 else 1)
        total += digit // 10 + digit % 10
    return total % 10 == 0


def equity_country_key(isin: str | None, asset_class: str | None, cusip: str | None = None, investment_country: str | None = None) -> str | None:
    if (asset_class or "") not in _EQUITY_CATS:
        return None
    explicit = (investment_country or "").strip().upper()
    if explicit:
        return explicit if explicit in _ISO_ALPHA2 else "UNKNOWN"
    candidates = [(isin or "").strip().upper()]
    if (cusip or "").strip().upper().startswith("IS:"):
        candidates.append((cusip or "").strip().upper()[3:])
    return next((candidate[:2] for candidate in candidates if _valid_isin(candidate)), "UNKNOWN")


def sector_label(holding: dict[str, Any], sector_map: dict[str, str]) -> str:
    asset, issuer = (holding.get("asset_class") or "").strip().upper(), (holding.get("sector") or "").strip().upper()
    if asset in _EQUITY_CATS:
        return sector_map.get(issuer_key(holding.get("cusip"), holding.get("isin"))) or sector_map.get((holding.get("isin") or "").strip().upper()) or "Unclassified"
    if asset in _FI_STRUCTURE_LABELS:
        return _FI_STRUCTURE_LABELS[asset]
    if asset == "DBT":
        return _DEBT_ISSUER_LABELS.get(issuer, "Debt")
    if asset in _DERIVATIVE_CATS:
        return "Derivatives"
    if asset == "RA":
        return "Repo"
    return _DEBT_ISSUER_LABELS[issuer] if issuer in _DEBT_ISSUER_CODES else "Other"


def expand_series(series_id: str, get_holdings: Callable[[str], tuple[date, list[dict[str, Any]]] | None], fund_map: dict[str, dict[str, str]], *, sector_map: dict[str, str] | None = None, get_equity_inputs: Callable[[str, date], EquityInputs | None] | None = None, max_depth: int = MAX_DEPTH) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    """Pure recursive V2 expansion, deliberately independent of DB/session code."""
    sector_map = sector_map or {}
    root = get_holdings(series_id)
    if root is None:
        raise LookupError(f"no holdings for root series {series_id}")
    root_date, root_holdings = root
    exposures: dict[tuple[str, str], dict[str, Any]] = {}
    summary: dict[str, Any] = {"report_date": root_date, "oldest_report_date": root_date, "direct_pct": 0.0, "indirect_pct": 0.0, "expanded_fund_pct": 0.0, "nondecomposable_fund_pct": 0.0, "derivatives_gross_pct": 0.0, "derivatives_net_pct": 0.0, "gross_equity_pct": 0.0, "net_equity_pct": 0.0, "unidentified_pct": 0.0, "n_holdings": len(root_holdings), "n_children_expanded": 0}
    expanded_children: set[str] = set()

    def accumulate(holding: dict[str, Any], pct: float, depth: int, has_equity_rollup: bool) -> None:
        side = "direct_pct" if depth == 0 else "indirect_pct"
        keys: list[tuple[str, str, str | None]] = [("issuer", issuer_key(holding.get("cusip"), holding.get("isin")), holding.get("issuer_name")), ("asset_class", holding.get("asset_class") or "UNKNOWN", None), ("sector", sector_label(holding, sector_map), None), ("currency", holding.get("currency") or "UNKNOWN", None)]
        if not has_equity_rollup and (country := equity_country_key(holding.get("isin"), holding.get("asset_class"), holding.get("cusip"), holding.get("investment_country"))) is not None:
            keys.append(("country", country, None))
        for dimension, key, label in keys:
            cell = exposures.setdefault((dimension, key), {"label": None, "direct_pct": 0.0, "indirect_pct": 0.0})
            cell[side] += pct
            if label and not cell["label"]:
                cell["label"] = label
        summary[side] += pct
        if (holding.get("asset_class") or "") in DERIVATIVE_CLASSES:
            summary["derivatives_gross_pct"] += abs(pct)
            summary["derivatives_net_pct"] += pct
        if not has_equity_rollup and (holding.get("asset_class") or "") in _EQUITY_CATS:
            summary["gross_equity_pct"] += abs(pct)
            summary["net_equity_pct"] += pct
        if (holding.get("cusip") or "").startswith(UNIDENTIFIED_PREFIXES):
            summary["unidentified_pct"] += abs(pct)

    def weight_key(holding: dict[str, Any]) -> str:
        value = (holding.get("cusip") or "").strip().upper()
        if value.startswith(SYNTHETIC_PREFIXES) or (len(value) == 9 and value.isalnum() and set(value) != {"0"}):
            return value
        isin = (holding.get("isin") or "").strip().upper()
        return f"IS:{isin}" if isin else value

    def walk(sid: str, weight: float, depth: int, ancestors: frozenset[str]) -> None:
        record = get_holdings(sid)
        if record is None:
            raise LookupError(f"no holdings for series {sid}")
        report_date, holdings = record
        summary["oldest_report_date"] = min(summary["oldest_report_date"], report_date)
        equity = get_equity_inputs(sid, report_date) if get_equity_inputs else None

        def child_for(holding: dict[str, Any]) -> str | None:
            child = match_fund(holding, fund_map)
            return child if child and child not in ancestors and depth < max_depth and get_holdings(child) is not None else None

        wrappers = [holding for holding in holdings if (holding.get("asset_class") or "").strip().upper() in _EQUITY_CATS and child_for(holding) is not None]
        use_rollup = equity is not None
        gross, net = (equity.gross_equity_pct, equity.net_equity_pct) if equity else (0.0, 0.0)
        countries = dict(equity.country_exposures_pct) if equity else {}
        seen: set[str] = set()
        for wrapper in wrappers:
            key = weight_key(wrapper)
            if key in seen:
                continue
            seen.add(key)
            exact = equity.signed_holding_weights_pct.get(key) if equity else None
            exact_gross = equity.gross_holding_weights_pct.get(key) if equity else None
            country = equity_country_key(wrapper.get("isin"), wrapper.get("asset_class"), wrapper.get("cusip"), wrapper.get("investment_country"))
            if exact is None or exact_gross is None or country not in countries:
                use_rollup = False
                break
            gross -= exact_gross
            net -= exact
            countries[country] -= exact
        if use_rollup:
            side = "direct_pct" if depth == 0 else "indirect_pct"
            summary["gross_equity_pct"] += gross * abs(weight)
            summary["net_equity_pct"] += net * weight
            for country, value in countries.items():
                if abs(value) >= 1e-12:
                    exposures.setdefault(("country", country), {"label": None, "direct_pct": 0.0, "indirect_pct": 0.0})[side] += value * weight
        for holding in holdings:
            if holding.get("pct_of_nav") is None:
                continue
            exact = equity.signed_holding_weights_pct.get(weight_key(holding)) if equity else None
            local = float(exact) if exact is not None else float(holding["pct_of_nav"])
            payoff = (holding.get("payoff_profile") or "").strip().upper()
            if exact is None and payoff == "SHORT":
                local = -abs(local)
            if exact is None and payoff == "LONG":
                local = abs(local)
            pct, child, expanded = local * weight, match_fund(holding, fund_map), child_for(holding)
            if expanded is not None:
                if depth == 0:
                    summary["expanded_fund_pct"] += pct
                expanded_children.add(expanded)
                walk(expanded, weight * local / 100.0, depth + 1, ancestors | {sid})
            else:
                accumulate(holding, pct, depth, use_rollup)
                if child is not None:
                    summary["nondecomposable_fund_pct"] += abs(pct)

    walk(series_id, 1.0, 0, frozenset({series_id}))
    summary["n_children_expanded"] = len(expanded_children)
    summary["sum_pct_total"] = summary["direct_pct"] + summary["indirect_pct"]
    return exposures, summary


def _require_sha(value: Any, label: str, *, minimum: int = 64, exact: bool = False) -> str:
    if not isinstance(value, str) or len(value) < minimum or (exact and len(value) != minimum) or any(
        char not in "0123456789abcdef" for char in value.lower()
    ):
        raise ArtifactValidationError(f"{label} must be a hexadecimal SHA")
    return value


def validate_phase4_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the immutable Phase 4 completion manifest."""
    if not isinstance(manifest, dict) or manifest.get("schema_version") != _PHASE4_SCHEMA:
        raise ArtifactValidationError("unsupported Phase 4 schema version")
    if manifest.get("state") != "complete":
        raise ArtifactValidationError("Phase 4 manifest must be terminal complete")
    packages = manifest.get("packages")
    if not isinstance(packages, list) or len(packages) != 82:
        raise ArtifactValidationError("Phase 4 manifest must contain exactly 82 packages")
    identities = [package.get("package_id") for package in packages if isinstance(package, dict)]
    if len(identities) != 82 or any(not identity for identity in identities):
        raise ArtifactValidationError("every package requires an identity")
    if len(set(identities)) != len(identities):
        raise ArtifactValidationError("duplicate package identity")
    if any(package.get("state") != "successful" for package in packages):
        raise ArtifactValidationError("every package must be successful")
    counts = Counter(package.get("form") for package in packages)
    if dict(counts) != _FORM_COUNTS:
        raise ArtifactValidationError("Phase 4 form counts must be nport=26, ncen=17, rr1=39")
    w1_sha = _require_sha(manifest.get("w1_producer_sha"), "W1 producer SHA", minimum=1)
    inventory_hash = _require_sha(manifest.get("inventory_hash"), "inventory hash", exact=True)
    source = manifest.get("v2_holdings_source")
    if not isinstance(source, dict) or set(source) != {"relation", "publication_id"}:
        raise ArtifactValidationError("V2 source schema mismatch")
    if source.get("relation") != "sec_nport_holdings_v2":
        raise ArtifactValidationError("V2 source relation must be sec_nport_holdings_v2")
    publication_id = source.get("publication_id")
    if not isinstance(publication_id, str) or not publication_id:
        raise ArtifactValidationError("V2 publication identity is required")
    return {
        "schema_version": manifest["schema_version"],
        "form_counts": dict(_FORM_COUNTS),
        "w1_producer_sha": w1_sha,
        "inventory_hash": inventory_hash,
        "v2_publication_id": publication_id,
    }


def derive_governed_anchors(report_dates: list[date]) -> list[dict[str, str]]:
    """Select the latest available report from each of eight consecutive quarters."""
    latest_by_quarter: dict[tuple[int, int], date] = {}
    for report_date in report_dates:
        if not isinstance(report_date, date):
            raise ArtifactValidationError("report dates must be calendar dates")
        quarter = (report_date.month - 1) // 3 + 1
        key = (report_date.year, quarter)
        latest_by_quarter[key] = max(report_date, latest_by_quarter.get(key, report_date))
    selected = sorted(latest_by_quarter.items())[-8:]
    if len(selected) != 8:
        raise ArtifactValidationError("exactly eight governed anchors are required")
    quarter_numbers = [year * 4 + quarter for (year, quarter), _ in selected]
    if any(current - previous != 1 for previous, current in zip(quarter_numbers, quarter_numbers[1:])):
        raise ArtifactValidationError("governed anchor chain gap")
    dates = [report_date for _, report_date in selected]
    if (dates[-1].year - dates[0].year) * 12 + dates[-1].month - dates[0].month < 21:
        raise ArtifactValidationError("governed anchors must span at least 21 months")
    if any((current - previous).days > 180 for previous, current in zip(dates, dates[1:])):
        raise ArtifactValidationError("governed anchor chain gap exceeds 180 days")
    return [
        {
            "anchor_id": f"{year}Q{quarter}-{report_date.isoformat()}",
            "report_date": report_date.isoformat(),
        }
        for ((year, quarter), report_date) in selected
    ]


def _json_default(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def canonical_json(value: Any) -> bytes:
    """Return the canonical JSON bytes used for all hashes and artifacts."""
    return json.dumps(
        value, default=_json_default, ensure_ascii=True, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _parse_date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise ArtifactValidationError(f"{label} must be an ISO calendar date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ArtifactValidationError(f"{label} must be an ISO calendar date") from error


def _validate_v2_input(
    artifact: dict[str, Any], expected_publication_id: str,
) -> tuple[list[str], dict[str, list[tuple[date, list[dict[str, Any]]]]], dict[str, dict[str, str]], dict[str, str], dict[tuple[str, date], EquityInputs]]:
    if not isinstance(artifact, dict) or artifact.get("schema_version") != _INPUT_SCHEMA:
        raise ArtifactValidationError("unsupported V2 input schema version")
    allowed = {"schema_version", "source", "source_declarations", "root_series_ids", "fund_map", "holdings", "sector_map", "equity_sidecars"}
    if set(artifact) != allowed:
        raise ArtifactValidationError("V2 input artifact has unknown or missing fields")
    source = artifact.get("source")
    if not isinstance(source, dict) or set(source) != {"relation", "publication_id"}:
        raise ArtifactValidationError("V2 input source schema mismatch")
    if source.get("relation") != "sec_nport_holdings_v2":
        raise ArtifactValidationError("input relation must be sec_nport_holdings_v2")
    if source.get("publication_id") != expected_publication_id:
        raise ArtifactValidationError("V2 publication identity mismatch")
    source_declarations = artifact.get("source_declarations", [])
    if not isinstance(source_declarations, list) or any(
        declaration != "sec_nport_holdings_v2" for declaration in source_declarations
    ):
        raise ArtifactValidationError("legacy-contaminated source declaration")
    roots = artifact.get("root_series_ids")
    if not isinstance(roots, list) or not roots or any(not isinstance(root, str) or not root for root in roots):
        raise ArtifactValidationError("declared root series are required")
    if len(set(roots)) != len(roots):
        raise ArtifactValidationError("duplicate declared root series")
    raw_holdings = artifact.get("holdings")
    if not isinstance(raw_holdings, list):
        raise ArtifactValidationError("V2 holdings records are required")
    by_series: dict[str, list[tuple[date, list[dict[str, Any]]]]] = {}
    identities: set[tuple[str, date]] = set()
    for record in raw_holdings:
        if not isinstance(record, dict):
            raise ArtifactValidationError("holding record must be an object")
        series_id = record.get("series_id")
        report_date = _parse_date(record.get("report_date"), "holding report_date")
        holdings = record.get("holdings")
        if not isinstance(series_id, str) or not series_id or not isinstance(holdings, list):
            raise ArtifactValidationError("holding record requires series, date, and holdings")
        identity = (series_id, report_date)
        if identity in identities:
            raise ArtifactValidationError("duplicate V2 series/report identity")
        identities.add(identity)
        by_series.setdefault(series_id, []).append((report_date, holdings))
    for values in by_series.values():
        values.sort(key=lambda value: value[0])
    fund_map = artifact["fund_map"]
    if not isinstance(fund_map, dict) or set(fund_map) != {"cusip", "isin"} or not all(isinstance(fund_map[key], dict) for key in ("cusip", "isin")):
        raise ArtifactValidationError("fund map must declare cusip and isin maps")
    if any(not isinstance(key, str) or not isinstance(value, str) for mapping in fund_map.values() for key, value in mapping.items()):
        raise ArtifactValidationError("fund map values must be strings")
    sector_map = artifact["sector_map"]
    if not isinstance(sector_map, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in sector_map.items()):
        raise ArtifactValidationError("sector map must be a string mapping")
    sidecars = artifact["equity_sidecars"]
    if not isinstance(sidecars, list):
        raise ArtifactValidationError("equity sidecars must be a list")
    equity: dict[tuple[str, date], EquityInputs] = {}
    expected_sidecar = {"series_id", "report_date", "gross_equity_pct", "net_equity_pct", "country_exposures_pct", "signed_holding_weights_pct", "gross_holding_weights_pct"}
    for sidecar in sidecars:
        if not isinstance(sidecar, dict) or set(sidecar) != expected_sidecar:
            raise ArtifactValidationError("equity sidecar schema mismatch")
        series_id = sidecar["series_id"]
        report_date = _parse_date(sidecar["report_date"], "equity sidecar report_date")
        maps = (sidecar["country_exposures_pct"], sidecar["signed_holding_weights_pct"], sidecar["gross_holding_weights_pct"])
        numeric = (sidecar["gross_equity_pct"], sidecar["net_equity_pct"])
        if not isinstance(series_id, str) or not series_id or any(not isinstance(value, (int, float)) or not math.isfinite(value) for value in numeric) or any(not isinstance(mapping, dict) or any(not isinstance(key, str) or not isinstance(value, (int, float)) or not math.isfinite(value) for key, value in mapping.items()) for mapping in maps):
            raise ArtifactValidationError("invalid equity sidecar values")
        identity = (series_id, report_date)
        if identity in equity:
            raise ArtifactValidationError("duplicate equity sidecar identity")
        equity[identity] = EquityInputs(float(numeric[0]), float(numeric[1]), {key: float(value) for key, value in maps[0].items()}, {key: float(value) for key, value in maps[1].items()}, {key: float(value) for key, value in maps[2].items()})
    return roots, by_series, fund_map, sector_map, equity


def _require_external_output_dir(output_dir: Path) -> Path:
    resolved = output_dir.resolve()
    if resolved.parent == resolved:
        raise ArtifactValidationError("output directory cannot be a filesystem root")
    forbidden = [Path(__file__).resolve().parents[2], Path("E:/Edgard/nport"), Path("E:/Edgard/ncen"), Path("E:/Edgard/RR1"), Path("E:/Edgard/13-F")]
    for root in forbidden:
        protected = root.resolve()
        try:
            resolved.relative_to(protected)
        except ValueError:
            try:
                protected.relative_to(resolved)
            except ValueError:
                continue
        raise ArtifactValidationError("output directory must be outside Git and source roots")
    return resolved


def _atomic_json_write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as temporary:
            temporary.write(canonical_json(value))
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
        raise


def _valid_content_hash(value: dict[str, Any]) -> bool:
    claimed = value.get("content_hash")
    without_hash = dict(value)
    without_hash.pop("content_hash", None)
    return isinstance(claimed, str) and claimed == canonical_sha256(without_hash)


def _verify_bundle(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    aggregate = _read_json(path / "aggregate_manifest.json")
    if aggregate != expected or aggregate.get("schema_version") != _RUN_SCHEMA or aggregate.get("state") != "complete" or not _valid_content_hash(aggregate):
        raise ArtifactValidationError("output bundle identity mismatch")
    anchors = aggregate.get("anchors")
    if not isinstance(anchors, list) or len(anchors) != 8:
        raise ArtifactValidationError("output bundle anchor manifest mismatch")
    for entry in anchors:
        if not isinstance(entry, dict) or not isinstance(entry.get("anchor_id"), str):
            raise ArtifactValidationError("output bundle anchor identity mismatch")
        anchor = _read_json(path / "anchors" / f"{entry['anchor_id']}.json")
        if anchor.get("schema_version") != _ANCHOR_SCHEMA or anchor.get("state") != "complete" or not _valid_content_hash(anchor) or entry.get("content_hash") != anchor.get("content_hash"):
            raise ArtifactValidationError("output bundle anchor hash mismatch")
    return aggregate


def _write_bundle(destination: Path, anchor_outputs: list[dict[str, Any]], aggregate: dict[str, Any]) -> dict[str, Any]:
    if destination.exists():
        return _verify_bundle(destination, aggregate)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.stage-", dir=destination.parent))
    try:
        for anchor in anchor_outputs:
            metadata = anchor["anchor"]
            _atomic_json_write(staging / "anchors" / f"{metadata['anchor_id']}.json", anchor)
        _atomic_json_write(staging / "aggregate_manifest.json", aggregate)
        _verify_bundle(staging, aggregate)
        os.replace(staging, destination)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return aggregate


def _exposure_rows(exposures: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "dimension": dimension,
            "key": key,
            "label": cell["label"],
            "direct_pct": cell["direct_pct"],
            "indirect_pct": cell["indirect_pct"],
        }
        for (dimension, key), cell in sorted(exposures.items())
    ]


_ANCHOR_ID = re.compile(r"^(?P<year>\d{4})Q(?P<quarter>[1-4])-(?P<date>\d{4}-\d{2}-\d{2})$")
_SUMMARY_GROUPS = {
    "signed": ("sum_pct_total",), "gross": ("derivatives_gross_pct", "gross_equity_pct"),
    "net": ("derivatives_net_pct", "net_equity_pct"),
    "residual": ("expanded_fund_pct", "nondecomposable_fund_pct", "unidentified_pct"),
    "staleness": ("oldest_report_date",),
}


def _anchor_identity_valid(anchor_id: Any, report_date: Any) -> bool:
    if not isinstance(anchor_id, str) or not isinstance(report_date, str) or not (match := _ANCHOR_ID.fullmatch(anchor_id)):
        return False
    try:
        parsed = date.fromisoformat(report_date)
    except ValueError:
        return False
    return match.group("date") == report_date and (parsed.month - 1) // 3 + 1 == int(match.group("quarter")) and parsed.year == int(match.group("year"))


def _read_prior_complete_anchor_outputs(prior_complete_run: Path, candidate_anchors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    root = prior_complete_run.resolve()
    aggregate = _read_json(root / "aggregate_manifest.json")
    anchors = aggregate.get("anchors")
    if aggregate.get("schema_version") != _RUN_SCHEMA or aggregate.get("state") != "complete" or not _valid_content_hash(aggregate) or not isinstance(anchors, list) or len(anchors) != 8:
        raise ArtifactValidationError("prior artifact run is not an exact complete bundle")
    anchor_entry_fields = {"anchor_id", "report_date", "series_count", "output_row_count", "content_hash"}
    if any(not isinstance(entry, dict) or set(entry) != anchor_entry_fields or not isinstance(entry["anchor_id"], str) or not isinstance(entry["report_date"], str) or type(entry["series_count"]) is not int or type(entry["output_row_count"]) is not int or not isinstance(entry["content_hash"], str) for entry in anchors):
        raise ArtifactValidationError("prior artifact anchor entry schema mismatch")
    cohort = [(value.get("anchor_id"), value.get("report_date")) for value in anchors if isinstance(value, dict)]
    expected = [(value["anchor_id"], value["report_date"]) for value in candidate_anchors]
    if len(cohort) != 8 or len(set(cohort)) != 8 or cohort != expected or any(not _anchor_identity_valid(anchor_id, report_date) for anchor_id, report_date in cohort):
        raise ArtifactValidationError("prior artifact run has foreign or mixed anchor cohort")
    outputs: list[dict[str, Any]] = []
    required_binding = {"phase4_manifest_hash", "v2_publication_id", "w1_producer_sha", "runner_sha", "input_artifact_hash", "series_count"}
    for entry in anchors:
        anchor_id, report_date = entry["anchor_id"], entry["report_date"]
        anchor_path = (root / "anchors" / f"{anchor_id}.json").resolve()
        try:
            anchor_path.relative_to(root)
        except ValueError as error:
            raise ArtifactValidationError("prior artifact anchor escapes bundle") from error
        output = _read_json(anchor_path)
        metadata, binding, series = output.get("anchor"), output.get("binding"), output.get("series")
        if not _valid_content_hash(output) or output.get("content_hash") != entry.get("content_hash"):
            raise ArtifactValidationError("prior artifact anchor hash mismatch")
        if output.get("schema_version") != _ANCHOR_SCHEMA or output.get("state") != "complete" or not isinstance(metadata, dict) or (metadata.get("anchor_id"), metadata.get("report_date")) != (anchor_id, report_date) or not isinstance(binding, dict) or set(binding) != required_binding or not isinstance(series, list) or binding.get("series_count") != len(series):
            raise ArtifactValidationError("prior artifact anchor consistency failure")
        for key in ("phase4_manifest_hash", "v2_publication_id", "w1_producer_sha", "runner_sha", "input_artifact_hash"):
            if binding.get(key) != aggregate.get(key):
                raise ArtifactValidationError("prior artifact binding consistency failure")
        if any(not isinstance(value.get("content_hash"), str) or value.get("content_hash") != canonical_sha256({"summary": value.get("summary"), "exposures": value.get("exposures")}) for value in series if isinstance(value, dict)) or any(not isinstance(value, dict) for value in series):
            raise ArtifactValidationError("prior artifact series hash mismatch")
        if type(binding["series_count"]) is not int or entry["series_count"] != binding["series_count"] or entry["output_row_count"] != sum(value.get("output_row_count", -1) for value in series) or any(type(value.get("output_row_count")) is not int or not isinstance(value.get("exposures"), list) or value["output_row_count"] != len(value["exposures"]) for value in series):
            raise ArtifactValidationError("prior artifact row count mismatch")
        outputs.append(output)
    return outputs


def _numeric(value: Any) -> float:
    if isinstance(value, str):
        return float(date.fromisoformat(value).toordinal())
    return float(value)


def _value_groups(anchor_outputs: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    groups: dict[str, dict[str, float]] = {name: {} for name in ("direct", "indirect", *list(_SUMMARY_GROUPS), "country", "currency", "issuer", "asset_class", "sector")}
    for anchor in anchor_outputs:
        anchor_id = anchor["anchor"]["anchor_id"]
        for series in anchor["series"]:
            prefix = f"{anchor_id}|{series['series_id']}"
            summary = series["summary"]
            for group, metrics in _SUMMARY_GROUPS.items():
                for metric in metrics:
                    groups[group][f"{prefix}|{metric}"] = _numeric(summary[metric])
            for row in series["exposures"]:
                dimension = row["dimension"]
                key = f"{prefix}|{dimension}|{row['key']}"
                groups["direct"][key] = float(row["direct_pct"])
                groups["indirect"][key] = float(row["indirect_pct"])
                if dimension in {"country", "currency", "issuer", "asset_class", "sector"}:
                    groups[dimension][f"{key}|direct"] = float(row["direct_pct"])
                    groups[dimension][f"{key}|indirect"] = float(row["indirect_pct"])
    return groups


def _parity_evidence(current: list[dict[str, Any]], prior: list[dict[str, Any]]) -> dict[str, Any]:
    current_groups, prior_groups = _value_groups(current), _value_groups(prior)
    result: dict[str, Any] = {"evidence_only": True}
    for group, values in current_groups.items():
        previous = prior_groups[group]
        common = sorted(set(values) & set(previous))
        deltas = [values[key] - previous[key] for key in common]
        result[group] = {"matched_count": sum(delta == 0 for delta in deltas), "changed_count": sum(delta != 0 for delta in deltas), "added_count": len(set(values) - set(previous)), "removed_count": len(set(previous) - set(values)), "value_delta": sum(deltas), "max_abs_delta": max((abs(delta) for delta in deltas), default=0.0)}
    return result


def run_artifact(
    phase4_manifest: dict[str, Any],
    input_artifact: dict[str, Any],
    output_dir: Path | str,
    *,
    runner_sha: str,
    require_all_eight: bool = False,
    prior_complete_run: Path | str | None = None,
) -> dict[str, Any]:
    """Compute all governed anchors from explicit V2 JSON and atomically emit evidence."""
    if require_all_eight is not True:
        raise ArtifactValidationError("--require-all-eight is mandatory")
    manifest = validate_phase4_manifest(phase4_manifest)
    _require_sha(runner_sha, "runner SHA", minimum=1)
    roots, by_series, fund_map, sector_map, equity_sidecars = _validate_v2_input(input_artifact, manifest["v2_publication_id"])
    all_dates = [report_date for records in by_series.values() for report_date, _ in records]
    anchors = derive_governed_anchors(all_dates)
    artifact_hash = canonical_sha256(input_artifact)
    manifest_hash = canonical_sha256(phase4_manifest)
    anchor_outputs: list[dict[str, Any]] = []
    for anchor in anchors:
        anchor_date = _parse_date(anchor["report_date"], "anchor report_date")
        quarter_start = date(anchor_date.year, ((anchor_date.month - 1) // 3) * 3 + 1, 1)

        series_outputs: list[dict[str, Any]] = []
        for root in roots:
            quarter_records = [record for record in by_series.get(root, []) if quarter_start <= record[0] <= anchor_date]
            root_record = quarter_records[-1] if quarter_records else None
            if root_record is None:
                raise ArtifactValidationError(f"missing root series {root} at anchor {anchor_date.isoformat()}")

            def get_holdings(series_id: str) -> tuple[date, list[dict[str, Any]]] | None:
                if series_id == root:
                    return root_record
                candidates = [record for record in by_series.get(series_id, []) if record[0] <= anchor_date]
                return candidates[-1] if candidates else None

            exposures, summary = expand_series(
                root, get_holdings, fund_map, sector_map=sector_map,
                get_equity_inputs=lambda sid, report_date: equity_sidecars.get((sid, report_date)),
            )
            rows = _exposure_rows(exposures)
            normalized_summary = {
                key: (value.isoformat() if isinstance(value, date) else value)
                for key, value in summary.items()
            }
            series_outputs.append(
                {
                    "series_id": root,
                    "report_date": root_record[0].isoformat(),
                    "summary": normalized_summary,
                    "exposures": rows,
                    "output_row_count": len(rows),
                    "content_hash": canonical_sha256({"summary": normalized_summary, "exposures": rows}),
                }
            )
        anchor_output = {
            "schema_version": "nport_v2_lookthrough_anchor/v1",
            "state": "complete",
            "anchor": anchor,
            "binding": {
                "phase4_manifest_hash": manifest_hash,
                "v2_publication_id": manifest["v2_publication_id"],
                "w1_producer_sha": manifest["w1_producer_sha"],
                "runner_sha": runner_sha,
                "input_artifact_hash": artifact_hash,
                "series_count": len(series_outputs),
            },
            "series": series_outputs,
        }
        anchor_output["content_hash"] = canonical_sha256(anchor_output)
        anchor_outputs.append(anchor_output)
    aggregate = {
        "schema_version": "nport_v2_lookthrough_run/v1",
        "state": "complete",
        "phase4_manifest_hash": manifest_hash,
        "v2_publication_id": manifest["v2_publication_id"],
        "w1_producer_sha": manifest["w1_producer_sha"],
        "runner_sha": runner_sha,
        "input_artifact_hash": artifact_hash,
        "anchors": [
            {
                "anchor_id": value["anchor"]["anchor_id"],
                "report_date": value["anchor"]["report_date"],
                "series_count": value["binding"]["series_count"],
                "output_row_count": sum(item["output_row_count"] for item in value["series"]),
                "content_hash": value["content_hash"],
            }
            for value in anchor_outputs
        ],
    }
    if prior_complete_run is not None:
        aggregate["parity"] = _parity_evidence(
            anchor_outputs, _read_prior_complete_anchor_outputs(Path(prior_complete_run), aggregate["anchors"]),
        )
    aggregate["content_hash"] = canonical_sha256(aggregate)
    destination = _require_external_output_dir(Path(output_dir))
    return _write_bundle(destination, anchor_outputs, aggregate)


def run_shadow_unconfigured(
    phase4_manifest: dict[str, Any],
    input_artifact: dict[str, Any],
    authorization: dict[str, Any],
    *,
    runner_sha: str,
) -> None:
    """Validate a tightly bound shadow authorization, then fail without I/O.

    A writer and schema are intentionally absent from this task.  Keeping the
    refusal after validation makes a future implementation unable to bypass the
    authorization contract while still guaranteeing no connection is attempted.
    """
    manifest = validate_phase4_manifest(phase4_manifest)
    _validate_v2_input(input_artifact, manifest["v2_publication_id"])
    if not isinstance(authorization, dict):
        raise ArtifactValidationError("shadow authorization record is required")
    expected: dict[str, str] = {
        "stage": "phase4b_shadow",
        "runner_sha": runner_sha,
        "phase4_manifest_hash": canonical_sha256(phase4_manifest),
        "v2_publication_id": manifest["v2_publication_id"],
        "command": "shadow-db-write",
        "target": "isolated-shadow",
        "role": "shadow_writer",
    }
    if set(authorization) != {*expected, "table_allowlist"}:
        raise ArtifactValidationError("shadow authorization schema mismatch")
    for binding_name, value in expected.items():
        if authorization.get(binding_name) != value:
            raise ArtifactValidationError(f"shadow authorization {binding_name} mismatch")
    if tuple(authorization.get("table_allowlist", ())) != SHADOW_TABLE_ALLOWLIST:
        raise ArtifactValidationError("shadow authorization table allowlist mismatch")
    raise ArtifactValidationError("shadow_writer_unconfigured")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactValidationError("explicit JSON artifact could not be read") from error
    if not isinstance(value, dict):
        raise ArtifactValidationError("explicit JSON artifact must be an object")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run governed artifact-only V2 N-PORT look-through anchors")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--artifact-only", action="store_true")
    modes.add_argument("--shadow-db-write", action="store_true")
    parser.add_argument("--phase4-manifest", type=Path, required=True)
    parser.add_argument("--input-artifact", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--authorization-record", type=Path)
    parser.add_argument("--prior-complete-run", type=Path)
    parser.add_argument("--runner-sha", required=True)
    parser.add_argument("--require-all-eight", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manifest = _read_json(args.phase4_manifest)
        artifact = _read_json(args.input_artifact)
        if args.artifact_only:
            if args.output_dir is None:
                raise ArtifactValidationError("artifact-only mode requires --output-dir")
            result = run_artifact(
                manifest, artifact, args.output_dir, runner_sha=args.runner_sha,
                require_all_eight=args.require_all_eight, prior_complete_run=args.prior_complete_run,
            )
            print(canonical_json(result).decode("utf-8"))
            return 0
        if args.authorization_record is None:
            raise ArtifactValidationError("shadow mode requires --authorization-record")
        run_shadow_unconfigured(
            manifest, artifact, _read_json(args.authorization_record), runner_sha=args.runner_sha,
        )
    except ArtifactValidationError as error:
        print(str(error))
        return 1
    return 1


if __name__ == "__main__":  # pragma: no cover - exercised through module invocation
    raise SystemExit(main())
