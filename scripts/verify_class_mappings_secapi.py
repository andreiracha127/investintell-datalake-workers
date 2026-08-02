#!/usr/bin/env python3
"""Report-only cross-check of composed class mappings against sec-api.io N-CEN.

THIS IS NEVER A COMPOSER DEPENDENCY.  ``compose_fund_regulatory_serving_mappings``
resolves ``class_id`` from datalake relations only; it must stay runnable with no
network and no vendor credential.  This script is a separate, after-the-fact
opinion: it samples the composed publication, asks the SEC's own N-CEN
series/class table whether each composed class really belongs to its series, and
prints what it found.  It changes no row and gates nothing.

What it can and cannot prove
----------------------------
N-CEN publishes ``seriesClass.reportSeriesClass.rptSeriesClassInfo[]`` -- one
entry per series with the full list of its ``classIds``.  It does NOT publish
the class ticker, so this check is set membership, not ticker attribution: it
catches a class id that does not belong to the series at all (the exact failure
mode of the poisoned ``sec_fund_classes`` relation, whose ``class_id`` column
holds a SERIES id) but it cannot arbitrate WHICH sibling class a ticker means.
Read a 100% agreement as "no composed class is foreign to its series", not as
"every ticker attribution is confirmed".

Tail probe
----------
For every instrument the cascade could not resolve, the script asks N-CEN for
its series and, if no N-CEN filing exists, falls back to N-PORT ``genInfo``.  A
single-class N-CEN answer is a candidate a human can act on; it is reported,
never written.

Usage
-----
    py -3.13 scripts/verify_class_mappings_secapi.py --dsn "$DATABASE_URL" \
        --sample 50 --json report.json

Credentials come from ``secapi_env.load_api_key()`` (``secapi-api-key`` in
``backend/.env`` -- a hyphenated name no shell can export).  The SDK ships the
key as a ``?token=`` query parameter, so every message that could carry a URL is
passed through ``secapi_env.scrub()`` before it is printed.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import connect  # noqa: E402

# The credential/SDK helpers live in the installed sec-api-io skill; they own the
# two environment traps (hyphenated key name, SDK only in the global 3.13).
DEFAULT_SKILL_SCRIPTS = Path.home() / ".claude" / "skills" / "sec-api-io" / "scripts"

NCEN_SERIES_FIELD = "seriesClass.reportSeriesClass.rptSeriesClassInfo.seriesId"
NPORT_SERIES_FIELD = "genInfo.seriesId"


def _load_secapi_env(skill_scripts: Path):
    if str(skill_scripts) not in sys.path:
        sys.path.insert(0, str(skill_scripts))
    try:
        import secapi_env  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - environment guard
        raise SystemExit(
            f"sec-api-io skill helpers not found at {skill_scripts}; pass --skill-scripts"
        ) from exc
    return secapi_env


def normalize_dsn(dsn: str | None) -> str | None:
    """Drop a SQLAlchemy driver suffix (``postgresql+asyncpg://``) if present."""
    if not dsn:
        return dsn
    parts = urlsplit(dsn)
    if "+" in parts.scheme:
        return urlunsplit(parts._replace(scheme=parts.scheme.split("+", 1)[0]))
    return dsn


def _call(api: Any, query: str, scrub, *, size: int = 1, attempts: int = 3) -> dict[str, Any]:
    """One bounded, sorted request.  No paging loop: every query here is small.

    The 10 000-record ceiling that makes hand-rolled paging dangerous cannot be
    reached from a single ``size<=10`` request for the newest filing of one
    series, so the skill's bisecting fetcher is not needed -- but the explicit
    ``sort`` is, or page boundaries drift.
    """
    payload = {"query": query, "from": "0", "size": str(size),
               "sort": [{"filedAt": {"order": "desc"}}]}
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            return api.get_data(payload)
        except Exception as exc:  # the SDK raises bare Exception with the code in the text
            last = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"sec-api request failed: {scrub(str(last))}")


def _records(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Parsed endpoints answer under ``data``; some under ``filings``."""
    return response.get("data") or response.get("filings") or []


def ncen_classes_for_series(api: Any, series_id: str, scrub, *, depth: int = 5) -> dict[str, Any]:
    """The class id set N-CEN publishes for one series.

    A filer may answer the series/class table with ``includeAllClassesFlag``
    instead of enumerating ``classIds`` -- the entry is then present and correct
    but carries NO class list.  Measured on the sample: 24 of 50 newest filings
    answer that way.  Collapsing that into "no filing" would misreport coverage,
    so the three outcomes stay distinct and the newest ``depth`` filings are
    scanned for one that does enumerate.
    """
    response = _call(api, f'{NCEN_SERIES_FIELD}:"{series_id}"', scrub, size=depth)
    records = _records(response)
    if not records:
        return {"classes": set(), "filed_at": None, "state": "no_ncen_filing"}
    include_all = False
    for record in records:
        entries = (
            record.get("seriesClass", {})
            .get("reportSeriesClass", {})
            .get("rptSeriesClassInfo", [])
        )
        for entry in entries:
            if entry.get("seriesId") != series_id:
                continue
            classes = {c for c in entry.get("classIds", []) if c}
            if classes:
                return {"classes": classes, "filed_at": record.get("filedAt"), "state": "enumerated"}
            include_all = include_all or bool(entry.get("includeAllClassesFlag"))
    return {
        "classes": set(),
        "filed_at": records[0].get("filedAt"),
        "state": "class_list_omitted" if include_all else "series_absent_from_filing",
    }


def composed_classes(conn, app_publication_id: UUID) -> dict[str, set[str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT series_id, class_id FROM fund_regulatory_serving_instrument_mappings"
            " WHERE app_publication_id = %s AND mapping_state = 'resolved' AND class_id <> ''",
            (app_publication_id,),
        )
        rows = cur.fetchall()
    by_series: dict[str, set[str]] = {}
    for series_id, class_id in rows:
        by_series.setdefault(series_id, set()).add(class_id)
    return by_series


def unresolved_series(conn, app_publication_id: UUID) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT m.series_id, coalesce(upper(btrim(ii.ticker)), '')"
            " FROM fund_regulatory_serving_instrument_mappings m"
            " LEFT JOIN instrument_identity ii ON ii.instrument_id = m.instrument_id"
            " WHERE m.app_publication_id = %s AND m.mapping_state <> 'resolved'"
            " ORDER BY m.series_id",
            (app_publication_id,),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def resolve_pointer(conn) -> UUID:
    with conn.cursor() as cur:
        cur.execute("SELECT app_publication_id FROM fund_regulatory_serving_app_current_pointer")
        row = cur.fetchone()
    if row is None:
        raise SystemExit("no app pointer is set; pass --app-publication-id")
    return row[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn", default=None, help="target DSN (default: DATABASE_URL)")
    parser.add_argument("--app-publication-id", default=None, help="default: the current app pointer")
    parser.add_argument("--sample", type=int, default=50, help="series sampled for the N-CEN cross-check")
    parser.add_argument("--seed", type=int, default=20260802, help="sampling seed (reproducibility)")
    parser.add_argument("--tail-series", default=None, help="probe only this series instead of the composed tail")
    parser.add_argument("--skill-scripts", default=str(DEFAULT_SKILL_SCRIPTS))
    parser.add_argument("--json", default=None, help="also write the report as JSON to this path")
    args = parser.parse_args(argv)

    secapi_env = _load_secapi_env(Path(args.skill_scripts))
    secapi_env.require_sdk()
    scrub = secapi_env.scrub
    api_key = secapi_env.load_api_key()
    from sec_api import FormNcenApi, FormNportApi  # noqa: PLC0415 - after require_sdk

    ncen = FormNcenApi(api_key)
    nport = FormNportApi(api_key)

    with connect(normalize_dsn(args.dsn)) as conn:
        app_id = UUID(args.app_publication_id) if args.app_publication_id else resolve_pointer(conn)
        by_series = composed_classes(conn, app_id)
        tail = [(args.tail_series, "")] if args.tail_series else unresolved_series(conn, app_id)

    series_ids = sorted(by_series)
    random.Random(args.seed).shuffle(series_ids)
    sample = sorted(series_ids[: max(0, args.sample)])

    checks: list[dict[str, Any]] = []
    for series_id in sample:
        found = ncen_classes_for_series(ncen, series_id, scrub)
        classes = found["classes"]
        composed = sorted(by_series[series_id])
        foreign = sorted(c for c in composed if classes and c not in classes)
        if found["state"] != "enumerated":
            status = found["state"]
        else:
            status = "agree" if not foreign else "foreign_class"
        checks.append(
            {
                "series_id": series_id,
                "composed_class_ids": composed,
                "ncen_class_count": len(classes),
                "ncen_filed_at": found["filed_at"],
                "status": status,
                "foreign_class_ids": foreign,
            }
        )

    tail_report: list[dict[str, Any]] = []
    for series_id, ticker in tail:
        found = ncen_classes_for_series(ncen, series_id, scrub)
        classes = found["classes"]
        entry: dict[str, Any] = {
            "series_id": series_id,
            "ticker": ticker,
            "ncen_class_ids": sorted(classes),
            "ncen_filed_at": found["filed_at"],
            "ncen_state": found["state"],
        }
        if classes:
            entry["verdict"] = (
                "single_class_candidate" if len(classes) == 1 else "multiple_classes_still_ambiguous"
            )
        else:
            response = _call(nport, f'{NPORT_SERIES_FIELD}:"{series_id}"', scrub)
            records = _records(response)
            gen = records[0].get("genInfo", {}) if records else {}
            entry["nport_series_name"] = gen.get("seriesName")
            entry["nport_filed_at"] = records[0].get("filedAt") if records else None
            entry["verdict"] = "nport_has_no_class_grain" if records else "no_filing_in_either_form"
        tail_report.append(entry)

    agree = sum(1 for c in checks if c["status"] == "agree")
    foreign = sum(1 for c in checks if c["status"] == "foreign_class")
    omitted = sum(1 for c in checks if c["status"] == "class_list_omitted")
    missing = sum(1 for c in checks if c["status"] == "no_ncen_filing")
    absent = sum(1 for c in checks if c["status"] == "series_absent_from_filing")
    comparable = agree + foreign
    report = {
        "app_publication_id": str(app_id),
        "series_sampled": len(checks),
        "series_comparable": comparable,
        "series_agree": agree,
        "series_with_foreign_class": foreign,
        "series_with_class_list_omitted": omitted,
        "series_absent_from_filing": absent,
        "series_without_ncen_filing": missing,
        "agreement_pct": round(100.0 * agree / comparable, 2) if comparable else None,
        "checks": checks,
        "tail": tail_report,
    }

    print(f"app_publication_id        = {report['app_publication_id']}")
    print(f"series sampled            = {report['series_sampled']}")
    print(f"comparable (enumerated)   = {comparable}")
    print(f"agree (no foreign class)  = {agree}  ({report['agreement_pct']}%)")
    print(f"foreign class id          = {foreign}")
    print(f"class list omitted        = {omitted}  (includeAllClassesFlag)")
    print(f"series absent from filing = {absent}")
    print(f"no N-CEN filing           = {missing}")
    for check in checks:
        if check["status"] != "agree":
            print(f"  {check['series_id']}: {check['status']} {check['foreign_class_ids'] or ''}")
    for entry in tail_report:
        print(f"tail {entry['series_id']} ({entry['ticker']}): {entry['verdict']} {entry['ncen_class_ids']}")

    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(f"json report written to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
