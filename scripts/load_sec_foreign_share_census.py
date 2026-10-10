"""Replay annual-report share-class statements from a verified, read-only W1c cache.

There is no discovery or network path. Only --apply opens a database connection.
The separate census artifact can also be reconciled by W1c's explicit --apply
extension; existing foreign-listing evidence reconciliation is unchanged.
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Iterable

if __package__:
    from . import load_sec_foreign_listing_evidence as w1c
else:
    import load_sec_foreign_listing_evidence as w1c

FORMS = frozenset({"20-F", "20-F/A", "40-F", "40-F/A"})
MANIFEST_VERSION = "sec-foreign-share-census-v1"
FACT_COLUMNS = (
    "fact_hash", "cik", "adsh", "form", "filed", "accepted", "period_end",
    "shares_as_of", "date_explicit", "classes", "stated_total", "computed_total",
    "complete", "conflicting", "reasons", "cross_checks", "publication_floor_on",
    "available_on", "loaded_on", "source_package", "source_url", "source_sha256",
    "parser_version", "evidence_text", "evidence_location",
)
_CLIENT = None
_COUNTS: dict[tuple[int, str], list[dict]] = {}
_COUNT_SHA = ""


def census_fact_hash(row: dict) -> str:
    """Reading identity excludes load clocks and the derived publication date."""
    values = {key: row.get(key) for key in FACT_COLUMNS
              if key not in {"fact_hash", "available_on", "loaded_on"}}
    return hashlib.md5(w1c.canonical_json(values).encode(), usedforsecurity=False).hexdigest()


def cross_check(row: dict, counts: list[dict], input_sha256: str) -> dict:
    """Compare, never replace, same-accession W1 readings with source counts."""
    checks = {"undimensioned": [], "class_dimensioned": [],
              "w1_same_filing_counts": len(counts), "w1_input_sha256": input_sha256}
    by_class: dict[str, list[dict]] = {}
    for cls in row["classes"]:
        if cls.get("class_key"):
            by_class.setdefault(cls["class_key"], []).append(cls)
    for count in counts:
        label = count.get("normalized_class_key")
        unbound = False
        cls = None
        if count["class_key"] == "":
            value = row.get("stated_total")
            if value is None and all(c.get("shares") is not None for c in row["classes"]):
                value = row.get("computed_total")
            target = checks["undimensioned"]
        else:
            candidates = by_class.get(label, [])
            cls = candidates[0] if len(candidates) == 1 else None
            value = cls.get("shares") if cls else None
            unbound = cls is None
            target = checks["class_dimensioned"]
        # Exports use exact integral numeric strings; there is no float bridge.
        from decimal import Decimal
        actual = Decimal(str(count["shares"]))
        status = ("unbound" if unbound else "census_count_missing") if value is None else (
            "match" if Decimal(str(value)) == actual else "mismatch")
        absent_named_class = (unbound and not candidates and row["complete"] and label is not None
                              and not re.search(r"deposit[ao]ry|\bads\b|\badr\b", count["class_key"], re.I))
        if absent_named_class:
            # Positive W1 evidence of another named class invalidates an
            # otherwise exhaustive census; unknown member strings do not bind.
            status = "mismatch"
        target.append({"w1_id": count.get("id"), "class_key": count["class_key"],
                       "normalized_class_key": label,
                       "class_name": cls.get("class_name") if cls else None,
                       "stated_on": count["stated_on"], "w1_shares": str(actual),
                       "census_shares": value, "status": status,
                       "date_match": str(count["stated_on"]) == str(row.get("shares_as_of")),
                       "mismatch_reason": "class_absent_from_census" if absent_named_class
                       else "shares_differ" if status == "mismatch" else None})
    mismatch = any(c["status"] == "mismatch" for name in ("undimensioned", "class_dimensioned")
                   for c in checks[name])
    row["cross_checks"] = checks
    row["conflicting"] = bool(row.get("conflicting")) or mismatch
    if mismatch:
        row["reasons"] = list(dict.fromkeys([*row["reasons"], "w1_count_mismatch"]))
    if row["conflicting"]:
        row["status"] = "conflicting"
    return row


def _initialize(cache: str, raw_cache: str, counts_path: str) -> None:
    global _CLIENT, _COUNTS, _COUNT_SHA
    _CLIENT = w1c.SecClient(Path(cache), offline=True, raw_cache_dir=Path(raw_cache))
    raw = Path(counts_path).read_bytes()
    _COUNT_SHA = w1c.digest(raw)
    _COUNTS = {}
    for row in json.loads(raw):
        if row.get("retired_on") is None:
            _COUNTS.setdefault((int(row["cik"]), row["adsh"]), []).append(row)


def _parse(document: dict) -> tuple[dict, dict]:
    if __package__:
        from .sec_foreign_share_census_parser import PARSER_VERSION, parse_share_census
    else:
        from sec_foreign_share_census_parser import PARSER_VERSION, parse_share_census
    document = w1c.canonical_document(document)
    raw, sha = _CLIENT.document(document["source_url"], expected_sha256=document.get("source_sha256"))
    pdf_ranges = None
    source_lines = None
    if raw.lstrip().startswith(b"%PDF-"):
        pages, method = w1c.extract_pdf_pages(raw, cache_dir=_CLIENT.cache)
        html, pdf_text, pdf_ranges = w1c.pdf_parser_input(pages)
        source_lines = [line for page in pages for line in page.splitlines()]
        document = {**document, "content_format": "pdf", "pdf_text_extractor": method}
    else:
        html = raw.decode("utf-8-sig", errors="replace")
    result = parse_share_census(html, period_end=document.get("period"), source_lines=source_lines)
    source_available = max(date.fromisoformat(document["filed"]) + timedelta(days=1),
                           date.fromisoformat(document["publication_floor_on"]))
    row = {**result, "cik": int(document["cik"]), "adsh": document["adsh"],
           "form": document["form"], "filed": document["filed"],
           "accepted": document.get("accepted"), "source_package": document["source_package"],
           "source_url": document["source_url"], "source_sha256": sha,
           "parser_version": PARSER_VERSION,
           "publication_floor_on": document["publication_floor_on"],
           "source_available_on": source_available.isoformat(),
           "available_on": source_available.isoformat(),
           "evidence_text": (result.get("cover_region_text") or result.get("source_text", "")),
           "evidence_location": result.get("source_location", ""),
           "conflicting": result.get("status") == "conflicting"}
    if result.get("cover_region_location"):
        row["evidence_location"] += ";" + result["cover_region_location"]
        # The refusal marker is diagnostic, not a quotation of SEC source text.
        row["evidence_text"] = row["evidence_text"].removesuffix(" [COVER REGION TRUNCATED]").strip()
    if pdf_ranges is not None and row["evidence_text"]:
        # The existing W1c mapper appends the verified page and text offsets.
        w1c.locate_pdf_evidence(row, pdf_text, pdf_ranges)
    cross_check(row, _COUNTS.get((row["cik"], row["adsh"]), []), _COUNT_SHA)
    row["fact_hash"] = census_fact_hash(row)
    updated = {**document, "source_sha256": sha, "parser_version": PARSER_VERSION,
               "census_status": row["status"], "census_count": int(row["status"] != "none"),
               "census_fact_hash": row["fact_hash"]}
    return updated, row


def replay(manifest: dict, cache_dir: Path, raw_cache_dir: Path, output: Path,
           counts_path: Path, workers: int = 4) -> dict:
    """Bounded spawn pool, deterministic publication, source failures are gaps."""
    if not 1 <= workers <= 12:
        raise ValueError("Parse workers must be between 1 and 12")
    raw_root = raw_cache_dir.resolve()
    if any(path.resolve().is_relative_to(raw_root) for path in (cache_dir, output)):
        raise ValueError("All new artifacts must be outside the read-only raw cache")
    if not manifest.get("complete"):
        raise ValueError("A complete W1c discovery manifest is required")
    documents = [w1c.canonical_document(d) for d in manifest["documents"]
                 if d["form"] in FORMS and d.get("document_role", "primary") == "primary"]
    documents = sorted(documents, key=lambda d: d["source_package"])
    if len({d["source_package"] for d in documents}) != len(documents):
        raise ValueError("Duplicate primary source identity")
    w1c.require_authoritative_filing_dates({**manifest, "documents": documents})
    cache_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    results: dict[str, tuple[dict, dict]] = {}
    gaps = []
    initargs = (str(cache_dir), str(raw_cache_dir), str(counts_path))

    def record(document, parsed=None, error=None):
        if error:
            gap = {"cik": document["cik"], "adsh": document["adsh"],
                   "source_url": document["source_url"], "source_package": document["source_package"],
                   "error": type(error).__name__ + ": " + str(error)[:300]}
            gaps.append(gap)
            updated = {**document, "census_status": "gap", "census_count": 0, "error": gap["error"]}
            results[document["source_package"]] = (updated, None)
        else:
            results[document["source_package"]] = parsed
        if len(results) % 200 == 0 or len(results) == len(documents):
            print(w1c.canonical_json({"event": "census_progress", "completed": len(results),
                                      "total": len(documents), "gaps": len(gaps),
                                      "elapsed_s": round(time.monotonic() - started, 1)}), flush=True)

    if workers == 1:
        _initialize(*initargs)
        for document in documents:
            try:
                parsed = _parse(document)
            except Exception as exc:
                record(document, error=exc)
            else:
                record(document, parsed)
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"),
                                 initializer=_initialize, initargs=initargs) as pool:
            iterator = iter(documents)
            pending = {}
            while True:
                while len(pending) < workers * 2:
                    document = next(iterator, None)
                    if document is None:
                        break
                    pending[pool.submit(_parse, document)] = document
                if not pending:
                    break
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    document = pending.pop(future)
                    try:
                        parsed = future.result()
                    except Exception as exc:
                        record(document, error=exc)
                    else:
                        record(document, parsed)
    status_counts: dict[str, int] = {}
    rows = []
    updated_documents = []
    for key in sorted(results):
        document, row = results[key]
        updated_documents.append(document)
        status_counts[document["census_status"]] = status_counts.get(document["census_status"], 0) + 1
        if row is not None:
            rows.append(row)
    # Failed sources never authorize retirement. Preserve a prior published artifact.
    metrics = {"documents": len(documents), "statuses": status_counts, "workers": workers,
               "max_outstanding_tasks": workers * 2, "elapsed_s": round(time.monotonic() - started, 3),
               "raw_cache_read_only": True, "network_requests": 0, "gaps": len(gaps)}
    output_manifest = {**manifest, "manifest_version": MANIFEST_VERSION,
                       "documents": updated_documents, "parse_complete": not gaps,
                       "census_complete": not gaps, "w1_counts_sha256": w1c.digest(counts_path.read_bytes()),
                       "run_metrics": metrics, "gaps": gaps}
    repository = Path(__file__).resolve().parents[1]
    output_manifest["implementation_sha256s"] = {
        name: w1c.digest((repository / name).read_bytes()) for name in (
            "scripts/sec_foreign_share_census_parser.py", "scripts/load_sec_foreign_share_census.py",
            "scripts/sec_foreign_listing_parser.py", "schemas/sec_foreign_share_census_v1.sql")}
    if not gaps:
        w1c.write_bytes(output, "".join(w1c.canonical_json(row) + "\n" for row in rows).encode())
        output_manifest["census_sha256"] = w1c.digest(output.read_bytes())
    w1c.write_json(cache_dir / "manifest.json", output_manifest)
    w1c.write_json(cache_dir / "run-metrics.json", metrics)
    return output_manifest


def require_schema(cursor: Any) -> None:
    """Pin the additive contract after obtaining the shared migration lock."""
    cursor.execute("SELECT obj_description('public.sec_foreign_share_census'::regclass), "
                   "p.provolatile,p.proisstrict,p.prosecdef,p.proconfig,l.lanname, "
                   "pg_get_userbyid(p.proowner),p.prosrc FROM pg_proc p "
                   "JOIN pg_language l ON l.oid=p.prolang "
                   "WHERE p.oid='public.sec_foreign_share_census_at(bigint,date)'::regprocedure")
    found = cursor.fetchone()
    schema = Path(__file__).resolve().parents[1] / "schemas/sec_foreign_share_census_v1.sql"
    expected_body = re.search(r"AS \$fn\$(.*?)\$fn\$;", schema.read_text(encoding="utf-8"), re.S).group(1)
    if not found or found[1:7] != ("s", False, False, None, "sql", "worker_writer") or found[7] != expected_body:
        raise ValueError("Foreign share census schema contract is not installed or has drifted")
    cursor.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                   "WHERE n.nspname='public' AND c.relname IN "
                   "('sec_foreign_share_census','sec_foreign_share_census_sources') "
                   "AND pg_get_userbyid(c.relowner)='worker_writer'")
    if cursor.fetchone()[0] != 2:
        raise ValueError("Foreign share census relation ownership has drifted")


def apply_census(connection: Any, manifest: dict, rows: Iterable[dict], observed_on: date) -> dict:
    """W1c v2 reconciliation: same-byte corrections restate, new bytes are prospective."""
    if (manifest.get("manifest_version") != MANIFEST_VERSION or not manifest.get("complete")
            or not manifest.get("parse_complete") or not manifest.get("census_complete")):
        raise ValueError("Only a complete census replay manifest can be applied")
    w1c.require_authoritative_filing_dates(manifest)
    documents = {d["source_package"]: d for d in manifest["documents"]}
    if len(documents) != len(manifest["documents"]):
        raise ValueError("Census manifest contains duplicate source identities")
    grouped: dict[str, list[dict]] = {}
    seen = set()
    for row in rows:
        package = row["source_package"]
        if package not in documents:
            raise ValueError("Census artifact contains an unmanifested source")
        if row["fact_hash"] != census_fact_hash(row):
            raise ValueError("Census fact hash mismatch")
        if __package__:
            from .validate_sec_foreign_share_census import validate_row
        else:
            from validate_sec_foreign_share_census import validate_row
        validate_row(row)
        document = documents[package]
        if package in seen:
            raise ValueError("Census artifact contains duplicate source rows")
        seen.add(package)
        derived_status = ("conflicting" if row["conflicting"] else "complete" if row["complete"]
                          else "incomplete" if row.get("evidence_text") else "none")
        if (row["status"] != derived_status or document.get("census_status") != derived_status
                or document.get("census_fact_hash") != row["fact_hash"]):
            raise ValueError("Census source status or fact hash differs from manifest")
        for key in ("cik", "adsh", "form", "filed", "source_url", "source_sha256", "publication_floor_on", "parser_version"):
            if str(row.get(key)) != str(document.get(key)):
                raise ValueError("Census provenance differs from source manifest: " + key)
        if row["status"] != "none":
            grouped.setdefault(package, []).append(row)
    if seen != set(documents):
        raise ValueError("Census artifact is missing manifested source rows")
    counters = {"inserted": 0, "retired": 0, "unchanged": 0, "sources": 0}
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(79311, 173)")
        require_schema(cursor)
        cursor.execute("SELECT source_package,cik,adsh,source_sha256,last_loaded_on "
                       "FROM public.sec_foreign_share_census_sources FOR UPDATE")
        prior_sources = {}
        prior_accessions = {}
        for package, cik, adsh, sha, loaded in cursor.fetchall():
            prior_sources[package] = (sha, loaded)
            identity = (int(cik), adsh)
            prior_accessions[identity] = max(loaded, prior_accessions.get(identity, loaded))
        cursor.execute("SELECT source_package,fact_hash,available_on "
                       "FROM public.sec_foreign_share_census WHERE retired_on IS NULL")
        current: dict[str, dict[str, date]] = {}
        for package, fact_hash, available in cursor.fetchall():
            current.setdefault(package, {})[fact_hash] = available
        insert_sql = ("INSERT INTO public.sec_foreign_share_census (" + ",".join(FACT_COLUMNS) + ") VALUES ("
                      + ",".join(["%s"] * len(FACT_COLUMNS)) + ")")
        retire_sql = ("UPDATE public.sec_foreign_share_census SET retired_on=%s,retired_reason=%s "
                      "WHERE source_package=%s AND retired_on IS NULL AND fact_hash=ANY(%s)")
        source_sql = """INSERT INTO public.sec_foreign_share_census_sources
            (source_package,adsh,cik,source_url,source_sha256,parser_version,first_loaded_on,last_loaded_on,census_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(source_package) DO UPDATE SET source_sha256=EXCLUDED.source_sha256,
            parser_version=EXCLUDED.parser_version,last_loaded_on=EXCLUDED.last_loaded_on,census_count=EXCLUDED.census_count"""
        inserts, retirements, sources = [], [], []
        from psycopg.types.json import Jsonb
        for package, document in documents.items():
            facts = grouped.get(package, [])
            if len(facts) != document["census_count"] or len({r["fact_hash"] for r in facts}) != len(facts):
                raise ValueError("Census count differs from manifest or contains duplicate facts")
            prior = prior_sources.get(package)
            prior_accession = prior_accessions.get((int(document["cik"]), document["adsh"]))
            if (prior and prior[1] > observed_on) or (prior_accession and prior_accession > observed_on):
                raise ValueError("Reconciliation date cannot precede prior load")
            reason = "parser_correction" if prior and prior[0] == document["source_sha256"] else "source"
            previous = current.get(package, {})
            removed = set(previous) - {r["fact_hash"] for r in facts}
            replaced_available = max((previous[key] for key in removed), default=None)
            if removed:
                retirements.append((observed_on, reason, package, sorted(removed)))
            for fact in facts:
                if fact["fact_hash"] in previous:
                    counters["unchanged"] += 1
                    continue
                source_available = max(date.fromisoformat(fact["filed"]) + timedelta(days=1),
                                       date.fromisoformat(fact["publication_floor_on"]))
                available = (max(source_available, replaced_available or source_available)
                             if reason == "parser_correction" else
                             max(source_available, observed_on) if prior_accession else source_available)
                values = {**fact, "available_on": available, "loaded_on": observed_on}
                for key in ("classes", "cross_checks"):
                    values[key] = Jsonb(values[key])
                inserts.append(tuple(values.get(key) for key in FACT_COLUMNS))
                counters["inserted"] += 1
            sources.append((package, document["adsh"], document["cik"], document["source_url"],
                            document["source_sha256"], document["parser_version"], observed_on, observed_on, len(facts)))
            counters["sources"] += 1
        # Validation above completes before any writes. Transaction ownership is the caller's.
        for sql, values in ((retire_sql, retirements), (insert_sql, inserts), (source_sql, sources)):
            for offset in range(0, len(values), 500):
                cursor.executemany(sql, values[offset:offset + 500])
                if sql == retire_sql:
                    counters["retired"] += cursor.rowcount
    return counters


def verified_artifact(manifest_path: Path, output: Path) -> tuple[dict, list[dict]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw = output.read_bytes()
    if w1c.digest(raw) != manifest.get("census_sha256"):
        raise ValueError("Census artifact hash mismatch")
    return manifest, [json.loads(line) for line in raw.splitlines() if line.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--raw-cache-dir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--w1-counts", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--database-url-env", default="FOREIGN_CENSUS_DATABASE_URL")
    parser.add_argument("--observed-on", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    args = parser.parse_args(argv)
    if not 1 <= args.workers <= 12:
        parser.error("--workers must be between 1 and 12")
    if not args.offline and not args.apply:
        parser.error("Choose --offline replay or --apply of a verified artifact")
    manifest_path = args.manifest
    if args.offline:
        if not args.raw_cache_dir or not args.w1_counts:
            parser.error("--offline requires --raw-cache-dir and --w1-counts")
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        manifest["input_manifest_sha256"] = w1c.digest(args.manifest.read_bytes())
        manifest = replay(manifest, args.cache_dir, args.raw_cache_dir, args.output, args.w1_counts, args.workers)
        manifest_path = args.cache_dir / "manifest.json"
        print(w1c.canonical_json(manifest["run_metrics"]), flush=True)
        if not manifest["census_complete"]:
            return 2
    if args.apply:
        manifest, rows = verified_artifact(manifest_path, args.output)
        dsn = os.environ.get(args.database_url_env)
        if not dsn:
            raise ValueError("Configured census database URL variable is empty")
        import psycopg
        with psycopg.connect(dsn) as connection:
            result = apply_census(connection, manifest, rows, args.observed_on)
        print(w1c.canonical_json({"event": "census_applied", **result}), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        # Database/transport errors must never serialize connection credentials.
        print(w1c.canonical_json({"status": "failed", "error_type": type(error).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
