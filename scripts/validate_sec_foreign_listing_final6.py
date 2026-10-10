"""Capture and compare W1c final5/final6 answers using disposable local PG18.

This helper performs read-only database work and never loads an artifact. Load
each artifact into a fresh database in the same disposable container first:
the B1 isolated comparison is preserved independently of B1b's applied-over-final5
restatement acceptance.
The saved final5 cohorts are the query and reviewed-answer oracle. All generated
reports must be written to C:, outside the repository and original raw cache.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import datetime as dt
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
from typing import Any


REPOSITORY = Path(__file__).resolve().parents[1]
YEAR_ENDS = ("2010-12-31", "2015-12-31", "2020-12-31", "2025-12-31")
COHORT_SIZES = {"acceptance": 17, "frozen30": 30, "changed10": 10}
SEMANTIC_FIELDS = ("status", "listed_type", "listing_status", "ratio_status", "ratio")
REVIEW_FIELDS = (
    "id", "fact_hash", "cik", "symbol", "adsh", "form", "filed",
    "available_on", "source_available_on", "effective_from", "effective_to",
    "underlying_class", "ordinary_candidate", "ratio_numerator", "ratio_denominator",
    "source_kind", "evidence_kind", "effective_date_explicit",
    "ratio_change_program_key", "ratio_change_correction_kind",
    "ratio_change_correction_text", "ratio_effectiveness_pending",
    "ratio_effectiveness_pending_text", "ratio_effectiveness_conditions",
    "ratio_effectiveness_confirmed", "ratio_effectiveness_confirmed_conditions",
    "ratio_effectiveness_confirmation_text", "operative_date_conflict",
    "operative_date_candidates", "operative_date_conflict_text", "evidence_text",
    "evidence_location", "source_package", "source_url", "source_sha256",
    "parser_version",
)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, ensure_ascii=False) + "\n",
                    encoding="utf-8", newline="\n")


def output_directory(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.drive.upper() != "C:" or resolved.is_relative_to(REPOSITORY):
        raise ValueError("New validation artifacts must be written to C:, outside the repository")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def semantic_answer(row: dict) -> dict:
    answer = {field: row[field] for field in SEMANTIC_FIELDS[:-1]}
    numerator, denominator = row.get("ratio_numerator"), row.get("ratio_denominator")
    ratio = None
    if numerator is not None and denominator is not None:
        exact = Fraction(str(numerator)) / Fraction(str(denominator))
        ratio = f"{exact.numerator}/{exact.denominator}"
    answer["ratio"] = ratio
    return answer


def query_key(row: dict) -> tuple[int, str, str]:
    return int(row["cik"]), row["symbol"], row["as_of"]


def reviewed_cohorts(directory: Path) -> dict:
    cohorts = {}
    for name, size in COHORT_SIZES.items():
        path = directory / f"sec-foreign-listing-20261009-run2-final5-{name}.json"
        cases = read_json(path)["cases"]
        if len(cases) != size or len({query_key(row) for row in cases}) != size:
            raise ValueError(f"The frozen {name} cohort must contain {size} distinct queries")
        if name != "acceptance" and any(
                row["final4_reviewed_answer"] != row["final5_answer"] for row in cases):
            raise ValueError(f"The {name} cohort no longer matches its reviewed final5 answers")
        cohorts[name] = {
            "path": str(path.resolve()), "sha256": sha256(path),
            "cases": [{"cik": row["cik"], "symbol": row["symbol"], "as_of": row["as_of"],
                       "reviewed_expected": row["final5_answer"],
                       **({"historical_final4_expected": row["expected"]}
                          if name == "acceptance" else {})} for row in cases],
        }
    return cohorts


def artifact_inputs(manifest_path: Path, evidence_path: Path) -> tuple[dict, list[dict]]:
    manifest = read_json(manifest_path)
    if not manifest.get("complete") or not manifest.get("parse_complete"):
        raise ValueError("Only a complete parse manifest may be validated")
    if sha256(evidence_path) != manifest.get("evidence_sha256"):
        raise ValueError("Evidence JSONL does not match its manifest hash")
    with evidence_path.open(encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    if len(rows) != manifest.get("evidence_count"):
        raise ValueError("Evidence row count does not match its manifest")
    return manifest, rows


def local_dsn(environment_name: str, expected_database_name: str) -> str:
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.environ.get(environment_name)
    if not dsn:
        raise ValueError(f"Environment variable {environment_name} is empty")
    info = conninfo_to_dict(dsn)
    if info.get("host") not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Validation requires an explicitly addressed loopback database")
    if info.get("hostaddr", info["host"]) not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Validation hostaddr must also be loopback")
    if info.get("port") == "65432" or info.get("dbname") == "market":
        raise ValueError("The production tunnel/database is forbidden")
    if info.get("dbname") != expected_database_name:
        raise ValueError("The database URL does not name the expected disposable database")
    return dsn


def verify_loaded_artifact(connection: Any, manifest: dict, artifact_rows: list[dict]) -> list[dict]:
    sources = connection.execute("SELECT * FROM public.sec_foreign_listing_sources").fetchall()
    documents = {row["source_package"]: row for row in manifest["documents"]}
    if len(sources) != len(documents):
        raise ValueError("Disposable database source count does not match the artifact")
    for source in sources:
        expected = documents.get(source["source_package"])
        if expected is None or any(source[field] != expected[field] for field in (
                "adsh", "cik", "source_url", "source_sha256", "parser_version", "evidence_count")):
            raise ValueError("Disposable database source identity does not match the artifact")
    rows = connection.execute(
        "SELECT * FROM public.sec_foreign_listing_evidence WHERE retired_on IS NULL ORDER BY id"
    ).fetchall()
    expected_facts = {(row["source_package"], row["fact_hash"]) for row in artifact_rows}
    actual_facts = {(row["source_package"], row["fact_hash"]) for row in rows}
    if len(rows) != len(artifact_rows) or actual_facts != expected_facts:
        raise ValueError("Disposable database active facts do not match the artifact")
    if connection.execute(
            "SELECT count(*) AS n FROM public.sec_foreign_listing_evidence WHERE retired_on IS NOT NULL"
    ).fetchone()["n"]:
        raise ValueError("Use a fresh artifact load; retained revisions obscure the historical comparison")
    return rows


def capture(args: argparse.Namespace) -> int:
    import psycopg
    from psycopg.rows import dict_row

    output = output_directory(args.output_dir)
    manifest, artifact_rows = artifact_inputs(args.manifest, args.evidence)
    universe = read_json(args.universe)
    observations = read_json(args.observations)
    current_statuses = read_json(args.current_statuses) if args.current_statuses else []
    cohorts = reviewed_cohorts(args.reviewed_reports)
    lines = sorted({(int(row["cik"]), row["symbol"]) for row in universe})
    queries: dict[tuple[int, str, str], set[str]] = defaultdict(set)
    for cik, symbol in lines:
        for day in YEAR_ENDS:
            queries[(cik, symbol, day)].add("year_end")
    for name, cohort in cohorts.items():
        for case in cohort["cases"]:
            queries[query_key(case)].add(name)
    results = []
    dsn = local_dsn(args.database_url_env, args.expected_database_name)
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL statement_timeout = '120s'")
        database = connection.execute(
            "SELECT current_database() AS name, current_setting('server_version') AS version, "
            "current_setting('transaction_read_only') AS read_only"
        ).fetchone()
        if database["name"] != args.expected_database_name or not database["version"].startswith("18."):
            raise ValueError("Validation requires the named disposable PostgreSQL 18 database")
        evidence = verify_loaded_artifact(connection, manifest, artifact_rows)
        ordered_queries = sorted(queries)
        for offset in range(0, len(ordered_queries), 64):
            batch = ordered_queries[offset:offset + 64]
            values = ",".join(["(%s::bigint,%s::text,%s::date)"] * len(batch))
            parameters = [value for key in batch for value in key]
            answer_rows = connection.execute(
                f"SELECT q.cik,q.symbol,q.as_of,r.* FROM (VALUES {values}) "
                "AS q(cik,symbol,as_of) CROSS JOIN LATERAL "
                "public.sec_foreign_listing_at(q.cik,q.symbol,q.as_of) r "
                "ORDER BY q.cik,q.symbol,q.as_of", parameters,
            ).fetchall()
            if len(answer_rows) != len(batch):
                raise ValueError("Resolver failed to return exactly one answer for every query")
            for row in answer_rows:
                row["as_of"] = str(row["as_of"])
                results.append({"cik": row["cik"], "symbol": row["symbol"], "as_of": row["as_of"],
                                "groups": sorted(queries[query_key(row)]),
                                "answer": semantic_answer(row), "evidence_ids": row["evidence_ids"]})
            if offset == 0 or offset + len(batch) == len(ordered_queries) or offset % 1024 == 0:
                print(json.dumps({"event": "capture_progress", "label": args.label,
                                  "completed": offset + len(batch), "total": len(ordered_queries)}), flush=True)
    by_query = {query_key(row): row for row in results}
    coverage = []
    refused = {(int(row["cik"]), row["symbol"]) for row in current_statuses if row["status"] == "refused"}
    for day in YEAR_ENDS:
        dated = [by_query[(cik, symbol, day)] for cik, symbol in lines]
        evidenced = {(int(row["cik"]), row["ticker_key"]) for row in observations
                     if row["available_on"] <= day and (not row.get("retired_on") or row["retired_on"] > day)}
        evidenced.intersection_update(lines)
        counts = Counter(row["answer"]["status"] for row in dated)
        coverage.append({
            "as_of": day, "foreign_lines": len(lines), "w1_evidenced_lines": len(evidenced),
            "resolved_both": counts["resolved"],
            "resolved_type": sum(row["answer"]["listing_status"] == "resolved" for row in dated),
            "resolved_among_w1_evidenced": sum(row["answer"]["status"] == "resolved" and
                                               (row["cik"], row["symbol"]) in evidenced for row in dated),
            "resolved_among_current_refused": sum(row["answer"]["status"] == "resolved" and
                                                   (row["cik"], row["symbol"]) in refused for row in dated),
            "ambiguous": counts["ambiguous"], "none": counts["none"],
            "resolved_types": dict(Counter(row["answer"]["listed_type"] for row in dated
                                           if row["answer"]["status"] == "resolved")),
        })
    cohort_reports = {}
    for name, cohort in cohorts.items():
        cases = [{**case, f"{args.label}_answer": by_query[query_key(case)]["answer"],
                  "matches_reviewed_expected": by_query[query_key(case)]["answer"] == case["reviewed_expected"]}
                 for case in cohort["cases"]]
        report = {"reviewed_cohort_path": cohort["path"], "reviewed_cohort_sha256": cohort["sha256"],
                  "passed": sum(case["matches_reviewed_expected"] for case in cases),
                  "total": len(cases), "cases": cases}
        cohort_reports[name] = report
        write_json(output / f"{name}.json", report)
    snapshot = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "label": args.label,
        "database": database, "schema_sha256": sha256(REPOSITORY / "schemas/sec_foreign_listing_evidence.sql"),
        "inputs": {"manifest": {"path": str(args.manifest.resolve()), "sha256": sha256(args.manifest)},
                   "evidence": {"path": str(args.evidence.resolve()), "sha256": sha256(args.evidence)},
                   "universe": {"path": str(args.universe.resolve()), "sha256": sha256(args.universe)},
                   "observations": {"path": str(args.observations.resolve()), "sha256": sha256(args.observations)},
                   "current_statuses": {"path": str(args.current_statuses.resolve()),
                                        "sha256": sha256(args.current_statuses)} if args.current_statuses else None},
        "source_count": len(manifest["documents"]), "evidence_count": len(evidence),
        "confirmation_text_rows": sum(bool(row.get("ratio_effectiveness_confirmation_text")) for row in evidence),
        "confirmed_rows": sum(bool(row.get("ratio_effectiveness_confirmed")) for row in evidence),
        "query_count": len(results), "coverage": coverage, "cohorts": cohort_reports,
        "results": results, "evidence": [{key: row.get(key) for key in REVIEW_FIELDS} for row in evidence],
    }
    write_json(output / "snapshot.json", snapshot)
    write_json(output / "coverage.json", {key: value for key, value in snapshot.items()
                                          if key not in {"results", "evidence", "cohorts"}})
    print(json.dumps({"event": "captured", "label": args.label, "queries": len(results),
                      "sources": snapshot["source_count"], "evidence": snapshot["evidence_count"],
                      "confirmation_text_rows": snapshot["confirmation_text_rows"],
                      "cohorts": {name: f"{report['passed']}/{report['total']}"
                                  for name, report in cohort_reports.items()}}), flush=True)
    return 0


def confirmation_key(row: dict) -> tuple:
    return tuple(str(row.get(field)) for field in (
        "source_package", "source_kind", "evidence_kind", "symbol", "underlying_class",
        "ratio_numerator", "ratio_denominator", "effective_from", "effective_to", "evidence_text",
    ))


def compare(args: argparse.Namespace) -> int:
    output = output_directory(args.output_dir)
    baseline, candidate = read_json(args.baseline_snapshot), read_json(args.candidate_snapshot)
    for field in ("universe", "observations", "current_statuses"):
        if baseline["inputs"][field] != candidate["inputs"][field]:
            raise ValueError(f"Comparison inputs differ: {field}")
    for name in COHORT_SIZES:
        if baseline["cohorts"][name]["reviewed_cohort_sha256"] != candidate["cohorts"][name]["reviewed_cohort_sha256"]:
            raise ValueError("The reviewed cohort changed between captures")
    before = {query_key(row): row for row in baseline["results"]}
    after = {query_key(row): row for row in candidate["results"]}
    if before.keys() != after.keys():
        raise ValueError("Baseline and candidate must cover identical query keys")
    facts_before = {row["id"]: row for row in baseline["evidence"]}
    facts_after = {row["id"]: row for row in candidate["evidence"]}
    changes = []
    for key in sorted(before):
        old, new = before[key], after[key]
        if old["answer"] == new["answer"]:
            continue
        toward_fail_closed = (
            old["answer"]["status"] == "resolved" and new["answer"]["status"] in {"ambiguous", "none"}
        ) or (
            old["answer"]["status"] == "none" and new["answer"]["status"] == "ambiguous"
        )
        changes.append({"cik": key[0], "symbol": key[1], "as_of": key[2], "groups": old["groups"],
                        "before": old["answer"], "after": new["answer"],
                        "before_evidence_ids": old["evidence_ids"], "after_evidence_ids": new["evidence_ids"],
                        "direction": "towards_fail_closed" if toward_fail_closed else "source_justification_required",
                        "manual_review_status": "required",
                        "before_facts": [facts_before[value] for value in old["evidence_ids"]],
                        "after_facts": [facts_after[value] for value in new["evidence_ids"]],
                        "issuer_controls_before": [row for row in baseline["evidence"] if row["cik"] == key[0]
                                                   and (row.get("ratio_effectiveness_pending")
                                                        or row.get("ratio_effectiveness_confirmation_text"))],
                        "issuer_controls_after": [row for row in candidate["evidence"] if row["cik"] == key[0]
                                                  and (row.get("ratio_effectiveness_pending")
                                                       or row.get("ratio_effectiveness_confirmation_text"))]})
    old_confirmations = [row for row in baseline["evidence"] if row.get("ratio_effectiveness_confirmation_text")]
    new_confirmations = [row for row in candidate["evidence"] if row.get("ratio_effectiveness_confirmation_text")]
    old_by_key: dict[tuple, list[dict]] = defaultdict(list)
    new_by_key: dict[tuple, list[dict]] = defaultdict(list)
    for row in old_confirmations:
        old_by_key[confirmation_key(row)].append(row)
    for row in new_confirmations:
        new_by_key[confirmation_key(row)].append(row)
    removed, added = [], []
    for key in sorted(old_by_key.keys() | new_by_key.keys()):
        old_rows, new_rows = old_by_key[key], new_by_key[key]
        paired_count = min(len(old_rows), len(new_rows))
        removed.extend(old_rows[paired_count:])
        added.extend(new_rows[paired_count:])
    confirmation_report = {
        "before_text_rows": len(old_confirmations), "after_text_rows": len(new_confirmations),
        "before_confirmed_rows": baseline["confirmed_rows"], "after_confirmed_rows": candidate["confirmed_rows"],
        "removed_or_replaced_extractions": len(removed), "added_or_replaced_extractions": len(added),
        "identity_note": "Pairs source/program/class/ratio/date/evidence text one row at a time, preserving duplicate multiplicity and ignoring parser version and fact hash; a narrowed narrative extraction may replace an old broad row.",
        "removed_or_replaced": [{"manual_review_status": "required", **row} for row in removed],
        "added_or_replaced": added,
        "all_before": old_confirmations, "all_after": new_confirmations,
    }
    report = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "baseline_snapshot_sha256": sha256(args.baseline_snapshot),
        "candidate_snapshot_sha256": sha256(args.candidate_snapshot),
        "query_count": len(before), "total_changed_answers": len(changes),
        "changed_distinct_lines": len({(row["cik"], row["symbol"]) for row in changes}),
        "changed_counts_by_date": dict(Counter(row["as_of"] for row in changes)),
        "directions": dict(Counter(row["direction"] for row in changes)), "cases": changes,
        "confirmation_summary": {key: value for key, value in confirmation_report.items()
                                 if key not in {"removed_or_replaced", "added_or_replaced", "all_before", "all_after"}},
    }
    write_json(output / "changed-answers.json", report)
    write_json(output / "confirmation-diff.json", confirmation_report)
    write_json(output / "source-review-packet.json", {"manual_review_status": "required", "cases": changes})
    lines = ["| CIK | Line | Date | Final5 | Final6 | Direction |", "|---|---|---|---|---|---|"]
    def describe(answer: dict) -> str:
        return f"{answer['status']} {answer['listed_type'] or ''} {answer['ratio'] or ''}".strip()

    for row in changes:
        lines.append(f"| {row['cik']} | {row['symbol']} | {row['as_of']} | {describe(row['before'])} | "
                     f"{describe(row['after'])} | {row['direction']} |")
    (output / "changed-answers.md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps({key: value for key, value in report.items() if key != "cases"}, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture_parser = commands.add_parser("capture", help="Read-only snapshot of a freshly loaded artifact")
    capture_parser.add_argument("--label", choices=("final5", "final6"), required=True)
    for name in ("universe", "observations", "manifest", "evidence", "output-dir"):
        capture_parser.add_argument(f"--{name}", type=Path, required=True)
    capture_parser.add_argument("--current-statuses", type=Path)
    capture_parser.add_argument("--reviewed-reports", type=Path, default=REPOSITORY / "docs/validation")
    capture_parser.add_argument("--database-url-env", default="SEC_FOREIGN_TEST_DATABASE_URL")
    capture_parser.add_argument("--expected-database-name", required=True)
    comparison_parser = commands.add_parser("compare", help="Compare two saved captures without a DB connection")
    comparison_parser.add_argument("--baseline-snapshot", type=Path, required=True)
    comparison_parser.add_argument("--candidate-snapshot", type=Path, required=True)
    comparison_parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    return capture(args) if args.command == "capture" else compare(args)


if __name__ == "__main__":
    raise SystemExit(main())
