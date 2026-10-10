"""Validate an offline foreign cover share census without database or network access.

Coverage uses primary annual filings, not annual-form securities exhibits. The
precision packet is deterministic; only an explicit reviewed packet certifies
manual precision. Impact measures class-scope proof potential, not admission.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
from typing import Any

if __package__:
    from .sec_foreign_share_census_parser import normalize_class_key
else:
    from sec_foreign_share_census_parser import normalize_class_key

ANNUAL_FORMS = {"20-F", "20-F/A", "40-F", "40-F/A"}
SAMPLE_SEED = "foreign-share-census-precision-v1-20261010"
NAMED_ISSUERS = {
    "DLO": (1846832, "0000950170-25-058197"),
    "BIDU": (1329099, "0001193125-25-066199"),
    "NVO": (353278, "0001628280-25-003920"),
    "TSM": (1046179, "0001193125-25-083423"),
    "ASML": (937966, "0000937966-25-000009"),
    "QGEN": (1015820, "0001015820-25-000027"),
    "ZIM": (1654126, "0001178913-25-000793"),
    "NTES": (1110646, "0001410578-25-000728"),
    "SAP": (1000184, "0001104659-25-017815"),
    "CNQ": (1017413, "0001017413-25-000024"),
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8", newline="\n")


def number(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid share count") from exc
    if not result.is_finite() or result < 0 or result != result.to_integral_value():
        raise ValueError("Share count must be finite, integral and nonnegative")
    return result


def identity(row: dict) -> tuple[int, str, str]:
    return int(row["cik"]), row["adsh"], row["source_url"]


def row_status(row: dict | None) -> str:
    if row is None:
        return "none"
    if row.get("conflicting") or row.get("status") == "conflicting":
        return "conflicting"
    if row.get("complete") is True:
        return "complete"
    return "none" if row.get("status") == "none" else "incomplete"


def validate_row(row: dict) -> None:
    classes = row.get("classes", [])
    for entry in classes:
        if not entry.get("class_name") or entry.get("class_kind") not in {
                "ordinary", "common", "ordinary_common", "preferred", "preference", "other"}:
            raise ValueError("Census class lacks original name or supported kind")
        number(entry.get("shares"))
    if row.get("complete"):
        if not classes or any(number(entry.get("shares")) is None for entry in classes):
            raise ValueError("Complete census lacks class counts")
        if row.get("numeric_residue"):
            raise ValueError("Complete census has unparsed numeric residue")
        if row.get("text_residue"):
            raise ValueError("Complete census has unsupported statement grammar")
        keys = [entry.get("class_key") or entry["class_name"].lower() for entry in classes]
        if len(keys) != len(set(keys)):
            raise ValueError("Complete census has duplicate class keys")
        computed = sum((number(entry["shares"]) for entry in classes), Decimal(0))
        if number(row.get("stated_total")) not in (None, computed):
            raise ValueError("Complete census disagrees with its stated total")
        if number(row.get("computed_total")) != computed:
            raise ValueError("Computed census sum differs from class counts")
        if not row.get("shares_as_of"):
            raise ValueError("Complete census lacks measurement date")


def sample_identity(row: dict) -> str:
    return "|".join(map(str, identity(row)))


def coverage(documents: list[dict], rows: dict[tuple, dict]) -> dict:
    annual = [document for document in documents if document.get("form") in ANNUAL_FORMS
              and document.get("document_role", "primary") == "primary"]
    if len({(int(d["cik"]), d["adsh"]) for d in annual}) != len(annual):
        raise ValueError("Primary corpus contains duplicate issuer/accession filings")
    statuses: Counter = Counter()
    by_year: dict[str, Counter] = defaultdict(Counter)
    by_form: dict[str, Counter] = defaultdict(Counter)
    missing = []
    for document in annual:
        row = rows.get(identity(document))
        status = row_status(row)
        statuses[status] += 1
        by_year[document["filed"][:4]][status] += 1
        by_form[document["form"]][status] += 1
        if row is None:
            missing.append({key: document[key] for key in ("cik", "adsh", "source_url", "filed", "form")})
    def counts(value: Counter) -> dict:
        return {"filings": sum(value.values()), **{key: value[key] for key in (
            "complete", "incomplete", "conflicting", "none")}}
    return {"denominator": "Primary annual filings; document_role absent or primary; exact 20-F/20-F/A/40-F/40-F/A",
            "year_basis": "Official filing year from pinned W1c manifest",
            "overall": counts(statuses),
            "by_year": {year: counts(value) for year, value in sorted(by_year.items())},
            "by_form": {form: counts(value) for form, value in sorted(by_form.items())},
            "excluded_annual_exhibits": sum(document.get("form") in ANNUAL_FORMS
                and document.get("document_role", "primary") != "primary" for document in documents),
            "missing_census_records": missing}


def cross_check_statistics(rows: list[dict]) -> dict:
    result = {}
    for kind in ("undimensioned", "class_dimensioned"):
        checks = [check for row in rows for check in row.get("cross_checks", {}).get(kind, [])]
        result[kind] = {"checks": len(checks), "statuses": dict(sorted(Counter(
            check["status"] for check in checks).items())),
            "date_matches": sum(check.get("date_match") is True for check in checks),
            "date_differences": sum(check.get("date_match") is False for check in checks),
            "mismatches_same_date": sum(check["status"] == "mismatch" and check.get("date_match") is True for check in checks),
            "mismatches_different_date": sum(check["status"] == "mismatch" and check.get("date_match") is False for check in checks),
            "filings_with_checks": sum(bool(row.get("cross_checks", {}).get(kind)) for row in rows)}
    result["filings_without_w1_counts"] = sum(row.get("cross_checks", {}).get("w1_same_filing_counts", 0) == 0 for row in rows)
    result["conflicting_filings"] = sum(row_status(row) == "conflicting" for row in rows)
    result["comparison_policy"] = "Active W1 counts from same CIK and accession; mismatches retain both values and conflict; dates are retained, never aligned by inference"
    return result


def normalized_label(value: Any) -> str | None:
    if not value or not isinstance(value, str):
        return None
    return normalize_class_key(value)


def source_available_on(row: dict) -> str:
    if row.get("source_available_on"):
        return str(row["source_available_on"])
    filed_next = (date.fromisoformat(row["filed"]) + timedelta(days=1)).isoformat()
    return max(filed_next, row.get("publication_floor_on") or filed_next)


def impact(sizing: dict | list, rows: list[dict]) -> dict:
    measurements = sizing["rows"] if isinstance(sizing, dict) else sizing
    refused = [row for row in measurements if str(row.get("as_of")) == "2025-12-31"
               and str(row.get("refusal", "")).startswith("share_total_class_scope_unverified")]
    by_filing: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for row in rows:
        by_filing[(int(row["cik"]), row["adsh"])].append(row)
    detail = []
    for line in refused:
        candidates = by_filing.get((int(line["cik"]), line.get("adsh")), [])
        reason = "same_accession_census_missing"
        category = None
        selected = candidates[0] if len(candidates) == 1 else None
        if len(candidates) > 1:
            reason = "same_accession_census_ambiguous"
        elif selected:
            if row_status(selected) != "complete":
                reason = "census_" + row_status(selected)
            elif source_available_on(selected) > "2025-12-31":
                reason = "census_not_public_by_cutoff"
            elif not (0 <= (date(2025, 12, 31) - date.fromisoformat(selected["shares_as_of"])).days <= 400):
                reason = "census_count_stale_or_future"
            else:
                ordinary = [entry for entry in selected["classes"] if entry["class_kind"] in
                            {"ordinary", "common", "ordinary_common"}]
                other_classes_nonordinary = all(entry in ordinary
                    or entry["class_kind"] in {"preferred", "preference"}
                    or (entry["class_kind"] == "other" and re.search(
                        r"\b(?:deferred|founder)\b", entry["class_name"], re.I))
                    for entry in selected["classes"])
                evidence = line.get("evidence") or {}
                labels = [line.get("canonical_underlying_class_id"),
                          evidence.get("listing_class"), evidence.get("ratio_class"),
                          *evidence.get("count_labels", [])]
                known = {label for value in labels
                    if (label := normalized_label(value))}
                if len(known) > 1:
                    reason = "line_class_labels_disagree"
                elif len(ordinary) == 1 and other_classes_nonordinary and number(ordinary[0]["shares"]) > 0:
                    if known and ordinary[0]["class_key"] not in known and not (
                            known <= {"ordinary", "common"} and ordinary[0]["class_key"] in {"ordinary", "common"}):
                        reason = "sole_ordinary_class_differs_from_line"
                    else:
                        category = "single_ordinary_class"
                        reason = "positive_scope_proof"
                elif len(known) == 1 and any(entry["class_key"] in known
                        and number(entry["shares"]) > 0 for entry in ordinary):
                    category = "explicit_line_class_count"
                    reason = "positive_scope_proof"
                else:
                    reason = ("other_class_kind_unverified" if len(ordinary) == 1
                              and not other_classes_nonordinary else "explicit_line_class_unbound_or_nonpositive")
        detail.append({"cik": line["cik"], "symbol": line["symbol"], "adsh": line.get("adsh"),
            "shares_as_of": line.get("shares_as_of"), "category": category, "reason": reason,
            "w1c_resolved": line.get("w1c_status") == "resolved",
            "count_non_stale": (line.get("evidence") or {}).get("count_non_stale") is True,
            "same_w1_count_date": bool(selected) and selected.get("shares_as_of") == str(line.get("shares_as_of")),
            "census_shares_as_of": selected.get("shares_as_of") if selected else None,
            "census_status": row_status(selected),
            "census_source_url": selected.get("source_url") if selected else None})
    categories = Counter(row["category"] for row in detail if row["category"])
    eligible = [row for row in detail if row["category"] and row["w1c_resolved"] and row["count_non_stale"]]
    return {"as_of": "2025-12-31", "refusal_code": "share_total_class_scope_unverified",
            "sizing_provenance": {"scope": "Local PG18 restored W1/W1c exports with the corrected sibling sizing SQL bytes; schema hashes identify measured code, not a committed PR head or current production behavior",
                                  "metadata": sizing.get("metadata") if isinstance(sizing, dict) else None},
            "refusing_lines": len(refused), "refusing_ciks": len({row["cik"] for row in refused}),
            "scope_proof_lines": sum(categories.values()), "categories": dict(categories),
            "scope_proof_ciks": len({row["cik"] for row in detail if row["category"]}),
            "scope_proof_with_w1c_resolved_and_non_stale": len(eligible),
            "scope_proof_same_w1_count_date_lines": sum(bool(row["category"]) and row["same_w1_count_date"] for row in detail),
            "refusal_reasons": dict(sorted(Counter(row["reason"] for row in detail if not row["category"]).items())),
            "method": "Same-accession complete nonconflicting census public by cutoff with its own measurement date 0..400 days old; all known line class labels must agree; count is positive. Sole-ordinary proof requires every other class explicitly preferred/preference, deferred or founder; untyped other classes cannot prove it. The census supplies its own dated count, and the same-W1-count-date subset is separate. Categories are mutually exclusive, sole ordinary first. Other sizing/price/unit/action/currency gates remain required.",
            "lines": detail}


def precision_packet(rows: list[dict], census_sha: str, review_path: Path | None,
                     raw_cache_dir: Path | None = None, stage_dir: Path | None = None) -> dict:
    complete = [row for row in rows if row_status(row) == "complete"]
    sample = sorted(complete, key=lambda row: hashlib.sha256((SAMPLE_SEED + "|" +
        sample_identity(row)).encode()).hexdigest())[:40]
    records = [{"sample_id": sample_identity(row), "cik": row["cik"], "adsh": row["adsh"],
                "source_url": row["source_url"], "source_sha256": row["source_sha256"],
                "classes": row["classes"], "shares_as_of": row["shares_as_of"],
                "stated_total": row.get("stated_total"), "computed_total": row.get("computed_total"),
                "source_text": row.get("source_text", ""), "source_location": row.get("source_location"),
                "review_status": "pending"} for row in sample]
    if raw_cache_dir is not None:
        if stage_dir is None:
            raise ValueError("Source verification needs a separate staging directory")
        if __package__:
            from . import load_sec_foreign_listing_evidence as w1c
            from .sec_foreign_listing_parser import _Document
        else:
            import load_sec_foreign_listing_evidence as w1c
            from sec_foreign_listing_parser import _Document
        client = w1c.SecClient(stage_dir, offline=True, raw_cache_dir=raw_cache_dir)
        for entry in records:
            raw, actual = client.document(entry["source_url"], expected_sha256=entry["source_sha256"])
            if raw.lstrip().startswith(b"%PDF-"):
                pages, _ = w1c.extract_pdf_pages(raw, cache_dir=stage_dir)
                html, _, _ = w1c.pdf_parser_input(pages)
            else:
                html = raw.decode("utf-8-sig", errors="replace")
            text = _Document(html).text
            offset = text.find(entry["source_text"])
            if offset < 0 or not entry["source_text"]:
                raise ValueError("Precision statement quote does not match verified source text")
            entry["source_verified_sha256"] = actual
            entry["source_context"] = text[max(0, offset - 500):offset + len(entry["source_text"]) + 700]
    report = {"seed": SAMPLE_SEED, "selection": "Lowest SHA256(seed|CIK|accession|source URL) over complete nonconflicting censuses",
              "census_sha256": census_sha, "eligible": len(complete), "sample_size": len(sample),
              "reviewed": 0, "correct": 0, "incorrect": 0, "precision": None,
              "review_status": "pending", "records": records}
    if review_path:
        review = json.loads(review_path.read_text(encoding="utf-8"))
        if review["census_sha256"] != census_sha:
            raise ValueError("Precision review is for a different census artifact")
        review_rows = {entry["sample_id"]: entry for entry in review["records"]}
        if set(review_rows) != {entry["sample_id"] for entry in records}:
            raise ValueError("Precision review does not cover the exact deterministic sample")
        for entry in records:
            reviewed = review_rows[entry["sample_id"]]
            if reviewed.get("review_status") not in ("correct", "incorrect") or not reviewed.get("review_note"):
                raise ValueError("Precision review lacks an explicit source judgment")
            entry.update(review_status=reviewed["review_status"], review_note=reviewed["review_note"])
        report["reviewed"] = len(records)
        report["correct"] = sum(entry["review_status"] == "correct" for entry in records)
        report["incorrect"] = len(records) - report["correct"]
        report["precision"] = report["correct"] / len(records) if records else None
        report["review_status"] = "complete"
        report["review_file_sha256"] = sha256(review_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", type=Path, required=True)
    parser.add_argument("--corpus-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sizing-rows", type=Path)
    parser.add_argument("--precision-review", type=Path)
    parser.add_argument("--raw-cache-dir", type=Path,
                        help="Verify the 40 source hashes/quotes and include nearby text; offline only")
    args = parser.parse_args()
    raw_root = args.corpus_manifest.resolve().parent
    if args.output_dir.resolve().is_relative_to(raw_root):
        raise ValueError("Validation output must be outside the raw corpus")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(args.corpus_manifest.read_text(encoding="utf-8"))
    parsed = [json.loads(line) for line in args.census.read_text(encoding="utf-8").splitlines() if line]
    rows = {}
    for row in parsed:
        validate_row(row)
        key = identity(row)
        if key in rows:
            raise ValueError("Duplicate census source identity")
        rows[key] = row
    census_hash = sha256(args.census)
    result = {"census_sha256": census_hash, "corpus_manifest_sha256": sha256(args.corpus_manifest),
              "coverage": coverage(manifest["documents"], rows),
              "cross_checks": cross_check_statistics(parsed),
              "precision": precision_packet(parsed, census_hash, args.precision_review,
                  args.raw_cache_dir, args.output_dir / "source-stage")}
    named = []
    for symbol, (cik, adsh) in NAMED_ISSUERS.items():
        selected = [row for row in parsed if int(row["cik"]) == cik and row["adsh"] == adsh]
        named.append({"symbol": symbol, "cik": cik, "adsh": adsh, "censuses": selected,
                      "review_status": "pending"})
    write_json(args.output_dir / "named-censuses.json", named)
    write_json(args.output_dir / "precision-sample.json", result["precision"])
    if args.sizing_rows:
        sizing = json.loads(args.sizing_rows.read_text(encoding="utf-8"))
        result["sizing_rows_sha256"] = sha256(args.sizing_rows)
        result["impact"] = impact(sizing, parsed)
        write_json(args.output_dir / "sizing-impact.json", result["impact"])
    write_json(args.output_dir / "validation.json", result)
    print(json.dumps({"coverage": result["coverage"]["overall"],
        "precision_review_status": result["precision"]["review_status"],
        "impact_scope_proof_lines": result.get("impact", {}).get("scope_proof_lines")}, sort_keys=True))


if __name__ == "__main__":
    main()
