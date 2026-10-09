"""Read-only, reproducible coverage and manual-review packet for W1c evidence.

The universe is the saved production read-only export, never a present-day
vendor ticker mapping. This reports evidence coverage, not W1 admission.
"""
from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import random


YEAR_ENDS = ("2010-12-31", "2015-12-31", "2020-12-31", "2025-12-31")


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", type=Path, required=True)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--current-statuses", type=Path,
                        help="Saved read-only production W1 status results for the same universe")
    parser.add_argument("--manifest", type=Path,
                        help="Completed collection manifest, included as report provenance")
    parser.add_argument("--database-url-env", default="SEC_FOREIGN_TEST_DATABASE_URL")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--sample-seed", type=int, default=173)
    args = parser.parse_args()
    import psycopg
    from psycopg.rows import dict_row

    universe = read_json(args.universe)
    observations = read_json(args.observations)
    current_statuses = read_json(args.current_statuses) if args.current_statuses else []
    manifest = read_json(args.manifest) if args.manifest else None
    refused_today = {(int(r["cik"]), r["symbol"]) for r in current_statuses
                     if r["status"] == "refused"}
    lines = sorted({(int(row["cik"]), row["symbol"]) for row in universe})
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    summaries = []
    all_results = []
    with psycopg.connect(os.environ[args.database_url_env], row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '30s'")
        for day in YEAR_ENDS:
            evidenced = {(int(o["cik"]), o["ticker_key"]) for o in observations
                         if o["available_on"] <= day
                         and (not o.get("retired_on") or o["retired_on"] > day)}
            results = []
            for cik, symbol in lines:
                row = conn.execute(
                    "SELECT * FROM public.sec_foreign_listing_at(%s,%s,%s::date)",
                    (cik, symbol, day),
                ).fetchone()
                results.append({"cik": cik, "symbol": symbol, "as_of": day,
                                "w1_evidenced_by_date": (cik, symbol) in evidenced, **row})
            counts = Counter(row["status"] for row in results)
            summaries.append({
                "as_of": day, "foreign_lines": len(lines),
                "w1_evidenced_lines": len(evidenced),
                "resolved_both": counts["resolved"],
                "resolved_type": sum(r["listing_status"] == "resolved" for r in results),
                "resolved_among_w1_evidenced": sum(r["status"] == "resolved" and
                                                   r["w1_evidenced_by_date"] for r in results),
                "resolved_among_current_refused": sum(r["status"] == "resolved" and
                                                       (r["cik"], r["symbol"]) in refused_today
                                                       for r in results),
                "ambiguous": counts["ambiguous"], "none": counts["none"],
                "resolved_types": dict(Counter(r["listed_type"] for r in results
                                               if r["status"] == "resolved")),
            })
            all_results.extend(results)
        eligible = [r for r in all_results if r["as_of"] == YEAR_ENDS[-1]
                    and r["status"] == "resolved"]
        rng = random.Random(args.sample_seed)
        ads = [r for r in eligible if r["listed_type"] == "ads"]
        direct = [r for r in eligible if r["listed_type"] == "ordinary_direct"]
        sample = rng.sample(ads, min(args.sample_size // 2, len(ads)))
        sample += rng.sample(direct, min(args.sample_size - len(sample), len(direct)))
        if len(sample) < min(args.sample_size, len(eligible)):
            remaining = [r for r in eligible if r not in sample]
            sample += rng.sample(remaining, min(args.sample_size - len(sample), len(remaining)))
        for row in sample:
            row["evidence"] = conn.execute(
                "SELECT * FROM public.sec_foreign_listing_evidence WHERE id = ANY(%s) ORDER BY id",
                (row["evidence_ids"],),
            ).fetchall()
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "universe_sha256": hashlib.sha256(args.universe.read_bytes()).hexdigest(),
        "observations_sha256": hashlib.sha256(args.observations.read_bytes()).hexdigest(),
        "ciks": len({c for c, _ in lines}), "coverage": summaries,
        "current_w1_status_counts": dict(Counter(r["status"] for r in current_statuses)),
        "current_w1_refusal_counts": dict(Counter(r["refusal"] for r in current_statuses
                                                  if r.get("refusal"))),
        "sample_seed": args.sample_seed, "sample_size": len(sample),
        "sample_method": "Seeded sample without replacement from resolved 2025 lines, half ADS and half direct where available; spare slots filled from the other group",
        "sample_types": dict(Counter(r["listed_type"] for r in sample)),
    }
    if manifest is not None:
        summary["collection"] = {
            "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
            "discovery_complete": manifest.get("complete"),
            "parse_complete": manifest.get("parse_complete"),
            "source_documents": len(manifest["documents"]),
            "unique_urls": len({d["source_url"] for d in manifest["documents"]}),
            "source_statuses": dict(Counter(d.get("status", "unprocessed")
                                             for d in manifest["documents"])),
            "evidence_rows": manifest.get("evidence_count"),
            "evidence_sha256": manifest.get("evidence_sha256"),
        }
    repo = Path(__file__).resolve().parents[1]
    summary["implementation_sha256"] = {
        name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
        for name in ("schemas/sec_foreign_listing_evidence.sql",
                     "scripts/sec_foreign_listing_parser.py",
                     "scripts/load_sec_foreign_listing_evidence.py",
                     "scripts/run_sec_foreign_listing_evidence_shards.py",
                     "scripts/validate_sec_foreign_listing_evidence.py")
    }
    for name, data in (("coverage.json", summary), ("resolutions.json", all_results),
                       ("manual_review_packet.json", sample)):
        (output / name).write_text(json.dumps(data, indent=2, default=str) + "\n",
                                   encoding="utf-8", newline="\n")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
