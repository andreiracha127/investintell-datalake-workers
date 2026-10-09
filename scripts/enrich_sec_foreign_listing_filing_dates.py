"""Replace filing-search acceptance dates with authoritative SEC filing dates.

Quarterly master indexes are matched by accession across all cofilers. Cached
SEC submission headers and the supplied read-only W1 export are explicit
fallbacks. Unresolved or conflicting dates never revert to API acceptance dates.
The input manifest and any existing shard plan remain untouched.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
import io
import json
from pathlib import Path
import re

if __package__:
    from . import load_sec_foreign_listing_evidence as loader
else:
    import load_sec_foreign_listing_evidence as loader

DATE_PROOF_VERSION = "sec-official-filing-date-v2"


def adjacent_quarters(value: str) -> set[tuple[int, int]]:
    parsed = date.fromisoformat(value[:10])
    serial = parsed.year * 4 + (parsed.month - 1) // 3
    return {(number // 4, number % 4 + 1) for number in (serial - 1, serial, serial + 1) if number // 4 >= 1993}


def load_index(client: loader.SecClient, root: Path, year: int, quarter: int, *, download: bool) -> tuple[bytes, dict]:
    path = root / f"master-{year}-Q{quarter}.idx"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    sidecar = path.with_suffix(".idx.json")
    if path.exists():
        raw = path.read_bytes()
        if sidecar.exists():
            metadata = json.loads(sidecar.read_text(encoding="utf-8"))
            if metadata.get("source_url") != url or metadata.get("source_sha256") != loader.digest(raw):
                raise ValueError("SEC quarterly index cache integrity mismatch")
    elif download:
        try:
            raw = client.request(url)
        except RuntimeError:
            if not client.key:
                raise
            raw = client.request(url.replace("https://www.sec.gov", "https://edgar-mirror.sec-api.io", 1))
        if b"CIK|Company Name|Form Type|Date Filed|Filename" not in raw[:20000]:
            raise ValueError("SEC quarterly index response has no official master-index header")
        loader.write_bytes(path, raw)
        loader.write_json(sidecar, {"source_url": url, "source_sha256": loader.digest(raw), "bytes": len(raw)})
    else:
        raise FileNotFoundError(f"Missing cached quarterly index {year} Q{quarter}")
    if b"CIK|Company Name|Form Type|Date Filed|Filename" not in raw[:20000]:
        raise ValueError("SEC quarterly index cache has no official master-index header")
    return raw, {"source_url": url, "source_sha256": loader.digest(raw)}


def index_dates(raw: bytes, provenance: dict, accessions: set[str]) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for binary_line in io.BytesIO(raw):
        fields = binary_line.rstrip(b"\r\n").split(b"|")
        if len(fields) != 5:
            continue
        path = re.fullmatch(rb"edgar/data/(\d+)/(\d{10}-\d{2}-\d{6})\.txt", fields[4].strip())
        if path is None:
            continue
        accession = path.group(2).decode("ascii")
        if accession not in accessions:
            continue
        if not fields[0].strip().isdigit() or int(fields[0]) != int(path.group(1)):
            raise ValueError("SEC quarterly index CIK disagrees with its archival path")
        filed = date.fromisoformat(fields[3].strip().decode("ascii"))
        record = {**provenance, "line": binary_line.rstrip(b"\r\n").decode("utf-8", errors="replace"),
                  "filed": str(filed), "cik": int(fields[0])}
        result.setdefault(accession, []).append(record)
    return result


def sgml_filed_date(raw: bytes, accession: str) -> str | None:
    header = raw[:65536]
    identity = re.search(rb"(?im)^(?:ACCESSION NUMBER:|<ACCESSION-NUMBER>)[ \t]*([0-9-]+)", header)
    if identity is None:
        identity = re.search(rb"(?im)^<SEC-DOCUMENT>([0-9-]+)\.txt", header)
    if identity is None or identity.group(1).decode("ascii") != accession:
        return None
    matches = re.findall(rb"(?im)^(?:FILED AS OF DATE:|<FILING-DATE>)[ \t]*(\d{8})\b", header)
    dates = {date(int(value[:4]), int(value[4:6]), int(value[6:8])).isoformat() for value in matches}
    if len(dates) > 1:
        raise ValueError("SEC submission header contains conflicting filing dates")
    return next(iter(dates)) if dates else None


def daily_index_proof(client: loader.SecClient, root: Path, documents: list[dict], *, download: bool) -> dict | None:
    """Recover a historical accession retained in a daily, but revised quarterly index."""
    accession = documents[0]["adsh"]
    dates = set()
    for document in documents:
        hint = date.fromisoformat(document["query_accepted_on"][:10])
        dates.update(hint + timedelta(days=delta) for delta in range(-7, 8))
    records = []
    missing = []
    for day in sorted(dates):
        path = root / ("master." + day.strftime("%Y%m%d") + ".idx")
        url = f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{(day.month - 1) // 3 + 1}/master.{day:%Y%m%d}.idx"
        if path.exists():
            raw = path.read_bytes()
            records.extend(index_dates(raw, {"source_url": url, "source_sha256": loader.digest(raw)}, {accession}).get(accession, []))
        else:
            missing.append((day, path, url))
    if not records and download:
        hints = {date.fromisoformat(document["query_accepted_on"][:10]) for document in documents}
        missing.sort(key=lambda item: (min(abs((item[0] - hint).days) for hint in hints), item[0]))
        for day, path, url in missing:
            if day > date.today():
                continue
            try:
                raw = client.request(url)
            except (RuntimeError, OSError):
                continue
            if b"CIK|Company Name|Form Type|Date Filed|" not in raw[:20000]:
                continue
            loader.write_bytes(path, raw)
            records.extend(index_dates(raw, {"source_url": url, "source_sha256": loader.digest(raw)}, {accession}).get(accession, []))
            if records:
                break
    filed_dates = {record["filed"] for record in records}
    if len(filed_dates) > 1:
        return {"source": "sec_daily_master_index", "status": "ambiguous", "records": records}
    if filed_dates:
        return {"source": "sec_daily_master_index", "filed": next(iter(filed_dates)), "records": records}
    return None


def _submission_proof(client: loader.SecClient, documents: list[dict], *, download: bool) -> dict | None:
    accessions = {document["adsh"] for document in documents}
    if len(accessions) != 1:
        raise ValueError("Submission lookup requires exactly one accession")
    accession = next(iter(accessions))
    urls = set()
    for document in documents:
        locator = loader.sgml_recovery_locator(document["source_url"])
        if locator:
            urls.add(locator[0])
        elif document["source_url"].endswith(accession + ".txt"):
            urls.add(document["source_url"])
    cached = loader.SecClient(client.cache, offline=True)
    reader = client if download or client.offline else cached
    records = []
    for url in sorted(urls):
        try:
            raw, sha = reader.document(url, _allow_sgml_recovery=False)
        except (RuntimeError, ValueError, OSError):
            continue
        try:
            filed = sgml_filed_date(raw, accession)
        except ValueError as exc:
            return {"source": "sec_submission_header", "status": "ambiguous",
                    "records": [{"source_url": url, "source_sha256": sha, "error": str(exc)}]}
        if filed:
            records.append({"source_url": url, "source_sha256": sha, "filed": filed})
    dates = {record["filed"] for record in records}
    if len(dates) > 1:
        return {"source": "sec_submission_header", "status": "ambiguous", "records": records}
    if dates:
        return {"source": "sec_submission_header", "filed": next(iter(dates)), "records": records}
    return None


def publication_floor(client: loader.SecClient, documents: list[dict], filed: str, *, download: bool) -> tuple[str | None, dict]:
    reported = sorted({date.fromisoformat(document["query_accepted_on"][:10]).isoformat() for document in documents})
    floor = max(reported)
    proof = {"source": "discovery_reported_publication_floor", "reported_dates": reported,
             "publication_floor_on": floor}
    accession = documents[0]["adsh"]
    year_number = int(accession[11:13])
    accession_year = 1900 + year_number if year_number >= 93 else 2000 + year_number
    if int(filed[:4]) >= accession_year or int(floor[:4]) >= accession_year:
        return floor, proof
    # An accession created in a later year cannot be made visible in an earlier
    # year using a backdated full-text-search date. Require its actual SEC header.
    reader = client if download or client.offline else loader.SecClient(client.cache, offline=True)
    records = []
    for document in documents:
        locator = loader.sgml_recovery_locator(document["source_url"])
        if not locator:
            continue
        try:
            raw, sha = reader.document(locator[0], _allow_sgml_recovery=False)
        except (RuntimeError, ValueError, OSError):
            continue
        header = raw[:65536]
        identity = re.search(rb"(?im)^(?:ACCESSION NUMBER:|<ACCESSION-NUMBER>)[ \t]*([0-9-]+)", header)
        accepted = re.search(rb"(?im)^<ACCEPTANCE-DATETIME>[ \t]*(\d{8})\d{6}\b", header)
        if identity is None or identity.group(1).decode("ascii") != accession or accepted is None:
            continue
        stamp = accepted.group(1).decode("ascii")
        value = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8])).isoformat()
        if int(value[:4]) >= accession_year:
            records.append({"source_url": locator[0], "source_sha256": sha, "accepted_on": value})
    accepted_dates = {record["accepted_on"] for record in records}
    if len(accepted_dates) == 1:
        floor = max(floor, next(iter(accepted_dates)))
        return floor, {"source": "sec_acceptance_header", "reported_dates": reported,
                       "publication_floor_on": floor, "records": records}
    return None, {"source": "sec_acceptance_header", "status": "ambiguous" if len(accepted_dates) > 1 else "none", "records": records}


def enrich_manifest(client: loader.SecClient, manifest: dict, *, observations: list[dict] | None = None,
                    download_indexes: bool = False, index_root: Path | None = None) -> tuple[dict, dict]:
    enriched = json.loads(loader.canonical_json(manifest))
    documents = enriched["documents"]
    groups: dict[str, list[dict]] = {}
    quarters = set()
    for document in documents:
        accepted = document.get("query_accepted_on") or document["filed"]
        document["query_accepted_on"] = accepted
        groups.setdefault(document["adsh"], []).append(document)
        quarters.update(adjacent_quarters(accepted))
    # An index for a future quarter cannot exist yet. Earlier/current adjacent
    # quarters are still queried, including negative API-vs-filed discrepancies.
    today = date.today()
    quarters = {quarter for quarter in quarters if quarter <= (today.year, (today.month - 1) // 3 + 1)}
    root = index_root or (client.cache / "documents").resolve().parent
    indexes: dict[str, list[dict]] = {}
    index_errors = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(load_index, client, root, year, quarter, download=download_indexes): (year, quarter)
                   for year, quarter in sorted(quarters)}
        for index, future in enumerate(as_completed(futures), 1):
            year, quarter = futures.pop(future)
            try:
                raw, proof = future.result()
                for accession, records in index_dates(raw, proof, set(groups)).items():
                    indexes.setdefault(accession, []).extend(records)
            except (RuntimeError, ValueError, OSError) as exc:
                index_errors.append({"year": year, "quarter": quarter, "error_type": type(exc).__name__, "error": str(exc)[:160]})
            if index % 10 == 0 or index == len(quarters):
                print(loader.canonical_json({"event": "filing_date_indexes", "completed": index, "total": len(quarters),
                                             "matched_accessions": len(indexes), "index_errors": len(index_errors)}), flush=True)
    observation_hash = loader.digest(loader.canonical_json(observations).encode()) if observations is not None else None
    w1: dict[str, list[dict]] = {}
    for observation in observations or []:
        if observation.get("filed"):
            w1.setdefault(observation["adsh"], []).append({key: observation[key] for key in ("cik", "adsh", "filed")})
    statuses = {"resolved": 0, "ambiguous": 0, "none": 0}
    sources: dict[str, int] = {}
    changed = 0
    availability_postponed = 0
    unresolved = []
    for accession, group in sorted(groups.items()):
        records = sorted(indexes.get(accession, []), key=lambda record: (record["source_url"], record["line"]))
        dates = {record["filed"] for record in records}
        proof = None
        if records:
            proof = {"source": "sec_quarter_master_index", "records": records}
            if len(dates) == 1:
                proof["filed"] = next(iter(dates))
            else:
                proof["status"] = "ambiguous"
        else:
            proof = daily_index_proof(client, root, group, download=False)
            if proof is None:
                proof = _submission_proof(client, group, download=False)
            if proof is None and accession in w1:
                w1_dates = {date.fromisoformat(str(record["filed"])[:10]).isoformat() for record in w1[accession]}
                proof = {"source": "w1_same_accession", "records": w1[accession], "observations_sha256": observation_hash}
                if len(w1_dates) == 1:
                    proof["filed"] = next(iter(w1_dates))
                else:
                    proof["status"] = "ambiguous"
            if proof is None and download_indexes:
                proof = daily_index_proof(client, root, group, download=True)
                if proof is None:
                    proof = _submission_proof(client, group, download=True)
        status = "resolved" if proof and proof.get("filed") else "ambiguous" if proof and proof.get("status") == "ambiguous" else "none"
        floor, floor_proof = publication_floor(client, group, proof["filed"], download=download_indexes) if status == "resolved" else (None, None)
        if status == "resolved" and floor is None:
            status = "ambiguous" if floor_proof.get("status") == "ambiguous" else "none"
        statuses[status] += 1
        if proof:
            sources[proof["source"]] = sources.get(proof["source"], 0) + 1
        if status != "resolved":
            unresolved.append({"adsh": accession, "status": status})
        for document in group:
            document["filed"] = proof["filed"] if status == "resolved" else None
            document["filing_date_status"] = status
            document["filing_date_proof"] = proof
            document["publication_floor_on"] = floor
            document["publication_floor_proof"] = floor_proof
            changed += status == "resolved" and document["filed"] != document["query_accepted_on"]
            availability_postponed += (status == "resolved" and date.fromisoformat(floor)
                                       > date.fromisoformat(document["filed"]) + timedelta(days=1))
            for key in ("status", "error", "evidence_count", "parser_version", "issuer_binding_proof"):
                document.pop(key, None)
    discovery_complete = enriched.get("discovery_complete", enriched.get("complete", False))
    summary = {"version": DATE_PROOF_VERSION, "documents": len(documents), "accessions": len(groups),
               "resolved_accessions": statuses["resolved"], "ambiguous_accessions": statuses["ambiguous"],
               "unresolved_accessions": statuses["none"], "changed_documents": changed,
               "publication_floor_postponed_documents": availability_postponed,
               "sources": sources, "index_errors": index_errors, "unresolved": unresolved,
               "complete": not unresolved}
    enriched["discovery_complete"] = discovery_complete
    enriched["complete"] = bool(discovery_complete and not unresolved)
    enriched["parse_complete"] = False
    enriched["filing_date_enrichment"] = summary
    for key in ("evidence_sha256", "evidence_count", "shard_provenance"):
        enriched.pop(key, None)
    return enriched, summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-manifest", required=True, type=Path)
    parser.add_argument("--output-manifest", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--observations", type=Path)
    parser.add_argument("--download-indexes", action="store_true")
    parser.add_argument("--requests-per-second", type=float, default=2)
    parser.add_argument("--dotenv", type=Path, default=Path("E:/investintell-light/backend/.env"))
    args = parser.parse_args(argv)
    if args.input_manifest.resolve() == args.output_manifest.resolve():
        parser.error("Use a new output manifest; preserve the original manifest and shard plan")
    key = loader.load_key(args.dotenv) if args.download_indexes else ""
    client = loader.SecClient(args.cache_dir, key, offline=not args.download_indexes,
                              requests_per_second=args.requests_per_second)
    manifest = json.loads(args.input_manifest.read_text(encoding="utf-8"))
    observations = json.loads(args.observations.read_text(encoding="utf-8-sig")) if args.observations else None
    enriched, summary = enrich_manifest(client, manifest, observations=observations, download_indexes=args.download_indexes)
    loader.write_json(args.output_manifest, enriched)
    loader.write_json(args.output_manifest.with_suffix(".dates-summary.json"), summary)
    print(loader.canonical_json(summary), flush=True)
    return 0 if enriched["complete"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(loader.canonical_json({"status": "failed", "error_type": type(error).__name__, "error": str(error)[:200]}), flush=True)
        raise SystemExit(1) from None
