"""Run two local foreign-evidence collectors, then verify and combine their output.

prepare snapshots a complete discovery manifest and partitions canonical source
URLs. collect handles one partition at four requests/second. combine publishes
the complete local evidence artifact only after both partitions pass integrity
and coverage checks. This wrapper has no database operation or apply option.
"""
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

if __package__:
    from . import load_sec_foreign_listing_evidence as loader
else:
    import load_sec_foreign_listing_evidence as loader

PART_COUNT = 2
PLAN_VERSION = 1
SUCCESS_STATUSES = {"parsed", "issuer_binding_unverified", "not_securities_description"}
# Only these fields are produced or refreshed while parsing original bytes.
# All other discovery metadata remains pinned to the immutable parent snapshot,
# including binding, symbols, issuer identity, attachment and date provenance.
PARSE_OUTPUT_FIELDS = {"status", "error", "evidence_count", "parser_version", "source_sha256",
                       "content_format", "pdf_text_extractor", "pdf_pages",
                       "issuer_binding_proof", "document_recovery_proof"}


def file_sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def partition(document: dict, count: int = PART_COUNT) -> int:
    url = loader.canonical_sec_url(document["source_url"])
    return int(loader.digest(url.encode("utf-8")), 16) % count


def _parent_documents(parent: dict) -> dict[str, dict]:
    documents = {}
    for source in parent["documents"]:
        normalized = loader.canonical_document(source)
        if normalized["source_package"] != source["source_package"] or normalized["source_url"] != source["source_url"]:
            raise ValueError("Parent discovery must contain canonical SEC source identities")
        if source["source_package"] in documents:
            raise ValueError("Parent discovery contains a duplicate source package")
        documents[source["source_package"]] = source
    return documents


def _matches_parent_metadata(document: dict, parent: dict) -> bool:
    child_metadata = {key: value for key, value in document.items() if key not in PARSE_OUTPUT_FIELDS}
    parent_metadata = {key: value for key, value in parent.items() if key not in PARSE_OUTPUT_FIELDS}
    return (loader.canonical_json(child_metadata) == loader.canonical_json(parent_metadata)
            and (not parent.get("source_sha256") or document.get("source_sha256") == parent["source_sha256"]))


def _load_plan(cache: Path) -> tuple[dict, dict]:
    plan = json.loads((cache / "shards" / "plan.json").read_text(encoding="utf-8"))
    if plan["version"] != PLAN_VERSION or plan["parts"] != PART_COUNT:
        raise ValueError("Unsupported shard plan")
    parent_file = cache / "shards" / ("parent-" + plan["parent_manifest_sha256"] + ".json")
    if file_sha256(parent_file) != plan["parent_manifest_sha256"]:
        raise ValueError("Immutable parent manifest hash mismatch")
    parent = json.loads(parent_file.read_text(encoding="utf-8"))
    if not parent.get("complete") or parent.get("universe_sha256") != plan["universe_sha256"]:
        raise ValueError("Parent discovery is incomplete or its universe hash changed")
    loader.require_authoritative_filing_dates(parent)
    return plan, parent


def _shard_identity(plan: dict, part: int) -> dict:
    return {"parent_manifest_sha256": plan["parent_manifest_sha256"], "index": part, "count": PART_COUNT}


def prepare(cache: Path) -> dict:
    raw = (cache / "manifest.json").read_bytes()
    parent = json.loads(raw)
    if not parent.get("complete"):
        raise ValueError("Shard preparation requires complete foreign-universe discovery")
    loader.require_authoritative_filing_dates(parent)
    documents = _parent_documents(parent)
    parent_hash = loader.digest(raw)
    plan = {"version": PLAN_VERSION, "parts": PART_COUNT, "parent_manifest_sha256": parent_hash,
            "universe_sha256": parent["universe_sha256"], "documents": len(documents)}
    plan_path = cache / "shards" / "plan.json"
    if plan_path.exists() and json.loads(plan_path.read_text(encoding="utf-8")) != plan:
        raise ValueError("Existing shard plan belongs to a different parent snapshot")
    snapshot = cache / "shards" / ("parent-" + parent_hash + ".json")
    if snapshot.exists() and file_sha256(snapshot) != parent_hash:
        raise ValueError("Immutable parent manifest hash mismatch")
    if not snapshot.exists():
        loader.write_bytes(snapshot, raw)
    loader.write_json(plan_path, plan)
    shared_documents = cache / "documents"
    shared_documents.mkdir(parents=True, exist_ok=True)
    links_required = []
    sizes = []
    for part in range(PART_COUNT):
        directory = cache / "parts" / str(part)
        directory.mkdir(parents=True, exist_ok=True)
        selected = [documents[key] for key in sorted(documents) if partition(documents[key]) == part]
        child = {**parent, "documents": selected, "complete": False, "parse_complete": False,
                 "shard": _shard_identity(plan, part)}
        for key in ("evidence_sha256", "evidence_count", "shard_provenance"):
            child.pop(key, None)
        loader.write_json(directory / "input-manifest.json", child)
        if not (directory / "manifest.json").exists():
            loader.write_json(directory / "manifest.json", child)
        link = directory / "documents"
        if link.exists():
            if link.resolve() != shared_documents.resolve():
                raise ValueError("Part document cache does not resolve to the shared original cache")
        else:
            try:
                os.symlink(shared_documents.resolve(), link, target_is_directory=True)
            except OSError:
                # Windows may require a junction when symbolic-link creation is
                # unavailable. The owner can create it without copying originals.
                links_required.append({"link": str(link), "target": str(shared_documents.resolve())})
        sizes.append({"part": part, "documents": len(selected), "urls": len({row["source_url"] for row in selected})})
    return {**plan, "partitions": sizes, "links_required": links_required}


def collect(cache: Path, part: int, observations: Path | None = None, *, offline: bool = False,
            dotenv: Path | None = None, workers: int = 8) -> dict:
    if part not in range(PART_COUNT):
        raise ValueError("Invalid shard index")
    plan, parent = _load_plan(cache)
    directory = cache / "parts" / str(part)
    manifest = json.loads((directory / "input-manifest.json").read_text(encoding="utf-8"))
    parent_documents = _parent_documents(parent)
    expected = {key for key, source in parent_documents.items() if partition(source) == part}
    if (manifest.get("shard") != _shard_identity(plan, part) or manifest.get("complete") is not False
            or {row["source_package"] for row in manifest["documents"]} != expected
            or len(manifest["documents"]) != len(expected)):
        raise ValueError("Child input does not match its immutable parent partition")
    if any(loader.canonical_json(document) != loader.canonical_json(parent_documents[document["source_package"]])
           for document in manifest["documents"]):
        raise ValueError("Child input parsing or binding metadata differs from its immutable parent")
    if (directory / "documents").resolve() != (cache / "documents").resolve():
        raise ValueError("Create the child's documents link to the shared original cache before collection")
    observation_rows = json.loads(observations.read_text(encoding="utf-8-sig")) if observations else None
    key = "" if offline else loader.load_key(dotenv)
    client = loader.SecClient(directory, key, offline=offline, requests_per_second=8 / PART_COUNT)
    result = loader.parse_manifest(client, manifest, directory / "evidence.jsonl", workers, observation_rows,
                                   binding_sources=parent["documents"])
    # parse_manifest preserves complete=False. No partial partition can pass the
    # ordinary loader's apply_evidence complete-manifest requirement.
    return {"part": part, "parent_manifest_sha256": plan["parent_manifest_sha256"], **result}


def _validate_child(cache: Path, plan: dict, parent_documents: dict, part: int) -> tuple[dict, Path]:
    directory = cache / "parts" / str(part)
    manifest_file = directory / "manifest.json"
    child = json.loads(manifest_file.read_text(encoding="utf-8"))
    if child.get("shard") != _shard_identity(plan, part) or child.get("complete") is not False:
        raise ValueError("Child manifest parent identity or partial status is invalid")
    if not child.get("parse_complete") or child.get("universe_sha256") != plan["universe_sha256"]:
        raise ValueError("Child parsing is incomplete or universe hash mismatches")
    loader.require_authoritative_filing_dates(child)
    expected = {key for key, source in parent_documents.items() if partition(source) == part}
    documents = {row["source_package"]: row for row in child["documents"]}
    if len(documents) != len(child["documents"]) or set(documents) != expected:
        raise ValueError("Child source package coverage is missing, extra, or duplicated")
    immutable = ("cik", "adsh", "form", "filed", "source_url")
    for package, document in documents.items():
        if (document.get("status") not in SUCCESS_STATUSES
                or not _matches_parent_metadata(document, parent_documents[package])):
            raise ValueError("Child source status or immutable parsing and binding metadata mismatches")
    evidence = directory / "evidence.jsonl"
    if file_sha256(evidence) != child.get("evidence_sha256"):
        raise ValueError("Child evidence artifact hash mismatch")
    counts = dict.fromkeys(documents, 0)
    hashes = set()
    previous = ""
    with evidence.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            package = row["source_package"]
            if package not in documents or package < previous:
                raise ValueError("Child evidence has an extra or unordered source package")
            previous = package
            document = documents[package]
            if any(row[key] != document[key] for key in immutable + ("source_sha256",)):
                raise ValueError("Child evidence source provenance does not match its manifest")
            claimed = row["fact_hash"]
            actual = hashlib.md5(loader.canonical_json({key: value for key, value in row.items() if key != "fact_hash"}).encode(), usedforsecurity=False).hexdigest()
            if claimed != actual or claimed in hashes:
                raise ValueError("Child evidence fact hash mismatches or is duplicated")
            hashes.add(claimed)
            counts[package] += 1
    if any(counts[key] != document["evidence_count"] for key, document in documents.items()):
        raise ValueError("Child evidence per-document counts mismatch")
    if sum(counts.values()) != child.get("evidence_count"):
        raise ValueError("Child evidence total count mismatch")
    return child, evidence


def combine(cache: Path, output: Path) -> dict:
    plan, parent = _load_plan(cache)
    parent_documents = _parent_documents(parent)
    children = [_validate_child(cache, plan, parent_documents, part) for part in range(PART_COUNT)]
    observation_hashes = {child.get("observations_sha256") for child, _ in children}
    if len(observation_hashes) != 1 or (parent.get("observations_sha256") is not None
                                      and parent["observations_sha256"] not in observation_hashes):
        raise ValueError("Child W1 observation hashes differ from one another or the parent")
    all_documents = [document for child, _ in children for document in child["documents"]]
    packages = [row["source_package"] for row in all_documents]
    if len(packages) != len(set(packages)) or set(packages) != set(parent_documents):
        raise ValueError("Combined evidence does not cover the exact full parent source universe")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + "." + uuid4().hex + ".tmp")
    streams = [path.open("rb") for _, path in children]
    source_hashes = [hashlib.sha256() for _ in children]
    pending = []

    def advance(part: int) -> None:
        for raw in streams[part]:
            source_hashes[part].update(raw)
            if raw.strip():
                row = json.loads(raw)
                heapq.heappush(pending, (row["source_package"], part, row))
                return

    count = 0
    try:
        for part in range(PART_COUNT):
            advance(part)
        with temporary.open("wb") as handle:
            while pending:
                _, part, row = heapq.heappop(pending)
                handle.write((loader.canonical_json(row) + "\n").encode())
                count += 1
                advance(part)
        if any(source_hashes[index].hexdigest() != child["evidence_sha256"] for index, (child, _) in enumerate(children)):
            raise ValueError("Child evidence changed during combination")
        if count != sum(child["evidence_count"] for child, _ in children):
            raise ValueError("Combined evidence count mismatch")
        loader.replace_file(temporary, output)
    finally:
        for stream in streams:
            stream.close()
        temporary.unlink(missing_ok=True)
    provenance = {"parent_manifest_sha256": plan["parent_manifest_sha256"], "parts": [
        {"index": index, "evidence_sha256": child["evidence_sha256"], "evidence_count": child["evidence_count"]}
        for index, (child, _) in enumerate(children)
    ]}
    complete = {**parent, "documents": sorted(all_documents, key=lambda row: row["source_package"]),
                "complete": True, "parse_complete": True, "evidence_count": count,
                "evidence_sha256": file_sha256(output), "observations_sha256": next(iter(observation_hashes)),
                "shard_provenance": provenance}
    loader.write_json(cache / "manifest.json", complete)
    summary = {"documents": len(all_documents), "evidence_rows": count, "failed_documents": 0,
               "unverified_bindings": sum(row.get("status") == "issuer_binding_unverified" for row in all_documents),
               "rejected_description_candidates": sum(row.get("status") == "not_securities_description" for row in all_documents),
               "discovery_complete": True, "parse_complete": True, "shard_provenance": provenance}
    loader.write_json(output.with_suffix(".summary.json"), summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "collect", "combine"))
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--parts", type=int, choices=(PART_COUNT,), default=PART_COUNT)
    parser.add_argument("--part", type=int, choices=range(PART_COUNT))
    parser.add_argument("--observations", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--workers", type=int, choices=range(1, 9), default=8)
    parser.add_argument("--dotenv", type=Path, default=Path("E:/investintell-light/backend/.env"))
    args = parser.parse_args(argv)
    if args.command == "prepare":
        result = prepare(args.cache_dir)
        code = 2 if result["links_required"] else 0
    elif args.command == "collect":
        if args.part is None:
            parser.error("collect requires --part")
        result = collect(args.cache_dir, args.part, args.observations, offline=args.offline,
                         dotenv=args.dotenv, workers=args.workers)
        code = 0 if result["parse_complete"] else 2
    else:
        result = combine(args.cache_dir, args.output or args.cache_dir / "evidence.jsonl")
        code = 0
    print(loader.canonical_json(result), flush=True)
    return code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(loader.canonical_json({"status": "failed", "error_type": type(error).__name__, "error": str(error)[:240]}), flush=True)
        raise SystemExit(1) from None
