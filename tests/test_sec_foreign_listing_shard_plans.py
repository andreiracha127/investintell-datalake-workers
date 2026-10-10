"""Immutable configurable shard plans, exact coverage and total worker budgets."""
import hashlib
import json
import multiprocessing

import pytest

from scripts import load_sec_foreign_listing_evidence as loader
from scripts import run_sec_foreign_listing_evidence_shards as shards
from scripts import sec_parse_resources as resources


@pytest.fixture(autouse=True)
def fixed_budget(monkeypatch):
    monkeypatch.setattr(resources, "parse_resource_budget", lambda _mb: {"max_workers": 7})


def parent_manifest(cache):
    documents = []
    for number in range(11):
        source = {
            "cik": 123, "adsh": "0001234567-20-000001", "form": "20-F",
            "source_url": f"https://www.sec.gov/Archives/edgar/data/123/report-{number}.htm",
            "source_sha256": "a" * 64, "filed": "2020-03-01", "evidence_count": 1,
            "filing_date_status": "resolved", "query_accepted_on": "2020-03-01",
            "publication_floor_on": "2020-03-01",
            "publication_floor_proof": {"source": "discovery_reported_publication_floor"},
            "filing_date_proof": {"source": "w1_same_accession", "filed": "2020-03-01"},
        }
        documents.append(loader.canonical_document(source))
    # Two issuer bindings for a single URL must remain in the same partition.
    documents.append(loader.canonical_document({**documents[0], "cik": 456}))
    parent = {"complete": True, "documents": documents, "universe_sha256": "b" * 64,
              "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"}}
    loader.write_json(cache / "manifest.json", parent)
    return parent


def complete_children(cache, count):
    for part in range(count):
        directory = cache / "parts" / str(part)
        child = json.loads((directory / "input-manifest.json").read_text())
        rows = []
        for document in child["documents"]:
            document["status"] = "parsed"
            row = {key: document[key] for key in
                   ("source_package", "source_url", "source_sha256", "cik", "adsh", "form", "filed")}
            row["fact_hash"] = hashlib.md5(loader.canonical_json(row).encode(), usedforsecurity=False).hexdigest()
            rows.append(row)
        artifact = directory / "evidence.jsonl"
        loader.write_bytes(artifact, "".join(loader.canonical_json(row) + "\n" for row in rows).encode())
        child.update(parse_complete=True, evidence_count=len(rows),
                     evidence_sha256=shards.file_sha256(artifact), observations_sha256="c" * 64)
        loader.write_json(directory / "manifest.json", child)


def test_three_uneven_shards_cover_canonical_sources_and_combine_deterministically(tmp_path):
    parent = parent_manifest(tmp_path)
    plan = shards.prepare(tmp_path, 3, workers=7)
    assert plan["parts"] == 3
    assert len({part["documents"] for part in plan["partitions"]}) > 1
    inputs = [json.loads((tmp_path / "parts" / str(part) / "input-manifest.json").read_text())
              for part in range(3)]
    packages = [row["source_package"] for child in inputs for row in child["documents"]]
    assert len(set(packages)) == len(packages) == len(parent["documents"])
    assert set(packages) == {row["source_package"] for row in parent["documents"]}
    url = parent["documents"][0]["source_url"]
    assert sum(any(row["source_url"] == url for row in child["documents"]) for child in inputs) == 1
    complete_children(tmp_path, 3)
    output = tmp_path / "combined.jsonl"
    shards.combine(tmp_path, output, parts=3)
    first = output.read_bytes()
    shards.combine(tmp_path, output)
    assert output.read_bytes() == first
    assert [json.loads(line)["source_package"] for line in first.splitlines()] == sorted(packages)


def test_resume_collect_and_combine_reject_different_count(tmp_path):
    parent_manifest(tmp_path)
    first = shards.prepare(tmp_path, 3)
    assert shards.prepare(tmp_path)["plan_sha256"] == first["plan_sha256"]
    with pytest.raises(ValueError, match="different parent snapshot"):
        shards.prepare(tmp_path, 2)
    with pytest.raises(ValueError, match="count differs"):
        shards.collect(tmp_path, 0, parts=2)
    with pytest.raises(ValueError, match="count differs"):
        shards.combine(tmp_path, tmp_path / "combined.jsonl", parts=2)


@pytest.mark.parametrize("mutation", ["plan_count", "child_count", "plan_budget", "child_plan"])
def test_plan_and_child_identity_changes_fail_before_publication(tmp_path, mutation):
    parent_manifest(tmp_path)
    shards.prepare(tmp_path, 3)
    complete_children(tmp_path, 3)
    path = tmp_path / "shards" / "plan.json" if mutation.startswith("plan_") else tmp_path / "parts" / "0" / "manifest.json"
    data = json.loads(path.read_text())
    if mutation == "plan_count":
        data["parts"] = 2
    elif mutation == "plan_budget":
        data["total_workers"] += 1
    elif mutation == "child_count":
        data["shard"]["count"] = 2
    else:
        data["shard"]["plan_sha256"] = "d" * 64
    loader.write_json(path, data)
    output = tmp_path / "combined.jsonl"
    with pytest.raises(ValueError, match="identity"):
        shards.combine(tmp_path, output)
    assert not output.exists()


def test_collect_uses_parent_wide_bindings_and_one_total_worker_budget(tmp_path, monkeypatch):
    parent = parent_manifest(tmp_path)
    shards.prepare(tmp_path, 3, workers=7)
    captures = []
    monkeypatch.setattr(loader, "SecClient", lambda *args, **kwargs: object())
    def parse(client, manifest, output, workers, observation_rows, *, binding_sources):
        captures.append((workers, binding_sources))
        return {"parse_complete": True}
    monkeypatch.setattr(loader, "parse_manifest", parse)
    for part in range(3):
        shards.collect(tmp_path, part, offline=True, raw_cache_dir=tmp_path)
    assert [workers for workers, _ in captures] == [3, 2, 2]
    assert all(bindings == parent["documents"] for _, bindings in captures)
    with pytest.raises(ValueError, match="worker budget differs"):
        shards.collect(tmp_path, 0, workers=6)


@pytest.mark.parametrize("count", [0, -1, True])
def test_invalid_shard_counts_rejected(tmp_path, count):
    with pytest.raises(ValueError, match="positive integer"):
        shards.prepare(tmp_path, count)


def test_low_resource_budget_rejects_plan_and_collect_without_allocation(tmp_path, monkeypatch):
    parent_manifest(tmp_path)
    monkeypatch.setattr(resources, "parse_resource_budget", lambda _mb: {"max_workers": 1})
    with pytest.raises(ValueError, match="exceeds the total parsing worker budget"):
        shards.prepare(tmp_path, 2, workers=7)
    assert not (tmp_path / "shards" / "plan.json").exists()
    assert shards.prepare(tmp_path, 1, workers=7)["total_workers"] == 1

    other = tmp_path / "other"
    parent_manifest(other)
    monkeypatch.setattr(resources, "parse_resource_budget", lambda _mb: {"max_workers": 7})
    shards.prepare(other, 3, workers=7)
    monkeypatch.setattr(resources, "parse_resource_budget", lambda _mb: {"max_workers": 1})
    monkeypatch.setattr(loader, "SecClient", lambda *_args, **_kwargs: pytest.fail("Unallocated shard reached client"))
    with pytest.raises(ValueError, match="cannot allocate a worker"):
        shards.collect(other, 1, offline=True, raw_cache_dir=other)


def _hold_collect_lock(directory, pipe):
    with shards._collect_lock(directory):
        pipe.send("locked")
        pipe.recv()


def test_duplicate_collect_process_rejected_and_exit_releases_lock(tmp_path, monkeypatch):
    parent_manifest(tmp_path)
    shards.prepare(tmp_path, 3)
    monkeypatch.setattr(loader, "SecClient", lambda *args, **kwargs: object())
    monkeypatch.setattr(loader, "parse_manifest", lambda *args, **kwargs: {"parse_complete": True})
    context = multiprocessing.get_context("spawn")
    parent_pipe, child_pipe = context.Pipe()
    process = context.Process(target=_hold_collect_lock, args=(tmp_path / "parts" / "0", child_pipe))
    process.start()
    try:
        assert parent_pipe.poll(15), "Spawned lock holder did not become ready"
        assert parent_pipe.recv() == "locked"
        with pytest.raises(ValueError, match="collection is already active"):
            shards.collect(tmp_path, 0, offline=True, raw_cache_dir=tmp_path)
        # Simulate an abrupt collector crash: the kernel must release its lock.
        process.terminate()
        process.join(10)
        assert not process.is_alive()
        assert shards.collect(tmp_path, 0, offline=True, raw_cache_dir=tmp_path)["parse_complete"]
    finally:
        if process.is_alive():
            process.terminate()
            process.join(10)
        parent_pipe.close()
        child_pipe.close()


def test_collect_parse_failure_releases_exclusive_lock(tmp_path, monkeypatch):
    parent_manifest(tmp_path)
    shards.prepare(tmp_path, 3)
    monkeypatch.setattr(loader, "SecClient", lambda *args, **kwargs: object())
    def fail(*args, **kwargs):
        raise RuntimeError("synthetic parser failure")
    monkeypatch.setattr(loader, "parse_manifest", fail)
    with pytest.raises(RuntimeError, match="synthetic parser failure"):
        shards.collect(tmp_path, 0, offline=True, raw_cache_dir=tmp_path)
    monkeypatch.setattr(loader, "parse_manifest", lambda *args, **kwargs: {"parse_complete": True})
    assert shards.collect(tmp_path, 0, offline=True, raw_cache_dir=tmp_path)["parse_complete"]
