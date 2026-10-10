"""Real spawned replay, full-parent binding and immutable offline inputs."""
from __future__ import annotations

from copy import deepcopy
from concurrent.futures import Future
import json
import io
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

from scripts import load_sec_foreign_listing_evidence as loader
from scripts import sec_parse_resources as resources


def _authority(filed):
    return {"filed": filed, "filing_date_status": "resolved", "query_accepted_on": filed,
            "publication_floor_on": filed,
            "publication_floor_proof": {"source": "discovery_reported_publication_floor"},
            "filing_date_proof": {"source": "w1_same_accession", "filed": filed, "records": []}}


def _fixture_sources(raw_cache):
    fixtures = Path(__file__).parent / "fixtures" / "sec_foreign_listing_evidence"
    entries = json.loads((fixtures / "manifest.json").read_text())
    names = {"tsm", "azn_2015_f6_parent_full", "azn_2015_amendment_full"}
    sources = []
    for item in entries:
        if item["name"] not in names:
            continue
        amendment = item["name"] == "azn_2015_amendment_full"
        document = {"cik": item["cik"], "adsh": item["accession_number"],
                    "form": item["form_type"], "source_url": item["source_url"],
                    "symbols": [item.get("symbol", "AZN")],
                    "binding": "issuer_name_in_f6" if amendment else "registrant_cik",
                    "issuer_name": "ASTRAZENECA PLC" if amendment else "",
                    **_authority(item["filing_date"])}
        document = loader.canonical_document(document)
        raw = (fixtures / item["fixture"]).read_bytes()
        key = loader.digest(document["source_url"].encode())
        loader.write_bytes(raw_cache / "documents" / (key + ".bin"), raw)
        loader.write_json(raw_cache / "documents" / (key + ".json"),
                          {"url": document["source_url"], "sha256": loader.digest(raw), "bytes": len(raw)})
        document["source_sha256"] = loader.digest(raw)
        sources.append(document)
    assert len(sources) == 3
    return sources


def test_spawned_replay_matches_serial_with_full_parent_binding(tmp_path, monkeypatch):
    raw_cache = tmp_path / "originals"
    sources = _fixture_sources(raw_cache)
    before = {str(path): path.read_bytes() for path in raw_cache.rglob("*") if path.is_file()}
    # Parent cover is outside this shard. Its binding proof must still survive.
    shard = [source for source in reversed(sources) if source["binding"] == "issuer_name_in_f6" or source["form"] == "20-F"]
    assert len(shard) == 2
    manifest = {"complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
                "documents": shard}
    monkeypatch.setattr(loader, "resolve_parse_workers", lambda requested: requested)
    process_pool = loader.ProcessPoolExecutor
    pool_calls = []
    def record_pool(*args, **kwargs):
        pool_calls.append((kwargs["max_workers"], kwargs["mp_context"].get_start_method()))
        return process_pool(*args, **kwargs)
    monkeypatch.setattr(loader, "ProcessPoolExecutor", record_pool)
    outputs = []
    for workers in (1, 2):
        staging = tmp_path / f"run-{workers}"
        output = staging / "evidence.jsonl"
        summary = loader.parse_manifest(loader.SecClient(staging, offline=True, raw_cache_dir=raw_cache),
                                        deepcopy(manifest), output, workers=workers, binding_sources=sources)
        assert summary["failed_documents"] == 0
        assert summary["evidence_rows"] > 0
        outputs.append(output.read_bytes())
        assert not list((staging / "parse-context").glob("*.json"))
    assert outputs[0] == outputs[1]
    assert pool_calls == [(2, "spawn")]
    rows = [json.loads(line) for line in outputs[1].splitlines()]
    assert any(row.get("issuer_binding_proof") for row in rows)
    assert [row["source_package"] for row in rows] == sorted(row["source_package"] for row in rows)
    assert before == {str(path): path.read_bytes() for path in raw_cache.rglob("*") if path.is_file()}


def test_offline_missing_original_preserves_published_output(tmp_path, monkeypatch):
    raw_cache = tmp_path / "originals"
    sources = _fixture_sources(raw_cache)
    staging = tmp_path / "staging"
    output = staging / "evidence.jsonl"
    loader.write_bytes(output, b"prior complete evidence\n")
    for path in (raw_cache / "documents").glob("*.bin"):
        path.unlink()
    manifest = {"complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
                "documents": sources}
    monkeypatch.setattr(loader, "resolve_parse_workers", lambda requested: requested)
    summary = loader.parse_manifest(loader.SecClient(staging, offline=True, raw_cache_dir=raw_cache),
                                    manifest, output, workers=2)
    assert summary["failed_documents"] == 3
    assert not summary["parse_complete"]
    assert not summary["output_published"]
    assert output.read_bytes() == b"prior complete evidence\n"
    assert all("Offline document cache missing" in row["error"] for row in manifest["documents"])


def test_reversed_completion_is_deterministic_and_queue_is_bounded(tmp_path, monkeypatch):
    raw_cache = tmp_path / "originals"
    source = next(item for item in _fixture_sources(raw_cache) if item["form"] == "20-F")
    raw, sha = loader.SecClient(raw_cache, offline=True).document(source["source_url"])
    documents = []
    for index in range(9):
        url = source["source_url"].rsplit("/", 1)[0] + f"/completion-{index}.htm"
        item = loader.canonical_document({**source, "source_url": url})
        key = loader.digest(url.encode())
        loader.write_bytes(raw_cache / "documents" / (key + ".bin"), raw)
        loader.write_json(raw_cache / "documents" / (key + ".json"), {"url": url, "sha256": sha})
        documents.append(item)
    manifest = {"complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
                "documents": documents}
    monkeypatch.setattr(loader, "resolve_parse_workers", lambda requested: requested)
    serial = tmp_path / "serial" / "evidence.jsonl"
    loader.parse_manifest(loader.SecClient(serial.parent, offline=True, raw_cache_dir=raw_cache),
                          deepcopy(manifest), serial, workers=1)
    state = {"outstanding": 0, "peak": 0, "submitted": 0, "completed": []}
    class CountedFuture(Future):
        def result(self, *args, **kwargs):
            state["outstanding"] -= 1
            return super().result(*args, **kwargs)
    class Pool:
        def __init__(self, *, max_workers, initializer, initargs, **kwargs):
            assert max_workers == 2
            initializer(*initargs)
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
        def submit(self, function, *args):
            future = CountedFuture()
            future.order = state["submitted"]
            state["submitted"] += 1
            state["outstanding"] += 1
            state["peak"] = max(state["peak"], state["outstanding"])
            future.set_result(function(*args))
            return future
    def reversed_wait(pending, **kwargs):
        chosen = max(pending, key=lambda future: future.order)
        state["completed"].append(chosen.order)
        return {chosen}, set(pending) - {chosen}
    monkeypatch.setattr(loader, "ProcessPoolExecutor", Pool)
    monkeypatch.setattr(loader, "wait", reversed_wait)
    reordered = tmp_path / "reordered" / "evidence.jsonl"
    loader.parse_manifest(loader.SecClient(reordered.parent, offline=True, raw_cache_dir=raw_cache),
                          deepcopy(manifest), reordered, workers=2)
    assert reordered.read_bytes() == serial.read_bytes()
    assert state["peak"] == 4
    assert state["outstanding"] == 0
    assert state["completed"] != sorted(state["completed"])
    assert state["completed"][-1] == 0


def test_total_budget_is_distributed_without_multiplication(monkeypatch):
    monkeypatch.setattr(resources, "parse_resource_budget", lambda _mb: {"max_workers": 7})
    allocations = [resources.resolve_parse_workers(20, shard_count=4, shard_index=index) for index in range(4)]
    assert allocations == [2, 2, 2, 1]
    assert sum(allocations) == 7
    assert resources.resolve_parse_workers(2, total_budget=1) == 1
    with pytest.raises(ValueError):
        resources.resolve_parse_workers(0)


def test_separate_raw_cache_cannot_be_used_online(tmp_path):
    with pytest.raises(ValueError, match="offline"):
        loader.SecClient(tmp_path / "output", raw_cache_dir=tmp_path / "raw")


@pytest.mark.parametrize("cpus,expected_workers", [(1, 1), (2, 1), (4, 1), (8, 4)])
def test_cpu_budget_preserves_four_core_reserve_on_small_hosts(monkeypatch, cpus, expected_workers):
    class Process:
        def cpu_affinity(self):
            return list(range(cpus))
    fake_psutil = SimpleNamespace(virtual_memory=lambda: SimpleNamespace(available=16 * 1024**3, total=32 * 1024**3),
                                 Process=Process, Error=OSError)
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil)
    monkeypatch.setattr(resources.os, "cpu_count", lambda: cpus)
    monkeypatch.setattr(resources.os, "sched_getaffinity", lambda _pid: set(range(cpus)), raising=False)
    monkeypatch.setattr(resources.Path, "read_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()))
    monkeypatch.delenv("SEC_PARSE_MEMORY_RESERVE_MB", raising=False)
    monkeypatch.delenv("SEC_PARSE_WORKER_MEMORY_MB", raising=False)
    assert resources.parse_resource_budget()["max_workers"] == expected_workers
    assert resources.resolve_parse_workers(20) == expected_workers


@pytest.mark.parametrize("available_mb,container_mb,expected_workers", [(3700, None, 2), (1024, 1024, 1), (700, 1024, 0), (500, None, 0)])
def test_memory_budget_preserves_reserve_and_scales_small_container(monkeypatch, available_mb, container_mb, expected_workers):
    class Process:
        def cpu_affinity(self):
            return list(range(24))
    fake_psutil = SimpleNamespace(virtual_memory=lambda: SimpleNamespace(available=available_mb * 1024**2, total=32 * 1024**3),
                                 Process=Process, Error=OSError)
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil)
    monkeypatch.setattr(resources.os, "cpu_count", lambda: 24)
    monkeypatch.setattr(resources.os, "sched_getaffinity", lambda _pid: set(range(24)), raising=False)
    monkeypatch.setattr(resources.Path, "read_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()))
    def read_limit(path):
        if container_mb is not None and path == "/sys/fs/cgroup/memory.max":
            return container_mb * 1024**2
        if container_mb is not None and path == "/sys/fs/cgroup/memory.current":
            return 0
        return None
    monkeypatch.setattr(resources, "_read_limit", read_limit)
    monkeypatch.delenv("SEC_PARSE_MEMORY_RESERVE_MB", raising=False)
    monkeypatch.delenv("SEC_PARSE_WORKER_MEMORY_MB", raising=False)
    budget = resources.parse_resource_budget()
    assert budget["max_workers"] == expected_workers
    assert budget["memory_reserve_bytes"] + expected_workers * 600 * 1024**2 <= budget["available_memory_bytes"] or expected_workers == 0
    if expected_workers == 0:
        with pytest.raises(MemoryError, match="memory reserve"):
            resources.resolve_parse_workers(1)


def test_readonly_raw_cache_rejects_nested_staging_and_evidence(tmp_path):
    raw_cache = tmp_path / "raw"
    with pytest.raises(ValueError, match="Staging"):
        loader.SecClient(raw_cache / "staging", offline=True, raw_cache_dir=raw_cache)
    client = loader.SecClient(tmp_path / "staging", offline=True, raw_cache_dir=raw_cache)
    with pytest.raises(ValueError, match="Evidence output"):
        loader.parse_manifest(client, {}, raw_cache / "evidence.jsonl", workers=1)


@pytest.mark.parametrize("recovery_kind", ["submission", "index"])
def test_offline_recovery_uses_verified_readonly_sources_and_stages_proof(tmp_path, monkeypatch, recovery_kind):
    raw_cache, staging = tmp_path / "originals", tmp_path / "staging"
    primary = "https://www.sec.gov/Archives/edgar/data/123/000123456720000001/primary.htm"
    original = (b"<html><body>Depositary registration statement. "
                b"Each American Depositary Share represents five ordinary shares. "
                b"Terms of the deposited securities are described below.</body></html>\n")
    if recovery_kind == "submission":
        cached_url, accession, _ = loader.sgml_recovery_locator(primary)
        cached_bytes = (f"<SEC-DOCUMENT>{accession}.txt\nACCESSION NUMBER:\t{accession}\n".encode()
                        + b"<DOCUMENT>\n<TYPE>F-6\n<SEQUENCE>1\n<FILENAME>primary.htm\n<TEXT>\n"
                        + original + b"</TEXT>\n</DOCUMENT>\n</SEC-DOCUMENT>\n")
    else:
        cached_url = primary.replace("/123/", "/456/")
        cached_bytes = original
        index = b"CIK|Company Name|Form Type|Date Filed|Filename\n456|ACME PLC/ADR|F-6|2020-03-01|edgar/data/456/0001234567-20-000001.txt\n"
        loader.write_bytes(raw_cache / "master-2020-Q1.idx", index)
    key = loader.digest(cached_url.encode())
    loader.write_bytes(raw_cache / "documents" / (key + ".bin"), cached_bytes)
    loader.write_json(raw_cache / "documents" / (key + ".json"),
                      {"url": cached_url, "sha256": loader.digest(cached_bytes)})
    before = {str(path): path.read_bytes() for path in raw_cache.rglob("*") if path.is_file()}
    monkeypatch.setattr(loader.SecClient, "request", lambda *_args, **_kwargs: pytest.fail("Offline recovery reached network"))
    proofs = []
    for _ in range(2):
        client = loader.SecClient(staging, offline=True, raw_cache_dir=raw_cache)
        assert client.document(primary) == (original, loader.digest(original))
        proofs.append(client.recovery_proofs[primary])
    assert proofs[0] == proofs[1]
    assert proofs[0]["recovery_filename"] == "primary.htm"
    assert proofs[0].get("recovery_source_url", proofs[0].get("recovery_retrieval_url")) == cached_url
    primary_key = loader.digest(primary.encode())
    metadata = json.loads((staging / "documents" / (primary_key + ".json")).read_text())
    assert metadata["url"] == primary
    assert metadata["sha256"] == loader.digest(original)
    assert before == {str(path): path.read_bytes() for path in raw_cache.rglob("*") if path.is_file()}
    assert not (raw_cache / "documents" / (primary_key + ".json")).exists()


class _Response(io.BytesIO):
    def __init__(self, raw, declared=None):
        super().__init__(raw)
        self.headers = {"Content-Length": str(len(raw) if declared is None else declared)}


def test_mirror_failure_has_one_government_fallback_with_canonical_identity(tmp_path):
    url = "https://www.sec.gov/Archives/edgar/data/123/report.htm"
    raw = b"Original SEC filing bytes " * 12
    calls = []
    class Transport:
        def open(self, destination, **kwargs):
            calls.append(destination)
            if len(calls) == 1:
                raise RuntimeError("Provider HTTP 403")
            return _Response(raw)
    client = loader.SecClient(tmp_path, "never-persist-this-key")
    client.transport = Transport()
    assert client.document(url, _allow_sgml_recovery=False) == (raw, loader.digest(raw))
    assert calls == ["https://edgar-mirror.sec-api.io/123/report.htm", url]
    metadata = json.loads(next((tmp_path / "documents").glob("*.json")).read_text())
    assert metadata["url"] == url
    assert "never-persist-this-key" not in json.dumps(metadata)
    assert loader.SecClient(tmp_path, offline=True).document(url) == (raw, loader.digest(raw))


@pytest.mark.parametrize("bad_response", ["truncated", "maintenance", "throttled"])
def test_bad_download_response_never_promotes_raw_cache(tmp_path, bad_response):
    url = "https://www.sec.gov/Archives/edgar/data/123/report.htm"
    raw = b"Original SEC filing bytes " * 12
    if bad_response == "maintenance":
        raw = b"<html><head><title>SEC.gov | Website Maintenance</title></head>" + b"x" * 150
    elif bad_response == "throttled":
        raw = b"<html>Request Rate Threshold Exceeded" + b"x" * 150
    calls = []
    class Transport:
        def open(self, destination, **kwargs):
            calls.append(destination)
            return _Response(raw, len(raw) + 10 if bad_response == "truncated" else len(raw))
    client = loader.SecClient(tmp_path, "fake")
    client.transport = Transport()
    with pytest.raises(ValueError):
        client.document(url, _allow_sgml_recovery=False)
    assert calls == ["https://edgar-mirror.sec-api.io/123/report.htm", url]
    assert not list((tmp_path / "documents").glob("*.json"))
    assert not list((tmp_path / "documents").glob("*.bin*"))
