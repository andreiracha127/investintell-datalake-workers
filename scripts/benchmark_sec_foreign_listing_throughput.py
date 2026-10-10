"""Measure an isolated, network-disabled raw-cache foreign-listing replay.

Requires psutil. Outputs must be new directories on C:. No original parsed
spools or evidence are used as inputs; canonical evidence is only hashed.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

MANIFEST_SHA = "793cc435a00d41309a5b1b72724eb940ae43b8c86b97e2d7412d5fe0439a5646"
EVIDENCE_SHA = "9ca17573dd649db4075a7eb40065274e7e0b1d536649b553c4b790c3dfc1accc"


def sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def no_network(*args, **kwargs):
    raise RuntimeError("Offline benchmark forbids every network request")


def pdf_executable() -> str | None:
    """Match the parser's PATH lookup and bundled Windows Git fallback."""
    executable = shutil.which("pdftotext")
    if not executable and os.name == "nt":
        bundled = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/mingw64/bin/pdftotext.exe"
        if bundled.is_file():
            executable = str(bundled)
    return executable


if os.environ.get("SEC_BENCHMARK_NO_NETWORK") == "1":
    # This module is imported as __mp_main__ by Windows spawned workers too.
    socket.socket.connect = no_network
    socket.socket.connect_ex = no_network
    socket.create_connection = no_network


def select_pilot(loader, documents: list[dict], raw: Path, count: int) -> list[dict]:
    """Select large sources, large PDFs and balanced parser/proof strata."""
    unique = {row["source_url"]: row for row in documents}
    sizes = {}
    for url in unique:
        key = loader.digest(loader.canonical_sec_url(url).encode())
        meta = json.loads((raw / "documents" / (key + ".json")).read_text(encoding="utf-8"))
        sizes[url] = int(meta.get("bytes", 0))
    ranked = sorted(unique, key=lambda url: (-sizes[url], url))
    selected = set(ranked[:min(20, count // 4)])
    pdfs = [url for url in ranked if unique[url].get("content_format") == "pdf"]
    selected.update(pdfs[:min(20, count // 4)])
    groups = {}
    for url, row in unique.items():
        key = (row["form"], row["binding"], row.get("document_role", "primary"),
               row.get("content_format", "text"), row.get("status"), row["filed"][:3])
        groups.setdefault(key, []).append(url)
    queues = [sorted(values, key=lambda url: loader.digest(url.encode())) for _, values in sorted(groups.items())]
    for index in range(max(map(len, queues), default=0)):
        for queue in queues:
            if index < len(queue):
                selected.add(queue[index])
            if len(selected) >= count:
                return [row for row in documents if row["source_url"] in selected]
    return [row for row in documents if row["source_url"] in selected]


def child(args: argparse.Namespace) -> int:
    import psutil

    process = psutil.Process()
    allowed = process.cpu_affinity()
    chosen = allowed[args.cpu_offset:args.cpu_offset + args.cpu_cores]
    if len(chosen) != args.cpu_cores:
        raise ValueError("Requested CPU affinity exceeds the available affinity")
    process.cpu_affinity(chosen)
    socket.socket.connect = no_network
    socket.socket.connect_ex = no_network
    socket.create_connection = no_network
    import urllib.request
    urllib.request.urlopen = no_network
    urllib.request.OpenerDirector.open = no_network
    sys.path.insert(0, str(args.source / "scripts"))
    loader = importlib.import_module("load_sec_foreign_listing_evidence")
    loader.urlopen = no_network
    loader.SecClient.request = no_network
    manifest_path = args.raw / "manifest.json"
    if sha256(manifest_path) != MANIFEST_SHA or sha256(args.raw / "evidence.jsonl") != EVIDENCE_SHA:
        raise ValueError("Original manifest/evidence identity mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parent_sources = list(manifest["documents"])
    observations = json.loads(args.observations.read_text(encoding="utf-8-sig"))
    observations_sha = loader.digest(loader.canonical_json(observations).encode())
    if manifest.get("observations_sha256") != observations_sha:
        raise ValueError("Observations do not match the immutable parent manifest")
    expected_sha = EVIDENCE_SHA
    if args.pilot_count:
        manifest["documents"] = select_pilot(loader, parent_sources, args.raw, args.pilot_count)
        packages = {row["source_package"] for row in manifest["documents"]}
        expected = hashlib.sha256()
        expected_rows = 0
        with (args.raw / "evidence.jsonl").open("rb") as original:
            for line in original:
                if json.loads(line)["source_package"] in packages:
                    expected.update(line)
                    expected_rows += 1
        expected_sha = expected.hexdigest()
        write_json(args.output / "selection.json", {
            "source_packages": sorted(packages), "expected_evidence_sha256": expected_sha,
            "expected_rows": expected_rows, "requested_unique_urls": args.pilot_count,
            "selection": "20 largest raw sources, 20 largest PDFs, balanced form/binding/role/format/status/decade strata",
            "parent_binding_sources": len(parent_sources),
        })
    # Only raw compressed documents and metadata are read from the original.
    # The delegate cache never supplies the parsed spool or PDF extraction.
    reader = loader.SecClient(args.raw, offline=True)

    class ReadOnlyRawClient(loader.SecClient):
        def document(self, url, **kwargs):
            canonical = loader.canonical_sec_url(url)
            key = loader.digest(canonical.encode())
            directory = args.raw / "documents"
            if not (directory / (key + ".json")).is_file() or not any(
                (directory / (key + suffix)).is_file()
                for suffix in (".bin.xz", ".bin.gz", ".bin")
            ):
                raise FileNotFoundError("Missing offline raw document: " + key)
            return reader.document(url, **kwargs)

    if "raw_cache_dir" in inspect.signature(loader.SecClient).parameters:
        client = loader.SecClient(args.output, offline=True, raw_cache_dir=args.raw)
    else:
        client = ReadOnlyRawClient(args.output, offline=True)
        client.recovery_proofs = reader.recovery_proofs
    # Spawned optimized workers may need a raw-directory link. Their cache
    # includes only originals, while all staged parsing remains in output.
    if args.link_raw:
        subprocess.run(["cmd", "/c", "mklink", "/J", str(args.output / "documents"),
                        str(args.raw / "documents")], check=True, capture_output=True)
    started = time.perf_counter()
    summary = loader.parse_manifest(client, manifest, args.output / "evidence.jsonl",
                                    workers=args.workers, observations=observations,
                                    binding_sources=parent_sources)
    summary.update({"parse_wall_seconds": time.perf_counter() - started,
                    "output_sha256": sha256(args.output / "evidence.jsonl"),
                    "input_manifest_sha256": MANIFEST_SHA,
                    "input_evidence_sha256": EVIDENCE_SHA,
                    "observations_file_sha256": sha256(args.observations),
                    "cpu_affinity": process.cpu_affinity(),
                    "source_loader_sha256": sha256(args.source / "scripts/load_sec_foreign_listing_evidence.py"),
                    "source_parser_sha256": sha256(args.source / "scripts/sec_foreign_listing_parser.py")})
    summary["expected_output_sha256"] = expected_sha
    summary["identical_evidence"] = summary["output_sha256"] == expected_sha
    write_json(args.output / "result.json", summary)
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if summary["identical_evidence"] and summary["failed_documents"] == 0 else 2


def supervise(args: argparse.Namespace) -> int:
    import psutil

    if args.output.exists():
        raise ValueError("Benchmark output must be a new directory")
    if args.output.resolve().drive.lower() != "c:":
        raise ValueError("Benchmark artifacts must stay on C:")
    args.output.mkdir(parents=True)
    temp = args.output / "temp"
    temp.mkdir()
    env = {**os.environ, "TMP": str(temp), "TEMP": str(temp), "SEC_BENCHMARK_NO_NETWORK": "1"}
    executable = pdf_executable()
    version = subprocess.run([executable, "-v"], capture_output=True, text=True) if executable else None
    command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--child"]
    metadata = {"command": command, "python": sys.version, "platform": sys.platform,
                "requested_workers": args.workers, "requested_cpu_cores": args.cpu_cores,
                "requested_cpu_offset": args.cpu_offset,
                "resource_policy_env": {
                    name: os.environ.get(name)
                    for name in ("SEC_PARSE_WORKER_MEMORY_MB", "SEC_PARSE_MEMORY_RESERVE_MB")
                    if os.environ.get(name, "").isdigit()
                },
                "memory_before": dict(psutil.virtual_memory()._asdict()),
                "pdftotext_path": executable, "pdftotext_sha256": sha256(Path(executable)) if executable else None,
                "pdftotext_version": (version.stdout + version.stderr).strip() if version else None,
                "pypdf_version": importlib.metadata.version("pypdf"),
                "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    write_json(args.output / "metadata.json", metadata)
    started = time.perf_counter()
    peak_rss = peak_count = 0
    cpu_seen: dict[tuple[int, float], float] = {}
    process_peaks = {}
    process_os_peaks = {}
    with (args.output / "stdout.log").open("w", encoding="utf-8") as stdout, (args.output / "stderr.log").open("w", encoding="utf-8") as stderr:
        running = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env)
        write_json(args.output / "pid.json", {"supervisor_pid": os.getpid(), "child_pid": running.pid})
        root = psutil.Process(running.pid)
        with (args.output / "resources.jsonl").open("w", encoding="utf-8") as samples:
            while running.poll() is None:
                rss = count = 0
                try:
                    tree = [root, *root.children(recursive=True)]
                except psutil.NoSuchProcess:
                    tree = []
                for item in tree:
                    try:
                        key = (item.pid, item.create_time())
                        cpu = item.cpu_times()
                        cpu_seen[key] = max(cpu_seen.get(key, 0), cpu.user + cpu.system)
                        memory = item.memory_info()
                        item_rss = memory.rss
                        rss += item_rss
                        process_peaks[str(key)] = max(process_peaks.get(str(key), 0), item_rss)
                        os_peak = getattr(memory, "peak_wset", item_rss)
                        process_os_peaks[str(key)] = max(process_os_peaks.get(str(key), 0), os_peak)
                        count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                peak_rss = max(peak_rss, rss)
                peak_count = max(peak_count, count)
                elapsed = time.perf_counter() - started
                sample = {"elapsed_seconds": elapsed, "process_tree_rss_bytes": rss,
                          "process_count": count, "observed_cpu_seconds": sum(cpu_seen.values()),
                          "available_ram_bytes": psutil.virtual_memory().available}
                samples.write(json.dumps(sample, sort_keys=True) + "\n")
                samples.flush()
                write_json(args.output / "progress.json", sample)
                time.sleep(1)
        code = running.wait()
    wall = time.perf_counter() - started
    measurement = {"returncode": code, "whole_wall_seconds": wall,
                   "peak_process_tree_rss_bytes": peak_rss, "peak_tree_rss_metric": "simultaneous RSS sampled every 1 second",
                   "peak_process_count": peak_count,
                   "observed_cpu_seconds": sum(cpu_seen.values()),
                   "cpu_utilization_percent_one_core": 100 * sum(cpu_seen.values()) / wall,
                   "cpu_utilization_percent_budget": 100 * sum(cpu_seen.values()) / wall / args.cpu_cores,
                   "distinct_processes_observed": len(cpu_seen),
                   "process_peak_rss_bytes": process_peaks,
                   "process_os_peak_rss_bytes": process_os_peaks,
                   "process_os_peak_rss_metric": "Windows peak_wset where available, sampled RSS elsewhere",
                   "process_observed_cpu_seconds": {str(key): value for key, value in cpu_seen.items()},
                   "sampling_interval_seconds": 1,
                   "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    write_json(args.output / "measurement.json", measurement)
    print(json.dumps(measurement, sort_keys=True), flush=True)
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--cpu-cores", type=int, default=2)
    parser.add_argument("--cpu-offset", type=int, default=0)
    parser.add_argument("--link-raw", action="store_true")
    parser.add_argument("--pilot-count", type=int, default=0)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.workers < 1 or args.cpu_cores < 1:
        parser.error("workers and CPU cores must be positive")
    if args.cpu_offset < 0 or args.pilot_count < 0:
        parser.error("CPU offset and pilot count must be nonnegative")
    return child(args) if args.child else supervise(args)


if __name__ == "__main__":
    raise SystemExit(main())
