"""Offline receipts exercise real detached worktrees and isolated child processes."""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

from scripts import receipt_chain_coverage as coverage
from scripts import receipt_chain_production as receipt
from src.input_packs.hashing import canonical_json_sha256

ROW_COLUMNS = (
    "as_of", "quadrant", "candidate_quadrant", "status", "candidate_confidence",
    "growth_score", "inflation_score", "coverage_quality", "transition_pending",
    "basis", "pack_sha256", "chain_start", "code_commit", "loaded_at",
)
WORKER_SOURCE = '''\
import datetime as dt
import json
import os
from pathlib import Path
from types import SimpleNamespace

ROW_COLUMNS = {columns!r}
CHAIN_START = dt.date(2014, 3, 31)
FACTOR = {factor!r}

def compute_series(macro_rows, eod_rows, target):
    assert "DATABASE_URL" not in os.environ, "compute child inherited a DSN"
    assert isinstance(target, dt.date)
    assert len(macro_rows) == 1 and len(eod_rows) == 1
    return [SimpleNamespace(as_of=target, growth=macro_rows[0]["value"] * FACTOR)]

def build_row(decision, pack_sha256, commit, now):
    values = (decision.as_of, "Goldilocks", "Goldilocks", "valid", 0.75,
              decision.growth, 0.25, 1.0, False, "fixture", pack_sha256,
              CHAIN_START, commit, now)
    return dict(zip(ROW_COLUMNS, values))

def run(*args, **kwargs):
    raise AssertionError("receipt invoked worker run/write path")

def publish(*args, **kwargs):
    raise AssertionError("receipt invoked worker publication")
'''
HASHING_SOURCE = '''\
import hashlib
import json

def canonical_json_sha256(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
                     ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
'''


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
    ).stdout.strip()


def _worker_source(factor: float) -> str:
    from scripts.chain_production_snapshot import CONTRACT_CONSTANTS, CONTRACT_FUNCTIONS

    source = WORKER_SOURCE.format(columns=ROW_COLUMNS, factor=factor)
    for name in CONTRACT_CONSTANTS:
        if name != "CHAIN_START":
            source += f"\n{name} = {name!r}\n"
    for name in CONTRACT_FUNCTIONS:
        if name == "verify_pack":
            source += (
                "\ndef verify_pack():\n"
                "    return json.loads(Path(__file__).with_name('pack_identity.json').read_text())\n"
            )
            continue
        result = ("GDP", "CPI") if name == "arm_series_ids" else "fixture"
        source += f"\ndef {name}(*args, **kwargs):\n    return {result!r}\n"
    return source


def _load_fixture_worker(path):
    spec = importlib.util.spec_from_file_location("receipt_fixture_worker", path)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    return worker


@pytest.fixture
def revision_repo(tmp_path):
    repo = tmp_path / "revision repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "receipt-test@example.invalid")
    _git(repo, "config", "user.name", "Receipt Test")
    _git(repo, "config", "core.autocrlf", "false")
    for directory in (repo / "src", repo / "src/workers", repo / "src/input_packs"):
        directory.mkdir(exist_ok=True)
        (directory / "__init__.py").write_text("", encoding="utf-8")
    for directory in (repo / "harness", repo / "harness/phase0q"):
        directory.mkdir(exist_ok=True)
        (directory / "__init__.py").write_text("", encoding="utf-8")
    (repo / "harness/phase0q/decision.py").write_text(
        "def month_end_decision_dates(start, target):\n    return [target]\n",
        encoding="utf-8",
    )
    worker = repo / "src/workers/open_macro_v03_chain.py"
    worker.write_text(_worker_source(1.0), encoding="utf-8")
    worker.with_name("pack_identity.json").write_text(
        json.dumps({"input_pack_sha256": "a" * 64}), encoding="utf-8",
    )
    (repo / "src/input_packs/hashing.py").write_text(HASHING_SOURCE, encoding="utf-8")
    _git(repo, "add", "src", "harness")
    _git(repo, "commit", "-m", "before fixture")
    before = _git(repo, "rev-parse", "HEAD")
    worker.write_text(_worker_source(2.0), encoding="utf-8")
    _git(repo, "add", "src/workers/open_macro_v03_chain.py")
    _git(repo, "commit", "-m", "different after fixture")
    after = _git(repo, "rev-parse", "HEAD")
    return repo, before, after


@pytest.fixture
def snapshot(revision_repo):
    from scripts.chain_production_snapshot import input_contract_sha256

    worker = _load_fixture_worker(revision_repo[0] / "src/workers/open_macro_v03_chain.py")
    inputs = {
        "macro_rows": [{"date": "2026-06-30", "value": 0.125}],
        "eod_rows": [{"date": "2026-06-30", "value": 100.0}],
    }
    return {
        "reference_date": "2026-07-02", "target_date": "2026-06-30",
        "pack_sha256": "a" * 64, "inputs": inputs,
        "pointers": {"fixture_publication": "fixture-publication-1"},
        "row_counts": {key: len(value) for key, value in inputs.items()},
        "input_digests": {key: canonical_json_sha256(value) for key, value in inputs.items()},
        "inputs_sha256": canonical_json_sha256(inputs),
        "input_contract_sha256": input_contract_sha256(worker),
    }


def _write_snapshot(path, snapshot):
    path.write_text(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":"), allow_nan=False),
        encoding="utf-8", newline="\n",
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_no_detached_worktrees(repo):
    records = _git(repo, "worktree", "list", "--porcelain")
    assert records.count("worktree ") == 1, records
    assert "detached" not in records


def test_both_revisions_receive_byte_identical_snapshot(revision_repo, snapshot, tmp_path,
                                                       monkeypatch):
    repo, before, _ = revision_repo
    snapshot_path = tmp_path / "snapshot with spaces.json"
    sha = _write_snapshot(snapshot_path, snapshot)
    monkeypatch.setenv("DATABASE_URL", "postgresql://never-connect.invalid/test")

    result = receipt.compare_revisions(repo, before, before, snapshot_path)

    assert result["identical"] is True
    assert result["before"]["snapshot_sha256"] == sha
    assert result["after"]["snapshot_sha256"] == sha
    assert hashlib.sha256(snapshot_path.read_bytes()).hexdigest() == sha
    assert result["before"]["all_months_sha256"] == result["after"]["all_months_sha256"]
    assert result["before"]["latest_row"]["as_of"] == "2026-06-30"
    assert result["first_differences"] == []
    _assert_no_detached_worktrees(repo)


def test_main_captures_once_and_reports_different_computation(
    revision_repo, snapshot, tmp_path, monkeypatch, capsys,
):
    from scripts import chain_production_snapshot as loader

    repo, before, after = revision_repo
    calls = []

    def capture(reference_date, *, statement_timeout_ms):
        calls.append((reference_date, statement_timeout_ms))
        return snapshot

    monkeypatch.setattr(loader, "capture_snapshot", capture)
    monkeypatch.chdir(repo)
    monkeypatch.setenv("DATABASE_URL", "postgresql://never-connect.invalid/test")
    snapshot_path, output = tmp_path / "snapshot.json", tmp_path / "receipt.json"
    result = receipt.main([
        "--before", before, "--after", after,
        "--snapshot", str(snapshot_path), "--output", str(output),
        "--reference-date", "2026-07-02", "--statement-timeout-ms", "2500",
    ])

    assert result != 0
    assert len(calls) == 1
    assert calls[0] == (dt.date(2026, 7, 2), 2500)
    produced = json.loads(output.read_text(encoding="utf-8"))
    assert produced["identical"] is False
    assert produced["before"]["commit"] == before
    assert produced["after"]["commit"] == after
    assert produced["before"]["all_months_sha256"] != produced["after"]["all_months_sha256"]
    assert produced["first_differences"][0]["fields"]["growth_score"] == {
        "before": 0.125, "after": 0.25,
    }
    sha = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
    assert produced["snapshot"]["file_sha256"] == sha
    assert produced["before"]["snapshot_sha256"] == produced["after"]["snapshot_sha256"] == sha
    assert produced["snapshot"]["pointers"] == snapshot["pointers"]
    printed = capsys.readouterr()
    assert "growth_score" in printed.out
    assert "open-macro-v03-chain" in printed.err
    assert "growth_score" in printed.err
    _assert_no_detached_worktrees(repo)


def test_worktrees_removed_when_child_fails(revision_repo, snapshot, tmp_path):
    repo, before, _ = revision_repo
    snapshot["inputs"]["macro_rows"] = []
    snapshot_path = tmp_path / "invalid-input.json"
    _write_snapshot(snapshot_path, snapshot)

    with pytest.raises((RuntimeError, subprocess.CalledProcessError)):
        receipt.compare_revisions(repo, before, before, snapshot_path)

    _assert_no_detached_worktrees(repo)


@pytest.mark.parametrize("change", ["query", "assembly", "pack_identity", "pack_path"])
def test_changed_input_contract_refuses_comparison(revision_repo, snapshot, tmp_path, change):
    repo, before, _ = revision_repo
    worker_path = repo / "src/workers/open_macro_v03_chain.py"
    if change == "pack_identity":
        # The reader implementation remains byte-identical; only the resolved
        # pack identity changes. Function-source hashing alone cannot catch it.
        worker_path.with_name("pack_identity.json").write_text(
            json.dumps({"input_pack_sha256": "c" * 64}), encoding="utf-8",
        )
    elif change == "pack_path":
        # Path rebinding must be checked even when reader source and the pack
        # manifest returned by verify_pack remain byte-identical.
        worker_path.write_text(
            worker_path.read_text(encoding="utf-8") + "\nMACRO_JSON = 'alternate.json'\n",
            encoding="utf-8",
        )
    else:
        old, new = {
            "query": (
                "MACRO_DELTA_SQL = 'MACRO_DELTA_SQL'",
                "MACRO_DELTA_SQL = 'changed input query'",
            ),
            "assembly": (
                "def run(*args, **kwargs):\n",
                "def run(*args, **kwargs):\n    if kwargs:\n        return None\n",
            ),
        }[change]
        source = worker_path.read_text(encoding="utf-8")
        assert old in source
        worker_path.write_text(source.replace(old, new), encoding="utf-8")
    _git(repo, "add", "src/workers")
    _git(repo, "commit", "-m", "change input read contract")
    after = _git(repo, "rev-parse", "HEAD")
    snapshot_path = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_path, snapshot)

    with pytest.raises(subprocess.CalledProcessError) as failure:
        receipt.compare_revisions(repo, before, after, snapshot_path)

    assert "input loader contract changed" in failure.value.stderr
    _assert_no_detached_worktrees(repo)


def test_missing_input_contract_refuses_comparison(revision_repo, snapshot, tmp_path):
    repo, before, _ = revision_repo
    del snapshot["input_contract_sha256"]
    snapshot_path = tmp_path / "missing-contract.json"
    _write_snapshot(snapshot_path, snapshot)

    with pytest.raises(subprocess.CalledProcessError) as failure:
        receipt.compare_revisions(repo, before, before, snapshot_path)

    assert "input_contract_sha256" in failure.value.stderr
    _assert_no_detached_worktrees(repo)


def test_snapshot_tampering_between_children_is_refused(
    revision_repo, snapshot, tmp_path, monkeypatch,
):
    repo, before, _ = revision_repo
    snapshot_path = tmp_path / "snapshot.json"
    original_sha = _write_snapshot(snapshot_path, snapshot)
    subprocess_run = subprocess.run
    children = []

    def run(command, *args, **kwargs):
        is_replay = any(str(item).endswith("receipt_chain_replay.py") for item in command)
        if is_replay:
            children.append(command)
            assert command[command.index("--snapshot-sha256") + 1] == original_sha
        result = subprocess_run(command, *args, **kwargs)
        if is_replay and len(children) == 1:
            snapshot_path.write_bytes(snapshot_path.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(receipt.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError) as failure:
        receipt.compare_revisions(repo, before, before, snapshot_path)

    assert len(children) == 2
    assert "snapshot bytes changed before replay" in failure.value.stderr
    _assert_no_detached_worktrees(repo)


def test_digest_definition_is_shared_with_certified_receipt(tmp_path):
    # Coverage deliberately imports this module by its bare name so its script
    # can run by absolute path against checkouts predating the shared helper.
    from chain_receipt_common import (
        EXCLUDED_COLUMNS,
        STABLE_COLUMNS,
        hash_definition,
        project_rows,
        row_digests,
    )

    assert coverage.project_rows is project_rows
    assert coverage.row_digests is row_digests
    assert coverage.hash_definition is hash_definition
    path = tmp_path / "fixture_worker.py"
    path.write_text(_worker_source(1.0), encoding="utf-8")
    worker = _load_fixture_worker(path)
    from types import SimpleNamespace

    decision = SimpleNamespace(as_of=dt.date(2026, 6, 30), growth=0.125)
    rows = project_rows(worker, [decision], "a" * 64, "b" * 40)
    reference_row = worker.build_row(decision, "a" * 64, "b" * 40, dt.datetime.now(dt.timezone.utc))
    expected = [{
        key: value.isoformat() if isinstance(value, dt.date) else value
        for key, value in reference_row.items() if key not in EXCLUDED_COLUMNS
    }]
    assert len(STABLE_COLUMNS) == 12
    assert rows == expected
    assert row_digests(rows, canonical_json_sha256)["all_months_sha256"] == (
        canonical_json_sha256(expected)
    )
    assert project_rows(worker, [decision], "a" * 64, "c" * 40) == rows
