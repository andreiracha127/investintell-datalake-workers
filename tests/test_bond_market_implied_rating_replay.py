"""Determinism replay: exact-type export, two fresh processes, compared digests, no writes.

Fake connections only; the replay children are REAL subprocesses of the test
interpreter running the pure build on a tiny synthetic export. No DSN, no DB.
"""
from __future__ import annotations

import json
import os
import pickle
import stat
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from src.bonds import implied_rating as policy
from src.bonds import implied_rating_replay as replay
from src.bonds.implied_rating_build import build_payload_from_snapshot
from src.workers import bond_market_implied_rating as worker

MONTHS = ("2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01")
PARENT = {
    "publication_id": "panel-current",
    "first_month": date(2026, 5, 1),
    "last_closed_month": date(2026, 8, 1),
    "open_month": date(2026, 9, 1),
}
REVISION = "replay-revision"
POINTER = "implied-current"
SECRET_DSN = "postgresql://worker_writer:hunter2@db.internal:5432/market"


def _rows() -> list[tuple]:
    """Raw fetchall-shaped tuples: Decimal numerics, dates, an int-with-null column."""
    rows = []
    for index, month in enumerate(MONTHS):
        rows.append((
            "AAA000001", date.fromisoformat(month), Decimal("95.1250"),
            Decimal("180.50"), Decimal("5.25"), 10 + index, Decimal("1500000.00"),
            date(2035, 1, 1),
        ))
        rows.append((
            "BBB000002", date.fromisoformat(month), Decimal("40.0000"),
            Decimal("2600.00"), Decimal("3.10"), None if index == 1 else 12,
            Decimal("900000.00"), None,
        ))
    return rows


def _snapshot() -> pd.DataFrame:
    # The same constructor shape as worker._read_snapshot (list of tuples + names).
    return pd.DataFrame(_rows(), columns=list(worker.STAGE_COLUMNS))


def _direct_build(snapshot: pd.DataFrame, *, pinned_anchor=None):
    return build_payload_from_snapshot(
        snapshot, last_closed_month=PARENT["last_closed_month"], revision=REVISION,
        panel_publication_id=PARENT["publication_id"],
        input_fingerprint=policy.snapshot_fingerprint(snapshot), pinned_anchor=pinned_anchor,
    )


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
def test_export_roundtrip_preserves_exact_types_and_fingerprint(tmp_path):
    snapshot = _snapshot()
    fingerprint = policy.snapshot_fingerprint(snapshot)
    assert snapshot["price"].map(type).eq(Decimal).all()
    assert snapshot["maturity_date"].iloc[0].__class__ is date
    assert str(snapshot["trade_count"].dtype) == "float64"  # int-with-null, as pandas infers it
    export_path = tmp_path / "snapshot_export.pkl"
    export = replay.write_export(
        export_path, snapshot=snapshot, parent=PARENT, pointer=POINTER, pinned_anchor=None,
        input_fingerprint=fingerprint, revision=REVISION,
    )
    assert export["row_count"] == len(snapshot)
    assert export["columns"] == list(worker.STAGE_COLUMNS)
    if os.name != "nt":  # read-only mode bits (root ignores them, so test the bits, not access())
        assert stat.S_IMODE(export_path.stat().st_mode) == 0o444
    roundtrip = replay.verify_export_roundtrip(
        export_path, sha256=export["sha256"], snapshot=snapshot, input_fingerprint=fingerprint
    )
    assert roundtrip == {"fingerprint": fingerprint, "row_count": len(snapshot)}
    reloaded = replay.load_export(export_path, expected_sha256=export["sha256"])["frame"]
    assert reloaded["price"].map(type).eq(Decimal).all()
    assert reloaded["maturity_date"].iloc[0].__class__ is date
    assert reloaded["trade_count"].isna().sum() == 1
    pd.testing.assert_frame_equal(reloaded, snapshot, check_dtype=True, check_exact=True)


def test_export_refuses_a_tampered_file_before_unpickling(tmp_path):
    snapshot = _snapshot()
    export_path = tmp_path / "snapshot_export.pkl"
    export = replay.write_export(
        export_path, snapshot=snapshot, parent=PARENT, pointer=None, pinned_anchor=None,
        input_fingerprint=policy.snapshot_fingerprint(snapshot), revision=REVISION,
    )
    export_path.chmod(0o644)
    export_path.write_bytes(export_path.read_bytes() + b"\n")
    with pytest.raises(replay.ReplayError) as excinfo:
        replay.load_export(export_path, expected_sha256=export["sha256"])
    assert excinfo.value.reason == "export_sha256_mismatch"


def test_export_roundtrip_detects_a_frame_that_does_not_fingerprint_back(tmp_path):
    snapshot = _snapshot()
    export_path = tmp_path / "snapshot_export.pkl"
    export = replay.write_export(
        export_path, snapshot=snapshot, parent=PARENT, pointer=None, pinned_anchor=None,
        input_fingerprint=policy.snapshot_fingerprint(snapshot), revision=REVISION,
    )
    with pytest.raises(replay.ReplayError) as excinfo:
        replay.verify_export_roundtrip(
            export_path, sha256=export["sha256"], snapshot=snapshot, input_fingerprint="0" * 64
        )
    assert excinfo.value.reason == "export_fingerprint_mismatch"


# --------------------------------------------------------------------------- #
# Child
# --------------------------------------------------------------------------- #
def test_child_reproduces_the_direct_build_and_reports_a_manifest(tmp_path):
    snapshot = _snapshot()
    direct = _direct_build(snapshot)
    export_path = tmp_path / "snapshot_export.pkl"
    export = replay.write_export(
        export_path, snapshot=snapshot, parent=PARENT, pointer=None, pinned_anchor=None,
        input_fingerprint=direct["input_fingerprint"], revision=REVISION,
    )
    out = tmp_path / "child.json"
    rows_out = tmp_path / "rows.pkl"
    assert replay.child_main(export=export_path, sha256=export["sha256"], out=out, rows_out=rows_out) == 0
    result = json.loads(out.read_text())
    assert result["ok"] is True
    assert result["pid"] == os.getpid()
    assert result["rows_digest"] == direct["rows_digest"]
    assert result["row_count"] == direct["publication"].row_count
    assert result["publication_id"] == direct["publication"].publication_id
    assert result["l_anchor"] == {"repr": repr(direct["l_anchor"]), "hex": direct["l_anchor"].hex()}
    assert result["build_manifest"]["schema"] == "bond_build_manifest/1"
    with rows_out.open("rb") as handle:
        rows = pickle.load(handle)
    pd.testing.assert_frame_equal(rows, direct["rows"], check_exact=True)


def test_child_refuses_a_globally_dark_snapshot_with_the_typed_reason(tmp_path):
    snapshot = _snapshot()
    snapshot["trade_count"] = 0  # nothing witnessed anywhere
    export_path = tmp_path / "snapshot_export.pkl"
    export = replay.write_export(
        export_path, snapshot=snapshot, parent=PARENT, pointer=None, pinned_anchor=None,
        input_fingerprint=policy.snapshot_fingerprint(snapshot), revision=REVISION,
    )
    out = tmp_path / "child.json"
    code = replay.child_main(export=export_path, sha256=export["sha256"], out=out, rows_out=tmp_path / "r.pkl")
    assert code == replay.EXIT_CHILD_REFUSED
    result = json.loads(out.read_text())
    assert result["ok"] is False
    assert result["refusal"]["reason"] == "no_market_level_observation"


def test_child_environment_never_carries_the_database():
    env = replay.child_environment({
        "DATABASE_URL": SECRET_DSN, "DB_TLS_KEY_PEM": "pem", "PGPASSWORD": "pw",
        "PATH": "/usr/bin", "RAILWAY_GIT_COMMIT_SHA": "abc", "OMP_NUM_THREADS": "1",
    })
    assert "DATABASE_URL" not in env and "DB_TLS_KEY_PEM" not in env and "PGPASSWORD" not in env
    assert env["RAILWAY_GIT_COMMIT_SHA"] == "abc"
    assert env["OMP_NUM_THREADS"] == "1"
    assert env["PYTHONPATH"] == str(replay.ROOT)


# --------------------------------------------------------------------------- #
# Parent: end to end with a fake connection and REAL child processes
# --------------------------------------------------------------------------- #
class _FakeConnection:
    def __init__(self, *, options=None):
        self.options = options
        self.commits = 0
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        self.commits += 1

    def execute(self, sql, params=None):
        self.statements.append(sql)
        return self

    def fetchone(self):
        return None


def _patch_capture(monkeypatch, *, snapshot, pointer=None, pinned_anchor=None, moved=None):
    conn = _FakeConnection()
    seen = {}

    def read_only_connect(dsn, **kwargs):
        seen["dsn"] = dsn
        seen["timeouts"] = kwargs
        return conn

    monkeypatch.setattr(replay, "read_only_connect", read_only_connect)
    monkeypatch.setattr(worker, "_code_revision", lambda: REVISION)
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    state = {"reads": 0}

    def current_panel(_conn):
        if moved == "panel" and state["reads"] > 0:
            return {**PARENT, "publication_id": "panel-next"}
        return dict(PARENT)

    def current_pointer(_conn):
        if moved == "pointer" and state["reads"] > 0:
            return "implied-next"
        return pointer

    def read_snapshot(_conn, **kwargs):
        state["reads"] += 1
        return snapshot

    monkeypatch.setattr(worker, "_current_panel", current_panel)
    monkeypatch.setattr(worker, "_current_pointer", current_pointer)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: True)
    monkeypatch.setattr(worker, "_read_snapshot", read_snapshot)
    monkeypatch.setattr(worker, "_pointer_build", lambda _conn, p: None)
    monkeypatch.setattr(replay, "current_pinned_anchor", lambda _conn: pinned_anchor)
    monkeypatch.setattr(worker, "install_schema", lambda _conn: pytest.fail("DDL in a determinism check"))
    monkeypatch.setattr(worker, "materialize", lambda *a, **k: pytest.fail("write in a determinism check"))
    return conn, seen


def test_determinism_check_runs_two_fresh_processes_and_writes_a_receipt(monkeypatch, tmp_path):
    snapshot = _snapshot()
    direct = _direct_build(snapshot)
    conn, seen = _patch_capture(monkeypatch, snapshot=snapshot)
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    receipt_path = tmp_path / "receipt.json"
    code, receipt = replay.determinism_check(
        SECRET_DSN, work_dir=tmp_path / "work", receipt_path=receipt_path,
        statement_timeout_s=7, lock_timeout_s=3, idle_timeout_s=5,
    )
    assert code == 0, receipt
    assert receipt["verdict"] == "deterministic"
    assert receipt["mismatch_reasons"] == []
    assert receipt["rows_digest"] == direct["rows_digest"]
    assert receipt["row_count"] == direct["publication"].row_count
    assert receipt["publication_id"] == direct["publication"].publication_id
    assert receipt["input_fingerprint"] == direct["input_fingerprint"]
    assert receipt["policy_digest"] == policy.POLICY_DIGEST
    assert receipt["code_revision"] == REVISION
    assert receipt["l_anchor"] == {"repr": repr(direct["l_anchor"]), "hex": direct["l_anchor"].hex()}
    pids = receipt["child_pids"]
    assert len(pids) == 2 and pids[0] != pids[1] and os.getpid() not in pids
    assert [child["pid"] for child in receipt["children"]] == pids
    assert all(child["build_manifest"]["schema"] == "bond_build_manifest/1" for child in receipt["children"])
    assert receipt["parent_build_manifest"]["schema"] == "bond_build_manifest/1"
    assert receipt["export"]["sha256"] and receipt["export"]["roundtrip"]["fingerprint"] == direct["input_fingerprint"]
    assert seen["timeouts"] == {"statement_timeout_s": 7, "lock_timeout_s": 3, "idle_timeout_s": 5}
    # Persisted receipt is the same document, and it carries no secret.
    on_disk = json.loads(receipt_path.read_text())
    assert on_disk["verdict"] == "deterministic"
    text = receipt_path.read_text()
    assert SECRET_DSN not in text and "hunter2" not in text
    assert conn.commits >= 2


def test_determinism_check_resolves_relative_paths_once_from_a_cwd_outside_root(monkeypatch, tmp_path):
    """The children chdir to ROOT: a relative --work-dir / --receipt given from another
    cwd must still name the SAME files in parent and children (resolved to absolute once)."""
    snapshot = _snapshot()
    direct = _direct_build(snapshot)
    _patch_capture(monkeypatch, snapshot=snapshot)
    outside = tmp_path / "operator-cwd"
    outside.mkdir()
    assert replay.ROOT not in outside.parents and outside != replay.ROOT
    monkeypatch.chdir(outside)
    code, receipt = replay.determinism_check(
        SECRET_DSN, work_dir=Path("relative-work"), receipt_path=Path("relative-work/receipt.json"),
    )
    assert code == 0, receipt
    assert receipt["verdict"] == "deterministic"
    assert receipt["rows_digest"] == direct["rows_digest"]
    work = Path(receipt["work_dir"])
    assert work.is_absolute() and work == (outside / "relative-work").resolve()
    assert Path(receipt["receipt_path"]).is_absolute()
    assert Path(receipt["receipt_path"]) == (outside / "relative-work" / "receipt.json").resolve()
    assert Path(receipt["receipt_path"]).is_file()
    assert Path(receipt["export"]["path"]).is_absolute()
    assert Path(receipt["export"]["path"]).parent == work
    for child in receipt["children"]:
        assert Path(child["rows_path"]).is_absolute()
        assert Path(child["rows_path"]).parent == work
        assert Path(child["rows_path"]).is_file()
    assert sorted(path.name for path in work.iterdir()) == [
        "child-1-rows.pkl", "child-1.json", "child-2-rows.pkl", "child-2.json",
        "receipt.json", "snapshot_export.pkl",
    ]
    assert not (replay.ROOT / "relative-work").exists()


def test_determinism_check_reports_a_mismatch_between_children(monkeypatch, tmp_path):
    snapshot = _snapshot()
    _patch_capture(monkeypatch, snapshot=snapshot)
    real_run_child = replay.run_child

    def run_child(index, **kwargs):
        result = real_run_child(index, **kwargs)
        if index == 2:
            result["rows_digest"] = "f" * 64  # a second process that did not reproduce
        return result

    monkeypatch.setattr(replay, "run_child", run_child)
    code, receipt = replay.determinism_check(SECRET_DSN, work_dir=tmp_path)
    assert code == replay.EXIT_MISMATCH
    assert receipt["verdict"] == "mismatch"
    assert "rows_digest_mismatch" in receipt["mismatch_reasons"]
    assert "second_rows_digest_not_reproduced_by_parent" in receipt["mismatch_reasons"]


def test_compare_children_detects_a_divergent_frame(tmp_path):
    snapshot = _snapshot()
    direct = _direct_build(snapshot)
    results = []
    for index in (1, 2):
        rows = direct["rows"].copy()
        if index == 2:
            rows.loc[0, "carry_months"] = 7
        path = tmp_path / f"rows-{index}.pkl"
        with path.open("wb") as handle:
            pickle.dump(rows, handle, protocol=5)
        results.append({
            "pid": 100 + index, "rows_digest": direct["rows_digest"],
            "row_count": len(rows), "publication_id": "p", "d_confirmed_count": 0,
            "d_candidate_count": 0, "l_anchor": None, "first_month": "a", "last_month": "b",
            "rows_path": str(path), "rows_sha256": replay._sha256_file(path),
        })
    reasons = replay.compare_children(results)
    assert "rows_frame_mismatch" in reasons
    assert "second_rows_digest_not_reproduced_by_parent" in reasons


@pytest.mark.parametrize("moved", ["panel", "pointer"])
def test_determinism_check_refuses_inputs_that_moved_during_the_read(monkeypatch, tmp_path, moved):
    _patch_capture(monkeypatch, snapshot=_snapshot(), pointer=POINTER, moved=moved)
    monkeypatch.setattr(replay, "run_child", lambda *a, **k: pytest.fail("no child on moved inputs"))
    code, receipt = replay.determinism_check(SECRET_DSN, work_dir=tmp_path)
    assert code == replay.EXIT_OPERATIONAL
    assert receipt["verdict"] == "refused"
    assert receipt["refusal"]["input_reasons"] in (["inputs_moved"], ["panel_pointer_moved"])


def test_determinism_check_refuses_an_unresolvable_revision(monkeypatch, tmp_path):
    _patch_capture(monkeypatch, snapshot=_snapshot())
    monkeypatch.setattr(worker, "_code_revision", lambda: "unknown")
    monkeypatch.setattr(replay, "read_only_connect", lambda *a, **k: pytest.fail("no connection"))
    code, receipt = replay.determinism_check(SECRET_DSN, work_dir=tmp_path)
    assert code == replay.EXIT_OPERATIONAL
    assert receipt["refusal"]["input_reasons"] == ["code_revision_absent"]


def test_determinism_check_applies_expectations(monkeypatch, tmp_path):
    snapshot = _snapshot()
    direct = _direct_build(snapshot)
    _patch_capture(monkeypatch, snapshot=snapshot)
    monkeypatch.setattr(replay, "run_child", lambda *a, **k: pytest.fail("fingerprint checked first"))
    code, receipt = replay.determinism_check(
        SECRET_DSN, work_dir=tmp_path / "a", expect_input_fingerprint="0" * 64
    )
    assert (code, receipt["mismatch_reasons"]) == (1, ["expected_input_fingerprint_mismatch"])
    monkeypatch.undo()
    _patch_capture(monkeypatch, snapshot=snapshot)
    code, receipt = replay.determinism_check(
        SECRET_DSN, work_dir=tmp_path / "b", expect_input_fingerprint=direct["input_fingerprint"],
        expect_rows_digest="0" * 64,
    )
    assert code == 1
    assert receipt["mismatch_reasons"] == ["expected_rows_digest_mismatch"]
    assert receipt["rows_digest"] == direct["rows_digest"]


def test_read_only_connect_pins_read_only_and_bounded_timeouts(monkeypatch):
    seen = {}

    def connect(dsn, **kwargs):
        seen["dsn"] = dsn
        seen["kwargs"] = kwargs
        return "conn"

    monkeypatch.setattr(replay.psycopg, "connect", connect)
    assert replay.read_only_connect(SECRET_DSN, statement_timeout_s=10, lock_timeout_s=2, idle_timeout_s=3) == "conn"
    assert seen["dsn"] == SECRET_DSN
    assert seen["kwargs"] == {"options": (
        "-c default_transaction_read_only=on -c statement_timeout=10000 "
        "-c lock_timeout=2000 -c idle_in_transaction_session_timeout=3000"
    )}


def test_replay_module_never_references_schema_install_or_materialize():
    source = Path(replay.__file__).read_text(encoding="utf-8")
    body = source.split('"""', 2)[2]  # skip the module docstring, which names them to forbid them
    assert "install_schema" not in body
    assert "materialize(" not in body
