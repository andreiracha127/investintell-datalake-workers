"""Determinism replay (G1(a)) for ``bond_market_implied_rating_v1`` -- never writes.

One read of the served closed snapshot, exported once to an immutable file,
then rebuilt by TWO sequential FRESH interpreter processes that each run the
same pure build (``src.bonds.implied_rating_build.build_payload_from_snapshot``:
anchor validation under the fixed policy, pinned-anchor gate, state machine,
``rows_digest``, identity) and emit a result + build manifest. The parent
compares the two digests, row counts, publication ids and the full published
frames cell by cell, and writes a JSON receipt. Any mismatch, missing digest,
refusal, short-circuit, roundtrip drift or moved input is a non-zero exit.

What it NEVER does: ``install_schema``, ``materialize``, a pointer move, a DDL
statement, a write of any kind. The database session is opened with
``default_transaction_read_only=on`` and bounded ``statement_timeout`` /
``lock_timeout`` / ``idle_in_transaction_session_timeout``, so a stray write
is refused by PostgreSQL itself and a stuck read cannot hold the ledger.

Process model: the children are spawned with ``subprocess`` (exec of
``sys.executable -m src.bonds.implied_rating_replay --child``), i.e. a fresh
interpreter with its own NumPy import and SIMD dispatch -- never a fork of
the parent after NumPy is loaded. The children receive no DSN (DATABASE_URL,
DB_TLS_* and PG* are stripped from their environment) and cannot reach the
database at all.

Export format: a pickle (protocol 5) of the exact in-memory frame the worker
would have built from, so Decimal / date / int-with-null cells keep their
exact Python/pandas types; the file is made read-only, its sha256 is pinned
in the receipt and re-verified before every unpickle, and the parent proves
the roundtrip by recomputing ``snapshot_fingerprint`` and comparing the
reloaded frame to the original with exact equality before any child runs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import pickle
import stat
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg

from src.bonds import implied_rating as policy
from src.bonds.build_manifest import collect_build_manifest, manifest_summary
from src.bonds.implied_rating_build import build_payload_from_snapshot
from src.bonds.implied_rating_materializer import PRODUCT, current_pinned_anchor
from src.db import resolve_dsn
from src.workers import bond_market_implied_rating as worker

LOGGER = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]

EXPORT_SCHEMA = "bond_implied_rating_snapshot_export/1"
RECEIPT_SCHEMA = "bond_implied_rating_determinism_receipt/1"
CHILD_RESULT_SCHEMA = "bond_implied_rating_replay_child/1"
CHILD_COUNT = 2

DEFAULT_STATEMENT_TIMEOUT_S = 1800
DEFAULT_LOCK_TIMEOUT_S = 30
DEFAULT_IDLE_TIMEOUT_S = 600
DEFAULT_CHILD_TIMEOUT_S = 6 * 3600

EXIT_DETERMINISTIC = 0
EXIT_MISMATCH = 1
EXIT_OPERATIONAL = 2
EXIT_CHILD_REFUSED = 3

#: Environment keys a replay child must never see (it cannot reach the database).
_CHILD_ENV_STRIPPED_PREFIXES = ("DATABASE_URL", "DB_TLS_", "PG")


class ReplayError(Exception):
    """A typed, sanitized refusal of the determinism check (no secrets)."""

    def __init__(self, reason: str, **detail: Any) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


# --------------------------------------------------------------------------- #
# Read-only capture
# --------------------------------------------------------------------------- #
def read_only_connect(
    dsn: str | None = None, *, statement_timeout_s: int = DEFAULT_STATEMENT_TIMEOUT_S,
    lock_timeout_s: int = DEFAULT_LOCK_TIMEOUT_S, idle_timeout_s: int = DEFAULT_IDLE_TIMEOUT_S,
) -> psycopg.Connection:
    """A session PostgreSQL itself keeps read-only, with bounded timeouts."""
    options = (
        "-c default_transaction_read_only=on "
        f"-c statement_timeout={int(statement_timeout_s) * 1000} "
        f"-c lock_timeout={int(lock_timeout_s) * 1000} "
        f"-c idle_in_transaction_session_timeout={int(idle_timeout_s) * 1000}"
    )
    return psycopg.connect(resolve_dsn(dsn), options=options)


def _parent_moved(before: dict[str, Any], after: dict[str, Any] | None) -> bool:
    return (
        after is None
        or after["publication_id"] != before["publication_id"]
        or after["last_closed_month"] != before["last_closed_month"]
    )


def capture_snapshot(conn: psycopg.Connection, *, revision: str, started: float) -> dict[str, Any]:
    """Gates + ONE snapshot read + the pinned anchor, then prove nothing moved.

    Returns ``{"failure": <worker-typed dict>}`` or ``{"parent", "pointer",
    "pointer_build", "rebuild_reasons", "snapshot", "input_fingerprint",
    "pinned_anchor"}``. Uses the worker's own gate and read functions so the
    replay cannot read a different snapshot than a publication would.
    """
    gates = worker._gates(conn, started=started)
    if "failure" in gates:
        return gates
    parent = gates["parent"]
    pointer = gates["pointer"]
    _current, rebuild_reasons = worker._currentness(
        conn, parent=parent, revision=revision, pointer=pointer
    )
    inputs = worker._read_snapshot_inputs(conn, parent=parent, started=started)
    if "failure" in inputs:
        return inputs
    pointer_build = worker._pointer_build(conn, pointer) if pointer is not None else None
    pinned_anchor = (
        current_pinned_anchor(conn)
        if worker._relation_exists(conn, f"{PRODUCT}_builds") else None
    )
    conn.commit()
    if _parent_moved(parent, worker._current_panel(conn)) or worker._current_pointer(conn) != pointer:
        return {"failure": worker._failure(
            "implied_rating_gate_failed", elapsed=time.monotonic() - started,
            input_reasons=["inputs_moved"], panel_publication_id=parent["publication_id"],
        )}
    conn.commit()
    return {
        "parent": parent,
        "pointer": pointer,
        "pointer_build": pointer_build,
        "rebuild_reasons": rebuild_reasons,
        "snapshot": inputs["snapshot"],
        "input_fingerprint": inputs["input_fingerprint"],
        "pinned_anchor": pinned_anchor,
    }


# --------------------------------------------------------------------------- #
# Immutable export
# --------------------------------------------------------------------------- #
def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_export(
    path: Path, *, snapshot: pd.DataFrame, parent: dict[str, Any], pointer: str | None,
    pinned_anchor: float | None, input_fingerprint: str, revision: str,
) -> dict[str, Any]:
    """Pickle the exact frame once, make the file read-only, return its identity."""
    document = {
        "schema": EXPORT_SCHEMA,
        "product": PRODUCT,
        "policy_version": policy.POLICY_VERSION,
        "policy_digest": policy.POLICY_DIGEST,
        "code_revision": revision,
        "panel_publication_id": parent["publication_id"],
        "last_closed_month": parent["last_closed_month"],
        "pointer": pointer,
        "pinned_anchor": pinned_anchor,
        "input_fingerprint": input_fingerprint,
        "row_count": len(snapshot),
        "columns": list(snapshot.columns),
        "dtypes": {column: str(dtype) for column, dtype in snapshot.dtypes.items()},
        "frame": snapshot,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as handle:
        pickle.dump(document, handle, protocol=5)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    return {
        "path": str(path),
        "sha256": _sha256_file(path),
        "bytes": path.stat().st_size,
        "row_count": document["row_count"],
        "columns": document["columns"],
        "dtypes": document["dtypes"],
    }


def load_export(path: Path, *, expected_sha256: str) -> dict[str, Any]:
    """Verify the pinned sha256 BEFORE unpickling; refuse any other document."""
    actual = _sha256_file(path)
    if actual != expected_sha256:
        raise ReplayError("export_sha256_mismatch", expected=expected_sha256, actual=actual)
    with path.open("rb") as handle:
        document = pickle.load(handle)  # sha256-pinned above; written by this module
    if not isinstance(document, dict) or document.get("schema") != EXPORT_SCHEMA:
        raise ReplayError("export_schema_mismatch")
    if not isinstance(document.get("frame"), pd.DataFrame):
        raise ReplayError("export_frame_missing")
    return document


def verify_export_roundtrip(path: Path, *, sha256: str, snapshot: pd.DataFrame, input_fingerprint: str) -> dict[str, Any]:
    """Reload the export and prove it is the exact frame (types included)."""
    document = load_export(path, expected_sha256=sha256)
    frame = document["frame"]
    reloaded_fingerprint = policy.snapshot_fingerprint(frame)
    if reloaded_fingerprint != input_fingerprint:
        raise ReplayError(
            "export_fingerprint_mismatch", expected=input_fingerprint, actual=reloaded_fingerprint,
        )
    try:
        pd.testing.assert_frame_equal(frame, snapshot, check_dtype=True, check_exact=True)
    except AssertionError as exc:
        raise ReplayError("export_frame_mismatch", detail=str(exc)[:500]) from exc
    return {"fingerprint": reloaded_fingerprint, "row_count": len(frame)}


# --------------------------------------------------------------------------- #
# Child: one fresh process, one pure build
# --------------------------------------------------------------------------- #
def _anchor_repr(value: float | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {"repr": repr(float(value)), "hex": float(value).hex()}


def child_main(*, export: Path, sha256: str, out: Path, rows_out: Path) -> int:
    started = time.monotonic()
    manifest = collect_build_manifest()
    LOGGER.info("replay child pid=%s manifest: %s", os.getpid(), manifest_summary(manifest))
    document = load_export(export, expected_sha256=sha256)
    frame = document["frame"]
    fingerprint = policy.snapshot_fingerprint(frame)
    if fingerprint != document["input_fingerprint"]:
        raise ReplayError(
            "child_fingerprint_mismatch", expected=document["input_fingerprint"], actual=fingerprint,
        )
    if document["policy_digest"] != policy.POLICY_DIGEST:
        raise ReplayError(
            "child_policy_digest_mismatch", expected=document["policy_digest"],
            actual=policy.POLICY_DIGEST,
        )
    built = build_payload_from_snapshot(
        frame,
        last_closed_month=document["last_closed_month"],
        revision=document["code_revision"],
        panel_publication_id=document["panel_publication_id"],
        input_fingerprint=document["input_fingerprint"],
        pinned_anchor=document["pinned_anchor"],
        logger=LOGGER,
    )
    result: dict[str, Any] = {
        "schema": CHILD_RESULT_SCHEMA,
        "pid": os.getpid(),
        "export_sha256": sha256,
        "input_fingerprint": fingerprint,
        "build_manifest": manifest,
        "elapsed_seconds": None,
    }
    if "refusal" in built:
        result.update({"ok": False, "refusal": built["refusal"]})
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        out.write_text(json.dumps(result, default=str, sort_keys=True), encoding="utf-8")
        return EXIT_CHILD_REFUSED
    publication = built["publication"]
    rows: pd.DataFrame = built["rows"]
    with rows_out.open("wb") as handle:
        pickle.dump(rows, handle, protocol=5)
    result.update({
        "ok": True,
        "publication_id": publication.publication_id,
        "rows_digest": built["rows_digest"],
        "row_count": publication.row_count,
        "first_month": publication.first_month.isoformat(),
        "last_month": publication.last_month.isoformat(),
        "l_anchor": _anchor_repr(built["l_anchor"]),
        "resolved_l_anchor": _anchor_repr(built["resolved_l_anchor"]),
        "pinned_l_anchor": _anchor_repr(built["pinned_l_anchor"]),
        "anchor_source": built["anchor_source"],
        "anchor_diagnostic_reason": built["anchor_diagnostic_reason"],
        "anchor_diagnostic_drift": built["anchor_diagnostic_drift"],
        "d_confirmed_count": built["d_confirmed_count"],
        "d_candidate_count": built["d_candidate_count"],
        "bucket_counts": built["bucket_counts"],
        "rows_sha256": _sha256_file(rows_out),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    })
    out.write_text(json.dumps(result, default=str, sort_keys=True), encoding="utf-8")
    return EXIT_DETERMINISTIC


def child_environment(environ: dict[str, str]) -> dict[str, str]:
    """The child's environment: everything except anything that names the database."""
    child = {
        key: value for key, value in environ.items()
        if not key.startswith(_CHILD_ENV_STRIPPED_PREFIXES)
    }
    child["PYTHONPATH"] = str(ROOT)
    return child


def run_child(
    index: int, *, export: Path, sha256: str, work_dir: Path, timeout_s: int,
) -> dict[str, Any]:
    out = work_dir / f"child-{index}.json"
    rows_out = work_dir / f"child-{index}-rows.pkl"
    argv = [
        sys.executable, "-m", "src.bonds.implied_rating_replay", "--child",
        "--export", str(export), "--sha256", sha256,
        "--out", str(out), "--rows-out", str(rows_out),
    ]
    completed = subprocess.run(  # fixed argv, no shell: exec of sys.executable
        argv, cwd=str(ROOT), env=child_environment(dict(os.environ)),
        capture_output=False, check=False, timeout=timeout_s,
    )
    if not out.is_file():
        raise ReplayError("child_result_missing", index=index, returncode=completed.returncode)
    result = json.loads(out.read_text(encoding="utf-8"))
    result["returncode"] = completed.returncode
    result["rows_path"] = str(rows_out)
    if result.get("schema") != CHILD_RESULT_SCHEMA or "pid" not in result:
        raise ReplayError("child_result_invalid", index=index)
    if completed.returncode != 0 or not result.get("ok"):
        raise ReplayError(
            "child_refused", index=index, returncode=completed.returncode,
            refusal=result.get("refusal"),
        )
    if not result.get("rows_digest"):
        raise ReplayError("child_digest_missing", index=index)
    return result


# --------------------------------------------------------------------------- #
# Parent: capture, export, two children, compare, receipt
# --------------------------------------------------------------------------- #
def _load_rows(result: dict[str, Any]) -> pd.DataFrame:
    path = Path(result["rows_path"])
    actual = _sha256_file(path)
    if actual != result["rows_sha256"]:
        raise ReplayError("child_rows_sha256_mismatch", pid=result["pid"])
    with path.open("rb") as handle:
        rows = pickle.load(handle)  # sha256-pinned above; written by the child
    if not isinstance(rows, pd.DataFrame):
        raise ReplayError("child_rows_invalid", pid=result["pid"])
    return rows


def compare_children(results: list[dict[str, Any]]) -> list[str]:
    """Every reason the two fresh builds are NOT the same publication."""
    if len(results) != CHILD_COUNT:
        return ["child_count"]
    first, second = results
    reasons: list[str] = []
    if first["pid"] == second["pid"]:
        reasons.append("same_pid")
    for key in ("rows_digest", "row_count", "publication_id", "d_confirmed_count",
                "d_candidate_count", "l_anchor", "first_month", "last_month"):
        if first.get(key) != second.get(key):
            reasons.append(f"{key}_mismatch")
    rows_first = _load_rows(first)
    rows_second = _load_rows(second)
    if policy.rows_digest(rows_first) != first["rows_digest"]:
        reasons.append("first_rows_digest_not_reproduced_by_parent")
    if policy.rows_digest(rows_second) != second["rows_digest"]:
        reasons.append("second_rows_digest_not_reproduced_by_parent")
    try:
        pd.testing.assert_frame_equal(rows_first, rows_second, check_dtype=True, check_exact=True)
    except AssertionError:
        reasons.append("rows_frame_mismatch")
    return reasons


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, default=str, sort_keys=True, indent=1), encoding="utf-8")


def determinism_check(
    dsn: str | None = None, *, work_dir: Path | None = None, receipt_path: Path | None = None,
    expect_input_fingerprint: str | None = None, expect_rows_digest: str | None = None,
    statement_timeout_s: int = DEFAULT_STATEMENT_TIMEOUT_S,
    lock_timeout_s: int = DEFAULT_LOCK_TIMEOUT_S, idle_timeout_s: int = DEFAULT_IDLE_TIMEOUT_S,
    child_timeout_s: int = DEFAULT_CHILD_TIMEOUT_S,
) -> tuple[int, dict[str, Any]]:
    """Run the full G1(a) replay; return ``(exit_code, receipt)`` and write the receipt."""
    started = time.monotonic()
    started_at = datetime.now(UTC).isoformat()
    work = Path(work_dir) if work_dir is not None else Path(
        tempfile.mkdtemp(prefix="bond-implied-rating-replay-")
    )
    work.mkdir(parents=True, exist_ok=True)
    receipt_file = Path(receipt_path) if receipt_path is not None else work / "determinism_receipt.json"
    parent_manifest = collect_build_manifest()
    LOGGER.info("determinism check parent pid=%s manifest: %s", os.getpid(), manifest_summary(parent_manifest))
    receipt: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "product": PRODUCT,
        "started_at": started_at,
        "parent_pid": os.getpid(),
        "work_dir": str(work),
        "policy_version": policy.POLICY_VERSION,
        "policy_digest": policy.POLICY_DIGEST,
        "parent_build_manifest": parent_manifest,
        "expectations": {
            "input_fingerprint": expect_input_fingerprint,
            "rows_digest": expect_rows_digest,
        },
        "verdict": None,
        "mismatch_reasons": [],
    }

    def finish(code: int, verdict: str, **extra: Any) -> tuple[int, dict[str, Any]]:
        receipt.update(extra)
        receipt["verdict"] = verdict
        receipt["exit_code"] = code
        receipt["elapsed_seconds"] = round(time.monotonic() - started, 3)
        receipt["finished_at"] = datetime.now(UTC).isoformat()
        _write_receipt(receipt_file, receipt)
        receipt["receipt_path"] = str(receipt_file)
        return code, receipt

    revision, refusal = worker._revision_or_failure(started)
    if refusal is not None:
        return finish(EXIT_OPERATIONAL, "refused", refusal=refusal)
    receipt["code_revision"] = revision
    try:
        with read_only_connect(
            dsn, statement_timeout_s=statement_timeout_s, lock_timeout_s=lock_timeout_s,
            idle_timeout_s=idle_timeout_s,
        ) as conn:
            captured = capture_snapshot(conn, revision=revision, started=started)
    except psycopg.Error as exc:
        return finish(EXIT_OPERATIONAL, "refused", refusal={
            "reason": "database_error", "error": type(exc).__name__,
        })
    if "failure" in captured:
        return finish(EXIT_OPERATIONAL, "refused", refusal=captured["failure"])
    parent = captured["parent"]
    receipt.update({
        "panel_publication_id": parent["publication_id"],
        "panel_last_closed_month": parent["last_closed_month"].isoformat(),
        "current_pointer": captured["pointer"],
        "pointer_build": captured["pointer_build"],
        "rebuild_reasons": captured["rebuild_reasons"],
        "input_fingerprint": captured["input_fingerprint"],
        "snapshot_row_count": len(captured["snapshot"]),
        "pinned_l_anchor": _anchor_repr(captured["pinned_anchor"]),
    })
    if expect_input_fingerprint is not None and expect_input_fingerprint != captured["input_fingerprint"]:
        return finish(EXIT_MISMATCH, "mismatch", mismatch_reasons=["expected_input_fingerprint_mismatch"])
    try:
        export_path = work / "snapshot_export.pkl"
        export = write_export(
            export_path, snapshot=captured["snapshot"], parent=parent, pointer=captured["pointer"],
            pinned_anchor=captured["pinned_anchor"], input_fingerprint=captured["input_fingerprint"],
            revision=revision,
        )
        export["roundtrip"] = verify_export_roundtrip(
            export_path, sha256=export["sha256"], snapshot=captured["snapshot"],
            input_fingerprint=captured["input_fingerprint"],
        )
        receipt["export"] = export
        del captured["snapshot"]  # the children load the file; the parent never builds
        results: list[dict[str, Any]] = []
        for index in range(1, CHILD_COUNT + 1):
            results.append(run_child(
                index, export=export_path, sha256=export["sha256"], work_dir=work,
                timeout_s=child_timeout_s,
            ))
            receipt["children"] = results
        reasons = compare_children(results)
    except ReplayError as exc:
        return finish(EXIT_MISMATCH, "mismatch", mismatch_reasons=[exc.reason], refusal={
            "reason": exc.reason, **exc.detail,
        })
    except subprocess.TimeoutExpired:
        return finish(EXIT_OPERATIONAL, "refused", refusal={"reason": "child_timeout"})
    first = results[0]
    receipt.update({
        "rows_digest": first["rows_digest"],
        "row_count": first["row_count"],
        "publication_id": first["publication_id"],
        "l_anchor": first["l_anchor"],
        "resolved_l_anchor": first["resolved_l_anchor"],
        "d_confirmed_count": first["d_confirmed_count"],
        "d_candidate_count": first["d_candidate_count"],
        "child_pids": [result["pid"] for result in results],
    })
    if expect_rows_digest is not None and expect_rows_digest != first["rows_digest"]:
        reasons.append("expected_rows_digest_mismatch")
    if reasons:
        return finish(EXIT_MISMATCH, "mismatch", mismatch_reasons=reasons)
    return finish(EXIT_DETERMINISTIC, "deterministic")


# --------------------------------------------------------------------------- #
# Entry point (child mode only; the parent is driven by the backfill CLI)
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="replay child (internal)")
    parser.add_argument("--child", action="store_true", required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--rows-out", required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        return child_main(
            export=Path(args.export), sha256=args.sha256, out=Path(args.out),
            rows_out=Path(args.rows_out),
        )
    except ReplayError as exc:
        Path(args.out).write_text(json.dumps({
            "schema": CHILD_RESULT_SCHEMA, "pid": os.getpid(), "ok": False,
            "refusal": {"reason": exc.reason, **exc.detail},
        }, default=str, sort_keys=True), encoding="utf-8")
        return EXIT_CHILD_REFUSED


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
