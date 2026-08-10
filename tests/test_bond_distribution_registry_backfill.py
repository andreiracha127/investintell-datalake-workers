"""Production adapter contracts for a sealed Regulation S registry bundle."""
from __future__ import annotations

import contextlib
from datetime import date, datetime, timezone
import inspect
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from src.bonds.distribution_series import (
    DistributionPairDecision,
    DistributionPairIdentifier,
    ImmutableRegistryConflictError,
    distribution_snapshot_content_hash,
)
from src.workers import bond_distribution_registry_backfill as worker


def test_bundle_date_only_timestamp_is_normalized_to_utc() -> None:
    assert worker._parse_datetime("2025-01-01", "filed_at") == datetime(
        2025, 1, 1, tzinfo=timezone.utc
    )


def _bundle(*, snapshot_id: str = "draft-1", decisions: bool = True) -> dict[str, Any]:
    decision_rows = ([{
        "decision_id": "decision-1", "snapshot_id": snapshot_id, "decision_state": "approved",
        "source_observation_id": "observation-1", "valid_from": "2025-01-01", "valid_to": None,
        "pair_key": "pair-1",
    }] if decisions else [])
    identifier_rows = ([{
        "identifier_id": "identifier-1", "decision_id": "decision-1", "source_observation_id": "observation-1",
        "distribution_rule": "rule_144a", "identifier_kind": "cusip9", "identifier_value": "344045AB5",
        "identifier_tenure": "not_stated", "valid_from": "2025-01-01", "valid_to": None,
    }] if decisions else [])
    content_hash = distribution_snapshot_content_hash(snapshot_id, (
        DistributionPairDecision("decision-1", snapshot_id, "approved", "observation-1", date(2025, 1, 1), pair_key="pair-1"),
    ) if decisions else (), (
        DistributionPairIdentifier("identifier-1", "decision-1", "observation-1", "rule_144a", "cusip9", "344045AB5", "not_stated", date(2025, 1, 1)),
    ) if decisions else ())
    return {
        "database_writes": 0,
        "source_evidence_rows": [{
            "source_evidence_id": "source-1", "sec_accession": "a-1", "form_type": "424B2",
            "document_type": "EX-4.1", "source_url": "https://sec.example/a", "document_url": None,
            "filed_at": "2025-01-01T00:00:00+00:00", "retrieved_at": "2025-01-02T00:00:00+00:00",
            "raw_document_sha256": "a" * 64, "parser_version": "explicit-label-v1", "search_query_id": None,
        }],
        "parser_observation_rows": [{
            "parser_observation_id": "observation-1", "source_evidence_id": "source-1",
            "parser_version": "explicit-label-v1", "block_locator": "table[0]",
            "exact_source_label": "Rule 144A CUSIP", "source_value": "344045AB5",
            "normalized_value": "344045AB5", "observation_state": "validated",
        }],
        "mapping_snapshot_rows": [{"snapshot_id": snapshot_id, "snapshot_status": "draft", "content_hash": content_hash}],
        "pair_decision_rows": decision_rows,
        "pair_identifier_rows": identifier_rows,
        "snapshot_approval_rows": [{"snapshot_id": snapshot_id, "content_hash": content_hash}],
        "skipped_records": [],
    }


@contextlib.contextmanager
def _lock(_conn: object, _lock_id: int):
    yield True


class _Connection:
    def __init__(self, *, missing_relation: int | None = None) -> None:
        self.missing_relation = missing_relation
        self.executed: list[str] = []
        self.transaction_exits: list[type[BaseException] | None] = []

    def execute(self, sql: str, _values: object = None) -> "_Connection":
        self.executed.append(sql)
        return self

    def fetchone(self) -> tuple[object, ...]:
        relations: list[object] = [
            "bond_distribution_source_evidence",
            "bond_distribution_parser_observation",
            "bond_distribution_mapping_snapshot",
            "bond_distribution_pair_decision",
            "bond_distribution_pair_identifier",
            "bond_distribution_snapshot_approval",
        ]
        if self.missing_relation is not None:
            relations[self.missing_relation] = None
        return tuple(relations)

    @contextlib.contextmanager
    def transaction(self):
        try:
            yield self
        except BaseException as error:
            self.transaction_exits.append(type(error))
            raise
        else:
            self.transaction_exits.append(None)

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_args: object) -> None:
        pass


def _env(monkeypatch: pytest.MonkeyPatch, root: Path, *, mode: str = "draft", authorization: str = "rev-1") -> None:
    monkeypatch.setenv("BOND_DISTRIBUTION_OUTPUT_ROOT", str(root))
    monkeypatch.setenv("BOND_DISTRIBUTION_SNAPSHOT_ID", "draft-1")
    monkeypatch.setenv("BOND_DISTRIBUTION_LOAD_MODE", mode)
    monkeypatch.setenv("CODE_REVISION", "rev-1")
    monkeypatch.setenv("BOND_DISTRIBUTION_LOAD_AUTHORIZATION", authorization)


def test_wrong_authorization_refuses_before_connection_or_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path, authorization="different")
    monkeypatch.setattr(worker, "connect", lambda _dsn: pytest.fail("must not connect"))

    with pytest.raises(ValueError, match="authorization"):
        worker.run("postgresql://unused")


def test_draft_loads_typed_bundle_via_public_loader_without_approval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path)
    conn = _Connection()
    captured: dict[str, Any] = {}
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle())
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "advisory_lock", _lock)
    monkeypatch.setattr(worker, "load_distribution_registry", lambda _conn, **kwargs: captured.update(kwargs) or {
        "source_evidence": 1, "parser_observations": 1, "snapshots": 1, "decisions": 1, "identifiers": 1, "approvals": 0,
    })
    monkeypatch.setattr(worker, "approve_mapping_snapshot", lambda *_args, **_kwargs: pytest.fail("draft must not approve"))

    result = worker.run("postgresql://unused")

    assert result == {
        "state": "ok", "mode": "draft", "snapshot_id": "draft-1", "content_hash": _bundle()["mapping_snapshot_rows"][0]["content_hash"],
        "rows": {"source_evidence": 1, "parser_observations": 1, "snapshots": 1, "decisions": 1, "identifiers": 1, "approvals": 0},
    }
    assert captured["approvals"] == ()
    assert captured["snapshots"][0].status == "draft"
    assert captured["decisions"][0].valid_from.isoformat() == "2025-01-01"


def test_approve_replays_the_complete_bundle_before_using_public_approval_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path, mode="approve")
    conn = _Connection()
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle())
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "advisory_lock", _lock)
    replay: dict[str, Any] = {}
    monkeypatch.setattr(worker, "load_distribution_registry", lambda _conn, **kwargs: replay.update(kwargs) or {
        "source_evidence": 0, "parser_observations": 0, "snapshots": 0,
        "decisions": 0, "identifiers": 0, "approvals": 0,
    })
    called: dict[str, str] = {}
    monkeypatch.setattr(worker, "approve_mapping_snapshot", lambda _conn, **kwargs: called.update(kwargs) or True)

    result = worker.run("postgresql://unused")

    assert result == {"state": "ok", "mode": "approve", "snapshot_id": "draft-1", "content_hash": _bundle()["mapping_snapshot_rows"][0]["content_hash"], "approval_inserted": True}
    assert called == {"snapshot_id": "draft-1", "content_hash": _bundle()["mapping_snapshot_rows"][0]["content_hash"]}
    assert replay["approvals"] == ()
    assert replay["source_evidence"][0].source_url == "https://sec.example/a"
    assert replay["parser_observations"][0].source_evidence_id == "source-1"
    assert replay["snapshots"][0].snapshot_id == "draft-1"
    assert replay["decisions"][0].decision_id == "decision-1"
    assert replay["identifiers"][0].identifier_id == "identifier-1"
    assert conn.transaction_exits == [None]


def test_approve_refuses_metadata_conflict_without_approval_or_healing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path, mode="approve")
    conn = _Connection()
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle())
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "advisory_lock", _lock)
    monkeypatch.setattr(
        worker, "load_distribution_registry",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ImmutableRegistryConflictError("immutable_conflict:source-1")),
    )
    monkeypatch.setattr(worker, "approve_mapping_snapshot", lambda *_args, **_kwargs: pytest.fail("must not approve"))

    with pytest.raises(ImmutableRegistryConflictError, match="source-1"):
        worker.run("postgresql://unused")

    assert conn.transaction_exits == [ImmutableRegistryConflictError]


def test_approve_refuses_missing_draft_rows_instead_of_healing_them(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path, mode="approve")
    conn = _Connection()
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle())
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "advisory_lock", _lock)
    monkeypatch.setattr(worker, "load_distribution_registry", lambda *_args, **_kwargs: {
        "source_evidence": 1, "parser_observations": 0, "snapshots": 0,
        "decisions": 0, "identifiers": 0, "approvals": 0,
    })
    monkeypatch.setattr(worker, "approve_mapping_snapshot", lambda *_args, **_kwargs: pytest.fail("must not approve"))

    with pytest.raises(RuntimeError, match="already-loaded"):
        worker.run("postgresql://unused")

    assert conn.transaction_exits == [RuntimeError]


@pytest.mark.parametrize("mode", ["", "live", "APPROVE"])
def test_invalid_mode_refuses_before_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    _env(monkeypatch, tmp_path, mode=mode)
    monkeypatch.setattr(worker, "connect", lambda _dsn: pytest.fail("must not connect"))

    with pytest.raises(ValueError, match="mode"):
        worker.run("postgresql://unused")


def test_snapshot_mismatch_empty_cohort_and_absent_schema_fail_before_loader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path)
    conn = _Connection(missing_relation=3)
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle(snapshot_id="other"))
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "advisory_lock", _lock)
    monkeypatch.setattr(worker, "load_distribution_registry", lambda *_args, **_kwargs: pytest.fail("must not load"))

    with pytest.raises(ValueError, match="snapshot"):
        worker.run("postgresql://unused")

    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle(decisions=False))
    with pytest.raises(ValueError, match="empty"):
        worker.run("postgresql://unused")

    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle())
    with pytest.raises(RuntimeError, match="schema"):
        worker.run("postgresql://unused")
    assert "bond_distribution_pair_decision" in conn.executed[-1]


def test_content_hash_mismatch_refuses_before_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path)
    bundle = _bundle()
    bundle["mapping_snapshot_rows"][0]["content_hash"] = "c" * 64
    bundle["snapshot_approval_rows"][0]["content_hash"] = "c" * 64
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: bundle)
    monkeypatch.setattr(worker, "connect", lambda _dsn: pytest.fail("must not connect"))

    with pytest.raises(ValueError, match="content hash"):
        worker.run("postgresql://unused")


def test_unbound_source_evidence_refuses_before_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path)
    bundle = _bundle()
    bundle["parser_observation_rows"][0]["source_evidence_id"] = "missing-source"
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: bundle)
    monkeypatch.setattr(worker, "connect", lambda _dsn: pytest.fail("must not connect"))

    with pytest.raises(ValueError, match="source evidence"):
        worker.run("postgresql://unused")


def test_lock_contention_aborts_without_schema_or_loader_work(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, tmp_path)
    conn = _Connection()
    monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: _bundle())
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "advisory_lock", lambda *_args: contextlib.nullcontext(False))
    monkeypatch.setattr(worker, "load_distribution_registry", lambda *_args, **_kwargs: pytest.fail("must not load"))

    assert worker.run("postgresql://unused") == {
        "state": "locked", "mode": "draft", "snapshot_id": "draft-1",
        "content_hash": _bundle()["mapping_snapshot_rows"][0]["content_hash"], "aborted": True,
    }
    assert conn.executed == []


def test_dispatcher_exits_nonzero_for_locked_worker_without_loading(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from src import run_worker as dispatcher

    calls: list[str] = []
    monkeypatch.setenv("WORKER", "bond_distribution_registry_backfill")
    monkeypatch.delenv("WORKER_LIMIT", raising=False)
    monkeypatch.delenv("WORKER_CALC_DATE", raising=False)
    monkeypatch.setattr(dispatcher, "resolve_dsn", lambda: "postgresql://private")
    monkeypatch.setattr(worker, "run", lambda dsn: calls.append(dsn) or {"state": "locked", "aborted": True})

    with pytest.raises(SystemExit) as exit_info:
        dispatcher.main()

    assert exit_info.value.code != 0
    assert calls == ["postgresql://private"]
    assert json.loads(capsys.readouterr().out) == {
        "worker": "bond_distribution_registry_backfill", "state": "locked", "aborted": True,
    }


@pytest.mark.parametrize(("name", "value"), [("WORKER_LIMIT", "1"), ("WORKER_CALC_DATE", "2025-01-01")])
def test_dispatcher_rejects_inherited_controls_before_registry_worker_runs(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], name: str, value: str,
) -> None:
    from src import run_worker as dispatcher

    monkeypatch.setenv("WORKER", "bond_distribution_registry_backfill")
    monkeypatch.delenv("WORKER_LIMIT", raising=False)
    monkeypatch.delenv("WORKER_CALC_DATE", raising=False)
    monkeypatch.setenv(name, value)
    monkeypatch.setattr(dispatcher, "resolve_dsn", lambda: pytest.fail("must not resolve dsn"))

    with pytest.raises(SystemExit) as exit_info:
        dispatcher.main()

    assert exit_info.value.code != 0
    assert name.split("_")[-1].lower() in str(exit_info.value.code)
    assert capsys.readouterr().out == ""
    assert "calc_date" not in inspect.signature(worker.run).parameters
    assert "limit" not in inspect.signature(worker.run).parameters


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL unavailable",
)
def test_postgres_approve_replays_exact_bundle_and_rejects_changed_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The approve route is a byte-for-byte replay, never a draft-healing write."""
    import psycopg
    from psycopg import sql

    from src.bonds.distribution_series import install_schema

    schema = f"test_distribution_registry_worker_{uuid4().hex}"
    conn = psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"])
    bundle = _bundle()
    try:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        install_schema(conn)
        conn.commit()
        _env(monkeypatch, tmp_path)
        monkeypatch.setattr(worker, "connect", lambda _dsn: contextlib.nullcontext(conn))
        monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: bundle)

        draft = worker.run("postgresql://unused")
        assert draft["rows"] == {
            "source_evidence": 1, "parser_observations": 1, "snapshots": 1,
            "decisions": 1, "identifiers": 1, "approvals": 0,
        }

        changed = _bundle()
        changed["source_evidence_rows"][0]["source_url"] = "https://sec.example/changed"
        monkeypatch.setenv("BOND_DISTRIBUTION_LOAD_MODE", "approve")
        monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: changed)
        with pytest.raises(ImmutableRegistryConflictError, match="source-1"):
            worker.run("postgresql://unused")
        assert conn.execute("SELECT count(*) FROM bond_distribution_snapshot_approval").fetchone()[0] == 0

        monkeypatch.setattr(worker, "build_registry_bundle", lambda *_args: bundle)
        assert worker.run("postgresql://unused")["approval_inserted"] is True
        assert worker.run("postgresql://unused")["approval_inserted"] is False
    finally:
        conn.execute("SET search_path TO public")
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        conn.commit()
        conn.close()
