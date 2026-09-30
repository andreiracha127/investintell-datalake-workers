"""Worker identity and mirror-provenance gates, independent of policy arithmetic."""
from __future__ import annotations

import logging
from datetime import date
from types import SimpleNamespace
from uuid import UUID

import pandas as pd
import pytest

from src.workers import bond_market_implied_rating as worker

PARENT = {
    "publication_id": "panel-current",
    "first_month": date(2026, 7, 1),
    "last_closed_month": date(2026, 8, 1),
    "open_month": date(2026, 9, 1),
}
REVISION = "runtime-revision"
POINTER = "implied-current"


def _snapshot():
    return pd.DataFrame([
        {
            "cusip_id": "AAA000001", "month": pd.Timestamp(month), "price": 95.0,
            "spread_final_bps": 80.0, "mod_dur": 5.0, "trade_count": 10,
            "dollar_volume": 1_000_000.0, "maturity_date": date(2035, 1, 1),
        }
        for month in ("2026-07-01", "2026-08-01")
    ])


def _matching_build():
    return {
        "publication_id": POINTER,
        "panel_publication_id": PARENT["publication_id"],
        "policy_digest": worker.policy.POLICY_DIGEST,
        "code_revision": REVISION,
        "panel_last_closed_month": PARENT["last_closed_month"],
        "lifecycle_state": "validated",
        "input_fingerprint": "a" * 64,
    }


class _FakeConnection:
    def __init__(self, row=None):
        self.row = row
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        return self

    def fetchone(self):
        return self.row


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("lifecycle_state", "prepared", "pointer_not_validated"),
        ("panel_publication_id", "panel-previous", "panel_publication_changed"),
        ("panel_last_closed_month", date(2026, 7, 1), "panel_month_changed"),
        ("policy_digest", "previous-policy", "policy_digest_changed"),
        ("code_revision", "previous-revision", "code_revision_changed"),
    ],
)
def test_currentness_mismatch_identifies_each_changed_input(field, value, reason):
    build = {**_matching_build(), field: value}
    assert worker._currentness_mismatch(build, parent=PARENT, revision=REVISION) == [reason]


def test_currentness_mismatch_reports_an_absent_pointer_build():
    assert worker._currentness_mismatch(None, parent=PARENT, revision=REVISION) == [
        "pointer_build_absent"
    ]


def test_currentness_mismatch_accepts_all_matching_identity_inputs():
    assert worker._currentness_mismatch(_matching_build(), parent=PARENT, revision=REVISION) == []


def test_currentness_mismatch_reports_all_reasons_in_gate_order():
    build = {
        **_matching_build(), "lifecycle_state": "prepared",
        "panel_publication_id": "old-panel", "panel_last_closed_month": date(2026, 7, 1),
        "policy_digest": "old-policy", "code_revision": "old-revision",
    }
    assert worker._currentness_mismatch(build, parent=PARENT, revision=REVISION) == [
        "pointer_not_validated", "panel_publication_changed", "panel_month_changed",
        "policy_digest_changed", "code_revision_changed",
    ]


def test_pointer_build_selects_only_the_pointer_with_bound_parameters(monkeypatch):
    publication_id = UUID("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa")
    panel_id = UUID("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb")
    conn = _FakeConnection((
        publication_id, panel_id, worker.policy.POLICY_DIGEST, REVISION,
        PARENT["last_closed_month"], "validated", "a" * 64,
    ))
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    assert worker._pointer_build(conn, str(publication_id)) == {
        **_matching_build(), "publication_id": str(publication_id),
        "panel_publication_id": str(panel_id),
    }
    (sql, params), = conn.statements
    assert sql == (
        "SELECT b.publication_id::text, b.panel_publication_id::text, b.policy_digest, "
        "b.code_revision, b.panel_last_closed_month, s.lifecycle_state, b.input_fingerprint "
        f"FROM {worker.PRODUCT}_builds b "
        "JOIN sec_derived_publications s USING (publication_id) WHERE b.publication_id = %s"
    )
    assert params == (str(publication_id),)


def test_pointer_build_missing_relation_does_not_query_the_ledger(monkeypatch):
    conn = _FakeConnection()
    names = []

    def relation_exists(_conn, name):
        names.append(name)
        return False

    monkeypatch.setattr(worker, "_relation_exists", relation_exists)
    assert worker._pointer_build(conn, POINTER) is None
    assert names == [f"{worker.PRODUCT}_builds"]
    assert conn.statements == []


def test_pointer_build_reports_a_missing_build_row(monkeypatch):
    conn = _FakeConnection()
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    assert worker._pointer_build(conn, POINTER) is None
    assert len(conn.statements) == 1


@pytest.mark.parametrize("serves_panel", [True, False])
def test_mirror_serves_panel_probes_publication_rows_at_or_after_the_closed_month(serves_panel):
    conn = _FakeConnection((serves_panel,))
    assert worker._mirror_serves_panel(conn, parent=PARENT) is serves_panel
    assert conn.statements == [(
        (f"SELECT EXISTS (SELECT 1 FROM {worker.SNAPSHOT_MATVIEW} "
         "WHERE month >= %s AND publication_id = %s)"),
        (PARENT["last_closed_month"], PARENT["publication_id"]),
    )]


def test_already_current_with_no_pointer_does_not_query():
    conn = _FakeConnection()
    assert worker._already_current(conn, parent=PARENT, revision=REVISION, pointer=None) is None
    assert conn.statements == []


def test_already_current_returns_only_a_matching_validated_pointer(monkeypatch, caplog):
    build = _matching_build()
    monkeypatch.setattr(worker, "_pointer_build", lambda _conn, pointer: build)
    assert worker._already_current(object(), parent=PARENT, revision=REVISION, pointer=POINTER) == POINTER
    with caplog.at_level(logging.INFO, logger=worker.__name__):
        assert worker._already_current(
            object(), parent=PARENT, revision="next-revision", pointer=POINTER
        ) is None
    assert f"bond_market_implied_rating_v1 pointer {POINTER} not current" in caplog.text
    assert "code_revision_changed" in caplog.text


def _patch_worker(monkeypatch, *, build=None, pointer=POINTER, mirror=True, defaults=1):
    conn = _FakeConnection(None if build is None else (
        build["publication_id"], build["panel_publication_id"], build["policy_digest"],
        build["code_revision"], build["panel_last_closed_month"], build["lifecycle_state"],
        build["input_fingerprint"],
    ))
    events = []
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "resolve_dsn", lambda _dsn: "postgresql://example")
    monkeypatch.setattr(worker, "_code_revision", lambda: REVISION)
    monkeypatch.delenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", raising=False)
    monkeypatch.delenv("BOND_IMPLIED_RATING_ENABLED", raising=False)
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    monkeypatch.setattr(worker, "_current_panel", lambda _conn: dict(PARENT))
    monkeypatch.setattr(worker, "_current_pointer", lambda _conn: pointer)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: mirror)
    monkeypatch.setattr(worker, "install_schema", lambda _conn: events.append("install_schema"))
    monkeypatch.setattr(worker, "current_pinned_anchor", lambda _conn: None)

    def read_snapshot(_conn, **kwargs):
        events.append("read_snapshot")
        return _snapshot()

    monkeypatch.setattr(worker, "_read_snapshot", read_snapshot)

    def build_payload(_conn, *, parent, revision, started, snapshot_inputs=None):
        events.append("build_payload")
        publication = worker.ImpliedRatingPublication(
            publication_id="rebuilt-publication", panel_publication_id=parent["publication_id"],
            policy_version=worker.policy.POLICY_VERSION, policy_digest=worker.policy.POLICY_DIGEST,
            code_revision=revision, panel_last_closed_month=parent["last_closed_month"],
            first_month=parent["first_month"], last_month=parent["last_closed_month"],
            input_fingerprint=(
                snapshot_inputs["input_fingerprint"] if snapshot_inputs is not None else "a" * 64
            ), l_anchor=5.0, rows_digest="b" * 64,
            d_confirmed_count=defaults, d_candidate_count=0, row_count=2,
        )
        return {
            "publication": publication, "rows": pd.DataFrame(),
            "input_fingerprint": publication.input_fingerprint, "rows_digest": publication.rows_digest,
            "d_confirmed_count": defaults, "d_candidate_count": 0,
            "l_anchor": publication.l_anchor, "bucket_counts": {"D": defaults},
            "last_closed_month": publication.panel_last_closed_month,
        }

    def materialize(_conn, publication, rows, *, expected_pointer):
        events.append(("materialize", expected_pointer))
        return SimpleNamespace(publication_id=publication.publication_id, row_count=2, reused=False)

    monkeypatch.setattr(worker, "_build_payload", build_payload)
    monkeypatch.setattr(worker, "materialize", materialize)
    return conn, events


def test_run_current_reports_the_runtime_revision_without_reading_or_writing(monkeypatch):
    conn, events = _patch_worker(monkeypatch, build=_matching_build())
    result = worker.run("postgresql://example")
    assert result["state"] == "current"
    assert result["publication_id"] == POINTER
    assert result["code_revision"] == REVISION
    assert events == []
    assert len(conn.statements) == 1


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [("code_revision", "previous-revision", "code_revision_changed"),
     ("panel_publication_id", "panel-previous", "panel_publication_changed")],
)
def test_run_rebuilds_changed_identity_and_reports_reasons(monkeypatch, field, value, reason):
    conn, events = _patch_worker(monkeypatch, build={**_matching_build(), field: value})
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert result["rebuild_reasons"] == [reason]
    assert result["panel_publication_id"] == PARENT["publication_id"]
    assert result["code_revision"] == REVISION
    expected_events = ["read_snapshot", "install_schema", "build_payload", ("materialize", POINTER)]
    assert events == expected_events
    assert len(conn.statements) == (2 if field == "panel_publication_id" else 1)


@pytest.mark.parametrize("defaults", [0, 1])
def test_run_first_publication_has_no_rebuild_reasons(monkeypatch, defaults):
    conn, events = _patch_worker(monkeypatch, pointer=None, defaults=defaults)
    result = worker.run("postgresql://example")
    assert result["state"] == ("published_no_defaults" if defaults == 0 else "published")
    assert result["rebuild_reasons"] == []
    assert events[-1] == ("materialize", None)
    assert conn.statements == []


def test_run_rebuilds_an_absent_pointer_build(monkeypatch):
    _patch_worker(monkeypatch)
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert result["rebuild_reasons"] == ["pointer_build_absent"]


def test_run_force_republish_bypasses_current_identity_but_preserves_cas(monkeypatch):
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    monkeypatch.setenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", "1")
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert result["rebuild_reasons"] == []
    assert events[-1] == ("materialize", POINTER)


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
def test_worker_refuses_a_stale_panel_mirror_before_currentness_or_ddl(monkeypatch, entrypoint):
    conn, events = _patch_worker(monkeypatch, build=_matching_build(), mirror=False)
    result = entrypoint("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["reason"] == "implied_rating_gate_failed"
    assert result["input_reasons"] == ["snapshot_mirror_stale"]
    assert result["panel_publication_id"] == PARENT["publication_id"]
    assert result["aborted"] is True
    assert conn.statements == []
    assert events == []


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
def test_panel_only_change_with_identical_inputs_is_current_without_building(monkeypatch, entrypoint):
    fingerprint = worker.policy.snapshot_fingerprint(_snapshot())
    conn, events = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous",
        "input_fingerprint": fingerprint,
    })
    monkeypatch.setattr(
        worker.policy, "build_publication_rows", lambda *args, **kwargs: pytest.fail("state machine")
    )
    monkeypatch.setattr(worker, "_build_payload", lambda *args, **kwargs: pytest.fail("build payload"))
    monkeypatch.setattr(worker, "install_schema", lambda _conn: pytest.fail("DDL"))
    monkeypatch.setattr(worker, "materialize", lambda *args, **kwargs: pytest.fail("materialize"))
    for _ in range(2):
        result = entrypoint("postgresql://example")
        assert result["state"] == "current"
        assert result["reason"] == "panel_inputs_unchanged"
        assert result["publication_id"] == POINTER
        assert result["panel_publication_id"] == PARENT["publication_id"]
        assert result["build_panel_publication_id"] == "panel-previous"
        assert result["input_fingerprint"] == fingerprint
        assert result["code_revision"] == REVISION
        assert result["policy_digest"] == worker.policy.POLICY_DIGEST
        assert result["aborted"] is False
        assert not result.get("rebuild_reasons")
    assert events == ["read_snapshot", "read_snapshot"]
    assert len(conn.statements) == 4


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
def test_panel_only_change_with_different_inputs_reuses_the_read_and_fingerprint(monkeypatch, entrypoint):
    real_build = worker._build_payload
    real_fingerprint = worker.policy.snapshot_fingerprint
    real_rows = worker.policy.build_publication_rows
    _, events = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous",
    })
    calls = []

    def fingerprint(snapshot):
        calls.append(("fingerprint", snapshot))
        return real_fingerprint(snapshot)

    def rows(snapshot, **kwargs):
        calls.append(("state_machine", snapshot))
        return real_rows(snapshot, **kwargs)

    monkeypatch.setattr(worker, "_build_payload", real_build)
    monkeypatch.setattr(worker.policy, "snapshot_fingerprint", fingerprint)
    monkeypatch.setattr(worker.policy, "market_anchor_for_snapshot", lambda *args, **kwargs: 5.0)
    monkeypatch.setattr(worker.policy, "build_publication_rows", rows)
    result = entrypoint("postgresql://example")
    assert result["state"] == ("published_no_defaults" if entrypoint is worker.run else "planned")
    if entrypoint is worker.run:
        assert result["rebuild_reasons"] == ["panel_publication_changed"]
    assert result["input_fingerprint"] == real_fingerprint(_snapshot())
    assert events.count("read_snapshot") == 1
    assert [name for name, _ in calls] == ["fingerprint", "state_machine"]
    assert calls[0][1] is calls[1][1]


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
def test_panel_and_revision_change_cannot_use_the_fingerprint_shortcut(monkeypatch, entrypoint):
    _, events = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous",
        "code_revision": "previous-revision",
        "input_fingerprint": worker.policy.snapshot_fingerprint(_snapshot()),
    })
    monkeypatch.setattr(
        worker, "_panel_inputs_current", lambda *args, **kwargs: pytest.fail("fingerprint shortcut")
    )
    result = entrypoint("postgresql://example")
    assert result["state"] == ("published" if entrypoint is worker.run else "planned")
    if entrypoint is worker.run:
        assert result["rebuild_reasons"] == ["panel_publication_changed", "code_revision_changed"]
    assert "build_payload" in events
    assert events.count("read_snapshot") == 1


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
def test_force_republish_bypasses_panel_input_convergence(monkeypatch, entrypoint):
    _, events = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous",
        "input_fingerprint": worker.policy.snapshot_fingerprint(_snapshot()),
    })
    monkeypatch.setenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", "1")
    monkeypatch.setattr(
        worker, "_panel_inputs_current", lambda *args, **kwargs: pytest.fail("fingerprint shortcut")
    )
    result = entrypoint("postgresql://example")
    assert result["state"] == ("published" if entrypoint is worker.run else "planned")
    if entrypoint is worker.run:
        assert result["rebuild_reasons"] == ["panel_publication_changed"]
        assert events[-1] == ("materialize", POINTER)
    assert "build_payload" in events
    assert events.count("read_snapshot") == 1


def test_reused_publication_reports_its_persisted_build_parent(monkeypatch):
    conn, _ = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous",
    })
    monkeypatch.setenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", "1")
    monkeypatch.setattr(worker, "materialize", lambda *args, **kwargs: SimpleNamespace(
        publication_id="rebuilt-publication", row_count=2, reused=True,
    ))
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert result["reused_publication"] is True
    assert result["panel_publication_id"] == "panel-previous"
    assert conn.statements[-1][1] == (result["publication_id"],)


@pytest.mark.parametrize("snapshot_error", [False, True], ids=["empty", "unreadable"])
def test_panel_input_convergence_preserves_snapshot_refusals(monkeypatch, snapshot_error):
    _, events = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous",
    })

    def read_snapshot(*args, **kwargs):
        if snapshot_error:
            raise worker.psycopg.Error("unreadable")
        return pd.DataFrame()

    monkeypatch.setattr(worker, "_read_snapshot", read_snapshot)
    result = worker.run("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == (
        ["snapshot_unreadable:Error"] if snapshot_error else ["snapshot_empty"]
    )
    assert events == []


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
@pytest.mark.parametrize("convergence", [False, True], ids=["rebuild", "convergence"])
@pytest.mark.parametrize("moved", ["publication", "window", "same_id_window", "absent"])
def test_panel_pointer_moved_during_snapshot_read_refuses_before_building(
    monkeypatch, entrypoint, convergence, moved
):
    build = {
        **_matching_build(), "panel_publication_id": "panel-previous",
        "input_fingerprint": worker.policy.snapshot_fingerprint(_snapshot()),
    } if convergence else None
    _, events = _patch_worker(monkeypatch, build=build, pointer=POINTER if convergence else None)
    current = dict(PARENT)
    probes = []

    def read_snapshot(_conn, **kwargs):
        nonlocal current
        events.append("read_snapshot")
        if moved == "absent":
            current = None
        else:
            current = dict(PARENT)
            if moved != "same_id_window":
                current["publication_id"] = "panel-next"
            if moved != "publication":
                current["last_closed_month"] = date(2026, 9, 1)
        return _snapshot()

    monkeypatch.setattr(worker, "_current_panel", lambda _conn: current)
    monkeypatch.setattr(worker, "_read_snapshot", read_snapshot)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: probes.append(kwargs) or True)
    monkeypatch.setattr(worker, "install_schema", lambda _conn: pytest.fail("DDL"))
    monkeypatch.setattr(worker, "_build_payload", lambda *args, **kwargs: pytest.fail("build payload"))
    monkeypatch.setattr(worker, "materialize", lambda *args, **kwargs: pytest.fail("materialize"))
    monkeypatch.setattr(
        worker.policy, "snapshot_fingerprint", lambda *args, **kwargs: pytest.fail("fingerprint decision")
    )
    monkeypatch.setattr(
        worker.policy, "build_publication_rows", lambda *args, **kwargs: pytest.fail("state machine")
    )
    result = entrypoint("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["reason"] == "implied_rating_gate_failed"
    assert result["aborted"] is True
    assert result["input_reasons"] == ["panel_pointer_moved"]
    assert result["panel_publication_id"] == PARENT["publication_id"]
    assert result["current_panel_publication_id"] == (
        current["publication_id"] if current is not None else None
    )
    assert events == ["read_snapshot"]
    assert len(probes) == 1


@pytest.mark.parametrize("entrypoint", [worker.run, worker.plan], ids=["run", "plan"])
@pytest.mark.parametrize("convergence", [False, True], ids=["rebuild", "convergence"])
def test_stable_panel_pointer_preserves_rebuild_and_convergence(monkeypatch, entrypoint, convergence):
    build = {
        **_matching_build(), "panel_publication_id": "panel-previous",
        "input_fingerprint": worker.policy.snapshot_fingerprint(_snapshot()),
    } if convergence else None
    _, events = _patch_worker(monkeypatch, build=build, pointer=POINTER if convergence else None)
    sequence = []
    read_snapshot = worker._read_snapshot

    def read(_conn, **kwargs):
        sequence.append("snapshot")
        return read_snapshot(_conn, **kwargs)

    monkeypatch.setattr(worker, "_current_panel", lambda _conn: sequence.append("panel") or dict(PARENT))
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: sequence.append("mirror") or True)
    monkeypatch.setattr(worker, "_read_snapshot", read)
    result = entrypoint("postgresql://example")
    assert result["state"] == (
        "current" if convergence else "published" if entrypoint is worker.run else "planned"
    )
    if convergence:
        assert result["reason"] == "panel_inputs_unchanged"
    assert result["panel_publication_id"] == PARENT["publication_id"]
    assert sequence == ["panel", "mirror", "snapshot", "panel"]
    assert events.count("read_snapshot") == 1
