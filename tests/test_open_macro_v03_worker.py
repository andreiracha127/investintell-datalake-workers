"""Unit tests for the open_macro_v03 Stage B runtime worker.

No real Postgres: the gate short-circuits (flag_off / governance_blocked / pin
mismatch) need no DB, and the DB-touching helpers (publish, staleness ledger,
invalidate, staleness-block run path) are driven through a duck-typed fake conn.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

import src.workers.open_macro_v03 as w


@pytest.fixture(autouse=True)
def _approved_writer_identity(monkeypatch):
    # the WRITER gate requires the runtime to present the approved (platform-neutral)
    # writer identity; default it so tests exercising an ACTIVE envelope pass (dedicated
    # tests clear it). Every other identity source is cleared so a stray env var on the
    # developer/CI machine cannot decide the gate.
    for name in w.WRITER_IDENTITY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("WORKER_SERVICE_IDENTITY", "open-macro-v03-worker")


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self.rowcount = -1
        self._rows: list = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self.conn.executed.append((sql, params))
        resp = self.conn.responder(sql, params)
        self.rowcount = resp.get("rowcount", 1)
        self._rows = resp.get("rows", [])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, responder=None) -> None:
        self.executed: list = []
        self.commits = 0
        self.closed = False
        self.responder = responder or (lambda sql, params: {})

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def _lock_responder(inner=None):
    """Wrap a responder so advisory-lock acquire/release + search_path pin work."""
    def responder(sql, params):
        if "pg_try_advisory_lock" in sql:
            return {"rows": [(True,)]}
        if "pg_advisory_unlock" in sql:
            return {"rows": [(1,)]}
        if "SET search_path" in sql:
            return {"rows": []}
        if "SHOW search_path" in sql:
            return {"rows": [("public",)]}
        return (inner or (lambda s, p: {}))(sql, params)
    return responder


@dataclass
class _FakeDecision:
    as_of: _dt.date
    quadrant: str | None
    status: str = "valid"
    candidate_confidence: float | None = 0.5
    coverage_quality: float = 0.9
    growth_score: float | None = 0.1
    inflation_score: float | None = -0.2

    def has_valid_quadrant(self) -> bool:
        return self.status == "valid" and self.quadrant is not None


# --------------------------------------------------------------------------- #
# Gate 1/2/3 short-circuits (no DB)
# --------------------------------------------------------------------------- #
def test_flag_off_is_inert_without_db(monkeypatch):
    monkeypatch.delenv("open_macro_v03_runtime_activation", raising=False)

    def _no_connect(*a, **k):
        raise AssertionError("must not connect when the flag is off")

    monkeypatch.setattr(w, "connect", _no_connect)
    assert w.run("unused-dsn") == {"status": "flag_off"}


def test_committed_active_envelope_passes_governance_wrong_service_blocks(monkeypatch):
    """B4 flipped: the COMMITTED envelope passes check_governance. The next key is
    the WRITER runtime identity (Gate 2b): without the approved writer identity the
    run stops wrong_service BEFORE any pins/pack/DB work - the feature flag env var
    stays the second key and the writer identity the third."""
    committed = w._load_json(w.ENVELOPE_PATH)
    assert w.check_governance(committed) is None

    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    for name in w.WRITER_IDENTITY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    def _no_connect(*a, **k):
        raise AssertionError("must not connect from an unapproved service")

    monkeypatch.setattr(w, "connect", _no_connect)
    result = w.run("unused-dsn")
    assert result["status"] == "wrong_service"


def test_blocked_builder_envelope_still_blocks_without_db(monkeypatch):
    """The builder's BLOCKED base (the deploy-ahead state every service carried
    before this flip) must still short-circuit governance_blocked with no DB."""
    from harness.direct_activation.build_stage_b_artifacts import (
        build_activation_envelope)
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    blocked = build_activation_envelope()
    real_load = w._load_json
    monkeypatch.setattr(
        w, "_load_json",
        lambda path: blocked if Path(path) == Path(w.ENVELOPE_PATH) else real_load(path))

    def _no_connect(*a, **k):
        raise AssertionError("must not connect while governance is blocked")

    monkeypatch.setattr(w, "connect", _no_connect)
    result = w.run("unused-dsn")
    assert result["status"] == "governance_blocked"


def _complete_matrix() -> dict:
    return {role: {"owner": "Andrei Rachadel", "approval_status": "approved",
                   "approval_evidence": "signed", "timestamp": "2026-07-06T00:00:00Z",
                   "blocking": True}
            for role in w.APPROVAL_ROLES}


def _active_envelope() -> dict:
    return {
        **w.ENVELOPE_IDENTITY,
        "runtime_activation": True, "activation_allowed": True, "allow_db_write": True,
        "db_write_official": True, "db_write_mode": "open_macro_v03_new_tables_only",
        "allocator_publish": True, "allow_allocator_publish": True,
        "official_result": True, "A5": "active",
        "freeze_ready": False, "production_endpoint_activation": "none",
        "allowed_tables": sorted(w.ALLOWED_TABLES),
        "environment": {"railway_service_name": "open-macro-v03-worker"},
        "approval_matrix": _complete_matrix(),
        "approval_matrix_complete": True,
    }


def test_check_governance_all_gates(monkeypatch):
    assert w.check_governance(_active_envelope()) is None
    # each single missing gate blocks
    for key, bad in [
        ("runtime_activation", False), ("activation_allowed", False),
        ("allow_db_write", False), ("db_write_official", False),
        ("db_write_mode", "none"), ("allocator_publish", False),
        ("allow_allocator_publish", False), ("official_result", False),
        ("A5", "blocked"), ("freeze_ready", True),
        ("production_endpoint_activation", "exposed"),
    ]:
        env = _active_envelope()
        env[key] = bad
        assert w.check_governance(env) is not None, key
    # a Railway service other than the approved one blocks
    env = _active_envelope()
    env["environment"] = {"railway_service_name": "staging-worker"}
    assert w.check_governance(env) is not None
    # string 'true' must not spoof a boolean flip
    env = _active_envelope()
    env["runtime_activation"] = "true"
    assert w.check_governance(env) == "runtime_activation!=true"
    # allowed_tables must be exactly the three
    env = _active_envelope()
    env["allowed_tables"] = ["open_macro_v03_decisions"]
    assert w.check_governance(env) is not None
    # environment must name the service
    env = _active_envelope()
    env["environment"] = None
    assert w.check_governance(env) is not None
    # a wrong/stale envelope identity is rejected before any activation boolean
    for key in w.ENVELOPE_IDENTITY:
        env = _active_envelope()
        env[key] = "WRONG"
        reason = w.check_governance(env)
        assert reason is not None and "envelope identity" in reason, key


def test_check_writer_runtime_requires_the_approved_identity(monkeypatch):
    # WRITER-only gate: the runtime must present the approved writer identity. An
    # absent identity (local/misconfigured runner) or a different one blocks.
    def _only(name, value):
        for env in w.WRITER_IDENTITY_ENV_VARS:
            monkeypatch.delenv(env, raising=False)
        if name is not None:
            monkeypatch.setenv(name, value)

    _only("WORKER_SERVICE_IDENTITY", "open-macro-v03-worker")
    assert w.check_writer_runtime() is None
    _only(None, "")
    reason = w.check_writer_runtime()
    assert reason is not None and "absent" in reason
    _only("WORKER_SERVICE_IDENTITY", "open-macro-v03-monitor")
    assert w.check_writer_runtime() is not None
    # ...and this is NOT part of check_governance, so the monitor (a separate workload)
    # can share the governance predicate and still pass.
    assert w.check_governance(_active_envelope()) is None


def test_writer_identity_is_platform_neutral(monkeypatch):
    """The gate is the LOGICAL writer identity, not a platform hostname: Cloud Run
    (job or service), Railway and an explicit declaration all satisfy it, and the
    explicit declaration WINS so a Cloud Run job whose own name differs can still
    present the approved identity."""
    def _env(**values):
        for env in w.WRITER_IDENTITY_ENV_VARS:
            monkeypatch.delenv(env, raising=False)
        for key, value in values.items():
            monkeypatch.setenv(key, value)

    for source in ("WORKER_SERVICE_IDENTITY", "CLOUD_RUN_JOB", "K_SERVICE",
                   "RAILWAY_SERVICE_NAME"):
        _env(**{source: "open-macro-v03-worker"})
        assert w.check_writer_runtime() is None, source
        assert w.resolve_writer_identity() == ("open-macro-v03-worker", source)

    # a Cloud Run job named dl-open-macro-v03 declares the logical identity explicitly
    _env(CLOUD_RUN_JOB="dl-open-macro-v03",
         WORKER_SERVICE_IDENTITY="open-macro-v03-worker")
    assert w.check_writer_runtime() is None
    # ...and without that declaration the platform's own name is NOT the approved
    # identity: fail-closed, with the platform name named in the reason.
    _env(CLOUD_RUN_JOB="dl-open-macro-v03")
    reason = w.check_writer_runtime()
    assert reason is not None and "dl-open-macro-v03" in reason and "CLOUD_RUN_JOB" in reason
    # whitespace-only is not an identity
    _env(WORKER_SERVICE_IDENTITY="   ", K_SERVICE="open-macro-v03-worker")
    assert w.resolve_writer_identity() == ("open-macro-v03-worker", "K_SERVICE")


def test_governance_accepts_either_envelope_identity_key():
    """The envelope declares the LOGICAL writer identity. The ratified artifact spells
    it `railway_service_name`; the platform-neutral key `writer_identity` is accepted
    too, and either way the value must be the ONE approved identity."""
    env = _active_envelope()
    env["environment"] = {"writer_identity": w.APPROVED_WRITER_IDENTITY}
    assert w.check_governance(env) is None
    env["environment"] = {"railway_service_name": w.APPROVED_WRITER_IDENTITY}
    assert w.check_governance(env) is None
    env["environment"] = {"writer_identity": "staging-worker"}
    assert w.check_governance(env) is not None
    # the committed Stage B artifact still validates unchanged
    assert w.check_governance(w._load_json(w.ENVELOPE_PATH)) is None
    assert w.APPROVED_RAILWAY_SERVICE == w.APPROVED_WRITER_IDENTITY


def test_check_governance_requires_real_per_role_approvals():
    # a named owner is NOT a sign-off: pending status or missing evidence/timestamp on
    # any single role must block, even with approval_matrix_complete=true.
    role = next(iter(w.APPROVAL_ROLES))
    for bad in ({"approval_status": "pending"}, {"approval_evidence": None},
                {"approval_evidence": ""}, {"timestamp": None}):
        env = _active_envelope()
        env["approval_matrix"][role] = {**env["approval_matrix"][role], **bad}
        reason = w.check_governance(env)
        assert reason is not None and role in reason, bad


def test_check_governance_requires_the_approval_matrix():
    # active envelope WITHOUT a matrix at all
    env = _active_envelope()
    del env["approval_matrix"]
    assert "approval_matrix" in w.check_governance(env)
    # incomplete matrix: only five roles
    env = _active_envelope()
    del env["approval_matrix"]["final_approver"]
    assert "approval_matrix" in w.check_governance(env)
    # unrecognized role in place of technical_owner ("engineering" never counts)
    env = _active_envelope()
    env["approval_matrix"]["engineering"] = env["approval_matrix"].pop("technical_owner")
    assert "approval_matrix" in w.check_governance(env)
    # a role without a named holder blocks
    env = _active_envelope()
    env["approval_matrix"]["risk_owner"]["owner"] = None
    assert "approval_matrix.risk_owner" in w.check_governance(env)
    env = _active_envelope()
    env["approval_matrix"]["quant_owner"]["owner"] = "   "
    assert "approval_matrix.quant_owner" in w.check_governance(env)
    # strict-bool completeness: string "true" never passes
    env = _active_envelope()
    env["approval_matrix_complete"] = "true"
    assert w.check_governance(env) == "approval_matrix_complete!=true"
    # complete matrix passes
    assert w.check_governance(_active_envelope()) is None


def test_blocked_builder_envelope_is_blocked_by_the_matrix_gate_itself():
    """Flip every boolean/scope gate of the builder's BLOCKED envelope but keep its
    pending matrix: the approval-matrix gate must still block (not an accident of
    the earlier boolean gates). Uses the builder base because the COMMITTED envelope
    now carries the ratified, complete matrix."""
    from harness.direct_activation.build_stage_b_artifacts import (
        build_activation_envelope)
    forged = dict(build_activation_envelope())
    forged.update({
        "runtime_activation": True, "activation_allowed": True, "allow_db_write": True,
        "db_write_official": True, "db_write_mode": "open_macro_v03_new_tables_only",
        "allocator_publish": True, "allow_allocator_publish": True,
        "official_result": True, "A5": "active",
        "allowed_tables": sorted(w.ALLOWED_TABLES),
        "environment": {"railway_service_name": "open-macro-v03-worker"},
    })
    reason = w.check_governance(forged)
    assert reason is not None
    assert "approval_matrix" in reason  # pending owners (None) fail the holder gate


def test_worker_json_loader_is_strict(tmp_path):
    dup = tmp_path / "dup.json"
    dup.write_text('{"runtime_activation": true, "runtime_activation": false}',
                   encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate JSON key 'runtime_activation'"):
        w._load_json(dup)
    nan = tmp_path / "nan.json"
    nan.write_text('{"value": NaN}', encoding="utf-8")
    with pytest.raises(ValueError, match="non-finite JSON constant"):
        w._load_json(nan)
    overflow = tmp_path / "overflow.json"
    overflow.write_text('{"value": 1e9999}', encoding="utf-8")
    with pytest.raises(ValueError, match="non-finite JSON number"):
        w._load_json(overflow)


def test_active_envelope_pin_mismatch_raises_before_db(tmp_path, monkeypatch):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    env_path = tmp_path / "activation_envelope.json"
    env_path.write_text(json.dumps(_active_envelope()), encoding="utf-8")
    pins_path = tmp_path / "module_pins.json"
    pins_path.write_text(json.dumps({
        "modules": {"src/quadrant_score.py": "0" * 64},  # truncated + wrong
        "pack": {}, "module_pins_sha256": "deadbeef",
    }), encoding="utf-8")
    monkeypatch.setattr(w, "ENVELOPE_PATH", env_path)
    monkeypatch.setattr(w, "PINS_PATH", pins_path)

    def _no_connect(*a, **k):
        raise AssertionError("must not connect on a pin mismatch")

    monkeypatch.setattr(w, "connect", _no_connect)
    # a truncated manifest is rejected by the set-completeness gate before any DB
    with pytest.raises(w.OpenMacroV03Error, match="module pin set diverges"):
        w.run("unused-dsn")


def test_verify_module_pins_rejects_truncated_or_altered_manifest():
    pins = w._load_json(w.PINS_PATH)
    # dropping any pinned module trips the set-completeness gate (P1: a truncated
    # module_pins.json must not pass by iterating only the keys it contains)
    truncated = {**pins, "modules": {k: v for k, v in list(pins["modules"].items())[1:]}}
    with pytest.raises(w.OpenMacroV03Error, match="module pin set diverges"):
        w.verify_module_pins(truncated, w.ROOT)
    # a doctored module_pins_sha256 (block altered) is rejected by the recompute
    altered = {**pins, "module_pins_sha256": "0" * 64}
    with pytest.raises(w.OpenMacroV03Error, match="module_pins_sha256"):
        w.verify_module_pins(altered, w.ROOT)


def test_verify_module_pins_accepts_the_committed_pins():
    pins = json.loads(w.PINS_PATH.read_text(encoding="utf-8"))
    w.verify_module_pins(pins, w.ROOT)  # must not raise


# --------------------------------------------------------------------------- #
# Gate 6 — as_of resolution
# --------------------------------------------------------------------------- #
def test_resolve_as_of_business_day_and_weekend():
    assert w.resolve_as_of(today=_dt.date(2026, 7, 6)) == _dt.date(2026, 7, 6)  # Mon
    assert w.resolve_as_of(today=_dt.date(2026, 7, 4)) is None                  # Sat
    assert w.resolve_as_of(today=_dt.date(2026, 7, 5)) is None                  # Sun
    assert w.resolve_as_of("2026-06-30") == _dt.date(2026, 6, 30)               # explicit


def test_resolve_as_of_rejects_future_arg_override():
    # a future arg override (> current NY day) is refused before any publish
    with pytest.raises(w.OpenMacroV03Error, match="future"):
        w.resolve_as_of("2026-07-07", today=_dt.date(2026, 7, 6))
    # current-or-past BUSINESS-day overrides stay trusted
    assert w.resolve_as_of("2026-07-06", today=_dt.date(2026, 7, 6)) == _dt.date(2026, 7, 6)
    assert w.resolve_as_of("2026-07-03", today=_dt.date(2026, 7, 6)) == _dt.date(2026, 7, 3)  # Fri


def test_resolve_as_of_rejects_future_env_override(monkeypatch):
    monkeypatch.setenv("OPEN_MACRO_V03_AS_OF", "2026-07-10")
    with pytest.raises(w.OpenMacroV03Error, match="future"):
        w.resolve_as_of(today=_dt.date(2026, 7, 6))


def test_resolve_as_of_skips_weekend_override():
    # a weekend override is a non-business day -> None (never publishes an official row),
    # exactly like the auto path; the monitor also exits non_business_day on weekends.
    assert w.resolve_as_of("2026-07-04", today=_dt.date(2026, 7, 6)) is None  # Saturday
    assert w.resolve_as_of("2026-07-05", today=_dt.date(2026, 7, 6)) is None  # Sunday


def test_resolve_as_of_rejects_pre_cut_override():
    # an override before PACK_CUT (2026-06-30) would be evaluated with pack data from the
    # future of that as_of (compose_inputs always loads through the cut) -> reject
    with pytest.raises(w.OpenMacroV03Error, match="before the pack cut"):
        w.resolve_as_of("2026-06-26", today=_dt.date(2026, 7, 6))


# --------------------------------------------------------------------------- #
# Gate 7 — prefix hash gate
# --------------------------------------------------------------------------- #
def test_prefix_hash_matches_pack_pin():
    pins = w.prefix_pins()
    mv = json.loads((w.PACK / "data" / "canonical" / "macro_observation_vintage.json")
                    .read_text(encoding="utf-8"))
    ep = json.loads((w.PACK / "data" / "canonical" / "eod_prices.json")
                    .read_text(encoding="utf-8"))
    w.verify_prefix_hashes(mv, ep, pins)  # must not raise


def test_prefix_hash_mismatch_raises():
    pins = w.prefix_pins()
    mv = json.loads((w.PACK / "data" / "canonical" / "macro_observation_vintage.json")
                    .read_text(encoding="utf-8"))
    tampered = list(mv)
    tampered[0] = {**tampered[0], "value": tampered[0]["value"] + 1}
    with pytest.raises(w.OpenMacroV03Error, match="prefix hash"):
        w.verify_prefix_hashes(tampered, [], pins)


# --------------------------------------------------------------------------- #
# Gate 3b — pack v2 REAL bytes
# --------------------------------------------------------------------------- #
def test_verify_pack_bytes_accepts_the_committed_pack():
    w.verify_pack_bytes()  # SOURCE pins + recomputed aggregate must all hold


def test_verify_pack_bytes_mutated_byte_raises_before_db(tmp_path, monkeypatch):
    """A single flipped byte in a canonical data file must abort — and abort BEFORE
    any DB connection is even attempted in run()."""
    import shutil
    forged = tmp_path / "pack"
    shutil.copytree(w.PACK, forged)
    target = forged / "data" / "canonical" / "eod_prices.json"
    data = bytearray(target.read_bytes())
    idx = data.index(b'"close": ') + len(b'"close": ')
    data[idx] = data[idx] ^ 0x01  # flip one digit byte in a real value
    target.write_bytes(bytes(data))

    with pytest.raises(w.OpenMacroV03Error, match="pack byte verification failed"):
        w.verify_pack_bytes(forged)

    # run() calls verify_pack_bytes before connecting: a pack failure must never
    # reach the DB.
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "verify_pack_bytes",
                        lambda *a, **k: (_ for _ in ()).throw(
                            w.OpenMacroV03Error("pack byte verification failed: forged")))

    def _no_connect(*a, **k):
        raise AssertionError("must not connect after a pack-bytes failure")

    monkeypatch.setattr(w, "connect", _no_connect)
    with pytest.raises(w.OpenMacroV03Error, match="pack byte verification failed"):
        w.run("unused-dsn")


# --------------------------------------------------------------------------- #
# Ledger × output mutual exclusion + immutable ledger
# --------------------------------------------------------------------------- #
def _blocked_day_responder(*, resolution=None, superseded=False, extra=None):
    """Fake-conn responder for a day that CARRIES a staleness block: the block detail
    lookup answers, the resolution lookup answers what the test asks for."""
    def responder(sql, params):
        if "FROM open_macro_v03_staleness_blocks WHERE as_of" in sql:
            return {"rows": [("block-run-id", "1" * 64, "2" * 64)]}
        if "resolution_state = 'resolved'" in sql:
            return {"rows": [resolution] if resolution is not None else []}
        if "resolution_state = 'superseded'" in sql:
            return {"rows": [(1,)] if superseded else []}
        if extra is not None:
            return extra(sql, params)
        return {"rowcount": 1}
    return responder


def test_publish_refuses_when_ledger_row_exists():
    conn = _FakeConn(_blocked_day_responder())
    with pytest.raises(w.OpenMacroV03Error, match="publish refused.*staleness-block"):
        w.publish(conn, _decision_row(), _allocation_row())
    assert conn.commits == 0
    dml = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_decisions" not in dml
    # the message must NAME the sanctioned recovery path, not just refuse
    with pytest.raises(w.OpenMacroV03Error, match="resolve-staleness"):
        w.publish(_FakeConn(_blocked_day_responder()), _decision_row(), _allocation_row())


def _resolution_row():
    return ("11111111-1111-1111-1111-111111111111", "Andrei Rachadel", "sources refreshed",
            _dt.datetime(2026, 7, 17, 12, 0, tzinfo=_dt.timezone.utc))


def _proof():
    return {"verified_as_of": "2026-07-06", "breaches": [], "series": {}, "prices": {},
            "criteria": {}}


def test_publish_on_a_resolved_block_appends_the_superseded_event():
    """A day whose block carries a 'resolved' event publishes — and the publication
    APPENDS the 'superseded' event in the SAME transaction, so the ledger reads
    block -> resolution -> output. The block row itself is never touched."""
    conn = _FakeConn(_blocked_day_responder(resolution=_resolution_row()))
    w.publish(conn, _decision_row(), _allocation_row(), proof=_proof())
    assert conn.commits == 1
    dml = [sql for sql, _ in conn.executed]
    assert any("INSERT INTO open_macro_v03_decisions" in s for s in dml)
    inserts = [(s, p) for s, p in conn.executed
               if f"INSERT INTO {w.RESOLUTIONS_TABLE}" in s]
    assert len(inserts) == 1
    params = inserts[0][1]
    assert params["resolution_state"] == "superseded"
    assert params["resolved_by"] == w.APPROVED_WRITER_IDENTITY
    assert params["block_run_id"] == "block-run-id"
    assert params["run_id"] == _decision_row()["run_id"]
    assert json.loads(params["freshness_proof"])["breaches"] == []
    # nothing UPDATEs or DELETEs the immutable block ledger
    assert not any(s.strip().upper().startswith(("UPDATE", "DELETE")) for s in dml)


def test_publish_supersedes_only_once_per_day():
    """Append-only, not append-repeatedly: an idempotent re-run of an already
    superseded day republishes without stacking a second event."""
    conn = _FakeConn(_blocked_day_responder(resolution=_resolution_row(), superseded=True))
    w.publish(conn, _decision_row(), _allocation_row(), proof=_proof())
    assert not any(f"INSERT INTO {w.RESOLUTIONS_TABLE}" in s for s, _ in conn.executed)


def test_publish_on_a_resolved_block_requires_the_freshness_proof():
    """The supersede event must carry the freshness report of the publishing run;
    publishing over a resolved block without it fails loud instead of writing a
    provenance-free event."""
    conn = _FakeConn(_blocked_day_responder(resolution=_resolution_row()))
    with pytest.raises(w.OpenMacroV03Error, match="proof"):
        w.publish(conn, _decision_row(), _allocation_row())
    assert conn.commits == 0


def test_record_staleness_block_refuses_over_published_output():
    def responder(sql, params):
        if "SELECT 1 FROM open_macro_v03_allocations" in sql:
            return {"rows": [(1,)]}
        return {"rowcount": 1}

    conn = _FakeConn(responder)
    with pytest.raises(w.OpenMacroV03Error, match="staleness-block refused"):
        w.record_staleness_block(conn, {"as_of": _dt.date(2026, 7, 6)})
    assert conn.commits == 0
    dml = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_staleness_blocks" not in dml


def test_record_staleness_block_is_immutable_on_rerun():
    """ON CONFLICT DO NOTHING: a re-run of a still-stale day preserves the first
    record; rowcount 0 is reported as already-recorded, never an error."""
    def responder(sql, params):
        if sql.startswith("INSERT INTO open_macro_v03_staleness_blocks"):
            assert "DO NOTHING" in sql
            return {"rowcount": 0}
        return {}

    conn = _FakeConn(responder)
    inserted = w.record_staleness_block(conn, {
        "as_of": _dt.date(2026, 7, 6), "reason": "r", "stale_detail": "{}",
        "input_vintage_sha256": "a" * 64, "input_prices_sha256": "b" * 64,
        "pack_v2_sha256": "c" * 64, "module_pins_sha256": "d" * 64,
        "code_commit": "e" * 40, "run_id": "rid"})
    assert inserted is False
    assert conn.commits == 1


# --------------------------------------------------------------------------- #
# Catalog verification (verify_schema)
# --------------------------------------------------------------------------- #
def _catalog_responder(*, drop_column: str | None = None,
                       drop_constraint: str | None = None,
                       column_override: dict | None = None,
                       constraint_override: dict | None = None):
    column_override = column_override or {}
    constraint_override = constraint_override or {}

    def responder(sql, params):
        if "information_schema.columns" in sql:
            rows = []
            for table in sorted(w.EXPECTED_SCHEMA):
                for col, meta in w.EXPECTED_SCHEMA[table]["columns"].items():
                    if drop_column and col == drop_column:
                        continue
                    dtype, clen, nullable, default = column_override.get(col, meta)
                    rows.append((table, col, dtype, clen, nullable, default))
            return {"rows": rows}
        if "pg_constraint" in sql:
            rows = []
            for table in sorted(w.EXPECTED_SCHEMA):
                for name, meta in w.EXPECTED_SCHEMA[table]["constraints"].items():
                    if drop_constraint and name == drop_constraint:
                        continue
                    ctype, cdef = constraint_override.get(name, meta)
                    rows.append((table, name, ctype, cdef))
            return {"rows": rows}
        return {}
    return responder


def test_verify_schema_passes_and_returns_the_catalog_view():
    conn = _FakeConn(_catalog_responder())
    verified = w.verify_schema(conn)
    assert set(verified) == set(w.EXPECTED_SCHEMA)
    assert verified["open_macro_v03_decisions"]["columns"]["quadrant"] == ("text", None, "NO", None)
    assert (verified["open_macro_v03_allocations"]["constraints"]
            ["open_macro_v03_allocations_as_of_fkey"]
            == ("f", "FOREIGN KEY (as_of) REFERENCES open_macro_v03_decisions(as_of)"))


def test_expected_schema_carries_the_carry_decay_v1_shape():
    """carry_decay_v1 (phase0q_005, ratified 2026-07-11): EXPECTED_SCHEMA must expect
    the POST-migration catalog — nullable carry-provenance columns, the widened
    decision_validity / book vocabularies ('carried_expired' / 'center_50') and the
    consistency CHECKs — so verify_schema certifies the migrated production catalog
    and fails loud against an unmigrated one (the worker never writes new-shaped rows
    into an old-shaped schema)."""
    dec = w.EXPECTED_SCHEMA["open_macro_v03_decisions"]
    assert dec["columns"]["carry_age_months"] == ("integer", None, "YES", None)
    assert dec["columns"]["carry_expired"] == ("boolean", None, "YES", None)
    validity_def = dec["constraints"]["open_macro_v03_decisions_decision_validity_check"][1]
    assert "'carried_expired'::text" in validity_def
    seed_def = dec["constraints"]["open_macro_v03_decisions_validity_seed"][1]
    assert "'carried_expired'::text" in seed_def and "carry_seed_as_of < as_of" in seed_def
    assert "open_macro_v03_decisions_carry_expired_consistent" in dec["constraints"]

    alloc = w.EXPECTED_SCHEMA["open_macro_v03_allocations"]
    assert alloc["columns"]["carry_age_months"] == ("integer", None, "YES", None)
    assert alloc["columns"]["carry_seed_as_of"] == ("date", None, "YES", None)
    assert alloc["columns"]["carry_expired"] == ("boolean", None, "YES", None)
    book_def = alloc["constraints"]["open_macro_v03_allocations_book_check"][1]
    assert "'compressed_50'::text" in book_def and "'center_50'::text" in book_def
    assert "open_macro_v03_allocations_center_book_consistent" in alloc["constraints"]


def test_verify_schema_raises_on_missing_column():
    conn = _FakeConn(_catalog_responder(drop_column="carry_seed_as_of"))
    with pytest.raises(w.OpenMacroV03Error, match="column set diverges"):
        w.verify_schema(conn)


def test_verify_schema_raises_on_missing_constraint():
    conn = _FakeConn(_catalog_responder(
        drop_constraint="open_macro_v03_allocations_weights_sum"))
    with pytest.raises(w.OpenMacroV03Error, match="weights_sum"):
        w.verify_schema(conn)


def test_verify_schema_raises_on_relaxed_constraint_definition():
    # same name + contype but a drifted definition (CREATE TABLE IF NOT EXISTS never
    # repairs this) must still fail the gate.
    conn = _FakeConn(_catalog_responder(constraint_override={
        "open_macro_v03_allocations_risk_cap": ("c", "CHECK ((risk_assets_weight <= 999))")}))
    with pytest.raises(w.OpenMacroV03Error, match="risk_cap"):
        w.verify_schema(conn)


def test_verify_schema_raises_on_missing_inline_check():
    # an inline auto-named CHECK (e.g. the quadrant enum) dropped from a pre-existing
    # table must fail even though the PK/FK and named custom checks are present.
    conn = _FakeConn(_catalog_responder(
        drop_constraint="open_macro_v03_decisions_quadrant_check"))
    with pytest.raises(w.OpenMacroV03Error, match="quadrant_check"):
        w.verify_schema(conn)


def test_verify_schema_raises_on_unexpected_constraint():
    # an EXTRA CHECK from a manual migration (CREATE TABLE IF NOT EXISTS never removes
    # it) must fail — a later valid publish() could break against it.
    base = _catalog_responder()

    def responder(sql, params):
        result = base(sql, params)
        if "pg_constraint" in sql:
            result["rows"].append(("open_macro_v03_decisions", "extra_manual_check",
                                   "c", "CHECK ((quadrant <> 'z'::text))"))
        return result

    conn = _FakeConn(responder)
    with pytest.raises(w.OpenMacroV03Error, match="unexpected constraints"):
        w.verify_schema(conn)


def test_verify_schema_raises_when_dml_omitted_default_missing():
    # a table missing DEFAULT now() on a DML-omitted NOT NULL column would pass a bare
    # name+type gate and then fail on the first real write.
    conn = _FakeConn(_catalog_responder(column_override={
        "created_at": ("timestamp with time zone", None, "NO", None)}))
    with pytest.raises(w.OpenMacroV03Error, match="created_at: signature"):
        w.verify_schema(conn)


def test_verify_schema_raises_on_char_length_drift():
    # CHAR(64) vs CHAR(40) both report data_type='character'; the length must be checked.
    conn = _FakeConn(_catalog_responder(column_override={
        "code_commit": ("character", 64, "NO", None)}))
    with pytest.raises(w.OpenMacroV03Error, match="code_commit: signature"):
        w.verify_schema(conn)


def test_verify_schema_raises_on_nullability_drift():
    conn = _FakeConn(_catalog_responder(column_override={
        "input_prices_sha256": ("character", 64, "YES", None)}))
    with pytest.raises(w.OpenMacroV03Error, match="input_prices_sha256: signature"):
        w.verify_schema(conn)


# --------------------------------------------------------------------------- #
# search_path pin (public, before any DDL/table access)
# --------------------------------------------------------------------------- #
def test_pin_search_path_forces_public_and_verifies():
    conn = _FakeConn(_lock_responder())
    w.pin_search_path(conn)
    executed = " ".join(sql for sql, _ in conn.executed)
    assert "SET search_path TO public" in executed
    assert conn.commits == 1


def test_pin_search_path_raises_when_not_public():
    def responder(sql, params):
        if "SHOW search_path" in sql:
            return {"rows": [("scratch, public",)]}
        return {"rows": []}

    conn = _FakeConn(responder)
    with pytest.raises(w.OpenMacroV03Error, match="non-public schema"):
        w.pin_search_path(conn)


# --------------------------------------------------------------------------- #
# Gate 8 — staleness breach ⇒ ledger + NO output rows (drives run())
# --------------------------------------------------------------------------- #
def _stale_inputs():
    old = "2026-01-01T00:00:00+00:00"
    vintages = [{"series_id": s, "observation_period": "2025-12-01",
                 "vintage_date": "2026-01-01", "value": 1.0, "available_at": old,
                 "revision_number": 0, "source": "alfred", "source_spec_version": "v1"}
                for s in ("ACOGNO", "AHETPI", "CPILFESL", "INDPRO", "MICH",
                          "PAYEMS", "PCEC96", "PPIFIS")]
    prices = [{"ticker": t, "date": "2026-01-02", "close": 10.0,
               "adjusted_close": 10.0, "volume": 1000}
              for t in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")]
    return vintages, prices


def test_staleness_breach_writes_ledger_and_no_output(monkeypatch):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_pack_bytes", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_schema", lambda conn: {})
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(w, "compose_inputs", lambda conn, as_of: _stale_inputs())
    monkeypatch.setattr(w, "code_commit", lambda: "a" * 40)
    # date resolution has its own dedicated tests; pin the as_of so this staleness
    # test is independent of the real clock (and the future-override guard).
    monkeypatch.setattr(w, "resolve_as_of", lambda *a, **k: _dt.date(2026, 7, 6))

    def responder(sql, params):
        if "SELECT 1 FROM open_macro_v03_staleness_blocks" in sql:
            return {"rows": [(1,)]}
        return {}

    conn = _FakeConn(_lock_responder(responder))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)

    result = w.run("dsn", as_of="2026-07-06")
    assert result["status"] == "staleness_block"
    assert result["as_of"] == "2026-07-06"
    dml = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_staleness_blocks" in dml
    assert "INSERT INTO open_macro_v03_decisions" not in dml
    assert "INSERT INTO open_macro_v03_allocations" not in dml


def _patched_load_json(monkeypatch):
    real = w._load_json

    def loader(path):
        if Path(path) == Path(w.ENVELOPE_PATH):
            return _active_envelope()
        if Path(path) == Path(w.PINS_PATH):
            return {"modules": {}, "module_pins_sha256": "stub"}
        return real(path)
    return loader


# --------------------------------------------------------------------------- #
# resolve-staleness — the SANCTIONED recovery path for a blocked day
# --------------------------------------------------------------------------- #
def _resolve_env(monkeypatch, *, inputs, block=("block-run-id", "1" * 64, "2" * 64),
                 existing=None):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_pack_bytes", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_schema", lambda conn: {})
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "compose_inputs", lambda conn, as_of: inputs())
    monkeypatch.setattr(w, "code_commit", lambda: "a" * 40)

    def responder(sql, params):
        if "FROM open_macro_v03_staleness_blocks WHERE as_of" in sql:
            return {"rows": [block] if block is not None else []}
        if "resolution_state = 'resolved'" in sql:
            return {"rows": [existing] if existing is not None else []}
        return {"rowcount": 1}

    conn = _FakeConn(_lock_responder(responder))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)
    return conn


def test_resolve_staleness_records_the_event_with_a_freshness_proof(monkeypatch):
    """The operator path that did not exist: it APPENDS a 'resolved' event carrying
    the recomputed staleness report (per-source ages against the bounds in force) —
    and it never touches the immutable block ledger."""
    conn = _resolve_env(monkeypatch, inputs=_fresh_inputs)
    result = w.resolve_staleness_block("dsn", as_of="2026-07-06",
                                       resolved_by="Andrei Rachadel",
                                       reason="ALFRED published the July prints")
    assert result["status"] == "resolved"
    assert result["block_run_id"] == "block-run-id"
    assert result["freshness_proof"]["breaches"] == []
    assert result["freshness_proof"]["series"]  # the per-source proof is not empty
    inserts = [(s, p) for s, p in conn.executed
               if f"INSERT INTO {w.RESOLUTIONS_TABLE}" in s]
    assert len(inserts) == 1
    params = inserts[0][1]
    assert params["resolution_state"] == "resolved"
    assert params["resolved_by"] == "Andrei Rachadel"
    assert params["block_input_vintage_sha256"] == "1" * 64
    assert json.loads(params["freshness_proof"])["breaches"] == []
    # append-only: no statement mutates or removes the block ledger
    assert not any(s.strip().upper().startswith(("UPDATE", "DELETE"))
                   for s, _ in conn.executed)


def test_resolve_staleness_refuses_while_the_inputs_are_still_stale(monkeypatch):
    """No rubber stamp: the worker recomputes freshness itself and records NOTHING
    when the sources still breach the SLO."""
    conn = _resolve_env(monkeypatch, inputs=_stale_inputs)
    result = w.resolve_staleness_block("dsn", as_of="2026-07-06",
                                       resolved_by="Andrei Rachadel", reason="hoping")
    assert result["status"] == "still_stale"
    assert result["reason"]
    assert not any(f"INSERT INTO {w.RESOLUTIONS_TABLE}" in s for s, _ in conn.executed)


def test_resolve_staleness_is_idempotent_and_needs_a_block(monkeypatch):
    conn = _resolve_env(monkeypatch, inputs=_fresh_inputs, block=None)
    assert w.resolve_staleness_block("dsn", as_of="2026-07-06", resolved_by="A",
                                     reason="r")["status"] == "no_block"
    assert not any(f"INSERT INTO {w.RESOLUTIONS_TABLE}" in s for s, _ in conn.executed)

    conn = _resolve_env(monkeypatch, inputs=_fresh_inputs, existing=_resolution_row())
    result = w.resolve_staleness_block("dsn", as_of="2026-07-06", resolved_by="A",
                                       reason="r")
    assert result["status"] == "already_resolved"
    assert not any(f"INSERT INTO {w.RESOLUTIONS_TABLE}" in s for s, _ in conn.executed)


def test_resolve_staleness_is_fail_closed_before_any_db(monkeypatch):
    """Same gate ordering as run(): flag, governance, WRITER identity — all before a
    connection is even attempted. A clearance is a write on the official surface."""
    def _no_connect(*a, **k):
        raise AssertionError("resolve-staleness must not connect behind a closed gate")

    monkeypatch.setattr(w, "connect", _no_connect)
    monkeypatch.delenv("open_macro_v03_runtime_activation", raising=False)
    assert w.resolve_staleness_block("dsn", as_of="2026-07-06", resolved_by="A",
                                     reason="r") == {"status": "flag_off"}

    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    for name in w.WRITER_IDENTITY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    assert w.resolve_staleness_block("dsn", as_of="2026-07-06", resolved_by="A",
                                     reason="r")["status"] == "wrong_service"

    monkeypatch.setenv("WORKER_SERVICE_IDENTITY", "open-macro-v03-worker")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_pack_bytes", lambda *a, **k: None)
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    for bad in ({"resolved_by": "  "}, {"reason": ""}):
        kwargs = {"as_of": "2026-07-06", "resolved_by": "A", "reason": "r", **bad}
        with pytest.raises(w.OpenMacroV03Error):
            w.resolve_staleness_block("dsn", **kwargs)


def test_resolve_staleness_cli_wires_the_subcommand(monkeypatch, capsys):
    seen = {}

    def fake(dsn, *, as_of, resolved_by, reason):
        seen.update(dsn=dsn, as_of=as_of, resolved_by=resolved_by, reason=reason)
        return {"status": "resolved", "as_of": as_of}

    monkeypatch.setattr(w, "resolve_dsn", lambda: "dsn-from-env")
    monkeypatch.setattr(w, "resolve_staleness_block", fake)
    code = w.main(["resolve-staleness", "--as-of", "2026-07-17",
                   "--resolved-by", "Andrei Rachadel", "--reason", "prints landed"])
    assert code == 0
    assert seen == {"dsn": "dsn-from-env", "as_of": "2026-07-17",
                    "resolved_by": "Andrei Rachadel", "reason": "prints landed"}
    assert json.loads(capsys.readouterr().out)["status"] == "resolved"
    # a refusal is a non-zero exit: an operator/job never reads success from a block
    monkeypatch.setattr(w, "resolve_staleness_block",
                        lambda *a, **k: {"status": "still_stale"})
    assert w.main(["resolve-staleness", "--as-of", "2026-07-17",
                   "--resolved-by", "A", "--reason", "r"]) == 1


# --------------------------------------------------------------------------- #
# Provenance — code_commit is platform neutral
# --------------------------------------------------------------------------- #
def test_fleet_image_copies_every_runtime_input_of_the_gates():
    """The fleet image must carry what the fail-closed gates READ at runtime: the
    pinned pure modules (harness/, scripts/, src/), the ratified Stage B artifact and
    the certified pack. A missing tree turns a gate into an ImportError at 09:30 UTC."""
    dockerfile = (w.ROOT / "Dockerfile").read_text(encoding="utf-8")
    for root in sorted({p.split("/")[0] for p in w.EXPECTED_PINNED_MODULES}):
        assert f"COPY {root}/" in dockerfile, f"{root}/ missing from the image"
    assert w.STAGE_B_DIR.relative_to(w.ROOT).as_posix() in dockerfile.replace("\\", "/")
    assert w.PACK.relative_to(w.ROOT).as_posix() in dockerfile.replace("\\", "/")


def test_both_ledgers_are_append_only_in_the_source(monkeypatch):
    """Static guard: the worker holds NO statement that updates or deletes either the
    staleness-block ledger or its resolution ledger. Clearance is a new event, always."""
    source = Path(w.__file__).read_text(encoding="utf-8")
    for table in ("open_macro_v03_staleness_blocks", w.RESOLUTIONS_TABLE):
        for verb in ("UPDATE ", "DELETE FROM "):
            assert f"{verb}{table}" not in source, f"{verb}{table} in the worker source"
    ddl = (w.ROOT / "schemas" / f"{w.RESOLUTIONS_TABLE}.sql").read_text(encoding="utf-8")
    assert "ON CONFLICT" not in ddl and "UPDATE" not in ddl


def test_staleness_block_result_names_the_recovery_path(monkeypatch):
    """A blocked run must hand the operator the command, not a demand for an
    'explicit operator resolution' with no implementation behind it."""
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_pack_bytes", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_schema", lambda conn: {})
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "compose_inputs", lambda conn, as_of: _stale_inputs())
    monkeypatch.setattr(w, "code_commit", lambda: "a" * 40)
    monkeypatch.setattr(w, "resolve_as_of", lambda *a, **k: _dt.date(2026, 7, 6))

    def responder(sql, params):
        if "SELECT 1 FROM open_macro_v03_staleness_blocks" in sql:
            return {"rows": [(1,)]}
        return {}

    monkeypatch.setattr(w, "connect", lambda dsn: _FakeConn(_lock_responder(responder)))
    result = w.run("dsn", as_of="2026-07-06")
    assert result["status"] == "staleness_block"
    assert "resolve-staleness --as-of 2026-07-06" in result["resolution_path"]


def test_code_commit_reads_the_neutral_revision_env_vars(monkeypatch):
    for name in w.REVISION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    for name in w.REVISION_ENV_VARS:
        monkeypatch.setenv(name, "b" * 40)
        assert w.code_commit() == "b" * 40
        monkeypatch.delenv(name)


def test_code_commit_rejects_a_short_revision(monkeypatch):
    """CHAR(40) would blank-pad a short value into a lie about which code ran."""
    for name in w.REVISION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CODE_REVISION", "abc1234")
    with pytest.raises(w.OpenMacroV03Error, match="40-hex"):
        w.code_commit()


def test_code_commit_fails_loud_without_env_or_git(monkeypatch):
    for name in w.REVISION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    def _no_git(*a, **k):
        raise FileNotFoundError("git")

    monkeypatch.setattr(w.subprocess, "run", _no_git)
    with pytest.raises(w.OpenMacroV03Error, match="CODE_REVISION"):
        w.code_commit()


# --------------------------------------------------------------------------- #
# Gate 9 — fresh vs carried
# --------------------------------------------------------------------------- #
def test_consumable_fresh_when_decided_on_as_of():
    as_of = _dt.date(2026, 6, 30)  # month-end
    chain = [_FakeDecision(_dt.date(2026, 5, 31), "expansion"),
             _FakeDecision(as_of, "slowdown")]
    last, validity, seed = w.consumable_today(chain, as_of)
    assert validity == "fresh" and seed == as_of and last.quadrant == "slowdown"


def test_consumable_carried_when_last_decision_predates_as_of():
    as_of = _dt.date(2026, 7, 6)  # not a month-end
    chain = [_FakeDecision(_dt.date(2026, 6, 30), "recovery")]
    last, validity, seed = w.consumable_today(chain, as_of)
    assert validity == "carried" and seed == _dt.date(2026, 6, 30)


# --------------------------------------------------------------------------- #
# Gate 10 — allocation weights / risk cap / defensive floor
# --------------------------------------------------------------------------- #
def _priced_rows(date="2026-06-30"):
    return [{"ticker": t, "date": date, "close": 100.0, "adjusted_close": 100.0,
             "volume": 1000} for t in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")]


@pytest.mark.parametrize("quadrant", ["recovery", "expansion", "slowdown", "contraction"])
def test_build_allocation_respects_sum_cap_floor(quadrant):
    alloc = w.build_allocation(quadrant, _priced_rows(), _dt.date(2026, 6, 30))
    weights = alloc["weights"]
    assert abs(sum(weights.values()) - 1.0) < 1e-9
    assert alloc["risk_assets_weight"] <= 0.65 + 1e-9
    assert alloc["defensive_assets_weight"] >= 0.20 - 1e-9
    assert set(weights) == {"SPY", "TLT", "TIP", "GLD", "DBC", "SHY"}


def test_build_allocation_fails_loud_on_nan_price():
    rows = _priced_rows()
    rows[0]["adjusted_close"] = None  # SPY unusable -> no date prices the full sleeve
    with pytest.raises(w.OpenMacroV03Error, match="full sleeve"):
        w.build_allocation("expansion", rows, _dt.date(2026, 6, 30))


def test_build_allocation_uses_latest_common_date_on_split_ingest():
    # partial ingest: only SPY has the newest session, so the global max date would
    # leave the other tickers unpriced. The sleeve is priced at the latest COMMON date
    # instead of raising after the ledger gate (a silent missing_output).
    rows = _priced_rows("2026-06-30")
    rows.append({"ticker": "SPY", "date": "2026-07-01", "close": 101.0,
                 "adjusted_close": 101.0, "volume": 1000})
    alloc = w.build_allocation("expansion", rows, _dt.date(2026, 7, 1))
    assert alloc["priced_at"] == _dt.date(2026, 6, 30)


def test_build_allocation_refuses_stale_common_date():
    # every ticker has a recent-but-UNUSABLE latest print (passes staleness_report's
    # date-only check), forcing the common usable date older than the 3-business-day
    # SLO -> refuse rather than publish a stale allocation.
    rows = _priced_rows("2026-06-22")  # >3 business days before the as_of below
    rows += [{"ticker": t, "date": "2026-06-30", "close": 0.0,  # newer but unusable
              "adjusted_close": 0.0, "volume": 1000}
             for t in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")]
    with pytest.raises(w.OpenMacroV03Error, match="business days old"):
        w.build_allocation("expansion", rows, _dt.date(2026, 6, 30))


def test_build_allocation_center_book_when_degraded():
    """carry_decay_v1 ACTIVE: an expired carry publishes the mandate-tilted CENTER
    book ('center_50'), not the stale seed quadrant's compressed_50 book. The same
    sum/cap/floor gates hold."""
    from harness.direct_activation import carry_decay
    from harness.phase0q import sleeve as sleeve_mod
    alloc = w.build_allocation("contraction", _priced_rows(), _dt.date(2026, 6, 30),
                               degraded_to_center=True)
    assert alloc["book"] == "center_50"
    expected = carry_decay.center_book_50(
        sleeve_mod.SleeveParams(candidate_id=w.CANDIDATE_ID),
        list(sleeve_mod.SLEEVE_TICKERS))
    assert alloc["weights"] == {t: expected.get(t, 0.0)
                                for t in sleeve_mod.SLEEVE_TICKERS}
    assert abs(sum(alloc["weights"].values()) - 1.0) < 1e-9
    assert alloc["risk_assets_weight"] <= 0.65 + 1e-9
    assert alloc["defensive_assets_weight"] >= 0.20 - 1e-9
    # the degraded book differs from the seed quadrant's book (that is the point).
    seed_book = w.build_allocation("contraction", _priced_rows(), _dt.date(2026, 6, 30))
    assert seed_book["book"] == "compressed_50"
    assert alloc["weights"] != seed_book["weights"]


# --------------------------------------------------------------------------- #
# carry_decay_v1 publish path (run() end-to-end over fakes)
# --------------------------------------------------------------------------- #
def _fresh_inputs():
    """Inputs that PASS the staleness gate at as_of 2026-07-06 (Monday): every SEED
    series printed 2026-07-01; every sleeve ticker priced Friday 2026-07-03."""
    vintages = [{"series_id": s, "observation_period": "2026-06-01",
                 "vintage_date": "2026-07-01",
                 "value": 1.0, "available_at": "2026-07-01T00:00:00+00:00",
                 "revision_number": 0, "source": "alfred", "source_spec_version": "v1"}
                for s in ("ACOGNO", "AHETPI", "CPILFESL", "INDPRO", "MICH",
                          "PAYEMS", "PCEC96", "PPIFIS")]
    prices = [{"ticker": t, "date": "2026-07-03", "close": 100.0,
               "adjusted_close": 100.0, "volume": 1000}
              for t in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")]
    return vintages, prices


def _publish_capture_responder(state):
    """Fake-conn responder for a full publish run: allows the ledger check, captures
    both INSERT param dicts, and echoes them back to post_write_verify."""
    def responder(sql, params):
        if "SELECT 1 FROM open_macro_v03_staleness_blocks" in sql:
            return {"rows": []}
        if "INSERT INTO open_macro_v03_decisions" in sql:
            state["decision"] = params
            return {"rowcount": 1}
        if "INSERT INTO open_macro_v03_allocations" in sql:
            state["allocation"] = params
            return {"rowcount": 1}
        if "SELECT quadrant, publish_state, valid_status" in sql:
            return {"rows": [(state["decision"]["quadrant"], "published", "valid")]}
        if "SELECT w_spy" in sql:
            a = state["allocation"]
            return {"rows": [(float(a["w_spy"]), float(a["w_tlt"]), float(a["w_tip"]),
                              float(a["w_gld"]), float(a["w_dbc"]), float(a["w_shy"]),
                              "published", "valid")]}
        return {}
    return responder


def _run_to_publish(monkeypatch, chain, state):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_pack_bytes", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_schema", lambda conn: {})
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(w, "compose_inputs", lambda conn, as_of: _fresh_inputs())
    monkeypatch.setattr(w, "code_commit", lambda: "a" * 40)
    monkeypatch.setattr(w, "resolve_as_of", lambda *a, **k: _dt.date(2026, 7, 6))
    monkeypatch.setattr(w.decision_mod, "run_decision_series_v3",
                        lambda rows, prices, start, end: chain)
    conn = _FakeConn(_lock_responder(_publish_capture_responder(state)))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)
    return w.run("dsn", as_of="2026-07-06")


def test_run_publishes_center_book_when_carry_expired(monkeypatch):
    """carry_decay_v1 ACTIVE end-to-end: a 5-calendar-month-old carry (seed
    2026-02-28, as_of 2026-07-06) publishes decision_validity 'carried_expired'
    with honest provenance columns and the 'center_50' allocation book — the
    degraded position actually lands in the DB rows, not just the result dict."""
    from harness.direct_activation import carry_decay
    from harness.phase0q import sleeve as sleeve_mod
    state: dict = {}
    chain = [_FakeDecision(_dt.date(2026, 2, 28), "contraction")]
    result = _run_to_publish(monkeypatch, chain, state)

    assert result["status"] == "published"
    assert result["decision_validity"] == "carried_expired"
    assert result["book"] == "center_50"
    assert result["carry_provenance"]["carry_age_months"] == 5
    assert result["carry_provenance"]["carry_expired"] is True
    assert result["carry_provenance"]["degraded_to_center"] is True

    dec = state["decision"]
    assert dec["decision_validity"] == "carried_expired"
    assert dec["carry_age_months"] == 5
    assert dec["carry_expired"] is True
    assert dec["carry_seed_as_of"] == _dt.date(2026, 2, 28)
    assert dec["quadrant"] == "contraction"  # seed quadrant preserved as reference

    alloc = state["allocation"]
    assert alloc["book"] == "center_50"
    assert alloc["carry_age_months"] == 5
    assert alloc["carry_expired"] is True
    assert alloc["carry_seed_as_of"] == _dt.date(2026, 2, 28)
    expected = carry_decay.center_book_50(
        sleeve_mod.SleeveParams(candidate_id=w.CANDIDATE_ID),
        list(sleeve_mod.SLEEVE_TICKERS))
    for ticker in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY"):
        assert float(alloc[f"w_{ticker.lower()}"]) == pytest.approx(
            expected.get(ticker, 0.0), abs=1e-12)


def test_run_publishes_seed_book_when_carry_within_cap(monkeypatch):
    """A 1-month-old carry stays on the seed quadrant's compressed_50 book with
    carried validity and carry_expired=false provenance."""
    state: dict = {}
    chain = [_FakeDecision(_dt.date(2026, 6, 30), "expansion")]
    result = _run_to_publish(monkeypatch, chain, state)

    assert result["status"] == "published"
    assert result["decision_validity"] == "carried"
    assert result["book"] == "compressed_50"
    assert result["carry_provenance"]["carry_age_months"] == 1
    assert result["carry_provenance"]["carry_expired"] is False

    dec = state["decision"]
    assert dec["decision_validity"] == "carried"
    assert dec["carry_age_months"] == 1
    assert dec["carry_expired"] is False
    alloc = state["allocation"]
    assert alloc["book"] == "compressed_50"
    assert alloc["carry_expired"] is False


# --------------------------------------------------------------------------- #
# Gate 5 is READ-ONLY: no schema establishment before verification
# --------------------------------------------------------------------------- #
def _no_gate5_mocks(monkeypatch):
    """Pre-DB gates mocked; Gate 5 (catalog verification) runs FOR REAL so the
    read-only fail-loud behaviour is actually exercised."""
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "verify_pack_bytes", lambda *a, **k: None)
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "code_commit", lambda: "a" * 40)
    monkeypatch.setattr(w, "resolve_as_of", lambda *a, **k: _dt.date(2026, 7, 6))


_MUTATING_SQL = r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|COPY|GRANT)\b"


def test_run_is_read_only_and_fails_loud_against_an_absent_catalog(monkeypatch):
    """run() must NOT establish schema before verifying. Against an ABSENT catalog
    (fresh database), Gate 5 fails loud with ZERO mutating statements and no
    schema/data commit — schema lifecycle (base DDL + carry_decay migration) belongs
    to the ORCHESTRATOR, never the worker."""
    import re
    _no_gate5_mocks(monkeypatch)
    conn = _FakeConn(_lock_responder())  # catalog queries return no rows
    monkeypatch.setattr(w, "connect", lambda dsn: conn)
    with pytest.raises(w.OpenMacroV03Error, match="table missing from the catalog"):
        w.run("dsn", as_of="2026-07-06")
    mutating = [sql for sql, _ in conn.executed if re.search(_MUTATING_SQL, sql, re.I)]
    assert mutating == [], f"mutating SQL executed before/at Gate 5: {mutating}"
    # the ONLY commit is the session search_path pin (SET search_path is a session
    # setting, not a schema/data write); nothing durable was committed.
    assert conn.commits == 1


def test_run_is_read_only_and_fails_loud_against_an_unmigrated_catalog(monkeypatch):
    """Against a PRESENT but UNMIGRATED catalog (base tables without the carry_decay
    columns), Gate 5 fails loud with zero mutating statements: the worker never
    writes new-shaped rows into an old-shaped schema and never mutates the schema
    itself."""
    import re
    _no_gate5_mocks(monkeypatch)
    # simulate the pre-migration catalog: a carry provenance column is absent.
    conn = _FakeConn(_lock_responder(_catalog_responder(drop_column="carry_expired")))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)
    with pytest.raises(w.OpenMacroV03Error, match="column set diverges"):
        w.run("dsn", as_of="2026-07-06")
    mutating = [sql for sql, _ in conn.executed if re.search(_MUTATING_SQL, sql, re.I)]
    assert mutating == [], f"mutating SQL executed before/at Gate 5: {mutating}"
    assert conn.commits == 1


# --------------------------------------------------------------------------- #
# Gate 11 — publish never resurrects an invalidated row
# --------------------------------------------------------------------------- #
def _decision_row():
    return {"as_of": _dt.date(2026, 6, 30), "quadrant": "expansion",
            "decision_validity": "fresh", "carry_seed_as_of": _dt.date(2026, 6, 30),
            "carry_age_months": 0, "carry_expired": False,
            "candidate_confidence": 0.5, "coverage_quality": 0.9, "growth_score": 0.1,
            "inflation_score": -0.2, "input_vintage_sha256": "a" * 64,
            "input_prices_sha256": "b" * 64, "pack_v2_sha256": "c" * 64,
            "module_pins_sha256": "d" * 64, "judgment_ref": "j", "threshold_ref": "t",
            "code_commit": "e" * 40, "run_id": "rid", "valid_until": _dt.datetime.now()}


def _allocation_row():
    return {"as_of": _dt.date(2026, 6, 30), "book": "compressed_50", "w_spy": 0.5,
            "w_tlt": 0.1, "w_tip": 0.1, "w_gld": 0.1, "w_dbc": 0.1, "w_shy": 0.1,
            "risk_assets_weight": 0.6, "defensive_assets_weight": 0.3, "risk_cap": 0.65,
            "defensive_floor": 0.20, "priced_at": _dt.date(2026, 6, 30),
            "carry_age_months": 0, "carry_seed_as_of": _dt.date(2026, 6, 30),
            "carry_expired": False,
            "input_prices_sha256": "b" * 64, "pack_v2_sha256": "c" * 64,
            "module_pins_sha256": "d" * 64, "code_commit": "e" * 40, "run_id": "rid",
            "valid_until": _dt.datetime.now()}


def test_publish_writes_both_and_commits():
    conn = _FakeConn(lambda sql, params: {"rowcount": 1})
    w.publish(conn, _decision_row(), _allocation_row())
    tables = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_decisions" in tables
    assert "INSERT INTO open_macro_v03_allocations" in tables
    assert conn.commits == 1


# --------------------------------------------------------------------------- #
# NUMERIC write fidelity (production defect 2026-07-06: float8->numeric casts
# truncate at 15 significant digits; Decimal(repr(x)) round-trips exactly)
# --------------------------------------------------------------------------- #
# Adversarial floats, headed by the REAL production values the Stage C verifier
# caught truncated (recomputed 0.8121545618518331, stored 0.812154561851833).
_ADVERSARIAL_FLOATS = [
    0.8121545618518331,           # candidate_confidence — the observed abort
    0.1 + 0.2,                    # 0.30000000000000004 (17 sig digits)
    1.0 / 3.0, 2.0 / 3.0,
    0.39374999999999993,
    1e-17, 1.7976931348623157e308, 5e-324,
    -0.8121545618518331,
    0.0, 1.0, 0.65, 0.2,
]


def test_exact_numeric_round_trips_adversarial_floats():
    import decimal
    for x in _ADVERSARIAL_FLOATS:
        converted = w._exact_numeric(x)
        assert isinstance(converted, decimal.Decimal), x
        assert float(converted) == x, f"{x!r} did not round-trip"
        # str(Decimal(repr(x))) is repr(x) (Decimal upper-cases the exponent marker):
        # the shortest decimal that round-trips, carrying up to 17 significant digits
        # when the float needs them
        assert str(converted).lower() == repr(x).lower()
    # 17 significant digits preserved where 15 would truncate
    assert str(w._exact_numeric(0.8121545618518331)) == "0.8121545618518331"
    assert len("8121545618518331") == 16  # > the 15-digit float8->numeric cast
    # None and non-floats pass through untouched
    assert w._exact_numeric(None) is None
    assert w._exact_numeric("abc") == "abc"
    assert w._exact_numeric(7) == 7 and type(w._exact_numeric(7)) is int
    d = _dt.date(2026, 7, 6)
    assert w._exact_numeric(d) is d


def test_exact_numeric_round_trips_the_real_sleeve_weights():
    """The actual compressed_50 target weights for every quadrant (the exact floats
    production publishes) must survive Decimal(repr(x)) round-trip."""
    from harness.phase0q import sleeve as sleeve_mod
    for quadrant in ("recovery", "expansion", "slowdown", "contraction"):
        weights = sleeve_mod.target_weights(
            quadrant, sleeve_mod.SleeveParams(candidate_id=w.CANDIDATE_ID),
            list(sleeve_mod.SLEEVE_TICKERS), compressed=True)
        for ticker, value in weights.items():
            converted = w._exact_numeric(value)
            assert float(converted) == value, (quadrant, ticker)


def test_publish_sends_no_raw_float_parameter():
    """The write-fidelity guard: EVERY parameter dict the publish path executes is
    float-free (floats became exact Decimals through the single chokepoint)."""
    captured: list = []

    def responder(sql, params):
        captured.append(params)
        return {"rowcount": 1}

    conn = _FakeConn(responder)
    decision = _decision_row()
    allocation = _allocation_row()
    decision["candidate_confidence"] = 0.8121545618518331
    allocation["w_spy"] = 0.39374999999999993
    w.publish(conn, decision, allocation)
    assert captured, "publish executed nothing"
    for params in captured:
        if isinstance(params, dict):
            for key, value in params.items():
                assert not isinstance(value, float), \
                    f"raw float leaked to the driver: {key}={value!r}"


def test_record_staleness_block_sends_no_raw_float_parameter():
    captured: list = []

    def responder(sql, params):
        captured.append(params)
        return {"rowcount": 1}

    conn = _FakeConn(responder)
    w.record_staleness_block(conn, {
        "as_of": _dt.date(2026, 7, 6), "reason": "r", "stale_detail": "{}",
        "input_vintage_sha256": "a" * 64, "input_prices_sha256": "b" * 64,
        "pack_v2_sha256": "c" * 64, "module_pins_sha256": "d" * 64,
        "code_commit": "e" * 40, "run_id": "rid"})
    for params in captured:
        if isinstance(params, dict):
            for key, value in params.items():
                assert not isinstance(value, float), key


def test_publish_raises_when_row_is_invalidated():
    # decision upsert conflicts with an invalidated row (rowcount 0)
    def responder(sql, params):
        return {"rowcount": 0 if "open_macro_v03_decisions" in sql else 1}

    conn = _FakeConn(responder)
    with pytest.raises(w.OpenMacroV03Error, match="resurrect an invalidated decision"):
        w.publish(conn, _decision_row(), _allocation_row())
    assert conn.commits == 0  # never committed


# --------------------------------------------------------------------------- #
# invalidate CLI — updates both output tables, never the ledger
# --------------------------------------------------------------------------- #
def test_invalidate_updates_both_tables_not_ledger(monkeypatch):
    conn = _FakeConn(_lock_responder(lambda sql, params: {"rowcount": 2}))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)
    result = w.invalidate("dsn", as_of="2026-06-30", to="2026-07-06", reason="abort")
    assert result["status"] == "invalidated"
    assert result["decisions_invalidated"] == 2
    assert result["allocations_invalidated"] == 2
    updates = [sql for sql, _ in conn.executed if sql.startswith("UPDATE")]
    assert any("open_macro_v03_decisions" in s for s in updates)
    assert any("open_macro_v03_allocations" in s for s in updates)
    assert not any("staleness_blocks" in s for s, _ in conn.executed)
    # the kill switch pins search_path to public first (same as run())
    assert any("SET search_path" in s for s, _ in conn.executed)


# --------------------------------------------------------------------------- #
# valid_until — next business day at 14:00 UTC
# --------------------------------------------------------------------------- #
def test_valid_until_next_business_day_1400_utc():
    # Tue -> Wed
    assert w.valid_until(_dt.date(2026, 6, 30)) == \
        _dt.datetime(2026, 7, 1, 14, 0, tzinfo=_dt.timezone.utc)
    # Fri -> Mon (skips the weekend)
    assert w.valid_until(_dt.date(2026, 7, 3)) == \
        _dt.datetime(2026, 7, 6, 14, 0, tzinfo=_dt.timezone.utc)
