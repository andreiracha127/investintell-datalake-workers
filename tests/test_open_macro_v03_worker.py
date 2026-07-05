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


def test_committed_blocked_envelope_blocks_without_db(monkeypatch):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")

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


def test_committed_blocked_envelope_is_blocked_by_the_matrix_gate_itself():
    """Flip every boolean/scope gate of the COMMITTED envelope but keep its pending
    matrix: the NEW approval-matrix gate must still block (not an accident of the
    earlier boolean gates)."""
    committed = json.loads(w.ENVELOPE_PATH.read_text(encoding="utf-8"))
    forged = dict(committed)
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
    # current-or-past overrides stay trusted
    assert w.resolve_as_of("2026-07-06", today=_dt.date(2026, 7, 6)) == _dt.date(2026, 7, 6)
    assert w.resolve_as_of("2026-07-05", today=_dt.date(2026, 7, 6)) == _dt.date(2026, 7, 5)


def test_resolve_as_of_rejects_future_env_override(monkeypatch):
    monkeypatch.setenv("OPEN_MACRO_V03_AS_OF", "2026-07-10")
    with pytest.raises(w.OpenMacroV03Error, match="future"):
        w.resolve_as_of(today=_dt.date(2026, 7, 6))


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
def test_publish_refuses_when_ledger_row_exists():
    def responder(sql, params):
        if "SELECT 1 FROM open_macro_v03_staleness_blocks" in sql:
            return {"rows": [(1,)]}
        return {"rowcount": 1}

    conn = _FakeConn(responder)
    with pytest.raises(w.OpenMacroV03Error, match="publish refused.*staleness-block"):
        w.publish(conn, _decision_row(), _allocation_row())
    assert conn.commits == 0
    dml = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_decisions" not in dml


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


# --------------------------------------------------------------------------- #
# Gate 11 — publish never resurrects an invalidated row
# --------------------------------------------------------------------------- #
def _decision_row():
    return {"as_of": _dt.date(2026, 6, 30), "quadrant": "expansion",
            "decision_validity": "fresh", "carry_seed_as_of": _dt.date(2026, 6, 30),
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
