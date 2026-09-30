"""Bond Default Events worker (``WORKER=bond_default_events``): modes, safeguards, loader.

Offline suites use the frozen synthetic bundles and the package's in-memory store, which
mirrors the SQL lifecycle (immutability, CAS, revocation, T/K monotonicity). The disposable
PostgreSQL lifecycle test runs only with an explicit loopback
``BOND_DEFAULT_TEST_DATABASE_URL`` and is skipped otherwise; a skip is not acceptance evidence.
"""

from __future__ import annotations

import ast
import contextlib
import datetime as dt
import hashlib
import json
import os
import re
import socket
import uuid
from pathlib import Path

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import publication as pub
from src.workers import bond_default_events as w

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bond_default_events"
ALL_ENV = (
    w.ENV_MODE,
    w.ENV_MANIFEST,
    w.ENV_SCHEMA,
    w.ENV_PUBLICATION_ID,
    w.ENV_EXPECTED_POINTER,
    w.ENV_CONFIRM_PROMOTE,
    w.ENV_RELEASE_ID,
    w.ENV_EXPECTED_DIAGNOSTIC_POINTER,
    w.ENV_CONFIRM_DIAGNOSTIC_PROMOTE,
    w.ENV_PLAN_READ_PANEL,
    w.ENV_CONFIRM_INSTALL,
    w.ENV_EXPECTED_OWNER,
    w.ENV_RUNTIME_ROLE,
    w.ENV_OTHER_ROLES,
    w.ENV_CODE_DIGEST,
    "DATABASE_URL",
    "WORKER",
    "WORKER_LIMIT",
    "WORKER_CALC_DATE",
    "DB_TLS_CA_PEM",
    "DB_TLS_CERT_PEM",
    "DB_TLS_KEY_PEM",
)
SECRET_DSN = "postgresql://svc:hunter2-secret@db.example.invalid:5432/market"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ALL_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def _no_real_database(monkeypatch):
    """Nothing in this module may open a real connection unless a test opts in."""

    def forbidden(dsn):
        raise AssertionError("a database connection was attempted")

    monkeypatch.setattr(w, "_connect", forbidden)
    monkeypatch.setattr(w, "_connect_autocommit", forbidden)


def _bundle_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _write_invocation(
    tmp_path: Path,
    fixture: str = "bundle_qualified.json",
    *,
    data: bytes | None = None,
    sha: str | None = None,
    expected: dict | None = None,
    **top,
) -> Path:
    raw = _bundle_bytes(fixture) if data is None else data
    bundle = c.CreditBundle.from_json_bytes(_bundle_bytes(fixture))
    m = bundle.manifest
    (tmp_path / "bundle.json").write_bytes(raw)
    doc = {
        "invocation_version": w.INVOCATION_VERSION,
        "product": c.PRODUCT,
        "contract_version": c.CONTRACT_VERSION,
        "input": {
            "kind": "bundle",
            "path": "bundle.json",
            "sha256": sha or hashlib.sha256(raw).hexdigest(),
        },
        "expected": {
            "publication_id": str(bundle.publication_id),
            "target_month": m["target_month"].isoformat(),
            "knowledge_cutoff": m["knowledge_cutoff"]
            .astimezone(dt.timezone.utc)
            .isoformat(),
            "quality_state": m["quality_state"],
            "build_scope": m["build_scope"],
            **(expected or {}),
        },
    }
    doc.update(top)
    path = tmp_path / "invocation.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def _factory(store):
    return lambda dsn, schema: contextlib.nullcontext(store)


def _run(store, **kwargs):
    kwargs.setdefault("dsn", SECRET_DSN)
    kwargs.setdefault("schema", "bond_credit")
    return w.run(store_factory=_factory(store), **kwargs)


def _pid(fixture: str = "bundle_qualified.json") -> uuid.UUID:
    return c.CreditBundle.from_json_bytes(_bundle_bytes(fixture)).publication_id


@pytest.fixture
def block_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("network access attempted")

    for target in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, target, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


# ---------------------------------------------------------------------------
# Mode parsing and defaults
# ---------------------------------------------------------------------------
def test_default_mode_is_plan_and_needs_a_manifest():
    with pytest.raises(w.ConfigError, match="manifest_required"):
        w.run(SECRET_DSN, env={})


def test_default_mode_is_a_dry_run_that_never_touches_the_store(tmp_path):
    invocation = _write_invocation(tmp_path)

    def factory(dsn, schema):
        raise AssertionError("plan must not open a store")

    stats = w.run(SECRET_DSN, manifest=invocation, store_factory=factory, env={})
    assert stats["mode"] == "plan" and stats["state"] == "planned"
    assert stats["database"] == "not_contacted" and stats["network"] == "not_used"
    assert stats["promotable_as_qualified"] is True
    assert stats["publication_id"] == str(_pid())


@pytest.mark.parametrize("mode", ["apply", "PLAN", "install", "promote-now"])
def test_unknown_mode_is_refused(mode, tmp_path):
    with pytest.raises(w.ConfigError, match="mode_unknown"):
        w.run(SECRET_DSN, mode=mode, manifest=_write_invocation(tmp_path), env={})


def test_blank_mode_falls_back_to_the_safe_default(tmp_path):
    stats = w.run(
        SECRET_DSN, manifest=_write_invocation(tmp_path), env={w.ENV_MODE: "  "}
    )
    assert stats["mode"] == "plan"


def test_mode_can_come_from_the_environment(tmp_path):
    env = {w.ENV_MODE: "plan", w.ENV_MANIFEST: str(_write_invocation(tmp_path))}
    assert w.run(SECRET_DSN, env=env)["state"] == "planned"


@pytest.mark.parametrize("mode", ["prepare", "verify", "promote"])
def test_writing_modes_need_an_explicit_dsn_and_schema(mode, tmp_path):
    common = {
        "manifest": _write_invocation(tmp_path),
        "publication_id": str(_pid()),
        "env": {},
    }
    if mode == "prepare":
        common.pop("publication_id")
    with pytest.raises(w.ConfigError, match="dsn_required"):
        w.run(None, mode=mode, **common)
    with pytest.raises(w.ConfigError, match="schema_required"):
        w.run(SECRET_DSN, mode=mode, **common)


def test_schema_identifier_is_validated_before_connecting():
    with (
        pytest.raises(w.ConfigError, match="schema_identifier_invalid"),
        w._open_store(SECRET_DSN, 'x"; DROP SCHEMA public; --'),
    ):
        pass


def test_promote_options_are_rejected_outside_promote_mode(tmp_path):
    invocation = _write_invocation(tmp_path)
    with pytest.raises(w.ConfigError, match="promote_options_outside_promote_mode"):
        w.run(SECRET_DSN, manifest=invocation, expected_pointer="none", env={})
    with pytest.raises(w.ConfigError, match="promote_options_outside_promote_mode"):
        w.run(SECRET_DSN, manifest=invocation, confirm_promote=str(_pid()), env={})
    with pytest.raises(w.ConfigError, match="publication_id_not_applicable"):
        w.run(SECRET_DSN, manifest=invocation, publication_id=str(_pid()), env={})


# ---------------------------------------------------------------------------
# Invocation manifest strictness
# ---------------------------------------------------------------------------
def _mutated(tmp_path, mutate):
    path = _write_invocation(tmp_path)
    doc = json.loads(path.read_text(encoding="utf-8"))
    mutate(doc)
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda d: d.update(extra=1), "manifest_fields_mismatch"),
        (lambda d: d.pop("expected"), "manifest_fields_mismatch"),
        (lambda d: d.update(invocation_version="v0"), "unsupported_invocation_version"),
        (lambda d: d.update(product="other"), "product_or_contract_version_mismatch"),
        (
            lambda d: d.update(contract_version="bond_default_event_bundle_v1"),
            "product_or_contract_version_mismatch",
        ),
        (lambda d: d["input"].pop("sha256"), "manifest_fields_mismatch"),
        (lambda d: d["input"].update(sha256="ABC"), "input_sha256_expected"),
        (lambda d: d["input"].update(kind="download"), "input_kind_unknown"),
        (lambda d: d["input"].update(path=""), "input_path_expected"),
        (
            lambda d: d["expected"].update(knowledge_cutoff="2026-09-25T12:00:00"),
            "knowledge_cutoff_needs_timezone",
        ),
        (
            lambda d: d["expected"].update(knowledge_cutoff="soon"),
            "knowledge_cutoff_expected",
        ),
        (
            lambda d: d["expected"].update(target_month="20260801"),
            "target_month_not_canonical",
        ),
        (lambda d: d["expected"].update(publication_id="not-a-uuid"), "uuid_expected"),
        (
            lambda d: d["expected"].update(quality_state="great"),
            "quality_state_unknown",
        ),
        (lambda d: d["expected"].update(build_scope="partial"), "build_scope_unknown"),
    ],
)
def test_invocation_manifest_is_strict(tmp_path, mutate, code):
    with pytest.raises(w.InvocationError, match=code):
        w.run(SECRET_DSN, manifest=_mutated(tmp_path, mutate), env={})


def test_duplicate_json_keys_and_missing_files_are_refused(tmp_path):
    path = tmp_path / "invocation.json"
    path.write_text(
        '{"invocation_version": "a", "invocation_version": "b"}', encoding="utf-8"
    )
    with pytest.raises(w.InvocationError, match="manifest_not_strict_json"):
        w.run(SECRET_DSN, manifest=path, env={})
    with pytest.raises(w.InvocationError, match="manifest_unreadable"):
        w.run(SECRET_DSN, manifest=tmp_path / "absent.json", env={})


# ---------------------------------------------------------------------------
# Plan: hash pin, contract, identity (T/K)
# ---------------------------------------------------------------------------
def test_plan_of_a_partial_bundle_reports_it_is_not_promotable(tmp_path):
    stats = w.run(
        SECRET_DSN, manifest=_write_invocation(tmp_path, "bundle_partial.json"), env={}
    )
    assert (
        stats["quality_state"] == "partial"
        and stats["promotable_as_qualified"] is False
    )
    assert stats["frame_counts"]["source_packages"] > 0


def test_plan_refuses_a_bundle_whose_bytes_do_not_match_the_pin(tmp_path):
    invocation = _write_invocation(tmp_path, sha="0" * 64)
    with pytest.raises(w.IntegrityError, match="bundle_sha256_mismatch"):
        w.run(SECRET_DSN, manifest=invocation, env={})


def test_plan_refuses_a_bundle_that_hashes_right_but_breaks_the_contract(tmp_path):
    doc = json.loads(_bundle_bytes("bundle_qualified.json"))
    doc["manifest"]["coverage_count"] += 1  # derived field no longer matches the frames
    tampered = json.dumps(doc).encode("utf-8")
    with pytest.raises(w.ContractViolation, match="bundle_contract_violation"):
        w.run(SECRET_DSN, manifest=_write_invocation(tmp_path, data=tampered), env={})


def test_plan_refuses_a_non_json_or_legacy_bundle(tmp_path):
    with pytest.raises(w.ContractViolation):
        w.run(
            SECRET_DSN, manifest=_write_invocation(tmp_path, data=b"not json"), env={}
        )
    legacy = _bundle_bytes("legacy_v1_bundle_rejection.json")
    with pytest.raises(
        w.ContractViolation,
        match="unsupported_contract_version|bundle_contract_violation",
    ):
        w.run(SECRET_DSN, manifest=_write_invocation(tmp_path, data=legacy), env={})


def test_plan_refuses_a_missing_bundle_file(tmp_path):
    invocation = _write_invocation(tmp_path)
    (tmp_path / "bundle.json").unlink()
    with pytest.raises(w.IntegrityError, match="bundle_unreadable"):
        w.run(SECRET_DSN, manifest=invocation, env={})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("target_month", "2026-01-01"),
        ("knowledge_cutoff", "2026-01-01T00:00:00+00:00"),
        ("publication_id", "00000000-0000-8000-8000-000000000000"),
        ("quality_state", "unavailable"),
        ("build_scope", "limited"),
    ],
)
def test_plan_refuses_any_pinned_identity_mismatch(tmp_path, field, value):
    invocation = _write_invocation(tmp_path, expected={field: value})
    with pytest.raises(w.IntegrityError, match=f"bundle_{field}_mismatch"):
        w.run(SECRET_DSN, manifest=invocation, env={})


def test_plan_and_prepare_use_no_network(tmp_path, block_network):
    invocation = _write_invocation(tmp_path)
    assert w.run(SECRET_DSN, manifest=invocation, env={})["state"] == "planned"
    store = pub.InMemoryPublicationStore()
    assert (
        _run(store, mode="prepare", manifest=invocation, env={})["prepare_outcome"]
        == "inserted"
    )


# ---------------------------------------------------------------------------
# Prepare
# ---------------------------------------------------------------------------
def test_prepare_persists_a_prepared_build_and_replays_exactly(tmp_path):
    invocation = _write_invocation(tmp_path)
    store = pub.InMemoryPublicationStore()
    first = _run(store, mode="prepare", manifest=invocation, env={})
    assert first["state"] == "prepared" and first["prepare_outcome"] == "inserted"
    state = store.state(_pid())
    assert (
        state.lifecycle_state == "prepared" and store.pointer is None
    )  # never validates or promotes
    again = _run(store, mode="prepare", manifest=invocation, env={})
    assert again["prepare_outcome"] == "replayed"
    assert len(store.publications) == 1


def test_prepare_does_not_open_the_store_when_the_offline_gate_fails(tmp_path):
    invocation = _write_invocation(tmp_path, sha="1" * 64)

    def factory(dsn, schema):
        raise AssertionError(
            "the store must not be opened before the offline gate passes"
        )

    with pytest.raises(w.IntegrityError):
        w.run(
            SECRET_DSN,
            mode="prepare",
            manifest=invocation,
            schema="bond_credit",
            store_factory=factory,
            env={},
        )


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
def _prepared(tmp_path, fixture="bundle_qualified.json"):
    store = pub.InMemoryPublicationStore()
    invocation = _write_invocation(tmp_path, fixture)
    _run(store, mode="prepare", manifest=invocation, env={})
    return store, invocation


def test_verify_validates_a_prepared_publication_without_promoting(tmp_path):
    store, invocation = _prepared(tmp_path)
    stats = _run(
        store, mode="verify", publication_id=str(_pid()), manifest=invocation, env={}
    )
    assert stats["state"] == "verified" and stats["lifecycle_state"] == "validated"
    assert stats["revoked"] is False and store.pointer is None


def test_verify_of_an_unknown_publication_fails_loud():
    with pytest.raises(w.PublicationRefused, match="unknown_publication"):
        _run(
            pub.InMemoryPublicationStore(),
            mode="verify",
            publication_id=str(uuid.uuid4()),
            env={},
        )


def test_verify_cross_check_refuses_a_pinned_tk_mismatch_before_validating(tmp_path):
    store, _ = _prepared(tmp_path)
    wrong = _write_invocation(
        tmp_path, expected={"knowledge_cutoff": "2026-01-01T00:00:00+00:00"}
    )
    with pytest.raises(w.IntegrityError, match="persisted_knowledge_cutoff_mismatch"):
        _run(store, mode="verify", publication_id=str(_pid()), manifest=wrong, env={})
    assert (
        store.state(_pid()).lifecycle_state == "prepared"
    )  # no state change on a mismatch


def test_verify_refuses_a_manifest_naming_another_publication(tmp_path):
    store, invocation = _prepared(tmp_path)
    with pytest.raises(w.IntegrityError, match="manifest_publication_id_mismatch"):
        _run(
            store,
            mode="verify",
            publication_id=str(uuid.uuid4()),
            manifest=invocation,
            env={},
        )


def test_verify_refuses_a_revoked_publication(tmp_path):
    store, _ = _prepared(tmp_path)
    _run(store, mode="verify", publication_id=str(_pid()), env={})
    store.revoke(_pid(), "test revocation", "sha256:" + "a" * 64)
    with pytest.raises(w.PublicationRefused, match="revoked"):
        _run(store, mode="verify", publication_id=str(_pid()), env={})


# ---------------------------------------------------------------------------
# Promote: explicit only
# ---------------------------------------------------------------------------
def _validated(tmp_path, fixture="bundle_qualified.json"):
    store, invocation = _prepared(tmp_path, fixture)
    _run(store, mode="verify", publication_id=str(_pid(fixture)), env={})
    return store, invocation


def _promote_kwargs(fixture="bundle_qualified.json", **override):
    pid = str(_pid(fixture))
    base = {
        "mode": "promote",
        "publication_id": pid,
        "expected_pointer": "none",
        "confirm_promote": pid,
        "env": {},
    }
    base.update(override)
    return base


def test_promote_requires_an_explicit_expected_pointer(tmp_path):
    store, _ = _validated(tmp_path)
    kwargs = _promote_kwargs()
    del kwargs["expected_pointer"]
    with pytest.raises(w.PromoteNotAuthorized, match="expected_pointer_required"):
        _run(store, **kwargs)
    with pytest.raises(w.PromoteNotAuthorized, match="expected_pointer_required"):
        _run(store, **_promote_kwargs(expected_pointer=""))
    assert store.pointer is None


@pytest.mark.parametrize(
    "confirm",
    [None, "", "yes", "true", "1", "3d1f5a52-8f0b-4b7c-9d0e-6a1c2b7e4f10"],
)
def test_promote_requires_confirmation_equal_to_the_publication_id(tmp_path, confirm):
    store, _ = _validated(tmp_path)
    with pytest.raises(w.PromoteNotAuthorized, match="confirmation_required"):
        _run(store, **_promote_kwargs(confirm_promote=confirm))
    assert store.pointer is None


def test_promote_sets_the_pointer_only_with_every_safeguard(tmp_path):
    store, invocation = _validated(tmp_path)
    stats = _run(store, **_promote_kwargs(manifest=invocation))
    assert stats["state"] == "promoted" and stats["pointer"] == str(_pid())
    assert stats["expected_pointer"] is None
    assert store.pointer == _pid()


def test_promote_configuration_can_come_from_the_environment(tmp_path):
    store, _ = _validated(tmp_path)
    pid = str(_pid())
    env = {
        w.ENV_MODE: "promote",
        w.ENV_PUBLICATION_ID: pid,
        w.ENV_EXPECTED_POINTER: "NONE",
        w.ENV_CONFIRM_PROMOTE: pid,
        w.ENV_SCHEMA: "bond_credit",
    }
    assert (
        w.run(SECRET_DSN, env=env, store_factory=_factory(store))["state"] == "promoted"
    )


def test_promote_is_a_compare_and_set(tmp_path):
    store, _ = _validated(tmp_path)
    with pytest.raises(w.PublicationRefused, match="cas_mismatch"):
        _run(store, **_promote_kwargs(expected_pointer=str(uuid.uuid4())))
    assert store.pointer is None
    _run(store, **_promote_kwargs())
    with pytest.raises(w.PublicationRefused, match="cas_mismatch"):
        _run(
            store, **_promote_kwargs(expected_pointer="none")
        )  # pointer already set: stale expectation


def test_promote_refuses_an_unvalidated_build(tmp_path):
    store, _ = _prepared(tmp_path)
    with pytest.raises(w.PublicationRefused, match="not_validated"):
        _run(store, **_promote_kwargs())
    assert store.pointer is None


def test_promote_refuses_partial_builds_the_qualified_pointer_is_not_weakened(tmp_path):
    store, _ = _validated(tmp_path, "bundle_partial.json")
    with pytest.raises(w.PublicationRefused, match="not_qualified_complete"):
        _run(store, **_promote_kwargs("bundle_partial.json"))
    assert store.pointer is None


def test_promote_refuses_a_revoked_build(tmp_path):
    store, _ = _validated(tmp_path)
    store.revoke(_pid(), "test revocation", "sha256:" + "b" * 64)
    with pytest.raises(w.PublicationRefused, match="revoked"):
        _run(store, **_promote_kwargs())
    assert store.pointer is None


def test_promote_cross_checks_the_pinned_identity(tmp_path):
    store, _ = _validated(tmp_path)
    wrong = _write_invocation(tmp_path, expected={"target_month": "2026-01-01"})
    with pytest.raises(w.IntegrityError, match="persisted_target_month_mismatch"):
        _run(store, **_promote_kwargs(manifest=wrong))
    assert store.pointer is None


def test_promote_of_an_unknown_publication_fails_loud():
    store = pub.InMemoryPublicationStore()
    with pytest.raises(w.PublicationRefused, match="unknown_publication"):
        _run(
            store,
            **_promote_kwargs(publication_id=str(_pid()), confirm_promote=str(_pid())),
        )


# ---------------------------------------------------------------------------
# No DDL, sanitized connection handling
# ---------------------------------------------------------------------------
def test_worker_source_references_schema_installation_only_in_the_install_mode():
    tree = ast.parse(Path(w.__file__).read_text(encoding="utf-8"))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
        n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)
    }
    installers = {
        "install_schema",
        "install_diagnostic_schema",
        "harden_installed_privileges",
    }
    assert (
        installers <= names
    )  # only the explicit install-schema mode references them ...
    for node in ast.walk(tree):  # ... and only inside _install_schema
        if isinstance(node, ast.FunctionDef) and node.name != "_install_schema":
            inner = {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)} | {
                n.id for n in ast.walk(node) if isinstance(n, ast.Name)
            }
            assert not inner & installers, node.name
    ddl = re.compile(r"^\s*(CREATE|ALTER|DROP|GRANT|TRUNCATE)\b", re.IGNORECASE)
    strings = [
        n.value
        for n in ast.walk(tree)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    ]
    assert not [
        s for s in strings if ddl.match(s) and len(s) < 200 and "\n" not in s.strip()
    ]


def test_connect_failure_is_sanitized(monkeypatch):
    def failing(dsn):
        raise RuntimeError(f"could not connect using {dsn}")

    monkeypatch.setattr(w, "_connect", failing)
    with pytest.raises(w.InfrastructureError, match="db_connect_failed") as caught:
        w.run(
            SECRET_DSN,
            mode="verify",
            publication_id=str(uuid.uuid4()),
            schema="bond_credit",
            env={},
        )
    assert "hunter2" not in str(caught.value) and caught.value.__cause__ is None
    assert caught.value.detail == "RuntimeError"


def test_real_store_plumbing_runs_no_statement_and_closes_the_connection(monkeypatch):
    class Info:
        transaction_status = (
            "not-idle"  # PostgresPublicationStore refuses before executing anything
        )

    class Conn:
        info = Info()
        closed = False

        def __init__(self):
            self.executed = []

        def execute(self, *args, **kwargs):
            self.executed.append(args)

        def close(self):
            self.closed = True

    conn = Conn()
    monkeypatch.setattr(w, "_connect", lambda dsn: conn)
    with pytest.raises(w.PublicationRefused, match="connection_not_idle"):
        w.run(
            SECRET_DSN,
            mode="verify",
            publication_id=str(uuid.uuid4()),
            schema="bond_credit",
            env={},
        )
    assert conn.executed == [] and conn.closed is True


# ---------------------------------------------------------------------------
# CLI and typed exit codes
# ---------------------------------------------------------------------------
def test_cli_plan_prints_json_and_exits_zero(tmp_path, capsys):
    assert w.main(["--manifest", str(_write_invocation(tmp_path))]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["worker"] == "bond_default_events" and out["state"] == "planned"


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["--mode", "plan"], 2),  # manifest_required
        (["--mode", "bogus"], 2),  # argparse refusal
        (
            ["--mode", "verify", "--publication-id", str(uuid.uuid4())],
            2,
        ),  # dsn_required
    ],
)
def test_cli_config_errors_exit_2(argv, expected, capsys):
    assert w.main(argv) == expected


def test_cli_typed_exit_codes(tmp_path, capsys, monkeypatch):
    bad_hash = _write_invocation(tmp_path, sha="2" * 64)
    assert w.main(["--manifest", str(bad_hash)]) == w.IntegrityError.exit_code == 4
    assert json.loads(capsys.readouterr().out)["code"] == "bundle_sha256_mismatch"
    sources = _mutated(tmp_path, lambda d: d.update(input={"kind": "sources"}))
    assert w.main(["--manifest", str(sources)]) == w.InvocationError.exit_code == 3
    bad = tmp_path / "bad.json"
    bad.write_text("{}", encoding="utf-8")
    assert w.main(["--manifest", str(bad)]) == 3
    tampered = json.loads(_bundle_bytes("bundle_qualified.json"))
    tampered["manifest"]["coverage_count"] += 1
    contract = _write_invocation(tmp_path, data=json.dumps(tampered).encode("utf-8"))
    assert w.main(["--manifest", str(contract)]) == 5
    capsys.readouterr()


def test_cli_promote_status_codes_and_dsn_never_printed(tmp_path, capsys, monkeypatch):
    store, _ = _validated(tmp_path)
    monkeypatch.setattr(w, "_open_store", _factory(store))
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    pid = str(_pid())
    base = ["--mode", "promote", "--schema", "bond_credit", "--publication-id", pid]
    assert (
        w.main([*base, "--expected-pointer", "none"])
        == w.PromoteNotAuthorized.exit_code
        == 7
    )
    assert w.main([*base, "--confirm-promote", pid]) == 7
    assert (
        w.main(
            [*base, "--expected-pointer", str(uuid.uuid4()), "--confirm-promote", pid]
        )
        == 6
    )
    assert store.pointer is None
    assert w.main([*base, "--expected-pointer", "none", "--confirm-promote", pid]) == 0
    assert store.pointer == _pid()
    assert "hunter2" not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Loader integration (WORKER=bond_default_events through src.run_worker)
# ---------------------------------------------------------------------------
def test_loader_runs_the_worker_in_default_plan_mode(tmp_path, monkeypatch, capsys):
    import src.run_worker as rw

    monkeypatch.setenv("WORKER", "bond_default_events")
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    monkeypatch.setenv(w.ENV_MANIFEST, str(_write_invocation(tmp_path)))
    rw.main()  # returns (exit 0); would raise SystemExit on a non-zero contract
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["worker"] == "bond_default_events" and payload["state"] == "planned"
    assert "hunter2" not in out


def test_loader_propagates_a_refusal_as_a_non_zero_exit(tmp_path, monkeypatch):
    import src.run_worker as rw

    monkeypatch.setenv("WORKER", "bond_default_events")
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    monkeypatch.setenv(w.ENV_MANIFEST, str(_write_invocation(tmp_path, sha="3" * 64)))
    with pytest.raises(w.IntegrityError):
        rw.main()


@pytest.mark.parametrize(
    ("name", "value"), [("WORKER_LIMIT", "5"), ("WORKER_CALC_DATE", "2026-08-31")]
)
def test_loader_refuses_options_the_worker_does_not_take(monkeypatch, name, value):
    import src.run_worker as rw

    monkeypatch.setenv("WORKER", "bond_default_events")
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    monkeypatch.setenv(name, value)
    with pytest.raises(SystemExit):
        rw.main()


def test_run_signature_matches_the_loader_contract():
    import inspect

    params = inspect.signature(w.run).parameters
    assert (
        next(iter(params)) == "dsn"
        and "limit" not in params
        and "calc_date" not in params
    )


# ---------------------------------------------------------------------------
# Disposable PostgreSQL lifecycle (skipped without an explicit loopback test DSN)
# ---------------------------------------------------------------------------
DB_ENV = "BOND_DEFAULT_TEST_DATABASE_URL"
_DB_NAME = re.compile(r"^[a-z0-9_]*disposable[a-z0-9_]*$")


def _disposable_params() -> dict[str, str]:
    raw = os.environ.get(DB_ENV)
    if raw is None:
        pytest.skip(f"{DB_ENV} not set")
    from psycopg.conninfo import conninfo_to_dict

    params = {k: str(v) for k, v in conninfo_to_dict(raw).items() if v is not None}
    unexpected = set(params) - {
        "host",
        "hostaddr",
        "port",
        "dbname",
        "user",
        "password",
    }
    if (
        unexpected
        or params.get("host") not in {"127.0.0.1", "localhost", "::1"}
        or "hostaddr" in params
    ):
        pytest.fail(f"refusing {DB_ENV}: not an explicit loopback DSN")
    if not _DB_NAME.fullmatch(params.get("dbname", "")):
        pytest.fail(f"refusing {DB_ENV}: database name must be disposable")
    return {**params, "sslmode": "disable", "connect_timeout": "10"}


def test_disposable_database_lifecycle_prepare_verify_promote(tmp_path, monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    params = _disposable_params()
    schema = "bdw_" + uuid.uuid4().hex[:12]
    with psycopg.connect(**params, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        pub.install_schema(
            admin, schema
        )  # test-owned DDL; the worker itself never does this
    try:
        monkeypatch.setattr(
            w,
            "_connect",
            lambda dsn: psycopg.connect(**params, options=f"-csearch_path={schema}"),
        )
        invocation = _write_invocation(tmp_path)
        pid = str(_pid())
        kw = {"schema": schema, "env": {}}
        assert (
            w.run("unused", mode="prepare", manifest=invocation, **kw)[
                "prepare_outcome"
            ]
            == "inserted"
        )
        assert (
            w.run("unused", mode="prepare", manifest=invocation, **kw)[
                "prepare_outcome"
            ]
            == "replayed"
        )
        assert (
            w.run(
                "unused", mode="verify", publication_id=pid, manifest=invocation, **kw
            )["state"]
            == "verified"
        )
        with pytest.raises(w.PublicationRefused, match="cas_mismatch"):
            w.run(
                "unused",
                mode="promote",
                publication_id=pid,
                expected_pointer=str(uuid.uuid4()),
                confirm_promote=pid,
                **kw,
            )
        stats = w.run(
            "unused",
            mode="promote",
            publication_id=pid,
            expected_pointer="none",
            confirm_promote=pid,
            **kw,
        )
        assert stats["state"] == "promoted" and stats["pointer"] == pid
    finally:
        with psycopg.connect(**params, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


# ---------------------------------------------------------------------------
# Sources input: frontier manifest -> coverage-only bundle -> diagnostic release
# ---------------------------------------------------------------------------
import importlib.util
import sys

from src.bonds.default_events import source_bundle as sb


def _load_source_helpers():
    name = "bond_default_source_bundle_helpers"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).resolve().parent / "test_bond_default_source_bundle.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


SB = _load_source_helpers()
RELEASE_ID = uuid.UUID("0a0a0a0a-0a0a-8a0a-8a0a-0a0a0a0a0a0a")


class FakeConn:
    def __init__(self):
        self.transactions = 0

    @contextlib.contextmanager
    def transaction(self):
        self.transactions += 1
        yield


class SourceStore(pub.InMemoryPublicationStore):
    """The package's in-memory store plus the ``conn`` attribute the sources path hands to the builder."""

    def __init__(self):
        super().__init__()
        self.conn = FakeConn()


class FakeDiag:
    """Records the diagnostic adapter calls; refusals raise its own ``DiagnosticError``."""

    class DiagnosticError(RuntimeError):
        def __init__(self, reason, detail=""):
            super().__init__(reason)
            self.reason = reason
            self.code = reason
            self.detail = detail

    def __init__(self, events):
        self.events = events
        self.pointer = None
        self.releases = {}

    def prepare_diagnostic(
        self,
        conn,
        *,
        schema,
        publication_id,
        projection,
        source_frontier_manifest_digest,
    ):
        self.events.append(("diag.prepare", schema, publication_id))
        assert source_frontier_manifest_digest.startswith("sha256:")
        self.releases[RELEASE_ID] = publication_id
        return RELEASE_ID

    def verify_diagnostic_report(self, conn, *, schema, release_id):
        self.events.append(("diag.verify", release_id))
        if release_id not in self.releases:
            raise self.DiagnosticError("unknown_release")
        return {
            "release_id": release_id,
            "publication_id": self.releases[release_id],
            "projection_digest": "sha256:" + "5" * 64,
            "target_month": SB.T.isoformat(),
            "knowledge_cutoff": SB.K.isoformat(),
            "is_current": self.pointer == release_id,
            "counts": {"coverage_cells": 360, "source_frontiers": 6},
            "projection": {"must_not": "be printed"},
        }

    def promote_diagnostic(self, conn, *, schema, release_id, expected_release_id):
        self.events.append(("diag.promote", release_id, expected_release_id))
        if expected_release_id != self.pointer:
            raise self.DiagnosticError("cas_mismatch")
        self.pointer = release_id


class SourcesRig:
    """A complete pure rig: real manifest, real composition on a fake panel, injected DB seams."""

    def __init__(self, tmp_path, monkeypatch, *, counts=None):
        self.events = []
        self.tmp = tmp_path
        self.counts = counts or SB.varied_counts()
        self.panel_id = SB.PROD_PANEL_ID
        self.doc = SB.manifest_for(self.panel_id)
        self.frontier_bytes = (
            json.dumps(self.doc, sort_keys=True, indent=2) + "\n"
        ).encode()
        (tmp_path / "frontier.json").write_bytes(self.frontier_bytes)
        self.bundle = SB.compose(SB.fake_panel(self.counts, self.panel_id), self.doc)
        self.store = SourceStore()
        self.diag = FakeDiag(self.events)
        self.builder_calls = []
        monkeypatch.setattr(w, "_utcnow", lambda: SB.K + dt.timedelta(minutes=1))
        monkeypatch.setattr(
            pub, "prepare_bundle", self._traced("credit.prepare", pub.prepare_bundle)
        )
        monkeypatch.setattr(
            pub, "validate_bundle", self._traced("credit.validate", pub.validate_bundle)
        )

    def _traced(self, name, fn):
        def wrapper(*args, **kwargs):
            self.events.append((name,))
            return fn(*args, **kwargs)

        return wrapper

    def builder(self, document, **kwargs):
        self.builder_calls.append(kwargs)
        self.events.append(("build",))
        return self.bundle

    def collab(self, **overrides):
        args = {
            "builder": self.builder,
            "projection_builder": lambda bundle, document: (
                self.events.append(("projection",)) or "projection"
            ),
            "frontier_digest_builder": lambda bundle: "sha256:" + "6" * 64,
            "code_digest": lambda: SB.CODE_DIGEST,
            "diagnostic": self.diag,
        }
        args.update(overrides)
        return w.Collaborators(**args)

    def invocation(
        self, *, publication_id="derive", frontier_sha=None, mutate=None, **top
    ):
        m = self.bundle.manifest
        doc = {
            "invocation_version": w.INVOCATION_VERSION,
            "product": c.PRODUCT,
            "contract_version": c.CONTRACT_VERSION,
            "input": {
                "kind": "sources",
                "source_manifest": {
                    "path": "frontier.json",
                    "sha256": frontier_sha
                    or hashlib.sha256(self.frontier_bytes).hexdigest(),
                },
                "expected_panel_publication_id": str(self.panel_id),
                "code_digest": SB.CODE_DIGEST,
            },
            "expected": {
                "publication_id": str(self.bundle.publication_id)
                if publication_id == "derive"
                else publication_id,
                "target_month": m["target_month"].isoformat(),
                "knowledge_cutoff": m["knowledge_cutoff"]
                .astimezone(dt.timezone.utc)
                .isoformat(),
                "quality_state": "partial",
                "build_scope": "limited",
            },
        }
        doc.update(top)
        if mutate:
            mutate(doc)
        path = self.tmp / "sources_invocation.json"
        path.write_text(json.dumps(doc), encoding="utf-8")
        return path

    def run(self, **kwargs):
        kwargs.setdefault("dsn", SECRET_DSN)
        kwargs.setdefault("schema", "public")
        kwargs.setdefault("env", {})
        kwargs.setdefault("collaborators", self.collab())
        return w.run(store_factory=_factory(self.store), **kwargs)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    return SourcesRig(tmp_path, monkeypatch)


def test_sources_plan_is_offline_and_validates_the_frontier_manifest(rig):
    def factory(dsn, schema):
        raise AssertionError("plan without read_panel must not open a store")

    stats = w.run(
        None,
        manifest=rig.invocation(),
        store_factory=factory,
        env={},
        collaborators=rig.collab(),
    )
    assert stats["state"] == "planned" and stats["input_kind"] == "sources"
    assert stats["database"] == "not_contacted" and stats["network"] == "not_used"
    assert stats["frontier_records"] == 6 and stats["max_grid_rows"] == 1_200_000
    assert stats["panel_publication_id"] == str(rig.panel_id)
    assert stats["source_manifest_digest"] == rig.doc["digest"]
    assert rig.builder_calls == [] and rig.events == []


def test_sources_plan_does_not_resolve_a_dsn_eagerly(rig, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "not-a-url")
    monkeypatch.setattr(w, "_open_store", lambda *a, **k: pytest.fail("store opened"))
    stats = w.run(
        manifest=rig.invocation(),
        env={},
        collaborators=rig.collab(),
    )
    assert stats["state"] == "planned"


def test_sources_plan_may_omit_the_derived_publication_pin(rig):
    stats = w.run(
        None,
        manifest=rig.invocation(publication_id=None),
        env={},
        collaborators=rig.collab(),
    )
    assert stats["pinned_publication_id"] is None


def test_sources_plan_with_read_panel_reads_read_only_and_persists_nothing(rig):
    stats = rig.run(mode="plan", manifest=rig.invocation(), read_panel=True)
    assert stats["database"] == "read_only" and stats["state"] == "planned"
    assert stats["panel_grid_count"] == sum(rig.counts.values())
    assert (
        stats["rating_rows"] == 2 * sum(rig.counts.values())
        and stats["coverage_cells"] == 360
    )
    assert stats["publication_id"] == str(rig.bundle.publication_id)
    assert rig.events == [("build",)] and rig.store.publications == {}
    kwargs = rig.builder_calls[0]
    assert kwargs["expected_panel_publication_id"] == rig.panel_id
    assert (
        kwargs["knowledge_mode"] == "current_run"
        and kwargs["code_digest"] == SB.CODE_DIGEST
    )
    assert kwargs["conn"] is rig.store.conn


def test_sources_plan_read_panel_needs_a_dsn_and_a_schema_and_is_plan_only(rig):
    with pytest.raises(w.ConfigError, match="dsn_required"):
        w.run(
            None,
            manifest=rig.invocation(),
            read_panel=True,
            env={},
            collaborators=rig.collab(),
        )
    with pytest.raises(w.ConfigError, match="schema_required"):
        w.run(
            SECRET_DSN,
            manifest=rig.invocation(),
            read_panel=True,
            env={},
            collaborators=rig.collab(),
        )
    with pytest.raises(w.ConfigError, match="read_panel_only_in_plan_mode"):
        rig.run(mode="prepare", manifest=rig.invocation(), read_panel=True)
    env = {w.ENV_PLAN_READ_PANEL: "1"}
    stats = rig.run(mode="plan", manifest=rig.invocation(), env=env)
    assert stats["database"] == "read_only"


def test_read_panel_is_refused_for_a_bundle_input(tmp_path):
    with pytest.raises(w.ConfigError, match="read_panel_requires_sources_input"):
        _run(
            pub.InMemoryPublicationStore(),
            mode="plan",
            manifest=_write_invocation(tmp_path),
            read_panel=True,
            env={},
        )


def test_sources_prepare_is_one_credit_prepare_one_validate_one_diagnostic_prepare_no_election(
    rig,
):
    stats = rig.run(mode="prepare", manifest=rig.invocation())
    assert [e[0] for e in rig.events] == [
        "build",
        "projection",
        "credit.prepare",
        "credit.validate",
        "diag.prepare",
        "diag.verify",
    ]
    assert stats["state"] == "prepared" and stats["prepare_outcome"] == "inserted"
    assert stats["lifecycle_state"] == "validated"
    assert (
        stats["release_id"] == str(RELEASE_ID)
        and stats["diagnostic_release_verified"] is True
    )
    assert (
        stats["credit_pointer_elected"] is False
        and stats["diagnostic_pointer_elected"] is False
    )
    assert rig.store.pointer is None and rig.diag.pointer is None
    assert len(rig.store.publications) == 1
    assert rig.store.state(rig.bundle.publication_id).lifecycle_state == "validated"
    assert stats["publication_id"] == str(rig.bundle.publication_id)
    assert stats["panel_grid_digest"] == rig.bundle.manifest["panel_grid_digest"]
    assert stats["rating_rows"] == 2 * sum(rig.counts.values())
    assert stats["diagnostic_release"]["counts"] == {
        "coverage_cells": 360,
        "source_frontiers": 6,
    }
    assert "projection" not in stats[
        "diagnostic_release"
    ] and "must_not" not in json.dumps(stats)
    assert (
        rig.store.conn.transactions == 2
    )  # one diagnostic prepare + one diagnostic verify
    assert "hunter2" not in json.dumps(stats, default=str)


def test_sources_prepare_never_dispatches_a_promotion(rig, monkeypatch):
    monkeypatch.setattr(
        pub, "promote_bundle", lambda *a, **k: pytest.fail("credit promote called")
    )
    rig.diag.promote_diagnostic = lambda *a, **k: pytest.fail(
        "diagnostic promote called"
    )
    rig.run(mode="prepare", manifest=rig.invocation())
    assert not any(e[0].endswith("promote") for e in rig.events)


def test_sources_prepare_replays_exactly(rig):
    first = rig.run(mode="prepare", manifest=rig.invocation())
    again = rig.run(mode="prepare", manifest=rig.invocation())
    assert (first["prepare_outcome"], again["prepare_outcome"]) == (
        "inserted",
        "replayed",
    )
    assert (
        len(rig.store.publications) == 1 and again["release_id"] == first["release_id"]
    )


def test_sources_prepare_requires_the_pinned_publication_id(rig):
    with pytest.raises(w.ConfigError, match="publication_id_pin_required"):
        rig.run(mode="prepare", manifest=rig.invocation(publication_id=None))
    assert rig.events == []


def test_sources_prepare_refuses_a_pinned_identity_mismatch_before_any_write(rig):
    wrong = "00000000-0000-8000-8000-000000000000"
    with pytest.raises(w.IntegrityError, match="bundle_publication_id_mismatch"):
        rig.run(mode="prepare", manifest=rig.invocation(publication_id=wrong))
    assert rig.store.publications == {} and ("credit.prepare",) not in rig.events


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda d: d["expected"].update(quality_state="qualified"),
            "sources_quality_scope_mismatch",
        ),
        (
            lambda d: d["expected"].update(build_scope="complete"),
            "sources_quality_scope_mismatch",
        ),
        (
            lambda d: d["expected"].update(target_month="2026-07-01"),
            "manifest_target_month_mismatch",
        ),
        (
            lambda d: d["expected"].update(
                knowledge_cutoff="2026-09-25T00:00:00+00:00"
            ),
            "manifest_observed_after_cutoff",
        ),
        (
            lambda d: d["input"].update(
                expected_panel_publication_id=str(uuid.uuid4())
            ),
            "manifest_panel_publication_mismatch",
        ),
    ],
)
def test_sources_prepare_refuses_pin_or_cutoff_violations_before_the_database(
    rig, mutate, code
):
    def factory(dsn, schema):
        raise AssertionError(
            "the store must not be opened before the offline gate passes"
        )

    with pytest.raises(w.IntegrityError, match=code):
        w.run(
            SECRET_DSN,
            mode="prepare",
            schema="public",
            manifest=rig.invocation(mutate=mutate),
            store_factory=factory,
            env={},
            collaborators=rig.collab(),
        )
    assert rig.events == []


def test_sources_prepare_refuses_a_corrupt_or_swapped_frontier_manifest(rig):
    with pytest.raises(w.IntegrityError, match="source_manifest_sha256_mismatch"):
        rig.run(mode="prepare", manifest=rig.invocation(frontier_sha="1" * 64))
    (rig.tmp / "frontier.json").write_bytes(rig.frontier_bytes + b" ")
    with pytest.raises(w.IntegrityError, match="source_manifest_sha256_mismatch"):
        rig.run(mode="prepare", manifest=rig.invocation())
    bad = b'{"digest": "x", "digest": "y"}'
    (rig.tmp / "frontier.json").write_bytes(bad)
    with pytest.raises(w.IntegrityError, match="source_manifest_not_strict_json"):
        rig.run(
            mode="prepare",
            manifest=rig.invocation(frontier_sha=hashlib.sha256(bad).hexdigest()),
        )
    (rig.tmp / "frontier.json").unlink()
    with pytest.raises(w.IntegrityError, match="source_manifest_unreadable"):
        rig.run(mode="prepare", manifest=rig.invocation(frontier_sha="2" * 64))
    assert rig.events == []


def test_sources_invocation_shape_is_strict(rig):
    for mutate, code in (
        (lambda d: d["input"].pop("code_digest"), "fields_mismatch|unexpected|missing"),
        (lambda d: d["input"].update(code_digest="abc"), "code_digest_expected"),
        (lambda d: d["input"].update(extra=1), "fields_mismatch|unexpected|missing"),
        (
            lambda d: d["input"]["source_manifest"].update(sha256="short"),
            "input_sha256_expected",
        ),
        (
            lambda d: d["input"].update(expected_panel_publication_id="nope"),
            "uuid_expected",
        ),
    ):
        with pytest.raises(w.InvocationError, match=code):
            rig.run(mode="plan", manifest=rig.invocation(mutate=mutate))


def test_sources_prepare_fails_before_any_read_when_the_diagnostic_layer_is_missing(
    rig, monkeypatch
):
    monkeypatch.setitem(
        sys.modules, "src.bonds.default_events.diagnostic_publication", None
    )
    with pytest.raises(
        w.OrchestrationUnavailable, match="diagnostic_layer_unavailable"
    ):
        rig.run(
            mode="prepare",
            manifest=rig.invocation(),
            collaborators=rig.collab(diagnostic=None),
        )
    assert rig.events == [] and rig.store.publications == {}


def test_sources_builder_refusals_are_typed(rig):
    def bounded(document, **kwargs):
        raise sb.BoundsExceeded("grid_rows_exceed_bound", "grid=9 max=1")

    with pytest.raises(w.IntegrityError, match="grid_rows_exceed_bound"):
        rig.run(
            mode="prepare",
            manifest=rig.invocation(),
            collaborators=rig.collab(builder=bounded),
        )
    assert rig.store.publications == {}

    def drift(document, **kwargs):
        raise sb.PanelReadError("panel_pointer_mismatch", "rows=1")

    with pytest.raises(w.IntegrityError, match="panel_pointer_mismatch"):
        rig.run(
            mode="plan",
            manifest=rig.invocation(),
            read_panel=True,
            collaborators=rig.collab(builder=drift),
        )


def test_sources_driver_errors_never_echo_parameters(rig):
    psycopg = pytest.importorskip("psycopg")

    def broken(document, **kwargs):
        raise psycopg.OperationalError(f"connection to {SECRET_DSN} failed")

    with pytest.raises(w.InfrastructureError, match="panel_read_failed") as caught:
        rig.run(
            mode="prepare",
            manifest=rig.invocation(),
            collaborators=rig.collab(builder=broken),
        )
    assert "hunter2" not in str(caught.value) and caught.value.__cause__ is None


def test_sources_diagnostic_refusal_is_a_typed_publication_refusal(rig):
    def refuse(*args, **kwargs):
        raise FakeDiag.DiagnosticError("projection_mismatch", "detail")

    rig.diag.prepare_diagnostic = refuse
    with pytest.raises(w.PublicationRefused, match="projection_mismatch"):
        rig.run(mode="prepare", manifest=rig.invocation())
    assert rig.store.pointer is None and rig.diag.pointer is None


# ---------------------------------------------------------------------------
# verify --release-id, diagnostic-promote and the qualified/diagnostic separation
# ---------------------------------------------------------------------------
def _rig_prepared(rig):
    rig.run(mode="prepare", manifest=rig.invocation())
    rig.events.clear()
    return rig.bundle.publication_id


def test_verify_with_a_release_id_reverifies_by_frozen_ids_and_never_promotes(
    rig, monkeypatch
):
    pid = _rig_prepared(rig)
    monkeypatch.setattr(
        pub, "promote_bundle", lambda *a, **k: pytest.fail("promote called")
    )
    stats = rig.run(mode="verify", publication_id=str(pid), release_id=str(RELEASE_ID))
    assert stats["state"] == "verified" and stats["release_id"] == str(RELEASE_ID)
    assert (
        stats["diagnostic_release_verified"] is True
        and stats["diagnostic_release"]["is_current"] is False
    )
    assert ("diag.verify", RELEASE_ID) in rig.events and not any(
        e[0] == "diag.promote" for e in rig.events
    )
    plain = rig.run(mode="verify", publication_id=str(pid))
    assert "release_id" not in plain


def test_verify_refuses_an_unknown_release_id(rig):
    pid = _rig_prepared(rig)
    with pytest.raises(w.PublicationRefused, match="unknown_release"):
        rig.run(mode="verify", publication_id=str(pid), release_id=str(uuid.uuid4()))


@pytest.mark.parametrize("mode", ["plan", "prepare", "promote"])
def test_a_release_id_can_never_reach_plan_prepare_or_qualified_promotion(rig, mode):
    kwargs = {"release_id": str(RELEASE_ID)}
    if mode == "promote":
        kwargs.update(
            publication_id=str(rig.bundle.publication_id),
            expected_pointer="none",
            confirm_promote=str(rig.bundle.publication_id),
        )
    else:
        kwargs["manifest"] = rig.invocation()
    with pytest.raises(w.ConfigError, match="release_id_not_applicable"):
        rig.run(mode=mode, **kwargs)
    assert rig.events == []


def test_qualified_promote_by_a_release_id_used_as_publication_id_is_refused(
    rig, monkeypatch
):
    _rig_prepared(rig)
    monkeypatch.setattr(
        pub, "promote_bundle", lambda *a, **k: pytest.fail("promote called")
    )
    with pytest.raises(w.PublicationRefused):
        rig.run(
            mode="promote",
            publication_id=str(RELEASE_ID),
            expected_pointer="none",
            confirm_promote=str(RELEASE_ID),
        )
    assert rig.store.pointer is None and rig.diag.pointer is None


def test_a_partial_limited_credit_build_is_never_qualified_promotable(rig):
    pid = _rig_prepared(rig)
    with pytest.raises(w.PublicationRefused, match="not_qualified_complete"):
        rig.run(
            mode="promote",
            publication_id=str(pid),
            expected_pointer="none",
            confirm_promote=str(pid),
        )
    assert rig.store.pointer is None


def _dpromote(rig, **overrides):
    kwargs = {
        "mode": "diagnostic-promote",
        "release_id": str(RELEASE_ID),
        "expected_diagnostic_pointer": "none",
        "confirm_diagnostic_promote": str(RELEASE_ID),
    }
    kwargs.update(overrides)
    return rig.run(**kwargs)


def test_diagnostic_promote_needs_release_pointer_expectation_and_confirmation(rig):
    _rig_prepared(rig)
    with pytest.raises(w.ConfigError, match="release_id_required"):
        rig.run(
            mode="diagnostic-promote",
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(RELEASE_ID),
        )
    with pytest.raises(w.PromoteNotAuthorized, match="expected_pointer_required"):
        rig.run(
            mode="diagnostic-promote",
            release_id=str(RELEASE_ID),
            confirm_diagnostic_promote=str(RELEASE_ID),
        )
    with pytest.raises(w.PromoteNotAuthorized, match="confirmation_required"):
        _dpromote(rig, confirm_diagnostic_promote=None)
    with pytest.raises(w.PromoteNotAuthorized, match="confirmation_required"):
        _dpromote(rig, confirm_diagnostic_promote=str(uuid.uuid4()))
    with pytest.raises(w.ConfigError, match="uuid_expected"):
        _dpromote(rig, expected_diagnostic_pointer="latest")
    assert rig.diag.pointer is None and not any(
        e[0] == "diag.promote" for e in rig.events
    )


def test_diagnostic_promote_sets_only_the_diagnostic_pointer(rig, monkeypatch):
    _rig_prepared(rig)
    monkeypatch.setattr(
        pub, "promote_bundle", lambda *a, **k: pytest.fail("credit promote called")
    )
    stats = _dpromote(rig)
    assert stats["state"] == "diagnostic_promoted" and stats["release_id"] == str(
        RELEASE_ID
    )
    assert (
        stats["expected_diagnostic_pointer"] is None
        and stats["qualified_pointer_touched"] is False
    )
    assert rig.diag.pointer == RELEASE_ID and rig.store.pointer is None
    assert [e[0] for e in rig.events] == ["diag.verify", "diag.promote"]
    assert rig.events[-1] == ("diag.promote", RELEASE_ID, None)
    again = uuid.UUID("1b1b1b1b-1b1b-8b1b-8b1b-1b1b1b1b1b1b")
    with pytest.raises(w.PublicationRefused, match="cas_mismatch"):
        _dpromote(rig, expected_diagnostic_pointer=str(again))
    with pytest.raises(w.PublicationRefused, match="cas_mismatch"):
        _dpromote(rig, expected_diagnostic_pointer="none")
    assert rig.diag.pointer == RELEASE_ID


def test_diagnostic_promote_refuses_qualified_options_and_a_publication_id(rig):
    _rig_prepared(rig)
    with pytest.raises(w.ConfigError, match="promote_options_outside_promote_mode"):
        _dpromote(rig, expected_pointer="none")
    with pytest.raises(w.ConfigError, match="promote_options_outside_promote_mode"):
        _dpromote(rig, confirm_promote=str(RELEASE_ID))
    with pytest.raises(w.ConfigError, match="publication_id_not_applicable"):
        _dpromote(rig, publication_id=str(rig.bundle.publication_id))
    assert rig.diag.pointer is None


@pytest.mark.parametrize("mode", ["plan", "prepare", "verify", "promote"])
def test_diagnostic_promote_options_are_rejected_in_every_other_mode(rig, mode):
    kwargs = {"expected_diagnostic_pointer": "none"}
    if mode in ("plan", "prepare"):
        kwargs["manifest"] = rig.invocation()
    else:
        kwargs["publication_id"] = str(rig.bundle.publication_id)
    with pytest.raises(w.ConfigError, match="diagnostic_promote_options_outside_mode"):
        rig.run(mode=mode, **kwargs)
    with pytest.raises(w.ConfigError, match="diagnostic_promote_options_outside_mode"):
        rig.run(
            mode=mode,
            confirm_diagnostic_promote=str(RELEASE_ID),
            **{k: v for k, v in kwargs.items() if k != "expected_diagnostic_pointer"},
        )


def test_diagnostic_promote_needs_the_adapter_and_a_dsn_and_schema(rig, monkeypatch):
    with pytest.raises(w.ConfigError, match="dsn_required"):
        w.run(
            None,
            mode="diagnostic-promote",
            release_id=str(RELEASE_ID),
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(RELEASE_ID),
            env={},
        )
    with pytest.raises(w.ConfigError, match="schema_required"):
        w.run(
            SECRET_DSN,
            mode="diagnostic-promote",
            release_id=str(RELEASE_ID),
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(RELEASE_ID),
            env={},
        )
    monkeypatch.setitem(
        sys.modules, "src.bonds.default_events.diagnostic_publication", None
    )
    with pytest.raises(
        w.OrchestrationUnavailable, match="diagnostic_layer_unavailable"
    ):
        rig.run(
            mode="diagnostic-promote",
            release_id=str(RELEASE_ID),
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(RELEASE_ID),
            collaborators=rig.collab(diagnostic=None),
        )


def test_diagnostic_promote_environment_configuration(rig):
    _rig_prepared(rig)
    env = {
        w.ENV_MODE: "diagnostic-promote",
        w.ENV_RELEASE_ID: str(RELEASE_ID),
        w.ENV_EXPECTED_DIAGNOSTIC_POINTER: "none",
        w.ENV_CONFIRM_DIAGNOSTIC_PROMOTE: str(RELEASE_ID),
    }
    stats = rig.run(env=env)
    assert stats["state"] == "diagnostic_promoted" and rig.diag.pointer == RELEASE_ID


def test_cli_diagnostic_promote_exit_codes_and_secrets(rig, monkeypatch, capsys):
    _rig_prepared(rig)
    monkeypatch.setattr(w, "_open_store", _factory(rig.store))
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    collab = rig.collab()
    monkeypatch.setattr(w, "Collaborators", lambda: collab)
    base = [
        "--mode",
        "diagnostic-promote",
        "--schema",
        "public",
        "--release-id",
        str(RELEASE_ID),
    ]
    assert w.main(base) == w.PromoteNotAuthorized.exit_code == 7
    assert w.main([*base, "--expected-diagnostic-pointer", "none"]) == 7
    assert rig.diag.pointer is None
    ok = [
        *base,
        "--expected-diagnostic-pointer",
        "none",
        "--confirm-diagnostic-promote",
        str(RELEASE_ID),
    ]
    assert w.main(ok) == 0
    assert rig.diag.pointer == RELEASE_ID
    assert w.main(ok) == w.PublicationRefused.exit_code == 6
    assert (
        w.main(
            [*base, "--expected-diagnostic-pointer", "none", "--confirm-promote", "x"]
        )
        == 2
    )
    assert "hunter2" not in capsys.readouterr().out


def test_cli_sources_prepare_prints_sanitized_json_only(rig, monkeypatch, capsys):
    monkeypatch.setattr(w, "_open_store", _factory(rig.store))
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    collab = rig.collab()
    monkeypatch.setattr(w, "Collaborators", lambda: collab)
    code = w.main(
        ["--mode", "prepare", "--schema", "public", "--manifest", str(rig.invocation())]
    )
    out = capsys.readouterr().out
    assert code == 0 and "hunter2" not in out and "db.example.invalid" not in out
    payload = json.loads(out)
    assert payload["state"] == "prepared" and payload["release_id"] == str(RELEASE_ID)


def test_loader_reads_sources_plan_without_a_database(rig, monkeypatch, capsys):
    import src.run_worker as rw

    monkeypatch.setenv("WORKER", "bond_default_events")
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    monkeypatch.setenv(w.ENV_MANIFEST, str(rig.invocation()))
    monkeypatch.setattr(w, "_open_store", lambda *a, **k: pytest.fail("store opened"))
    rw.main()
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "planned" and payload["input_kind"] == "sources"


def test_worker_module_never_reads_pickles_or_the_environment_file():
    text = Path(w.__file__).read_text(encoding="utf-8")
    assert "pickle" not in text and "dotenv" not in text and "open(" not in text
    tree = ast.parse(text)
    imported = {
        a.name.split(".")[0]
        for n in ast.walk(tree)
        if isinstance(n, ast.Import)
        for a in n.names
    }
    assert not imported & {
        "pickle",
        "shelve",
        "marshal",
        "requests",
        "urllib",
        "httpx",
        "socket",
        "subprocess",
    }


def test_railway_service_config_documents_the_sources_contract():
    toml = (
        Path(__file__).resolve().parents[1] / "railway.bond-default-events.toml"
    ).read_text(encoding="utf-8")
    for name in (
        w.ENV_RELEASE_ID,
        w.ENV_EXPECTED_DIAGNOSTIC_POINTER,
        w.ENV_CONFIRM_DIAGNOSTIC_PROMOTE,
        w.ENV_PLAN_READ_PANEL,
        "diagnostic-promote",
    ):
        assert name in toml
    assert "cronSchedule" not in toml and 'restartPolicyType = "never"' in toml
    assert re.search(
        r'^startCommand = "python -m src\.(run_worker|workers\.bond_default_events)"$',
        toml,
        re.MULTILINE,
    )
    assert not re.search(r"^\s*cronSchedule", toml, re.MULTILINE)


def test_mode_set_is_closed():
    assert w.MODES == (
        "plan",
        "prepare",
        "verify",
        "promote",
        "diagnostic-promote",
        "install-schema",
    )


# ---------------------------------------------------------------------------
# Real end-to-end on a production-shaped disposable database (roles, diagnostic SQL, panel tables)
# ---------------------------------------------------------------------------
def _diagnostic_db_module():
    name = "_bde_worker_diagnostic_db"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name,
        Path(__file__).resolve().parent
        / "test_bond_default_diagnostic_publication_db.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _bare_db(diagdb, target):
    """A throwaway database with the production roles and default privileges but NO bond objects."""
    from types import SimpleNamespace

    from psycopg import sql

    dbname = f"bdefault_disposable_wi_{uuid.uuid4().hex[:8]}"
    passwords = diagdb.ROLE_PASSWORDS
    with diagdb.connect_disposable(target, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        for role in ("worker_writer", "app_runtime", "app_analytics_ro"):
            diagdb._ensure_role(conn, role, login=True, password=passwords[role])
        for role in diagdb.GROUP_ROLES:
            diagdb._ensure_role(conn, role, login=False, password=None)
    admin_target = target.derive(dbname=dbname)
    with diagdb.connect_disposable(admin_target, autocommit=True) as conn:
        conn.execute("GRANT USAGE, CREATE ON SCHEMA public TO worker_writer")
        conn.execute(
            "GRANT USAGE ON SCHEMA public TO bond_credit_reader, bond_credit_writer, bond_credit_auditor, "
            "bond_default_diagnostic_reader, app_runtime"
        )
        conn.execute("GRANT bond_credit_writer TO worker_writer")
        conn.execute("GRANT bond_default_diagnostic_reader TO app_runtime")
        for grant in (
            "GRANT INSERT, SELECT, UPDATE, DELETE ON TABLES TO app_runtime",
            "GRANT SELECT ON TABLES TO app_analytics_ro",
            "GRANT EXECUTE ON FUNCTIONS TO app_runtime",
        ):  # the measured production default-privilege leaks the hardening must close
            conn.execute(
                f"ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA public {grant}"
            )
    writer_target = admin_target.derive(
        user="worker_writer", password=passwords["worker_writer"]
    )
    app_target = admin_target.derive(
        user="app_runtime", password=passwords["app_runtime"]
    )
    return SimpleNamespace(
        dbname=dbname,
        admin=lambda **kw: diagdb.connect_disposable(admin_target, **kw),
        writer=lambda **kw: diagdb.connect_disposable(writer_target, **kw),
        app=lambda **kw: diagdb.connect_disposable(app_target, **kw),
    )


def _install_kwargs(db, **overrides):
    kwargs = {
        "mode": "install-schema",
        "schema": "public",
        "confirm_install": w.INSTALL_CONFIRMATION,
        "env": {},
        "connect_factory": lambda dsn: db.writer(autocommit=True),
    }
    kwargs.update(overrides)
    return kwargs


@pytest.fixture(scope="module")
def bare_target():
    raw = os.environ.get(DB_ENV)
    if raw is None:
        pytest.skip(f"{DB_ENV} not set")
    pytest.importorskip("psycopg")
    diagdb = _diagnostic_db_module()
    try:
        target = diagdb.guard.check_test_dsn(raw)
    except diagdb.guard.UnsafeTestDsn as exc:
        pytest.fail(f"refusing {DB_ENV}: {exc}")
    return diagdb, target


@pytest.fixture
def bare_db(bare_target):
    diagdb, target = bare_target
    env = _bare_db(diagdb, target)
    try:
        yield env
    finally:
        diagdb.drop_env(target, env)


@pytest.fixture(scope="module")
def prod_like(bare_target):
    """A production-shaped database installed THROUGH the worker's install-schema mode, plus the three
    panel tables (test-owned minimal fixture built from the production DDL text)."""
    diagdb, target = bare_target
    env = _bare_db(diagdb, target)
    try:
        with env.admin(autocommit=True) as admin:
            SB.install_panel_tables(admin)
            admin.execute(
                "GRANT SELECT ON public.bond_panel_app_pointer, public.bond_panel_publications, "
                "public.bond_panel_snapshot TO worker_writer"
            )
        env.install_report = w.run("unused", **_install_kwargs(env))
        yield diagdb, env
    finally:
        diagdb.drop_env(target, env)
        from psycopg import sql as _sql

        with diagdb.connect_disposable(target, autocommit=True) as admin:
            for role in ("worker_writer", "app_runtime", "app_analytics_ro"):
                with contextlib.suppress(Exception):
                    admin.execute(
                        _sql.SQL("DROP ROLE IF EXISTS {}").format(_sql.Identifier(role))
                    )


def test_real_database_prepare_verify_promote_diagnostic_end_to_end(
    prod_like, tmp_path, monkeypatch
):
    _diagdb, env = prod_like
    REAL_CODE_DIGEST = sb.code_digest()
    counts = SB.varied_counts()
    with env.admin() as admin:
        panel_id = SB.seed_flat_panel(admin, counts, excluded_every=3)
    doc = SB.manifest_for(panel_id)
    frontier = (json.dumps(doc, sort_keys=True, indent=2) + "\n").encode()
    (tmp_path / "frontier.json").write_bytes(frontier)
    with (
        env.writer() as reader
    ):  # learn the derived publication id (READ ONLY panel read)
        bundle = sb.build_bundle_from_sources(
            doc,
            conn=reader,
            target_month=SB.T,
            knowledge_cutoff=SB.K,
            expected_panel_publication_id=panel_id,
            code_digest=REAL_CODE_DIGEST,
        )
    pid = bundle.publication_id
    invocation = {
        "invocation_version": w.INVOCATION_VERSION,
        "product": c.PRODUCT,
        "contract_version": c.CONTRACT_VERSION,
        "input": {
            "kind": "sources",
            "source_manifest": {
                "path": "frontier.json",
                "sha256": hashlib.sha256(frontier).hexdigest(),
            },
            "expected_panel_publication_id": str(panel_id),
            "code_digest": REAL_CODE_DIGEST,
        },
        "expected": {
            "publication_id": str(pid),
            "target_month": SB.T.isoformat(),
            "knowledge_cutoff": SB.K.isoformat(),
            "quality_state": "partial",
            "build_scope": "limited",
        },
    }
    path = tmp_path / "invocation.json"
    path.write_text(json.dumps(invocation), encoding="utf-8")
    monkeypatch.setattr(w, "_connect", lambda dsn: env.writer())
    kw = {"schema": "public", "env": {}}

    planned = w.run("unused", mode="plan", manifest=path, read_panel=True, **kw)
    assert planned["database"] == "read_only" and planned["publication_id"] == str(pid)
    assert (
        planned["code_digest"] == REAL_CODE_DIGEST
        and planned["code_digest_matches"] is True
    )
    report = env.install_report
    assert (
        report["state"] == "installed"
        and report["checks_ok"] is True
        and report["owner"] == "worker_writer"
    )
    with env.admin() as admin:
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_credit_publications"
            ).fetchone()[0]
            == 0
        )

    first = w.run("unused", mode="prepare", manifest=path, **kw)
    assert first["state"] == "prepared" and first["prepare_outcome"] == "inserted"
    assert (
        first["lifecycle_state"] == "validated"
        and first["credit_pointer_elected"] is False
    )
    release_id = uuid.UUID(first["release_id"])
    assert first["diagnostic_release"]["release_id"] == str(release_id)
    assert first["diagnostic_release"]["counts"]["coverage_cells"] == 360
    assert first["rating_rows"] == 2 * sum(counts.values())
    with env.admin() as admin:
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_credit_current_pointer"
            ).fetchone()[0]
            == 0
        )
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_default_diagnostic_pointer"
            ).fetchone()[0]
            == 0
        )
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_default_diagnostic_releases"
            ).fetchone()[0]
            == 1
        )
        assert admin.execute(
            "SELECT lifecycle_state, quality_state, build_scope FROM public.bond_credit_publications"
        ).fetchone() == ("validated", "partial", "limited")
    with env.app() as app, pytest.raises(Exception) as unavailable:
        app.execute("SELECT public.bond_default_current_diagnostic_release()")
    assert "diagnostic_not_published" in str(unavailable.value)

    again = w.run("unused", mode="prepare", manifest=path, **kw)
    assert (
        again["prepare_outcome"] == "replayed"
        and again["release_id"] == first["release_id"]
    )

    verified = w.run(
        "unused",
        mode="verify",
        publication_id=str(pid),
        release_id=str(release_id),
        manifest=path,
        **kw,
    )
    assert (
        verified["state"] == "verified"
        and verified["diagnostic_release"]["is_current"] is False
    )

    other_release = uuid.UUID("5e5e5e5e-5e5e-8e5e-8e5e-5e5e5e5e5e5e")
    with pytest.raises(w.PublicationRefused):  # an unknown release id
        w.run(
            "unused",
            mode="verify",
            publication_id=str(pid),
            release_id=str(other_release),
            **kw,
        )
    with pytest.raises(w.ConfigError, match="schema_must_be_public"):
        w.run("unused", mode="prepare", manifest=path, schema="bond_credit", env={})
    with pytest.raises(
        w.PublicationRefused
    ):  # a partial/limited build is never qualified-promotable
        w.run(
            "unused",
            mode="promote",
            publication_id=str(pid),
            expected_pointer="none",
            confirm_promote=str(pid),
            **kw,
        )
    with pytest.raises(w.PublicationRefused):  # a release id is never a publication id
        w.run(
            "unused",
            mode="promote",
            publication_id=str(release_id),
            expected_pointer="none",
            confirm_promote=str(release_id),
            **kw,
        )
    with pytest.raises(w.PromoteNotAuthorized, match="confirmation_required"):
        w.run(
            "unused",
            mode="diagnostic-promote",
            release_id=str(release_id),
            expected_diagnostic_pointer="none",
            **kw,
        )

    wrong_k = json.loads(path.read_text(encoding="utf-8"))
    wrong_k["expected"]["knowledge_cutoff"] = "2026-09-29T18:00:01+00:00"
    wrong_path = tmp_path / "invocation_wrong_k.json"
    wrong_path.write_text(json.dumps(wrong_k), encoding="utf-8")
    with pytest.raises(w.IntegrityError, match="release_knowledge_cutoff_mismatch"):
        w.run(
            "unused",
            mode="diagnostic-promote",
            release_id=str(release_id),
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(release_id),
            manifest=wrong_path,
            **kw,
        )
    with env.admin() as admin:
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_default_diagnostic_pointer"
            ).fetchone()[0]
            == 0
        )
    promoted = w.run(
        "unused",
        mode="diagnostic-promote",
        release_id=str(release_id),
        expected_diagnostic_pointer="none",
        confirm_diagnostic_promote=str(release_id),
        manifest=path,
        **kw,
    )
    assert (
        promoted["state"] == "diagnostic_promoted"
        and promoted["qualified_pointer_touched"] is False
    )
    with pytest.raises(w.PublicationRefused, match="cas_mismatch"):
        w.run(
            "unused",
            mode="diagnostic-promote",
            release_id=str(release_id),
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(release_id),
            **kw,
        )
    with env.admin() as admin:
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_credit_current_pointer"
            ).fetchone()[0]
            == 0
        )
        assert (
            admin.execute(
                "SELECT release_id FROM public.bond_default_diagnostic_pointer"
            ).fetchone()[0]
            == release_id
        )
    with env.app() as app:
        served = app.execute(
            "SELECT public.bond_default_current_diagnostic_release()"
        ).fetchone()[0]
    assert served["display_mode"] == "coverage_only" and served["release_id"] == str(
        release_id
    )
    assert (
        served["counts"]["panel_grid_keys"] == sum(counts.values())
        and served["accepted_events"] == []
    )
    assert len(served["coverage"]) == 360 and len(served["source_frontiers"]) == 6
    assert "hunter2" not in json.dumps(promoted) + json.dumps(first)

    # N5: once a pointer is promoted, the default install-schema rerun's read-back refuses ...
    with pytest.raises(w.PublicationRefused, match="privileges_unverified"):
        w.run("unused", **_install_kwargs(env))
    # ... and an explicit opt-in re-hardens without requiring empty pointers.
    again = w.run(
        "unused",
        **_install_kwargs(env, env={w.ENV_ALLOW_PROMOTED_POINTERS: "1"}),
    )
    assert again["checks_ok"] is True and again["require_empty_pointers"] is False
    assert "pointers_empty" not in again["checks"]
    assert again["code_digest"] == REAL_CODE_DIGEST
    with env.admin() as admin:  # the promoted pointer survived the re-harden
        assert (
            admin.execute(
                "SELECT release_id FROM public.bond_default_diagnostic_pointer"
            ).fetchone()[0]
            == release_id
        )


# ---------------------------------------------------------------------------
# Correction batch: code digest (M4), release/publication cross-checks (L6), K window (L7),
# public-only schema (L8)
# ---------------------------------------------------------------------------
def test_sources_plan_prints_the_computed_code_digest_and_whether_it_matches(rig):
    stats = rig.run(mode="plan", manifest=rig.invocation())
    assert stats["code_digest"] == SB.CODE_DIGEST == stats["code_digest_pinned"]
    assert stats["code_digest_matches"] is True
    real = w.run(
        None,
        manifest=rig.invocation(),
        env={},
        store_factory=_factory(rig.store),
        collaborators=rig.collab(code_digest=None),
    )
    assert real["code_digest"] == sb.code_digest() != SB.CODE_DIGEST
    assert real["code_digest_matches"] is False and real["state"] == "planned"


def test_sources_prepare_refuses_a_code_digest_that_differs_from_the_tree(rig):
    with pytest.raises(w.IntegrityError, match="code_digest_mismatch") as caught:
        rig.run(
            mode="prepare",
            manifest=rig.invocation(),
            collaborators=rig.collab(
                code_digest=None
            ),  # the real tree's digest != the fake pin
        )
    assert w.IntegrityError.exit_code == 4 and sb.code_digest() in str(caught.value)
    assert rig.events == [] and rig.store.publications == {}


def test_sources_plan_read_panel_refuses_a_code_digest_mismatch_before_the_read(rig):
    with pytest.raises(w.IntegrityError, match="code_digest_mismatch"):
        rig.run(
            mode="plan",
            manifest=rig.invocation(),
            read_panel=True,
            collaborators=rig.collab(code_digest=lambda: "sha256:" + "9" * 64),
        )
    assert rig.events == []


def test_the_code_digest_env_pin_must_equal_the_invocation_pin(rig):
    env = {w.ENV_CODE_DIGEST: "sha256:" + "1" * 64}
    with pytest.raises(w.IntegrityError, match="code_digest_env_mismatch"):
        rig.run(mode="prepare", manifest=rig.invocation(), env=env)
    assert rig.events == []
    env = {w.ENV_CODE_DIGEST: SB.CODE_DIGEST}
    assert (
        rig.run(mode="prepare", manifest=rig.invocation(), env=env)["state"]
        == "prepared"
    )


def test_the_real_code_digest_pin_lets_prepare_run(rig):
    real = sb.code_digest()

    def pin(d):
        d["input"]["code_digest"] = real

    stats = rig.run(
        mode="prepare",
        manifest=rig.invocation(mutate=pin),
        collaborators=rig.collab(code_digest=None),
    )
    assert stats["state"] == "prepared"


def test_verify_refuses_a_release_that_belongs_to_another_publication(rig):
    pid = _rig_prepared(rig)
    other = uuid.UUID("7f7f7f7f-7f7f-8f7f-8f7f-7f7f7f7f7f7f")
    rig.diag.releases[RELEASE_ID] = other
    with pytest.raises(w.IntegrityError, match="release_publication_mismatch"):
        rig.run(mode="verify", publication_id=str(pid), release_id=str(RELEASE_ID))
    # refused before the credit publication was touched by this verify
    assert not any(e[0] == "credit.validate" for e in rig.events)


def test_diagnostic_promote_cross_checks_the_invocation_manifest(rig):
    _rig_prepared(rig)
    ok = _dpromote(rig, manifest=rig.invocation())
    assert ok["state"] == "diagnostic_promoted" and rig.diag.pointer == RELEASE_ID


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda d: d["expected"].update(
                publication_id="00000000-0000-8000-8000-000000000001"
            ),
            "release_publication_mismatch",
        ),
        (
            lambda d: d["expected"].update(
                knowledge_cutoff="2026-09-29T18:00:01+00:00"
            ),
            "release_knowledge_cutoff_mismatch",
        ),
        (
            lambda d: d["expected"].update(target_month="2026-07-01"),
            "release_target_month_mismatch",
        ),
        (
            lambda d: d["expected"].update(publication_id=None),
            "publication_id_pin_required",
        ),
    ],
)
def test_diagnostic_promote_refuses_an_invocation_that_disagrees_with_the_release(
    rig, mutate, code
):
    _rig_prepared(rig)
    with pytest.raises(w.IntegrityError, match=code):
        _dpromote(rig, manifest=rig.invocation(mutate=mutate, publication_id="derive"))
    assert rig.diag.pointer is None and not any(
        e[0] == "diag.promote" for e in rig.events
    )


@pytest.mark.parametrize(
    ("cutoff", "code"),
    [
        ("2026-08-31T23:59:59+00:00", "cutoff_before_month_close"),
        ("2026-07-15T00:00:00+00:00", "cutoff_before_month_close"),
    ],
)
def test_prepare_refuses_a_cutoff_before_the_month_closes(rig, cutoff, code):
    def early(d):
        d["expected"]["knowledge_cutoff"] = cutoff

    with pytest.raises(w.IntegrityError, match=code):
        rig.run(mode="prepare", manifest=rig.invocation(mutate=early))
    assert rig.events == [] and rig.store.publications == {}


def test_prepare_cutoff_window_boundaries(rig, monkeypatch):
    def at(iso):
        def mutate(d):
            d["expected"]["knowledge_cutoff"] = iso

        return mutate

    first_day = "2026-09-01T00:00:00+00:00"  # exactly the first day after T: allowed by the window
    now = dt.datetime(2026, 9, 29, 19, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(w, "_utcnow", lambda: now)
    window = w.CUTOFF_FUTURE_SKEW
    assert window == dt.timedelta(minutes=10)
    inv_ok = w.load_invocation(rig.invocation(mutate=at(first_day)))
    w._check_cutoff_window(inv_ok)  # boundary passes
    at_limit = (now + window).isoformat()
    w._check_cutoff_window(w.load_invocation(rig.invocation(mutate=at(at_limit))))
    beyond = (now + window + dt.timedelta(seconds=1)).isoformat()
    with pytest.raises(w.IntegrityError, match="cutoff_in_the_future"):
        w._check_cutoff_window(w.load_invocation(rig.invocation(mutate=at(beyond))))
    with pytest.raises(w.IntegrityError, match="cutoff_before_month_close"):
        w._check_cutoff_window(
            w.load_invocation(
                rig.invocation(mutate=at("2026-08-31T23:59:59.999999+00:00"))
            )
        )


def test_prepare_refuses_a_cutoff_in_the_future_before_any_database_use(
    rig, monkeypatch
):
    monkeypatch.setattr(w, "_utcnow", lambda: SB.K - dt.timedelta(minutes=11))
    with pytest.raises(w.IntegrityError, match="cutoff_in_the_future"):
        rig.run(mode="prepare", manifest=rig.invocation())
    assert rig.events == [] and rig.store.publications == {}
    monkeypatch.setattr(w, "_utcnow", lambda: SB.K - dt.timedelta(minutes=10))
    assert rig.run(mode="prepare", manifest=rig.invocation())["state"] == "prepared"


def test_prepare_refuses_a_non_public_schema_before_any_credit_prepare(rig):
    for schema in ("bond_credit", "public2", "Public"):
        with pytest.raises(
            (w.ConfigError, w.IntegrityError),
            match="schema_must_be_public|schema_identifier",
        ):
            rig.run(mode="prepare", manifest=rig.invocation(), schema=schema)
    assert rig.events == [] and rig.store.publications == {}


def test_release_id_and_read_panel_need_the_public_schema(rig):
    pid = _rig_prepared(rig)
    with pytest.raises(w.ConfigError, match="schema_must_be_public"):
        rig.run(
            mode="verify",
            publication_id=str(pid),
            release_id=str(RELEASE_ID),
            schema="other",
        )
    with pytest.raises(w.ConfigError, match="schema_must_be_public"):
        rig.run(
            mode="diagnostic-promote",
            release_id=str(RELEASE_ID),
            schema="other",
            expected_diagnostic_pointer="none",
            confirm_diagnostic_promote=str(RELEASE_ID),
        )
    with pytest.raises(w.ConfigError, match="schema_must_be_public"):
        rig.run(mode="plan", manifest=rig.invocation(), read_panel=True, schema="other")
    assert rig.diag.pointer is None


# ---------------------------------------------------------------------------
# install-schema mode
# ---------------------------------------------------------------------------
def test_install_schema_guards_are_evaluated_before_any_connection():
    def factory(dsn):
        raise AssertionError("no connection before the guards pass")

    base = {"mode": "install-schema", "env": {}, "connect_factory": factory}
    with pytest.raises(w.ConfigError, match="schema_required"):
        w.run(SECRET_DSN, confirm_install=w.INSTALL_CONFIRMATION, **base)
    with pytest.raises(w.ConfigError, match="schema_must_be_public"):
        w.run(
            SECRET_DSN,
            schema="bond_credit",
            confirm_install=w.INSTALL_CONFIRMATION,
            **base,
        )
    with pytest.raises(w.PromoteNotAuthorized, match="confirmation_required"):
        w.run(SECRET_DSN, schema="public", **base)
    with pytest.raises(w.PromoteNotAuthorized, match="confirmation_required"):
        w.run(SECRET_DSN, schema="public", confirm_install="yes", **base)
    with pytest.raises(w.ConfigError, match="dsn_required"):
        w.run(None, schema="public", confirm_install=w.INSTALL_CONFIRMATION, **base)
    with pytest.raises(
        w.ConfigError, match="install_schema_takes_no_publication_options"
    ):
        w.run(
            SECRET_DSN,
            schema="public",
            confirm_install=w.INSTALL_CONFIRMATION,
            publication_id=str(uuid.UUID(int=1)),
            **base,
        )
    with pytest.raises(w.ConfigError, match="install_options_outside_install_mode"):
        w.run(
            SECRET_DSN,
            mode="plan",
            schema="public",
            confirm_install=w.INSTALL_CONFIRMATION,
            env={},
            manifest="x",
        )
    assert w.SchemaPrerequisiteError.exit_code == 10 and "install-schema" in w.MODES


def test_install_schema_needs_an_autocommit_connection_and_sanitizes_connect_errors():
    class Manual:
        autocommit = False

        def close(self):
            pass

    with pytest.raises(w.ConfigError, match="autocommit_connection_required"):
        w.run(
            SECRET_DSN,
            mode="install-schema",
            schema="public",
            confirm_install=w.INSTALL_CONFIRMATION,
            env={},
            connect_factory=lambda dsn: Manual(),
            collaborators=w.Collaborators(diagnostic=FakeDiag([])),
        )

    def broken(dsn):
        raise RuntimeError(f"cannot connect to {SECRET_DSN}")

    with pytest.raises(w.InfrastructureError, match="db_connect_failed") as caught:
        w.run(
            SECRET_DSN,
            mode="install-schema",
            schema="public",
            confirm_install=w.INSTALL_CONFIRMATION,
            env={},
            connect_factory=broken,
            collaborators=w.Collaborators(diagnostic=FakeDiag([])),
        )
    assert "hunter2" not in str(caught.value) and caught.value.__cause__ is None


def test_install_schema_runs_as_non_superuser_owner_and_is_idempotent(bare_db):
    first = w.run("unused", **_install_kwargs(bare_db))
    assert first["state"] == "installed" and first["mode"] == "install-schema"
    assert first["owner"] == "worker_writer" and first["superuser"] is False
    assert (
        first["checks_ok"] is True and all(first["checks"].values()) and first["checks"]
    )
    assert set(first["roles"]) == {
        "bond_credit_reader",
        "bond_credit_writer",
        "bond_credit_auditor",
        "bond_default_diagnostic_reader",
    } and all(r["nologin"] for r in first["roles"].values())
    assert first["pins"]["policy_digest"] == c.POLICY_DIGEST
    assert first["pins"]["contract_digest"] == c.SCHEMA_DIGEST
    assert first["pins"]["contract_version"] == c.CONTRACT_VERSION
    assert first["harden"]["revoked_count"] > 0 and first["harden"]["tables"] >= 20
    assert first["harden"]["functions"] > 0
    assert first["runtime_role"] == "app_runtime" and first["other_roles"] == [
        "app_analytics_ro"
    ]
    dumped = json.dumps(first)
    assert "password" not in dumped.lower() and "postgresql://" not in dumped
    second = w.run("unused", **_install_kwargs(bare_db))
    assert second["checks_ok"] is True and second["harden"]["revoked_count"] == 0
    assert second["harden"]["manifest_digest"] == first["harden"]["manifest_digest"]
    with bare_db.app() as app:
        assert (
            app.execute(
                "SELECT has_table_privilege('app_runtime','public.bond_credit_publications','SELECT')"
            ).fetchone()[0]
            is False
        )
        assert (
            app.execute(
                "SELECT has_table_privilege('app_analytics_ro','public.bond_credit_publications','SELECT')"
            ).fetchone()[0]
            is False
        )
    with bare_db.admin() as admin:
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_credit_current_pointer"
            ).fetchone()[0]
            == 0
        )
        assert (
            admin.execute(
                "SELECT count(*) FROM public.bond_default_diagnostic_pointer"
            ).fetchone()[0]
            == 0
        )


def test_install_schema_refuses_a_superuser_service_role(bare_db):
    with pytest.raises(w.SchemaPrerequisiteError, match="service_role_is_superuser"):
        w.run(
            "unused",
            **_install_kwargs(
                bare_db,
                connect_factory=lambda dsn: bare_db.admin(autocommit=True),
                env={w.ENV_EXPECTED_OWNER: "postgres"},
            ),
        )
    with bare_db.admin() as admin:  # nothing was installed
        assert (
            admin.execute(
                "SELECT to_regclass('public.bond_credit_publications')"
            ).fetchone()[0]
            is None
        )


def test_install_schema_refuses_a_role_that_is_not_the_expected_owner(bare_db):
    with pytest.raises(
        w.SchemaPrerequisiteError, match="service_role_not_expected_owner"
    ):
        w.run(
            "unused",
            **_install_kwargs(bare_db, env={w.ENV_EXPECTED_OWNER: "app_runtime"}),
        )
    with bare_db.admin() as admin:
        assert (
            admin.execute(
                "SELECT to_regclass('public.bond_credit_publications')"
            ).fetchone()[0]
            is None
        )


def test_install_schema_names_the_missing_group_roles_and_installs_nothing(bare_db):
    diag = FakeDiag([])
    diag.INTENDED_ROLES = ("bond_credit_reader", "bond_role_missing_for_test")
    with pytest.raises(
        w.SchemaPrerequisiteError, match="group_roles_missing"
    ) as caught:
        w.run(
            "unused",
            **_install_kwargs(bare_db, collaborators=w.Collaborators(diagnostic=diag)),
        )
    assert "bond_role_missing_for_test" in str(caught.value) and "administrator" in str(
        caught.value
    )
    with bare_db.admin() as admin:
        assert (
            admin.execute(
                "SELECT to_regclass('public.bond_credit_publications')"
            ).fetchone()[0]
            is None
        )


def test_install_schema_sets_a_pinned_search_path_for_the_session(bare_db):
    seen = []

    class Spy:
        def __init__(self, conn):
            self._conn = conn
            self.autocommit = True

        def execute(self, statement, *args, **kwargs):
            text = (
                statement
                if isinstance(statement, str)
                else statement.as_string(self._conn)
            )
            if "search_path" in text and text.startswith("SET"):
                seen.append(text)
            return self._conn.execute(statement, *args, **kwargs)

        def close(self):
            self._conn.close()

        def __getattr__(self, name):
            return getattr(self._conn, name)

    w.run(
        "unused",
        **_install_kwargs(
            bare_db, connect_factory=lambda dsn: Spy(bare_db.writer(autocommit=True))
        ),
    )
    assert seen and seen[0] == "SET search_path TO public, pg_temp"


def test_cli_install_schema_exit_codes_and_secret_hygiene(bare_db, monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    monkeypatch.setattr(
        w, "_connect_autocommit", lambda dsn: bare_db.writer(autocommit=True)
    )
    assert (
        w.main(["--mode", "install-schema", "--schema", "public"]) == 7
    )  # no confirmation
    assert (
        w.main(
            [
                "--mode",
                "install-schema",
                "--schema",
                "bond_credit",
                "--confirm-install",
                "install-public-v1",
            ]
        )
        == 2
    )
    monkeypatch.setenv(w.ENV_EXPECTED_OWNER, "app_runtime")
    assert (
        w.main(
            [
                "--mode",
                "install-schema",
                "--schema",
                "public",
                "--confirm-install",
                "install-public-v1",
            ]
        )
        == 10
    )
    monkeypatch.delenv(w.ENV_EXPECTED_OWNER)
    capsys.readouterr()
    assert (
        w.main(
            [
                "--mode",
                "install-schema",
                "--schema",
                "public",
                "--confirm-install",
                "install-public-v1",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert json.loads(out)["state"] == "installed" and "hunter2" not in out


# ---------------------------------------------------------------------------
# Preflight: plan works from a checkout that holds only src/, schemas/ and contracts/
# ---------------------------------------------------------------------------
def _minimal_checkout(dest: Path) -> None:
    import shutil

    root = Path(w.__file__).resolve().parents[2]
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    for name in ("src", "schemas", "contracts"):
        shutil.copytree(root / name, dest / name, ignore=ignore)


def _write_repo_invocation(dest: Path, code_digest: str) -> Path:
    doc = SB.manifest_for(SB.PROD_PANEL_ID)
    frontier = (json.dumps(doc, sort_keys=True, indent=2) + "\n").encode()
    (dest / "run").mkdir()
    (dest / "run" / "frontier.json").write_bytes(frontier)
    invocation = {
        "invocation_version": w.INVOCATION_VERSION,
        "product": c.PRODUCT,
        "contract_version": c.CONTRACT_VERSION,
        "input": {
            "kind": "sources",
            "source_manifest": {
                "path": "frontier.json",
                "sha256": hashlib.sha256(frontier).hexdigest(),
            },
            "expected_panel_publication_id": str(SB.PROD_PANEL_ID),
            "code_digest": code_digest,
        },
        "expected": {
            "publication_id": None,
            "target_month": SB.T.isoformat(),
            "knowledge_cutoff": SB.K.isoformat(),
            "quality_state": "partial",
            "build_scope": "limited",
        },
    }
    path = dest / "run" / "invocation.json"
    path.write_text(json.dumps(invocation), encoding="utf-8")
    return path


def _subprocess_env(root: Path) -> dict:
    env = {
        k: v for k, v in os.environ.items() if not k.startswith("BOND_DEFAULT_EVENTS_")
    }
    env.pop("BOND_DEFAULT_TEST_DATABASE_URL", None)
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(root),
            *[
                p
                for p in env.get("PYTHONPATH", "").split(os.pathsep)
                if p and Path(p).resolve() != Path(w.__file__).resolve().parents[2]
            ],
        ]
    )
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def test_plan_runs_from_a_minimal_repo_root_checkout(tmp_path):
    import subprocess

    _minimal_checkout(tmp_path)
    real = sb.code_digest()
    assert (
        sb.code_digest(tmp_path) == real
    )  # the minimal tree pins the same producer digest
    invocation = _write_repo_invocation(tmp_path, real)
    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.workers.bond_default_events",
            "--mode",
            "plan",
            "--manifest",
            str(invocation),
        ],
        cwd=tmp_path,
        env=_subprocess_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert run.returncode == 0, run.stdout[-500:] + run.stderr[-800:]
    stats = json.loads(run.stdout)
    assert stats["state"] == "planned" and stats["database"] == "not_contacted"
    assert stats["code_digest"] == real and stats["code_digest_matches"] is True
    assert stats["frontier_records"] == 6 and stats["panel_publication_id"] == str(
        SB.PROD_PANEL_ID
    )
    assert str(tmp_path) not in run.stdout  # no local path leaks into the report
    # the same through the Railway entry point (WORKER selects the module; DATABASE_URL is only resolved)
    env = _subprocess_env(tmp_path)
    env.update(
        {
            "WORKER": "bond_default_events",
            "DATABASE_URL": "postgresql://u:pw@db.invalid/x",
            w.ENV_MANIFEST: str(invocation),
        }
    )
    via = subprocess.run(
        [sys.executable, "-m", "src.run_worker"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert via.returncode == 0, via.stdout[-500:] + via.stderr[-800:]
    assert json.loads(via.stdout.strip().splitlines()[-1])["state"] == "planned"
    assert "pw@" not in via.stdout + via.stderr


def test_paths_resolve_from_the_repository_root_and_the_timezone_database_is_present():
    root = Path(w.__file__).resolve().parents[2]
    assert c.ROOT == root and all(p.is_file() for p in c.SQL_PATHS)
    assert c.POLICY_PATH.is_file() and c.SCHEMA_PATH.is_file()
    assert (
        root / "contracts" / "bonds" / "default_events_frontier_manifest_2026-08.json"
    ).is_file()
    assert (
        c.EASTERN.key == "America/New_York"
    )  # resolved at import; missing tzdata would have failed there
    import zoneinfo

    assert zoneinfo.ZoneInfo("America/New_York").utcoffset(
        dt.datetime(2026, 8, 1, 12, tzinfo=dt.timezone.utc)
    ) == dt.timedelta(hours=-4)


# ---------------------------------------------------------------------------
# N4 / N5: install-schema is bound to the reviewed tree; re-harden after promotion is opt-in
# ---------------------------------------------------------------------------
def test_install_schema_code_digest_env_pin_is_checked_before_any_connection():
    def factory(dsn):
        raise AssertionError("no connection may be opened on a code digest mismatch")

    base = {
        "mode": "install-schema",
        "schema": "public",
        "confirm_install": w.INSTALL_CONFIRMATION,
        "connect_factory": factory,
    }
    with pytest.raises(w.IntegrityError, match="code_digest_env_mismatch") as caught:
        w.run(SECRET_DSN, env={w.ENV_CODE_DIGEST: "sha256:" + "1" * 64}, **base)
    assert w.IntegrityError.exit_code == 4 and sb.code_digest() in str(caught.value)
    with pytest.raises(w.IntegrityError, match="code_digest_env_mismatch"):
        w.run(
            SECRET_DSN,
            env={w.ENV_CODE_DIGEST: SB.CODE_DIGEST},
            collaborators=w.Collaborators(code_digest=lambda: "sha256:" + "2" * 64),
            **base,
        )
    # a matching pin passes the guard and reaches the (sanitized) connection attempt
    with pytest.raises(w.InfrastructureError, match="db_connect_failed"):
        w.run(SECRET_DSN, env={w.ENV_CODE_DIGEST: sb.code_digest()}, **base)


def test_install_schema_binds_to_the_reviewed_tree_and_reports_all_digests(bare_db):
    real = sb.code_digest()
    with pytest.raises(w.IntegrityError, match="code_digest_env_mismatch"):
        w.run(
            "unused",
            **_install_kwargs(bare_db, env={w.ENV_CODE_DIGEST: "sha256:" + "3" * 64}),
        )
    with bare_db.admin() as admin:  # refused before any DDL
        assert (
            admin.execute(
                "SELECT to_regclass('public.bond_credit_publications')"
            ).fetchone()[0]
            is None
        )
    report = w.run("unused", **_install_kwargs(bare_db, env={w.ENV_CODE_DIGEST: real}))
    assert report["code_digest"] == real
    assert report["pins"]["sql_digest"] == c.sql_digest()
    assert report["pins"]["diagnostic_sql_digest"].startswith("sha256:")
    assert report["pins"]["diagnostic_sql_digest"] == sb.dp.diagnostic_sql_digest()
    assert (
        report["require_empty_pointers"] is True
        and "empty" in report["idempotency_note"]
    )
    assert w.ENV_ALLOW_PROMOTED_POINTERS in report["idempotency_note"]
    assert report["checks"]["pointers_empty"] is True


def test_install_schema_requires_empty_pointers_unless_explicitly_allowed(bare_db):
    seen = []

    class Recording:
        def __getattr__(self, name):
            return getattr(sb.dp, name)

        def verify_installed_privileges(self, conn, **kwargs):
            seen.append(kwargs["require_empty_pointers"])
            return sb.dp.verify_installed_privileges(conn, **kwargs)

    collab = w.Collaborators(diagnostic=Recording())
    w.run("unused", **_install_kwargs(bare_db, collaborators=collab))
    w.run(
        "unused",
        **_install_kwargs(
            bare_db, collaborators=collab, env={w.ENV_ALLOW_PROMOTED_POINTERS: "1"}
        ),
    )
    w.run(
        "unused",
        **_install_kwargs(
            bare_db, collaborators=collab, env={w.ENV_ALLOW_PROMOTED_POINTERS: "0"}
        ),
    )
    assert seen == [True, False, True]


def test_install_schema_help_documents_idempotency_and_the_tree_binding():
    text = w._parser().format_help()
    assert "install-public-v1" in text and w.ENV_ALLOW_PROMOTED_POINTERS in text
    assert w.ENV_CODE_DIGEST in text
    doc = w.__doc__
    assert w.ENV_ALLOW_PROMOTED_POINTERS in doc and "require_empty_pointers" in doc
    assert "code_digest_env_mismatch" in doc


# ---------------------------------------------------------------------------
# `python -m src.workers.bond_default_events` (no src.run_worker): DSN, exit codes, secrecy
# ---------------------------------------------------------------------------
_CLOSED_PORT_DSN = "postgresql://svc:hunter2@127.0.0.1:1/x?connect_timeout=2"


def _cli(args, env_extra, cwd=None, drop=()):
    import subprocess

    root = Path(w.__file__).resolve().parents[2]
    env = _subprocess_env(root)
    for name in drop:
        env.pop(name, None)
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "src.workers.bond_default_events", *args],
        cwd=cwd or root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_module_entrypoint_reads_the_dsn_from_database_url_and_never_prints_it():
    confirm = ["--confirm-install", w.INSTALL_CONFIRMATION]
    base = ["--mode", "install-schema", "--schema", "public", *confirm]
    # DATABASE_URL is the default source: an unreachable DSN yields the typed infrastructure exit
    run = _cli(base, {"DATABASE_URL": _CLOSED_PORT_DSN})
    out = json.loads(run.stdout)
    assert run.returncode == w.InfrastructureError.exit_code == 9
    assert out["code"] == "db_connect_failed" and out["exit_code"] == 9
    for stream in (run.stdout, run.stderr):
        assert (
            "hunter2" not in stream
            and "127.0.0.1" not in stream
            and "svc" not in stream
        )
    # no DSN at all: configuration error, not a crash
    missing = _cli(base, {}, drop=("DATABASE_URL",))
    assert missing.returncode == w.ConfigError.exit_code == 2
    assert json.loads(missing.stdout)["code"] == "dsn_required"
    # another variable name needs the explicit flag
    named = _cli(
        [*base, "--dsn-env", "BDE_DSN"],
        {"BDE_DSN": _CLOSED_PORT_DSN},
        drop=("DATABASE_URL",),
    )
    assert named.returncode == 9 and "hunter2" not in named.stdout + named.stderr


def test_module_entrypoint_typed_exit_codes_without_touching_the_database():
    dsn = {"DATABASE_URL": _CLOSED_PORT_DSN}
    unconfirmed = _cli(["--mode", "install-schema", "--schema", "public"], dsn)
    assert unconfirmed.returncode == w.PromoteNotAuthorized.exit_code == 7
    pin = _cli(
        [
            "--mode",
            "install-schema",
            "--schema",
            "public",
            "--confirm-install",
            w.INSTALL_CONFIRMATION,
        ],
        {**dsn, w.ENV_CODE_DIGEST: "sha256:" + "4" * 64},
    )
    assert (
        pin.returncode == w.IntegrityError.exit_code == 4
    )  # refused before any connection attempt
    assert json.loads(pin.stdout)["code"] == "code_digest_env_mismatch"
    non_public = _cli(
        [
            "--mode",
            "install-schema",
            "--schema",
            "bond_credit",
            "--confirm-install",
            w.INSTALL_CONFIRMATION,
        ],
        dsn,
    )
    assert non_public.returncode == 2
    bogus = _cli(["--mode", "nope"], dsn)
    assert bogus.returncode == 2
    for result in (unconfirmed, pin, non_public, bogus):
        assert "hunter2" not in result.stdout + result.stderr
    # plan (the default mode) never resolves or contacts the DSN
    plan_without_manifest = _cli([], dsn)
    assert plan_without_manifest.returncode == 2
    assert json.loads(plan_without_manifest.stdout)["code"] == "manifest_required"


# ---------------------------------------------------------------------------
# The committed frozen invocation manifest must stay in step with the tree it pins
# ---------------------------------------------------------------------------
COMMITTED_INVOCATION = (
    Path(w.__file__).resolve().parents[2]
    / "configs"
    / "bond_default_events"
    / "invocation_2026-08_v1.json"
)


def test_fleet_image_copies_the_invocation_and_its_pinned_inputs():
    """The Railway one-shot runs from the root Dockerfile image; it must carry the
    invocation manifests and every tree the pinned code digest and frontier manifest
    read at runtime, or the service fails with manifest_unreadable before any work."""
    root = COMMITTED_INVOCATION.parents[2]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    for tree in ("src/", "schemas/", "contracts/", "configs/bond_default_events/"):
        assert f"COPY {tree}" in dockerfile, f"{tree} missing from the fleet image"


def test_committed_invocation_manifest_plans_offline_against_the_current_tree():
    computed = sb.code_digest()
    stats = w.run(None, manifest=COMMITTED_INVOCATION, env={})
    # A change to any of the code-digest files (see source_bundle.code_digest_files) changes the digest:
    # update input.code_digest in configs/bond_default_events/invocation_2026-08_v1.json together with it.
    assert stats["code_digest_matches"] is True, (
        f"stale code_digest pin: computed {computed}, pinned {stats['code_digest_pinned']}"
    )
    assert stats["code_digest"] == computed == stats["code_digest_pinned"]
    assert stats["state"] == "planned" and stats["mode"] == "plan"
    assert stats["database"] == "not_contacted" and stats["network"] == "not_used"
    assert stats["panel_publication_id"] == "65156481-8cb4-52b5-8676-cf77edc5644f"
    assert stats["frontier_records"] == 6
    assert (stats["max_grid_rows"], stats["max_bundle_bytes"]) == (
        1_200_000,
        805_306_368,
    )
    inv = w.load_invocation(COMMITTED_INVOCATION)
    assert inv.kind == "sources" and (inv.quality_state, inv.build_scope) == (
        "partial",
        "limited",
    )
    assert inv.target_month == dt.date(2026, 8, 1)
    assert inv.knowledge_cutoff == dt.datetime(
        2026, 9, 29, 19, 30, tzinfo=dt.timezone.utc
    )
    frontier = json.loads(inv.source_manifest_path.read_text(encoding="utf-8"))
    assert stats["source_manifest_digest"] == frontier["digest"]
    assert (
        inv.source_manifest_sha256
        == hashlib.sha256(inv.source_manifest_path.read_bytes()).hexdigest()
    )
    # K is at or after every frontier observation and after month T has closed
    latest = max(f["observed_at"] for f in frontier["frontiers"])
    assert inv.knowledge_cutoff >= dt.datetime.fromisoformat(latest)
    assert inv.knowledge_cutoff >= dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)


def test_committed_invocation_lives_outside_the_pinned_tree_and_is_deterministic_json():
    rel = COMMITTED_INVOCATION.relative_to(c.ROOT).as_posix()
    assert rel not in sb.code_digest_files() and not rel.startswith("contracts/bonds/")
    raw = COMMITTED_INVOCATION.read_bytes()
    assert raw.endswith(b"\n")  # LF or CRLF checkout: the pins never hash this file
    assert not re.search(rb"[A-Za-z]:\\|/Users/|/home/|/tmp/|postgres", raw)


def test_a_stale_pin_in_the_committed_invocation_is_refused_before_any_connection(
    tmp_path,
):
    invocation = json.loads(COMMITTED_INVOCATION.read_text(encoding="utf-8"))
    invocation["input"]["code_digest"] = "sha256:" + "0" * 64
    invocation["input"]["source_manifest"]["path"] = str(
        (
            COMMITTED_INVOCATION.parent / invocation["input"]["source_manifest"]["path"]
        ).resolve()
    )
    stale = tmp_path / "stale.json"
    stale.write_text(json.dumps(invocation), encoding="utf-8")
    plan = w.run(None, manifest=stale, env={})
    assert plan["code_digest_matches"] is False  # plan reports the drift ...

    def factory(dsn, schema):
        raise AssertionError("no store may be opened for a stale pin")

    with pytest.raises(
        w.IntegrityError, match="code_digest_mismatch"
    ) as caught:  # ... the read refuses
        w.run(
            SECRET_DSN,
            mode="plan",
            manifest=stale,
            schema="public",
            read_panel=True,
            env={},
            store_factory=factory,
        )
    assert w.IntegrityError.exit_code == 4 and sb.code_digest() in str(caught.value)
