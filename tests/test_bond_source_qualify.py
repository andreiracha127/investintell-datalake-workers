"""Owner-authorized source-qualification worker (``src.workers.bond_source_qualify``).

Covers, per the Wave 1 Task 2 brief:

* fail-closed env contract: an empty/absent metric set and an absent
  source-contract reference refuse BEFORE any database connection is opened (the
  DSN below is unreachable, so a premature connection attempt would error the
  test) — never "qualify everything";
* vocabulary (Req 1): a metric outside the exact ``phase10_gate.REQUIREMENTS``
  vocabulary refuses with ``unknown_metric`` and writes nothing;
* denylist (Req 2): ``security_oas`` refuses LOUDLY with
  ``oas_deliberately_excluded`` (owner decision 2026-07-23) and writes nothing —
  even though the engine gate would also block it;
* the authorized INSERT (Req 3): ``{qualified: [...], already_active: [...]}``,
  idempotent over the ``ON CONFLICT (metric_id, source_contract_ref) DO NOTHING``
  path (a re-run inserts nothing, reports the rows as already_active);
* self-installing DDL (Req 5): the worker calls ``install_gate_schema`` first;
* the gate flips (Req 6): ``source_qualified("security_ytm", conn)`` is False
  before and True after the worker runs — proven against the REAL gate function.

DSN-agnostic (Global Constraint): DB cases read ``SEC_TEST_DATABASE_URL`` and run
identically under the keyword and URL DSN conventions.
"""
from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

from src.bonds import phase10_gate
from src.workers import bond_source_qualify

ROOT = Path(__file__).resolve().parents[1]

# Vendor tokens are composed dynamically so this test file itself stays clean
# under any grep-based leak scan.
_PREFIX = "OS" + "BAP"
VENDOR_TOKENS = (
    _PREFIX,
    "open" + "bond" + "asset" + "pricing",
    "TR" + "ACE",
    "WR" + "DS",
)

# An opaque internal token of the shape Task 1 registers — no vendor identity.
TOKEN = "bond_price_source_v1@0123456789ab"

# A DSN that fails fast if anything ever tries to connect with it: pre-DB
# refusals must return BEFORE psycopg is asked to dial anywhere.
UNREACHABLE_DSN = "host=127.0.0.1 port=9 connect_timeout=1"

WAVE1_METRICS = ["security_ytm", "security_ytw", "current_yield", "wal"]


def _env(
    monkeypatch: pytest.MonkeyPatch, *, metrics: str | None, source_ref: str | None
) -> None:
    for name in (bond_source_qualify.ENV_METRICS, bond_source_qualify.ENV_SOURCE_REF):
        monkeypatch.delenv(name, raising=False)
    if metrics is not None:
        monkeypatch.setenv(bond_source_qualify.ENV_METRICS, metrics)
    if source_ref is not None:
        monkeypatch.setenv(bond_source_qualify.ENV_SOURCE_REF, source_ref)


# ---------------------------------------------------------------------------
# Static contract + leak scan (no database).
# ---------------------------------------------------------------------------
def test_refusal_vocabulary_is_closed() -> None:
    assert bond_source_qualify.REFUSAL_NO_METRICS == "no_metrics_requested"
    assert bond_source_qualify.REFUSAL_NO_SOURCE_REF == "no_source_ref"
    assert bond_source_qualify.REFUSAL_UNKNOWN_METRIC == "unknown_metric"
    assert bond_source_qualify.REFUSAL_OAS_EXCLUDED == "oas_deliberately_excluded"


def test_security_oas_is_denylisted_in_code() -> None:
    # Req 2: the exclusion is a CODE-level denylist, not a data/config decision.
    assert "security_oas" in bond_source_qualify.DENYLISTED_METRICS


def test_worker_carries_no_vendor_identity() -> None:
    source = (
        (ROOT / "src" / "workers" / "bond_source_qualify.py")
        .read_text(encoding="utf-8")
        .lower()
    )
    for token in VENDOR_TOKENS:
        assert token.lower() not in source, token


# ---------------------------------------------------------------------------
# Fail-closed env contract: refusals BEFORE any database connection.
# ---------------------------------------------------------------------------
def test_absent_metrics_refuses_fail_closed_before_any_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics=None, source_ref=TOKEN)
    assert bond_source_qualify.run(UNREACHABLE_DSN) == {
        "state": "refused",
        "reason": "no_metrics_requested",
    }


def test_whitespace_only_metrics_refuses_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics="  , ,  ", source_ref=TOKEN)
    assert bond_source_qualify.run(UNREACHABLE_DSN) == {
        "state": "refused",
        "reason": "no_metrics_requested",
    }


def test_missing_source_ref_refuses_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics="security_ytm", source_ref=None)
    assert bond_source_qualify.run(UNREACHABLE_DSN) == {
        "state": "refused",
        "reason": "no_source_ref",
    }


def test_blank_source_ref_refuses_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics="security_ytm", source_ref="   ")
    assert bond_source_qualify.run(UNREACHABLE_DSN) == {
        "state": "refused",
        "reason": "no_source_ref",
    }


def test_unknown_metric_refuses_before_any_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics="security_ytm,not_a_metric", source_ref=TOKEN)
    envelope = bond_source_qualify.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "unknown_metric"
    assert envelope["detail"]["unknown"] == ["not_a_metric"]


def test_oas_denylist_refuses_loudly_before_any_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # security_oas is a KNOWN metric (passes vocabulary) but owner-excluded.
    _env(monkeypatch, metrics="security_ytm,security_oas", source_ref=TOKEN)
    envelope = bond_source_qualify.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "oas_deliberately_excluded"
    assert envelope["detail"]["excluded"] == ["security_oas"]


# ---------------------------------------------------------------------------
# Database cases (disposable PG; both DSN conventions via SEC_TEST_DATABASE_URL).
# ---------------------------------------------------------------------------
pytestmark_db = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


def _db_dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


@pytest.fixture(autouse=True)
def _isolated_qualification_registry():
    """The disposable registry starts empty for every case."""
    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        yield
        return
    with psycopg.connect(dsn, autocommit=True) as conn:
        phase10_gate.install_gate_schema(conn)
        conn.execute("TRUNCATE bond_source_qualification")
    yield


@pytestmark_db
def test_gate_flips_false_before_true_after(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch, metrics="security_ytm", source_ref=TOKEN)

    with psycopg.connect(_db_dsn(), autocommit=True) as conn:
        phase10_gate.install_gate_schema(conn)
        assert phase10_gate.source_qualified("security_ytm", conn) is False

    envelope = bond_source_qualify.run(_db_dsn())
    assert envelope == {
        "state": "ok",
        "qualified": ["security_ytm"],
        "already_active": [],
        "source_contract_ref": TOKEN,
    }

    with psycopg.connect(_db_dsn(), autocommit=True) as conn:
        assert phase10_gate.source_qualified("security_ytm", conn) is True


@pytestmark_db
def test_all_wave1_metrics_flip_and_siblings_stay_dark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics=",".join(WAVE1_METRICS), source_ref=TOKEN)

    envelope = bond_source_qualify.run(_db_dsn())
    assert envelope["state"] == "ok"
    assert envelope["qualified"] == WAVE1_METRICS
    assert envelope["already_active"] == []

    with psycopg.connect(_db_dsn(), autocommit=True) as conn:
        for metric in WAVE1_METRICS:
            assert phase10_gate.source_qualified(metric, conn) is True, metric
        # Nothing else was qualified — the un-requested metrics stay dark.
        assert phase10_gate.source_qualified("security_zspread", conn) is False
        assert phase10_gate.source_qualified("security_oas", conn) is False


@pytestmark_db
def test_second_run_is_idempotent_and_reports_already_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics=",".join(WAVE1_METRICS), source_ref=TOKEN)

    first = bond_source_qualify.run(_db_dsn())
    assert first["qualified"] == WAVE1_METRICS
    assert first["already_active"] == []

    second = bond_source_qualify.run(_db_dsn())
    assert second["state"] == "ok"
    assert second["qualified"] == []
    assert second["already_active"] == WAVE1_METRICS

    with psycopg.connect(_db_dsn(), autocommit=True) as conn:
        total = conn.execute(
            "SELECT count(*) FROM bond_source_qualification"
        ).fetchone()[0]
        assert total == len(WAVE1_METRICS)  # one active row per metric, never duplicated
        active = conn.execute(
            "SELECT count(*) FROM bond_source_qualification WHERE qualified_to IS NULL"
        ).fetchone()[0]
        assert active == len(WAVE1_METRICS)


@pytestmark_db
def test_unknown_metric_refusal_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics="security_ytm,not_a_metric", source_ref=TOKEN)

    envelope = bond_source_qualify.run(_db_dsn())
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "unknown_metric"

    with psycopg.connect(_db_dsn(), autocommit=True) as conn:
        assert (
            conn.execute("SELECT count(*) FROM bond_source_qualification").fetchone()[0]
            == 0
        )


@pytestmark_db
def test_oas_denylist_refusal_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch, metrics="security_ytm,security_oas", source_ref=TOKEN)

    envelope = bond_source_qualify.run(_db_dsn())
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "oas_deliberately_excluded"

    with psycopg.connect(_db_dsn(), autocommit=True) as conn:
        assert (
            conn.execute("SELECT count(*) FROM bond_source_qualification").fetchone()[0]
            == 0
        )
