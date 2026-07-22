"""DB-backed tests for the Phase-10 gate machine (src.bonds.phase10_gate).

DSN-agnostic (reads SEC_TEST_DATABASE_URL); disposable-schema per test.  Proves
each DB predicate in BOTH senses (a fixture that satisfies it and one that does
not), the aggregate all-fail state TODAY with the SPECIFIC reasons per metric, the
existence of a full-pass path (so the machine is not rigged to always fail), and
THE GUARD: evaluating the gate writes NOTHING to any serving / publication /
qualification table and the frozen serving digests do not move.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _phase10_gate_fixtures import (  # noqa: E402
    base_fixture,
    dsn,
    insert_eligible_price,
    insert_ineligible_price,
    publish_curve,
    publish_licensed_ratings,
    publish_unlicensed_ratings,
    qualify_source,
)

from src.bonds import phase10_gate as g  # noqa: E402
from src.bonds import serving_contract  # noqa: E402
from src.sec_serving import contract as reg_contract  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


def _connect():
    return psycopg.connect(dsn(), autocommit=True)


# --------------------------------------------------------------------------- #
# source_qualified — both senses
# --------------------------------------------------------------------------- #
def test_source_qualified_is_false_when_registry_empty() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, _run_id, _pkg = base_fixture(cur)
        for metric in g.metric_ids():
            assert g.source_qualified(metric, conn) is False, metric
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_source_qualified_true_only_for_a_metric_with_an_active_row() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, _run_id, _pkg = base_fixture(cur)
        qualify_source(conn, "security_ytm")
        assert g.source_qualified("security_ytm", conn) is True
        # Only the qualified metric flips; siblings stay False.
        assert g.source_qualified("security_ytw", conn) is False
        # A retired (qualified_to set) row does NOT count as active.
        conn.execute(
            "UPDATE bond_source_qualification SET qualified_to = now() + interval '1 day' "
            "WHERE metric_id = 'security_ytm'"
        )
        assert g.source_qualified("security_ytm", conn) is False
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_source_qualified_false_when_table_absent() -> None:
    # A schema without the qualification table means NOT qualified (never an error).
    with _connect() as conn, conn.cursor() as cur:
        schema = "phase10_gate_no_table_" + os.urandom(4).hex()
        cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
        assert g.source_qualified("security_ytm", conn) is False
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


# --------------------------------------------------------------------------- #
# pit_complete — one both-senses case per PIT input
# --------------------------------------------------------------------------- #
def test_pit_eligible_price_both_senses() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, _pkg = base_fixture(cur)
        # No price -> incomplete for a price-derived metric.
        assert g.pit_complete("security_ytm", conn) is False
        assert g.missing_pit_inputs("security_ytm", conn) == {g.PIT_ELIGIBLE_PRICE}
        # An INELIGIBLE price still leaves it incomplete (honest exclusion).
        insert_ineligible_price(conn, run_id)
        assert g.pit_complete("security_ytm", conn) is False
        # An ELIGIBLE price completes it.
        insert_eligible_price(conn, run_id)
        assert g.pit_complete("security_ytm", conn) is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_pit_spot_curve_both_senses() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, pkg = base_fixture(cur)
        # carry_rolldown needs ONLY a curve.
        assert g.pit_complete("carry_rolldown", conn) is False
        assert g.missing_pit_inputs("carry_rolldown", conn) == {g.PIT_SPOT_CURVE}
        publish_curve(conn, run_id, pkg)
        assert g.pit_complete("carry_rolldown", conn) is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_pit_licensed_ratings_both_senses() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, pkg = base_fixture(cur)
        assert g.pit_complete("rating_distribution", conn) is False
        # A LICENSE-GATED (not_applicable, empty) run is a validated publication but
        # does NOT satisfy the rating metric — the license gate holds through the gate.
        publish_unlicensed_ratings(conn, run_id, pkg)
        assert g.pit_complete("rating_distribution", conn) is False
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_pit_licensed_ratings_satisfied_by_active_product() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, pkg = base_fixture(cur)
        publish_licensed_ratings(conn, run_id, pkg)
        assert g.pit_complete("rating_distribution", conn) is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_metric_with_no_pit_requirement_is_vacuously_complete() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, _run_id, _pkg = base_fixture(cur)
        assert g.pit_complete("wal", conn) is True  # cash-flow metric, no PIT product
        assert g.pit_complete("real_yield", conn) is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


# --------------------------------------------------------------------------- #
# aggregate gate_status — TODAY all-fail with the specific reasons
# --------------------------------------------------------------------------- #
def test_gate_all_fail_today_with_specific_reasons_even_when_pit_satisfied() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, pkg = base_fixture(cur)
        # Satisfy EVERY PIT input so the ONLY remaining blockers are source + model.
        insert_eligible_price(conn, run_id)
        publish_curve(conn, run_id, pkg)
        publish_licensed_ratings(conn, run_id, pkg)

        for metric in g.metric_ids():
            status = g.gate_status(metric, conn)
            assert status.passed is False, f"{metric} must not pass today"
            # source is always a blocker (Constraint #3).
            assert g.REASON_NO_QUALIFIED_SOURCE in status.reasons, metric
            # With every PIT satisfied, the reasons reduce to the CODE-LEVEL set.
            assert status.reasons == g.static_gate_reasons(metric), metric

        # Spot-check the specific per-metric reason sets.
        assert g.gate_status("security_ytm", conn).reasons == (g.REASON_NO_QUALIFIED_SOURCE,)
        assert g.gate_status("security_oas", conn).reasons == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_MODEL_VALIDATION_INCOMPLETE,
        )
        assert g.gate_status("security_effective_duration", conn).reasons == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_DURATION_SAMPLE_PENDING,
        )
        assert g.gate_status("rating_migration", conn).reasons == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_MODEL_NOT_IMPLEMENTED,
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_gate_includes_pit_reason_when_inputs_missing() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, _run_id, _pkg = base_fixture(cur)
        # Nothing published: ytm blocks on source AND pit.
        assert g.gate_status("security_ytm", conn).reasons == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_PIT_INPUTS_MISSING,
        )
        # OAS blocks on source, pit AND model — all three, in stable order.
        assert g.gate_status("security_oas", conn).reasons == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_PIT_INPUTS_MISSING,
            g.REASON_MODEL_VALIDATION_INCOMPLETE,
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_full_pass_path_exists_for_a_validated_engine_metric() -> None:
    # Proves the machine is NOT rigged to always fail: with a qualified source, the
    # PIT input present, and a validated engine, a metric PASSES.  (Isolated fixture
    # schema only; production stays all-fail because no source is authorized.)
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, _pkg = base_fixture(cur)
        insert_eligible_price(conn, run_id)
        qualify_source(conn, "security_ytm")
        status = g.gate_status("security_ytm", conn)
        assert status.passed is True
        assert status.reasons == ()
        # A metric whose engine is NOT validated still fails even fully sourced
        # (the eligible price above already satisfies its PIT input).
        qualify_source(conn, "security_effective_duration")
        dur = g.gate_status("security_effective_duration", conn)
        assert dur.passed is False
        assert dur.reasons == (g.REASON_DURATION_SAMPLE_PENDING,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


# --------------------------------------------------------------------------- #
# THE GUARD: the gate is READ-ONLY and moves no production surface
# --------------------------------------------------------------------------- #
_WATCHED_TABLES = (
    "bond_source_qualification",
    "sec_derived_publications",
    "sec_derived_current_pointers",
    "bond_curve_v1",
    "bond_curve_node_v1",
    "bond_rating_history_v1",
    "bond_price_observation",
)


def _counts(conn) -> dict[str, int]:
    out: dict[str, int] = {}
    for table in _WATCHED_TABLES:
        out[table] = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return out


def test_gate_evaluation_writes_to_no_serving_or_publication_table() -> None:
    with _connect() as conn, conn.cursor() as cur:
        schema, run_id, pkg = base_fixture(cur)
        insert_eligible_price(conn, run_id)
        publish_curve(conn, run_id, pkg)
        publish_licensed_ratings(conn, run_id, pkg)

        before = _counts(conn)
        # Evaluate every predicate for every metric.
        for metric in g.metric_ids():
            g.source_qualified(metric, conn)
            g.pit_complete(metric, conn)
            g.model_validated(metric)
            g.gate_status(metric, conn)
        after = _counts(conn)

        assert before == after, f"gate must be read-only: {before} != {after}"
        # In particular the gate never writes its OWN qualification registry.
        assert after["bond_source_qualification"] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_serving_digests_are_unchanged_by_the_gate() -> None:
    # The gate touches no serving contract: both frozen digests still recompute
    # to their pinned constants (Global Constraint #3 — no production surface moves).
    assert serving_contract.compute_surface_digest() == serving_contract.SURFACE_DIGEST
    assert reg_contract.compute_surface_digest() == reg_contract.SURFACE_DIGEST
