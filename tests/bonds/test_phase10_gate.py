"""DB-free tests for the Phase-10 gate machine (src.bonds.phase10_gate).

Covers the parts of the gate that need no datalake: the metric->requirements map,
the engine code markers (and that the gate reads them, never re-interprets them),
``cashflow_validated`` / ``model_validated`` in BOTH senses across the metric set,
``static_gate_reasons``, and the closed reason vocabulary.  The DB-backed
predicates (``source_qualified`` / ``pit_complete`` / ``gate_status``) and the
read-only / digest guard live in ``tests/test_phase10_gate_machine.py``.
"""

from __future__ import annotations

from src.bonds import cashflows, oas, phase10_gate as g, pricing

# The sixteen Phase-10 security metrics (mirrors app.metrics.registry's
# PHASE10_EXPECTED and the app-side test_metric_registry).
EXPECTED_METRICS = {
    "security_ytm",
    "security_ytw",
    "security_oas",
    "security_zspread",
    "security_effective_duration",
    "rating_distribution",
    "rating_migration",
    "wal",
    "prepayment_extension",
    "carry_rolldown",
    "current_yield",
    "real_yield",
    "spread_duration",
    "key_rate_risk_security",
    "relative_value",
    "liquidity_score",
}

# Duration-family metrics: authoritative printed duration sample pending.
DURATION_METRICS = {
    "security_effective_duration",
    "spread_duration",
    "key_rate_risk_security",
}
# Estimated metrics with no research-grade engine this increment.
UNIMPLEMENTED_METRICS = {
    "rating_migration",
    "prepayment_extension",
    "real_yield",
    "relative_value",
    "liquidity_score",
}


def test_requirements_cover_exactly_the_sixteen_registry_metrics() -> None:
    assert set(g.metric_ids()) == EXPECTED_METRICS
    assert len(g.metric_ids()) == 16
    assert set(g.REQUIREMENTS) == EXPECTED_METRICS


def test_every_requirement_is_structurally_valid() -> None:
    known_engines = {
        g.ENGINE_PRICING_YIELD,
        g.ENGINE_PRICING_DURATION,
        g.ENGINE_OAS,
        g.ENGINE_CASHFLOW,
        g.ENGINE_NONE,
        g.ENGINE_UNIMPLEMENTED,
    }
    for metric, gate in g.REQUIREMENTS.items():
        assert gate.engine in known_engines, f"{metric}: bad engine {gate.engine}"
        assert gate.pit <= g.PIT_INPUTS, f"{metric}: unknown PIT input in {gate.pit}"


def test_engine_markers_are_read_verbatim_from_the_engine_modules() -> None:
    # The gate must consume the engines' OWN declared status strings, never a copy.
    assert cashflows.VALIDATION_STATUS == "convention_derived"
    assert pricing.PRICING_VALIDATION_STATUS["yield"] == "authoritative_published"
    assert pricing.PRICING_VALIDATION_STATUS["duration"] == "authoritative_sample_pending"
    assert oas.MODEL_VALIDATION_STATUS == "model_validation_incomplete"
    # The validated set is exactly the two engines that ARE validated.
    assert g._VALIDATED_STATUSES == {
        cashflows.VALIDATION_STATUS,
        pricing.VALIDATION_STATUS_AUTHORITATIVE,
    }


def test_cashflow_validated_is_true() -> None:
    # Cash flows are validated by convention (the convention IS the ground truth).
    assert g.cashflow_validated() is True


def test_model_validated_true_for_yield_cashflow_and_pure_pit_metrics() -> None:
    for metric in (
        "security_ytm",
        "security_ytw",
        "current_yield",
        "security_zspread",
        "carry_rolldown",  # pricing yield family
        "wal",  # cash-flow engine
        "rating_distribution",  # pure PIT-derived, no model
    ):
        assert g.model_validated(metric) is True, metric


def test_model_validated_false_for_oas_duration_and_unimplemented_metrics() -> None:
    assert g.model_validated("security_oas") is False
    for metric in DURATION_METRICS:
        assert g.model_validated(metric) is False, metric
    for metric in UNIMPLEMENTED_METRICS:
        assert g.model_validated(metric) is False, metric


def test_static_reasons_always_lead_with_no_qualified_source() -> None:
    for metric in g.metric_ids():
        reasons = g.static_gate_reasons(metric)
        assert reasons[0] == g.REASON_NO_QUALIFIED_SOURCE, metric
        assert set(reasons) <= g.GATE_REASONS, metric


def test_static_reasons_carry_the_specific_model_reason() -> None:
    assert g.static_gate_reasons("security_oas") == (
        g.REASON_NO_QUALIFIED_SOURCE,
        g.REASON_MODEL_VALIDATION_INCOMPLETE,
    )
    for metric in DURATION_METRICS:
        assert g.static_gate_reasons(metric) == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_DURATION_SAMPLE_PENDING,
        ), metric
    for metric in UNIMPLEMENTED_METRICS:
        assert g.static_gate_reasons(metric) == (
            g.REASON_NO_QUALIFIED_SOURCE,
            g.REASON_MODEL_NOT_IMPLEMENTED,
        ), metric


def test_validated_engine_metrics_have_only_the_source_reason_statically() -> None:
    for metric in ("security_ytm", "wal", "rating_distribution", "carry_rolldown"):
        assert g.static_gate_reasons(metric) == (g.REASON_NO_QUALIFIED_SOURCE,), metric


def test_reason_vocabulary_is_closed_and_stable() -> None:
    assert g.GATE_REASONS == {
        "no_qualified_source",
        "pit_inputs_missing",
        "model_validation_incomplete",
        "authoritative_duration_sample_pending",
        "model_not_implemented",
    }


def test_unknown_metric_is_rejected() -> None:
    import pytest

    with pytest.raises(ValueError, match="unknown Phase-10 metric"):
        g.model_validated("not_a_metric")
    with pytest.raises(ValueError, match="unknown Phase-10 metric"):
        g.static_gate_reasons("not_a_metric")
