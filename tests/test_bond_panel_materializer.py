from datetime import date

import pytest

from src.bonds.panel_materializer import InMemoryPublicationStore, MaterializationError, materialize_panel
import inspect
import src.bonds.panel_materializer as materializer


def test_surface_inserts_project_structured_distribution_identity_from_payload() -> None:
    source = inspect.getsource(materializer._insert_rows)
    assert "distribution_rule, reference_cusip9, distribution_decision_id" in source
    assert "->> 'distribution_rule'" in source
    assert "->> 'reference_cusip9'" in source
    assert "->> 'distribution_decision_id'" in source


def _facts(month: str = "2024-01-01") -> dict[str, list[dict[str, object]]]:
    return {
        "snapshot": [{"month": month, "cusip_id": "AAA", "eligibility_state": "included", "eligibility_reason": "eligible", "ytm_basis": "observed", "mod_dur_source": "observed"}],
        "returns": [{"month": month, "cusip_id": "AAA", "total_return": .01}],
        "rv_signal": [{"month": month, "cusip_id": "AAA", "rv_signal": .1}],
        "rating_pit": [{"month": month, "cusip_id": "AAA", "rating_bucket": "A"}],
    }


def _dual_facts(month: str = "2024-01-01") -> dict[str, list[dict[str, object]]]:
    facts = _facts(month)
    for surface, rows in facts.items():
        row = rows[0]
        row.update(
            distribution_rule="rule_144a",
            reference_cusip9="AAA",
            distribution_decision_id=None,
        )
        reg_s = dict(row)
        reg_s.update(
            cusip_id="BBB",
            distribution_rule="reg_s",
            reference_cusip9="AAA",
            distribution_decision_id="decision-1",
        )
        facts[surface] = [row, reg_s]
    return facts


def test_dual_lineage_rejects_missing_reference_identity() -> None:
    facts = _dual_facts()
    facts["snapshot"][0]["reference_cusip9"] = None

    with pytest.raises(MaterializationError, match="distribution identity"):
        materialize_panel(
            InMemoryPublicationStore(), as_of=date(2024, 1, 31), code_revision="dual",
            facts=facts, source_lineage={"distribution_rule": "rule_144a_and_reg_s", "distribution_mapping_snapshot_id": "snapshot-1"},
        )


def test_dual_lineage_rejects_surface_identity_mismatch() -> None:
    facts = _dual_facts()
    facts["returns"][1]["distribution_decision_id"] = "other-decision"

    with pytest.raises(MaterializationError, match="surface identity"):
        materialize_panel(
            InMemoryPublicationStore(), as_of=date(2024, 1, 31), code_revision="dual",
            facts=facts, source_lineage={"distribution_rule": "rule_144a_and_reg_s", "distribution_mapping_snapshot_id": "snapshot-1"},
        )


def test_dual_lineage_rejects_reg_s_execution_cusip_collision() -> None:
    facts = _dual_facts()
    for surface in facts:
        facts[surface][1]["cusip_id"] = "AAA"

    with pytest.raises(MaterializationError, match="duplicate fact keys|execution CUSIP collision"):
        materialize_panel(
            InMemoryPublicationStore(), as_of=date(2024, 1, 31), code_revision="dual",
            facts=facts, source_lineage={"distribution_rule": "rule_144a_and_reg_s", "distribution_mapping_snapshot_id": "snapshot-1"},
        )


def test_dual_lineage_requires_a_nonblank_mapping_snapshot() -> None:
    with pytest.raises(MaterializationError, match="mapping snapshot"):
        materialize_panel(
            InMemoryPublicationStore(), as_of=date(2024, 1, 31), code_revision="dual",
            facts=_dual_facts(), source_lineage={"distribution_rule": "rule_144a_and_reg_s"},
        )


def test_legacy_pointer_accepts_only_an_explicit_dual_series_child() -> None:
    store = InMemoryPublicationStore()
    legacy = materialize_panel(
        store, as_of=date(2024, 1, 31), code_revision="legacy", facts=_facts(),
        source_lineage={"panel": "legacy"},
    )
    store.publications[legacy.publication_id]["config_hash"] = "0c0d78a866bc1090"
    facts = _dual_facts("2024-02-01")
    open_facts = _dual_facts("2024-03-01")
    facts["snapshot"] += open_facts["snapshot"]
    facts["rating_pit"] += open_facts["rating_pit"]

    child = materialize_panel(
        store, as_of=date(2024, 3, 31), code_revision="dual-child", facts=facts,
        source_lineage={"distribution_rule": "rule_144a_and_reg_s", "distribution_mapping_snapshot_id": "snapshot-1"},
        parent_publication_id=legacy.publication_id, first_month=date(2024, 1, 1),
        last_closed_month=date(2024, 2, 1), open_month=date(2024, 3, 1),
    )
    assert store.pointer == child.publication_id
    assert store.publications[child.publication_id]["gate_evidence"]["config_transition"]["contract"] == "rule_144a_to_dual_series_delta_v1"


def test_legacy_pointer_rejects_a_nondual_config_child() -> None:
    store = InMemoryPublicationStore()
    legacy = materialize_panel(
        store, as_of=date(2024, 1, 31), code_revision="legacy", facts=_facts(),
        source_lineage={"panel": "legacy"},
    )
    store.publications[legacy.publication_id]["config_hash"] = "0c0d78a866bc1090"

    with pytest.raises(MaterializationError, match="parent config"):
        materialize_panel(
            store, as_of=date(2024, 3, 31), code_revision="wrong", facts=_facts("2024-02-01"),
            source_lineage={"panel": "wrong"}, parent_publication_id=legacy.publication_id,
            first_month=date(2024, 1, 1), last_closed_month=date(2024, 2, 1), open_month=date(2024, 3, 1),
        )


def test_later_dual_delta_can_continue_rule_144a_when_reg_s_is_omitted() -> None:
    store = InMemoryPublicationStore()
    lineage = {
        "distribution_rule": "rule_144a_and_reg_s",
        "distribution_mapping_snapshot_id": "snapshot-1",
    }
    parent = materialize_panel(
        store,
        as_of=date(2024, 1, 31),
        code_revision="dual-base",
        facts=_dual_facts(),
        source_lineage=lineage,
    )
    closed = _facts("2024-02-01")
    opened = _facts("2024-03-01")
    for rows in closed.values():
        for row in rows:
            row.update(
                distribution_rule="rule_144a",
                reference_cusip9="AAA",
                distribution_decision_id=None,
            )
    for surface in ("snapshot", "rating_pit"):
        row = opened[surface][0]
        row.update(
            distribution_rule="rule_144a",
            reference_cusip9="AAA",
            distribution_decision_id=None,
        )
        closed[surface].append(row)

    child = materialize_panel(
        store,
        as_of=date(2024, 3, 31),
        code_revision="dual-rule144a-only",
        facts=closed,
        source_lineage=lineage,
        parent_publication_id=parent.publication_id,
        first_month=date(2024, 1, 1),
        last_closed_month=date(2024, 2, 1),
        open_month=date(2024, 3, 1),
    )

    assert store.pointer == child.publication_id


def test_base_rejects_later_snapshot_month_without_returns_or_rv_coverage() -> None:
    facts = _facts("2024-01-01")
    facts["snapshot"] += _facts("2024-02-01")["snapshot"]
    facts["rating_pit"] += _facts("2024-02-01")["rating_pit"]

    with pytest.raises(MaterializationError, match="closed surface month coverage"):
        materialize_panel(
            InMemoryPublicationStore(),
            as_of=date(2024, 2, 29),
            code_revision="abc",
            facts=facts,
            source_lineage={"panel": "base"},
        )


def test_materializer_is_deterministic_idempotent_and_promotes_only_after_validation() -> None:
    store = InMemoryPublicationStore()
    first = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "run-1"})
    again = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "run-1"})
    assert first.publication_id == again.publication_id
    assert first.status == "validated"
    assert store.pointer == first.publication_id
    assert store.events == ["prepared", "write", "validated", "pointer"]


def test_gate_failure_leaves_old_pointer_untouched() -> None:
    store = InMemoryPublicationStore()
    original = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "run-1"})
    with pytest.raises(MaterializationError, match="zero rows"):
        materialize_panel(store, as_of=date(2024, 2, 29), code_revision="abc", facts={**_facts("2024-02-01"), "rv_signal": []}, source_lineage={"panel": "run-2"})
    assert store.pointer == original.publication_id


def test_delta_logical_pack_inherits_immutable_months_and_newest_month_wins() -> None:
    store = InMemoryPublicationStore()
    base_facts = {
        **_facts("2024-01-01"),
        "snapshot": _facts("2024-01-01")["snapshot"] + _facts("2024-02-01")["snapshot"],
        "returns": _facts("2024-01-01")["returns"] + _facts("2024-02-01")["returns"],
        "rv_signal": _facts("2024-01-01")["rv_signal"] + _facts("2024-02-01")["rv_signal"],
        "rating_pit": _facts("2024-01-01")["rating_pit"] + _facts("2024-02-01")["rating_pit"],
    }
    base = materialize_panel(store, as_of=date(2024, 2, 29), code_revision="abc", facts=base_facts, source_lineage={"panel": "base"})
    delta_facts = _facts("2024-03-01")
    delta_facts["snapshot"] = _facts("2024-03-01")["snapshot"] + _facts("2024-04-01")["snapshot"]
    delta_facts["rating_pit"] = _facts("2024-03-01")["rating_pit"] + _facts("2024-04-01")["rating_pit"]
    delta = materialize_panel(store, as_of=date(2024, 4, 30), code_revision="abc", facts=delta_facts, source_lineage={"panel": "delta"}, parent_publication_id=base.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 3, 1), open_month=date(2024, 4, 1))
    pack = store.logical_rows(delta.publication_id, "snapshot")
    assert [row["month"] for row in pack] == ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]


def test_delta_does_not_overwrite_a_sibling_pointer_advanced_after_validation() -> None:
    store = InMemoryPublicationStore()
    parent = materialize_panel(
        store,
        as_of=date(2024, 1, 31),
        code_revision="abc",
        facts=_facts(),
        source_lineage={"panel": "parent"},
    )
    store.pointer = None
    sibling = materialize_panel(
        store,
        as_of=date(2024, 1, 31),
        code_revision="abc",
        facts=_facts(),
        source_lineage={"panel": "sibling"},
    )
    store.pointer = parent.publication_id

    class AdvancePointerAfterValidation(list[str]):
        def append(self, event: str) -> None:
            super().append(event)
            if event == "validated":
                store.pointer = sibling.publication_id

    store.events = AdvancePointerAfterValidation()
    delta_facts = _facts("2024-02-01")
    delta_facts["snapshot"] += _facts("2024-03-01")["snapshot"]
    delta_facts["rating_pit"] += _facts("2024-03-01")["rating_pit"]
    with pytest.raises(MaterializationError, match="no longer current"):
        materialize_panel(
            store,
            as_of=date(2024, 2, 29),
            code_revision="abc",
            facts=delta_facts,
            source_lineage={"panel": "delta"},
            parent_publication_id=parent.publication_id,
            first_month=date(2024, 1, 1),
            last_closed_month=date(2024, 2, 1),
            open_month=date(2024, 3, 1),
        )

    assert store.pointer == sibling.publication_id
    assert [publication["status"] for publication in store.publications.values()].count("failed") == 1


def test_empty_lineage_is_a_gate_failure_that_cannot_move_pointer() -> None:
    store = InMemoryPublicationStore()
    original = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "run-1"})
    with pytest.raises(MaterializationError, match="lineage"):
        materialize_panel(store, as_of=date(2024, 2, 29), code_revision="abc", facts=_facts("2024-02-01"), source_lineage={})
    assert store.pointer == original.publication_id


def test_same_day_revised_input_gets_new_immutable_identity_but_exact_rerun_repoints() -> None:
    store = InMemoryPublicationStore()
    first = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "one"})
    store.pointer = None
    exact = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "one"})
    assert exact.publication_id == first.publication_id == store.pointer
    with pytest.raises(MaterializationError, match="cannot replace current pointer"):
        materialize_panel(
            store,
            as_of=date(2024, 1, 31),
            code_revision="abc",
            facts=_facts(),
            source_lineage={"panel": "two"},
        )
    assert store.pointer == first.publication_id


def test_delta_requires_explicit_month_partition_and_retains_logical_first_month() -> None:
    store = InMemoryPublicationStore()
    base = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="base", facts=_facts(), source_lineage={"panel": "base"}, first_month=date(2024, 1, 1), last_closed_month=date(2024, 1, 1))
    delta_facts = _facts("2024-02-01")
    delta_facts["snapshot"] = _facts("2024-02-01")["snapshot"] + _facts("2024-03-01")["snapshot"]
    delta_facts["rating_pit"] = _facts("2024-02-01")["rating_pit"] + _facts("2024-03-01")["rating_pit"]
    delta = materialize_panel(store, as_of=date(2024, 3, 15), code_revision="delta", facts=delta_facts, source_lineage={"panel": "delta"}, parent_publication_id=base.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 2, 1), open_month=date(2024, 3, 1))
    assert store.publications[delta.publication_id]["first_month"] == date(2024, 1, 1)
    assert store.publications[delta.publication_id]["open_month"] == date(2024, 3, 1)
    skipped_facts = _facts("2024-04-01")
    skipped_facts["snapshot"] = _facts("2024-04-01")["snapshot"] + _facts("2024-05-01")["snapshot"]
    skipped_facts["rating_pit"] = _facts("2024-04-01")["rating_pit"] + _facts("2024-05-01")["rating_pit"]
    with pytest.raises(MaterializationError, match="month partition"):
        materialize_panel(store, as_of=date(2024, 5, 15), code_revision="bad", facts=skipped_facts, source_lineage={"panel": "bad"}, parent_publication_id=delta.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 4, 1), open_month=date(2024, 5, 1))


def test_delta_rejects_a_parent_with_a_skipped_open_month() -> None:
    store = InMemoryPublicationStore()
    base = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="base", facts=_facts(), source_lineage={"panel": "base"})
    store.publications[base.publication_id]["open_month"] = date(2024, 3, 1)
    child_facts = _facts("2024-03-01")
    child_facts["snapshot"] = _facts("2024-03-01")["snapshot"] + _facts("2024-04-01")["snapshot"]
    child_facts["rating_pit"] = _facts("2024-03-01")["rating_pit"] + _facts("2024-04-01")["rating_pit"]

    with pytest.raises(MaterializationError, match="month partition"):
        materialize_panel(store, as_of=date(2024, 4, 15), code_revision="child", facts=child_facts, source_lineage={"panel": "child"}, parent_publication_id=base.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 3, 1), open_month=date(2024, 4, 1))
