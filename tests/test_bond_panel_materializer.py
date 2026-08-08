from datetime import date

import pytest

from src.bonds.panel_materializer import InMemoryPublicationStore, MaterializationError, materialize_panel
import inspect
import re
import src.bonds.panel_materializer as materializer


def test_surface_insert_placeholder_arities_are_deliberate() -> None:
    source = inspect.getsource(materializer._insert_rows)
    counts = [len(re.findall(r"%s", query)) for query in re.findall(r'cur\.executemany\(f"(INSERT INTO \{table\}[^\"]+)', source)]
    assert sorted(counts) == [6, 10, 10, 27]
    assert "[payload] * 24" in source
    assert "[payload] * 32" in source


def _facts(month: str = "2024-01-01") -> dict[str, list[dict[str, object]]]:
    return {
        "snapshot": [{"month": month, "cusip_id": "AAA", "eligibility_state": "included", "eligibility_reason": "eligible", "ytm_basis": "observed", "mod_dur_source": "observed"}],
        "returns": [{"month": month, "cusip_id": "AAA", "total_return": .01}],
        "rv_signal": [{"month": month, "cusip_id": "AAA", "rv_signal": .1}],
        "rating_pit": [{"month": month, "cusip_id": "AAA", "rating_bucket": "A"}],
    }


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
    base_facts = {**_facts("2024-01-01"), "snapshot": _facts("2024-01-01")["snapshot"] + _facts("2024-02-01")["snapshot"], "rating_pit": _facts("2024-01-01")["rating_pit"] + _facts("2024-02-01")["rating_pit"]}
    base = materialize_panel(store, as_of=date(2024, 2, 29), code_revision="abc", facts=base_facts, source_lineage={"panel": "base"})
    delta_facts = _facts("2024-01-01")
    delta_facts["snapshot"] = _facts("2024-01-01")["snapshot"] + _facts("2024-02-01")["snapshot"]
    delta_facts["rating_pit"] = _facts("2024-01-01")["rating_pit"] + _facts("2024-02-01")["rating_pit"]
    delta = materialize_panel(store, as_of=date(2024, 3, 31), code_revision="abc", facts=delta_facts, source_lineage={"panel": "delta"}, parent_publication_id=base.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 1, 1), open_month=date(2024, 2, 1))
    pack = store.logical_rows(delta.publication_id, "snapshot")
    assert [row["month"] for row in pack] == ["2024-01-01", "2024-02-01"]


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
    revised = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="abc", facts=_facts(), source_lineage={"panel": "two"})
    assert revised.publication_id != first.publication_id


def test_delta_requires_explicit_month_partition_and_retains_logical_first_month() -> None:
    store = InMemoryPublicationStore()
    base = materialize_panel(store, as_of=date(2024, 1, 31), code_revision="base", facts=_facts(), source_lineage={"panel": "base"}, first_month=date(2024, 1, 1), last_closed_month=date(2024, 1, 1))
    delta_facts = _facts("2024-01-01")
    delta_facts["snapshot"] = _facts("2024-01-01")["snapshot"] + _facts("2024-02-01")["snapshot"]
    delta_facts["rating_pit"] = _facts("2024-01-01")["rating_pit"] + _facts("2024-02-01")["rating_pit"]
    delta = materialize_panel(store, as_of=date(2024, 2, 15), code_revision="delta", facts=delta_facts, source_lineage={"panel": "delta"}, parent_publication_id=base.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 1, 1), open_month=date(2024, 2, 1))
    assert store.publications[delta.publication_id]["first_month"] == date(2024, 1, 1)
    assert store.publications[delta.publication_id]["open_month"] == date(2024, 2, 1)
    with pytest.raises(MaterializationError, match="month partition"):
        materialize_panel(store, as_of=date(2024, 2, 15), code_revision="bad", facts=_facts("2024-02-01"), source_lineage={"panel": "bad"}, parent_publication_id=delta.publication_id, first_month=date(2024, 1, 1), last_closed_month=date(2024, 2, 1), open_month=date(2024, 2, 1))
