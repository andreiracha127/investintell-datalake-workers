"""Pure FE-1 purpose-labelled diagnostic relations (offline diagnostics only)."""

from __future__ import annotations

import copy
import csv
import dataclasses
import datetime as dt
import gc
import hashlib
import inspect
import itertools
import json
import os
import pickle
import random
import sqlite3
import subprocess
import sys
import weakref
import zipfile
from collections import defaultdict, deque
from pathlib import Path
from types import MappingProxyType

import pytest

from src.bonds.default_events import ncen, nport

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
K = dt.datetime(2026, 9, 25, 18, tzinfo=UTC)
R = dt.date(2026, 3, 31)
CTX = "ncenctx:" + "1" * 64
ACCESSION = "0000000001-26-000001"
ALL_ROLES = (
    "current_primary",
    "current_sub",
    "terminated_primary",
    "terminated_sub",
    "underwriter",
)


def cik(number: int) -> str:
    return f"{number:010d}"


def row(
    registrant: int,
    role: str,
    locator: str,
    *,
    answer: str | None = None,
    name: str | None = None,
    file_number: str | None = None,
    crd: str | None = None,
    lei: str | None = None,
    series_id: str | None = "S000000001",
    series_scope: str = "series",
    attestation: str = "attested",
    reasons: tuple[str, ...] = (),
    uncertain_expansion_eligible: bool = False,
    public_at: dt.datetime | None = K - dt.timedelta(days=1),
    data_known_at: dt.datetime | None = K - dt.timedelta(days=1),
    retrieved_at: dt.datetime | None = K - dt.timedelta(days=1),
) -> ncen.DiagnosticSourceRow:
    if role == "b5" and series_id == "S000000001" and series_scope == "series":
        series_id = None
        series_scope = "registrant"
    return ncen.DiagnosticSourceRow(
        accession_number=ACCESSION,
        registrant_cik=cik(registrant),
        role=role,
        locator=locator,
        series_id=series_id,
        series_scope=series_scope,
        answer_raw=answer,
        name_raw=name,
        file_number_raw=file_number,
        crd_raw=crd,
        lei_raw=lei,
        attestation=attestation,
        reasons=reasons,
        uncertain_expansion_eligible=uncertain_expansion_eligible,
        public_at=public_at,
        data_known_at=data_known_at,
        retrieved_at=retrieved_at,
    )


def selection(
    registrant: int,
    *rows: ncen.DiagnosticSourceRow,
    complete: bool = True,
    reasons: tuple[str, ...] = (),
    selection_reason: str | None = None,
    report_date: dt.date = R,
    cutoff: dt.datetime = K,
    mode: str = "historical_reconstruction",
) -> ncen.DiagnosticSelection:
    return ncen.diagnostic_fixture_selection(
        cik=cik(registrant),
        accession_number=ACCESSION,
        rows=rows,
        reasons=reasons if reasons else (() if complete else ("voting_series_absent_from_effective_ncen",)),
        selection_reason=selection_reason,
        report_date=report_date,
        knowledge_cutoff=cutoff,
        mode=mode,
    )


def node_and_incidences(
    selected: ncen.DiagnosticSelection,
    *,
    context_id: str = CTX,
) -> tuple[ncen.DependenceNode, tuple[ncen.TypedIncidence, ...]]:
    family = ncen.reported_family_for(selected)
    node = ncen.DependenceNode(
        context_id=context_id,
        cik=selected.cik,
        evidence_state=selected.evidence_state,
        reasons=selected.reasons,
        reported_family=family,
    )
    return node, ncen.typed_incidences_for(selected, context_id=context_id, reported_family=family)


def projection(
    selected: tuple[ncen.DiagnosticSelection, ...],
    ablation_id: str = "full_observed",
) -> ncen.DependenceProjection:
    context = ncen.diagnostic_fixture_context(selected, report_date=selected[0].report_date,
                                              knowledge_cutoff=selected[0].knowledge_cutoff,
                                              mode=selected[0].mode)
    return ncen.project_dependence(
        context.nodes, context.incidences,
        spec=ncen.ablation_spec(ablation_id),
        admission=context,
    )


def component_members(result: ncen.DependenceProjection) -> set[frozenset[str]]:
    return {frozenset(component.members) for component in result.components}


def bfs_partition(
    nodes: tuple[ncen.DependenceNode, ...],
    incidences: tuple[ncen.TypedIncidence, ...],
) -> set[frozenset[str]]:
    by_key: dict[str, list[str]] = defaultdict(list)
    for incidence in incidences:
        if incidence.key_id is not None and incidence.attestation == "attested":
            by_key[incidence.key_id].append(incidence.cik)
    graph: dict[str, set[str]] = {node.cik: set() for node in nodes}
    for members in by_key.values():
        unique = sorted(set(members))
        for left, right in itertools.combinations(unique, 2):
            graph[left].add(right)
            graph[right].add(left)
    remaining = set(graph)
    answer: set[frozenset[str]] = set()
    while remaining:
        start = min(remaining)
        reached = {start}
        queue = deque([start])
        while queue:
            current = queue.popleft()
            for neighbour in graph[current] - reached:
                reached.add(neighbour)
                queue.append(neighbour)
        remaining -= reached
        answer.add(frozenset(reached))
    return answer


def context_id(
    report_date: dt.date,
    cutoff: dt.datetime,
    *,
    mode: str = "historical_reconstruction",
    inventory: str = "inventory-a",
) -> str:
    return ncen.diagnostic_context_id(
        report_date=report_date,
        knowledge_cutoff=cutoff,
        mode=mode,
        inventory_digest=inventory,
        cohort_digest="2" * 64,
        ncen_evidence_digest="3" * 64,
        exclusion_ledger_digest="4" * 64,
    )


def snapshot(
    report_date: dt.date,
    selected: tuple[ncen.DiagnosticSelection, ...],
    *,
    cutoff: dt.datetime,
    inventory: str,
    ablation_id: str = "full_observed",
    mode: str = "historical_reconstruction",
) -> ncen.DependenceSnapshot:
    rebound = tuple(selection(
        int(item.cik), *(dataclasses.replace(raw, attestation="attested", reasons=(),
                                             uncertain_expansion_eligible=False) for raw in item.rows),
        complete=item.evidence_state == "complete" or item.selection_reason is not None, reasons=tuple(
            reason for reason in item.reasons if reason != item.selection_reason),
        selection_reason=item.selection_reason, report_date=report_date, cutoff=cutoff, mode=mode,
    ) for item in selected)
    context = ncen.diagnostic_fixture_context(
        rebound, report_date=report_date, knowledge_cutoff=cutoff, inventory_digest=inventory, mode=mode,
    )
    result = ncen.project_dependence(
        context.nodes, context.incidences,
        spec=ncen.ablation_spec(ablation_id),
        admission=context,
    )
    return ncen.DependenceSnapshot(
        report_date=report_date,
        knowledge_cutoff=cutoff,
        mode=mode,
        inventory_digest=inventory,
        projection=result,
    )


def test_reported_family_equal_keys_share_label() -> None:
    a = selection(
        1,
        row(1, "b5", "a:b5", answer="Y", name="Acme Funds"),
        row(1, "current_primary", "a:p", crd="101", name="Provider A"),
    )
    b = selection(
        2,
        row(2, "b5", "b:b5", answer="Y", name="  acme\tFUNDS "),
        row(2, "current_primary", "b:p", crd="202", name="Provider B"),
    )
    family_a = ncen.reported_family_for(a)
    family_b = ncen.reported_family_for(b)
    assert family_a.state == family_b.state == "declared_family"
    assert family_a.name_key == family_b.name_key == "ACME FUNDS"
    assert family_a.label_id == family_b.label_id
    assert family_a.source_row_ids != family_b.source_row_ids

    c = selection(
        3,
        row(3, "b5", "c:b5", answer="Y", name="Other Group"),
        row(3, "current_sub", "c:p", crd="101", name="Different Display Name"),
    )
    assert ncen.reported_family_for(c).label_id != family_a.label_id
    result = projection((a, c))
    assert component_members(result) == {frozenset({cik(1), cik(3)})}
    changed_provider = dataclasses.replace(a.rows[1], crd_raw="999")
    changed = selection(1, a.rows[0], changed_provider)
    assert ncen.reported_family_for(changed).label_id == family_a.label_id
    stale_selection = selection(1, row(1, "b5", "new-b5", answer="N"))
    with pytest.raises(ncen.NcenError, match="reported_family_selection_mismatch"):
        ncen.typed_incidences_for(stale_selection, context_id=CTX, reported_family=family_a)


def test_reported_family_standalone_unknown_and_copy_conflicts() -> None:
    standalone_a = ncen.reported_family_for(selection(1, row(1, "b5", "n1", answer="N", name="")))
    standalone_b = ncen.reported_family_for(selection(2, row(2, "b5", "n2", answer="N", name=None)))
    assert standalone_a.state == standalone_b.state == "standalone"
    assert standalone_a.label_id != standalone_b.label_id

    missing = ncen.reported_family_for(selection(3, row(3, "b5", "m", answer=None, name=None)))
    sentinel = ncen.reported_family_for(selection(4, row(4, "b5", "s", answer="Y", name="N/A")))
    conflict = ncen.reported_family_for(selection(5, row(5, "b5", "c", answer="N", name="Named Family")))
    unparseable = ncen.reported_family_for(selection(9, row(9, "b5", "u", answer="MAYBE")))
    answer_conflict = ncen.reported_family_for(
        selection(6, row(6, "b5", "y", answer="Y", name="Acme"), row(6, "b5", "n", answer="N"))
    )
    for family in (missing, sentinel, conflict, answer_conflict, unparseable):
        assert family.state == "unknown" and family.label_id is None
    assert "diagnostic_b5_answer_name_conflict" in conflict.reasons
    assert conflict.answer == "N"
    assert sentinel.answer == "Y"
    assert "diagnostic_b5_answer_unparseable" in unparseable.reasons

    repeated = ncen.reported_family_for(
        selection(7, row(7, "b5", "r1", answer="Y", name="Acme"), row(7, "b5", "r2", answer="Y", name="ACME"))
    )
    assert repeated.state == "declared_family" and repeated.name_raw is None
    legacy_equal = ncen.reported_family_for(
        selection(
            8,
            row(8, "b5", "l1", answer="Y", name="Acme Fund"),
            row(8, "b5", "l2", answer="Y", name="Acme"),
        )
    )
    assert ncen.normalize_family_name("Acme Fund") == ncen.normalize_family_name("Acme") == "ACME"
    assert legacy_equal.state == "unknown"
    assert "diagnostic_b5_copy_key_conflict" in legacy_equal.reasons

    assert ncen.normalize_reported_name_key("  abc\u2003fund  ") == "ABC FUND"
    assert ncen.normalize_reported_name_key("ABC-Fund, Inc.") == "ABC-FUND, INC."
    assert ncen.normalize_reported_name_key("Café Funds") == "CAFÉ FUNDS"
    assert ncen.normalize_reported_name_key("ABC FUND") != ncen.normalize_reported_name_key("ABC")
    assert ncen.normalize_reported_name_key("N.A.") is None
    unicode_selection = selection(10, row(10, "b5", "unicode", answer="Y", name="ΐ Funds"))
    unicode_family = ncen.reported_family_for(unicode_selection)
    unicode_edges = ncen.typed_incidences_for(
        unicode_selection, context_id=CTX, reported_family=unicode_family
    )
    assert unicode_edges[0].identifier_value == ncen.normalize_reported_name_key("ΐ Funds")


def test_review_f10_b5_canonical_unicode_identity() -> None:
    precomposed = "CAFÉ Funds"
    decomposed = "CAFE\u0301 Funds"
    precomposed_key = ncen.normalize_reported_name_key(precomposed)
    decomposed_key = ncen.normalize_reported_name_key(decomposed)
    assert precomposed_key == decomposed_key == "CAFÉ FUNDS"
    assert ncen.normalize_reported_name_key("CAFE Funds") != precomposed_key
    assert ncen.normalize_reported_name_key("CAFÉ-Funds") != precomposed_key

    left = selection(21, row(21, "b5", "unicode-left", answer="Y", name=precomposed))
    right = selection(22, row(22, "b5", "unicode-right", answer="Y", name=decomposed))
    left_family = ncen.reported_family_for(left)
    right_family = ncen.reported_family_for(right)
    assert left_family.name_key == right_family.name_key == precomposed_key
    assert left_family.label_id == right_family.label_id
    left_incidence = ncen.typed_incidences_for(
        left, context_id=CTX, reported_family=left_family
    )[0]
    right_incidence = ncen.typed_incidences_for(
        right, context_id=CTX, reported_family=right_family
    )[0]
    assert (left_incidence.identifier_value, left_incidence.key_id) == (
        right_incidence.identifier_value,
        right_incidence.key_id,
    )
    assert ncen.diagnostic_b5_key_id("CAFÉ FUNDS") == ncen.diagnostic_b5_key_id(
        "CAFE\u0301 FUNDS"
    )
    assert dataclasses.replace(
        left_incidence,
        identifier_value="CAFE\u0301 FUNDS",
        key_id=ncen.diagnostic_b5_key_id("CAFE\u0301 FUNDS"),
    ) == left_incidence
    assert component_members(projection((left, right))) == {
        frozenset({cik(21), cik(22)})
    }

    greek = ncen.normalize_reported_name_key("ΐ Funds")
    assert greek == "\u03aa\u0301 FUNDS"
    assert tuple(map(ord, greek)) == (
        0x03AA,
        0x0301,
        0x0020,
        0x0046,
        0x0055,
        0x004E,
        0x0044,
        0x0053,
    )
    assert ncen.DIAGNOSTIC_NAME_NORMALIZER_VERSION == "ncen_reported_name_key_v2"
    greek_selection = selection(23, row(23, "b5", "unicode-greek", answer="Y", name="ΐ Funds"))
    greek_family = ncen.reported_family_for(greek_selection)
    greek_edge = ncen.typed_incidences_for(
        greek_selection, context_id=CTX, reported_family=greek_family
    )[0]
    assert greek_edge.identifier_value == greek
    assert greek_edge.key_id == ncen.diagnostic_b5_key_id(greek)

    for invalid_key in ("Café Funds", "CAFÉ  FUNDS", "N/A"):
        with pytest.raises(ncen.NcenError, match="reported_name_key_not_normalized"):
            ncen.diagnostic_b5_key_id(invalid_key)


def test_review_f11_public_carriers_exact_ids_and_counts() -> None:
    declared = ncen.reported_family_for(
        selection(31, row(31, "b5", "identity", answer="Y", name="Acme Funds"))
    )
    other_key_label = ncen._diagnostic_id(
        "ncenreported",
        [
            ncen.DIAGNOSTIC_REPORTED_FAMILY_VERSION,
            ncen.DIAGNOSTIC_NAME_NORMALIZER_VERSION,
            "OTHER FUNDS",
        ],
    )
    for invalid_label in ("ncenreported:bogus", other_key_label):
        with pytest.raises(ncen.NcenError):
            dataclasses.replace(declared, label_id=invalid_label)
    with pytest.raises(ncen.NcenError):
        dataclasses.replace(declared, name_key="OTHER FUNDS")

    standalone = ncen.reported_family_for(
        selection(32, row(32, "b5", "standalone", answer="N"))
    )
    other_cik_label = ncen._diagnostic_id(
        "ncenstandalone", [ncen.DIAGNOSTIC_REPORTED_FAMILY_VERSION, cik(33)]
    )
    with pytest.raises(ncen.NcenError):
        dataclasses.replace(standalone, label_id=other_cik_label)

    unknown = ncen.reported_family_for(selection(34, row(34, "b5", "unknown", answer=None)))
    with pytest.raises(ncen.NcenError):
        dataclasses.replace(unknown, name_key="ACME FUNDS")

    family_record = {
        "context_id": CTX,
        "purpose": "reported_family",
        "rule_version": ncen.DIAGNOSTIC_REPORTED_FAMILY_VERSION,
        "cik": declared.cik,
        "state": declared.state,
        "answer": declared.answer,
        "name_raw": declared.name_raw,
        "name_key": declared.name_key,
        "label_id": other_key_label,
        "source_row_ids": list(declared.source_row_ids),
        "normalizer": declared.normalizer,
        "reasons": [],
        "claim": declared.claim,
    }
    with pytest.raises(ncen.NcenError):
        ncen._purpose_validate_reported_family(family_record)

    result = projection(
        (
            selection(35, row(35, "current_primary", "provider-35", crd="35")),
            selection(36, row(36, "current_primary", "provider-36", crd="35")),
        )
    )
    component = result.components[0]
    assert component.complete_count == 2 and component.incomplete_count == 0
    with pytest.raises(ncen.NcenError):
        dataclasses.replace(component, complete_count=-1, incomplete_count=3)
    for field_name in ("complete_count", "incomplete_count", "distinct_reported_y_keys"):
        current = getattr(component, field_name)
        for invalid_count in (bool(current), float(current)):
            with pytest.raises(ncen.NcenError):
                dataclasses.replace(component, **{field_name: invalid_count})

    degree = result.key_degrees[0]
    assert degree.complete_registrants == 2 and degree.incomplete_registrants == 0
    for field_name in (
        "distinct_registrants",
        "complete_registrants",
        "incomplete_registrants",
        "incidence_count",
    ):
        current = getattr(degree, field_name)
        for invalid_count in (bool(current), float(current)):
            with pytest.raises(ncen.NcenError):
                dataclasses.replace(degree, **{field_name: invalid_count})

    transition = ncen.DependenceTransition(
        ablation_id="provider_only",
        from_context_id=CTX,
        to_context_id=CTX,
        from_component_id=component.snapshot_component_id,
        to_component_id=component.snapshot_component_id,
        intersection_count=1,
        union_count=1,
        kind="overlap",
    )
    for field_name in ("intersection_count", "union_count"):
        current = getattr(transition, field_name)
        for invalid_count in (bool(current), float(current)):
            with pytest.raises(ncen.NcenError):
                dataclasses.replace(transition, **{field_name: invalid_count})

    fold_scope = "ncenfoldscope:" + "5" * 64
    fold_members = (cik(35),)
    fold_group = ncen.TemporalFoldGroup(
        fold_scope_id=fold_scope,
        fold_group_id=ncen.fold_group_id(scope_id=fold_scope, members=fold_members),
        members=fold_members,
        has_unknown_dependence=False,
    )
    temporal = ncen.TemporalDependenceUnion(
        fold_scope_id=fold_scope,
        groups=(fold_group,),
        incidence_count=0,
        union_attempts=0,
        successful_unions=0,
    )
    assert fold_group.member_count == 1
    for field_name in ("incidence_count", "union_attempts", "successful_unions"):
        for invalid_count in (False, 0.0):
            with pytest.raises(ncen.NcenError):
                dataclasses.replace(temporal, **{field_name: invalid_count})
    with pytest.raises(ncen.NcenError):
        dataclasses.replace(temporal, incidence_count=1)

    for field_name in (
        "union_attempts",
        "successful_unions",
        "uncertain_incidence_count",
        "excluded_incidence_count",
    ):
        for invalid_count in (False, 0.0):
            with pytest.raises(ncen.NcenError):
                dataclasses.replace(result, **{field_name: invalid_count})

    component_record = {
        **dataclasses.asdict(component),
        "purpose": "reporting_dependence_block",
        "member_count": component.member_count,
    }
    ncen._purpose_validate_component(component_record)
    degree_record = {
        **dataclasses.asdict(degree),
        "context_id": CTX,
        "ablation_id": result.ablation_id,
    }
    ncen._purpose_validate_key_degree(degree_record)
    with pytest.raises(ncen.NcenError):
        ncen._purpose_validate_key_degree(
            {**degree_record, "complete_registrants": 3}
        )

    transition_record = dataclasses.asdict(transition)
    ncen._purpose_validate_transition(transition_record)
    for invalid_count in (True, 1.0):
        with pytest.raises(ncen.NcenError):
            ncen._purpose_validate_transition(
                {**transition_record, "intersection_count": invalid_count}
            )

    fold_record = {
        "fold_scope_id": fold_group.fold_scope_id,
        "fold_group_id": fold_group.fold_group_id,
        "member_count": fold_group.member_count,
        "has_unknown_dependence": fold_group.has_unknown_dependence,
        "usable_for_independence_claim": fold_group.usable_for_independence_claim,
    }
    ncen._purpose_validate_fold_group(fold_record)
    for invalid_count in (True, 1.0):
        with pytest.raises(ncen.NcenError):
            ncen._purpose_validate_fold_group(
                {**fold_record, "member_count": invalid_count}
            )

    zero_summary = {
        "context_id": CTX,
        "ablation_id": result.ablation_id,
        "node_count": 0,
        "complete_count": 0,
        "incomplete_count": 0,
        "component_count": 0,
        "largest_all": 0,
        "largest_complete": 0,
        "complete_square_sum": 0,
        "complete_total": 0,
        "uncertain_incidence_count": 0,
        "excluded_incidence_count": 0,
    }
    ncen._purpose_validate_summary(zero_summary)
    for field_name in zero_summary.keys() - {"context_id", "ablation_id"}:
        for invalid_count in (False, 0.0):
            with pytest.raises(ncen.NcenError):
                ncen._purpose_validate_summary(
                    {**zero_summary, field_name: invalid_count}
                )
    with pytest.raises(ncen.NcenError):
        ncen._purpose_validate_summary({**zero_summary, "complete_count": 1})


def test_role_and_identifier_projection() -> None:
    shared_lei = "5493001Z012YSB2A0K51"
    selected = tuple(
        selection(
            number,
            row(number, "b5", f"{number}:b5", answer="N"),
            row(
                number,
                role,
                f"{number}:provider",
                lei=shared_lei,
                file_number="801-00001" if role == "current_primary" else None,
                crd="000000123" if role == "current_primary" else None,
                series_id=None if role == "underwriter" else "S000000001",
                series_scope="registrant" if role == "underwriter" else "series",
                name="Shared Provider" if number < 6 else "Other Name",
            ),
        )
        for number, role in enumerate(ALL_ROLES, start=1)
    )
    built = [node_and_incidences(item) for item in selected]
    incidences = tuple(incidence for item in built for incidence in item[1])
    roles = {incidence.role for incidence in incidences if incidence.kind == "provider"}
    kinds = {incidence.identifier_kind for incidence in incidences if incidence.kind == "provider"}
    assert roles == set(ALL_ROLES)
    assert kinds == {"FN", "CRD", "LEI"}
    assert any(i.role == "underwriter" and i.identifier_kind == "LEI" for i in incidences)
    result = projection(selected, "provider_only")
    assert component_members(result) == {frozenset(cik(number) for number in range(1, 6))}

    different_names = (
        selection(10, row(10, "current_primary", "x", crd="10", name="Same Provider")),
        selection(11, row(11, "current_primary", "y", crd="11", name="Same Provider")),
    )
    assert len(projection(different_names, "provider_only").components) == 2

    invalid = (
        selection(12, row(12, "current_primary", "bad-x", file_number="bad", crd="0", lei="bad", name="Null Hub")),
        selection(13, row(13, "current_primary", "bad-y", file_number="bad", crd="0", lei="bad", name="Null Hub")),
    )
    invalid_result = projection(invalid, "provider_only")
    assert len(invalid_result.components) == 2
    assert all(incidence.key_id is None for incidence in invalid_result.incidences)
    assert not invalid_result.enabled_incidences
    with pytest.raises(ncen.NcenError, match="registrant_role_scope_invalid"):
        row(14, "underwriter", "bad-scope", crd="14")
    with pytest.raises(ncen.NcenError, match="series_id_not_normalized"):
        row(15, "current_primary", "bad-series", crd="15", series_id="")
    with pytest.raises(ncen.NcenError, match="unresolved_scope_has_series"):
        row(
            16,
            "current_primary",
            "bad-unresolved",
            crd="16",
            series_id="S000000001",
            series_scope="unresolved",
        )


def test_service_provider_chain() -> None:
    selected = (
        selection(1, row(1, "current_primary", "a-x", crd="11"), row(1, "b5", "a-b5", answer="Y", name="A")),
        selection(
            2,
            row(2, "current_primary", "b-x", crd="11"),
            row(2, "current_sub", "b-y", crd="22"),
            row(2, "b5", "b-b5", answer="Y", name="B"),
        ),
        selection(3, row(3, "underwriter", "c-y", crd="22", series_id=None, series_scope="registrant"), row(3, "b5", "c-b5", answer="Y", name="C")),
    )
    built = [node_and_incidences(item) for item in selected]
    nodes = tuple(item[0] for item in built)
    incidences = tuple(incidence for item in built for incidence in item[1])
    result = projection(selected, "full_observed")
    assert component_members(result) == bfs_partition(nodes, incidences) == {
        frozenset({cik(1), cik(2), cik(3)})
    }
    assert len(result.spanning_unions) == 2
    assert result.successful_unions == 2
    spanning_graph = {node.cik: set() for node in nodes}
    for witness in result.spanning_unions:
        spanning_graph[witness.left_cik].add(witness.right_cik)
        spanning_graph[witness.right_cik].add(witness.left_cik)
    reached = {cik(1)}
    queue = deque([cik(1)])
    while queue:
        current = queue.popleft()
        for neighbour in spanning_graph[current] - reached:
            reached.add(neighbour)
            queue.append(neighbour)
    assert reached == {cik(1), cik(2), cik(3)}
    assert all(node.reported_family is not None for node in nodes)
    assert {node.reported_family.label_id for node in nodes if node.reported_family is not None} == {
        ncen.reported_family_for(item).label_id for item in selected
    }


def test_incomplete_bridge_is_default_but_never_vote() -> None:
    a = selection(1, row(1, "current_primary", "a-x", crd="11"), row(1, "b5", "a-b5", answer="N"))
    b = selection(2, row(2, "current_primary", "b-y", crd="22"), row(2, "b5", "b-b5", answer="N"))
    bridge = selection(
        3,
        row(3, "current_sub", "c-x", crd="11"),
        row(3, "underwriter", "c-y", crd="22", series_id=None, series_scope="registrant"),
        complete=False,
    )
    full = projection((a, b, bridge))
    assert component_members(full) == {frozenset({cik(1), cik(2), cik(3)})}
    assert full.components[0].has_unknown_dependence
    assert all(not node.independent_vote_eligible for node in full.nodes)
    without = projection((a, b, bridge), "full_without_incomplete_edges")
    assert component_members(without) == {frozenset({cik(1)}), frozenset({cik(2)}), frozenset({cik(3)})}

    identifierless = selection(
        4,
        row(4, "current_primary", "d-x", name="Provider X"),
        row(4, "current_sub", "d-y", name="Provider Y"),
        complete=False,
    )
    no_bridge = projection((a, b, identifierless), "uncertain_expanded")
    assert len(no_bridge.components) == 3


def test_snapshot_track_edge_and_fold_ids() -> None:
    base = context_id(R, K)
    canonical_context = [
        ncen.DIAGNOSTIC_SCHEMA_VERSION,
        {
            "reported_family": ncen.DIAGNOSTIC_REPORTED_FAMILY_VERSION,
            "reporting_dependence_block": ncen.DIAGNOSTIC_DEPENDENCE_VERSION,
        },
        ncen.DIAGNOSTIC_EDGE_VERSION,
        ncen.DIAGNOSTIC_SELECTION_VERSION,
        ncen.DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        R.isoformat(),
        K.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "historical_reconstruction",
        "inventory-a",
        "2" * 64,
        "3" * 64,
        "4" * 64,
    ]
    expected_context = "ncenctx:" + hashlib.sha256(
        json.dumps(
            canonical_context,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert base == expected_context
    assert base != context_id(R + dt.timedelta(days=1), K)
    assert base != context_id(R, K + dt.timedelta(seconds=1))
    assert base != context_id(R, K, mode="current_run")
    assert base != context_id(R, K, inventory="inventory-b")
    assert base == context_id(R, K.astimezone(dt.timezone(dt.timedelta(hours=2))))

    first = selection(1, row(1, "current_primary", "a", crd="11"))
    second = selection(2, row(2, "current_primary", "b", crd="11"))
    primary = projection((first, second), "provider_only").components[0]
    changed_role = projection(
        (first, selection(2, row(2, "current_sub", "b", crd="11"))), "provider_only"
    ).components[0]
    changed_evidence = projection(
        (selection(1, row(1, "current_primary", "a-new", crd="11")), second), "provider_only"
    ).components[0]
    assert primary.membership_track_id == changed_role.membership_track_id == changed_evidence.membership_track_id
    assert primary.observed_edge_version_id != changed_role.observed_edge_version_id
    assert primary.evidence_digest != changed_evidence.evidence_digest

    other_context = ncen.snapshot_component_id(
        context_id="ncenctx:" + "9" * 64,
        ablation_id="provider_only",
        members=primary.members,
    )
    assert other_context != primary.snapshot_component_id
    namespaces = {
        primary.snapshot_component_id.split(":", 1)[0],
        primary.membership_track_id.split(":", 1)[0],
        primary.observed_edge_version_id.split(":", 1)[0],
        ncen.fold_scope_id(
            mode="historical_reconstruction",
            ablation_id="provider_only",
            contexts=((R, K, "inventory-a", base),),
        ).split(":", 1)[0],
    }
    assert namespaces == {"ncenblock", "ncentrack", "ncenedges", "ncenfoldscope"}
    assert "ncenfam" not in namespaces


def test_temporal_union_cross_date_provider_and_bridge() -> None:
    s1 = snapshot(
        dt.date(2026, 1, 31),
        (selection(1, row(1, "current_primary", "a-x", crd="11")),),
        cutoff=K,
        inventory="one",
        ablation_id="provider_only",
    )
    s2 = snapshot(
        dt.date(2026, 2, 28),
        (selection(2, row(2, "current_sub", "b-x", crd="11")),),
        cutoff=K + dt.timedelta(days=1),
        inventory="two",
        ablation_id="provider_only",
    )
    s3 = snapshot(
        dt.date(2026, 3, 31),
        (
            selection(2, row(2, "current_primary", "b-y", crd="22")),
            selection(3, row(3, "underwriter", "c-y", crd="22", series_id=None, series_scope="registrant"), complete=False),
        ),
        cutoff=K + dt.timedelta(days=2),
        inventory="three",
        ablation_id="provider_only",
    )
    contexts = tuple(
        ncen.FoldContext(
            report_date=item.report_date,
            knowledge_cutoff=item.knowledge_cutoff,
            inventory_digest=item.inventory_digest,
            context_id=item.projection.context_id,
        )
        for item in (s1, s2, s3)
    )
    scope = ncen.TemporalFoldScope(
        mode="historical_reconstruction", ablation_id="provider_only", contexts=contexts
    )
    folded = ncen.build_temporal_dependence_union((s1, s2, s3), fold_scope=scope)
    assert len(folded.groups) == 1
    assert folded.groups[0].members == (cik(1), cik(2), cik(3))
    assert folded.groups[0].has_unknown_dependence
    assert not folded.groups[0].usable_for_independence_claim

    transitions = ncen.build_dependence_transitions((s1, s2, s3))
    assert [(item.kind, item.intersection_count, item.union_count) for item in transitions] == [
        ("exit", 1, 1),
        ("entry", 1, 1),
        ("overlap", 1, 2),
        ("entry", 1, 2),
    ]
    with pytest.raises(ncen.NcenError, match="fold_context_schedule_mismatch"):
        ncen.build_temporal_dependence_union((s1, s3), fold_scope=scope)
    with pytest.raises(ncen.NcenError, match="diagnostic_projection_unadmitted"):
        dataclasses.replace(s2, mode="current_run")
    alternate = snapshot(
        dt.date(2026, 2, 28), (selection(2, row(2, "current_sub", "b-x", crd="11")),),
        cutoff=K + dt.timedelta(days=1), inventory="two", ablation_id="provider_only",
        mode="current_run",
    )
    mixed_scope = ncen.TemporalFoldScope(
        mode="historical_reconstruction", ablation_id="provider_only",
        contexts=tuple(ncen.FoldContext(item.report_date, item.knowledge_cutoff,
                                        item.inventory_digest, item.projection.context_id)
                       for item in (s1, alternate, s3)),
    )
    with pytest.raises(ncen.NcenError, match="fold_mode_or_ablation_mismatch"):
        ncen.build_temporal_dependence_union(
            (s1, alternate, s3), fold_scope=mixed_scope
        )


def test_review_f09_fold_attested_only_in_all_paths() -> None:
    uncertain = selection(
        1, row(1, "current_primary", "f09-uncertain", crd="11"),
        selection_reason=ncen.ORDER_UNRESOLVED,
    )
    attested = selection(2, row(2, "current_primary", "f09-attested", crd="11"))
    first_date = dt.date(2026, 1, 31)
    second_date = dt.date(2026, 2, 28)
    for spec in ncen.DIAGNOSTIC_ABLATIONS:
        first = snapshot(first_date, (uncertain,), cutoff=K, inventory="f09-one",
                         ablation_id=spec.ablation_id)
        second = snapshot(second_date, (attested,), cutoff=K + dt.timedelta(days=1),
                          inventory="f09-two", ablation_id=spec.ablation_id)
        scope = ncen.TemporalFoldScope(
            mode="historical_reconstruction", ablation_id=spec.ablation_id,
            contexts=tuple(ncen.FoldContext(item.report_date, item.knowledge_cutoff,
                                            item.inventory_digest, item.projection.context_id)
                           for item in (first, second)),
        )
        folded = ncen.build_temporal_dependence_union((first, second), fold_scope=scope)
        assert tuple(group.members for group in folded.groups) == ((cik(1),), (cik(2),))
        assert tuple(group.has_unknown_dependence for group in folded.groups) == (True, False)
        assert folded.incidence_count == folded.union_attempts == int("current_primary" in spec.provider_roles)
        assert folded.successful_unions == folded.incidence_count
        assert all(not group.usable_for_independence_claim for group in folded.groups)

        accumulator = ncen._PurposeFoldAccumulator(spec)
        accumulator.add(first)
        accumulator.add(second)
        groups, memberships = accumulator.records()
        assert {item["fold_group_id"]: (item["member_count"], item["has_unknown_dependence"])
                for item in groups} == {
                    group.fold_group_id: (group.member_count, group.has_unknown_dependence)
                    for group in folded.groups
                }
        assert {(item["fold_group_id"], item["cik"]) for item in memberships} == {
            (group.fold_group_id, member) for group in folded.groups for member in group.members
        }

        positive_first = snapshot(
            first_date, (selection(1, row(1, "current_primary", "f09-positive", crd="11")),),
            cutoff=K, inventory="f09-one", ablation_id=spec.ablation_id,
        )
        positive_scope = ncen.TemporalFoldScope(
            mode="historical_reconstruction", ablation_id=spec.ablation_id,
            contexts=(ncen.FoldContext(positive_first.report_date, positive_first.knowledge_cutoff,
                                      positive_first.inventory_digest, positive_first.projection.context_id),
                      scope.contexts[1]),
        )
        positive = ncen.build_temporal_dependence_union((positive_first, second),
                                                         fold_scope=positive_scope)
        expected_members = ((cik(1), cik(2)),) if "current_primary" in spec.provider_roles else (
            (cik(1),), (cik(2),))
        assert tuple(group.members for group in positive.groups) == expected_members
        assert positive.incidence_count == positive.union_attempts == (
            2 if "current_primary" in spec.provider_roles else 0)

    together = snapshot(
        first_date, (uncertain, attested), cutoff=K, inventory="f09-together",
        ablation_id="uncertain_expanded",
    )
    assert component_members(together.projection) == {frozenset({cik(1), cik(2)})}
    one_scope = ncen.TemporalFoldScope(
        mode="historical_reconstruction", ablation_id="uncertain_expanded",
        contexts=(ncen.FoldContext(together.report_date, together.knowledge_cutoff,
                                   together.inventory_digest, together.projection.context_id),),
    )
    one_fold = ncen.build_temporal_dependence_union((together,), fold_scope=one_scope)
    assert tuple(group.members for group in one_fold.groups) == ((cik(1),), (cik(2),))
    assert one_fold.incidence_count == one_fold.union_attempts == 1


def test_temporal_union_streams_declared_contexts(monkeypatch: pytest.MonkeyPatch) -> None:
    s1 = snapshot(
        dt.date(2026, 1, 31),
        (
            selection(
                1,
                row(
                    1,
                    "current_primary",
                    "uncertain-x",
                    crd="11",
                ),
                selection_reason=ncen.ORDER_UNRESOLVED,
            ),
        ),
        cutoff=K,
        inventory="stream-one",
        ablation_id="provider_only",
    )
    s2 = snapshot(
        dt.date(2026, 2, 28),
        (selection(2, row(2, "current_primary", "attested-x", crd="11")),),
        cutoff=K + dt.timedelta(days=1),
        inventory="stream-two",
        ablation_id="provider_only",
    )
    scope = ncen.TemporalFoldScope(
        mode="historical_reconstruction",
        ablation_id="provider_only",
        contexts=tuple(
            ncen.FoldContext(
                item.report_date,
                item.knowledge_cutoff,
                item.inventory_digest,
                item.projection.context_id,
            )
            for item in (s1, s2)
        ),
    )
    produced = 0

    def stream():
        nonlocal produced
        for item in (s1, s2):
            produced += 1
            yield item

    first_add_at: list[int] = []
    original_add = ncen._DiagnosticUnionFind.add

    def tracked_add(self, member):
        if not first_add_at:
            first_add_at.append(produced)
        return original_add(self, member)

    monkeypatch.setattr(ncen._DiagnosticUnionFind, "add", tracked_add)
    folded = ncen.build_temporal_dependence_union(stream(), fold_scope=scope)
    assert first_add_at == [1]
    assert any(group.members == (cik(1),) and group.has_unknown_dependence for group in folded.groups)


def test_permutations_and_hashseeds(tmp_path: Path) -> None:
    fixture = tmp_path / "purpose_fixture.json"
    fixture.write_text(
        json.dumps(
            [
                {"cik": 1, "role": "current_primary", "locator": "a-x", "crd": "11"},
                {"cik": 2, "role": "current_sub", "locator": "b-x", "crd": "11"},
                {"cik": 2, "role": "current_primary", "locator": "b-y", "crd": "22"},
                {"cik": 3, "role": "underwriter", "locator": "c-y", "crd": "22"},
            ],
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    runner = r'''
import datetime as dt
import json, sys
from collections import defaultdict
from pathlib import Path
from src.bonds.default_events import ncen
raw = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
cutoff = dt.datetime(2026, 9, 25, 18, tzinfo=dt.timezone.utc)
unordered = {json.dumps(item, sort_keys=True) for item in raw}
grouped = defaultdict(list)
for encoded in unordered:
    item = json.loads(encoded)
    number = item["cik"]
    grouped[number].append(ncen.DiagnosticSourceRow(
        accession_number="0000000001-26-000001", registrant_cik=f"{number:010d}",
        role=item["role"], locator=item["locator"], series_id=None if item["role"] == "underwriter" else "S000000001",
        series_scope="registrant" if item["role"] == "underwriter" else "series", crd_raw=item["crd"],
        public_at=cutoff - dt.timedelta(days=1), data_known_at=cutoff - dt.timedelta(days=1),
        retrieved_at=cutoff - dt.timedelta(days=1),
    ))
selected = tuple(ncen.diagnostic_fixture_selection(
    cik=f"{number:010d}", accession_number="0000000001-26-000001", rows=(*rows, rows[0]),
    report_date=dt.date(2026, 3, 31), knowledge_cutoff=cutoff,
    mode="historical_reconstruction",
) for number, rows in sorted(grouped.items()))
context = ncen.diagnostic_fixture_context(selected, report_date=dt.date(2026, 3, 31), knowledge_cutoff=cutoff)
result = ncen.project_dependence(context.nodes, context.incidences, spec=ncen.ablation_spec("provider_only"), admission=context)
sys.stdout.buffer.write(result.canonical_bytes())
'''
    outputs = []
    for seed in (1, 2, 977, 31337):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = str(seed)
        completed = subprocess.run(
            [sys.executable, "-c", runner, str(fixture)],
            cwd=ROOT,
            env=env,
            capture_output=True,
            check=True,
            timeout=60,
        )
        outputs.append(completed.stdout)
    assert all(output == outputs[0] for output in outputs)


def test_streaming_high_degree_envelope() -> None:
    count = 2_000
    star_selections = tuple(selection(number, row(number, "current_primary", f"star-{number}", crd="11"))
                            for number in range(1, count + 1))
    star_result = projection(star_selections, "provider_only")
    assert len(star_result.components) == 1
    assert star_result.union_attempts == star_result.successful_unions == count - 1
    assert len(star_result.spanning_unions) == count - 1
    assert len(star_result.spanning_unions) < len(star_result.incidences) * 2

    chain_count = 1_200
    chain_rows = defaultdict(list)
    for number in range(1, chain_count):
        for member in (number, number + 1):
            chain_rows[member].append(row(member, "current_primary", f"chain-{number}-{member}", crd=str(10_000 + number)))
    chain_result = projection(tuple(selection(member, *chain_rows[member])
                                    for member in range(1, chain_count + 1)), "provider_only")
    assert len(chain_result.components) == 1
    assert chain_result.union_attempts == chain_count - 1
    assert chain_result.successful_unions == chain_count - 1
    assert len(chain_result.spanning_unions) == chain_count - 1
    assert len(chain_result.spanning_unions) < len(chain_result.incidences)


def test_pure_core_strict_validation_and_fixed_ablations() -> None:
    assert tuple(spec.ablation_id for spec in ncen.DIAGNOSTIC_ABLATIONS) == (
        "full_observed",
        "b5_only",
        "primary_only",
        "sub_only",
        "terminated_only",
        "underwriter_only",
        "provider_only",
        "full_without_incomplete_edges",
        "uncertain_expanded",
    )
    forged = dataclasses.replace(ncen.ablation_spec("provider_only"), include_b5=True)
    with pytest.raises(ncen.NcenError, match="ablation_not_declared"):
        ncen.project_dependence((), (), spec=forged)
    with pytest.raises(ncen.NcenError, match="datetime_not_timezone_aware"):
        context_id(R, K.replace(tzinfo=None))
    with pytest.raises(ncen.NcenError, match="reasons_not_sorted_unique"):
        ncen.DependenceNode(CTX, cik(1), "incomplete", ("z", "a"), None)
    uncertain = selection(
        1,
        row(1, "current_primary", "uncertain", crd="11"),
        selection_reason=ncen.ORDER_UNRESOLVED,
    )
    anchor = selection(2, row(2, "current_primary", "anchor", crd="11"))
    observed = projection((uncertain, anchor), "full_observed")
    expanded = projection((uncertain, anchor), "uncertain_expanded")
    assert len(observed.components) == 2
    assert next(item for item in observed.components if item.members == (cik(1),)).has_unknown_dependence
    assert len(expanded.components) == 1 and expanded.components[0].has_unknown_dependence
    ref = ncen.BondIssuerGroupRef()
    assert dataclasses.asdict(ref) == {
        "purpose": "bond_issuer_group",
        "state": "unavailable",
        "reason": "light_a1_inventory_not_bound",
        "authority": "Light A1",
        "source_module": "backend/app/bond_optimizer/issuer_groups.py",
        "inventory_digest": None,
        "policy_digest": None,
        "as_of": None,
        "temporal_qualification": "NOT_EVALUABLE",
    }


def test_transition_split_merge_linear_envelope() -> None:
    count = 100
    connected = snapshot(
        dt.date(2026, 1, 31),
        tuple(
            selection(number, row(number, "current_primary", f"joined-{number}", crd="11"))
            for number in range(1, count + 1)
        ),
        cutoff=K,
        inventory="joined",
        ablation_id="provider_only",
    )
    split = snapshot(
        dt.date(2026, 2, 28),
        tuple(selection(number) for number in range(1, count + 1)),
        cutoff=K + dt.timedelta(days=1),
        inventory="split",
        ablation_id="provider_only",
    )
    transitions = ncen.build_dependence_transitions(iter((connected, split)))
    assert len(transitions) == count
    assert all(item.kind == "overlap" for item in transitions)
    assert all(item.intersection_count == 1 and item.union_count == count for item in transitions)


def test_outcome_blind_and_no_accepting_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("accepting or outcome path called")

    monkeypatch.setattr(ncen, "build_consensus_with_ncen", forbidden)
    monkeypatch.setattr(ncen, "diagnostic_per_state_components", forbidden)
    monkeypatch.setattr(ncen.VoteInventory, "target_votes", forbidden)
    selected = selection(1, row(1, "b5", "b5", answer="Y", name="Acme"))
    family = ncen.reported_family_for(selected)
    result = projection((selected,))
    assert not isinstance(family, nport.FamilyEvidence)
    assert not isinstance(result.components[0], nport.FamilyEvidence)
    assert "target" not in inspect.signature(ncen.project_dependence).parameters
    assert "outcome" not in inspect.signature(ncen.project_dependence).parameters
    with pytest.raises(TypeError):
        dataclasses.replace(family, family_id="ncenfam:forbidden")


def test_frozen_v3_and_prep1_unchanged() -> None:
    old_ncen_bytes = 95_275
    old_ncen_sha = "41521014f92ba1baf7ed15e4235005438e2c6bc52a6f47851bbb16bf184a9b29"
    old_test_sha = "a9bceb6cec88d3e9a4d3dd553d26351d231ffb9a8d3085ae4648b8e74cc8a026"
    source = Path(ncen.__file__).read_bytes()
    assert hashlib.sha256(source[:old_ncen_bytes]).hexdigest() == old_ncen_sha
    assert hashlib.sha256((ROOT / "tests" / "test_bond_default_ncen.py").read_bytes()).hexdigest() == old_test_sha
    assert source[old_ncen_bytes:].startswith(b"\n\n# === FE-1 purpose-labelled diagnostic core")


def test_review_f04_graph_requires_context_admission(tmp_path: Path) -> None:
    raw = row(1, "current_primary", "f04-primary", crd="11")
    missing_time = dataclasses.replace(raw, public_at=None)
    future = dataclasses.replace(raw, locator="f04-future", public_at=K + dt.timedelta(seconds=1))
    unheld = dataclasses.replace(raw, locator="f04-unheld", retrieved_at=None)
    quarantined = dataclasses.replace(raw, locator="f04-quarantined", custody_state="quarantined")
    with pytest.raises(ncen.NcenError, match="diagnostic_fixture_raw_row_invalid"):
        ncen.diagnostic_fixture_selection(
            cik=cik(1), accession_number=ACCESSION,
            rows=(dataclasses.replace(raw, attestation="uncertain", reasons=("diagnostic_order_unresolved",), uncertain_expansion_eligible=True),),
            report_date=R, knowledge_cutoff=K, mode="current_run",
        )
    selected = ncen.diagnostic_fixture_selection(
        cik=cik(1), accession_number=ACCESSION,
        rows=(raw, missing_time, future, unheld, quarantined),
        report_date=R, knowledge_cutoff=K, mode="current_run",
    )
    assert len(selected.rows) == 1
    assert set(selected.excluded_source_row_ids) == {
        item.source_row_id for item in (missing_time, future, unheld, quarantined)
    }
    assert not selected.rows[0].uncertain_expansion_eligible
    with pytest.raises(ncen.NcenError, match="diagnostic_selection_unadmitted"):
        ncen.typed_incidences_for(ncen.DiagnosticSelection.from_rows(
            cik=cik(1), accession_number=ACCESSION, rows=(raw,),
            evidence_state="complete", reasons=(),
        ), context_id=CTX)
    for unbound in (dataclasses.replace(selected), pickle.loads(pickle.dumps(selected))):
        with pytest.raises(ncen.NcenError, match="diagnostic_selection_unadmitted"):
            ncen.typed_incidences_for(unbound, context_id=CTX)

    bridge = ncen.diagnostic_fixture_selection(
        cik=cik(2), accession_number=ACCESSION,
        rows=(row(2, "current_sub", "f04-bridge", crd="11"),),
        report_date=R, knowledge_cutoff=K, mode="current_run",
        reasons=("voting_series_absent_from_effective_ncen",),
    )
    context = ncen.diagnostic_fixture_context(
        (selected, bridge), report_date=R, knowledge_cutoff=K, mode="current_run",
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_admission_missing"):
        ncen.project_dependence(context.nodes, context.incidences, spec=ncen.ablation_spec("full_observed"), admission=None)
    full = ncen.project_dependence(context.nodes, context.incidences, spec=ncen.ablation_spec("full_observed"), admission=context)
    without = ncen.project_dependence(context.nodes, context.incidences, spec=ncen.ablation_spec("full_without_incomplete_edges"), admission=context)
    assert component_members(full) == {frozenset({cik(1), cik(2)})}
    assert component_members(without) == {frozenset({cik(1)}), frozenset({cik(2)})}
    assert all(not node.independent_vote_eligible for node in full.nodes)
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_node_set_mismatch"):
        ncen.project_dependence((context.nodes[0],), context.incidences, spec=ncen.ablation_spec("full_observed"), admission=context)
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_incidence_set_mismatch"):
        ncen.project_dependence(context.nodes, (), spec=ncen.ablation_spec("full_observed"), admission=context)
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_incidence_set_mismatch"):
        ncen.project_dependence(context.nodes, (*context.incidences, context.incidences[0]),
                                spec=ncen.ablation_spec("full_observed"), admission=context)
    for nodes in (
        (*context.nodes, ncen.DependenceNode(context.context_id, cik(3), "complete", (), None)),
        (dataclasses.replace(context.nodes[0], evidence_state="incomplete", reasons=("missing",)), context.nodes[1]),
        (dataclasses.replace(context.nodes[0], context_id=CTX), context.nodes[1]),
    ):
        with pytest.raises(ncen.NcenError, match="diagnostic_graph_node_set_mismatch|diagnostic_context_mixed"):
            ncen.project_dependence(nodes, context.incidences, spec=ncen.ablation_spec("full_observed"), admission=context)
    for incidences in (
        (*context.incidences, dataclasses.replace(context.incidences[0], source_row_id="ncenrow:source_row:" + "f" * 64)),
        (dataclasses.replace(context.incidences[0], attestation="uncertain", reasons=("missing",)), *context.incidences[1:]),
        (dataclasses.replace(context.incidences[0], context_id=CTX), *context.incidences[1:]),
    ):
        with pytest.raises(ncen.NcenError, match="diagnostic_graph_incidence_set_mismatch|incidence_context_mismatch"):
            ncen.project_dependence(context.nodes, incidences, spec=ncen.ablation_spec("full_observed"), admission=context)

    class ForgedIncidence(ncen.TypedIncidence):
        @property
        def incidence_id(self) -> str:
            return context.incidences[0].incidence_id

    forged = ForgedIncidence(**{item.name: getattr(context.incidences[0], item.name)
                                 for item in dataclasses.fields(ncen.TypedIncidence)})
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_carrier_type_invalid"):
        ncen.project_dependence(context.nodes, (forged, *context.incidences[1:]),
                                spec=ncen.ablation_spec("full_observed"), admission=context)
    other = ncen.diagnostic_fixture_context((selected, bridge), report_date=R, knowledge_cutoff=K,
                                            mode="current_run", inventory_digest="different-inventory")
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_node_set_mismatch"):
        ncen.project_dependence(context.nodes, context.incidences,
                                spec=ncen.ablation_spec("full_observed"), admission=other)
    with pytest.raises(ncen.NcenError, match="selection_source_identity_mismatch"):
        ncen.diagnostic_fixture_context((selected, dataclasses.replace(bridge, cik=cik(3))),
                                        report_date=R, knowledge_cutoff=K, mode="current_run")
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_admission_missing"):
        ncen.diagnostic_fixture_context((selected, bridge), report_date=R,
                                        knowledge_cutoff=K + dt.timedelta(seconds=1), mode="current_run")
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_admission_missing"):
        ncen.diagnostic_fixture_context((selected, bridge), report_date=R,
                                        knowledge_cutoff=K, mode="historical_reconstruction")
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_admission_missing"):
        ncen.project_dependence(context.nodes, context.incidences,
                                spec=ncen.ablation_spec("full_observed"), admission=pickle.loads(pickle.dumps(context)))
    with pytest.raises(ncen.NcenError, match="diagnostic_projection_unadmitted"):
        ncen.DependenceSnapshot(R, K, "current_run", context.inventory_digest, dataclasses.replace(full))
    with pytest.raises(ncen.NcenError, match="diagnostic_projection_unadmitted"):
        ncen.DependenceSnapshot(R, K, "current_run", context.inventory_digest, pickle.loads(pickle.dumps(full)))
    with pytest.raises(ncen.NcenError, match="diagnostic_fixture_lane_not_exportable"):
        ncen.PurposeContextSnapshot(
            context.context_id, R, K, "current_run", context.inventory_digest,
            (), context.selections, context.reported_families, context.nodes,
            context.incidences, tuple(
                ncen.project_dependence(context.nodes, context.incidences, spec=spec, admission=context)
                for spec in ncen.DIAGNOSTIC_ABLATIONS
            ),
        )

    sealed = ncen.read_diagnostic_source_rows(_stage2a_fixture(tmp_path / "file-backed"))
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    index = _stage2a_index(acceptance_at=accepted, header_retrieved_at=accepted,
                           retrieved_at=accepted, data_known_at=accepted)
    selected_file = ncen.diagnostic_selection(index, sealed, cik(1), R, K, mode="current_run")
    assert ncen._diagnostic_bound_selection_admission(selected_file).lane == "synthetic_fixture"
    fund_keys = ("S000000099",)
    bound_gaps = ncen.diagnostic_selection(index, sealed, cik(1), R, K,
                                           mode="current_run", fund_keys=fund_keys)
    previous_gaps = ncen._purpose_apply_voting_gaps(index, selected_file, fund_keys)
    assert (bound_gaps.rows, bound_gaps.reasons, bound_gaps.evidence_state) == (
        previous_gaps.rows, previous_gaps.reasons, previous_gaps.evidence_state)
    assert ncen._diagnostic_bound_selection_admission(bound_gaps).lane == "synthetic_fixture"
    with pytest.raises(ncen.NcenError, match="diagnostic_fixture_lane_not_exportable"):
        ncen.diagnostic_fixture_context((bound_gaps,), report_date=R,
                                        knowledge_cutoff=K, mode="current_run")
    with pytest.raises(ncen.NcenError, match="diagnostic_graph_admission_missing"):
        ncen._diagnostic_build_context(
            (bound_gaps, bridge), report_date=R, knowledge_cutoff=K, mode="current_run",
            inventory_digest="fixture", cohort_digest="2" * 64,
            ncen_evidence_digest=sealed.evidence_digest,
            exclusion_ledger_digest="4" * 64,
        )


def _stage2a_timestamp(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _stage2a_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# === Synthetic N-CEN/A acquisition seals (same schema as the real acquisition run) ==========
# Every quarantine ledger and boundary example is now derived by the loader from a sealed
# acquisition run. Fixtures therefore write a small but complete seal: scope, terminal rows,
# schema evidence, raw headers, header records, raw XML, coverage, SHA256SUMS and receipt. Its
# anchors are its own actual byte hashes, never the real run's.

_ACQ_AMENDED = "0000000001-25-000001"
_ACQ_ACCEPTED = dt.datetime(2026, 3, 2, 12, tzinfo=UTC)
_ACQ_PERIOD = dt.date(2025, 12, 31)
_ACQ_REASONS: dict[str, tuple[str, ...]] = {
    "absent_amended_accession": ("amended_accession_invalid",),
    "schema_unavailable": ("schema_element_count:0",),
    "projection_conflict": ("xml_dera_projection_conflict",),
    "multiple": ("amended_accession_invalid", "schema_element_count:0"),
    "boundary": (),
    "verified": (),
}
_ACQ_BOUNDARY_REASONS = ["diagnostic_not_admitted_by_example"]


@dataclasses.dataclass(frozen=True)
class _AcqEntry:
    accession: str
    registrant: str
    kind: str
    acceptance: dt.datetime = _ACQ_ACCEPTED
    period: dt.date = _ACQ_PERIOD
    schema: str = "X0404"


@dataclasses.dataclass(frozen=True)
class _AcqSeal:
    root: Path
    pin: ncen.DiagnosticAcquisitionPin
    quarantines: tuple[dict[str, object], ...]
    boundaries: tuple[dict[str, object], ...]


_ACQ_DEFAULT_ENTRIES = (
    _AcqEntry("0000000999-26-010000", cik(10_000), "absent_amended_accession"),
    _AcqEntry("0000000999-26-010001", cik(10_001), "schema_unavailable"),
    _AcqEntry("0000000999-26-010002", cik(10_002), "projection_conflict"),
    _AcqEntry("0000000888-26-000000", cik(20_000), "boundary", schema="X0505"),
    _AcqEntry("0000000777-26-000000", cik(30_000), "verified"),
)
_ACQ_DEFAULT_QUARANTINES = 3


def _acq_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n").encode()


def _acq_edgar_time(value: dt.datetime) -> str:
    from zoneinfo import ZoneInfo

    assert value.microsecond == 0, "EDGAR acceptance has one-second resolution"
    return value.astimezone(ZoneInfo("America/New_York")).strftime("%Y%m%d%H%M%S")


def _acq_xml(entry: _AcqEntry) -> bytes:
    no_schema = entry.kind in {"schema_unavailable", "multiple"}
    no_amended = entry.kind in {"absent_amended_accession", "multiple"}
    schema = "" if no_schema else f"<schemaVersion>{entry.schema}</schemaVersion>"
    amended = "" if no_amended else f"<accessionNumber>{_ACQ_AMENDED}</accessionNumber>"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<edgarSubmission xmlns="{ncen.NCEN_NAMESPACE}">{schema}<headerData>'
        f"<submissionType>N-CEN/A</submissionType>{amended}<filerInfo><filer><issuerCredentials>"
        f"<cik>{entry.registrant}</cik></issuerCredentials></filer></filerInfo></headerData>"
        f'<formData><generalInfo reportEndingPeriod="{entry.period.isoformat()}"/><registrantInfo>'
        f"<registrantCik>{entry.registrant}</registrantCik>"
        '<registrantFamilyInvComp isRegistrantFamilyInvComp="Y" familyInvCompFullName="Acme Funds"/>'
        "</registrantInfo></formData></edgarSubmission>"
    ).encode()


def _acq_quarantine_record(entry: _AcqEntry) -> dict[str, object]:
    return {
        "accession_number": entry.accession,
        "registrant_cik": entry.registrant,
        "form_type": "N-CEN/A",
        "report_period_end": entry.period.isoformat(),
        "acceptance_at": _stage2a_timestamp(entry.acceptance),
        "classification": entry.kind,
        "reasons": list(_ACQ_REASONS[entry.kind]),
    }


def _acq_boundary_record(entry: _AcqEntry) -> dict[str, object]:
    return {
        "accession_number": entry.accession,
        "schema_version": entry.schema,
        "form_type": "N-CEN/A",
        "acceptance_at": _stage2a_timestamp(entry.acceptance),
        "policy_state": "example_only",
        "reasons": list(_ACQ_BOUNDARY_REASONS),
    }


def _acq_reseal(root: Path) -> ncen.DiagnosticAcquisitionPin:
    """(Re)write SHA256SUMS and its receipt over every file, like the acquisition ``seal``."""
    seal_files = {"SHA256SUMS", "SHA256SUMS.receipt.json"}
    files = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.relative_to(root).as_posix() not in seal_files),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    sums = "".join(f"{_stage2a_sha(path)}  {path.relative_to(root).as_posix()}\n" for path in files).encode()
    (root / "SHA256SUMS").write_bytes(sums)
    scope = json.loads((root / "scope.json").read_bytes())
    receipt = {
        "coverage_sha256": _stage2a_sha(root / "coverage.json"),
        "manifest_bytes": sum(path.stat().st_size for path in files),
        "manifest_entries": len(files),
        "schema": "bond_ncen_amendment_acquisition_receipt_v1",
        "schema_evidence_rows": len(scope["requests"]),
        "scope_sha256": _stage2a_sha(root / "scope.json"),
        "sealed_at": "2026-09-25T17:28:45.000000Z",
        "sha256sums_sha256": hashlib.sha256(sums).hexdigest(),
    }
    (root / "SHA256SUMS.receipt.json").write_bytes(_acq_json(receipt))
    return ncen.DiagnosticAcquisitionPin(
        root, _stage2a_sha(root / "scope.json"), hashlib.sha256(sums).hexdigest(), "synthetic_fixture"
    )


def _acquisition_seal(root: Path, entries: tuple[_AcqEntry, ...] = _ACQ_DEFAULT_ENTRIES) -> _AcqSeal:
    """Write one complete synthetic acquisition seal and return its caller-side pin."""
    from src.bonds.default_events import sec_acquisition

    root.mkdir(parents=True)
    requests: list[dict[str, object]] = []
    rows: list[dict[str, object]] = []
    quarantines: list[dict[str, object]] = []
    boundaries: list[dict[str, object]] = []
    ordered = sorted(entries, key=lambda item: item.accession)
    for number, entry in enumerate(ordered, start=1):
        accession = entry.accession
        folder = accession.replace("-", "")
        edgar = _acq_edgar_time(entry.acceptance)
        header_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(entry.registrant)}/{folder}/{accession}-index-headers.html"
        )
        raw_header = (
            "<SEC-HEADER>\n"
            f"<ACCESSION-NUMBER>{accession}\n"
            f"<ACCEPTANCE-DATETIME>{edgar}\n"
            "<TYPE>N-CEN/A\n"
            f"<CIK>{entry.registrant}\n"
            f"<FILING-DATE>{entry.acceptance.strftime('%Y%m%d')}\n"
            "</SEC-HEADER>\n"
        ).encode()
        header_retrieved = dt.datetime(2026, 9, 25, 7, tzinfo=UTC)
        header = sec_acquisition.parse_acceptance_header(
            raw_header,
            accession_number=accession,
            url=header_url,
            document_sha256=hashlib.sha256(raw_header).hexdigest(),
            retrieved_at=header_retrieved,
        )
        record = json.dumps(header.to_record(), sort_keys=True, indent=2).encode()
        xml = _acq_xml(entry)
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(entry.registrant)}/{folder}/primary_doc.xml"
        xml_retrieved = dt.datetime(2026, 9, 25, 16, tzinfo=UTC)
        parsed = ncen.parse_ncen_primary_doc(
            xml, accession_number=accession, source_url=xml_url, retrieved_at=xml_retrieved
        )
        conflict = entry.kind == "projection_conflict"
        dera_digest = (
            hashlib.sha256(f"synthetic-dera-projection:{accession}".encode()).hexdigest()
            if conflict
            else parsed.projection_digest
        )
        reasons = list(_ACQ_REASONS[entry.kind])
        schema_observed = entry.kind not in {"schema_unavailable", "multiple"}
        (root / "raw" / "header").mkdir(parents=True, exist_ok=True)
        (root / "raw" / "xml").mkdir(parents=True, exist_ok=True)
        (root / "terminal").mkdir(parents=True, exist_ok=True)
        (root / "raw" / "header" / f"{accession}-index-headers.html").write_bytes(raw_header)
        (root / "raw" / "header" / f"{accession}.json").write_bytes(record)
        (root / "raw" / "xml" / f"{accession}.xml").write_bytes(xml)
        request_header = {
            "acceptance_at": _stage2a_timestamp(header.acceptance_at),
            "acceptance_raw": header.acceptance_raw,
            "header_sha256": header.header_sha256,
            "raw_path": f"headers/raw/{accession}-index-headers.html",
            "raw_sha256": hashlib.sha256(raw_header).hexdigest(),
            "record_path": f"headers/records/{accession}.json",
            "record_sha256": hashlib.sha256(record).hexdigest(),
            "retrieved_at": _stage2a_timestamp(header_retrieved),
            "url": header_url,
        }
        boundary = entry.kind == "boundary"
        requests.append(
            {
                "accession_number": accession,
                "boundary_anomaly": boundary,
                "dera": {
                    "filing_date": entry.acceptance.date().isoformat(),
                    "package": "fixture-dera.zip",
                    "package_label": "fixture",
                    "report_ending_period": entry.period.isoformat(),
                    "row_locator": f"dera/dera_submissions.jsonl:{number}",
                },
                "filing_date": entry.acceptance.date().isoformat(),
                "header": request_header,
                "primary_doc_url": xml_url,
                "registrant_cik": entry.registrant,
                "request_id": f"ncen-a:{accession}",
            }
        )
        row = {
            "schema": "bond_ncen_amendment_schema_evidence_v1",
            "accession_number": accession,
            "registrant_cik": entry.registrant,
            "request_id": f"ncen-a:{accession}",
            "attempt_ids": [f"attempt:{accession}:1"],
            "boundary_anomaly": boundary,
            "classification_basis": "observed_xml" if schema_observed else "unobserved",
            "schema_observed": schema_observed,
            "schema_version": entry.schema if schema_observed else None,
            "schema_namespace": ncen.NCEN_NAMESPACE,
            "schema_xpath": f"/{{{ncen.NCEN_NAMESPACE}}}edgarSubmission/{{{ncen.NCEN_NAMESPACE}}}schemaVersion",
            "raw_xml": {
                "locator": f"raw/xml/{accession}.xml",
                "retrieved_at": _stage2a_timestamp(xml_retrieved),
                "sha256": hashlib.sha256(xml).hexdigest(),
                "size": len(xml),
                "url": xml_url,
            },
            "xml": {
                "amended_accession_number": None if entry.kind in {"absent_amended_accession", "multiple"} else _ACQ_AMENDED,
                "form_type": "N-CEN/A",
                "registrant_cik": entry.registrant,
                "report_period_end": entry.period.isoformat(),
            },
            "header": {
                "acceptance_at": request_header["acceptance_at"],
                "acceptance_raw": header.acceptance_raw,
                "document_sha256": request_header["raw_sha256"],
                "header_sha256": header.header_sha256,
                "locator": f"raw/header/{accession}-index-headers.html",
                "retrieved_at": request_header["retrieved_at"],
                "url": header_url,
            },
            "dera": {
                "package": "fixture-dera.zip",
                "projection_digest": dera_digest,
                "row_locator": f"dera/dera_submissions.jsonl:{number}",
                "source_refs": ["fixture"],
            },
            "projection_equality": "conflict" if conflict else "equal",
            "quarantine_reasons": reasons,
            "rule_refs": {
                "amendment_evidence_seal": "a" * 64,
                "definitive_evidence_seal": "b" * 64,
                "rule_version": ncen.RULE_VERSION,
            },
            "terminal_status": "quarantined" if reasons else "verified",
            "failure_kind": "evidence_quarantine" if reasons else None,
        }
        rows.append(row)
        (root / "terminal" / f"{accession}.json").write_bytes(_acq_json(row))
        # Unknown classes and non-X0505 boundary requests have no declarable carrier record.
        if reasons and entry.kind != "multiple":
            quarantines.append(_acq_quarantine_record(entry))
        elif boundary and entry.schema == "X0505":
            boundaries.append(_acq_boundary_record(entry))
    flagged = sum(1 for item in requests if item["boundary_anomaly"])
    scope = {
        "schema": "bond_ncen_amendment_acquisition_scope_v1",
        "classification_contract": "observed_xml_only_no_acceptance_era_inference",
        "cohort": {
            "boundary_anomaly_count": flagged,
            "boundary_definition": "exact official acceptance_at >= 2025-06-16T00:00:00Z",
            "request_count": len(requests),
        },
        "requests": requests,
    }
    scope_bytes = _acq_json(scope)
    (root / "scope.json").write_bytes(scope_bytes)
    (root / "SCOPE.sha256").write_bytes(f"{hashlib.sha256(scope_bytes).hexdigest()}  scope.json\n".encode())
    (root / "schema_evidence.jsonl").write_bytes(b"".join(_acq_json(item) for item in rows))
    statuses = defaultdict(int)
    for item in rows:
        statuses[str(item["terminal_status"])] += 1
    coverage = {
        "schema": "bond_ncen_amendment_acquisition_coverage_v1",
        "requested": len(requests),
        "terminal_status_disjoint_total": len(requests),
        "verified": statuses["verified"],
        "quarantined": statuses["quarantined"],
        "boundary_anomalies": {"requested": flagged},
    }
    (root / "coverage.json").write_bytes(_acq_json(coverage))
    return _AcqSeal(root, _acq_reseal(root), tuple(quarantines), tuple(boundaries))


def _stage2a_sealed_entry(record: dict[str, object]) -> _AcqEntry:
    """The sealed truth behind one declared quarantine record.

    Sealed custody always knows the registrant CIK, report period and one-second EDGAR
    acceptance; unknown or sub-second declared values are replaced here, so their declared
    record can never equal the loader's derivation.
    """
    acceptance_raw = record["acceptance_at"]
    acceptance = (
        _ACQ_ACCEPTED
        if acceptance_raw is None
        else dt.datetime.strptime(str(acceptance_raw), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC, microsecond=0)
    )
    period_raw = record["report_period_end"]
    return _AcqEntry(
        str(record["accession_number"]),
        str(record["registrant_cik"] or cik(1)),
        str(record["classification"]),
        acceptance,
        _ACQ_PERIOD if period_raw is None else dt.date.fromisoformat(str(period_raw)),
    )


def _stage2a_representable(record: dict[str, object]) -> bool:
    """Whether sealed acquisition custody can carry exactly this declared record."""
    acceptance = record["acceptance_at"]
    return (
        record["registrant_cik"] is not None
        and record["report_period_end"] is not None
        and isinstance(acceptance, str)
        and acceptance.endswith(".000000Z")
    )


def _stage2a_write_manifest(
    root: Path,
    artifacts: list[dict[str, object]],
    seal: _AcqSeal,
    *,
    quarantines: list[dict[str, object]],
    boundaries: list[dict[str, object]],
    manifest_kind: str = "synthetic_fixture",
    name: str = "manifest.json",
    ledger_name: str = "quarantine.json",
    acquisition_root: str | None = "acquisition",
) -> ncen.DiagnosticSourceManifestPin:
    ledger = root / ledger_name
    ledger.write_text(
        json.dumps(
            {"schema_version": ncen.DIAGNOSTIC_QUARANTINE_LEDGER_VERSION, "records": quarantines},
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
        newline="\n",
    )
    manifest = {
        "schema_version": ncen.DIAGNOSTIC_SOURCE_MANIFEST_VERSION,
        "manifest_kind": manifest_kind,
        "artifacts": artifacts,
        "quarantine_ledger": {
            "path": ledger.name,
            "sha256": _stage2a_sha(ledger),
            "bytes": ledger.stat().st_size,
            "scope_sha256": seal.pin.scope_sha256,
            "sha256sums_sha256": seal.pin.sha256sums_sha256,
            "acquisition_root": acquisition_root,
        },
        "boundary_examples": boundaries,
    }
    manifest_path = root / name
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8", newline="\n")
    return ncen.DiagnosticSourceManifestPin(
        manifest_path=manifest_path,
        manifest_sha256=_stage2a_sha(manifest_path),
        manifest_size=manifest_path.stat().st_size,
    )


def _stage2a_fixture(
    tmp_path: Path,
    **kwargs: object,
) -> ncen.DiagnosticSourceManifestPin:
    return _stage2a_fixture_with_seal(tmp_path, **kwargs)[0]  # type: ignore[arg-type]


def _stage2a_fixture_with_seal(
    tmp_path: Path,
    *,
    xml_family_name: str = "Acme Funds",
    xml_adviser_name: str = "Shared Adviser",
    xml_underwriter_lei: str = "5493001Z012YSB2A0K51",
    acceptance: str = "20260301120000",
    xml_retrieved_at: dt.datetime = K - dt.timedelta(days=1),
    manifest_kind: str = "synthetic_fixture",
    form_type: str = "N-CEN",
    xml_period: str = "2025-12-31",
    accession: str = ACCESSION,
    header_cik: str | None = None,
    extra_quarantines: tuple[dict[str, object], ...] = (),
    dera_extra_underwriters: tuple[tuple[str, str | None, str | None, str | None], ...] = (),
    xml_extra_underwriters: tuple[tuple[str, str | None, str | None, str | None], ...] = (),
    xml_schema: str = "X0505",
    include_xml: bool = True,
    xml_artifact_times: dict[str, dt.datetime | None] | None = None,
    extra_xml_schemas: tuple[str, ...] = (),
    acquisition_entries: tuple[_AcqEntry, ...] = _ACQ_DEFAULT_ENTRIES,
    include_second_registrant: bool = False,
) -> tuple[ncen.DiagnosticSourceManifestPin, _AcqSeal, list[dict[str, object]]]:
    """Write actual fixture files and pin their manifest.

    Every variation (extra quarantine ledger records, extra underwriter copies, header
    identity, XML copy schema/time/presence) is expressed in persisted bytes, so the loader
    verifies and derives it; no test can inject rows or exclusions into an already loaded index.
    ``xml_artifact_times`` overrides only the first XML copy's recorded times; extra XML copies
    (``extra_xml_schemas``) reuse the common artifact times. The quarantine ledger and boundary
    examples are declared exactly as the synthetic acquisition seal under ``acquisition/``
    derives them; ``extra_quarantines`` are sealed as their representable truth and declared as
    given, so an unknown or sub-second declared value is a membership mismatch (F6).
    Returns the pin, the seal and the manifest artifact list.
    """
    registrant = cik(1)
    second_accession = "0000000002-26-000001"
    header_registrant = registrant if header_cik is None else header_cik
    root = tmp_path / "stage2a"
    root.mkdir(parents=True)
    fund_id = f"{accession}_{registrant}_S000000001"
    tables = {
        "SUBMISSION": (
            ("ACCESSION_NUMBER", "SUBMISSION_TYPE", "CIK", "FILING_DATE", "REPORT_ENDING_PERIOD"),
            ((accession, form_type, registrant, "01-MAR-2026", "31-DEC-2025"),),
        ),
        "REGISTRANT": (
            ("ACCESSION_NUMBER", "CIK", "IS_FAMILY_INVESTMENT_COMPANY", "FAMILY_INVESTMENT_COMPANY_NAME"),
            ((accession, registrant, "Y", "Acme Funds"),),
        ),
        "FUND_REPORTED_INFO": (
            ("FUND_ID", "ACCESSION_NUMBER", "SERIES_ID"),
            ((fund_id, accession, "S000000001"),),
        ),
        "ADVISER": (
            ("FUND_ID", "ADVISER_TYPE", "FILE_NUM", "CRD_NUM", "ADVISER_LEI", "ADVISER_NAME"),
            ((fund_id, "Advisor", "801-00001", "000000011", "", "Shared Adviser"),),
        ),
        "PRINCIPAL_UNDERWRITER": (
            ("ACCESSION_NUMBER", "FILE_NUM", "CRD_NUM", "UNDERWRITER_LEI", "UNDERWRITER_NAME"),
            (
                (accession, "8-00001", "000000021", "5493001Z012YSB2A0K51", "Shared Underwriter"),
                *(
                    (accession, fn or "", crd or "", lei or "", name)
                    for name, fn, crd, lei in dera_extra_underwriters
                ),
            ),
        ),
    }
    if include_second_registrant:
        second_fund_id = f"{second_accession}_{cik(2)}_S000000001"
        additions = {
            "SUBMISSION": (second_accession, form_type, cik(2), "01-MAR-2026", "31-DEC-2025"),
            "REGISTRANT": (second_accession, cik(2), "Y", "Acme Funds"),
            "FUND_REPORTED_INFO": (second_fund_id, second_accession, "S000000001"),
            "ADVISER": (second_fund_id, "Advisor", "801-00001", "000000011", "", "Shared Adviser"),
            "PRINCIPAL_UNDERWRITER": (
                second_accession, "8-00001", "000000021", "5493001Z012YSB2A0K51", "Shared Underwriter"
            ),
        }
        tables = {
            table: (header, (*rows, additions[table]))
            for table, (header, rows) in tables.items()
        }
    xml_extra = "".join(
        "<principalUnderwriter>"
        f"<principalUnderwriterName>{name}</principalUnderwriterName>"
        + ("" if fn is None else f"<principalUnderwriterFileNumber>{fn}</principalUnderwriterFileNumber>")
        + ("" if crd is None else f"<principalUnderwriterCrdNumber>{crd}</principalUnderwriterCrdNumber>")
        + ("" if lei is None else f"<principalUnderwriterLei>{lei}</principalUnderwriterLei>")
        + "</principalUnderwriter>"
        for name, fn, crd, lei in xml_extra_underwriters
    )
    package = root / "dera.zip"
    member_payloads: dict[str, bytes] = {}
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for table in ncen.PINNED_TABLES:
            header, rows = tables[table]
            payload = ("\n".join(("\t".join(header), *("\t".join(row) for row in rows))) + "\n").encode()
            member = f"fixture/{table}.tsv"
            member_payloads[member] = payload
            archive.writestr(member, payload)

    namespace = ncen.NCEN_NAMESPACE

    def xml_document(schema: str, *, target_cik: str = registrant, family: str = xml_family_name) -> str:
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{namespace}"><schemaVersion>{schema}</schemaVersion><headerData><submissionType>{form_type}</submissionType><filerInfo><filer><issuerCredentials><cik>{target_cik}</cik></issuerCredentials></filer></filerInfo></headerData><formData><generalInfo reportEndingPeriod="{xml_period}"/><registrantInfo><registrantCik>{target_cik}</registrantCik><registrantFamilyInvComp isRegistrantFamilyInvComp="Y" familyInvCompFullName="{family}"/><principalUnderwriters><principalUnderwriter><principalUnderwriterName>Shared Underwriter</principalUnderwriterName><principalUnderwriterFileNumber>8-00001</principalUnderwriterFileNumber><principalUnderwriterCrdNumber>000000021</principalUnderwriterCrdNumber><principalUnderwriterLei>{xml_underwriter_lei}</principalUnderwriterLei></principalUnderwriter>{xml_extra}</principalUnderwriters></registrantInfo><managementInvestmentQuestionSeriesInfo><managementInvestmentQuestion><mgmtInvSeriesId>S000000001</mgmtInvSeriesId><investmentAdvisers><investmentAdviser><investmentAdviserName>{xml_adviser_name}</investmentAdviserName><investmentAdviserFileNo>801-00001</investmentAdviserFileNo><investmentAdviserCrdNo>000000011</investmentAdviserCrdNo><investmentAdviserLei></investmentAdviserLei></investmentAdviser></investmentAdvisers></managementInvestmentQuestion></managementInvestmentQuestionSeriesInfo></formData></edgarSubmission>'''

    xml_paths: list[tuple[str, Path, str]] = []
    if include_xml:
        xml = root / "primary_doc.xml"
        xml.write_text(xml_document(xml_schema), encoding="utf-8", newline="\n")
        xml_paths.append(("xml-1", xml, accession))
    for position, schema in enumerate(extra_xml_schemas, start=2):
        extra_xml = root / f"primary_doc_{position}.xml"
        extra_xml.write_text(xml_document(schema), encoding="utf-8", newline="\n")
        xml_paths.append((f"xml-{position}", extra_xml, accession))
    if include_second_registrant:
        second_xml = root / "second_primary_doc.xml"
        second_xml.write_text(xml_document(xml_schema, target_cik=cik(2), family="Acme Funds"),
                              encoding="utf-8", newline="\n")
        xml_paths.append((f"xml-{len(xml_paths) + 1}", second_xml, second_accession))
    header = root / "submission.txt"
    header.write_text(
        "<SEC-HEADER>\n"
        f"<ACCESSION-NUMBER>{accession}\n"
        f"<ACCEPTANCE-DATETIME>{acceptance}\n"
        f"<TYPE>{form_type}\n"
        f"<CIK>{header_registrant}\n"
        "<FILING-DATE>20260301\n"
        "<PERIOD>20251231\n"
        "</SEC-HEADER>\n",
        encoding="utf-8",
        newline="\n",
    )
    second_header = root / "second_submission.txt"
    if include_second_registrant:
        second_header.write_text(
            "<SEC-HEADER>\n"
            f"<ACCESSION-NUMBER>{second_accession}\n"
            f"<ACCEPTANCE-DATETIME>{acceptance}\n"
            f"<TYPE>{form_type}\n"
            f"<CIK>{cik(2)}\n"
            "<FILING-DATE>20260301\n"
            "<PERIOD>20251231\n"
            "</SEC-HEADER>\n",
            encoding="utf-8", newline="\n",
        )

    extra_accessions = [str(item["accession_number"]) for item in extra_quarantines]
    assert len(set(extra_accessions)) == len(extra_accessions), "one sealed terminal per accession"
    seal = _acquisition_seal(
        root / "acquisition",
        (*acquisition_entries, *(_stage2a_sealed_entry(dict(item)) for item in extra_quarantines)),
    )
    quarantine_records = [
        dict(item) for item in seal.quarantines if item["accession_number"] not in set(extra_accessions)
    ]
    quarantine_records.extend(dict(item) for item in extra_quarantines)
    accepted = (
        dt.datetime.strptime(acceptance, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    ).astimezone(UTC)
    common = {
        "retrieved_at": _stage2a_timestamp(xml_retrieved_at),
        "public_at": _stage2a_timestamp(accepted),
        "data_known_at": _stage2a_timestamp(accepted),
    }
    first_xml_times: dict[str, str | None] = dict(common)
    for key, value in (xml_artifact_times or {}).items():
        assert key in first_xml_times, key
        first_xml_times[key] = None if value is None else _stage2a_timestamp(value)
    xml_artifacts = [
        {
            "artifact_id": artifact_id,
            "kind": "edgar_xml",
            "path": path.name,
            "sha256": _stage2a_sha(path),
            "bytes": path.stat().st_size,
            "accession_number": source_accession,
            "source_url": "https://www.sec.gov/Archives/primary_doc.xml",
            "header_artifact_id": "header-1" if source_accession == accession else "header-2",
            **(first_xml_times if artifact_id == "xml-1" else common),
        }
        for artifact_id, path, source_accession in xml_paths
    ]
    artifacts: list[dict[str, object]] = [
        {
            "artifact_id": "header-1",
            "kind": "header",
            "path": header.name,
            "sha256": _stage2a_sha(header),
            "bytes": header.stat().st_size,
            "accession_number": accession,
            "registrant_cik": header_registrant,
            "source_url": "https://www.sec.gov/Archives/fixture.txt",
            **common,
        },
        {
            "artifact_id": "dera-1",
            "kind": "dera_zip",
            "path": package.name,
            "sha256": _stage2a_sha(package),
            "bytes": package.stat().st_size,
            "package_label": "fixture-2026q1",
            "members": [
                {
                    "path": member,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "bytes": len(payload),
                    "header": list(tables[Path(member).stem][0]),
                }
                for member, payload in sorted(member_payloads.items())
            ],
            **common,
        },
        *([{
            "artifact_id": "header-2",
            "kind": "header",
            "path": second_header.name,
            "sha256": _stage2a_sha(second_header),
            "bytes": second_header.stat().st_size,
            "accession_number": second_accession,
            "registrant_cik": cik(2),
            "source_url": "https://www.sec.gov/Archives/second_fixture.txt",
            **common,
        }] if include_second_registrant else []),
        *xml_artifacts,
    ]
    pin = _stage2a_write_manifest(
        root,
        artifacts,
        seal,
        quarantines=quarantine_records,
        boundaries=[dict(item) for item in seal.boundaries],
        manifest_kind=manifest_kind,
        acquisition_root=None if manifest_kind == "sealed_source" else "acquisition",
    )
    return pin, seal, artifacts


def _stage2a_index(
    *,
    acceptance_at: dt.datetime | None,
    header_retrieved_at: dt.datetime | None,
    retrieved_at: dt.datetime,
    data_known_at: dt.datetime,
    form: str = "N-CEN",
    schema: str = "X0505",
    status: str = "parsed",
    period: dt.date = dt.date(2025, 12, 31),
    family_name: str = "Acme Funds",
    underwriters: tuple[ncen.UnderwriterRecord, ...] | None = None,
) -> ncen.NcenFilingIndex:
    filing = _stage2a_filing(
        acceptance_at=acceptance_at,
        header_retrieved_at=header_retrieved_at,
        retrieved_at=retrieved_at,
        data_known_at=data_known_at,
        form=form,
        schema=schema,
        status=status,
        period=period,
        family_name=family_name,
        underwriters=underwriters,
    )
    return ncen.NcenFilingIndex({cik(1): (filing,)}, {}, {})


def _stage2a_filing(
    *,
    acceptance_at: dt.datetime | None,
    header_retrieved_at: dt.datetime | None,
    retrieved_at: dt.datetime,
    data_known_at: dt.datetime,
    form: str = "N-CEN",
    schema: str = "X0505",
    status: str = "parsed",
    period: dt.date = dt.date(2025, 12, 31),
    family_name: str = "Acme Funds",
    underwriters: tuple[ncen.UnderwriterRecord, ...] | None = None,
    accession: str = ACCESSION,
    registrant: str | None = None,
) -> ncen.NcenFiling:
    filed = dt.date(2026, 3, 1)
    return ncen.NcenFiling(
        accession_number=accession,
        registrant_cik=cik(1) if registrant is None else registrant,
        form_type=form,
        form_type_source="edgar_header",
        report_period_end=period,
        filing_date=filed,
        public_available_at=acceptance_at,
        public_time_basis="edgar_acceptance" if acceptance_at is not None else "date_only_conservative",
        data_known_at=data_known_at,
        source="dera+edgar_xml",
        source_refs=("fixture",),
        family_answer="Y",
        family_name_raw=family_name,
        funds=(
            ncen.NcenFund(
                "S000000001",
                (ncen.AdviserRecord("adviser", "801-1", "11", None, ("801-00001", "000000011", "")),),
            ),
        ),
        underwriters=(
            ncen.UnderwriterRecord("8-1", "21", "5493001Z012YSB2A0K51", ("8-00001", "000000021", "5493001Z012YSB2A0K51")),
        ) if underwriters is None else underwriters,
        status=status,
        reasons=() if status == "parsed" else ("fixture_quarantined",),
        schema_version=schema,
        retrieved_at=retrieved_at,
        acceptance_at=acceptance_at,
        public_date_bound=dt.datetime(2026, 3, 2, 5, tzinfo=UTC),
        header_retrieved_at=header_retrieved_at,
    )


def test_stage2a_source_manifest_binds_rows_headers_and_locators(tmp_path: Path) -> None:
    pin, seal, _artifacts = _stage2a_fixture_with_seal(tmp_path)
    sources = ncen.read_diagnostic_source_rows(pin)
    # Exact derived membership of the synthetic seal, not invented accessions with real counts.
    assert [
        (item.accession_number, item.registrant_cik, item.classification, list(item.reasons))
        for item in sources.exclusions
    ] == [
        (record["accession_number"], record["registrant_cik"], record["classification"], record["reasons"])
        for record in sorted(seal.quarantines, key=lambda value: str(value["accession_number"]))
    ]
    assert {item.classification for item in sources.exclusions} == {
        "absent_amended_accession",
        "schema_unavailable",
        "projection_conflict",
    }
    assert [item.accession_number for item in sources.boundary_examples] == [
        record["accession_number"] for record in seal.boundaries
    ]
    assert all(item.schema_version == "X0505" and item.policy_state == "example_only" for item in sources.boundary_examples)
    ledger = sources.acquisition_ledger()
    assert ledger.lane == "synthetic_fixture" and ledger.scope_sha256 == seal.pin.scope_sha256
    assert sources.exclusion_ledger_digest() == ledger.exclusion_ledger_digest
    data_rows = [item for item in sources.rows if item.role not in {"header", "index"}]
    assert {item.source_kind for item in data_rows} == {"dera", "edgar_xml"}
    assert all(item.artifact_sha256 and item.artifact_size and item.raw_row_sha256 for item in data_rows)
    assert all(item.header_source_id for item in data_rows)
    assert any("#data-row=1#field=ADVISER_NAME" in item.locator for item in data_rows)
    assert any("{http://www.sec.gov/edgar/ncen}investmentAdviserName[1]" in item.locator for item in data_rows)
    assert any(item.role == "underwriter" and item.lei_raw == "5493001Z012YSB2A0K51" for item in data_rows)
    assert any(item.name_raw == "Shared Adviser" and item.role == "current_primary" for item in data_rows)

    manifest = json.loads(pin.manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"][1]["members"][0]["header"].append("FORGED")
    pin.manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    rebound = dataclasses.replace(
        pin,
        manifest_sha256=_stage2a_sha(pin.manifest_path),
        manifest_size=pin.manifest_path.stat().st_size,
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_member_header_mismatch"):
        ncen.read_diagnostic_source_rows(rebound)


def test_stage2a_source_manifest_rejects_tampering_and_unsafe_paths(tmp_path: Path) -> None:
    pin = _stage2a_fixture(tmp_path)
    with pytest.raises(ncen.NcenError, match="diagnostic_manifest_sha256_mismatch"):
        ncen.read_diagnostic_source_rows(dataclasses.replace(pin, manifest_sha256="0" * 64))

    manifest = json.loads(pin.manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"][0]["path"] = "../submission.txt"
    pin.manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    unsafe = dataclasses.replace(
        pin,
        manifest_sha256=_stage2a_sha(pin.manifest_path),
        manifest_size=pin.manifest_path.stat().st_size,
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_artifact_path_unsafe"):
        ncen.read_diagnostic_source_rows(unsafe)

    surplus_pin = _stage2a_fixture(tmp_path / "surplus")
    surplus_manifest = json.loads(surplus_pin.manifest_path.read_text(encoding="utf-8"))
    surplus_manifest["artifacts"][1]["members"].append(
        {
            "path": "bogus/ADVISER.tsv",
            "sha256": "0" * 64,
            "bytes": 1,
            "header": list(ncen._DIAGNOSTIC_DERA_REQUIRED_COLUMNS["ADVISER"]),
        }
    )
    surplus_pin.manifest_path.write_text(
        json.dumps(surplus_manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    surplus_pin = dataclasses.replace(
        surplus_pin,
        manifest_sha256=_stage2a_sha(surplus_pin.manifest_path),
        manifest_size=surplus_pin.manifest_path.stat().st_size,
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_member_allowlist_mismatch"):
        ncen.read_diagnostic_source_rows(surplus_pin)

    quarantined_pin = _stage2a_fixture(tmp_path / "quarantined", xml_period="not-a-date")
    with pytest.raises(ncen.NcenError, match="diagnostic_xml_copy_quarantined"):
        ncen.read_diagnostic_source_rows(quarantined_pin)


def test_stage2a_attestation_and_time_fail_closed(tmp_path: Path) -> None:
    pin = _stage2a_fixture(tmp_path)
    sources = ncen.read_diagnostic_source_rows(pin)
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    baseline = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
    )
    selected = ncen.diagnostic_selection(
        baseline, sources, cik(1), R, K, mode="historical_reconstruction"
    )
    assert selected.selection_reason is None
    assert selected.evidence_state == "complete"
    assert selected.rows and all(item.attestation == "attested" for item in selected.rows)
    assert selected.dependencies and selected.knowledge_time == accepted

    date_only = _stage2a_index(
        acceptance_at=None,
        header_retrieved_at=None,
        retrieved_at=accepted,
        data_known_at=accepted,
    )
    unattested = ncen.diagnostic_selection(
        date_only, sources, cik(1), R, K, mode="historical_reconstruction"
    )
    assert unattested.selection_reason == "filing_acceptance_unattested"
    assert not unattested.rows

    after_k = _stage2a_index(
        acceptance_at=K + dt.timedelta(seconds=1),
        header_retrieved_at=K + dt.timedelta(seconds=1),
        retrieved_at=K + dt.timedelta(seconds=1),
        data_known_at=K + dt.timedelta(seconds=1),
    )
    late = ncen.diagnostic_selection(
        after_k, sources, cik(1), R, K, mode="historical_reconstruction"
    )
    assert late.selection_reason in {"no_effective_filing", "filing_acceptance_after_cutoff"}
    assert not late.rows

    late_possession = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=K + dt.timedelta(days=1),
        retrieved_at=K + dt.timedelta(days=1),
        data_known_at=accepted,
    )
    historical = ncen.diagnostic_selection(
        late_possession, sources, cik(1), R, K, mode="historical_reconstruction"
    )
    current = ncen.diagnostic_selection(late_possession, sources, cik(1), R, K, mode="current_run")
    assert historical.selection_reason is None and historical.rows
    assert current.selection_reason in {"no_effective_filing", "diagnostic_source_unheld"}
    assert not current.rows
    with pytest.raises(ncen.NcenError, match="datetime_not_timezone_aware"):
        ncen.diagnostic_selection(
            baseline, sources, cik(1), R, K.replace(tzinfo=None), mode="historical_reconstruction"
        )


def test_stage2a_copy_conflicts_and_name_only_never_links(tmp_path: Path) -> None:
    family_conflict = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(tmp_path / "family", xml_family_name="Acme Fund")
    )
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    baseline = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
    )
    selected = ncen.diagnostic_selection(
        baseline, family_conflict, cik(1), R, K, mode="historical_reconstruction"
    )
    assert selected.selection_reason == "diagnostic_b5_copy_key_conflict"
    assert selected.rows and all(item.attestation == "uncertain" for item in selected.rows)
    assert all(item.uncertain_expansion_eligible for item in selected.rows)
    assert ncen.reported_family_for(selected).state == "unknown"

    name_conflict = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(tmp_path / "names", xml_adviser_name="Different Adviser")
    )
    named = ncen.diagnostic_selection(
        baseline, name_conflict, cik(1), R, K, mode="historical_reconstruction"
    )
    assert named.selection_reason == "diagnostic_provider_name_copy_conflict"

    lei_conflict = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(tmp_path / "lei", xml_underwriter_lei="529900T8BM49AURSDO55")
    )
    lei = ncen.diagnostic_selection(
        baseline, lei_conflict, cik(1), R, K, mode="historical_reconstruction"
    )
    assert lei.selection_reason == "diagnostic_underwriter_lei_copy_conflict"

    no_ids = selection(2, row(2, "current_primary", "name-only-a", name="Same Name"))
    other_no_ids = selection(3, row(3, "current_primary", "name-only-b", name="Same Name"))
    assert len(projection((no_ids, other_no_ids), "uncertain_expanded").components) == 2


def test_stage2a_quarantine_and_unknown_amendment_never_admit(tmp_path: Path) -> None:
    sources = ncen.read_diagnostic_source_rows(_stage2a_fixture(tmp_path, form_type="N-CEN/A"))
    counts = defaultdict(int)
    for item in sources.exclusions:
        counts[item.classification] += 1
    assert dict(counts) == {
        "absent_amended_accession": 1,
        "projection_conflict": 1,
        "schema_unavailable": 1,
    }
    assert not ({item.accession_number for item in sources.exclusions} & {row.accession_number for row in sources.rows})

    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    unknown = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
        form="N-CEN/A",
        schema="X0404",
    )
    selected = ncen.diagnostic_selection(
        unknown, sources, cik(1), R, K, mode="historical_reconstruction"
    )
    assert selected.selection_reason == ncen.AMENDMENT_UNKNOWN_REASON
    assert selected.rows and all(item.attestation == "uncertain" for item in selected.rows)
    assert not any(item.attestation == "attested" for item in selected.rows)


def _stage2a_quarantine_record(
    *,
    accession: str,
    registrant: str | None,
    period: dt.date | None,
    acceptance: dt.datetime | None,
    classification: str = "absent_amended_accession",
) -> dict[str, object]:
    """One declared quarantine-ledger record (the loader assigns its locator).

    Its reasons are the sealed terminal reasons of its class, as the loader derives them.
    """
    return {
        "accession_number": accession,
        "registrant_cik": registrant,
        "form_type": "N-CEN/A",
        "report_period_end": None if period is None else period.isoformat(),
        "acceptance_at": None if acceptance is None else _stage2a_timestamp(acceptance),
        "classification": classification,
        "reasons": list(_ACQ_REASONS[classification]),
    }


def _stage2a_ledger_matches(
    sources: ncen.DiagnosticSourceIndex,
    record: dict[str, object],
) -> bool:
    return any(
        item.accession_number == record["accession_number"]
        and item.registrant_cik == record["registrant_cik"]
        and (None if item.report_period_end is None else item.report_period_end.isoformat())
        == record["report_period_end"]
        and (None if item.acceptance_at is None else _stage2a_timestamp(item.acceptance_at))
        == record["acceptance_at"]
        for item in sources.exclusions
    )


def test_stage2a_review_findings_fail_closed(tmp_path: Path) -> None:
    sources = ncen.read_diagnostic_source_rows(_stage2a_fixture(tmp_path / "base"))
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    baseline = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_source_index_unbound"):
        ncen.diagnostic_selection(
            baseline,
            ncen.DiagnosticSourceIndex.from_rows(sources.rows),
            cik(1),
            R,
            K,
            mode="historical_reconstruction",
        )

    # A caller-edited row can no longer reach selection at all (finding 1) ...
    data_row = next(item for item in sources.rows if item.role == "current_primary")
    tampered_rows = tuple(
        sorted(
            (
                dataclasses.replace(item, projection_digest="0" * 64) if item == data_row else item
                for item in sources.rows
            ),
            key=lambda item: item.source_row_id,
        )
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_source_index_unbound"):
        ncen.diagnostic_selection(
            baseline,
            dataclasses.replace(sources, rows=tampered_rows),
            cik(1),
            R,
            K,
            mode="historical_reconstruction",
        )
    # ... and verified copies whose projection differs from the selected baseline filing still
    # fail closed as an identity conflict.
    other_projection = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
        family_name="Other Funds",
    )
    tampered = ncen.diagnostic_selection(
        other_projection,
        sources,
        cik(1),
        R,
        K,
        mode="historical_reconstruction",
    )
    assert tampered.selection_reason == "diagnostic_source_identity_conflict"
    assert not tampered.rows

    # Persisted acceptance header for another registrant: loader-verified, never attested.
    wrong_header = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(tmp_path / "wrong-header", header_cik=cik(99))
    )
    header = next(item for item in wrong_header.rows if item.role == "header")
    assert header.registrant_cik == cik(99)
    unattested = ncen.diagnostic_selection(
        baseline,
        wrong_header,
        cik(1),
        R,
        K,
        mode="historical_reconstruction",
    )
    assert unattested.selection_reason == "filing_acceptance_unattested"
    assert not unattested.rows

    unknown_period = _stage2a_quarantine_record(
        accession="0000000999-26-999999",
        registrant=cik(1),
        period=None,
        acceptance=accepted,
    )
    # Sealed custody always derives the XML report period, so an unknown-period record cannot be
    # declared over it (F6); the conservative unknown-period rule is exercised on the carrier.
    assert _review_f08_predicate(unknown_period)
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_mismatch"):
        ncen.read_diagnostic_source_rows(
            _stage2a_fixture(tmp_path / "unknown-period", extra_quarantines=(unknown_period,))
        )
    later_same_period = _stage2a_quarantine_record(
        accession="0000000999-26-999999",
        registrant=cik(1),
        period=dt.date(2025, 12, 31),
        acceptance=accepted + dt.timedelta(hours=1),
    )
    quarantined = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(tmp_path / "later-same-period", extra_quarantines=(later_same_period,))
    )
    assert _stage2a_ledger_matches(quarantined, later_same_period)
    blocked = ncen.diagnostic_selection(
        baseline,
        quarantined,
        cik(1),
        R,
        K,
        mode="historical_reconstruction",
    )
    assert blocked.selection_reason == "diagnostic_quarantined_competitor"
    assert not blocked.rows


def test_stage2a_copy_conflict_alignment_is_per_copy() -> None:
    def bound(
        locator: str,
        copy: str,
        *,
        name: str,
        role: str = "current_primary",
        fn: str = "801-1",
        crd: str = "11",
        lei: str | None = None,
    ) -> ncen.DiagnosticSourceRow:
        return dataclasses.replace(
            row(
                1,
                role,
                locator,
                name=name,
                file_number=fn,
                crd=crd,
                lei=lei,
                series_id=None if role == "underwriter" else "S000000001",
                series_scope="registrant" if role == "underwriter" else "series",
            ),
            source_kind="edgar_xml",
            artifact_id=copy,
            artifact_path=f"{copy}.xml",
            artifact_sha256=hashlib.sha256(copy.encode()).hexdigest(),
            artifact_size=1,
            raw_row_sha256=hashlib.sha256(locator.encode()).hexdigest(),
            source_copy_id="ncencopy:" + hashlib.sha256(copy.encode()).hexdigest(),
        )

    agreeing_aliases = (
        bound("a-1", "copy-a", name="Alias A"),
        bound("a-2", "copy-a", name="Alias B"),
        bound("b-1", "copy-b", name="Alias A"),
        bound("b-2", "copy-b", name="Alias B"),
    )
    assert ncen._diagnostic_copy_conflicts(agreeing_aliases) == ()

    lei_a = "5493001Z012YSB2A0K51"
    lei_b = "529900T8BM49AURSDO55"
    swapped = (
        bound("a-1", "copy-a", name="U1", role="underwriter", fn="8-1", crd="21", lei=lei_a),
        bound("a-2", "copy-a", name="U2", role="underwriter", fn="8-2", crd="22", lei=lei_b),
        bound("b-1", "copy-b", name="U1", role="underwriter", fn="8-1", crd="21", lei=lei_b),
        bound("b-2", "copy-b", name="U2", role="underwriter", fn="8-2", crd="22", lei=lei_a),
    )
    assert "diagnostic_underwriter_lei_copy_conflict" in ncen._diagnostic_copy_conflicts(swapped)


def _stage2b_cohort(
    report_dates: tuple[dt.date, ...] = (R,),
    *, members_by_date: tuple[tuple[dt.date, int], ...] | None = None,
) -> ncen.DiagnosticCohort:
    source = ncen.InventorySource(
        package_label="synthetic-nport-package",
        zip_sha256="7" * 64,
        package_id="synthetic-package-id",
        retrieved_at=K - dt.timedelta(days=2),
        first_verified_public_at=K - dt.timedelta(days=10),
    )
    members = tuple(
        ncen.DiagnosticCohortMember(
            report_date=report_date,
            cik=cik(registrant),
            fund_keys=("S000000001",),
            known_at=K - dt.timedelta(days=1),
        )
        for report_date, registrant in (
            members_by_date if members_by_date is not None
            else tuple((item, 1) for item in report_dates)
        )
    )
    digest = ncen._diagnostic_cohort_digest(
        knowledge_cutoff=K,
        knowledge_mode="historical_reconstruction",
        inventory_digest="synthetic-inventory",
        sources=(source,),
        members=members,
    )
    return ncen.DiagnosticCohort(
        knowledge_cutoff=K,
        knowledge_mode="historical_reconstruction",
        inventory_digest="synthetic-inventory",
        sources=(source,),
        members=members,
        cohort_digest=digest,
        extraction_version=ncen.DIAGNOSTIC_COHORT_VERSION,
        derivation="sealed_vote_inventory_full_universe",
        full_cohort=True,
        outcome_fields_excluded=True,
        _seal=ncen._DIAGNOSTIC_COHORT_SEAL,
    )


def _stage2b_input_artifacts(pin: ncen.DiagnosticSourceManifestPin) -> list[dict[str, object]]:
    manifest = json.loads(pin.manifest_path.read_text(encoding="utf-8"))
    root = pin.manifest_path.parent
    artifacts = [
        {
            "root_id": "ncen-source",
            "path": item["path"],
            "sha256": item["sha256"],
            "bytes": item["bytes"],
        }
        for item in manifest["artifacts"]
    ]
    quarantine = manifest["quarantine_ledger"]
    artifacts.append(
        {
            "root_id": "ncen-source",
            "path": quarantine["path"],
            "sha256": quarantine["sha256"],
            "bytes": quarantine["bytes"],
        }
    )
    artifacts.append(
        {
            "root_id": "ncen-source",
            "path": pin.manifest_path.name,
            "sha256": pin.manifest_sha256,
            "bytes": pin.manifest_size,
        }
    )
    assert all((root / item["path"]).is_file() for item in artifacts)
    return sorted(artifacts, key=lambda item: (item["root_id"], item["path"]))


def _stage2b_declaration(
    pin: ncen.DiagnosticSourceManifestPin,
    sources: ncen.DiagnosticSourceIndex,
    cohort: ncen.DiagnosticCohort,
    *,
    predecessor_receipt_sha256: str | None = None,
) -> dict[str, object]:
    source_manifest = json.loads(pin.manifest_path.read_text(encoding="utf-8"))
    assert sources.evidence_digest is not None
    assert sources.manifest_kind is not None
    return {
        "schema_version": "ncen_purpose_diagnostics_declaration_v1",
        "diagnostic_schema_version": ncen.DIAGNOSTIC_SCHEMA_VERSION,
        "cohort_derivation_version": ncen.DIAGNOSTIC_COHORT_VERSION,
        "purpose_versions": {
            "reported_family": ncen.DIAGNOSTIC_REPORTED_FAMILY_VERSION,
            "reporting_dependence_block": ncen.DIAGNOSTIC_DEPENDENCE_VERSION,
        },
        "edge_version": ncen.DIAGNOSTIC_EDGE_VERSION,
        "selection_version": ncen.DIAGNOSTIC_SELECTION_VERSION,
        "contexts": [
            {
                "R": report_date.isoformat(),
                "K": _stage2a_timestamp(K),
                "mode": cohort.knowledge_mode,
            }
            for report_date in sorted({item.report_date for item in cohort.members})
        ],
        "inventory_digest": cohort.inventory_digest,
        "cohort_digest": cohort.cohort_digest,
        "cohort_provenance": {
            "derivation": cohort.derivation,
            "full_cohort": cohort.full_cohort,
            "inventory_version": ncen.INVENTORY_VERSION,
            "sealed_inventory": True,
            "outcome_fields_excluded": cohort.outcome_fields_excluded,
            "knowledge_cutoff": _stage2a_timestamp(cohort.knowledge_cutoff),
            "knowledge_mode": cohort.knowledge_mode,
            "inventory_sources": [
                ncen._diagnostic_inventory_source_payload(item) for item in cohort.sources
            ],
        },
        "baseline_seals": [
            {
                "name": "stage1",
                "sha256": "bc8aa345d10bf0c334dfdac9be414113c744b147d688c21ddc46f66fbcd9d5bf",
            },
            {
                "name": "stage2a",
                "sha256": "c74ffe1766bc836110cf8c32be6aa0e8d441a856be19a1fab21f81e892650a2b",
            },
            {
                "name": "v3",
                "sha256": "41521014f92ba1baf7ed15e4235005438e2c6bc52a6f47851bbb16bf184a9b29",
            },
        ],
        "source_code": [
            {
                "path": "src/bonds/default_events/ncen.py",
                "sha256": _stage2a_sha(ROOT / "src" / "bonds" / "default_events" / "ncen.py"),
            }
        ],
        "input_artifacts": _stage2b_input_artifacts(pin),
        "ncen_source_manifest": {
            "sha256": pin.manifest_sha256,
            "bytes": pin.manifest_size,
            "kind": sources.manifest_kind,
            "evidence_digest": sources.evidence_digest,
        },
        "ncen_evidence_digest": sources.evidence_digest,
        "exclusion_ledger_digest": source_manifest["quarantine_ledger"]["sha256"],
        "quarantine_seal": {
            "scope_sha256": ncen.DIAGNOSTIC_ACQUISITION_SCOPE_SHA256,
            "sha256sums_sha256": ncen.DIAGNOSTIC_ACQUISITION_SHA256SUMS_SHA256,
        },
        "ablations": [spec.ablation_id for spec in ncen.DIAGNOSTIC_ABLATIONS],
        "fold_protocol": ncen.DIAGNOSTIC_FOLD_PROTOCOL_VERSION,
        "normalizer": ncen.DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        "normalization_rule": ncen._PURPOSE_NORMALIZATION_RULE,
        "outcome_input_allowlist": list(ncen.DIAGNOSTIC_OUTCOME_INPUT_ALLOWLIST),
        "limits": {
            "memory_limit_bytes": ncen.DIAGNOSTIC_MEMORY_LIMIT_BYTES,
            "memory_soft_limit_bytes": ncen.DIAGNOSTIC_MEMORY_SOFT_LIMIT_BYTES,
            "max_contexts": 8,
            "max_source_rows": 10_000,
        },
        "sensitivity_stage": "not_computed",
        "predecessor_receipt_sha256": predecessor_receipt_sha256,
        "diagnostic_only": True,
    }


def _stage2b_checkpoint(
    root: Path,
    cohort: ncen.DiagnosticCohort,
) -> ncen.DiagnosticCohortManifestPin:
    root.mkdir()
    rows = sorted(
        [
            ncen._purpose_cohort_record(member, cohort.inventory_digest)
            for member in cohort.members
        ],
        key=lambda item: item["record_id"],
    )
    cohort_path = root / "cohort.jsonl"
    cohort_path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    manifest = {
        "schema_version": ncen.DIAGNOSTIC_COHORT_MANIFEST_VERSION,
        "status": "complete",
        "cohort_version": ncen.DIAGNOSTIC_COHORT_VERSION,
        "knowledge_cutoff": _stage2a_timestamp(cohort.knowledge_cutoff),
        "knowledge_mode": cohort.knowledge_mode,
        "inventory_digest": cohort.inventory_digest,
        "cohort_digest": cohort.cohort_digest,
        "inventory_sources": [
            ncen._diagnostic_inventory_source_payload(item) for item in cohort.sources
        ],
        "full_cohort_provenance": {
            "derivation": "sealed_vote_inventory_full_universe",
            "full_cohort": True,
            "inventory_version": ncen.INVENTORY_VERSION,
            "sealed_inventory": True,
            "source_inventory_digest": cohort.inventory_digest,
        },
        "files": [
            {
                "path": "cohort.jsonl",
                "sha256": _stage2a_sha(cohort_path),
                "bytes": cohort_path.stat().st_size,
                "rows": len(rows),
                "record_type": "cohort_member",
            }
        ],
        "outcome_fields_used": False,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    sums_path = root / "SHA256SUMS"
    sums_path.write_text(
        f"{_stage2a_sha(cohort_path)}  cohort.jsonl\n"
        f"{_stage2a_sha(manifest_path)}  manifest.json\n",
        encoding="ascii",
        newline="\n",
    )
    return ncen.DiagnosticCohortManifestPin(
        manifest_path=manifest_path,
        manifest_sha256=_stage2a_sha(manifest_path),
        manifest_size=manifest_path.stat().st_size,
        sha256sums_sha256=_stage2a_sha(sums_path),
    )


def _stage2b_export_fixture(
    tmp_path: Path,
    *,
    report_dates: tuple[dt.date, ...] = (R, dt.date(2026, 4, 30)),
) -> tuple[
    ncen.DiagnosticSourceManifestPin,
    ncen.DiagnosticSourceIndex,
    ncen.NcenFilingIndex,
    ncen.DiagnosticCohort,
    dict[str, object],
]:
    pin = _stage2a_fixture(tmp_path / "source")
    sources = ncen.read_diagnostic_source_rows(pin)
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    index = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
    )
    cohort = _stage2b_cohort(report_dates)
    declaration = _stage2b_declaration(pin, sources, cohort)
    return pin, sources, index, cohort, declaration


def _stage2b_write_export(
    tmp_path: Path,
    *,
    output_name: str = "diagnostic-export",
) -> tuple[ncen.PurposeExportResult, ncen.DiagnosticSourceManifestPin, dict[str, object]]:
    pin, sources, index, cohort, declaration = _stage2b_export_fixture(tmp_path)
    result = ncen.write_purpose_diagnostics(
        index,
        cohort,
        sources,
        declaration=declaration,
        output_root=tmp_path / output_name,
        code_root=ROOT,
        input_roots={"ncen-source": pin.manifest_path.parent},
    )
    return result, pin, declaration


def _stage2b_reseal(root: Path, receipt_path: Path) -> None:
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["files"]:
        path = root / item["path"]
        item["sha256"] = _stage2a_sha(path)
        item["bytes"] = path.stat().st_size
        item["rows"] = len(path.read_bytes().splitlines()) if path.suffix == ".jsonl" else 1
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    paths = sorted({item["path"] for item in manifest["files"]} | {"declaration.json", "manifest.json"})
    (root / "SHA256SUMS").write_text(
        "".join(f"{_stage2a_sha(root / path)}  {path}\n" for path in paths),
        encoding="ascii",
        newline="\n",
    )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["artifact_directory"] = root.name
    receipt["sha256sums_sha256"] = _stage2a_sha(root / "SHA256SUMS")
    receipt["manifest_sha256"] = _stage2a_sha(manifest_path)
    receipt["declaration_sha256"] = _stage2a_sha(root / "declaration.json")
    receipt_path.write_bytes(ncen._purpose_json_bytes(receipt))


def _stage2b_copy_export(
    result: ncen.PurposeExportResult,
    destination: Path,
) -> Path:
    import shutil

    shutil.copytree(result.root, destination)
    receipt = destination.parent / f"{destination.name}.receipt.json"
    shutil.copy2(result.receipt_path, receipt)
    _stage2b_reseal(destination, receipt)
    return receipt


def _assert_c1a_historical_envelope_refused(tmp_path: Path) -> None:
    """V1 complete-export attack fixtures cannot issue a new receipt under C1a."""
    pin, _sources, _index, _cohort, declaration = _stage2b_export_fixture(tmp_path / "old-v1")
    export = tmp_path / "old-v1" / "historical-export"
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_schema_unsupported$"):
        ncen.write_purpose_diagnostics(
            trusted_run=None, declaration=declaration, output_root=export,
            code_root=ROOT, input_roots={"ncen-source": pin.manifest_path.parent},
        )
    assert not export.exists()


def test_diagnostic_cohort_requires_sealed_full_provenance(tmp_path: Path) -> None:
    cohort = _stage2b_cohort((R,))
    checkpoint = tmp_path / "cohort-checkpoint"
    pin = _stage2b_checkpoint(checkpoint, cohort)
    loaded = ncen.load_diagnostic_cohort(checkpoint, trusted_manifest=pin)
    assert loaded == cohort
    assert not hasattr(loaded, "fingerprints")
    assert not hasattr(loaded, "target_votes")
    serialized = json.dumps(
        [ncen._purpose_cohort_record(item, loaded.inventory_digest) for item in loaded.members]
    ).lower()
    assert not any(token in serialized for token in ("cusip", "target", "fingerprint", "value"))

    manifest = json.loads(pin.manifest_path.read_text(encoding="utf-8"))
    manifest["full_cohort_provenance"]["sealed_inventory"] = False
    pin.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    sums_path = checkpoint / "SHA256SUMS"
    sums_path.write_text(
        f"{_stage2a_sha(checkpoint / 'cohort.jsonl')}  cohort.jsonl\n"
        f"{_stage2a_sha(pin.manifest_path)}  manifest.json\n",
        encoding="ascii",
        newline="\n",
    )
    rebound = dataclasses.replace(
        pin,
        manifest_sha256=_stage2a_sha(pin.manifest_path),
        manifest_size=pin.manifest_path.stat().st_size,
        sha256sums_sha256=_stage2a_sha(sums_path),
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_cohort_provenance_invalid"):
        ncen.load_diagnostic_cohort(checkpoint, trusted_manifest=rebound)
    with pytest.raises(ncen.NcenError, match="diagnostic_cohort_pickle_forbidden"):
        ncen.load_diagnostic_cohort(tmp_path / "cohort.pkl", trusted_manifest=pin)


def test_iter_purpose_snapshots_uses_slim_cohort_once_per_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pin, sources, index, cohort, declaration = _stage2b_export_fixture(tmp_path)
    calls: list[tuple[str, dt.date]] = []
    original = ncen.diagnostic_selection

    def tracked(*args, **kwargs):
        calls.append((args[2], args[3]))
        return original(*args, **kwargs)

    monkeypatch.setattr(ncen, "diagnostic_selection", tracked)
    snapshots = tuple(
        ncen.iter_purpose_snapshots(index, cohort, sources, declaration=declaration)
    )
    assert len(snapshots) == 2
    assert calls == [(cik(1), R), (cik(1), dt.date(2026, 4, 30))]
    assert all(len(item.projections) == len(ncen.DIAGNOSTIC_ABLATIONS) for item in snapshots)
    provider_snapshots = tuple(
        next(
            item
            for item in snapshot.dependence_snapshots()
            if item.projection.ablation_id == "provider_only"
        )
        for snapshot in snapshots
    )
    scope = ncen.TemporalFoldScope(
        mode="historical_reconstruction",
        ablation_id="provider_only",
        contexts=tuple(
            ncen.FoldContext(
                item.report_date,
                item.knowledge_cutoff,
                item.inventory_digest,
                item.projection.context_id,
            )
            for item in provider_snapshots
        ),
    )
    folded = ncen.build_temporal_dependence_union(provider_snapshots, fold_scope=scope)
    assert folded.groups[0].members == (cik(1),)
    assert pin.manifest_path.parent.exists()


def test_jsonl_strict_schema_and_full_replay(tmp_path: Path) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _assert_c1a_historical_envelope_refused(tmp_path)
        return  # No v2 complete export exists before C1b selection replay.
    result, pin, declaration = _stage2b_write_export(tmp_path)
    manifest = ncen.validate_purpose_export(
        result.root,
        receipt_path=result.receipt_path,
        code_root=ROOT,
        input_roots={"ncen-source": pin.manifest_path.parent},
    )
    assert manifest["status"] == "complete"
    assert manifest["outcome_inputs_used"] is False
    assert result.manifest_sha256 == _stage2a_sha(result.root / "manifest.json")
    assert result.receipt_sha256 == _stage2a_sha(result.receipt_path)
    assert json.loads((result.root / "issuer_group_ref.json").read_text(encoding="utf-8")) == dataclasses.asdict(
        ncen.BondIssuerGroupRef()
    )
    graph_files = [
        path
        for path, _record_type in ncen.DIAGNOSTIC_EXPORT_JSONL
        if path not in {"sources.jsonl", "cohort.jsonl"}
    ]
    graph_text = "".join((result.root / path).read_text(encoding="utf-8") for path in graph_files).lower()
    assert not any(token in graph_text for token in ("cusip", "target_vote", "consensus_y", "default_label"))
    assert declaration["diagnostic_only"] is True


@pytest.mark.parametrize("attack", ("path_traversal", "path_symlink", "orphan", "duplicate", "unsorted"))
def test_export_rejects_path_tamper_orphans_duplicates_unsorted_and_symlinks(
    tmp_path: Path,
    attack: str,
) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _c1b2b3b1_historical_attack(tmp_path / attack, attack)
        return
    result, pin, _declaration = _stage2b_write_export(tmp_path)
    validation = {
        "receipt_path": None,
        "code_root": ROOT,
        "input_roots": {"ncen-source": pin.manifest_path.parent},
    }

    orphan_root = tmp_path / "orphan"
    orphan_receipt = _stage2b_copy_export(result, orphan_root)
    incidence_path = orphan_root / "incidences.jsonl"
    incidences = [json.loads(line) for line in incidence_path.read_text(encoding="utf-8").splitlines()]
    incidences[0]["source_row_id"] = "ncenrow:source_row:" + "9" * 64
    payload = {
        key: value
        for key, value in incidences[0].items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    incidences[0] = ncen._purpose_record("incidence", payload)
    contexts = [
        json.loads(line)
        for line in (orphan_root / "contexts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    context_order = {item["context_id"]: index for index, item in enumerate(contexts)}
    incidence_path.write_bytes(
        ncen._purpose_jsonl_bytes(
            sorted(
                incidences,
                key=lambda item: (context_order[item["context_id"]], item["record_id"]),
            )
        )
    )
    _stage2b_reseal(orphan_root, orphan_receipt)
    with pytest.raises(ncen.NcenError, match="diagnostic_incidence_orphan_reference"):
        ncen.validate_purpose_export(
            orphan_root,
            receipt_path=orphan_receipt,
            code_root=ROOT,
            input_roots=validation["input_roots"],
        )

    duplicate_root = tmp_path / "duplicate"
    duplicate_receipt = _stage2b_copy_export(result, duplicate_root)
    source_path = duplicate_root / "sources.jsonl"
    lines = source_path.read_bytes().splitlines(keepends=True)
    source_path.write_bytes(b"".join((*lines, lines[0])))
    _stage2b_reseal(duplicate_root, duplicate_receipt)
    with pytest.raises(ncen.NcenError, match="diagnostic_duplicate_record_id"):
        ncen.validate_purpose_export(
            duplicate_root,
            receipt_path=duplicate_receipt,
            code_root=ROOT,
            input_roots=validation["input_roots"],
        )

    unsorted_root = tmp_path / "unsorted"
    unsorted_receipt = _stage2b_copy_export(result, unsorted_root)
    node_path = unsorted_root / "nodes.jsonl"
    node_lines = node_path.read_bytes().splitlines(keepends=True)
    assert len(node_lines) > 1
    node_path.write_bytes(b"".join(reversed(node_lines)))
    _stage2b_reseal(unsorted_root, unsorted_receipt)
    with pytest.raises(ncen.NcenError, match="purpose_export_record_order_invalid"):
        ncen.validate_purpose_export(
            unsorted_root,
            receipt_path=unsorted_receipt,
            code_root=ROOT,
            input_roots=validation["input_roots"],
        )

    traversal_root = tmp_path / "traversal"
    traversal_receipt = _stage2b_copy_export(result, traversal_root)
    manifest_path = traversal_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0]["path"] = "../cohort.jsonl"
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="manifest_file_path_unsafe"):
        ncen.validate_purpose_export(
            traversal_root,
            receipt_path=traversal_receipt,
            code_root=ROOT,
            input_roots=validation["input_roots"],
        )

    symlink_root = tmp_path / "symlink"
    symlink_receipt = _stage2b_copy_export(result, symlink_root)
    checks_path = symlink_root / "checks.json"
    checks_path.unlink()
    try:
        checks_path.symlink_to(result.root / "checks.json")
    except OSError:
        pytest.skip("symlink creation unavailable on this Windows host")
    with pytest.raises(ncen.NcenError, match="symlink_forbidden|non_regular_file"):
        ncen.validate_purpose_export(
            symlink_root,
            receipt_path=symlink_receipt,
            code_root=ROOT,
            input_roots=validation["input_roots"],
        )


@pytest.mark.parametrize("attack", ("raw_header", "interrupted"))
def test_export_rejects_raw_header_tamper_and_interrupted_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _c1b2b3b1_historical_attack(tmp_path / attack, attack)
        return
    result, pin, _declaration = _stage2b_write_export(tmp_path)
    header = pin.manifest_path.parent / "submission.txt"
    original_header = header.read_bytes()
    header.write_bytes(original_header + b"FORGED\n")
    with pytest.raises(ncen.NcenError, match="purpose_export_input_artifact_mismatch"):
        ncen.validate_purpose_export(
            result.root,
            receipt_path=result.receipt_path,
            code_root=ROOT,
            input_roots={"ncen-source": pin.manifest_path.parent},
        )
    header.write_bytes(original_header)

    second_root = tmp_path / "interrupted-source"
    pin2, sources2, index2, cohort2, declaration2 = _stage2b_export_fixture(second_root)

    def interrupted(*_args, **_kwargs):
        raise ncen.NcenError("synthetic_interruption")

    monkeypatch.setattr(ncen, "iter_purpose_snapshots", interrupted)
    partial_root = tmp_path / "partial"
    with pytest.raises(ncen.NcenError, match="synthetic_interruption"):
        ncen.write_purpose_diagnostics(
            index2,
            cohort2,
            sources2,
            declaration=declaration2,
            output_root=partial_root,
            code_root=ROOT,
            input_roots={"ncen-source": pin2.manifest_path.parent},
        )
    assert (partial_root / "declaration.json").is_file()
    assert not (partial_root / "manifest.json").exists()
    assert not (tmp_path / "partial.receipt.json").exists()
    with pytest.raises(ncen.NcenError, match="purpose_export_directory_must_be_new"):
        ncen.write_purpose_diagnostics(
            index2,
            cohort2,
            sources2,
            declaration=declaration2,
            output_root=partial_root,
            code_root=ROOT,
            input_roots={"ncen-source": pin2.manifest_path.parent},
        )

    monkeypatch.undo()
    predecessor_sha = _stage2a_sha(result.receipt_path)
    declaration2 = _stage2b_declaration(
        pin2,
        sources2,
        cohort2,
        predecessor_receipt_sha256=predecessor_sha,
    )
    resumed = ncen.write_purpose_diagnostics(
        index2,
        cohort2,
        sources2,
        declaration=declaration2,
        output_root=tmp_path / "resumed",
        code_root=ROOT,
        input_roots={"ncen-source": pin2.manifest_path.parent},
        predecessor_receipt=result.receipt_path,
        predecessor_input_roots={"ncen-source": pin.manifest_path.parent},
    )
    assert resumed.root != partial_root and resumed.receipt_path.is_file()


def test_stage2b_outcome_blind_and_no_accepting_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _assert_c1a_historical_envelope_refused(tmp_path)
        return
    pin, sources, index, cohort, declaration = _stage2b_export_fixture(tmp_path)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("accepting, inventory or target path called")

    monkeypatch.setattr(ncen, "build_vote_inventory", forbidden)
    monkeypatch.setattr(ncen.VoteInventory, "target_votes", forbidden)
    monkeypatch.setattr(ncen, "build_consensus_with_ncen", forbidden)
    monkeypatch.setattr(ncen, "diagnostic_per_state_components", forbidden)
    result = ncen.write_purpose_diagnostics(
        index,
        cohort,
        sources,
        declaration=declaration,
        output_root=tmp_path / "outcome-blind",
        code_root=ROOT,
        input_roots={"ncen-source": pin.manifest_path.parent},
    )
    performance = json.loads((result.root / "performance.json").read_text(encoding="utf-8"))
    assert performance["raw_nport_parse_calls"] == 0
    assert performance["inventory_build_calls"] == 0
    assert performance["target_vote_calls"] == 0


def test_stage2b_four_seed_export_bytes_identical(tmp_path: Path) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _assert_c1a_historical_envelope_refused(tmp_path)
        return  # Four-seed C1a trust admission has its own unchanged-input test below.
    pin, _sources, _index, cohort, declaration = _stage2b_export_fixture(tmp_path / "shared")
    checkpoint_pin = _stage2b_checkpoint(tmp_path / "cohort", cohort)
    declaration_path = tmp_path / "declaration.json"
    declaration_path.write_bytes(ncen._purpose_json_bytes(declaration))
    pins_path = tmp_path / "pins.json"
    pins_path.write_text(
        json.dumps(
            {
                "source_manifest": str(pin.manifest_path),
                "source_manifest_sha256": pin.manifest_sha256,
                "source_manifest_size": pin.manifest_size,
                "cohort_root": str(checkpoint_pin.manifest_path.parent),
                "cohort_manifest": str(checkpoint_pin.manifest_path),
                "cohort_manifest_sha256": checkpoint_pin.manifest_sha256,
                "cohort_manifest_size": checkpoint_pin.manifest_size,
                "cohort_sha256sums_sha256": checkpoint_pin.sha256sums_sha256,
                "declaration": str(declaration_path),
                "code_root": str(ROOT),
                "test_module": str(Path(__file__)),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    runner = r'''
import datetime as dt
import importlib
import json
import sys
from pathlib import Path
from src.bonds.default_events import ncen

cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
sys.path.insert(0, str(Path(cfg["test_module"]).parent))
module = importlib.import_module("test_bond_default_ncen_purposes")
source_path = Path(cfg["source_manifest"])
source_pin = ncen.DiagnosticSourceManifestPin(
    source_path,
    cfg["source_manifest_sha256"],
    cfg["source_manifest_size"],
)
sources = ncen.read_diagnostic_source_rows(source_pin)
accepted = dt.datetime(2026, 3, 1, 17, tzinfo=dt.timezone.utc)
index = module._stage2a_index(
    acceptance_at=accepted,
    header_retrieved_at=accepted,
    retrieved_at=accepted,
    data_known_at=accepted,
)
cohort_pin = ncen.DiagnosticCohortManifestPin(
    Path(cfg["cohort_manifest"]),
    cfg["cohort_manifest_sha256"],
    cfg["cohort_manifest_size"],
    cfg["cohort_sha256sums_sha256"],
)
cohort = ncen.load_diagnostic_cohort(cfg["cohort_root"], trusted_manifest=cohort_pin)
declaration = json.loads(Path(cfg["declaration"]).read_text(encoding="utf-8"))
ncen.write_purpose_diagnostics(
    index,
    cohort,
    sources,
    declaration=declaration,
    output_root=sys.argv[2],
    code_root=cfg["code_root"],
    input_roots={"ncen-source": str(source_path.parent)},
)
'''
    roots: list[Path] = []
    for seed in (1, 2, 977, 31337):
        root = tmp_path / f"seed-{seed}"
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = str(seed)
        completed = subprocess.run(
            [sys.executable, "-c", runner, str(pins_path), str(root)],
            cwd=ROOT,
            env=env,
            capture_output=True,
            check=False,
            timeout=120,
        )
        assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
        roots.append(root)
    deterministic_paths = [
        *(path for path, _record_type in ncen.DIAGNOSTIC_EXPORT_JSONL),
        "declaration.json",
        "issuer_group_ref.json",
    ]
    baseline = {path: (roots[0] / path).read_bytes() for path in deterministic_paths}
    for root in roots[1:]:
        assert {path: (root / path).read_bytes() for path in deterministic_paths} == baseline


@pytest.mark.parametrize("attack", ("forged_status_receipt", "unknown_key"))
def test_export_rejects_forged_complete_status_and_unknown_keys(tmp_path: Path, attack: str) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _c1b2b3b1_historical_attack(tmp_path / attack, attack)
        return
    result, pin, _declaration = _stage2b_write_export(tmp_path)
    input_roots = {"ncen-source": pin.manifest_path.parent}

    forged_root = tmp_path / "forged-status"
    forged_receipt = _stage2b_copy_export(result, forged_root)
    manifest_path = forged_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "partial"
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    _stage2b_reseal(forged_root, forged_receipt)
    with pytest.raises(ncen.NcenError, match="manifest_complete_status_forged"):
        ncen.validate_purpose_export(
            forged_root,
            receipt_path=forged_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )

    extra_root = tmp_path / "unknown-key"
    extra_receipt = _stage2b_copy_export(result, extra_root)
    node_path = extra_root / "nodes.jsonl"
    rows = [json.loads(line) for line in node_path.read_text(encoding="utf-8").splitlines()]
    rows[0]["unexpected"] = "forbidden"
    node_path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    _stage2b_reseal(extra_root, extra_receipt)
    with pytest.raises(ncen.NcenError, match="diagnostic_node_keys_invalid"):
        ncen.validate_purpose_export(
            extra_root,
            receipt_path=extra_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )


def test_stage2b_is_append_only_after_frozen_v3_stage1_and_stage2a() -> None:
    source = Path(ncen.__file__).read_bytes()
    stage1_marker = b"\n\n# === FE-1 purpose-labelled diagnostic core"
    stage2a_marker = b"\n\n# === FE-1 diagnostic source sidecars and selection attestation"
    stage2b_marker = b"\n\n# === FE-1 diagnostic synthetic export and sealed envelope"
    stage1 = source.index(stage1_marker)
    stage2a = source.index(stage2a_marker)
    stage2b = source.index(stage2b_marker)
    assert stage1 == 95_275
    assert hashlib.sha256(source[:stage1]).hexdigest() == (
        "41521014f92ba1baf7ed15e4235005438e2c6bc52a6f47851bbb16bf184a9b29"
    )
    # F4 graph admission and F9 attested-only temporal union change only diagnostics.
    assert hashlib.sha256(source[stage1:stage2a]).hexdigest() == (
        "bb2d084eef8bceea1022c5ac67aa0f9a1d9c1eac283c2db743bcc4171c86b114"
    )
    # B1 F10/F11 deliberately changes canonical B.5 identities and strict carrier invariants.
    # Stage2a pin moved deliberately for consolidated-review F5/F8 (was c74ffe17...0a2b), then
    # for F1/F12 loader custody and one-time lookups (was 8acaea51...2a12b), then for F3
    # eligible-copy amendment schema attestation (was 11cab4e7...0b20), then for F6 exact
    # acquisition ledger membership (was 8cefaa35...436cd), then for F13a verified descriptors,
    # one-at-a-time source bytes and the shared resource monitor (was bb56fa93...28760), then
    # F13b bounded TSV streaming and its disposable indexed SQLite join store, then the review
    # fixes for header CSV parity, real read/check bounds, and disclosed accession checkpoints.
    assert hashlib.sha256(source[stage2a:stage2b]).hexdigest() == (
        "a16889eb4e6cecc1e275ad9f73f6033978fc4182c5db1779523001aed4b9ee05"
    )
    assert hashlib.sha256(source[stage2b:]).hexdigest() != (
        "723a15039186c14181255e094aedb22d7cc6ac4834cccb8cdd7fea54c7eba22b"
    )  # C1a deliberately versions only the diagnostic export segment.


def test_validator_replays_source_family_and_cohort_derivations(tmp_path: Path) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _c1b2b3b1_historical_attack(tmp_path / "source-graph", "source_graph")
        return
    result, pin, _declaration = _stage2b_write_export(tmp_path)
    input_roots = {"ncen-source": pin.manifest_path.parent}

    source_root = tmp_path / "source-forgery"
    source_receipt = _stage2b_copy_export(result, source_root)
    source_path = source_root / "sources.jsonl"
    source_rows = [json.loads(line) for line in source_path.read_text(encoding="utf-8").splitlines()]
    invented = dict(source_rows[0])
    invented["locator"] = "invented/nonexistent#row=999"
    invented["raw_row_sha256"] = "9" * 64
    payload = {
        key: value
        for key, value in invented.items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    source_rows.append(ncen._purpose_record("source_row", payload))
    source_path.write_bytes(
        ncen._purpose_jsonl_bytes(sorted(source_rows, key=lambda item: item["record_id"]))
    )
    _stage2b_reseal(source_root, source_receipt)
    with pytest.raises(ncen.NcenError, match="purpose_export_source_row_derivation_mismatch"):
        ncen.validate_purpose_export(
            source_root,
            receipt_path=source_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )

    quarantine_root = tmp_path / "quarantine-forgery"
    quarantine_receipt = _stage2b_copy_export(result, quarantine_root)
    exclusion_path = quarantine_root / "exclusions.jsonl"
    exclusion_rows = [
        json.loads(line) for line in exclusion_path.read_text(encoding="utf-8").splitlines()
    ]
    exclusion_rows = [item for item in exclusion_rows if item["context_id"] is not None]
    exclusion_path.write_bytes(ncen._purpose_jsonl_bytes(exclusion_rows))
    _stage2b_reseal(quarantine_root, quarantine_receipt)
    with pytest.raises(ncen.NcenError, match="purpose_export_quarantine_ledger_mismatch"):
        ncen.validate_purpose_export(
            quarantine_root,
            receipt_path=quarantine_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )

    family_root = tmp_path / "family-forgery"
    family_receipt = _stage2b_copy_export(result, family_root)
    family_path = family_root / "reported_families.jsonl"
    families = [json.loads(line) for line in family_path.read_text(encoding="utf-8").splitlines()]
    families[0]["answer"] = "N"
    families[0]["label_id"] = "not-a-derived-label"
    family_payload = {
        key: value
        for key, value in families[0].items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    families[0] = ncen._purpose_record("reported_family", family_payload)
    contexts = [
        json.loads(line)
        for line in (family_root / "contexts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    context_order = {item["context_id"]: index for index, item in enumerate(contexts)}
    families.sort(key=lambda item: (context_order[item["context_id"]], item["record_id"]))
    family_path.write_bytes(ncen._purpose_jsonl_bytes(families))
    _stage2b_reseal(family_root, family_receipt)
    with pytest.raises(ncen.NcenError, match="reported_family_declared_invalid"):
        ncen.validate_purpose_export(
            family_root,
            receipt_path=family_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )

    cohort_root = tmp_path / "cohort-forgery"
    cohort_receipt = _stage2b_copy_export(result, cohort_root)
    cohort_path = cohort_root / "cohort.jsonl"
    members = [json.loads(line) for line in cohort_path.read_text(encoding="utf-8").splitlines()]
    members[0]["fund_keys"] = ["S999999999"]
    members[0]["known_at"] = "2099-01-01T00:00:00.000000Z"
    cohort_payload = {
        key: value
        for key, value in members[0].items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    members[0] = ncen._purpose_record("cohort_member", cohort_payload)
    members.sort(key=lambda item: item["record_id"])
    cohort_path.write_bytes(ncen._purpose_jsonl_bytes(members))
    _stage2b_reseal(cohort_root, cohort_receipt)
    with pytest.raises(ncen.NcenError, match="diagnostic_cohort_digest_mismatch"):
        ncen.validate_purpose_export(
            cohort_root,
            receipt_path=cohort_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )

    node_root = tmp_path / "node-forgery"
    node_receipt = _stage2b_copy_export(result, node_root)
    node_path = node_root / "nodes.jsonl"
    nodes = [json.loads(line) for line in node_path.read_text(encoding="utf-8").splitlines()]
    nodes[0]["selected_accession"] = "0000000001-26-999999"
    node_payload = {
        key: value
        for key, value in nodes[0].items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    nodes[0] = ncen._purpose_record("node", node_payload)
    node_contexts = [
        json.loads(line)
        for line in (node_root / "contexts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    node_context_order = {
        item["context_id"]: index for index, item in enumerate(node_contexts)
    }
    nodes.sort(key=lambda item: (node_context_order[item["context_id"]], item["record_id"]))
    node_path.write_bytes(ncen._purpose_jsonl_bytes(nodes))
    _stage2b_reseal(node_root, node_receipt)
    with pytest.raises(ncen.NcenError, match="diagnostic_node_selection_source_mismatch"):
        ncen.validate_purpose_export(
            node_root,
            receipt_path=node_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )

    context_root = tmp_path / "context-time-forgery"
    context_receipt = _stage2b_copy_export(result, context_root)
    context_path = context_root / "contexts.jsonl"
    context_rows = [
        json.loads(line) for line in context_path.read_text(encoding="utf-8").splitlines()
    ]
    context_rows[0]["knowledge_time"] = "2099-01-01T00:00:00.000000Z"
    context_payload = {
        key: value
        for key, value in context_rows[0].items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    context_rows[0] = ncen._purpose_record("context", context_payload)
    context_path.write_bytes(ncen._purpose_jsonl_bytes(context_rows))
    _stage2b_reseal(context_root, context_receipt)
    with pytest.raises(ncen.NcenError, match="diagnostic_context_knowledge_reconciliation_mismatch"):
        ncen.validate_purpose_export(
            context_root,
            receipt_path=context_receipt,
            code_root=ROOT,
            input_roots=input_roots,
        )


def test_uncertain_copy_conflict_exports_with_bound_source_ids(tmp_path: Path) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _assert_c1a_historical_envelope_refused(tmp_path)
        return
    pin = _stage2a_fixture(tmp_path / "source", xml_family_name="Acme Fund")
    sources = ncen.read_diagnostic_source_rows(pin)
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    index = _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
    )
    cohort = _stage2b_cohort((R,))
    declaration = _stage2b_declaration(pin, sources, cohort)
    result = ncen.write_purpose_diagnostics(
        index,
        cohort,
        sources,
        declaration=declaration,
        output_root=tmp_path / "uncertain-export",
        code_root=ROOT,
        input_roots={"ncen-source": pin.manifest_path.parent},
    )
    ncen.validate_purpose_export(
        result.root,
        receipt_path=result.receipt_path,
        code_root=ROOT,
        input_roots={"ncen-source": pin.manifest_path.parent},
    )
    families = [
        json.loads(line)
        for line in (result.root / "reported_families.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    incidences = [
        json.loads(line)
        for line in (result.root / "incidences.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert families[0]["state"] == "unknown"
    assert "diagnostic_b5_unattested" in families[0]["reasons"]
    assert incidences and all(item["attestation"] == "uncertain" for item in incidences)


def test_review_f09_writer_readback_rejects_resealed_uncertain_merge(tmp_path: Path) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _c1b2b3b1_historical_attack(tmp_path / "f09_fold", "f09_fold")
        return
    pin = _stage2a_fixture(
        tmp_path / "source", xml_family_name="Acme Fund", include_second_registrant=True,
    )
    sources = ncen.read_diagnostic_source_rows(pin)
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    first = _stage2a_filing(acceptance_at=accepted, header_retrieved_at=accepted,
                            retrieved_at=accepted, data_known_at=accepted)
    second = _stage2a_filing(
        acceptance_at=accepted, header_retrieved_at=accepted, retrieved_at=accepted,
        data_known_at=accepted, accession="0000000002-26-000001", registrant=cik(2),
    )
    index = ncen.NcenFilingIndex({cik(1): (first,), cik(2): (second,)}, {}, {})
    later = dt.date(2026, 4, 30)
    cohort = _stage2b_cohort(members_by_date=((R, 1), (later, 2)))
    declaration = _stage2b_declaration(pin, sources, cohort)
    snapshots = tuple(ncen.iter_purpose_snapshots(index, cohort, sources, declaration=declaration))
    assert len(snapshots) == 2
    uncertain = snapshots[0].incidences
    attested = snapshots[1].incidences
    assert uncertain and all(item.attestation == "uncertain" for item in uncertain)
    assert attested and all(item.attestation == "attested" for item in attested)
    assert {item.key_id for item in uncertain if item.key_id} & {
        item.key_id for item in attested if item.key_id
    }
    core_groups, core_memberships = ncen._purpose_fold_records(snapshots)
    root = tmp_path / "f09-export"
    input_roots = {"ncen-source": pin.manifest_path.parent}
    result = ncen.write_purpose_diagnostics(
        index, cohort, sources, declaration=declaration, output_root=root,
        code_root=ROOT, input_roots=input_roots,
    )
    ncen.validate_purpose_export(root, receipt_path=result.receipt_path,
                                 code_root=ROOT, input_roots=input_roots)
    groups_path = root / "fold_groups.jsonl"
    memberships_path = root / "fold_memberships.jsonl"
    groups = [json.loads(line) for line in groups_path.read_text(encoding="utf-8").splitlines()]
    memberships = [json.loads(line) for line in memberships_path.read_text(encoding="utf-8").splitlines()]
    assert {ncen._diagnostic_canonical(item) for item in groups} == {
        ncen._diagnostic_canonical(item) for item in core_groups
    }
    assert {ncen._diagnostic_canonical(item) for item in memberships} == {
        ncen._diagnostic_canonical(item) for item in core_memberships
    }
    uncertain_scope = ncen.fold_scope_id(
        mode="historical_reconstruction", ablation_id="uncertain_expanded",
        contexts=tuple((item.report_date, item.knowledge_cutoff, item.inventory_digest,
                        item.context_id) for item in snapshots),
    )
    target_groups = [item for item in groups if item["fold_scope_id"] == uncertain_scope]
    target_memberships = [item for item in memberships if item["fold_scope_id"] == uncertain_scope]
    assert len(target_groups) == len(target_memberships) == 2
    assert {item["cik"] for item in target_memberships} == {cik(1), cik(2)}
    assert {item["has_unknown_dependence"] for item in target_groups} == {True, False}

    merged_id = ncen.fold_group_id(scope_id=uncertain_scope, members=(cik(1), cik(2)))
    merged_group = ncen._purpose_record("fold_group", {
        "fold_scope_id": uncertain_scope, "fold_group_id": merged_id,
        "member_count": 2, "has_unknown_dependence": True,
        "usable_for_independence_claim": False,
    })
    merged_memberships = [ncen._purpose_record("fold_membership", {
        "fold_scope_id": uncertain_scope, "fold_group_id": merged_id, "cik": member,
    }) for member in (cik(1), cik(2))]
    groups_path.write_bytes(ncen._purpose_jsonl_bytes(sorted(
        [*(item for item in groups if item["fold_scope_id"] != uncertain_scope), merged_group],
        key=lambda item: item["record_id"],
    )))
    memberships_path.write_bytes(ncen._purpose_jsonl_bytes(sorted(
        [*(item for item in memberships if item["fold_scope_id"] != uncertain_scope),
         *merged_memberships], key=lambda item: item["record_id"],
    )))
    _stage2b_reseal(root, result.receipt_path)
    with pytest.raises(ncen.NcenError, match="diagnostic_fold_group_replay_mismatch"):
        ncen.validate_purpose_export(root, receipt_path=result.receipt_path,
                                     code_root=ROOT, input_roots=input_roots)


def test_receipt_is_final_commit_and_predecessor_must_exist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if ncen.DIAGNOSTIC_DECLARATION_VERSION.endswith("_v2"):
        _assert_c1a_historical_envelope_refused(tmp_path)
        return  # There is no v2 receipt to commit or resume at the C1a boundary.
    pin, sources, index, cohort, declaration = _stage2b_export_fixture(tmp_path / "late")
    original_validate = ncen._purpose_validate_export_root

    def fail_final(*args, **kwargs):
        if kwargs.get("require_receipt"):
            raise OSError("synthetic final validation failure")
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(ncen, "_purpose_validate_export_root", fail_final)
    output_root = tmp_path / "late" / "export"
    with pytest.raises(OSError, match="synthetic final validation failure"):
        ncen.write_purpose_diagnostics(
            index,
            cohort,
            sources,
            declaration=declaration,
            output_root=output_root,
            code_root=ROOT,
            input_roots={"ncen-source": pin.manifest_path.parent},
        )
    assert not (output_root.parent / "export.receipt.json").exists()
    monkeypatch.undo()

    pin2, sources2, index2, cohort2, _declaration2 = _stage2b_export_fixture(
        tmp_path / "predecessor"
    )
    fake_receipt = tmp_path / "fake.receipt.json"
    fake_receipt.write_bytes(
        ncen._purpose_json_bytes(
            {
                "schema_version": ncen.DIAGNOSTIC_RECEIPT_VERSION,
                "artifact_directory": "nonexistent-predecessor",
                "sha256sums_sha256": "1" * 64,
                "manifest_sha256": "2" * 64,
                "declaration_sha256": "3" * 64,
                "status": "complete",
                "predecessor_receipt_sha256": None,
                "diagnostic_only": True,
                "qualification": "NOT_EVALUABLE",
            }
        )
    )
    declaration2 = _stage2b_declaration(
        pin2,
        sources2,
        cohort2,
        predecessor_receipt_sha256=_stage2a_sha(fake_receipt),
    )
    with pytest.raises(ncen.NcenError, match="purpose_export_root_invalid"):
        ncen.write_purpose_diagnostics(
            index2,
            cohort2,
            sources2,
            declaration=declaration2,
            output_root=tmp_path / "predecessor" / "new-export",
            code_root=ROOT,
            input_roots={"ncen-source": pin2.manifest_path.parent},
            predecessor_receipt=fake_receipt,
        )


def test_record_id_hashes_schema_and_type_and_context_free_rows_are_global() -> None:
    payload = {
        "inventory_digest": "inventory",
        "R": "2026-03-31",
        "cik": cik(1),
        "fund_keys": ["S000000001"],
        "known_at": _stage2a_timestamp(K),
    }
    record = ncen._purpose_record("cohort_member", payload)
    identity = {
        "schema_version": ncen.DIAGNOSTIC_SCHEMA_VERSION,
        "record_type": "cohort_member",
        **payload,
    }
    expected = hashlib.sha256(
        json.dumps(
            identity,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert record["record_id"] == f"ncenrow:cohort_member:{expected}"


# === Consolidated FE-1 review regressions: sub-batch A-small (F5, F8) =====================
# Synthetic fixture lane only: every selection input is persisted in fixture files and issued
# loader custody by ``read_diagnostic_source_rows``; no production-attestation claim is made.

_REVIEW_LEI_A = "5493001Z012YSB2A0K51"
_REVIEW_LEI_B = "529900T8BM49AURSDO55"


def _review_f05_row(
    locator: str,
    copy: str,
    *,
    role: str = "current_primary",
    name: str | None = None,
    fn: str | None = None,
    crd: str | None = None,
    lei: str | None = None,
    answer: str | None = None,
    series_id: str | None = "S000000001",
    series_scope: str = "series",
) -> ncen.DiagnosticSourceRow:
    if role in {"underwriter", "b5"}:
        series_id, series_scope = None, "registrant"
    return dataclasses.replace(
        row(
            1,
            role,
            locator,
            answer=answer,
            name=name,
            file_number=fn,
            crd=crd,
            lei=lei,
            series_id=series_id,
            series_scope=series_scope,
        ),
        source_kind="edgar_xml",
        artifact_id=copy,
        artifact_path=f"{copy}.xml",
        artifact_sha256=hashlib.sha256(copy.encode()).hexdigest(),
        artifact_size=1,
        raw_row_sha256=hashlib.sha256(f"{copy}/{locator}".encode()).hexdigest(),
        source_copy_id="ncencopy:" + hashlib.sha256(copy.encode()).hexdigest(),
    )


def _review_f05_all_orders(rows: tuple[ncen.DiagnosticSourceRow, ...]) -> set[tuple[str, ...]]:
    return {ncen._diagnostic_copy_conflicts(ordered) for ordered in itertools.permutations(rows)}


def test_review_f05_nullable_copy_total_order() -> None:
    def pair(
        stem: str,
        *,
        a: dict[str, object],
        b: dict[str, object] | None = None,
    ) -> tuple[ncen.DiagnosticSourceRow, ncen.DiagnosticSourceRow]:
        return (
            _review_f05_row(f"a-{stem}", "copy-a", **a),  # type: ignore[arg-type]
            _review_f05_row(f"b-{stem}", "copy-b", **(a if b is None else b)),  # type: ignore[arg-type]
        )

    underwriter_fn = {"role": "underwriter", "name": "FN Only", "fn": "8-00001"}
    underwriter_crd = {"role": "underwriter", "name": "CRD Only", "crd": "000000021"}
    underwriter_lei = {"role": "underwriter", "name": "LEI Only", "lei": _REVIEW_LEI_A}
    agreeing_underwriters = (
        *pair("fn", a=underwriter_fn),
        *pair("crd", a=underwriter_crd),
        *pair("lei", a=underwriter_lei),
    )
    assert _review_f05_all_orders(agreeing_underwriters) == {()}

    adviser_series_fn = {"name": "Series Adviser", "fn": "801-00001"}
    adviser_unresolved_crd = {
        "name": "Unresolved Adviser",
        "crd": "000000011",
        "series_id": None,
        "series_scope": "unresolved",
    }
    adviser_lei_only = {"name": "Lei Adviser", "lei": _REVIEW_LEI_B}
    agreeing_advisers = (
        *pair("series", a=adviser_series_fn),
        *pair("unresolved", a=adviser_unresolved_crd),
        *pair("lei", a=adviser_lei_only),
    )
    assert _review_f05_all_orders(agreeing_advisers) == {()}

    b5_named = {"role": "b5", "answer": "Y", "name": "Acme Funds"}
    b5_null = {"role": "b5"}
    unnamed_adviser = {"fn": "801-00002"}
    mixed_roles = (
        *pair("b5-named", a=b5_named),
        *pair("b5-null", a=b5_null),
        *pair("unnamed", a=unnamed_adviser),
    )
    assert _review_f05_all_orders(mixed_roles) == {()}

    swapped_lei = (
        *pair(
            "swap-1",
            a={"role": "underwriter", "name": "U1", "fn": "8-00001", "lei": _REVIEW_LEI_A},
            b={"role": "underwriter", "name": "U1", "fn": "8-00001", "lei": _REVIEW_LEI_B},
        ),
        *pair(
            "swap-2",
            a={"role": "underwriter", "name": "U2", "crd": "000000022", "lei": _REVIEW_LEI_B},
            b={"role": "underwriter", "name": "U2", "crd": "000000022", "lei": _REVIEW_LEI_A},
        ),
    )
    assert _review_f05_all_orders(swapped_lei) == {("diagnostic_underwriter_lei_copy_conflict",)}

    identifier_presence = pair(
        "presence",
        a={"role": "underwriter", "name": "Same Underwriter", "fn": "8-00001"},
        b={"role": "underwriter", "name": "Same Underwriter", "crd": "000000021"},
    )
    assert _review_f05_all_orders((*identifier_presence, *pair("lei", a=underwriter_lei))) == {
        ("diagnostic_provider_name_copy_conflict", "diagnostic_underwriter_lei_copy_conflict")
    }

    multiplicity = (
        *pair("dup-1", a=underwriter_fn),
        _review_f05_row("a-dup-2", "copy-a", **underwriter_fn),  # type: ignore[arg-type]
    )
    assert _review_f05_all_orders(multiplicity) == {
        ("diagnostic_provider_name_copy_conflict", "diagnostic_underwriter_lei_copy_conflict")
    }

    series_association = (
        *pair(
            "assoc-1",
            a={"name": "Adviser One", "fn": "801-00001"},
            b={"name": "Adviser One", "fn": "801-00001", "series_id": None, "series_scope": "unresolved"},
        ),
        *pair(
            "assoc-2",
            a={"name": "Adviser Two", "fn": "801-00002", "series_id": None, "series_scope": "unresolved"},
            b={"name": "Adviser Two", "fn": "801-00002"},
        ),
    )
    assert _review_f05_all_orders(series_association) == {("diagnostic_provider_name_copy_conflict",)}

    b5_name_nulled = (
        *pair("b5-key", a=b5_named, b={"role": "b5", "answer": "Y"}),
        *pair("b5-null", a=b5_null),
    )
    assert _review_f05_all_orders(b5_name_nulled) == {("diagnostic_b5_copy_key_conflict",)}
    b5_answer_nulled = pair("b5-answer", a={"role": "b5"}, b={"role": "b5", "answer": "N"})
    assert _review_f05_all_orders((*b5_answer_nulled, *pair("b5-named", a=b5_named))) == {
        ("diagnostic_b5_copy_key_conflict",)
    }

    single_copy = tuple(item for item in agreeing_underwriters if item.artifact_id == "copy-a")
    assert _review_f05_all_orders(single_copy) == {()}


def _review_f05_baseline(
    extra: tuple[tuple[str, str | None, str | None, str | None], ...] = (),
) -> ncen.NcenFilingIndex:
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    return _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
        underwriters=(
            ncen.UnderwriterRecord("8-1", "21", _REVIEW_LEI_A, ("8-00001", "000000021", _REVIEW_LEI_A)),
            *(
                ncen.UnderwriterRecord(
                    ncen.normalize_file_number(fn), ncen.normalize_crd(crd), ncen.normalize_lei(lei), (fn, crd, lei)
                )
                for _name, fn, crd, lei in extra
            ),
        ),
    )


def test_review_f05_selection_nullable_copies_never_raise(tmp_path: Path) -> None:
    # Nullable identifiers are persisted in real copies: DERA keeps empty TSV fields, XML omits
    # the elements; the loader derives and verifies every row (no caller-injected rows).  The
    # verified copies must now agree with the selected baseline projection, so the baseline
    # lists the same underwriters.
    nullable = (
        ("Review Underwriter", "8-00002", None, None),
        ("Review Underwriter", None, "000000023", None),
        ("Review Underwriter", None, "000000024", _REVIEW_LEI_B),
    )
    sources = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(
            tmp_path / "agreeing",
            dera_extra_underwriters=nullable,
            xml_extra_underwriters=tuple(reversed(nullable)),
        )
    )
    underwriters = [item for item in sources.rows if item.role == "underwriter"]
    assert sorted(item.source_kind for item in underwriters) == ["dera"] * 4 + ["edgar_xml"] * 4
    assert len({item.source_copy_id for item in underwriters}) == 2
    agreeing = tuple(item for item in underwriters if item.name_raw == "Review Underwriter")
    assert len(agreeing) == 6
    assert {item.file_number_raw for item in agreeing if item.source_kind == "dera"} == {"8-00002", ""}
    assert {item.file_number_raw for item in agreeing if item.source_kind == "edgar_xml"} == {"8-00002", None}
    baseline = _review_f05_baseline(nullable)
    selected = ncen.diagnostic_selection(
        baseline,
        sources,
        cik(1),
        R,
        K,
        mode="historical_reconstruction",
    )
    assert selected.selection_reason is None
    assert selected.evidence_state == "complete"
    assert {item.source_row_id for item in agreeing} <= {item.source_row_id for item in selected.rows}
    assert all(item.attestation == "attested" for item in selected.rows)

    # LEI-only (null FN and CRD) copies still agree and stay attested; only the frozen legacy
    # profile marks the filing incomplete because the underwriter has no FN/CRD token.
    lei_only = (*nullable, ("Review Underwriter", None, None, _REVIEW_LEI_A))
    lei_sources = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(
            tmp_path / "lei-only",
            dera_extra_underwriters=lei_only,
            xml_extra_underwriters=tuple(reversed(lei_only)),
        )
    )
    lei_selected = ncen.diagnostic_selection(
        _review_f05_baseline(lei_only),
        lei_sources,
        cik(1),
        R,
        K,
        mode="historical_reconstruction",
    )
    assert lei_selected.selection_reason is None
    assert lei_selected.reasons == ("underwriter_without_identifier",)
    assert sum(1 for item in lei_selected.rows if item.role == "underwriter") == 10
    assert all(item.attestation == "attested" for item in lei_selected.rows)

    conflicting = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(
            tmp_path / "conflicting",
            dera_extra_underwriters=(("Review Underwriter", "8-00002", None, None),),
            xml_extra_underwriters=(("Review Underwriter", None, "000000023", None),),
        )
    )
    uncertain = ncen.diagnostic_selection(
        _review_f05_baseline(),
        conflicting,
        cik(1),
        R,
        K,
        mode="historical_reconstruction",
    )
    assert uncertain.selection_reason == "diagnostic_provider_name_copy_conflict"
    assert {
        "diagnostic_provider_name_copy_conflict",
        "diagnostic_underwriter_lei_copy_conflict",
    } <= set(uncertain.reasons)
    assert uncertain.rows and all(item.attestation == "uncertain" for item in uncertain.rows)


_REVIEW_F08_SAME_PERIOD = dt.date(2025, 12, 31)
_REVIEW_F08_ACCEPTED = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
_REVIEW_F08_OTHER = "0000000999-26-900001"


def _review_f08_select(
    root: Path,
    *quarantines: dict[str, object],
    selected_acceptance: dt.datetime | None = _REVIEW_F08_ACCEPTED,
    mode: str = "historical_reconstruction",
    data_accession: str = ACCESSION,
) -> tuple[ncen.DiagnosticSelection, ncen.DiagnosticSourceIndex]:
    """Persist the quarantine records in the fixture ledger, load it, then select ``ACCESSION``."""
    baseline = _stage2a_index(
        acceptance_at=selected_acceptance,
        header_retrieved_at=selected_acceptance,
        retrieved_at=_REVIEW_F08_ACCEPTED,
        data_known_at=_REVIEW_F08_ACCEPTED,
    )
    sources = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(root, accession=data_accession, extra_quarantines=quarantines)
    )
    return ncen.diagnostic_selection(baseline, sources, cik(1), R, K, mode=mode), sources


def _review_f08_predicate(
    record: dict[str, object],
    *,
    selected_acceptance: dt.datetime | None = _REVIEW_F08_ACCEPTED,
) -> bool:
    """The exact blocking rule of ``diagnostic_selection`` for one declared record's values.

    Loader-derived exclusions always carry a known CIK, period and one-second acceptance (F6), so
    unknown and sub-second values are exercised on the carrier with the same predicate that
    selection applies; representable records are also exercised end to end through the loader.
    """
    exclusion = ncen.DiagnosticSourceExclusion(
        accession_number=str(record["accession_number"]),
        registrant_cik=None if record["registrant_cik"] is None else str(record["registrant_cik"]),
        form_type="N-CEN/A",
        report_period_end=(
            None if record["report_period_end"] is None else dt.date.fromisoformat(str(record["report_period_end"]))
        ),
        acceptance_at=(
            None
            if record["acceptance_at"] is None
            else dt.datetime.strptime(str(record["acceptance_at"]), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        ),
        classification=str(record["classification"]),
        reasons=tuple(str(value) for value in record["reasons"]),  # type: ignore[attr-defined]
        ledger_sha256="0" * 64,
        ledger_size=1,
        locator="carrier#record=1",
    )
    return ncen._diagnostic_quarantine_blocks(
        exclusion,
        selected_accession=ACCESSION,
        cik=cik(1),
        report_date=R,
        cutoff=K,
        selected_period=_REVIEW_F08_SAME_PERIOD,
        selected_acceptance=selected_acceptance,
    )


def test_review_f08_quarantine_exact_acceptance_order(tmp_path: Path) -> None:
    def quarantine(
        *,
        period: dt.date | None = _REVIEW_F08_SAME_PERIOD,
        acceptance: dt.datetime | None,
        accession: str = _REVIEW_F08_OTHER,
        registrant: str | None = cik(1),
    ) -> dict[str, object]:
        return _stage2a_quarantine_record(
            accession=accession,
            registrant=registrant,
            period=period,
            acceptance=acceptance,
        )

    hour = dt.timedelta(hours=1)
    micro = dt.timedelta(microseconds=1)
    window_start = ncen.months_before(R, ncen.EFFECTIVE_WINDOW_MONTHS)
    older = dt.date(2025, 6, 30)
    assert window_start < older < _REVIEW_F08_SAME_PERIOD < R
    cases = {
        "same_period_strictly_earlier": (quarantine(acceptance=_REVIEW_F08_ACCEPTED - hour), False),
        "same_period_one_microsecond_earlier": (quarantine(acceptance=_REVIEW_F08_ACCEPTED - micro), False),
        "same_period_equal": (quarantine(acceptance=_REVIEW_F08_ACCEPTED), True),
        "same_period_one_microsecond_later": (quarantine(acceptance=_REVIEW_F08_ACCEPTED + micro), True),
        "same_period_strictly_later": (quarantine(acceptance=_REVIEW_F08_ACCEPTED + hour), True),
        "same_period_at_cutoff": (quarantine(acceptance=K), True),
        "same_period_unknown_acceptance": (quarantine(acceptance=None), True),
        "same_period_after_cutoff": (quarantine(acceptance=K + micro), False),
        "unknown_period_earlier": (quarantine(period=None, acceptance=_REVIEW_F08_ACCEPTED - hour), True),
        "unknown_period_unknown_acceptance": (quarantine(period=None, acceptance=None), True),
        "unknown_period_after_cutoff": (quarantine(period=None, acceptance=K + micro), False),
        "older_period_later_acceptance": (quarantine(period=older, acceptance=_REVIEW_F08_ACCEPTED + hour), False),
        "older_period_unknown_acceptance": (quarantine(period=older, acceptance=None), False),
        "window_start_period_unknown_acceptance": (quarantine(period=window_start, acceptance=None), False),
        "before_window_period": (
            quarantine(period=window_start - dt.timedelta(days=1), acceptance=None),
            False,
        ),
        "newer_period_earlier_acceptance": (quarantine(period=R, acceptance=_REVIEW_F08_ACCEPTED - hour), True),
        "newer_period_unknown_acceptance": (quarantine(period=dt.date(2026, 1, 31), acceptance=None), True),
        "newer_period_after_cutoff": (quarantine(period=R, acceptance=K + micro), False),
        "period_after_report_date": (
            quarantine(period=R + dt.timedelta(days=1), acceptance=_REVIEW_F08_ACCEPTED + hour),
            False,
        ),
        "other_registrant_same_period_later": (
            quarantine(registrant=cik(2), acceptance=_REVIEW_F08_ACCEPTED + hour),
            False,
        ),
        # Ordering is never inferred from accession sequence: acceptance time alone decides.
        "lower_accession_later_acceptance": (
            quarantine(accession="0000000001-26-000000", acceptance=_REVIEW_F08_ACCEPTED + hour),
            True,
        ),
        "higher_accession_earlier_acceptance": (
            quarantine(accession="0000000001-26-999999", acceptance=_REVIEW_F08_ACCEPTED - hour),
            False,
        ),
    }
    outcomes: dict[str, bool] = {}
    loader_checked = 0
    for label, (item, _expected) in cases.items():
        outcomes[label] = _review_f08_predicate(item)
        if not _stage2a_representable(item):
            # Unknown or sub-second values cannot be derived from sealed custody (F6).
            with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_mismatch"):
                _review_f08_select(tmp_path / label, item)
            continue
        selected, loaded = _review_f08_select(tmp_path / label, item)
        loader_checked += 1
        assert _stage2a_ledger_matches(loaded, item), label
        assert (selected.selection_reason == "diagnostic_quarantined_competitor") is outcomes[label], label
        if outcomes[label]:
            assert selected.evidence_state == "incomplete" and not selected.rows, label
            assert "diagnostic_quarantined_competitor" in selected.reasons, label
        else:
            assert selected.selection_reason is None, label
            assert selected.evidence_state == "complete", label
            assert selected.rows and all(row.attestation == "attested" for row in selected.rows), label
    assert outcomes == {label: expected for label, (_item, expected) in cases.items()}
    assert loader_checked >= 10
    # Second-resolution variants of the sub-second cases keep the same outcome end to end.
    second = dt.timedelta(seconds=1)
    for label, item, expected in (
        ("second_earlier", quarantine(acceptance=_REVIEW_F08_ACCEPTED - second), False),
        ("second_later", quarantine(acceptance=_REVIEW_F08_ACCEPTED + second), True),
        ("second_after_cutoff", quarantine(acceptance=K + second), False),
        ("newer_second_after_cutoff", quarantine(period=R, acceptance=K + second), False),
    ):
        assert _review_f08_predicate(item) is expected, label
        selected, loaded = _review_f08_select(tmp_path / label, item)
        assert _stage2a_ledger_matches(loaded, item), label
        assert (selected.selection_reason == "diagnostic_quarantined_competitor") is expected, label

    earlier = cases["same_period_strictly_earlier"][0]
    # One sealed terminal per accession: the later competitor is its own accession.
    later = quarantine(acceptance=_REVIEW_F08_ACCEPTED + hour, accession="0000000999-26-900002")
    for mode in ("historical_reconstruction", "current_run"):
        replacement, loaded = _review_f08_select(tmp_path / f"replacement-{mode}", earlier, mode=mode)
        assert replacement.evidence_state == "complete", mode
        assert _stage2a_ledger_matches(loaded, earlier), mode
        blocked, _ = _review_f08_select(tmp_path / f"blocked-{mode}", earlier, later, mode=mode)
        assert blocked.selection_reason == "diagnostic_quarantined_competitor", mode

    unknown_selected, _ = _review_f08_select(
        tmp_path / "unknown-selected", earlier, selected_acceptance=None
    )
    assert unknown_selected.accession_number == ACCESSION
    assert unknown_selected.selection_reason == "diagnostic_quarantined_competitor"
    assert not unknown_selected.rows
    assert not _review_f08_predicate(quarantine(acceptance=K + micro), selected_acceptance=None)
    unknown_selected_after_k, _ = _review_f08_select(
        tmp_path / "unknown-selected-after-k",
        quarantine(acceptance=K + second),
        selected_acceptance=None,
    )
    assert unknown_selected_after_k.selection_reason == "filing_acceptance_unattested"


@pytest.mark.parametrize("mode", ["historical_reconstruction", "current_run"])
def test_review_f08_same_accession_quarantine_precedence(tmp_path: Path, mode: str) -> None:
    """Precedence: known acceptance after K is invisible as of K for every record, the
    selected accession included; a K-eligible record for the selected accession then blocks
    whatever its period, time or recorded CIK; other accessions follow exact ordering.

    Loader custody forbids verified source rows for a quarantined accession
    (``diagnostic_quarantine_source_overlap``), so the persisted fixture data belongs to another
    accession and ``ACCESSION`` has no verified header: a non-blocking record is observed as
    ``filing_acceptance_unattested`` instead of the unreachable ``complete``.
    """
    data_accession = "0000000001-26-000002"
    # Source-manifest invariant: one acquisition terminal per accession, so verified rows and a
    # quarantine record for the SAME accession are manifest corruption, rejected at load
    # independently of K, period, recorded CIK or acceptance. Were a post-K same-accession record
    # ever loadable, ``complete`` would become representable below and this test would need a
    # different construction; K-scoped filtering stays in selection only.
    after_k = K + dt.timedelta(microseconds=1)
    overlap_cases: dict[str, tuple[dt.datetime | None, dt.date | None, str | None]] = {
        # label: (acceptance, report period, recorded CIK)
        "at_k": (K, _REVIEW_F08_SAME_PERIOD, cik(1)),
        "after_k": (after_k, _REVIEW_F08_SAME_PERIOD, cik(1)),
        "well_after_k": (K + dt.timedelta(days=30), _REVIEW_F08_SAME_PERIOD, cik(1)),
        "after_k_unknown_period_null_cik": (after_k, None, None),
        "after_k_older_period_other_cik": (after_k, dt.date(2025, 6, 30), cik(2)),
        "unknown_acceptance": (None, _REVIEW_F08_SAME_PERIOD, cik(1)),
    }
    for label, (acceptance, period, registrant) in overlap_cases.items():
        overlap = _stage2a_quarantine_record(
            accession=ACCESSION, registrant=registrant, period=period, acceptance=acceptance
        )
        with pytest.raises(ncen.NcenError, match="diagnostic_quarantine_source_overlap"):
            ncen.read_diagnostic_source_rows(
                _stage2a_fixture(tmp_path / f"overlap-{label}", extra_quarantines=(overlap,))
            )

    def quarantine(
        *,
        acceptance: dt.datetime | None,
        period: dt.date | None = _REVIEW_F08_SAME_PERIOD,
        accession: str = ACCESSION,
        registrant: str | None = cik(1),
    ) -> dict[str, object]:
        return _stage2a_quarantine_record(
            accession=accession,
            registrant=registrant,
            period=period,
            acceptance=acceptance,
        )

    unblocked, _ = _review_f08_select(tmp_path / "control", mode=mode, data_accession=data_accession)
    assert unblocked.accession_number == ACCESSION
    assert unblocked.selection_reason == "filing_acceptance_unattested" and not unblocked.rows

    hour = dt.timedelta(hours=1)
    micro = dt.timedelta(microseconds=1)
    older = dt.date(2025, 6, 30)
    before_selected = _REVIEW_F08_ACCEPTED - hour
    cases = {
        # Known acceptance after K: excluded from K-scoped blocking, retained for audit.
        "post_k_same_period": (quarantine(acceptance=K + micro), False),
        "post_k_older_period": (quarantine(acceptance=K + micro, period=older), False),
        "post_k_unknown_period": (quarantine(acceptance=K + micro, period=None), False),
        "post_k_wrong_cik": (quarantine(acceptance=K + micro, registrant=cik(2)), False),
        "post_k_null_cik": (quarantine(acceptance=K + micro, registrant=None), False),
        # Exact-K boundary is K-eligible: the selected accession cannot self-cure.
        "exact_k_same_period": (quarantine(acceptance=K), True),
        "exact_k_wrong_cik": (quarantine(acceptance=K, registrant=cik(2)), True),
        "exact_k_null_cik": (quarantine(acceptance=K, registrant=None), True),
        # Before K: no earlier time or period, and no wrong/null CIK, cures the selection.
        "pre_k_earlier_same_period": (quarantine(acceptance=before_selected), True),
        "pre_k_older_period": (quarantine(acceptance=before_selected, period=older), True),
        "pre_k_period_after_report_date": (
            quarantine(acceptance=before_selected, period=R + dt.timedelta(days=1)),
            True,
        ),
        "pre_k_equal_time": (quarantine(acceptance=_REVIEW_F08_ACCEPTED), True),
        "pre_k_later_time": (quarantine(acceptance=_REVIEW_F08_ACCEPTED + hour), True),
        "pre_k_wrong_cik": (quarantine(acceptance=before_selected, registrant=cik(2)), True),
        "pre_k_null_cik": (quarantine(acceptance=before_selected, registrant=None), True),
        "pre_k_wrong_cik_older_period": (
            quarantine(acceptance=before_selected, registrant=cik(2), period=older),
            True,
        ),
        # Unknown acceptance stays conservative.
        "unknown_time": (quarantine(acceptance=None), True),
        "unknown_time_null_cik_unknown_period": (
            quarantine(acceptance=None, registrant=None, period=None),
            True,
        ),
        # Other accessions keep exact ordering: strictly earlier same period does not block.
        "other_accession_strictly_earlier": (
            quarantine(accession=_REVIEW_F08_OTHER, acceptance=before_selected),
            False,
        ),
        "other_accession_later": (
            quarantine(accession=_REVIEW_F08_OTHER, acceptance=_REVIEW_F08_ACCEPTED + hour),
            True,
        ),
    }
    outcomes: dict[str, bool] = {}
    loader_checked = 0
    for label, (item, _expected) in cases.items():
        outcomes[label] = _review_f08_predicate(item)
        if not _stage2a_representable(item):
            # Unknown CIK/period/time or sub-second values are not derivable from custody (F6).
            with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_mismatch"):
                _review_f08_select(tmp_path / label, item, mode=mode, data_accession=data_accession)
            continue
        selected, loaded = _review_f08_select(
            tmp_path / label, item, mode=mode, data_accession=data_accession
        )
        loader_checked += 1
        assert _stage2a_ledger_matches(loaded, item), label
        assert selected.accession_number == ACCESSION, label
        assert selected.evidence_state == "incomplete" and not selected.rows, label
        assert (selected.selection_reason == "diagnostic_quarantined_competitor") is outcomes[label], label
        if outcomes[label]:
            assert "diagnostic_quarantined_competitor" in selected.reasons, label
        else:
            # Not blocked by the quarantine; the selected accession simply has no verified header.
            assert selected.selection_reason == "filing_acceptance_unattested", label
            assert "diagnostic_quarantined_competitor" not in selected.reasons, label
    assert outcomes == {label: expected for label, (_item, expected) in cases.items()}
    assert loader_checked >= 11
    second = dt.timedelta(seconds=1)
    post_k_sealed = quarantine(acceptance=K + second)
    for label, item in (
        ("post_k_second_same_period", post_k_sealed),
        ("post_k_second_older_period", quarantine(acceptance=K + second, period=older)),
        ("post_k_second_wrong_cik", quarantine(acceptance=K + second, registrant=cik(2))),
    ):
        assert not _review_f08_predicate(item), label
        selected, _ = _review_f08_select(tmp_path / label, item, mode=mode, data_accession=data_accession)
        assert selected.selection_reason == "filing_acceptance_unattested", label

    earlier_other = cases["other_accession_strictly_earlier"][0]
    combined, loaded = _review_f08_select(
        tmp_path / "combined", post_k_sealed, earlier_other, mode=mode, data_accession=data_accession
    )
    assert combined.selection_reason == "filing_acceptance_unattested"
    assert _stage2a_ledger_matches(loaded, post_k_sealed) and _stage2a_ledger_matches(loaded, earlier_other)
    # A non-blocking post-K record never masks a blocking one. Sealed custody holds one terminal
    # per accession, so the blocking record is another accession of the same registrant.
    blocked, _ = _review_f08_select(
        tmp_path / "combined-blocked",
        post_k_sealed,
        cases["other_accession_later"][0],
        mode=mode,
        data_accession=data_accession,
    )
    assert blocked.selection_reason == "diagnostic_quarantined_competitor"
    assert not _review_f08_predicate(cases["post_k_same_period"][0])
    assert _review_f08_predicate(cases["pre_k_older_period"][0])


# === Consolidated FE-1 review regressions: sub-batch A-custody (F1, F12) ==================
# Trust is issued only by ``read_diagnostic_source_rows`` after verifying actual fixture files.
# Graph admission of directly constructed selections/incidences is finding 4 (batch B).


class _Tripwire(tuple):  # type: ignore[type-arg]
    """Tuple that fails the test if a consumer touches it after it is armed."""

    armed = False

    def _check(self) -> None:
        if type(self).armed:
            raise AssertionError("source payload consumed before custody check")

    def __iter__(self):  # type: ignore[no-untyped-def]
        self._check()
        return super().__iter__()

    def __len__(self) -> int:
        self._check()
        return super().__len__()

    def __getitem__(self, item):  # type: ignore[no-untyped-def]
        self._check()
        return super().__getitem__(item)


def _review_f01_baseline() -> ncen.NcenFilingIndex:
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    return _stage2a_index(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
    )


def test_review_f01_source_index_requires_loader_custody(tmp_path: Path) -> None:
    pin = _stage2a_fixture(tmp_path / "fixture")
    sources = ncen.read_diagnostic_source_rows(pin)
    baseline = _review_f01_baseline()
    assert sources.custody_lane() == "synthetic_fixture"
    control = ncen.diagnostic_selection(baseline, sources, cik(1), R, K, mode="historical_reconstruction")
    assert control.evidence_state == "complete" and control.rows

    # The review attack: a nonexistent header and XML path, every adviser asserting invented
    # CRD 999999, bound to the real manifest identity with a recomputed public evidence digest.
    # Before loader custody this index selected ``complete`` with attested CRD 999999 rows.
    header = next(item for item in sources.rows if item.role == "header")
    adviser = next(
        item for item in sources.rows if item.role == "current_primary" and item.source_kind == "edgar_xml"
    )
    invented_header = dataclasses.replace(
        header,
        artifact_id="invented-header",
        artifact_path="invented/submission.txt",
        artifact_sha256="a" * 64,
        raw_row_sha256="a" * 64,
        source_copy_id="ncencopy:" + "a" * 64,
    )

    def invent(item: ncen.DiagnosticSourceRow) -> ncen.DiagnosticSourceRow:
        if item is header:
            return invented_header
        changes: dict[str, object] = {"header_source_id": invented_header.source_row_id}
        if item.source_kind == "edgar_xml":
            changes["artifact_path"] = "invented/primary_doc.xml"
        if item.role == "current_primary":
            changes["crd_raw"] = "999999"
        return dataclasses.replace(item, **changes)  # type: ignore[arg-type]

    assert not (pin.manifest_path.parent / "invented").exists()
    forged_rows = tuple(sorted((invent(item) for item in sources.rows), key=lambda item: item.source_row_id))
    assert sources.manifest_sha256 is not None and sources.manifest_kind is not None
    recomputed = ncen._diagnostic_source_index_evidence_digest(
        sources.manifest_sha256, forged_rows, sources.exclusions, sources.boundary_examples
    )
    # Re-sorted by ID: DERA row IDs follow the fixture ZIP's wall-clock member timestamps, so an
    # unsorted mutation would sometimes fail structural ordering instead of reaching custody.
    mutated_rows = tuple(
        sorted(
            (dataclasses.replace(item, crd_raw="999999") if item == adviser else item for item in sources.rows),
            key=lambda item: item.source_row_id,
        )
    )
    transplanted = dataclasses.replace(sources, rows=forged_rows, evidence_digest=recomputed)
    object.__setattr__(transplanted, "_custody", sources._custody)
    forgeries = {
        "public_constructor": ncen.DiagnosticSourceIndex(
            forged_rows,
            sources.exclusions,
            sources.boundary_examples,
            sources.manifest_sha256,
            sources.manifest_size,
            sources.manifest_kind,
            recomputed,
        ),
        "from_rows": ncen.DiagnosticSourceIndex.from_rows(sources.rows),
        "dataclasses_replace_forged": dataclasses.replace(sources, rows=forged_rows, evidence_digest=recomputed),
        "dataclasses_replace_identical": dataclasses.replace(sources),
        "row_mutation_rebound": dataclasses.replace(
            sources,
            rows=mutated_rows,
            evidence_digest=ncen._diagnostic_source_index_evidence_digest(
                sources.manifest_sha256, mutated_rows, sources.exclusions, sources.boundary_examples
            ),
        ),
        "copied_proof": transplanted,
        "copy": copy.copy(sources),
        "deepcopy": copy.deepcopy(sources),
        "unpickled": pickle.loads(pickle.dumps(sources)),
        "sealed_source_relabel": dataclasses.replace(sources, manifest_kind="sealed_source"),
    }
    for label, forged in forgeries.items():
        with pytest.raises(ncen.NcenError, match="diagnostic_source_index_unbound"):
            ncen.diagnostic_selection(baseline, forged, cik(1), R, K, mode="historical_reconstruction")
        with pytest.raises(ncen.NcenError, match="diagnostic_source_index_unbound"):
            forged.custody_lane()
        with pytest.raises(ncen.NcenError, match="diagnostic_source_index_unbound"):
            forged.work_counters()
        assert forged is not sources, label

    # No constructor, replace or serialized flag can carry the loader capability.
    with pytest.raises(TypeError):
        ncen.DiagnosticSourceIndex(sources.rows, _custody=sources._custody)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        ncen.DiagnosticSourceIndex(sources.rows, verified=True)  # type: ignore[call-arg]
    with pytest.raises((TypeError, ValueError)):  # init=False field; exception type varies by Python
        dataclasses.replace(sources, _custody=sources._custody)
    with pytest.raises(TypeError):
        ncen._DiagnosticSourceCustody()
    with pytest.raises(TypeError):
        pickle.dumps(sources._custody)
    with pytest.raises(AttributeError):
        sources._custody.rows = forged_rows  # type: ignore[misc]
    assert not [name for name, value in vars(ncen).items() if isinstance(value, ncen._DiagnosticSourceCustody)]

    # Snapshot and export entry points refuse before touching any source row or exclusion.
    _pin, export_sources, index, cohort, declaration = _stage2b_export_fixture(tmp_path / "export")
    snapshot_value = next(ncen.iter_purpose_snapshots(index, cohort, export_sources, declaration=declaration))
    _records, source_ids = ncen._purpose_source_records(export_sources)
    tripwire = ncen.DiagnosticSourceIndex(
        _Tripwire(export_sources.rows),
        _Tripwire(export_sources.exclusions),
        export_sources.boundary_examples,
        export_sources.manifest_sha256,
        export_sources.manifest_size,
        export_sources.manifest_kind,
        export_sources.evidence_digest,
    )
    output_root = tmp_path / "export" / "forged-export"
    _Tripwire.armed = True
    try:
        entry_points = {
            "diagnostic_selection": lambda: ncen.diagnostic_selection(
                index, tripwire, cik(1), R, K, mode="historical_reconstruction"
            ),
            "iter_purpose_snapshots": lambda: next(
                ncen.iter_purpose_snapshots(index, cohort, tripwire, declaration=declaration)
            ),
            "write_purpose_diagnostics": lambda: ncen.write_purpose_diagnostics(
                index,
                cohort,
                tripwire,
                declaration=declaration,
                output_root=output_root,
                code_root=ROOT,
                input_roots={"ncen-source": _pin.manifest_path.parent},
            ),
            "source_records": lambda: ncen._purpose_source_records(tripwire),
            "exclusion_records": lambda: ncen._purpose_exclusion_records((), tripwire, source_ids),
            "excluded_incidence_count": lambda: ncen._purpose_excluded_incidence_count(
                snapshot_value, tripwire
            ),
            "contextual_source_ids": lambda: ncen._purpose_contextual_source_ids(
                snapshot_value, tripwire, source_ids
            ),
        }
        for label, call in entry_points.items():
            if label == "write_purpose_diagnostics":
                with pytest.raises(TypeError):
                    call()
            else:
                with pytest.raises(ncen.NcenError, match="diagnostic_source_index_unbound"):
                    call()
            assert label
    finally:
        _Tripwire.armed = False
    assert not output_root.exists()

    # Fixture authority cannot switch to sealed-source trust: pin-less sealed_source stays HOLD
    # before any artifact read (F6 membership needs caller custody; F7 pins remain HOLD).
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_unverified"):
        ncen.read_diagnostic_source_rows(_stage2a_fixture(tmp_path / "sealed", manifest_kind="sealed_source"))
    sealed_declaration = json.loads(json.dumps(declaration))
    sealed_declaration["ncen_source_manifest"]["kind"] = "sealed_source"
    with pytest.raises(ncen.NcenError, match="declaration_source_manifest_binding_mismatch"):
        next(ncen.iter_purpose_snapshots(index, cohort, export_sources, declaration=sealed_declaration))

    # Changed real fixture bytes fail against the external pin (same length, new content).
    for name, old, new in (
        ("submission.txt", b"<ACCEPTANCE-DATETIME>20260301120000", b"<ACCEPTANCE-DATETIME>20260301120001"),
        ("primary_doc.xml", b"Shared Adviser", b"Shared Advisor"),
    ):
        changed_pin = _stage2a_fixture(tmp_path / f"changed-{name}")
        target = changed_pin.manifest_path.parent / name
        payload = target.read_bytes()
        assert old in payload
        target.write_bytes(payload.replace(old, new))
        with pytest.raises(ncen.NcenError, match="diagnostic_artifact_sha256_mismatch"):
            ncen.read_diagnostic_source_rows(changed_pin)

    # Reflection that mutates a loader-issued row in place is caught when the row is consumed.
    victim = ncen.read_diagnostic_source_rows(_stage2a_fixture(tmp_path / "victim"))
    victim_row = next(item for item in victim.rows if item.role == "current_primary")
    object.__setattr__(victim_row, "crd_raw", "999999")
    with pytest.raises(ncen.NcenError, match="diagnostic_source_row_custody_mismatch"):
        ncen.diagnostic_selection(baseline, victim, cik(1), R, K, mode="historical_reconstruction")


_REVIEW_F12_IRRELEVANT = 12


def _review_f12_fixture(
    tmp_path: Path,
    *,
    registrants: int,
    relevant_quarantines: int,
) -> tuple[ncen.DiagnosticSourceManifestPin, tuple[str, ...]]:
    """One DERA package plus one acceptance header per registrant; fixed source count."""
    root = tmp_path / "stage2a-f12"
    root.mkdir(parents=True)
    accessions = tuple(f"0000000001-26-{number:06d}" for number in range(1, registrants + 1))
    tables: dict[str, tuple[tuple[str, ...], list[tuple[str, ...]]]] = {
        "SUBMISSION": (
            ("ACCESSION_NUMBER", "SUBMISSION_TYPE", "CIK", "FILING_DATE", "REPORT_ENDING_PERIOD"),
            [],
        ),
        "REGISTRANT": (
            ("ACCESSION_NUMBER", "CIK", "IS_FAMILY_INVESTMENT_COMPANY", "FAMILY_INVESTMENT_COMPANY_NAME"),
            [],
        ),
        "FUND_REPORTED_INFO": (("FUND_ID", "ACCESSION_NUMBER", "SERIES_ID"), []),
        "ADVISER": (
            ("FUND_ID", "ADVISER_TYPE", "FILE_NUM", "CRD_NUM", "ADVISER_LEI", "ADVISER_NAME"),
            [],
        ),
        "PRINCIPAL_UNDERWRITER": (
            ("ACCESSION_NUMBER", "FILE_NUM", "CRD_NUM", "UNDERWRITER_LEI", "UNDERWRITER_NAME"),
            [],
        ),
    }
    for number, accession in enumerate(accessions, start=1):
        registrant = cik(number)
        fund_id = f"{accession}_{registrant}_S000000001"
        tables["SUBMISSION"][1].append((accession, "N-CEN", registrant, "01-MAR-2026", "31-DEC-2025"))
        tables["REGISTRANT"][1].append((accession, registrant, "Y", "Acme Funds"))
        tables["FUND_REPORTED_INFO"][1].append((fund_id, accession, "S000000001"))
        tables["ADVISER"][1].append((fund_id, "Advisor", "801-00001", "000000011", "", "Shared Adviser"))
        tables["PRINCIPAL_UNDERWRITER"][1].append(
            (accession, "8-00001", "000000021", _REVIEW_LEI_A, "Shared Underwriter")
        )
    package = root / "dera.zip"
    member_payloads: dict[str, bytes] = {}
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for table in ncen.PINNED_TABLES:
            header_row, rows = tables[table]
            payload = ("\n".join(("\t".join(header_row), *("\t".join(item) for item in rows))) + "\n").encode()
            member = f"fixture/{table}.tsv"
            member_payloads[member] = payload
            archive.writestr(member, payload)
    accepted = _REVIEW_F08_ACCEPTED
    common = {
        "retrieved_at": _stage2a_timestamp(K - dt.timedelta(days=1)),
        "public_at": _stage2a_timestamp(accepted),
        "data_known_at": _stage2a_timestamp(accepted),
    }
    artifacts: list[dict[str, object]] = []
    for number, accession in enumerate(accessions, start=1):
        header = root / f"submission-{number:03d}.txt"
        header.write_text(
            "<SEC-HEADER>\n"
            f"<ACCESSION-NUMBER>{accession}\n"
            "<ACCEPTANCE-DATETIME>20260301120000\n"
            "<TYPE>N-CEN\n"
            f"<CIK>{cik(number)}\n"
            "<FILING-DATE>20260301\n"
            "<PERIOD>20251231\n"
            "</SEC-HEADER>\n",
            encoding="utf-8",
            newline="\n",
        )
        artifacts.append(
            {
                "artifact_id": f"header-{number:03d}",
                "kind": "header",
                "path": header.name,
                "sha256": _stage2a_sha(header),
                "bytes": header.stat().st_size,
                "accession_number": accession,
                "registrant_cik": cik(number),
                "source_url": f"https://www.sec.gov/Archives/fixture-{number:03d}.txt",
                **common,
            }
        )
    artifacts.append(
        {
            "artifact_id": "dera-1",
            "kind": "dera_zip",
            "path": package.name,
            "sha256": _stage2a_sha(package),
            "bytes": package.stat().st_size,
            "package_label": "fixture-2026q1",
            "members": [
                {
                    "path": member,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "bytes": len(payload),
                    "header": list(tables[Path(member).stem][0]),
                }
                for member, payload in sorted(member_payloads.items())
            ],
            **common,
        }
    )
    # Irrelevant quarantines (other registrants) plus CIK-relevant older in-window records that
    # are visited but never block; all derived by the loader from one sealed acquisition run.
    entries = [
        _AcqEntry(f"0000000999-26-{number:06d}", cik(number), "absent_amended_accession")
        for number in range(10_000, 10_000 + _REVIEW_F12_IRRELEVANT)
    ]
    entries.extend(
        _AcqEntry(
            f"0000000999-27-{number:06d}",
            cik(number),
            "schema_unavailable",
            acceptance=accepted + dt.timedelta(hours=1),
            period=dt.date(2025, 6, 30),
        )
        for number in range(1, relevant_quarantines + 1)
    )
    seal = _acquisition_seal(root / "acquisition", tuple(entries))
    pin = _stage2a_write_manifest(
        root,
        artifacts,
        seal,
        quarantines=[dict(item) for item in seal.quarantines],
        boundaries=[],
    )
    return pin, accessions


def test_review_f12_source_index_work_linear(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registrants, relevant = 24, 6
    pin, accessions = _review_f12_fixture(tmp_path, registrants=registrants, relevant_quarantines=relevant)
    sources = ncen.read_diagnostic_source_rows(pin)
    total_rows = len(sources.rows)
    rows_per_accession = {
        accession: sum(1 for item in sources.rows if item.accession_number == accession) for accession in accessions
    }
    assert set(rows_per_accession.values()) == {4}  # header, B.5, adviser, underwriter
    assert total_rows == 4 * registrants and len(sources.exclusions) == _REVIEW_F12_IRRELEVANT + relevant
    loaded = sources.work_counters()
    assert loaded["index_passes"] == 1
    assert loaded["row_ids_computed"] == total_rows
    assert loaded["evidence_digest_builds"] == 1
    assert loaded["lookup_builds"] == 1
    assert loaded["accession_lookups"] == loaded["quarantine_lookups"] == 0
    filings = {
        cik(number): (
            _stage2a_filing(
                acceptance_at=dt.datetime(2026, 3, 1, 17, tzinfo=UTC),
                header_retrieved_at=dt.datetime(2026, 3, 1, 17, tzinfo=UTC),
                retrieved_at=dt.datetime(2026, 3, 1, 17, tzinfo=UTC),
                data_known_at=dt.datetime(2026, 3, 1, 17, tzinfo=UTC),
                accession=accession,
                registrant=cik(number),
            ),
        )
        for number, accession in enumerate(accessions, start=1)
    }
    baseline = ncen.NcenFilingIndex(filings, {}, {})

    digest_calls = 0
    original_digest = ncen._diagnostic_source_index_evidence_digest
    original_digest_ids = ncen._diagnostic_source_index_evidence_digest_from_ids

    def counted_digest(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal digest_calls
        digest_calls += 1
        return original_digest(*args, **kwargs)

    def counted_digest_ids(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal digest_calls
        digest_calls += 1
        return original_digest_ids(*args, **kwargs)

    row_id_calls = 0
    original_row_id = ncen.DiagnosticSourceRow.source_row_id

    def counted_row_id(self):  # type: ignore[no-untyped-def]
        nonlocal row_id_calls
        row_id_calls += 1
        return original_row_id.fget(self)

    monkeypatch.setattr(ncen, "_diagnostic_source_index_evidence_digest", counted_digest)
    monkeypatch.setattr(ncen, "_diagnostic_source_index_evidence_digest_from_ids", counted_digest_ids)
    monkeypatch.setattr(ncen.DiagnosticSourceRow, "source_row_id", property(counted_row_id))

    measurements: list[tuple[int, dict[str, int], int]] = []
    for count in (1, 2, 4, 8, 16, 24):
        before = sources.work_counters()
        row_ids_before = row_id_calls
        for number in range(1, count + 1):
            selected = ncen.diagnostic_selection(
                baseline, sources, cik(number), R, K, mode="historical_reconstruction"
            )
            assert selected.evidence_state == "complete", number
            assert selected.accession_number == accessions[number - 1]
        after = sources.work_counters()
        measurements.append(
            (count, {key: after[key] - before[key] for key in after}, row_id_calls - row_ids_before)
        )
    for count, delta, row_ids in measurements:
        assert delta["index_passes"] == delta["row_ids_computed"] == 0, count
        assert delta["evidence_digest_builds"] == delta["lookup_builds"] == 0, count
        assert delta["accession_lookups"] == count, count
        assert delta["accession_rows_visited"] == 4 * count, count
        assert delta["quarantine_lookups"] == count, count
        assert delta["quarantine_rows_visited"] == min(count, relevant), count
        assert row_ids < count * total_rows, count
    per_cik = {row_ids / count for count, _delta, row_ids in measurements}
    assert len(per_cik) == 1, per_cik  # exactly linear in selected CIKs
    assert next(iter(per_cik)) <= 8 * 4 < total_rows
    assert digest_calls == 0

    absent = sources.work_counters()
    missing = ncen.diagnostic_selection(baseline, sources, cik(9_999), R, K, mode="historical_reconstruction")
    assert missing.selection_reason == "no_effective_filing"
    assert sources.work_counters()["accession_rows_visited"] == absent["accession_rows_visited"]
    assert sources.work_counters()["quarantine_rows_visited"] == absent["quarantine_rows_visited"]

    # Increasing report dates reuse the single handle build; each context visits its own rows.
    monkeypatch.undo()
    witness_calls = 0
    original_witness = ncen._purpose_source_witness_key

    def counted_witness(row):  # type: ignore[no-untyped-def]
        nonlocal witness_calls
        witness_calls += 1
        return original_witness(row)

    monkeypatch.setattr(ncen, "_purpose_source_witness_key", counted_witness)
    for dates in ((R,), (R, dt.date(2026, 4, 30), dt.date(2026, 5, 31))):
        _pin, export_sources, index, cohort, declaration = _stage2b_export_fixture(
            tmp_path / f"dates-{len(dates)}", report_dates=dates
        )
        accession_rows = len(export_sources.rows_for(ACCESSION))
        before = export_sources.work_counters()
        snapshots = tuple(ncen.iter_purpose_snapshots(index, cohort, export_sources, declaration=declaration))
        _records, source_ids = ncen._purpose_source_records(export_sources)
        witness_calls = 0
        for snapshot_value in snapshots:
            ncen._purpose_contextual_source_ids(snapshot_value, export_sources, source_ids)
            ncen._purpose_excluded_incidence_count(snapshot_value, export_sources)
        after = export_sources.work_counters()
        assert len(snapshots) == len(dates)
        assert after["evidence_digest_builds"] == after["lookup_builds"] == after["index_passes"] == 1
        assert after["accession_lookups"] - before["accession_lookups"] == len(dates)
        assert after["accession_rows_visited"] - before["accession_rows_visited"] == len(dates) * accession_rows
        assert witness_calls == 0  # attested rows keep their loader identity; no per-context scan


# === Consolidated FE-1 review regressions: F3 eligible-copy amendment schema ===============
# The frozen merged baseline may carry ``X0505`` for an ``N-CEN/A`` because an XML copy exists
# somewhere; only an XML copy eligible at K/mode may establish complete-replacement semantics.

_REVIEW_F03_ACCEPTED = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
_REVIEW_F03_OLDER = "0000000001-25-000009"


def _review_f03_baseline(*, family_name: str = "Acme Funds") -> ncen.NcenFilingIndex:
    """Merged ``dera+edgar_xml`` X0505 ``N-CEN/A`` plus an older valid original in the window.

    The older original is a fallback target that must never be selected instead.
    """
    accepted = _REVIEW_F03_ACCEPTED
    older_accepted = dt.datetime(2025, 9, 1, 16, tzinfo=UTC)
    amended = _stage2a_filing(
        acceptance_at=accepted,
        header_retrieved_at=accepted,
        retrieved_at=accepted,
        data_known_at=accepted,
        form="N-CEN/A",
        schema="X0505",
        family_name=family_name,
    )
    older = _stage2a_filing(
        acceptance_at=older_accepted,
        header_retrieved_at=older_accepted,
        retrieved_at=older_accepted,
        data_known_at=older_accepted,
        form="N-CEN",
        period=dt.date(2025, 6, 30),
        accession=_REVIEW_F03_OLDER,
    )
    return ncen.NcenFilingIndex({cik(1): (older, amended)}, {}, {})


def _review_f03_select(
    root: Path, mode: str, **fixture: object
) -> tuple[ncen.DiagnosticSelection, ncen.DiagnosticSourceIndex]:
    sources = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(root, form_type="N-CEN/A", **fixture)  # type: ignore[arg-type]
    )
    return ncen.diagnostic_selection(_review_f03_baseline(), sources, cik(1), R, K, mode=mode), sources


@pytest.mark.parametrize("mode", ["historical_reconstruction", "current_run"])
def test_review_f03_future_schema_cannot_attest_early_dera(tmp_path: Path, mode: str) -> None:
    after = K + dt.timedelta(days=1)
    baseline_pick = ncen.effective_filing(_review_f03_baseline(), cik(1), R, K, knowledge_mode=mode)
    assert baseline_pick.filing is not None and baseline_pick.filing.accession_number == ACCESSION
    assert baseline_pick.reason is None and baseline_pick.amendment_semantics == ncen.AMENDMENT_COMPLETE

    def relationship_ids(sources: ncen.DiagnosticSourceIndex, kind: str | None = None) -> set[str]:
        return {
            item.source_row_id
            for item in sources.rows
            if item.role != "header" and (kind is None or item.source_kind == kind)
        }

    # Positive controls: an eligible, identity-aligned X0505 XML copy establishes the allowance,
    # and DERA plus XML rows are attested exactly as loaded (XML public/data-known exactly at K).
    for label, fixture in {
        "control": {},
        "xml_public_and_data_known_at_k": {"xml_artifact_times": {"public_at": K, "data_known_at": K}},
    }.items():
        control, sources = _review_f03_select(tmp_path / label, mode, **fixture)
        assert control.accession_number == ACCESSION, label
        assert control.selection_reason is None and control.evidence_state == "complete", label
        assert {item.source_row_id for item in control.rows} == relationship_ids(sources), label
        assert {item.source_kind for item in control.rows} == {"dera", "edgar_xml"}, label
        assert all(item.attestation == "attested" for item in control.rows), label
        assert ncen.reported_family_for(control).state == "declared_family", label

    # No eligible XML establishes the schema: schema-less DERA cannot borrow the merged X0505.
    # The frozen pick stays selected (no older fallback) and nothing is attested.
    unattested: dict[str, dict[str, object]] = {
        "xml_public_and_data_known_after_k": {
            "xml_artifact_times": {"public_at": after, "data_known_at": after}
        },
        "xml_data_known_after_k": {"xml_artifact_times": {"data_known_at": after}},
        "xml_public_after_k": {"xml_artifact_times": {"public_at": after}},
        "xml_time_unknown": {"xml_artifact_times": {"public_at": None, "data_known_at": None}},
        "xml_data_known_unknown": {"xml_artifact_times": {"data_known_at": None}},
        "schema_less_dera_only": {"include_xml": False},
    }
    late_xml_possession = {"xml_artifact_times": {"retrieved_at": after}}
    if mode == "current_run":
        unattested["xml_possession_after_k"] = late_xml_possession
    else:
        # Historical data-version availability is attested by public/data-known <= K.
        historical, _ = _review_f03_select(tmp_path / "xml_possession_after_k", mode, **late_xml_possession)
        assert historical.selection_reason is None and historical.evidence_state == "complete"
    for label, fixture in unattested.items():
        selected, sources = _review_f03_select(tmp_path / label, mode, **fixture)
        assert selected.accession_number == ACCESSION, label
        assert selected.evidence_state == "incomplete", label
        assert selected.selection_reason == ncen.AMENDMENT_UNKNOWN_REASON, label
        assert "diagnostic_amendment_schema_unattested" in selected.reasons, label
        assert selected.rows, label
        assert not any(item.attestation == "attested" for item in selected.rows), label
        assert all(item.uncertain_expansion_eligible for item in selected.rows), label
        assert all(ncen.AMENDMENT_UNKNOWN_REASON in item.reasons for item in selected.rows), label
        assert {item.source_kind for item in selected.rows} == {"dera"}, label
        assert relationship_ids(sources, "edgar_xml") <= set(selected.excluded_source_row_ids), label
        assert all(item.accession_number == ACCESSION for item in selected.rows), label
        assert ncen.reported_family_for(selected).state == "unknown", label

    # Eligible schema evidence that conflicts (among copies or with the selected filing) fails
    # closed: blocked, no rows. X0404 is never an allowance (no policy expansion).
    conflicts: dict[str, dict[str, object]] = {
        "eligible_xml_schema_differs_from_selected": {"xml_schema": "X0404"},
        "eligible_xml_copies_disagree": {"extra_xml_schemas": ("X0404",)},
        "future_x0505_with_eligible_x0404": {
            "xml_artifact_times": {"public_at": after, "data_known_at": after},
            "extra_xml_schemas": ("X0404",),
        },
    }
    for label, fixture in conflicts.items():
        selected, sources = _review_f03_select(tmp_path / label, mode, **fixture)
        assert selected.accession_number == ACCESSION, label
        assert selected.selection_reason == "diagnostic_amendment_schema_conflict", label
        assert selected.evidence_state == "incomplete" and not selected.rows, label
        assert relationship_ids(sources) <= set(selected.excluded_source_row_ids), label

    # An earlier blocking reason keeps precedence: an unattested schema never softens a DERA
    # projection mismatch against the selected filing into uncertainty.
    mismatch_sources = ncen.read_diagnostic_source_rows(
        _stage2a_fixture(
            tmp_path / "projection_mismatch",
            form_type="N-CEN/A",
            xml_artifact_times={"public_at": after, "data_known_at": after},
        )
    )
    mismatch = ncen.diagnostic_selection(
        _review_f03_baseline(family_name="Other Funds"), mismatch_sources, cik(1), R, K, mode=mode
    )
    assert mismatch.accession_number == ACCESSION
    assert mismatch.selection_reason == "diagnostic_source_identity_conflict" and not mismatch.rows
    assert "diagnostic_amendment_schema_unattested" in mismatch.reasons

    # Header possession after K in current_run blocks before any copy is considered.
    held_late, _ = _review_f03_select(tmp_path / "all_possession_after_k", mode, xml_retrieved_at=after)
    if mode == "current_run":
        assert held_late.selection_reason == "diagnostic_source_unheld" and not held_late.rows
    else:
        assert held_late.selection_reason is None and held_late.evidence_state == "complete"


# === Consolidated FE-1 review regressions: F6 acquisition membership, not counts ===========
# The declared quarantine ledger and boundary examples must equal, record for record, what
# the loader derives from a pinned acquisition seal. Counts are secondary accounting only.


def _acq_refresh(seal: _AcqSeal) -> _AcqSeal:
    """Rebuild scope digest, schema evidence and coverage from the terminals, then reseal."""
    root = seal.root
    scope = json.loads((root / "scope.json").read_bytes())
    scope["cohort"]["boundary_anomaly_count"] = sum(1 for item in scope["requests"] if item["boundary_anomaly"])
    scope_bytes = _acq_json(scope)
    (root / "scope.json").write_bytes(scope_bytes)
    (root / "SCOPE.sha256").write_bytes(f"{hashlib.sha256(scope_bytes).hexdigest()}  scope.json\n".encode())
    rows = [
        json.loads((root / "terminal" / f"{item['accession_number']}.json").read_bytes())
        for item in scope["requests"]
    ]
    (root / "schema_evidence.jsonl").write_bytes(b"".join(_acq_json(item) for item in rows))
    coverage = json.loads((root / "coverage.json").read_bytes())
    coverage["verified"] = sum(1 for item in rows if item["terminal_status"] == "verified")
    coverage["quarantined"] = sum(1 for item in rows if item["terminal_status"] == "quarantined")
    coverage["boundary_anomalies"] = {"requested": scope["cohort"]["boundary_anomaly_count"]}
    (root / "coverage.json").write_bytes(_acq_json(coverage))
    return dataclasses.replace(seal, pin=_acq_reseal(root))


def _acq_mutate(
    seal: _AcqSeal,
    accession: str,
    *,
    row: dict[str, object] | None = None,
    request: dict[str, object] | None = None,
    header: dict[str, object] | None = None,
) -> _AcqSeal:
    """Rewrite one terminal row and/or scope request consistently, then refresh the seal."""
    root = seal.root
    terminal = root / "terminal" / f"{accession}.json"
    value = json.loads(terminal.read_bytes())
    value.update(row or {})
    value["header"].update(header or {})
    terminal.write_bytes(_acq_json(value))
    scope = json.loads((root / "scope.json").read_bytes())
    for item in scope["requests"]:
        if item["accession_number"] == accession:
            item.update(request or {})
    (root / "scope.json").write_bytes(_acq_json(scope))
    return _acq_refresh(seal)


def _review_f06_case(
    base: Path, label: str, entries: tuple[_AcqEntry, ...] = _ACQ_DEFAULT_ENTRIES
) -> tuple[ncen.DiagnosticSourceManifestPin, _AcqSeal, list[dict[str, object]]]:
    return _stage2a_fixture_with_seal(base / label, acquisition_entries=entries)


def _review_f06_declare(
    pin: ncen.DiagnosticSourceManifestPin,
    artifacts: list[dict[str, object]],
    seal: _AcqSeal,
    label: str,
    *,
    quarantines: list[dict[str, object]] | None = None,
    boundaries: list[dict[str, object]] | None = None,
    **kwargs: object,
) -> ncen.DiagnosticSourceManifestPin:
    """A new manifest over the same verified artifacts and seal with a declared ledger."""
    return _stage2a_write_manifest(
        pin.manifest_path.parent,
        artifacts,
        seal,
        quarantines=[dict(item) for item in seal.quarantines] if quarantines is None else quarantines,
        boundaries=[dict(item) for item in seal.boundaries] if boundaries is None else boundaries,
        name=f"manifest-{label}.json",
        ledger_name=f"quarantine-{label}.json",
        **kwargs,  # type: ignore[arg-type]
    )


def test_review_f06_acquisition_membership_not_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pin, seal, artifacts = _review_f06_case(tmp_path, "control")
    sources = ncen.read_diagnostic_source_rows(pin)
    ledger = sources.acquisition_ledger()
    by_kind = {entry.kind: entry for entry in _ACQ_DEFAULT_ENTRIES}

    # Exact derived identities: accession, CIK, form, period, acceptance, class and reasons.
    assert [
        (
            item.accession_number,
            item.registrant_cik,
            item.form_type,
            item.report_period_end,
            item.acceptance_at,
            item.classification,
            item.reasons,
            item.terminal_status,
        )
        for item in ledger.exclusions
    ] == [
        (
            entry.accession,
            entry.registrant,
            "N-CEN/A",
            entry.period,
            entry.acceptance,
            entry.kind,
            _ACQ_REASONS[entry.kind],
            "quarantined",
        )
        for entry in sorted(
            (by_kind[kind] for kind in ("absent_amended_accession", "schema_unavailable", "projection_conflict")),
            key=lambda value: value.accession,
        )
    ]
    boundary_entry = by_kind["boundary"]
    assert [
        (item.accession_number, item.schema_version, item.acceptance_at, item.reasons, item.schema_observed)
        for item in ledger.boundaries
    ] == [(boundary_entry.accession, "X0505", boundary_entry.acceptance, ("diagnostic_not_admitted_by_example",), True)]
    # Acceptance after the June 2025 boundary never supplies a schema: the schema-less record
    # stays ``schema_unavailable`` with no schema and is not a boundary example.
    unavailable = next(item for item in ledger.exclusions if item.classification == "schema_unavailable")
    assert unavailable.acceptance_at >= dt.datetime(2025, 6, 16, tzinfo=UTC)
    assert unavailable.schema_version is None and unavailable.schema_observed is False
    conflict = next(item for item in ledger.exclusions if item.classification == "projection_conflict")
    assert conflict.projection_equality == "conflict"
    assert conflict.xml_projection_digest != conflict.dera_projection_digest
    assert (ledger.requests, ledger.verified_requests, ledger.lane) == (5, 2, "synthetic_fixture")
    # Every member is bound to exact sealed terminal/header/record/XML entries of its accession.
    for item in (*ledger.exclusions, *ledger.boundaries):
        accession = item.accession_number
        assert item.terminal_ref[0] == f"terminal/{accession}.json"
        assert item.header_raw_ref[0] == f"raw/header/{accession}-index-headers.html"
        assert item.header_record_ref[0] == f"raw/header/{accession}.json"
        assert item.raw_xml_ref[0] == f"raw/xml/{accession}.xml"
        for relative, digest, size in (item.terminal_ref, item.header_raw_ref, item.header_record_ref, item.raw_xml_ref):
            assert _stage2a_sha(seal.root / relative) == digest and (seal.root / relative).stat().st_size == size
    # Read-only custody reads only the allowlisted seal files; a verified non-boundary request is
    # accounted from its terminal alone, and nothing outside the seal (no Stage 1B) is opened.
    allowed_files = {"SHA256SUMS", "SHA256SUMS.receipt.json", "scope.json", "SCOPE.sha256", "coverage.json", "schema_evidence.jsonl"}
    assert all(
        path in allowed_files or path.startswith(("terminal/", "raw/header/", "raw/xml/"))
        for path in ledger.opened_paths
    )
    verified = by_kind["verified"].accession
    assert f"terminal/{verified}.json" in ledger.opened_paths
    assert not [path for path in ledger.opened_paths if verified in path and not path.startswith("terminal/")]
    assert ledger.rederived_requests == 4  # three quarantines and one boundary
    full = ncen.read_diagnostic_acquisition_ledger(seal.pin, verify_full_inventory=True)
    assert full.inventory_entries_hashed == full.inventory_entries == len((seal.root / "SHA256SUMS").read_text().splitlines())
    assert full.rederived_requests == full.requests == 5
    # Full mode re-derives verified terminals too: a hidden quarantine relabelled ``verified``
    # (consistently resealed) is caught from its raw XML, not trusted from its status.
    hidden_pin, hidden_seal, _hidden_artifacts = _review_f06_case(
        tmp_path,
        "hidden",
        (*_ACQ_DEFAULT_ENTRIES, _AcqEntry("0000000777-26-000001", cik(30_001), "schema_unavailable")),
    )
    hidden = _acq_mutate(
        hidden_seal,
        "0000000777-26-000001",
        row={
            "quarantine_reasons": [],
            "terminal_status": "verified",
            "failure_kind": None,
        },
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_terminal_evidence_mismatch"):
        ncen.read_diagnostic_acquisition_ledger(hidden.pin, verify_full_inventory=True)
    assert hidden_pin.manifest_path.is_file()
    assert full.exclusion_ledger_digest == ledger.exclusion_ledger_digest == sources.exclusion_ledger_digest()
    assert ledger.digest_object()["schema_version"] == ncen.DIAGNOSTIC_EXCLUSION_LEDGER_DIGEST_VERSION
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_ledger_digest_mismatch"):
        dataclasses.replace(ledger, exclusions=ledger.exclusions[:-1])
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_ledger_digest_mismatch"):
        dataclasses.replace(
            ledger,
            exclusions=(dataclasses.replace(ledger.exclusions[0], acceptance_raw="20260302070001"), *ledger.exclusions[1:]),
        )

    # The count-preserving substitution the review found: same classes, same totals, different
    # identities. A different seal with the same counts has a different ledger digest.
    swapped_entries = tuple(
        dataclasses.replace(entry, accession=entry.accession.replace("-26-01", "-26-02"))
        if entry.kind == "absent_amended_accession"
        else entry
        for entry in _ACQ_DEFAULT_ENTRIES
    )
    _pin2, twin, _artifacts2 = _review_f06_case(tmp_path, "twin", swapped_entries)
    twin_ledger = ncen.read_diagnostic_acquisition_ledger(twin.pin)
    assert twin_ledger.class_counts() == ledger.class_counts() and len(twin_ledger.boundaries) == len(ledger.boundaries)
    assert twin_ledger.exclusion_ledger_digest != ledger.exclusion_ledger_digest

    # Declared-ledger counterexamples over the unchanged seal: each keeps totals and labels.
    records = sorted((dict(item) for item in seal.quarantines), key=lambda value: str(value["accession_number"]))
    absent = next(index for index, item in enumerate(records) if item["classification"] == "absent_amended_accession")
    unavailable_index = next(index for index, item in enumerate(records) if item["classification"] == "schema_unavailable")

    def changed(index: int, **changes: object) -> list[dict[str, object]]:
        output = [dict(item) for item in records]
        output[index].update(changes)
        return output

    swapped = [dict(item) for item in records]
    swapped[absent], swapped[unavailable_index] = (
        {**swapped[absent], "classification": "schema_unavailable", "reasons": ["schema_element_count:0"]},
        {**swapped[unavailable_index], "classification": "absent_amended_accession", "reasons": ["amended_accession_invalid"]},
    )
    exclusion_cases: dict[str, list[dict[str, object]]] = {
        "substituted_accession": changed(absent, accession_number="0000000999-26-019999"),
        "switched_cik": changed(absent, registrant_cik=cik(10_002)),
        "switched_period": changed(absent, report_period_end="2025-09-30"),
        "shifted_acceptance_second": changed(absent, acceptance_at="2026-03-02T12:00:01.000000Z"),
        "unknown_acceptance": changed(absent, acceptance_at=None),
        "swapped_classification_same_totals": swapped,
        "forced_label_reasons": changed(absent, reasons=["diagnostic_absent_amended_accession"]),
        "dropped_member": records[:-1],
        "duplicated_member": [*records, dict(records[0])],
        "verified_request_claimed": [
            *records,
            {**records[absent], "accession_number": by_kind["verified"].accession, "registrant_cik": by_kind["verified"].registrant},
        ],
    }
    for label, declared in exclusion_cases.items():
        with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_mismatch:exclusions"):
            ncen.read_diagnostic_source_rows(_review_f06_declare(pin, artifacts, seal, label, quarantines=declared))
    boundary = dict(seal.boundaries[0])
    mismatch = "diagnostic_acquisition_membership_mismatch:boundaries"
    boundary_cases: dict[str, tuple[list[dict[str, object]], str]] = {
        "boundary_substituted": ([{**boundary, "accession_number": "0000000888-26-000001"}], mismatch),
        # A non-X0505 boundary claim is already structurally refused by the carrier.
        "boundary_schema_switched": ([{**boundary, "schema_version": "X0404"}], "diagnostic_boundary_schema_invalid"),
        "boundary_time_shifted": ([{**boundary, "acceptance_at": "2026-03-02T12:00:01.000000Z"}], mismatch),
        "boundary_dropped": ([], mismatch),
        "boundary_verified_claimed": (
            [boundary, {**boundary, "accession_number": by_kind["verified"].accession}],
            mismatch,
        ),
        "boundary_quarantine_claimed": (
            [boundary, {**boundary, "accession_number": str(records[absent]["accession_number"])}],
            mismatch,
        ),
    }
    for label, (declared_boundaries, match) in boundary_cases.items():
        with pytest.raises(ncen.NcenError, match=match):
            ncen.read_diagnostic_source_rows(
                _review_f06_declare(pin, artifacts, seal, label, boundaries=declared_boundaries)
            )
    exact = ncen.read_diagnostic_source_rows(_review_f06_declare(pin, artifacts, seal, "exact"))
    assert exact.exclusion_ledger_digest() == ledger.exclusion_ledger_digest

    # Seal custody counterexamples. Each mutation is resealed consistently (so its own anchors
    # match) unless the case is precisely an anchor/hash mismatch.
    quarantine_accessions = [str(item["accession_number"]) for item in records]
    first, second = quarantine_accessions[0], quarantine_accessions[1]

    def load(label: str, mutate, match: str, *, entries: tuple[_AcqEntry, ...] = _ACQ_DEFAULT_ENTRIES) -> None:  # type: ignore[no-untyped-def]
        case_pin, case_seal, case_artifacts = _review_f06_case(tmp_path, label, entries)
        mutated = mutate(case_seal)
        declared = _review_f06_declare(case_pin, case_artifacts, mutated, "mutated")
        with pytest.raises(ncen.NcenError, match=match):
            ncen.read_diagnostic_source_rows(declared)
        with pytest.raises(ncen.NcenError, match=match):
            ncen.read_diagnostic_acquisition_ledger(mutated.pin)

    def delete_terminal(value: _AcqSeal) -> _AcqSeal:
        (value.root / "terminal" / f"{first}.json").unlink()
        return dataclasses.replace(value, pin=_acq_reseal(value.root))

    def guessed_terminal_name(value: _AcqSeal) -> _AcqSeal:
        source = value.root / "terminal" / f"{first}.json"
        source.rename(value.root / "terminal" / f"{first}.terminal.json")
        return dataclasses.replace(value, pin=_acq_reseal(value.root))

    def swap_files(folder: str, suffix: str):  # type: ignore[no-untyped-def]
        def mutate(value: _AcqSeal) -> _AcqSeal:
            left = value.root / "raw" / folder / f"{first}{suffix}"
            right = value.root / "raw" / folder / f"{second}{suffix}"
            left_bytes, right_bytes = left.read_bytes(), right.read_bytes()
            left.write_bytes(right_bytes)
            right.write_bytes(left_bytes)
            return dataclasses.replace(value, pin=_acq_reseal(value.root))

        return mutate

    def swap_xml_rebound(value: _AcqSeal) -> _AcqSeal:
        other = (value.root / "raw" / "xml" / f"{second}.xml").read_bytes()
        (value.root / "raw" / "xml" / f"{first}.xml").write_bytes(other)
        raw_xml = json.loads((value.root / "terminal" / f"{first}.json").read_bytes())["raw_xml"]
        raw_xml.update(sha256=hashlib.sha256(other).hexdigest(), size=len(other))
        return _acq_mutate(value, first, row={"raw_xml": raw_xml})

    def swap_header_rebound(value: _AcqSeal) -> _AcqSeal:
        other = (value.root / "raw" / "header" / f"{second}-index-headers.html").read_bytes()
        (value.root / "raw" / "header" / f"{first}-index-headers.html").write_bytes(other)
        digest = hashlib.sha256(other).hexdigest()
        scope = json.loads((value.root / "scope.json").read_bytes())
        request = next(item for item in scope["requests"] if item["accession_number"] == first)
        return _acq_mutate(
            value,
            first,
            header={"document_sha256": digest},
            request={"header": {**request["header"], "raw_sha256": digest}},
        )

    def alter_record(value: _AcqSeal) -> _AcqSeal:
        path = value.root / "raw" / "header" / f"{first}.json"
        record = json.loads(path.read_bytes())
        record["retrieved_at"] = "2026-09-25T08:00:00.000000Z"
        path.write_bytes(json.dumps(record, sort_keys=True, indent=2).encode())
        return dataclasses.replace(value, pin=_acq_reseal(value.root))

    def forge_reasons(value: _AcqSeal) -> _AcqSeal:
        target = str(records[unavailable_index]["accession_number"])
        return _acq_mutate(value, target, row={"quarantine_reasons": ["amended_accession_invalid"]})

    def flag_quarantine_boundary(value: _AcqSeal) -> _AcqSeal:
        return _acq_mutate(value, first, row={"boundary_anomaly": True}, request={"boundary_anomaly": True})

    def extra_file(value: _AcqSeal) -> _AcqSeal:
        (value.root / "raw" / "xml" / "unlisted.xml").write_bytes(b"<x/>")
        return value

    def duplicate_line(value: _AcqSeal) -> _AcqSeal:
        sums = (value.root / "SHA256SUMS").read_bytes()
        line = next(item for item in sums.splitlines(keepends=True) if b" terminal/" in item)
        forged = sums + line.replace(b"terminal/", b"TERMINAL/")
        (value.root / "SHA256SUMS").write_bytes(forged)
        return dataclasses.replace(
            value, pin=dataclasses.replace(value.pin, sha256sums_sha256=hashlib.sha256(forged).hexdigest())
        )

    def traversal_line(value: _AcqSeal) -> _AcqSeal:
        sums = (value.root / "SHA256SUMS").read_bytes() + b"0" * 64 + b"  raw/../../outside.txt\n"
        (value.root / "SHA256SUMS").write_bytes(sums)
        return dataclasses.replace(
            value, pin=dataclasses.replace(value.pin, sha256sums_sha256=hashlib.sha256(sums).hexdigest())
        )

    def unresealed(value: _AcqSeal) -> _AcqSeal:
        path = value.root / "raw" / "xml" / f"{first}.xml"
        path.write_bytes(path.read_bytes().replace(b"Acme Funds", b"Acme Fundz"))
        return value

    custody_cases = {
        "missing_terminal": (delete_terminal, "diagnostic_acquisition_terminal_set_mismatch"),
        "guessed_terminal_name": (guessed_terminal_name, "diagnostic_acquisition_terminal_locator_invalid"),
        "swapped_raw_xml": (swap_files("xml", ".xml"), "diagnostic_acquisition_raw_xml_unbound"),
        "swapped_raw_header": (swap_files("header", "-index-headers.html"), "diagnostic_acquisition_header_unbound"),
        "swapped_raw_xml_rebound": (swap_xml_rebound, "diagnostic_acquisition_terminal_evidence_mismatch"),
        "swapped_raw_header_rebound": (swap_header_rebound, "diagnostic_acquisition_header_unparseable"),
        "altered_header_record": (alter_record, "diagnostic_acquisition_header_record_unresolved"),
        "forged_terminal_reasons": (forge_reasons, "diagnostic_acquisition_terminal_evidence_mismatch"),
        "boundary_flag_on_quarantine": (flag_quarantine_boundary, "diagnostic_acquisition_boundary_quarantined"),
        "extra_unlisted_file": (extra_file, "diagnostic_acquisition_inventory_not_closed"),
        "case_alias_duplicate": (duplicate_line, "diagnostic_acquisition_inventory_duplicate"),
        "traversal_entry": (traversal_line, "diagnostic_acquisition_inventory_path_unsafe"),
        "bytes_changed_without_reseal": (unresealed, "diagnostic_acquisition_sha256_mismatch"),
    }
    for label, (mutate, match) in custody_cases.items():
        load(label, mutate, match)
    # Unknown or multiple reason sets are a typed STOP, never forced into one bucket; an observed
    # non-X0505 schema on an acceptance-boundary request is never promoted by the date.
    load(
        "multiple_classes",
        lambda value: value,
        "diagnostic_acquisition_classification_unknown",
        entries=(*_ACQ_DEFAULT_ENTRIES, _AcqEntry("0000000999-26-010003", cik(10_003), "multiple")),
    )
    load(
        "boundary_x0404",
        lambda value: value,
        "diagnostic_acquisition_boundary_schema_unobserved",
        entries=(*_ACQ_DEFAULT_ENTRIES, _AcqEntry("0000000888-26-000009", cik(20_009), "boundary", schema="X0404")),
    )
    if sys.platform == "win32":
        import _winapi

        junction_pin, junction_seal, _junction_artifacts = _review_f06_case(tmp_path, "junction")
        outside = tmp_path / "junction-outside"
        outside.mkdir()
        _winapi.CreateJunction(str(outside), str(junction_seal.root / "raw" / "linked"))
        with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_link_forbidden"):
            ncen.read_diagnostic_source_rows(junction_pin)

    # External anchors and root: caller custody never comes from the manifest's own claims.
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_anchor_mismatch"):
        ncen.read_diagnostic_source_rows(
            pin, trusted_acquisition=dataclasses.replace(seal.pin, scope_sha256="1" * 64)
        )
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_anchor_mismatch"):
        ncen.read_diagnostic_source_rows(pin, trusted_acquisition=twin.pin)
    import shutil

    copied = tmp_path / "copied-seal"
    shutil.copytree(seal.root, copied)
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_root_mismatch"):
        ncen.read_diagnostic_source_rows(pin, trusted_acquisition=dataclasses.replace(seal.pin, root=copied))
    assert ncen.read_diagnostic_source_rows(pin, trusted_acquisition=seal.pin).exclusion_ledger_digest() == (
        ledger.exclusion_ledger_digest
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_synthetic_uses_real_anchor"):
        ncen.DiagnosticAcquisitionPin(
            seal.root,
            ncen.DIAGNOSTIC_ACQUISITION_SCOPE_SHA256,
            seal.pin.sha256sums_sha256,
            "synthetic_fixture",
        )
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_sealed_anchor_mismatch"):
        ncen.DiagnosticAcquisitionPin(seal.root, seal.pin.scope_sha256, seal.pin.sha256sums_sha256, "sealed_source")
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_root_unsafe"):
        ncen.DiagnosticAcquisitionPin(Path("relative"), seal.pin.scope_sha256, seal.pin.sha256sums_sha256, "synthetic_fixture")
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_root_unsafe"):
        ncen.read_diagnostic_source_rows(
            _review_f06_declare(pin, artifacts, seal, "escape", acquisition_root="../control/acquisition")
        )
    with pytest.raises(TypeError):
        ncen.read_diagnostic_source_rows(pin, trusted_acquisition=seal.root)  # type: ignore[arg-type]

    # Sealed lane: the real run's anchors are required, the root is never manifest-declared, and
    # exact membership precedes count accounting; success is still HOLD for F7 pin roles.
    sealed_manifest = _review_f06_declare(pin, artifacts, seal, "sealed", manifest_kind="sealed_source", acquisition_root=None)
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_unverified"):
        ncen.read_diagnostic_source_rows(sealed_manifest)
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_lane_mismatch"):
        ncen.read_diagnostic_source_rows(sealed_manifest, trusted_acquisition=seal.pin)
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_root_declared_for_sealed_source"):
        ncen.read_diagnostic_source_rows(
            _review_f06_declare(pin, artifacts, seal, "sealed-rooted", manifest_kind="sealed_source"),
            trusted_acquisition=seal.pin,
        )
    monkeypatch.setattr(ncen, "DIAGNOSTIC_ACQUISITION_SCOPE_SHA256", seal.pin.scope_sha256)
    monkeypatch.setattr(ncen, "DIAGNOSTIC_ACQUISITION_SHA256SUMS_SHA256", seal.pin.sha256sums_sha256)
    sealed_pin = ncen.DiagnosticAcquisitionPin(seal.root, seal.pin.scope_sha256, seal.pin.sha256sums_sha256, "sealed_source")
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_lane_mismatch"):
        ncen.read_diagnostic_source_rows(pin, trusted_acquisition=sealed_pin)
    with pytest.raises(ncen.NcenError, match="diagnostic_acquisition_membership_mismatch:exclusions"):
        ncen.read_diagnostic_source_rows(
            _review_f06_declare(
                pin,
                artifacts,
                seal,
                "sealed-substituted",
                quarantines=exclusion_cases["substituted_accession"],
                manifest_kind="sealed_source",
                acquisition_root=None,
            ),
            trusted_acquisition=sealed_pin,
        )
    # Exact membership holds, but the real run's 131/11/3 and 128 accounting does not.
    with pytest.raises(ncen.NcenError, match="diagnostic_quarantine_coverage_mismatch"):
        ncen.read_diagnostic_source_rows(sealed_manifest, trusted_acquisition=sealed_pin)
    monkeypatch.setattr(
        ncen,
        "_DIAGNOSTIC_ACQUISITION_SEALED_CLASS_COUNTS",
        {"absent_amended_accession": 1, "schema_unavailable": 1, "projection_conflict": 1},
    )
    with pytest.raises(ncen.NcenError, match="diagnostic_boundary_coverage_mismatch"):
        ncen.read_diagnostic_source_rows(sealed_manifest, trusted_acquisition=sealed_pin)
    monkeypatch.setattr(ncen, "_DIAGNOSTIC_ACQUISITION_SEALED_BOUNDARIES", 1)
    with pytest.raises(ncen.NcenError, match="diagnostic_required_pins_unverified"):
        ncen.read_diagnostic_source_rows(sealed_manifest, trusted_acquisition=sealed_pin)


# === Consolidated FE-1 review regressions: F13a source-byte retention and resource monitor ===
# Sub-batch 1: the loader admits artifacts through verified descriptors, holds at most one raw
# artifact (inline header/XML bytes or a private ZIP spool) at a time, and one shared monitor
# refuses typed at preflight hashing, ZIP/XML/header reads and per-artifact parsing. Sub-batch 2
# (F13b, below) replaced whole-table DERA TSV materialization with a bounded SQLite join store.

_MIB = 1024 * 1024
_REVIEW_F13_ACCESSIONS = 8
_REVIEW_F13_PADDING = 60_000  # per padded TSV field; below csv.field_size_limit()
_REVIEW_F13_PADDED_TABLES = ("SUBMISSION", "REGISTRANT", "FUND_REPORTED_INFO")


def _review_f13_xml(accession: str, registrant: str) -> bytes:
    namespace = ncen.NCEN_NAMESPACE
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<edgarSubmission xmlns="{namespace}"><schemaVersion>X0505</schemaVersion><headerData>'
        "<submissionType>N-CEN</submissionType><filerInfo><filer><issuerCredentials>"
        f"<cik>{registrant}</cik></issuerCredentials></filer></filerInfo></headerData><formData>"
        '<generalInfo reportEndingPeriod="2025-12-31"/><registrantInfo>'
        f"<registrantCik>{registrant}</registrantCik>"
        '<registrantFamilyInvComp isRegistrantFamilyInvComp="Y" familyInvCompFullName="Acme Funds"/>'
        "<principalUnderwriters><principalUnderwriter>"
        "<principalUnderwriterName>Shared Underwriter</principalUnderwriterName>"
        "<principalUnderwriterFileNumber>8-00001</principalUnderwriterFileNumber>"
        "<principalUnderwriterCrdNumber>000000021</principalUnderwriterCrdNumber>"
        f"<principalUnderwriterLei>{_REVIEW_LEI_A}</principalUnderwriterLei>"
        "</principalUnderwriter></principalUnderwriters></registrantInfo>"
        "<managementInvestmentQuestionSeriesInfo><managementInvestmentQuestion>"
        "<mgmtInvSeriesId>S000000001</mgmtInvSeriesId><investmentAdvisers><investmentAdviser>"
        "<investmentAdviserName>Shared Adviser</investmentAdviserName>"
        "<investmentAdviserFileNo>801-00001</investmentAdviserFileNo>"
        "<investmentAdviserCrdNo>000000011</investmentAdviserCrdNo>"
        "<investmentAdviserLei></investmentAdviserLei>"
        "</investmentAdviser></investmentAdvisers></managementInvestmentQuestion>"
        "</managementInvestmentQuestionSeriesInfo></formData></edgarSubmission>"
    ).encode()


def _review_f13_fixture(
    tmp_path: Path,
    *,
    packages: int,
) -> tuple[ncen.DiagnosticSourceManifestPin, list[dict[str, object]]]:
    """``packages`` valid DERA ZIPs (<2 MiB uncompressed each) plus one header and XML copy each.

    Padding lives in an extra, manifest-declared TSV column, so the parser structure stays valid
    and the retained output (source rows) is small while each raw package is large.
    """
    root = tmp_path / "stage2a-f13"
    root.mkdir(parents=True)
    accepted = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
    common = {
        "retrieved_at": _stage2a_timestamp(K - dt.timedelta(days=1)),
        "public_at": _stage2a_timestamp(accepted),
        "data_known_at": _stage2a_timestamp(accepted),
    }
    artifacts: list[dict[str, object]] = []
    for package in range(packages):
        rows: dict[str, tuple[tuple[str, ...], list[tuple[str, ...]]]] = {
            "SUBMISSION": (
                ("ACCESSION_NUMBER", "SUBMISSION_TYPE", "CIK", "FILING_DATE", "REPORT_ENDING_PERIOD", "PADDING"),
                [],
            ),
            "REGISTRANT": (
                ("ACCESSION_NUMBER", "CIK", "IS_FAMILY_INVESTMENT_COMPANY", "FAMILY_INVESTMENT_COMPANY_NAME", "PADDING"),
                [],
            ),
            "FUND_REPORTED_INFO": (("FUND_ID", "ACCESSION_NUMBER", "SERIES_ID", "PADDING"), []),
            "ADVISER": (
                ("FUND_ID", "ADVISER_TYPE", "FILE_NUM", "CRD_NUM", "ADVISER_LEI", "ADVISER_NAME"),
                [],
            ),
            "PRINCIPAL_UNDERWRITER": (
                ("ACCESSION_NUMBER", "FILE_NUM", "CRD_NUM", "UNDERWRITER_LEI", "UNDERWRITER_NAME"),
                [],
            ),
        }
        accessions = [f"{1_000 + package:010d}-26-{number:06d}" for number in range(1, _REVIEW_F13_ACCESSIONS + 1)]
        for number, accession in enumerate(accessions, start=1):
            registrant = cik(package * 100 + number)
            fund_id = f"{accession}_{registrant}_S000000001"
            pad = (hashlib.sha256(accession.encode()).hexdigest() * (_REVIEW_F13_PADDING // 64 + 1))[
                :_REVIEW_F13_PADDING
            ]
            rows["SUBMISSION"][1].append((accession, "N-CEN", registrant, "01-MAR-2026", "31-DEC-2025", pad))
            rows["REGISTRANT"][1].append((accession, registrant, "Y", "Acme Funds", pad))
            rows["FUND_REPORTED_INFO"][1].append((fund_id, accession, "S000000001", pad))
            rows["ADVISER"][1].append((fund_id, "Advisor", "801-00001", "000000011", "", "Shared Adviser"))
            rows["PRINCIPAL_UNDERWRITER"][1].append(
                (accession, "8-00001", "000000021", _REVIEW_LEI_A, "Shared Underwriter")
            )
        package_path = root / f"dera-{package:02d}.zip"
        payloads: dict[str, bytes] = {}
        with zipfile.ZipFile(package_path, "w", zipfile.ZIP_STORED) as archive:
            for table in ncen.PINNED_TABLES:
                header_row, table_rows = rows[table]
                payload = ("\n".join(("\t".join(header_row), *("\t".join(item) for item in table_rows))) + "\n").encode()
                member = f"fixture-{package:02d}/{table}.tsv"
                payloads[member] = payload
                archive.writestr(member, payload)
        assert sum(len(item) for item in payloads.values()) <= 2 * _MIB
        first, registrant = accessions[0], cik(package * 100 + 1)
        header = root / f"submission-{package:02d}.txt"
        header.write_text(
            "<SEC-HEADER>\n"
            f"<ACCESSION-NUMBER>{first}\n"
            "<ACCEPTANCE-DATETIME>20260301120000\n"
            "<TYPE>N-CEN\n"
            f"<CIK>{registrant}\n"
            "<FILING-DATE>20260301\n"
            "<PERIOD>20251231\n"
            "</SEC-HEADER>\n",
            encoding="utf-8",
            newline="\n",
        )
        xml = root / f"primary_doc-{package:02d}.xml"
        xml.write_bytes(_review_f13_xml(first, registrant))
        artifacts.append(
            {
                "artifact_id": f"header-{package:02d}",
                "kind": "header",
                "path": header.name,
                "sha256": _stage2a_sha(header),
                "bytes": header.stat().st_size,
                "accession_number": first,
                "registrant_cik": registrant,
                "source_url": f"https://www.sec.gov/Archives/fixture-{package:02d}.txt",
                **common,
            }
        )
        artifacts.append(
            {
                "artifact_id": f"dera-{package:02d}",
                "kind": "dera_zip",
                "path": package_path.name,
                "sha256": _stage2a_sha(package_path),
                "bytes": package_path.stat().st_size,
                "package_label": f"fixture-{package:02d}",
                "members": [
                    {
                        "path": member,
                        "sha256": hashlib.sha256(payload).hexdigest(),
                        "bytes": len(payload),
                        "header": list(rows[Path(member).stem][0]),
                    }
                    for member, payload in sorted(payloads.items())
                ],
                **common,
            }
        )
        artifacts.append(
            {
                "artifact_id": f"xml-{package:02d}",
                "kind": "edgar_xml",
                "path": xml.name,
                "sha256": _stage2a_sha(xml),
                "bytes": xml.stat().st_size,
                "accession_number": first,
                "source_url": f"https://www.sec.gov/Archives/primary_doc-{package:02d}.xml",
                "header_artifact_id": f"header-{package:02d}",
                **common,
            }
        )
    seal = _acquisition_seal(root / "acquisition")
    pin = _stage2a_write_manifest(
        root,
        artifacts,
        seal,
        quarantines=[dict(item) for item in seal.quarantines],
        boundaries=[dict(item) for item in seal.boundaries],
    )
    return pin, artifacts


def _review_f13_monitor(
    spill: Path,
    *,
    probe: object = None,
    **limits: object,
) -> ncen.DiagnosticResourceMonitor:
    spill.mkdir(parents=True, exist_ok=True)
    return ncen.DiagnosticResourceMonitor(
        ncen.DiagnosticResourceLimits(spill_dir=spill, **limits),  # type: ignore[arg-type]
        rss_probe=probe,  # type: ignore[arg-type]
    )


def _review_f13_assert_released(monitor: ncen.DiagnosticResourceMonitor, spill: Path) -> dict[str, object]:
    report = monitor.report()
    assert report["handles_open"] == 0 and report["handles_opened"] == report["handles_closed"]
    assert report["raw_artifacts_resident"] == 0 and report["raw_bytes_in_memory"] == 0
    assert report["spool_files_created"] == report["spool_files_removed"]
    assert report["spool_dirs_created"] == report["spool_dirs_removed"]
    # F13b: every per-package join store is deleted, on success and on every refusal.
    assert report["join_stores_created"] == report["join_stores_removed"]
    assert list(spill.iterdir()) == []
    return report


def test_review_f13a_loader_holds_one_raw_artifact(tmp_path: Path) -> None:
    import tracemalloc

    overhead: dict[int, int] = {}
    package_bytes: dict[int, int] = {}
    loaded_rows: dict[int, int] = {}
    fixtures: dict[int, tuple[ncen.DiagnosticSourceManifestPin, list[dict[str, object]]]] = {}
    for count in (4, 16):
        pin, artifacts = _review_f13_fixture(tmp_path / f"n{count}", packages=count)
        fixtures[count] = (pin, artifacts)
        package_bytes[count] = max(int(item["bytes"]) for item in artifacts if item["kind"] == "dera_zip")
        # Implementation-independent measurement through the default public API: transient
        # allocation above the retained index must not scale with the number of raw packages.
        tracemalloc.start()
        try:
            sources = ncen.read_diagnostic_source_rows(pin)
            current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        overhead[count] = peak - current
        loaded_rows[count] = len(sources.rows)
        del sources
    per_package = max(package_bytes.values())
    assert 1 * _MIB < per_package <= 2 * _MIB
    assert loaded_rows == {4: 4 * 28, 16: 16 * 28}  # 8 accessions x 3 DERA rows + header + 3 XML
    # Retaining the 12 additional raw packages would add >= 12 x per_package at the peak.
    assert overhead[16] - overhead[4] < 2 * per_package, (overhead, per_package)

    for count, (pin, artifacts) in fixtures.items():
        spill = tmp_path / f"spill-{count}"
        monitor = _review_f13_monitor(spill)
        sources = ncen.read_diagnostic_source_rows(pin, monitor=monitor)
        assert len(sources.rows) == loaded_rows[count]
        report = _review_f13_assert_released(monitor, spill)
        expected = [(item["artifact_id"], item["kind"], item["sha256"], item["bytes"]) for item in artifacts]
        log = report["artifact_log"]
        assert [
            (entry["artifact_id"], entry["kind"], entry["sha256"], entry["size"])
            for entry in log
            if entry["stage"] == "preflight"
        ] == expected
        assert sorted(
            (entry["artifact_id"], entry["kind"], entry["sha256"], entry["size"])
            for entry in log
            if entry["stage"] == "load"
        ) == sorted(expected)
        assert report["descriptors_built"] == len(artifacts) == 3 * count
        assert report["artifacts_loaded"] == {"dera_zip": count, "edgar_xml": count, "header": count}
        assert report["raw_artifacts_resident_max"] == 1
        assert report["raw_bytes_in_memory_max"] <= max(
            int(item["bytes"]) for item in artifacts if item["kind"] != "dera_zip"
        )
        assert report["spool_files_created"] == count and report["spool_bytes_max"] == per_package
        assert report["join_stores_created"] == count  # one disposable join store per DERA package
        # One source descriptor plus its private spool, or the spool plus its join store.
        assert report["handles_open_max"] <= 2
        assert report["max_bytes_between_checks"] <= _MIB
        assert 0 < report["max_rows_between_checks"] <= 1024
        assert report["rss_checks"] > 0 and report["rss_max_observed"] > 0
        # Honest disclosure: the hard cap is an in-process checkpoint, never a supervisor limit.
        assert report["supervisor_hard_cap"] is False
        assert report["hard_cap_enforcement"] == "in_process_checkpoints_only"
        assert report["limits"]["rss_soft_bytes"] == 7.5 * 1024**3
        assert report["limits"]["rss_hard_bytes"] == 8 * 1024**3
        del sources
        # No leaked handle: every source artifact can be removed once the loader returns.
        for item in artifacts:
            (pin.manifest_path.parent / str(item["path"])).unlink()


def test_review_f13a_tamper_after_pin_and_between_hash_and_parse(tmp_path: Path) -> None:
    pin, artifacts = _review_f13_fixture(tmp_path / "base", packages=4)
    root = pin.manifest_path.parent
    for target_id in ("dera-02", "xml-01", "header-03"):
        target = root / next(str(item["path"]) for item in artifacts if item["artifact_id"] == target_id)
        original = target.read_bytes()
        mutated = bytearray(original)
        mutated[len(mutated) // 2] ^= 0x01
        # (1) Changed after the external pin: refused at preflight, nothing loaded or parsed.
        target.write_bytes(bytes(mutated))
        spill = tmp_path / f"spill-pin-{target_id}"
        monitor = _review_f13_monitor(spill)
        with pytest.raises(ncen.NcenError, match="diagnostic_artifact_sha256_mismatch"):
            ncen.read_diagnostic_source_rows(pin, monitor=monitor)
        report = _review_f13_assert_released(monitor, spill)
        assert report["artifacts_loaded"] == {}
        assert report["spool_files_created"] == 0
        target.write_bytes(original)

        # (2) Changed after preflight admission but before the load stream: the bytes handed to
        # the parser are the bytes re-hashed at load, so the drift is refused before any parse.
        state = {"done": False}
        holder: dict[str, ncen.DiagnosticResourceMonitor] = {}

        def drifting_probe(
            target: Path = target,
            target_id: str = target_id,
            mutated: bytes = bytes(mutated),
            state: dict[str, bool] = state,
            holder: dict[str, ncen.DiagnosticResourceMonitor] = holder,
        ) -> int:
            active = holder["monitor"]
            if active.phase == "artifact_load" and active.artifact_id == target_id and not state["done"]:
                state["done"] = True
                target.write_bytes(mutated)
            return 64 * _MIB

        spill = tmp_path / f"spill-drift-{target_id}"
        holder["monitor"] = monitor = _review_f13_monitor(spill, probe=drifting_probe)
        with pytest.raises(ncen.NcenError, match="diagnostic_artifact_sha256_mismatch"):
            ncen.read_diagnostic_source_rows(pin, monitor=monitor)
        assert state["done"]
        report = _review_f13_assert_released(monitor, spill)
        assert target_id not in {
            entry["artifact_id"] for entry in report["artifact_log"] if entry["stage"] == "load"
        }
        target.write_bytes(original)
    assert len(ncen.read_diagnostic_source_rows(pin).rows) == 4 * 28


def test_review_f13a_low_cap_monitor_refuses_every_phase(tmp_path: Path) -> None:
    pin, _artifacts = _review_f13_fixture(tmp_path / "base", packages=2)
    phases = (
        "manifest_read",
        "preflight_hash",
        "artifact_load",
        "header_read",
        "header_parse",
        "spool_write",
        "zip_member_hash",
        "zip_rows",
        "dera_parse",
        "dera_rows",
        "xml_read",
        "xml_parse",
        "ledger_read",
        "acquisition_ledger",
        "issue_index",
    )
    seen: set[str] = set()
    baseline = _review_f13_monitor(tmp_path / "spill-baseline", probe=lambda: 64 * _MIB)
    ncen.read_diagnostic_source_rows(pin, monitor=baseline)
    for entry in baseline.report()["phases_checked"]:
        seen.add(str(entry))
    assert seen == set(phases), seen ^ set(phases)
    assert baseline.report()["rss_probe"] == "injected"
    for level, value in (("soft", 96 * _MIB), ("hard", 200 * _MIB)):
        for phase in phases:
            holder: dict[str, ncen.DiagnosticResourceMonitor] = {}

            def probe(
                phase: str = phase,
                value: int = value,
                holder: dict[str, ncen.DiagnosticResourceMonitor] = holder,
            ) -> int:
                return value if holder["monitor"].phase == phase else 64 * _MIB

            spill = tmp_path / f"spill-{level}-{phase}"
            holder["monitor"] = monitor = _review_f13_monitor(
                spill, probe=probe, rss_soft_bytes=80 * _MIB, rss_hard_bytes=128 * _MIB
            )
            with pytest.raises(ncen.NcenError, match=f"^diagnostic_resource_rss_{level}_limit_exceeded:{phase}$"):
                ncen.read_diagnostic_source_rows(pin, monitor=monitor)
            _review_f13_assert_released(monitor, spill)

    # The default platform probe measures real RSS; a tiny cap refuses at the first checkpoint.
    spill = tmp_path / "spill-real"
    monitor = _review_f13_monitor(spill, rss_soft_bytes=_MIB, rss_hard_bytes=2 * _MIB)
    with pytest.raises(ncen.NcenError, match="^diagnostic_resource_rss_hard_limit_exceeded:manifest_read$"):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    assert monitor.report()["rss_max_observed"] > 2 * _MIB

    # Spill disk: budget and free-space checks refuse before the ZIP is copied.
    spill = tmp_path / "spill-budget"
    monitor = _review_f13_monitor(spill, spill_budget_bytes=4096)
    with pytest.raises(ncen.NcenError, match="^diagnostic_spill_budget_exceeded$"):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    assert _review_f13_assert_released(monitor, spill)["spool_files_created"] == 0
    spill = tmp_path / "spill-free"
    spill.mkdir()
    monitor = ncen.DiagnosticResourceMonitor(
        ncen.DiagnosticResourceLimits(spill_dir=spill), disk_free_probe=lambda _path: 0
    )
    with pytest.raises(ncen.NcenError, match="^diagnostic_spill_disk_insufficient$"):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    assert _review_f13_assert_released(monitor, spill)["spool_files_created"] == 0

    # Limits are validated; nothing is relaxed silently.
    for bad in (
        {"rss_soft_bytes": 2 * _MIB, "rss_hard_bytes": _MIB},
        {"hash_block_bytes": 2 * _MIB},
        {"rows_per_check": 1025},
        {"spill_budget_bytes": 0},
    ):
        with pytest.raises(ncen.NcenError, match="^diagnostic_resource_limits_invalid$"):
            ncen.DiagnosticResourceLimits(**bad)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ncen.read_diagnostic_source_rows(pin, monitor=object())  # type: ignore[arg-type]


# === Consolidated FE-1 review regressions: F13b bounded DERA TSV spill and join store ========
# Sub-batch 2: DERA TSV members stream row by row (bounded raw line size, exact 1-based data-row
# locators and raw-line hashes) into one disposable per-package SQLite join store inside the
# private spool. Each accession is projected from indexed lookups through the unchanged frozen
# ``_dera_filings``; the whole-package legacy parser no longer runs in the diagnostic loader. The
# retired whole-table loader is kept below, test-locally, as the row and error parity oracle.

_F13B_HEADERS: dict[str, tuple[str, ...]] = {
    "SUBMISSION": ("ACCESSION_NUMBER", "SUBMISSION_TYPE", "CIK", "FILING_DATE", "REPORT_ENDING_PERIOD"),
    "REGISTRANT": ("ACCESSION_NUMBER", "CIK", "IS_FAMILY_INVESTMENT_COMPANY", "FAMILY_INVESTMENT_COMPANY_NAME"),
    "FUND_REPORTED_INFO": ("FUND_ID", "ACCESSION_NUMBER", "SERIES_ID"),
    "ADVISER": ("FUND_ID", "ADVISER_TYPE", "FILE_NUM", "CRD_NUM", "ADVISER_LEI", "ADVISER_NAME"),
    "PRINCIPAL_UNDERWRITER": ("ACCESSION_NUMBER", "FILE_NUM", "CRD_NUM", "UNDERWRITER_LEI", "UNDERWRITER_NAME"),
}
_F13B_RETRIEVED = K - dt.timedelta(days=1)
_F13B_PUBLIC = dt.datetime(2026, 3, 1, 17, tzinfo=UTC)
_F13B_ADVISER_TYPES = ("Advisor", "subadviser", " Terminated Advisor ", "TERMINATED SUBADVISOR")
_F13B_SCALE_ACCESSIONS = 50
_F13B_SCALE_ROWS = _F13B_SCALE_ACCESSIONS * (1 + 2 * 8 + 1)  # B.5 + 2 funds x 8 advisers + 1 underwriter
_F13B_SCALE_PADDING = 4_000
_F13bTables = dict[str, list[tuple[str, ...]]]


def _f13b_tables(
    package: int,
    *,
    accessions: int = 5,
    funds: int = 2,
    advisers: int = 3,
    underwriters: int = 2,
    padding: int = 0,
) -> _F13bTables:
    """Valid, varied DERA rows: mixed role spellings, blanks, N/A, series-less funds, padded keys."""
    tables: _F13bTables = {table: [] for table in ncen.PINNED_TABLES}
    filler = ("p" * padding,) if padding else ()
    for number in range(1, accessions + 1):
        accession = f"{2_000 + package:010d}-26-{number:06d}"
        registrant = cik(package * 1_000 + number)
        tables["SUBMISSION"].append(
            (accession, "N-CEN" if number % 3 else "N-CEN/A", registrant, "01-MAR-2026", "31-DEC-2025", *filler)
        )
        tables["REGISTRANT"].append((
            f" {accession} " if number == 2 else accession,
            registrant,
            "Y" if number % 2 else " n ",
            " Acme  Funds " if number % 2 else "N/A",
            *filler,
        ))
        for fund in range(1, funds + 1):
            fund_id = f"{accession}_{registrant}_{fund}"
            series = "" if fund == funds and number % 2 == 0 else f"S{number * 100 + fund:09d}"
            tables["FUND_REPORTED_INFO"].append((fund_id, accession, series, *filler))
            for adviser in range(advisers):
                tables["ADVISER"].append((
                    fund_id,
                    _F13B_ADVISER_TYPES[adviser % len(_F13B_ADVISER_TYPES)],
                    f"801-{adviser % 3 + 1:05d}",
                    "N/A" if adviser % 5 == 4 else f"{11 + adviser % 2:09d}",
                    "" if adviser % 2 else _REVIEW_LEI_A,
                    f" Adviser {adviser % 3} ",
                ))
        for underwriter in range(underwriters):
            tables["PRINCIPAL_UNDERWRITER"].append((
                accession,
                f"8-{underwriter + 1:05d}",
                "" if underwriter else "000000021",
                _REVIEW_LEI_B if underwriter else "",
                f"Underwriter {underwriter}",
            ))
    return tables


def _f13b_package(
    path: Path,
    tables: _F13bTables,
    *,
    padding: bool = False,
    raw: dict[str, bytes] | None = None,
    headers: dict[str, tuple[str, ...]] | None = None,
    newline: str = "\n",
) -> dict[str, object]:
    """Write one DERA ZIP at ``path``; returns its manifest artifact with exact member pins."""
    effective = {
        table: (*_F13B_HEADERS[table], *(("PADDING",) if padding and table in _REVIEW_F13_PADDED_TABLES else ()))
        for table in ncen.PINNED_TABLES
    }
    effective.update(headers or {})
    payloads: dict[str, bytes] = {}
    # Padding and long raw lines compress extremely well; store them so the ZIP ratio guard,
    # not the fixture, decides what is refused.
    compression = zipfile.ZIP_STORED if padding or raw else zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(path, "w", compression) as archive:
        for table in ncen.PINNED_TABLES:
            member = f"{path.stem}/{table}.tsv"
            if raw is not None and table in raw:
                payload = raw[table]
            else:
                lines = ("\t".join(effective[table]), *("\t".join(item) for item in tables[table]))
                payload = "".join(f"{line}{newline}" for line in lines).encode()
            payloads[member] = payload
            archive.writestr(member, payload)
    return {
        "artifact_id": f"dera-{path.stem}",
        "kind": "dera_zip",
        "path": path.name,
        "sha256": _stage2a_sha(path),
        "bytes": path.stat().st_size,
        "package_label": f"f13b-{path.stem}",
        "members": [
            {
                "path": member,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "bytes": len(payload),
                "header": list(effective[Path(member).stem]),
            }
            for member, payload in sorted(payloads.items())
        ],
        "retrieved_at": _stage2a_timestamp(_F13B_RETRIEVED),
        "public_at": _stage2a_timestamp(_F13B_PUBLIC),
        "data_known_at": _stage2a_timestamp(_F13B_PUBLIC),
    }


def _f13b_legacy(path: Path, artifact: dict[str, object]) -> ncen.NcenPackageResult:
    return ncen.parse_dera_ncen_package(
        path,
        expected_sha256=str(artifact["sha256"]),
        package_label=str(artifact["package_label"]),
        retrieved_at=_F13B_RETRIEVED,
        first_verified_public_at=_F13B_PUBLIC,
    )


def _f13b_streamed(tmp_path: Path, path: Path, artifact: dict[str, object]) -> tuple[ncen.NcenFiling, ...]:
    """Per-accession filings from the bounded join store (no legacy parser involved)."""
    spill = tmp_path / f"stream-{path.stem}"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB)
    store = ncen._DiagnosticJoinStore(spill / "join.sqlite3", monitor, capacity_bytes=256 * _MIB)
    try:
        ncen._diagnostic_dera_ingest(artifact, path, store, monitor=monitor)
        filings = tuple(
            ncen._diagnostic_dera_projections(
                store,
                package_label=str(artifact["package_label"]),
                zip_sha256=str(artifact["sha256"]),
                retrieved_at=_F13B_RETRIEVED,
                first_verified_public_at=_F13B_PUBLIC,
                monitor=monitor,
            )
        )
    finally:
        store.close()
    _review_f13_assert_released(monitor, spill)
    return filings


def _f13b_rows(
    tmp_path: Path,
    path: Path,
    artifact: dict[str, object],
    headers: dict[str, ncen.DiagnosticSourceRow] | None = None,
    **limits: object,
) -> tuple[ncen.DiagnosticSourceRow, ...]:
    """Rows of one package through the F13b loader, asserting its scratch is always released."""
    spill = tmp_path / f"rows-{path.stem}"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB, **limits)
    try:
        return ncen._diagnostic_dera_rows(
            artifact, path.name, path, headers or {}, monitor=monitor, store_path=spill / "join.sqlite3"
        )
    finally:
        _review_f13_assert_released(monitor, spill)


def _f13b_reference_rows(
    artifact: dict[str, object],
    path_label: str,
    path: Path,
    header_by_accession: dict[str, ncen.DiagnosticSourceRow],
) -> tuple[ncen.DiagnosticSourceRow, ...]:
    """The retired F13a whole-table DERA loader (same semantics), kept as the F13b oracle."""
    members_spec = artifact["members"]
    assert isinstance(members_spec, list)
    specs_by_path = {str(item["path"]): item for item in members_spec}
    tables: dict[str, tuple[tuple[dict[str, str], str, str], ...]] = {}
    member_by_table: dict[str, str] = {}
    with zipfile.ZipFile(path) as archive:
        members = ncen.inspect_zip(archive, ncen.NCEN_ZIP_LIMITS)
        for table in ncen.PINNED_TABLES:
            info = ncen._member_for(members, table)
            assert info is not None and info.filename in specs_by_path
            member_by_table[table] = info.filename
            with archive.open(info) as handle:
                header_line = handle.readline().decode("utf-8").rstrip("\n").rstrip("\r")
                header = tuple(header_line.split("\t"))
                assert header == tuple(specs_by_path[info.filename]["header"])
                rows: list[tuple[dict[str, str], str, str]] = []
                for number, raw_line in enumerate(handle, start=1):
                    decoded = raw_line.decode("utf-8").rstrip("\n").rstrip("\r")
                    parsed = next(csv.reader([decoded], delimiter="\t", quoting=csv.QUOTE_NONE))
                    if len(parsed) != len(header):
                        raise ncen.NcenError(f"diagnostic_member_row_width_mismatch:{table}:{number}")
                    rows.append(
                        (dict(zip(header, parsed, strict=True)), hashlib.sha256(raw_line).hexdigest(), str(number))
                    )
            tables[table] = tuple(rows)
    retrieved = ncen._diagnostic_parse_timestamp(artifact["retrieved_at"], "diagnostic_retrieved_at")
    data_known = ncen._diagnostic_parse_timestamp(artifact["data_known_at"], "diagnostic_data_known_at")
    assert retrieved is not None and data_known is not None
    package = ncen.parse_dera_ncen_package(
        path,
        expected_sha256=str(artifact["sha256"]),
        package_label=str(artifact["package_label"]),
        retrieved_at=retrieved,
        first_verified_public_at=data_known,
    )
    if package.status != "parsed":
        raise ncen.NcenError(f"diagnostic_dera_package_quarantined:{','.join(package.reasons)}")
    filings = {item.accession_number: item for item in package.filings}
    quarantined = [item for item in package.filings if not item.usable]
    if quarantined:
        detail = ";".join(f"{item.accession_number}:{','.join(item.reasons)}" for item in quarantined)
        raise ncen.NcenError(f"diagnostic_dera_copy_quarantined:{detail}")
    submissions: dict[str, tuple[dict[str, str], str, str]] = {}
    for raw, digest, number in tables["SUBMISSION"]:
        accession = ncen._clean(raw["ACCESSION_NUMBER"]) or ""
        if accession in submissions or ncen._ACCESSION.fullmatch(accession) is None:
            raise ncen.NcenError("diagnostic_submission_duplicate_or_invalid")
        submissions[accession] = (raw, digest, number)
    registrants: dict[str, tuple[dict[str, str], str, str]] = {}
    for raw, digest, number in tables["REGISTRANT"]:
        accession = ncen._clean(raw["ACCESSION_NUMBER"]) or ""
        if accession in registrants:
            raise ncen.NcenError("diagnostic_registrant_duplicate")
        registrants[accession] = (raw, digest, number)
    funds: dict[str, tuple[dict[str, str], str, str]] = {}
    for raw, digest, number in tables["FUND_REPORTED_INFO"]:
        fund_id = ncen._clean(raw["FUND_ID"]) or ""
        if not fund_id or fund_id in funds:
            raise ncen.NcenError("diagnostic_fund_duplicate_or_invalid")
        funds[fund_id] = (raw, digest, number)
    output: list[ncen.DiagnosticSourceRow] = []

    def common(accession: str, member: str) -> tuple[str, dict[str, object]]:
        filing = filings.get(accession)
        if filing is None or submissions.get(accession) is None or filing.registrant_cik is None:
            raise ncen.NcenError("diagnostic_dera_source_identity_unavailable")
        header = header_by_accession.get(accession)
        copy_id = ncen._diagnostic_source_copy_id("dera", str(artifact["artifact_id"]), str(artifact["sha256"]), accession)
        fields = ncen._diagnostic_common_row_fields(
            artifact,
            artifact_path=path_label,
            copy_id=copy_id,
            projection_digest=filing.projection_digest,
            filing=filing,
            header_source_id=None if header is None else header.source_row_id,
        )
        fields["source_kind"] = "dera"
        fields["member_path"] = member
        if header is not None:
            fields["acceptance_at"] = header.acceptance_at
        return filing.registrant_cik, fields

    for accession, (raw, digest, number) in sorted(registrants.items()):
        registrant, fields = common(accession, member_by_table["REGISTRANT"])
        submission = submissions[accession]
        if ncen.normalize_cik(raw["CIK"]) != registrant:
            raise ncen.NcenError("diagnostic_registrant_cik_mismatch")
        output.append(ncen.DiagnosticSourceRow(
            accession,
            registrant,
            "b5",
            ncen._diagnostic_row_locator(member_by_table["REGISTRANT"], int(number), "IS_FAMILY_INVESTMENT_COMPANY"),
            None,
            "registrant",
            answer_raw=raw["IS_FAMILY_INVESTMENT_COMPANY"],
            name_raw=raw["FAMILY_INVESTMENT_COMPANY_NAME"],
            raw_row_sha256=digest,
            supporting_rows=((
                ncen._diagnostic_row_locator(member_by_table["SUBMISSION"], int(submission[2]), "ACCESSION_NUMBER"),
                submission[1],
            ),),
            **fields,  # type: ignore[arg-type]
        ))
    role_map = {
        "adviser": "current_primary",
        "sub_adviser": "current_sub",
        "terminated_adviser": "terminated_primary",
        "terminated_sub_adviser": "terminated_sub",
    }
    for raw, digest, number in tables["ADVISER"]:
        fund = funds.get(ncen._clean(raw["FUND_ID"]) or "")
        if fund is None:
            raise ncen.NcenError("diagnostic_adviser_fund_join_missing")
        accession = ncen._clean(fund[0]["ACCESSION_NUMBER"]) or ""
        registrant, fields = common(accession, member_by_table["ADVISER"])
        source_role = ncen.ADVISER_ROLES.get((ncen._clean(raw["ADVISER_TYPE"]) or "").upper())
        if source_role is None:
            raise ncen.NcenError("diagnostic_adviser_role_unknown")
        series = ncen._clean(fund[0]["SERIES_ID"])
        series_id = series if series is not None and ncen.is_series_key(series) else None
        output.append(ncen.DiagnosticSourceRow(
            accession,
            registrant,
            role_map[source_role],
            ncen._diagnostic_row_locator(member_by_table["ADVISER"], int(number), "ADVISER_NAME"),
            series_id,
            "series" if series_id is not None else "unresolved",
            name_raw=raw["ADVISER_NAME"],
            file_number_raw=raw["FILE_NUM"],
            crd_raw=raw["CRD_NUM"],
            lei_raw=raw["ADVISER_LEI"],
            raw_row_sha256=digest,
            supporting_rows=((
                ncen._diagnostic_row_locator(member_by_table["FUND_REPORTED_INFO"], int(fund[2]), "FUND_ID"),
                fund[1],
            ),),
            **fields,  # type: ignore[arg-type]
        ))
    for raw, digest, number in tables["PRINCIPAL_UNDERWRITER"]:
        accession = ncen._clean(raw["ACCESSION_NUMBER"]) or ""
        registrant, fields = common(accession, member_by_table["PRINCIPAL_UNDERWRITER"])
        output.append(ncen.DiagnosticSourceRow(
            accession,
            registrant,
            "underwriter",
            ncen._diagnostic_row_locator(member_by_table["PRINCIPAL_UNDERWRITER"], int(number), "UNDERWRITER_NAME"),
            None,
            "registrant",
            name_raw=raw["UNDERWRITER_NAME"],
            file_number_raw=raw["FILE_NUM"],
            crd_raw=raw["CRD_NUM"],
            lei_raw=raw["UNDERWRITER_LEI"],
            raw_row_sha256=digest,
            **fields,  # type: ignore[arg-type]
        ))
    return tuple(output)


def _f13b_copy(tables: _F13bTables) -> _F13bTables:
    return {table: list(rows) for table, rows in tables.items()}


def _f13b_anomaly_tables() -> _F13bTables:
    """Accession-level defects the frozen parser quarantines, attributed exactly as it does."""
    tables = _f13b_tables(7, accessions=8, funds=2, advisers=2, underwriters=1)
    acc = {number: item[0] for number, item in enumerate(tables["SUBMISSION"], start=1)}
    sub, reg, fund, adv, uw = (tables[table] for table in ncen.PINNED_TABLES)

    def swap(rows: list[tuple[str, ...]], key: str, update: object, *, field: int = 0) -> None:
        for position, item in enumerate(rows):
            if item[field].strip() == key:
                rows[position] = update(item)  # type: ignore[operator]
                return
        raise AssertionError(f"fixture row missing: {key}")

    # A1: duplicate submission; the later copy (another CIK) wins as in the frozen parser.
    sub.append((acc[1], "N-CEN", cik(9_001), "02-MAR-2026", "31-DEC-2025"))
    # A2: duplicate registrant row.
    reg.append((acc[2], cik(7_002), "Y", "Other Funds"))
    # A3: registrant CIK mismatch plus an orphan adviser whose FUND_ID prefix is A3.
    swap(reg, acc[3], lambda item: (item[0], cik(9_003), *item[2:]))
    adv.append((f"{acc[3]}_missing_9", "Advisor", "801-00001", "000000011", "", "Orphan Adviser"))
    # A4: registrant missing; unparseable report period.
    reg[:] = [item for item in reg if item[0].strip() != acc[4]]
    swap(sub, acc[4], lambda item: (*item[:4], "31-FOO-2025"))
    # A5: invalid CIK, blank filing date and an unparseable B.5 answer.
    swap(sub, acc[5], lambda item: (item[0], item[1], "not-a-cik", "", item[4]))
    swap(reg, acc[5], lambda item: (item[0], item[1], "maybe", item[3]))
    # A6: in-accession FUND_ID duplicate, an invalid series, and first owner of FUND_ID "EARLY",
    # whose adviser has an unknown type (charged to A6, the owner, not to A7 below).
    six = next(item for item in fund if item[1] == acc[6])
    fund.append(six)
    swap(fund, f"{acc[6]}_{cik(7_006)}_2", lambda item: (item[0], item[1], "S12"))
    fund.insert(0, ("EARLY", acc[6], "S000000777"))
    adv.append(("EARLY", "Mystery Adviser", "801-00009", "", "", "Unknown Type"))
    # A7: a later row claiming "EARLY" is a cross-accession duplicate charged to A7 only.
    fund.append(("EARLY", acc[7], "S000000778"))
    # A8 keeps its fund: A5's later copy of that FUND_ID is charged to A5.
    eight = next(item for item in fund if item[1] == acc[8])
    fund.append((eight[0], acc[5], eight[2]))
    # Orphans the frozen parser only counts: registrant/fund/underwriter without a submission,
    # a blank FUND_ID, an adviser whose FUND_ID prefix is no accession, an invalid submission id.
    orphan = "0000009999-26-000001"
    reg.append((orphan, cik(9_999), "N", ""))
    fund.append(("", acc[8], "S000000001"))
    fund.append((f"{orphan}_x", orphan, "S000000002"))
    uw.append((orphan, "8-00009", "", "", "Orphan Underwriter"))
    adv.append(("NOPE_1", "Advisor", "801-00001", "", "", "Orphan"))
    sub.append(("bad-accession", "N-CEN", cik(9_998), "01-MAR-2026", "31-DEC-2025"))
    return tables


def test_review_f13b_streamed_projection_equals_frozen_parser(tmp_path: Path) -> None:
    valid = _f13b_tables(3, accessions=6, funds=3, advisers=5, underwriters=2)
    shuffled = _f13b_copy(valid)
    for position, table in enumerate(ncen.PINNED_TABLES):
        random.Random(position).shuffle(shuffled[table])
    cases = {"valid": valid, "shuffled": shuffled, "anomalies": _f13b_anomaly_tables()}
    digests: dict[str, dict[str, str]] = {}
    reasons: dict[str, tuple[str, ...]] = {}
    for name, tables in cases.items():
        path = tmp_path / f"{name}.zip"
        artifact = _f13b_package(path, tables)
        legacy = _f13b_legacy(path, artifact)
        assert legacy.status == "parsed"
        streamed = _f13b_streamed(tmp_path, path, artifact)
        # Full typed equality per accession: B.5, funds, advisers, underwriters, dates, times,
        # status and quarantine reasons, in the frozen parser's accession order.
        assert streamed == legacy.filings, name
        digests[name] = {item.accession_number: item.projection_digest for item in streamed}
        if name == "anomalies":
            reasons = {item.accession_number: item.reasons for item in streamed}
    # Input order never changes a projection.
    assert digests["shuffled"] == digests["valid"]
    # The anomaly fixture exercises every cross-accession attribution rule.
    acc = [f"{2_007:010d}-26-{number:06d}" for number in range(1, 9)]
    assert reasons[acc[7]] == ()
    assert "submission_duplicate" in reasons[acc[0]] and "registrant_duplicate" in reasons[acc[1]]
    assert {"registrant_cik_mismatch", "adviser_orphan"} <= set(reasons[acc[2]])
    assert {"registrant_missing", "report_period_unparseable"} <= set(reasons[acc[3]])
    assert {"cik_invalid", "filing_date_unparseable", "family_answer_unparseable", "fund_id_duplicate"} <= set(
        reasons[acc[4]]
    )
    assert {"fund_id_duplicate", "series_id_invalid", "adviser_type_unknown"} <= set(reasons[acc[5]])
    assert reasons[acc[6]] == ("fund_id_duplicate",)


def test_review_f13b_concentrated_accession_projection_stays_disclosed(tmp_path: Path) -> None:
    tables = _f13b_tables(8, accessions=1, funds=20, advisers=25, underwriters=10)
    path = tmp_path / "concentrated.zip"
    artifact = _f13b_package(path, tables)
    expected = _f13b_legacy(path, artifact)
    assert expected.status == "parsed"
    spill = tmp_path / "concentrated-spill"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB)
    store = ncen._DiagnosticJoinStore(spill / "join.sqlite3", monitor, capacity_bytes=256 * _MIB)
    try:
        ncen._diagnostic_dera_ingest(artifact, path, store, monitor=monitor)
        streamed = tuple(
            ncen._diagnostic_dera_projections(
                store,
                package_label=str(artifact["package_label"]),
                zip_sha256=str(artifact["sha256"]),
                retrieved_at=_F13B_RETRIEVED,
                first_verified_public_at=_F13B_PUBLIC,
                monitor=monitor,
            )
        )
        for filing in streamed:
            ncen._diagnostic_join_record(store, filing, monitor=monitor)
    finally:
        store.close()
    report = _review_f13_assert_released(monitor, spill)
    assert streamed == expected.filings
    assert report["tsv_rows_resident_max"] == 1 + 1 + 20 + 20 * 25 + 10
    assert "frozen_dera_per_accession_projection_entry_exit_only" in report["unmonitored_scopes"]
    assert report["join_stores_created"] == report["join_stores_removed"] == 1


def test_review_f13b_rows_equal_retired_whole_table_loader(tmp_path: Path) -> None:
    base = _f13b_tables(4, accessions=5, funds=2, advisers=4, underwriters=2)
    shuffled = _f13b_copy(base)
    for position, table in enumerate(ncen.PINNED_TABLES):
        random.Random(100 + position).shuffle(shuffled[table])
    duplicated = _f13b_copy(base)
    duplicated["ADVISER"].append(duplicated["ADVISER"][0])
    duplicated["ADVISER"].insert(3, duplicated["ADVISER"][3])
    duplicated["PRINCIPAL_UNDERWRITER"].append(duplicated["PRINCIPAL_UNDERWRITER"][1])
    unicode_values = _f13b_copy(base)
    unicode_values["ADVISER"][1] = (*unicode_values["ADVISER"][1][:5], "Conseil \u00c9pargne\x00 \u2713 \u2028")
    first = base["SUBMISSION"][0]
    header = ncen.DiagnosticSourceRow(
        first[0],
        first[2],
        "header",
        "/SEC-HEADER[1]",
        None,
        "registrant",
        source_kind="header",
        artifact_id="header-f13b",
        artifact_path="submission.txt",
        artifact_sha256="1" * 64,
        artifact_size=1,
        raw_row_sha256="2" * 64,
        source_copy_id="ncencopy:" + "3" * 64,
        acceptance_at=dt.datetime(2026, 3, 1, 17, tzinfo=UTC),
        name_state="unavailable",
    )
    cases: dict[str, tuple[_F13bTables, str, dict[str, ncen.DiagnosticSourceRow]]] = {
        "base": (base, "\n", {}),
        "shuffled": (shuffled, "\n", {}),
        "duplicated": (duplicated, "\n", {}),
        "unicode": (unicode_values, "\n", {}),
        "crlf": (base, "\r\n", {}),
        "header": (base, "\n", {first[0]: header}),
    }
    outputs: dict[str, tuple[ncen.DiagnosticSourceRow, ...]] = {}
    for name, (tables, newline, headers) in cases.items():
        path = tmp_path / f"{name}.zip"
        artifact = _f13b_package(path, tables, newline=newline)
        expected = _f13b_reference_rows(artifact, path.name, path, headers)
        actual = _f13b_rows(tmp_path, path, artifact, headers)
        assert actual == expected, name
        outputs[name] = actual
    # Exact duplicate lines keep their multiplicity: same raw bytes, distinct 1-based locators.
    by_digest: dict[str, list[ncen.DiagnosticSourceRow]] = defaultdict(list)
    for item in outputs["duplicated"]:
        if item.role != "b5":
            by_digest[str(item.raw_row_sha256)].append(item)
    repeated = [items for items in by_digest.values() if len(items) > 1]
    assert len(repeated) == 3
    for items in repeated:
        assert len({item.locator for item in items}) == len({item.source_row_id for item in items}) == 2
    assert len(outputs["duplicated"]) == len(outputs["base"]) + 3
    # CRLF changes only the raw-line hashes; header binding sets acceptance on its accession.
    def rows_in(name: str) -> list[str]:
        return [item.locator.split("/", 1)[1] for item in outputs[name]]

    assert rows_in("crlf") == rows_in("base")
    assert {item.raw_row_sha256 for item in outputs["crlf"]}.isdisjoint(
        {item.raw_row_sha256 for item in outputs["base"]}
    )
    assert {item.acceptance_at for item in outputs["header"] if item.accession_number == first[0]} == {
        header.acceptance_at
    }

    # Refusals and their precedence are exactly those of the retired loader.
    orphan = "0000009999-26-000001"
    fund_id = base["FUND_REPORTED_INFO"][0][0]
    mutations: dict[str, list[tuple[str, tuple[str, ...]]]] = {
        "copy_quarantined": [("SUBMISSION", base["SUBMISSION"][0])],
        "adviser_type_unknown": [("ADVISER", (fund_id, "Consultant", "801-00001", "", "", "X"))],
        "invalid_submission": [("SUBMISSION", ("x", "N-CEN", cik(1), "01-MAR-2026", "31-DEC-2025"))],
        "orphan_registrant_duplicate": [
            ("REGISTRANT", (orphan, cik(9), "Y", "")),
            ("REGISTRANT", (f" {orphan}", cik(9), "N", "")),
        ],
        "orphan_registrant": [("REGISTRANT", (orphan, cik(9), "Y", ""))],
        "blank_fund": [("FUND_REPORTED_INFO", (" ", base["SUBMISSION"][1][0], "S000000001"))],
        "orphan_fund_duplicate": [
            ("FUND_REPORTED_INFO", ("ORPH_1", orphan, "")),
            ("FUND_REPORTED_INFO", ("ORPH_1", orphan, "")),
        ],
        "adviser_join_missing": [("ADVISER", ("ZZZ_1", "Advisor", "801-00001", "", "", "X"))],
        "orphan_fund_adviser": [
            ("FUND_REPORTED_INFO", ("ORPH_2", orphan, "")),
            ("ADVISER", ("ORPH_2", "Advisor", "801-00001", "", "", "X")),
        ],
        "orphan_underwriter": [("PRINCIPAL_UNDERWRITER", (orphan, "8-00001", "", "", "X"))],
        "invalid_submission_precedes_orphan_registrant": [
            ("REGISTRANT", (orphan, cik(9), "Y", "")),
            ("SUBMISSION", ("y", "N-CEN", cik(1), "01-MAR-2026", "31-DEC-2025")),
        ],
    }
    for name, additions in mutations.items():
        tables = _f13b_copy(base)
        for table, item in additions:
            tables[table].append(item)
        path = tmp_path / f"error-{name}.zip"
        artifact = _f13b_package(path, tables)
        with pytest.raises(ncen.NcenError) as expected_error:
            _f13b_reference_rows(artifact, path.name, path, {})
        with pytest.raises(ncen.NcenError) as actual_error:
            _f13b_rows(tmp_path, path, artifact)
        assert str(actual_error.value) == str(expected_error.value), name


def test_review_f13b_bounded_lines_and_row_grammar_fail_closed(tmp_path: Path) -> None:
    base = _f13b_tables(5, accessions=2, funds=1, advisers=1, underwriters=1)
    fund_id = base["ADVISER"][0][0]
    header = "\t".join(_F13B_HEADERS["ADVISER"]).encode() + b"\n"
    prefix = f"{fund_id}\tAdvisor\t801-00001\t000000011\t\t".encode()

    def adviser_line(total: int) -> bytes:
        return prefix + b"n" * (total - len(prefix) - 1) + b"\n"

    good = "\t".join(base["ADVISER"][0]).encode() + b"\n"
    refused: dict[str, tuple[bytes, str, dict[str, int]]] = {
        # Default 1 MiB raw line bound, terminator included: one byte over refuses.
        "oversized_default": (adviser_line(_MIB + 1), "^diagnostic_member_row_oversized:ADVISER:1$", {}),
        "oversized_lowered": (
            good + adviser_line(257),
            "^diagnostic_member_row_oversized:ADVISER:2$",
            {"tsv_line_max_bytes": 256},
        ),
        "invalid_utf8": (good[:-3] + b"\xff\n", "^diagnostic_member_row_unparseable:ADVISER:1$", {}),
        "stray_cr": (good[:-3] + b"\r" + good[-3:], "^diagnostic_member_row_unparseable:ADVISER:1$", {}),
        "double_cr": (good[:-1] + b"\r\r\n", "^diagnostic_member_row_unparseable:ADVISER:1$", {}),
        "csv_field_limit": (adviser_line(200_000), "^diagnostic_member_row_unparseable:ADVISER:1$", {}),
        "width": (good[:-1] + b"\textra\n", "^diagnostic_member_row_width_mismatch:ADVISER:1$", {}),
    }
    for name, (lines, pattern, limits) in refused.items():
        path = tmp_path / f"{name}.zip"
        artifact = _f13b_package(path, base, raw={"ADVISER": header + lines})
        with pytest.raises(ncen.NcenError, match=pattern):
            _f13b_rows(tmp_path, path, artifact, **limits)
        if not limits:
            # The retired loader refused these too (csv/Unicode errors or a quarantined package).
            with pytest.raises((ncen.NcenError, csv.Error, UnicodeDecodeError)):
                _f13b_reference_rows(artifact, path.name, path, {})

    # A line of exactly the bound is accepted and equals the retired loader's rows.
    path = tmp_path / "at_bound.zip"
    artifact = _f13b_package(path, base, raw={"ADVISER": header + adviser_line(256)})
    accepted = _f13b_rows(tmp_path, path, artifact, tsv_line_max_bytes=256)
    assert accepted == _f13b_reference_rows(artifact, path.name, path, {})
    assert any(len(str(item.name_raw)) == 256 - len(prefix) - 1 for item in accepted)

    # A header carrying a stray CR is refused even when the manifest pins that exact header.
    extra = (*_F13B_HEADERS["ADVISER"], "EX\rTRA")
    path = tmp_path / "header_cr.zip"
    artifact = _f13b_package(
        path,
        base,
        raw={"ADVISER": ("\t".join(extra) + "\n" + "\t".join((*base["ADVISER"][0], "x")) + "\n").encode()},
        headers={"ADVISER": extra},
    )
    with pytest.raises(ncen.NcenError, match="^diagnostic_member_header_unparseable:ADVISER$"):
        _f13b_rows(tmp_path, path, artifact)
    with pytest.raises(ncen.NcenError, match="^diagnostic_dera_package_quarantined:"):
        _f13b_reference_rows(artifact, path.name, path, {})

    # The frozen parser consumes the TSV header through csv.reader as well as splitting it; its
    # standard-library field-size limit applies even when the complete pinned header is < 1 MiB.
    long_header = (*_F13B_HEADERS["ADVISER"], "H" * (csv.field_size_limit() + 1))
    path = tmp_path / "header_csv_field_limit.zip"
    artifact = _f13b_package(
        path,
        base,
        raw={
            "ADVISER": (
                "\t".join(long_header) + "\n" + "\t".join((*base["ADVISER"][0], "extra")) + "\n"
            ).encode()
        },
        headers={"ADVISER": long_header},
    )
    with pytest.raises(ncen.NcenError, match="^diagnostic_member_header_unparseable:ADVISER$"):
        _f13b_rows(tmp_path, path, artifact)
    with pytest.raises(ncen.NcenError, match="^diagnostic_dera_package_quarantined:.*tsv_unparseable"):
        _f13b_reference_rows(artifact, path.name, path, {})

    for bad in ({"tsv_line_max_bytes": _MIB + 1}, {"tsv_line_max_bytes": 0}, {"join_cache_bytes": 0}):
        with pytest.raises(ncen.NcenError, match="^diagnostic_resource_limits_invalid$"):
            ncen.DiagnosticResourceLimits(**bad)  # type: ignore[arg-type]


def test_review_f13b_tsv_reader_checks_after_each_bounded_read() -> None:
    import io

    raw = b"abcdefghijklmnopqrstuvwxyz0123456789ABCD\n"
    events: list[tuple[str, int, int]] = []

    class RecordingStream(io.BytesIO):
        def readline(self, size: int = -1) -> bytes:
            chunk = super().readline(size)
            events.append(("read", size, len(chunk)))
            return chunk

    monitor = ncen.DiagnosticResourceMonitor(
        ncen.DiagnosticResourceLimits(hash_block_bytes=8, tsv_line_max_bytes=64),
        rss_probe=lambda: 64 * _MIB,
    )
    bytes_processed = monitor.bytes_processed

    def record_check(count: int, phase: str) -> None:
        events.append(("check", count, 0))
        bytes_processed(count, phase)

    monitor.bytes_processed = record_check  # type: ignore[method-assign]
    line, oversized = ncen._diagnostic_read_bounded_tsv_line(
        RecordingStream(raw), max_bytes=64, monitor=monitor
    )
    assert line == raw and not oversized
    assert len(events) % 2 == 0
    for read, check in zip(events[::2], events[1::2], strict=True):
        assert read[0] == "read" and check[0] == "check"
        assert read[1] <= 8 and read[2] == check[1] <= 8


def _review_f13b_fixture(
    tmp_path: Path, *, packages: int
) -> tuple[ncen.DiagnosticSourceManifestPin, list[tuple[Path, dict[str, object]]]]:
    """``packages`` relationship-heavy valid DERA ZIPs (<= 2 MiB raw each) behind a sealed pin."""
    root = tmp_path / "stage2a-f13b"
    root.mkdir(parents=True)
    packaged: list[tuple[Path, dict[str, object]]] = []
    for package in range(packages):
        path = root / f"pkg-{package:02d}.zip"
        tables = _f13b_tables(
            package,
            accessions=_F13B_SCALE_ACCESSIONS,
            funds=2,
            advisers=8,
            underwriters=1,
            padding=_F13B_SCALE_PADDING,
        )
        artifact = _f13b_package(path, tables, padding=True)
        members = artifact["members"]
        assert isinstance(members, list) and sum(int(item["bytes"]) for item in members) <= 2 * _MIB
        packaged.append((path, artifact))
    seal = _acquisition_seal(root / "acquisition")
    pin = _stage2a_write_manifest(
        root,
        [artifact for _path, artifact in packaged],
        seal,
        quarantines=[dict(item) for item in seal.quarantines],
        boundaries=[dict(item) for item in seal.boundaries],
    )
    return pin, packaged


_F13B_MEASURE = r'''
import json, sys, tracemalloc
from pathlib import Path
from src.bonds.default_events import ncen
manifest, digest, size, spill, trace = sys.argv[1], sys.argv[2], int(sys.argv[3]), Path(sys.argv[4]), sys.argv[5]
spill.mkdir(parents=True, exist_ok=True)
pin = ncen.DiagnosticSourceManifestPin(manifest_path=Path(manifest), manifest_sha256=digest, manifest_size=size)
monitor = ncen.DiagnosticResourceMonitor(ncen.DiagnosticResourceLimits(spill_dir=spill))
if trace == "1":
    tracemalloc.start()
index = ncen.read_diagnostic_source_rows(pin, monitor=monitor)
report = monitor.report()
result = {"rows": len(index.rows), "rss_max": report["rss_max_observed"], "probe": report["rss_probe"]}
if trace == "1":
    current, peak = tracemalloc.get_traced_memory()
    result.update(heap_current=current, heap_peak=peak)
print(json.dumps(result, sort_keys=True))
'''


def test_review_f13b_join_store_bounded_4_vs_16_packages(tmp_path: Path) -> None:
    reports: dict[int, dict[str, object]] = {}
    fresh: dict[int, dict[str, int]] = {}
    for count in (4, 16):
        pin, packaged = _review_f13b_fixture(tmp_path / f"n{count}", packages=count)
        expected_ids: set[str] = set()
        for path, artifact in packaged:
            expected_ids.update(item.source_row_id for item in _f13b_reference_rows(artifact, path.name, path, {}))
        spill = tmp_path / f"spill-{count}"
        monitor = _review_f13_monitor(spill)
        index = ncen.read_diagnostic_source_rows(pin, monitor=monitor)
        # Exact parity with the retired whole-table loader, package by package.
        assert len(index.rows) == count * _F13B_SCALE_ROWS
        assert {item.source_row_id for item in index.rows} == expected_ids
        report = _review_f13_assert_released(monitor, spill)
        assert report["join_stores_created"] == report["join_stores_removed"] == count
        # Resident TSV rows never exceed one accession (1 + 1 + 2 funds + 16 advisers + 1).
        assert report["tsv_rows_resident_max"] == 21
        assert 0 < int(report["tsv_line_bytes_max"]) <= _MIB  # type: ignore[call-overload]
        assert report["handles_open_max"] <= 2
        assert 0 < int(report["max_rows_between_checks"]) <= 1024  # type: ignore[call-overload]
        assert int(report["max_bytes_between_checks"]) <= _MIB  # type: ignore[call-overload]
        assert int(report["join_plans_checked"]) > 0  # type: ignore[call-overload]
        assert {"zip_rows", "dera_parse", "dera_rows"} <= set(report["phases_checked"])  # type: ignore[arg-type]
        assert report["unmonitored_scopes"] == [
            "read_diagnostic_acquisition_ledger_entry_exit_only",
            "frozen_dera_per_accession_projection_entry_exit_only",
            "source_index_issuance_entry_exit_only",
        ]
        reports[count] = report
        del index
        for trace in ("0", "1"):
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    _F13B_MEASURE,
                    str(pin.manifest_path),
                    pin.manifest_sha256,
                    str(pin.manifest_size),
                    str(tmp_path / f"fresh-{count}-{trace}"),
                    trace,
                ],
                cwd=ROOT,
                capture_output=True,
                check=True,
                timeout=300,
            )
            fresh.setdefault(count, {}).update(json.loads(completed.stdout))
            assert list((tmp_path / f"fresh-{count}-{trace}").iterdir()) == []
    # The per-package spill does not scale with the number of packages.
    assert reports[4]["join_store_pages_max"] == reports[16]["join_store_pages_max"]
    assert 0 < int(reports[16]["join_store_bytes_max"]) <= 16 * _MIB  # type: ignore[call-overload]
    for count in (4, 16):
        assert fresh[count]["rows"] == count * _F13B_SCALE_ROWS
        assert fresh[count]["rss_max"] < 512 * _MIB
    # Fresh-process measurement: transient Python heap does not grow with the package count, and
    # peak RSS grows only with the retained index (64 MiB allocator margin).
    transient = {count: fresh[count]["heap_peak"] - fresh[count]["heap_current"] for count in fresh}
    assert transient[16] - transient[4] < 8 * _MIB, transient
    retained_growth = fresh[16]["heap_current"] - fresh[4]["heap_current"]
    assert fresh[16]["rss_max"] - fresh[4]["rss_max"] < 2 * retained_growth + 64 * _MIB, (fresh, retained_growth)


def test_review_f13b_python_heap_holds_no_whole_table(tmp_path: Path) -> None:
    import tracemalloc

    pin, artifacts = _review_f13_fixture(tmp_path / "base", packages=2)
    per_package = max(int(item["bytes"]) for item in artifacts if item["kind"] == "dera_zip")
    spill = tmp_path / "spill"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB, hash_block_bytes=64 * 1024)
    dera_phases = {"spool_write", "zip_member_hash", "zip_rows", "dera_parse", "dera_rows"}
    state: dict[str, object] = {"baseline": None}
    growth: dict[str, int] = {}
    checked = monitor.check

    def measuring_check(phase: str, artifact_id: str | None = None) -> None:
        # Python heap above the level at the start of each DERA package (retained plus
        # transient), measured at every checkpoint of that package's spill/join/emit phases.
        _current, peak = tracemalloc.get_traced_memory()
        baseline = state["baseline"]
        if isinstance(baseline, int) and phase in dera_phases:
            growth[phase] = max(growth.get(phase, 0), peak - baseline)
        if phase == "artifact_load" and str(artifact_id).startswith("dera-"):
            tracemalloc.reset_peak()
            state["baseline"] = tracemalloc.get_traced_memory()[0]
        elif phase not in dera_phases:
            state["baseline"] = None
        checked(phase, artifact_id)

    monitor.check = measuring_check  # type: ignore[method-assign]
    tracemalloc.start()
    try:
        sources = ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    finally:
        tracemalloc.stop()
    assert len(sources.rows) == 2 * 28
    report = _review_f13_assert_released(monitor, spill)
    assert set(growth) == dera_phases, growth
    # Each package carries three padded TSV tables of ~480 KB each; whole-table materialization
    # (plus the legacy re-parse) would add more than one package at these checkpoints. What
    # remains is one bounded ~60 KB raw line with its decoded and split copies.
    assert max(growth.values()) < per_package // 2, (growth, per_package)
    assert report["tsv_rows_resident_max"] == 5  # one accession: SUB, REG, FUND, ADVISER, UW
    assert report["tsv_line_bytes_max"] > _REVIEW_F13_PADDING


def test_review_f13b_spill_failures_leave_no_result_or_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin, packaged = _review_f13b_fixture(tmp_path / "base", packages=2)
    sizes = [int(artifact["bytes"]) for _path, artifact in packaged]  # type: ignore[call-overload]
    zip_bytes = max(sizes)

    # (1) Disk exhaustion mid-spill (ENOSPC analogue): free space vanishes after the ZIP spool
    # copy and the join store creation, at a periodic capacity check during row streaming.
    calls = {"count": 0}

    def free(_path: Path) -> int:
        calls["count"] += 1
        return 1 << 40 if calls["count"] <= 3 else 0

    spill = tmp_path / "spill-enospc"
    spill.mkdir()
    monitor = ncen.DiagnosticResourceMonitor(
        ncen.DiagnosticResourceLimits(spill_dir=spill, rows_per_check=16),
        rss_probe=lambda: 64 * _MIB,
        disk_free_probe=free,
    )
    with pytest.raises(ncen.NcenError, match="^diagnostic_spill_disk_insufficient$"):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    report = _review_f13_assert_released(monitor, spill)
    assert report["join_stores_created"] == 1 and 0 < int(report["rows_streamed"]) < _F13B_SCALE_ACCESSIONS  # type: ignore[call-overload]

    # (2) The spill budget caps the join store file: SQLITE_FULL mid-ingest refuses typed.
    spill = tmp_path / "spill-measure"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB)
    ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    needed = int(_review_f13_assert_released(monitor, spill)["join_store_pages_max"])  # type: ignore[call-overload]
    cap_pages = needed // 2
    assert cap_pages >= ncen._DIAGNOSTIC_JOIN_MIN_PAGES, needed
    budget = zip_bytes + cap_pages * 4096
    spill = tmp_path / "spill-cap"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB, spill_budget_bytes=budget)
    with pytest.raises(ncen.NcenError, match="^diagnostic_spill_budget_exceeded$"):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    report = _review_f13_assert_released(monitor, spill)
    assert report["join_stores_created"] == 1
    assert 0 < int(report["join_store_pages_max"]) <= (budget - min(sizes)) // 4096  # type: ignore[call-overload]
    # A budget leaving fewer than the minimum store pages refuses before any store exists.
    spill = tmp_path / "spill-no-room"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB, spill_budget_bytes=zip_bytes + 4096)
    with pytest.raises(ncen.NcenError, match="^diagnostic_spill_budget_exceeded$"):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    assert _review_f13_assert_released(monitor, spill)["join_stores_created"] == 0

    # (3) An I/O failure inside SQLite (injected at the 25th insert) refuses typed and cleans up.
    real_connect = sqlite3.connect

    class FailingConnection:
        def __init__(self, inner: sqlite3.Connection) -> None:
            self._inner = inner
            self.inserts = 0

        def execute(self, sql: str, params: object = ()) -> sqlite3.Cursor:
            if sql.startswith("INSERT"):
                self.inserts += 1
                if self.inserts == 25:
                    raise sqlite3.OperationalError("disk I/O error")
            return self._inner.execute(sql, params)  # type: ignore[arg-type]

        def close(self) -> None:
            self._inner.close()

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", lambda *args, **kwargs: FailingConnection(real_connect(*args, **kwargs)))
        spill = tmp_path / "spill-io"
        monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB)
        with pytest.raises(ncen.NcenError, match="^diagnostic_spill_io_failed$"):
            ncen.read_diagnostic_source_rows(pin, monitor=monitor)
        assert _review_f13_assert_released(monitor, spill)["join_stores_created"] == 1

    # (4) Cancellation while projecting accessions propagates and leaves no scratch behind.
    holder: dict[str, ncen.DiagnosticResourceMonitor] = {}

    def cancelling_probe() -> int:
        if holder["monitor"].phase == "dera_parse":
            raise KeyboardInterrupt
        return 64 * _MIB

    spill = tmp_path / "spill-cancel"
    holder["monitor"] = monitor = _review_f13_monitor(spill, probe=cancelling_probe)
    with pytest.raises(KeyboardInterrupt):
        ncen.read_diagnostic_source_rows(pin, monitor=monitor)
    _review_f13_assert_released(monitor, spill)

    # (5) After every refusal the same pinned inputs load completely.
    assert len(ncen.read_diagnostic_source_rows(pin).rows) == 2 * _F13B_SCALE_ROWS


def test_review_f13b_join_plans_refuse_unbounded_sorts(tmp_path: Path) -> None:
    spill = tmp_path / "spill"
    monitor = _review_f13_monitor(spill, probe=lambda: 64 * _MIB)
    path = spill / "join.sqlite3"
    store = ncen._DiagnosticJoinStore(path, monitor, capacity_bytes=16 * _MIB)
    try:
        assert path.is_file()
        store.capacity_check()
        # Schema page plus 16 empty B-trees (5 row tables, 6 key indexes, 5 derived tables/index).
        assert monitor.report()["join_store_pages_max"] == 17 < ncen._DIAGNOSTIC_JOIN_MIN_PAGES
        for sql in (
            "SELECT line FROM adv ORDER BY digest",
            "SELECT DISTINCT digest FROM uw",
            "SELECT a.line FROM adv AS a JOIN uw AS u ON u.digest = a.digest",
        ):
            with pytest.raises(ncen.NcenError, match="^diagnostic_join_plan_unbounded$"):
                list(store.rows(sql, phase="dera_parse"))
        assert list(store.rows("SELECT line FROM adv ORDER BY line", phase="dera_parse")) == []
    finally:
        store.close()
    assert not path.exists()
    report = _review_f13_assert_released(monitor, spill)
    assert report["join_stores_created"] == 1 and report["join_plans_checked"] == 1
    # A store path that already exists is never reused or overwritten.
    path.write_bytes(b"not ours")
    with pytest.raises(ncen.NcenError, match="^diagnostic_spill_io_failed$"):
        ncen._DiagnosticJoinStore(path, monitor, capacity_bytes=16 * _MIB)
    assert path.read_bytes() == b"not ours"


def test_c1a_public_export_requires_external_trust_and_never_certifies_v1(tmp_path: Path) -> None:
    pin, _sources, _index, _cohort, declaration = _stage2b_export_fixture(tmp_path)
    declaration["schema_version"] = "ncen_purpose_diagnostics_declaration_v1"
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_schema_unsupported$"):
        ncen.write_purpose_diagnostics(
            trusted_run=None, declaration=declaration,
            output_root=tmp_path / "old-envelope", code_root=ROOT,
            input_roots={"ncen-source": pin.manifest_path.parent},
        )
    assert not (tmp_path / "old-envelope").exists()


def test_c1a_trust_anchor_is_mandatory_and_independent(tmp_path: Path) -> None:
    trust_root = tmp_path / "external-trust"
    trust_root.mkdir()
    path = trust_root / "manifest.json"
    path.write_bytes(b'{}')
    with pytest.raises(ncen.NcenError, match="^diagnostic_trust_anchor_missing$"):
        ncen._purpose_admit_trust(None, root=tmp_path / "export", code_root=ROOT,
                                  input_roots={}, declaration=None)
    with pytest.raises(ncen.NcenError, match="^diagnostic_trust_anchor_mismatch$"):
        ncen._purpose_admit_trust(
            ncen.DiagnosticTrustPin(path, "f" * 64, 2), root=tmp_path / "export",
            code_root=ROOT, input_roots={}, declaration=None,
        )


def _c1b1a_finite_fixture(
    evidence: Path, *, mode: str = "historical_reconstruction", late_header: bool = False,
    historical_f09: bool = False,
) -> tuple[ncen.DiagnosticCohort, dict[str, object]]:
    """Author source witnesses before deriving the cohort; these dates are independent."""
    second_date = R + dt.timedelta(days=1)
    first = dt.datetime(2026, 6, 1, 11, tzinfo=UTC)
    retrieved = K - dt.timedelta(hours=2)
    header_retrieved = K - dt.timedelta(hours=3)
    definitions = (
        ("w-01", R, cik(1), "S000000001", "0000000001-26-000001", "header-1"),
        ("w-02", R, cik(1), "S000000001", "0000000001-26-000001", "header-1"),
        ("w-03", R, cik(1), "S000000002", "0000000001-26-000002", "header-2"),
        ("w-04", R, cik(2), None, "0000000002-26-000001", "header-3"),
        ("w-05", second_date, cik(1), "S000000001", "0000000001-26-000003", "header-4"),
        ("w-06", second_date, cik(1), "S000000002", "0000000001-26-000004", "header-5"),
    )
    if historical_f09:
        definitions = tuple((row_id, second_date if row_id == "w-04" else date,
                             registrant, "S000000001" if row_id == "w-04" else series, accession, header)
                            for row_id, date, registrant, series, accession, header in definitions)
    acceptances = (
        dt.datetime(2026, 6, 1, 12, tzinfo=UTC),
        dt.datetime(2026, 6, 1, 14, tzinfo=UTC),
        dt.datetime(2026, 6, 1, 13, tzinfo=UTC),
        K + dt.timedelta(days=1) if historical_f09 else dt.datetime(2026, 6, 2, 15, tzinfo=UTC),
        K + dt.timedelta(days=1),
    )
    header_descriptors = []
    headers = {}
    for (row_id, report_date, registrant, _series, accession, header_id), accepted in zip(
        (definitions[0], *definitions[2:]), acceptances, strict=True,
    ):
        assert row_id != "w-02"
        header = {"schema_version": "ncen_synthetic_membership_header_v1",
                  "accession": accession, "cik": registrant, "R": report_date.isoformat(),
                  "form_type": "NPORT-P", "acceptance_at": _stage2a_timestamp(accepted)}
        headers[header_id] = header
        path = f"{header_id}.json"
        (evidence / path).write_bytes(ncen._purpose_json_bytes(header))
        header_descriptors.append({"artifact_id": header_id, "root_id": "independent", "path": path,
                                   "sha256": _stage2a_sha(evidence / path), "bytes": (evidence / path).stat().st_size,
                                   "retrieved_at": _stage2a_timestamp(
                                       K + dt.timedelta(hours=1) if late_header and header_id == "header-1"
                                       else header_retrieved)})
    source_rows = [
        {"schema_version": "ncen_synthetic_membership_source_row_v1", "source_row_id": row_id,
         "R": report_date.isoformat(), "cik": registrant, "series_id": series,
         "accession": accession, "header_artifact_id": header_id}
        for row_id, report_date, registrant, series, accession, header_id in definitions
    ]
    (evidence / "membership.jsonl").write_bytes(ncen._purpose_jsonl_bytes(source_rows))
    source = {"artifact_id": "membership-1", "root_id": "independent", "path": "membership.jsonl",
              "sha256": _stage2a_sha(evidence / "membership.jsonl"),
              "bytes": (evidence / "membership.jsonl").stat().st_size, "rows": len(source_rows),
              "public_at": _stage2a_timestamp(first), "data_known_at": _stage2a_timestamp(first),
              "retrieved_at": _stage2a_timestamp(retrieved)}
    definition = {
        "schema_version": "ncen_synthetic_membership_definition_v1", "lane": "synthetic_fixture",
        "completeness_basis": "externally_declared_finite_membership_fixture",
        "fixture_id": "hand-authored-two-date-membership",
        "K": _stage2a_timestamp(K), "mode": mode,
        "report_dates": [R.isoformat(), second_date.isoformat()],
        "membership_artifacts": [source], "header_artifacts": header_descriptors,
        "derivation_version": "ncen_synthetic_membership_derivation_v1",
    }
    (evidence / "synthetic_membership_definition.json").write_bytes(ncen._purpose_json_bytes(definition))
    header_spec = {item["artifact_id"]: item for item in header_descriptors}
    inventory = []
    for line, row in enumerate(source_rows, start=1):
        header = headers[row["header_artifact_id"]]
        accepted = dt.datetime.strptime(header["acceptance_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        public = max(first, accepted)
        possession = max(retrieved, K + dt.timedelta(hours=1)
                         if late_header and row["header_artifact_id"] == "header-1" else header_retrieved)
        public_late = public > K
        possession_late = mode == "current_run" and possession > K
        excluded = public_late or possession_late
        inventory.append({
            "schema_version": "ncen_synthetic_membership_inventory_row_v1",
            "source_row_id": row["source_row_id"], "artifact_id": source["artifact_id"],
            "artifact_sha256": source["sha256"], "source_line": line,
            "source_row_sha256": hashlib.sha256(ncen._diagnostic_canonical(row)).hexdigest(),
            "header_artifact_id": row["header_artifact_id"],
            "header_sha256": header_spec[row["header_artifact_id"]]["sha256"],
            "R": row["R"], "cik": row["cik"], "series_id": row["series_id"],
            "fund_key": row["series_id"] if row["series_id"] else f"cik:{row['cik']}",
            "accession": row["accession"], "acceptance_at": header["acceptance_at"],
            "source_public_at": source["public_at"], "data_known_at": source["data_known_at"],
            "source_retrieved_at": source["retrieved_at"],
            "header_retrieved_at": header_spec[row["header_artifact_id"]]["retrieved_at"],
            "public_available_at": _stage2a_timestamp(public), "possession_at": _stage2a_timestamp(possession),
            "disposition": "excluded" if excluded else "included",
            "reasons": sorted((["public_after_cutoff"] if public_late else [])
                              + (["possession_after_cutoff"] if possession_late else [])),
        })
    (evidence / "synthetic_membership_inventory.jsonl").write_bytes(ncen._purpose_jsonl_bytes(inventory))
    source_provenance = ncen.InventorySource(
        "synthetic-nport-package", "7" * 64, "synthetic-package-id",
        K - dt.timedelta(days=2), K - dt.timedelta(days=10),
    )
    members = (
        ncen.DiagnosticCohortMember(R, cik(1),
                                    ("S000000002",) if late_header and mode == "current_run"
                                    else ("S000000001", "S000000002"), acceptances[1]),
        ncen.DiagnosticCohortMember(second_date if historical_f09 else R, cik(2),
                                    ("S000000001",) if historical_f09 else (f"cik:{cik(2)}",),
                                    acceptances[2] if historical_f09 else acceptances[1]),
        *((ncen.DiagnosticCohortMember(second_date, cik(1), ("S000000001",), acceptances[3]),)
          if not historical_f09 else ()),
    )
    digest = ncen._diagnostic_cohort_digest(
        knowledge_cutoff=K, knowledge_mode=mode,
        inventory_digest="synthetic-inventory", sources=(source_provenance,), members=members,
    )
    cohort = ncen.DiagnosticCohort(
        K, mode, "synthetic-inventory", (source_provenance,),
        members, digest, ncen.DIAGNOSTIC_COHORT_VERSION, "sealed_vote_inventory_full_universe",
        True, True, ncen._DIAGNOSTIC_COHORT_SEAL,
    )
    assert len(inventory) == 6
    return cohort, definition


def _c1a_package(
    tmp_path: Path, *, mode: str = "historical_reconstruction", late_header: bool = False,
    selected_fixture: bool = False, historical_f09: bool = False, later_parsed: bool = False,
) -> tuple[ncen.DiagnosticTrustPin, dict[str, object], dict[str, Path]]:
    source_pin, acq_seal, artifacts = _stage2a_fixture_with_seal(
        tmp_path / "source", xml_artifact_times={"retrieved_at": K + dt.timedelta(days=1)},
        xml_family_name="Acme Fund" if historical_f09 else "Acme Funds",
        include_second_registrant=historical_f09,
    )
    if later_parsed:
        source_root = source_pin.manifest_path.parent
        header = next(row for row in artifacts if row["kind"] == "header")
        xml = next(row for row in artifacts if row["kind"] == "edgar_xml")
        accession = "0000000001-26-000055"
        later_header = source_root / "later-submission.txt"
        later_xml = source_root / "later.xml"
        later_header.write_bytes((source_root / header["path"]).read_bytes()
                                 .replace(ACCESSION.encode(), accession.encode())
                                 .replace(b"20260301120000", b"20260302120000")
                                 .replace(b"20260301\n", b"20260302\n"))
        later_xml.write_bytes((source_root / xml["path"]).read_bytes())
        accepted = _stage2a_timestamp(dt.datetime(2026, 3, 2, 17, tzinfo=UTC))
        new_header = {**header, "artifact_id": "header-later", "path": later_header.name,
                      "sha256": _stage2a_sha(later_header), "bytes": later_header.stat().st_size,
                      "accession_number": accession, "public_at": accepted, "data_known_at": accepted}
        new_xml = {**xml, "artifact_id": "xml-later", "path": later_xml.name,
                   "sha256": _stage2a_sha(later_xml), "bytes": later_xml.stat().st_size,
                   "accession_number": accession, "header_artifact_id": "header-later",
                   "retrieved_at": _stage2a_timestamp(K - dt.timedelta(days=1)),
                   "public_at": accepted, "data_known_at": accepted}
        artifacts.extend((new_header, new_xml))
        source_manifest = json.loads(source_pin.manifest_path.read_bytes())
        source_manifest["artifacts"] = artifacts
        source_pin.manifest_path.write_bytes(ncen._purpose_json_bytes(source_manifest))
        source_pin = dataclasses.replace(
            source_pin, manifest_sha256=_stage2a_sha(source_pin.manifest_path),
            manifest_size=source_pin.manifest_path.stat().st_size)
    sources = ncen.read_diagnostic_source_rows(source_pin)
    evidence = tmp_path / "independent"
    evidence.mkdir()
    cohort, definition = _c1b1a_finite_fixture(
        evidence, mode=mode, late_header=late_header, historical_f09=historical_f09,
    )
    declaration = _stage2b_declaration(source_pin, sources, cohort)
    runtime_files = sorted(
        {*(f"src/{p.relative_to(ROOT / 'src').as_posix()}" for p in (ROOT / "src").rglob("*.py")),
         "tests/test_bond_default_ncen_purposes.py", "requirements.txt"}
    )
    runtime = {
        "schema_version": "ncen_diagnostic_runtime_code_manifest_v1",
        "launcher": "tests/test_bond_default_ncen_purposes.py",
        "dependencies": ["requirements.txt"],
        "files": [{"path": path, "sha256": _stage2a_sha(ROOT / path),
                   "bytes": (ROOT / path).stat().st_size} for path in runtime_files],
    }
    (evidence / "runtime.json").write_bytes(ncen._purpose_json_bytes(runtime))
    ledger = ncen.read_diagnostic_acquisition_ledger(acq_seal.pin)
    (evidence / "ledger.json").write_bytes(ncen._diagnostic_canonical(ledger.digest_object()))
    (evidence / "cohort.jsonl").write_bytes(ncen._purpose_jsonl_bytes([
        ncen._purpose_cohort_record(member, cohort.inventory_digest) for member in
        sorted(cohort.members, key=lambda item: ncen._purpose_cohort_record(item, cohort.inventory_digest)["record_id"])
    ]))
    (evidence / "cohort_derivation.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_diagnostic_cohort_derivation_v1",
        "lane": "synthetic_fixture", "cohort_derivation_version": ncen.DIAGNOSTIC_COHORT_VERSION,
        "K": definition["K"], "mode": definition["mode"],
        "inventory_digest": cohort.inventory_digest, "cohort_digest": cohort.cohort_digest,
        "inventory_sources": declaration["cohort_provenance"]["inventory_sources"],
        "cohort_sha256": _stage2a_sha(evidence / "cohort.jsonl"),
        "member_count": len(cohort.members),
        "fund_key_count": sum(len(item.fund_keys) for item in cohort.members),
        "derivation_code_manifest_sha256": _stage2a_sha(evidence / "runtime.json"),
        "completeness_basis": "externally_declared_finite_membership_fixture",
        "membership_definition_sha256": _stage2a_sha(evidence / "synthetic_membership_definition.json"),
        "membership_inventory_sha256": _stage2a_sha(evidence / "synthetic_membership_inventory.jsonl"),
        "membership_derivation_version": "ncen_synthetic_membership_derivation_v1",
        "report_dates": definition["report_dates"], "source_row_count": 6,
        "included_source_row_count": 4 if historical_f09 else 3 if late_header and mode == "current_run" else 5,
        "excluded_source_row_count": 2 if historical_f09 else 3 if late_header and mode == "current_run" else 1,
    }))
    # The finite fixture declares every index row before the checkpoint is assembled.
    # It includes a competing amendment, an index-only placeholder, an unknown-period
    # accession, an after-K accession and the F6-quarantined amendment.
    index_witnesses = (
        ("N-CEN", cik(1), "2026-03-01", ACCESSION),
        *((("N-CEN/A", cik(1), "2026-03-02", "0000000001-26-000002"),)
          if not selected_fixture else ()),
        *((('N-CEN', cik(2), '2026-03-01', '0000000002-26-000001'),)
          if historical_f09 else ()),
        *((('N-CEN', cik(2), '2026-03-03', '0000000002-26-000003'),)
          if not historical_f09 else ()),
        ("N-CEN", cik(3), "2026-03-04", "0000000003-26-000004"),
        ("N-CEN", cik(1), "2026-10-01", "0000000001-26-000005"),
        *((('N-CEN', cik(1), '2026-03-02', '0000000001-26-000055'),)
          if later_parsed else ()),
        ("N-CEN/A", cik(10_000), "2026-03-05", _ACQ_DEFAULT_ENTRIES[0].accession),
    )
    index_bytes = ("Form Type  Company Name  CIK  Date Filed  File Name\n"
                   + "-" * 60 + "\n"
                   + "".join(f"{form}  Fixture Fund  {registrant}  {date}  "
                             f"edgar/data/{int(registrant)}/{accession}.txt\n"
                             for form, registrant, date, accession in index_witnesses)).encode("latin-1")
    (evidence / "form.idx").write_bytes(index_bytes)
    from src.bonds.default_events.sec_acquisition import (
        index_date_public_available_at,
        parse_acceptance_header,
        parse_form_index,
    )

    index_entries = parse_form_index(index_bytes)
    assert len(index_entries) == len(index_witnesses)
    index_retrieved_at = _stage2a_timestamp(K + dt.timedelta(days=10))
    index_rows = [{"form_type": entry.form_type, "company_name": entry.company_name,
                   "cik": entry.cik, "date_filed": entry.date_filed.isoformat(),
                   "file_name": entry.file_name, "accession_number": entry.accession_number,
                   "index_artifact_id": "index:independent:form.idx",
                   "index_record_locator": f"form.idx:{line}",
                   "index_retrieved_at": index_retrieved_at}
                  for line, entry in enumerate(index_entries, start=3)]
    (evidence / "ncen_index_entries.jsonl").write_bytes(ncen._purpose_jsonl_bytes(index_rows))

    source_root = source_pin.manifest_path.parent
    headers = {}
    header_rows = []
    for artifact in artifacts:
        if artifact["kind"] != "header":
            continue
        header = parse_acceptance_header(
            (source_root / artifact["path"]).read_bytes(),
            accession_number=artifact["accession_number"], url=artifact["source_url"],
            document_sha256=artifact["sha256"],
            retrieved_at=ncen._purpose_parse_timestamp(artifact["retrieved_at"], code="fixture_header"),
        )
        headers[header.accession_number] = header
        header_rows.append({"artifact_id": artifact["artifact_id"],
                            "accession_number": header.accession_number,
                            "header_sha256": header.header_sha256,
                            "public_disposition": "after_K" if artifact["public_at"] > _stage2a_timestamp(K) else "by_K",
                            "possession_disposition": "after_K" if artifact["retrieved_at"] > _stage2a_timestamp(K) else "by_K"})
    copy_rows = []
    for artifact in artifacts:
        kind = artifact["kind"]
        if kind == "header":
            continue
        if kind == "edgar_xml":
            filings = (ncen.parse_ncen_primary_doc(
                (source_root / artifact["path"]).read_bytes(),
                accession_number=artifact["accession_number"], source_url=artifact["source_url"],
                retrieved_at=ncen._purpose_parse_timestamp(artifact["retrieved_at"], code="fixture_xml")),)
        else:
            filings = ncen.parse_dera_ncen_package(
                source_root / artifact["path"], expected_sha256=artifact["sha256"],
                package_label=artifact["package_label"],
                retrieved_at=ncen._purpose_parse_timestamp(artifact["retrieved_at"], code="fixture_dera"),
                first_verified_public_at=ncen._purpose_parse_timestamp(
                    artifact["data_known_at"], code="fixture_dera"),
            ).filings
        for filing in filings:
            header_id = artifact.get("header_artifact_id") if kind == "edgar_xml" else next(
                (item["artifact_id"] for item in artifacts
                 if item["kind"] == "header" and item["accession_number"] == filing.accession_number), None)
            copy_rows.append({
                "copy_id": ncen._diagnostic_source_copy_id(
                    kind, artifact["artifact_id"], artifact["sha256"], filing.accession_number),
                "artifact_id": artifact["artifact_id"], "source_kind": kind,
                "raw_sha256": artifact["sha256"],
                "raw_locator": (artifact["path"] if kind == "edgar_xml" else
                                f"{artifact['path']}:{filing.accession_number}"),
                "header_artifact_id": header_id,
                "header_sha256": None if header_id is None else headers[filing.accession_number].document_sha256,
                "filing": ncen._purpose_baseline_filing(filing),
            })
    copy_rows.sort(key=lambda row: row["copy_id"])
    (evidence / "ncen_copies.jsonl").write_bytes(ncen._purpose_jsonl_bytes(copy_rows))
    scope = json.loads((acq_seal.root / "scope.json").read_bytes())
    exclusions = {item.accession_number for item in ledger.exclusions}
    boundaries = {item.accession_number for item in ledger.boundaries}
    copy_accessions = {row["filing"]["accession_number"] for row in copy_rows}
    reconciliation = {
        "schema_version": "ncen_diagnostic_source_reconciliation_v1",
        "index_records": [
            {**{key: row[key] for key in (
                "index_artifact_id", "index_record_locator", "accession_number")},
             "disposition": ("f6_excluded" if row["accession_number"] in exclusions
                             else "after_K" if index_date_public_available_at(entry) > K
                             else "acquired" if row["accession_number"] in copy_accessions
                             else "missing_content"),
             "retrieval_disposition": "after_K" if index_retrieved_at > _stage2a_timestamp(K) else "by_K",
             "period_status": "known" if row["accession_number"] in copy_accessions else "unknown"}
            for row, entry in zip(index_rows, index_entries, strict=True)
        ],
        "copies": [
            {**{key: row[key] for key in (
                "copy_id", "artifact_id", "source_kind", "raw_sha256", "raw_locator")},
             "period_status": "unknown" if row["filing"]["report_period_end"] is None else "known",
             "public_disposition": "after_K" if next(
                 item for item in artifacts if item["artifact_id"] == row["artifact_id"]
             )["public_at"] > _stage2a_timestamp(K) else "by_K",
             "data_disposition": "after_K" if next(
                 item for item in artifacts if item["artifact_id"] == row["artifact_id"]
             )["data_known_at"] > _stage2a_timestamp(K) else "by_K",
             "possession_disposition": "after_K" if row["filing"]["retrieved_at"] > _stage2a_timestamp(K) else "by_K"}
            for row in copy_rows
        ],
        "headers": sorted(header_rows, key=lambda row: row["artifact_id"]),
        "parser_quarantine": [],
        "f6_exclusions": [item.payload() for item in ledger.exclusions],
        "f6_boundaries": [item.payload() for item in ledger.boundaries],
        "acquisition_records": [{"accession_number": item["accession_number"],
                                 "disposition": ("excluded" if item["accession_number"] in exclusions
                                                 else "boundary" if item["accession_number"] in boundaries
                                                 else "verified")}
                                for item in scope["requests"]],
    }
    (evidence / "source_reconciliation.json").write_bytes(ncen._purpose_json_bytes(reconciliation))
    (evidence / "synthetic-stage2b.bin").write_bytes(b"synthetic-pre-C1-stage2b-segment\n")

    def pin(root_id: str, path: str, root: Path) -> dict[str, object]:
        target = root / path
        return {"root_id": root_id, "path": path, "sha256": _stage2a_sha(target),
                "bytes": target.stat().st_size}

    roles: dict[str, list[dict[str, object]]] = {
        "runtime_code_manifest": [pin("independent", "runtime.json", evidence)],
        "cohort_checkpoint": [pin("independent", "cohort.jsonl", evidence)],
        "cohort_derivation_receipt": [pin("independent", "cohort_derivation.json", evidence)],
        "ncen_source_manifest": [pin("ncen-source", source_pin.manifest_path.name, source_root)],
        "ncen_parsed_copies": [pin("independent", "ncen_copies.jsonl", evidence)],
        "acquisition_scope": [pin("acquisition", "scope.json", acq_seal.root)],
        "acquisition_sha256sums": [pin("acquisition", "SHA256SUMS", acq_seal.root)],
        "acquisition_ledger": [pin("independent", "ledger.json", evidence)],
        "source_reconciliation": [pin("independent", "source_reconciliation.json", evidence)],
        "synthetic_membership_definition": [pin("independent", "synthetic_membership_definition.json", evidence)],
        "synthetic_membership_inventory": [pin("independent", "synthetic_membership_inventory.jsonl", evidence)],
        "ncen_dera_artifacts": [], "ncen_xml_artifacts": [], "ncen_header_artifacts": [],
    }
    for item in artifacts:
        roles[{"dera_zip": "ncen_dera_artifacts", "edgar_xml": "ncen_xml_artifacts",
               "header": "ncen_header_artifacts"}[item["kind"]]].append(
                   pin("ncen-source", str(item["path"]), source_root))
    for name in ("ncen_dera_artifacts", "ncen_xml_artifacts", "ncen_header_artifacts"):
        roles[name].sort(key=lambda item: (item["root_id"], item["path"]))
    index_pin = pin("independent", "form.idx", evidence)
    roles["ncen_index_artifacts"] = [index_pin]
    (evidence / "index-manifest.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_diagnostic_index_manifest_v1",
        "artifacts": [{"pin": index_pin, "retrieved_at": index_retrieved_at}],
    }))
    roles["ncen_index_manifest"] = [pin("independent", "index-manifest.json", evidence)]
    raw_artifacts = [
        {"artifact_id": item["artifact_id"], "kind": item["kind"],
         **pin("ncen-source", str(item["path"]), source_root),
         "retrieved_at": item.get("retrieved_at"), "public_at": item.get("public_at"),
         "data_known_at": item.get("data_known_at"),
         "package_label": item.get("package_label"),
         "accession_claim": item.get("accession_number"), "cik_claim": item.get("registrant_cik"),
         "source_url": item.get("source_url"),
         "header_artifact_ids": ([str(item["header_artifact_id"])]
                                 if item.get("header_artifact_id") is not None else []),
         "acquisition_request_locators": []}
        for item in artifacts
    ]
    (evidence / "raw-audit.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_diagnostic_raw_audit_manifest_v1",
        "lane": "synthetic_fixture", "purpose": "complete_ncen_candidate_audit",
        "artifacts": sorted(raw_artifacts, key=lambda item: item["artifact_id"]),
        "index_manifest_pin": roles["ncen_index_manifest"][0],
        "acquisition_scope_pin": roles["acquisition_scope"][0],
        "acquisition_sha256sums_pin": roles["acquisition_sha256sums"][0],
        "parser_code_manifest_sha256": roles["runtime_code_manifest"][0]["sha256"],
    }))
    roles["ncen_raw_audit_manifest"] = [pin("independent", "raw-audit.json", evidence)]
    raw_inventory = ncen._purpose_enumerate_raw_inputs(
        {"independent": evidence, "ncen-source": source_root, "acquisition": acq_seal.root},
        {key: tuple(value) for key, value in roles.items()},
        json.loads(source_pin.manifest_path.read_bytes()),
    )
    assert len(raw_inventory) == ((178 if selected_fixture else 179)
                                  + (7 if historical_f09 else 0) + (3 if later_parsed else 0))
    candidates = ncen._purpose_q2_candidates(
        {"independent": evidence, "ncen-source": source_root, "acquisition": acq_seal.root},
        {key: tuple(value) for key, value in roles.items()},
        json.loads(source_pin.manifest_path.read_bytes()), cohort_ciks={cik(1), cik(2)},
    )
    report = candidates["audit"]
    for path, payload in (
        ("raw_input_inventory.jsonl", ncen._purpose_jsonl_bytes(report["raw_input_inventory"])),
        ("ncen_index_entries.jsonl", ncen._purpose_jsonl_bytes(report["index_observations"])),
        ("ncen_copies.jsonl", ncen._purpose_jsonl_bytes(report["copy_observations"])),
        ("source_reconciliation.json", ncen._purpose_json_bytes(report["reconciliation"])),
    ):
        (evidence / path).write_bytes(payload)
    roles["ncen_parsed_copies"] = [pin("independent", "ncen_copies.jsonl", evidence)]
    roles["source_reconciliation"] = [pin("independent", "source_reconciliation.json", evidence)]
    (evidence / "checkpoint.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_diagnostic_baseline_checkpoint_v2",
        "lane": "synthetic_fixture", "purpose": "ncen_purpose_diagnostic_selection_replay",
        "diagnostic_only": True, "contexts": declaration["contexts"],
        "versions": {"selector": ncen.RULE_VERSION, "merge": ncen.RULE_VERSION,
                     "amendment": ncen.AMENDMENT_SEMANTICS_VERSION,
                     "cohort": ncen.DIAGNOSTIC_COHORT_VERSION,
                     "source": ncen.DIAGNOSTIC_SOURCE_ATTESTATION_VERSION,
                     "admission": ncen.DIAGNOSTIC_ADMISSION_VERSION,
                     "normalizer": ncen.DIAGNOSTIC_NAME_NORMALIZER_VERSION,
                     "edge": ncen.DIAGNOSTIC_EDGE_VERSION,
                     "fold": ncen.DIAGNOSTIC_FOLD_PROTOCOL_VERSION},
        "inventory_digest": declaration["inventory_digest"],
        "cohort_digest": declaration["cohort_digest"],
        "ncen_evidence_digest": sources.evidence_digest,
        "exclusion_ledger_digest": ledger.exclusion_ledger_digest,
        "files": [{"sha256": _stage2a_sha(evidence / path),
                   "bytes": (evidence / path).stat().st_size,
                   "path": path, "rows": (len((evidence / path).read_bytes().splitlines())
                                             if path.endswith("jsonl") else 1)}
                  for path, role in ncen._PURPOSE_BASELINE_FILES],
        "source_manifest_pin": roles["ncen_source_manifest"][0],
        "acquisition_pin": {"lane": "synthetic_fixture",
                            "scope": roles["acquisition_scope"][0],
                            "sha256sums": roles["acquisition_sha256sums"][0]},
    }))
    roles["diagnostic_baseline_checkpoint"] = [pin("independent", "checkpoint.json", evidence)]
    source_bytes = (ROOT / "src/bonds/default_events/ncen.py").read_bytes()
    markers = (
        b"\n\n# === FE-1 purpose-labelled diagnostic core",
        b"\n\n# === FE-1 diagnostic source sidecars and selection attestation",
        b"\n\n# === FE-1 diagnostic synthetic export and sealed envelope",
    )
    start1, start2, start3 = (source_bytes.index(marker) for marker in markers)
    for name, start, stop in (
        ("baseline_v3", 0, start1), ("baseline_stage1", start1, start2),
        ("baseline_stage2a", start2, start3),
    ):
        roles[name] = [{"root_id": "code", "path": "src/bonds/default_events/ncen.py",
                        "sha256": hashlib.sha256(source_bytes[start:stop]).hexdigest(),
                        "bytes": len(source_bytes), "offset": start, "length": stop - start}]
    stage2b = pin("independent", "synthetic-stage2b.bin", evidence)
    roles["baseline_stage2b"] = [{**stage2b, "offset": 0, "length": stage2b["bytes"]}]
    role_rows = [{"role": name, "pins": pins} for name, pins in sorted(roles.items())]
    trust = {
        "schema_version": "ncen_diagnostic_trust_manifest_v3", "lane": "synthetic_fixture",
        "roles": role_rows,
        "logical_ledger": {"version": ncen.DIAGNOSTIC_EXCLUSION_LEDGER_DIGEST_VERSION,
                           "digest": ledger.exclusion_ledger_digest},
    }
    manifest_path = evidence / "trust.json"
    manifest_path.write_bytes(ncen._purpose_json_bytes(trust))
    declaration["schema_version"] = ncen.DIAGNOSTIC_DECLARATION_VERSION
    declaration["diagnostic_schema_version"] = ncen.DIAGNOSTIC_EXPORT_VERSION
    declaration["pin_roles"] = role_rows
    declaration["lane"] = "synthetic_fixture"
    declaration["source_code"] = [{"path": item["path"], "sha256": item["sha256"]}
                                  for item in runtime["files"]]
    declaration["baseline_seals"] = [{"name": name, "sha256": roles[name][0]["sha256"]}
                                      for name in sorted(roles) if name.startswith("baseline_")]
    declaration["input_artifacts"] = sorted(
        ({key: item[key] for key in ("root_id", "path", "sha256", "bytes")}
         for name, pins in roles.items() if name not in {"runtime_code_manifest", *(
             name for name in roles if name.startswith("baseline_"))}
         for item in pins), key=lambda item: (item["root_id"], item["path"]),
    )
    declaration["exclusion_ledger_digest"] = ledger.exclusion_ledger_digest
    declaration["quarantine_seal"] = {"scope_sha256": acq_seal.pin.scope_sha256,
                                       "sha256sums_sha256": acq_seal.pin.sha256sums_sha256}
    roots = {"independent": evidence, "ncen-source": source_root, "acquisition": acq_seal.root}
    declaration["trusted_run_manifest_sha256"] = _stage2a_sha(manifest_path)
    return ncen.DiagnosticTrustPin(manifest_path, _stage2a_sha(manifest_path),
                                   manifest_path.stat().st_size), declaration, roots


def test_c1b1b_pinned_complete_index_and_copy_baseline(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    assert checkpoint.lane == "synthetic_fixture"
    assert ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)[
        "schema_version"] == "ncen_diagnostic_baseline_admission_v2"
    assert len(ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)[
        "index_entries"]) == 6
    export = tmp_path / "export"
    export.mkdir()
    (export / "declaration.json").write_bytes(ncen._purpose_json_bytes(declaration))
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.validate_purpose_export(export, trusted_run=trust,
                                     code_root=ROOT, input_roots=roots)
    admitted = _c1a_admit(trust, declaration, roots, tmp_path / "export")
    baseline = admitted["candidate_snapshot"]
    assert len(baseline["contexts"]) == 2
    assert len(baseline["members"]) == 3
    assert len(baseline["selections"]) == 3
    assert len(baseline["index_entries"]) == 6
    assert len(baseline["filings"]) == 2
    assert {filing.accession_number for filing in baseline["filings"]} == {ACCESSION}
    assert {filing.source for filing in baseline["filings"]} == {"dera", "edgar_xml"}
    assert list(baseline["headers"]) == [ACCESSION]
    index_rows = [json.loads(row) for row in
                   (roots["independent"] / "ncen_index_entries.jsonl").read_bytes().splitlines()]
    assert [row["entry"]["accession_number"] for row in index_rows if row["entry"] is not None] == [
        ACCESSION, "0000000001-26-000002", "0000000002-26-000003",
        "0000000003-26-000004", "0000000001-26-000005",
        _ACQ_DEFAULT_ENTRIES[0].accession,
    ]
    assert all(row["index_retrieved_at"] > _stage2a_timestamp(K) for row in index_rows)
    reconciliation = json.loads((roots["independent"] / "source_reconciliation.json").read_bytes())
    assert len(reconciliation["index_observation_ids"]) == len(index_rows)
    assert reconciliation["f1_subset_claim"]["verification"] == "verified_q2a"
    assert len(reconciliation["f6_exclusions"]) == 3
    assert len(reconciliation["f6_boundaries"]) == 1
    assert len(reconciliation["acquisition_requests"]) == 5
    assert len(reconciliation["physical_inputs"]) == 179
    copy_rows = [json.loads(line) for line in (roots["independent"] / "ncen_copies.jsonl").read_bytes().splitlines()]
    assert len(copy_rows) == 7
    assert any(row["source_kind"] == "edgar_xml" and
               row["filing"]["retrieved_at"] > _stage2a_timestamp(K)
               for row in copy_rows if row["filing"] is not None)
    assert sum(item["disposition"] == "frozen_index_placeholder"
               for item in baseline["coverage"].values()) > 0
    assert any(selection.cik == cik(1) and selection.selection_reason is not None
               for selection in baseline["selections"])


def test_c1b1b_q3_checkpoint_is_factory_only_and_identity_bound(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    assert checkpoint.__slots__ == ("_token", "__weakref__")
    assert not hasattr(checkpoint, "__dict__")
    assert not hasattr(checkpoint, "_admission")
    with pytest.raises(TypeError, match="factory_only"):
        ncen.DiagnosticBaselineCheckpoint(digest=checkpoint.digest, lane="synthetic_fixture")
    for name, value in (
        ("digest", "f" * 64), ("lane", "sealed_source"), ("contexts", ()),
        ("members", ()), ("index_entries", ()), ("filings", ()),
        ("coverage", {}), ("reconciliation", {}), ("raw_input_inventory", ()),
        ("copy_observations", ()),
    ):
        with pytest.raises((TypeError, ValueError)):
            dataclasses.replace(checkpoint, **{name: value})
    for operation in (copy.copy, copy.deepcopy, lambda value: pickle.loads(pickle.dumps(value))):
        with pytest.raises(TypeError, match="not_(copyable|serializable)"):
            operation(checkpoint)
    forged = object.__new__(ncen.DiagnosticBaselineCheckpoint)
    object.__setattr__(forged, "_token", object.__getattribute__(checkpoint, "_token"))
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(forged, trusted_run=trust)
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=dataclasses.replace(trust))
    separately_loaded_source = ncen.read_diagnostic_source_rows(ncen.DiagnosticSourceManifestPin(
        roots["ncen-source"] / "manifest.json",
        next(row["pins"][0]["sha256"] for row in json.loads(trust.manifest_path.read_bytes())["roles"]
             if row["role"] == "ncen_source_manifest"),
        (roots["ncen-source"] / "manifest.json").stat().st_size,
    ))
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(
            checkpoint, trusted_run=trust, source_index=separately_loaded_source)
    for name, value in (("digest", "f" * 64), ("lane", "sealed_source")):
        with pytest.raises(AttributeError):
            setattr(checkpoint, name, value)
        with pytest.raises(AttributeError):
            object.__setattr__(checkpoint, name, value)
    snapshot = ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)
    snapshot["contexts"][0]["mode"] = "current_run"
    snapshot["contexts"][0]["K"] = "2026-01-01T00:00:00.000000Z"
    snapshot["members"][0]["cik"] = cik(99)
    snapshot["members"][0]["fund_keys"] = ["S000000099"]
    snapshot["index_entries"].clear()
    snapshot["merged"]["excluded"].clear()
    snapshot["reconciliation"]["f6_exclusions"].clear()
    snapshot["raw_input_inventory"].clear()
    assert ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)["contexts"][0][
        "mode"] == "historical_reconstruction"
    assert ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)[
        "raw_input_inventory"]
    original_token = object.__getattribute__(checkpoint, "_token")
    object.__setattr__(checkpoint, "_token", object())
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)
    object.__setattr__(checkpoint, "_token", original_token)
    assert ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)[
        "schema_version"] == "ncen_diagnostic_baseline_admission_v2"


def test_c1b1b_q3_review_repro_omitted_candidate_self_reseal(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    assert not hasattr(checkpoint, "_admission")
    original_digest = checkpoint.digest
    original = ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)
    local = ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)
    assert local is not original and local["index_entries"] is not original["index_entries"]
    local["index_entries"].pop(-1)
    local["copy_observations"].pop(-1)
    local["raw_input_inventory"].pop(-1)
    local["contexts"].pop(-1)
    local_digest = hashlib.sha256(ncen._diagnostic_canonical(local)).hexdigest()
    assert local_digest != original_digest
    assert ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust) == original
    assert checkpoint.digest == original_digest
    with pytest.raises(TypeError, match="factory_only"):
        ncen.DiagnosticBaselineCheckpoint(digest=local_digest)


def test_c1b1b_q3_review_repro_foreign_trust_rebind(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    foreign_trust = dataclasses.replace(trust)
    assert not hasattr(checkpoint, "_admission")
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=foreign_trust)


@pytest.mark.parametrize("mutated", ["trust", "source", "membership"])
def test_c1b1b_q3_original_bound_object_content_is_frozen(mutated: str, tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    record = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(checkpoint)]
    if mutated == "trust":
        object.__setattr__(trust, "manifest_size", trust.manifest_size + 1)
    elif mutated == "source":
        row = record.source_index_ref.rows[0]
        object.__setattr__(row, "name_raw", "altered after admission")
    else:
        member = record.membership_ref.members[0]
        object.__setattr__(member, "fund_keys", ("S000000099",))
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust)


def test_c1b1b_q3_registry_record_and_membership_are_not_transferable(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    record = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(checkpoint)]
    assert record.trusted_run_ref is trust
    assert record.source_custody_ref is ncen._diagnostic_bound_source_custody(record.source_index_ref)
    assert record.membership_ref._seal is ncen._PURPOSE_MEMBERSHIP_SEAL
    assert not hasattr(record, "__dict__") and not hasattr(record, "admitted")
    for name in ("payload_bytes", "payload_digest", "trusted_run_ref", "source_index_ref"):
        with pytest.raises(AttributeError):
            setattr(record, name, None)
        with pytest.raises(AttributeError):
            object.__setattr__(record, name, None)
    foreign_membership = dataclasses.replace(record.membership_ref)
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen._diagnostic_bound_baseline_admission(
            checkpoint, trusted_run=trust, membership_checkpoint=foreign_membership)
    bound = ncen._diagnostic_bound_baseline_admission(
        checkpoint, trusted_run=trust, source_index=record.source_index_ref,
        membership_checkpoint=record.membership_ref)
    assert bound is not record and bound["source"]["source_index"]["rows"]
    assert bound["membership"]["checkpoint"]["members"]

    def only_json(value: object) -> bool:
        if type(value) is dict:
            return all(type(key) is str and only_json(item) for key, item in value.items())
        if type(value) is list:
            return all(only_json(item) for item in value)
        return value is None or type(value) in (str, int, bool, float)

    assert only_json(bound)
    assert ncen._diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust) is not bound


def test_c1b1b_q3_weak_registry_cleanup_guards_stale_callback(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    checkpoint = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    carrier_id = id(checkpoint)
    record = ncen._DIAGNOSTIC_BASELINE_REGISTRY[carrier_id]
    assert record.trusted_run_ref is trust
    other = object.__new__(ncen.DiagnosticBaselineCheckpoint)
    ncen._diagnostic_baseline_cleanup(carrier_id, weakref.ref(other))
    assert ncen._DIAGNOSTIC_BASELINE_REGISTRY[carrier_id] is record
    carrier_ref = weakref.ref(checkpoint)
    del checkpoint
    gc.collect()
    assert carrier_ref() is None
    assert carrier_id not in ncen._DIAGNOSTIC_BASELINE_REGISTRY


@pytest.mark.parametrize("lane", ["sealed_source", "pure_fixture"])
def test_c1b1b_q3_non_synthetic_lanes_stay_closed(lane: str, tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    manifest["lane"] = lane
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    changed_trust = ncen.DiagnosticTrustPin(
        trust.manifest_path, _stage2a_sha(trust.manifest_path), trust.manifest_path.stat().st_size)
    with pytest.raises(ncen.NcenError, match="^diagnostic_required_pins_unverified$"):
        ncen.read_diagnostic_baseline_checkpoint(changed_trust, code_root=ROOT, input_roots=roots)


@pytest.mark.parametrize("target", ["checkpoint.json", "ncen_index_entries.jsonl",
                                     "ncen_copies.jsonl", "raw_input_inventory.jsonl"])
def test_c1b1b_q3_fixed_trust_refuses_local_reseal(target: str, tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    path = roots["independent"] / target
    if target.endswith("jsonl"):
        rows = [json.loads(row) for row in path.read_bytes().splitlines()]
        rows.pop(-1)
        path.write_bytes(ncen._purpose_jsonl_bytes(rows))
        checkpoint_path = roots["independent"] / "checkpoint.json"
        checkpoint = json.loads(checkpoint_path.read_bytes())
        descriptor = next(row for row in checkpoint["files"] if row["path"] == target)
        descriptor.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size, rows=len(rows))
        checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
    else:
        path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ncen.NcenError, match="^diagnostic_checkpoint_pin_mismatch$"):
        ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)


@pytest.mark.parametrize("target,reason", [
    ("checkpoint.json", "diagnostic_checkpoint_pin_mismatch"),
    ("runtime.json", "diagnostic_runtime_code_mismatch"),
    ("cohort_derivation.json", "diagnostic_checkpoint_pin_mismatch"),
])
def test_q3_p2_fixed_pin_precedes_control_decoding(target: str, reason: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    path = roots["independent"] / target
    path.write_bytes(b'{"contexts":[null],"files":[null],"inventory_sources":[null]}\n')
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("target,field,value,reason", [
    ("checkpoint.json", "contexts", "missing", "diagnostic_pinned_input_invalid"),
    ("checkpoint.json", "contexts", [None], "diagnostic_pinned_input_invalid"),
    ("checkpoint.json", "contexts", None, "diagnostic_pinned_input_invalid"),
    ("checkpoint.json", "contexts", [{"R": "2026-06-01", "K": None, "mode": "current_run"}],
     "diagnostic_pinned_input_invalid"),
    ("checkpoint.json", "files", [None], "diagnostic_pinned_input_invalid"),
    ("checkpoint.json", "source_manifest_pin", None, "diagnostic_pinned_input_invalid"),
    ("checkpoint.json", "schema_version", "ncen_diagnostic_baseline_checkpoint_v999",
     "diagnostic_checkpoint_schema_unsupported"),
    ("runtime.json", "files", None, "diagnostic_pinned_input_invalid"),
    ("runtime.json", "files", [None], "diagnostic_pinned_input_invalid"),
    ("runtime.json", "files", [{"path": "src/example.py", "sha256": "a" * 64, "bytes": True}],
     "diagnostic_pinned_input_invalid"),
    ("runtime.json", "files", [{"path": "src/example.py", "sha256": "a" * 64, "bytes": -1}],
     "diagnostic_pinned_input_invalid"),
    ("runtime.json", "files", [{"path": "src/example.py", "sha256": "a" * 64, "bytes": 1.5}],
     "diagnostic_pinned_input_invalid"),
    ("runtime.json", "files", [{"path": "../escape.py", "sha256": "a" * 64, "bytes": 1}],
     "diagnostic_pinned_input_invalid"),
    ("runtime.json", "dependencies", [None], "diagnostic_pinned_input_invalid"),
    ("cohort_derivation.json", "inventory_sources", [None], "diagnostic_pinned_input_invalid"),
    ("cohort_derivation.json", "inventory_sources", [{"package_label": "partial"}],
     "diagnostic_pinned_input_invalid"),
    ("cohort_derivation.json", "member_count", True, "diagnostic_pinned_input_invalid"),
])
def test_q3_p2_reanchored_malformed_control_refused_before_projection(
    target: str, field: str, value: object, reason: str, tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    path = roots["independent"] / target
    control = json.loads(path.read_bytes())
    if value == "missing":
        control.pop(field)
    else:
        control[field] = value
    path.write_bytes(ncen._purpose_json_bytes(control))
    manifest = json.loads(trust.manifest_path.read_bytes())
    role = {"checkpoint.json": "diagnostic_baseline_checkpoint",
            "runtime.json": "runtime_code_manifest",
            "cohort_derivation.json": "cohort_derivation_receipt"}[target]
    pinned = next(item["pins"][0] for item in manifest["roles"] if item["role"] == role)
    pinned.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    repinned = _c1a_repin(trust)
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        ncen.read_diagnostic_baseline_checkpoint(repinned, code_root=ROOT, input_roots=roots)
    declaration["pin_roles"] = manifest["roles"]
    if target != "runtime.json":
        next(item for item in declaration["input_artifacts"] if item["path"] == target).update(
            sha256=pinned["sha256"], bytes=pinned["bytes"])
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        _c1a_admit(repinned, declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("target,mutation", [
    ("checkpoint.json", "duplicate"), ("checkpoint.json", "noncanonical"),
    ("runtime.json", "duplicate"), ("runtime.json", "noncanonical"),
    ("cohort_derivation.json", "duplicate"), ("cohort_derivation.json", "noncanonical"),
])
def test_q3_p2_reanchored_invalid_json_control_refused(
    target: str, mutation: str, tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    path = roots["independent"] / target
    raw = path.read_bytes()
    if mutation == "duplicate":
        path.write_bytes(b'{"schema_version":"duplicate",' + raw[1:])
    else:
        path.write_bytes(b" " + raw)
    manifest = json.loads(trust.manifest_path.read_bytes())
    role = {"checkpoint.json": "diagnostic_baseline_checkpoint",
            "runtime.json": "runtime_code_manifest",
            "cohort_derivation.json": "cohort_derivation_receipt"}[target]
    pinned = next(item["pins"][0] for item in manifest["roles"] if item["role"] == role)
    pinned.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    repinned = _c1a_repin(trust)
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        ncen.read_diagnostic_baseline_checkpoint(repinned, code_root=ROOT, input_roots=roots)
    declaration["pin_roles"] = manifest["roles"]
    if target != "runtime.json":
        next(item for item in declaration["input_artifacts"] if item["path"] == target).update(
            sha256=pinned["sha256"], bytes=pinned["bytes"])
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        _c1a_admit(repinned, declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("target,field", [
    ("checkpoint.json", "contexts"), ("runtime.json", "files"),
    ("cohort_derivation.json", "inventory_sources"),
])
def test_q3_p2_invalid_control_precedes_stale_declaration_echo(
    target: str, field: str, tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    path = roots["independent"] / target
    control = json.loads(path.read_bytes())
    control[field] = [None]
    path.write_bytes(ncen._purpose_json_bytes(control))
    manifest = json.loads(trust.manifest_path.read_bytes())
    role = {"checkpoint.json": "diagnostic_baseline_checkpoint",
            "runtime.json": "runtime_code_manifest",
            "cohort_derivation.json": "cohort_derivation_receipt"}[target]
    pinned = next(item["pins"][0] for item in manifest["roles"] if item["role"] == role)
    pinned.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


def _q3_reopen_trust(path: str, digest: str, size: int, roots_json: str) -> str:
    roots = {key: Path(value) for key, value in json.loads(roots_json).items()}
    trust = ncen.DiagnosticTrustPin(Path(path), digest, size)
    return ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots).digest


def test_c1b1b_q3_fresh_process_reopens_pin_without_transferring_handle(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    local = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    another = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    assert local is not another and object.__getattribute__(local, "_token") is not object.__getattribute__(
        another, "_token")
    assert local.digest == another.digest
    script = ("import runpy, sys; ns = runpy.run_path(sys.argv[1]); "
              "print(ns['_q3_reopen_trust'](sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]))")
    result = subprocess.run(
        [sys.executable, "-c", script, str(Path(__file__).resolve()), str(trust.manifest_path),
         trust.manifest_sha256, str(trust.manifest_size),
         json.dumps({key: str(value) for key, value in roots.items()})],
        cwd=ROOT, text=True, capture_output=True, check=True, timeout=60,
    )
    assert result.stdout.strip() == local.digest


@pytest.mark.parametrize("target", [
    "ncen_index_entries.jsonl", "ncen_copies.jsonl", "source_reconciliation.json",
    "form.idx", "index-manifest.json", "primary_doc.xml", "submission.txt", "dera.zip",
])
def test_c1b1b_fixed_external_pin_rejects_changed_inputs(target: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    base = roots["ncen-source"] if target in {"primary_doc.xml", "submission.txt", "dera.zip"} else roots["independent"]
    path = base / target
    path.write_bytes(path.read_bytes() + b" ")
    expected = ("diagnostic_artifact_size_mismatch" if base == roots["ncen-source"]
                else "diagnostic_checkpoint_pin_mismatch")
    with pytest.raises(ncen.NcenError, match=f"^{expected}$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("target,attack", [
    ("ncen_index_entries.jsonl", "drop"),
    ("ncen_index_entries.jsonl", "add"),
    ("ncen_index_entries.jsonl", "locator"),
    ("ncen_index_entries.jsonl", "unknown_key"),
    ("ncen_copies.jsonl", "drop"),
    ("ncen_copies.jsonl", "duplicate_id"),
    ("ncen_copies.jsonl", "projection"),
    ("ncen_copies.jsonl", "copy_time"),
    ("ncen_copies.jsonl", "header_time"),
    ("ncen_copies.jsonl", "header_sha"),
    ("ncen_copies.jsonl", "fabricated_complete"),
    ("source_reconciliation.json", "omit_f6"),
    ("source_reconciliation.json", "quarantine_cik"),
])
def test_c1b1b_reanchored_local_cache_cannot_replace_raw_truth(
    target: str, attack: str, tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    evidence = roots["independent"]
    path = evidence / target
    if target.endswith("jsonl"):
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        if attack == "drop":
            rows.pop(1)
        elif attack == "add":
            added = dict(rows[1])
            added["index_record_locator"] = "form.idx:100"
            rows.append(added)
        elif attack == "locator":
            rows[1]["index_record_locator"] = "form.idx:99"
        elif attack == "unknown_key":
            rows[0]["selection_label"] = "safe"
        elif attack == "duplicate_id":
            rows[1]["observation_id"] = rows[0]["observation_id"]
        elif attack == "projection":
            rows[0]["filing"]["funds"][0]["series_id"] = "S000000099"
        elif attack == "copy_time":
            rows[0]["filing"]["retrieved_at"] = _stage2a_timestamp(K)
        elif attack == "header_time":
            rows[0]["filing"]["header_retrieved_at"] = _stage2a_timestamp(K)
        elif attack == "fabricated_complete":
            placeholder = next(row for row in rows if row["parse_state"] == "index_only")
            claimed = next(row["filing"] for row in rows if row["filing"] is not None)
            placeholder["filing"] = {**claimed, "accession_number": placeholder["accession_claims"][0],
                                     "status": "parsed", "reasons": []}
        else:
            rows[0]["header_sha256"] = "f" * 64
        path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    else:
        payload = json.loads(path.read_bytes())
        if attack == "omit_f6":
            payload["f6_exclusions"].pop()
        else:
            payload["f6_exclusions"][0]["registrant_cik"] = cik(99)
        path.write_bytes(ncen._purpose_json_bytes(payload))
    checkpoint_path = evidence / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_bytes())
    descriptor = next(item for item in checkpoint["files"] if item["path"] == target)
    descriptor.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size,
                      rows=len(path.read_bytes().splitlines()) if target.endswith("jsonl") else 1)
    checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
    manifest = json.loads(trust.manifest_path.read_bytes())
    for row in manifest["roles"]:
        if row["role"] == "diagnostic_baseline_checkpoint" or (
                target == "ncen_copies.jsonl" and row["role"] == "ncen_parsed_copies") or (
                target == "source_reconciliation.json" and row["role"] == "source_reconciliation"):
            pinned = checkpoint_path if row["role"] == "diagnostic_baseline_checkpoint" else path
            row["pins"][0].update(sha256=_stage2a_sha(pinned), bytes=pinned.stat().st_size)
    declaration["pin_roles"] = manifest["roles"]
    for item in declaration["input_artifacts"]:
        if item["path"] in {target, "checkpoint.json"}:
            pinned = evidence / item["path"]
            item.update(sha256=_stage2a_sha(pinned), bytes=pinned.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^(diagnostic_candidate_universe_mismatch|diagnostic_source_checkpoint_mismatch)$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


def test_c1b1a_finite_membership_requires_independent_definition(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"] for item in declaration["pin_roles"]}
    assert {"synthetic_membership_definition", "synthetic_membership_inventory"} <= roles
    admitted = _c1a_admit(trust, declaration, roots, tmp_path / "export")
    checkpoint = admitted["membership_checkpoint"]
    assert checkpoint.report_dates == (R, R + dt.timedelta(days=1))
    assert (checkpoint.source_row_count, checkpoint.included_source_row_count,
            checkpoint.excluded_source_row_count) == (6, 5, 1)
    assert [(member.report_date, member.cik, member.fund_keys, member.known_at)
            for member in checkpoint.members] == [
        (R, cik(1), ("S000000001", "S000000002"), dt.datetime(2026, 6, 1, 14, tzinfo=UTC)),
        (R, cik(2), (f"cik:{cik(2)}",), dt.datetime(2026, 6, 1, 14, tzinfo=UTC)),
        (R + dt.timedelta(days=1), cik(1), ("S000000001",), dt.datetime(2026, 6, 2, 15, tzinfo=UTC)),
    ]
    with pytest.raises(TypeError, match="not_serializable"):
        import pickle

        pickle.dumps(checkpoint)


@pytest.mark.parametrize("mode,expected_included,expected_keys", [
    ("historical_reconstruction", 5, ("S000000001", "S000000002")),
    ("current_run", 3, ("S000000002",)),
])
def test_c1b1a_retrieval_after_k_only_excludes_current_run(
    mode: str, expected_included: int, expected_keys: tuple[str, ...], tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path, mode=mode, late_header=True)
    checkpoint = _c1a_admit(trust, declaration, roots, tmp_path / "export")["membership_checkpoint"]
    assert checkpoint.included_source_row_count == expected_included
    assert checkpoint.members[0].fund_keys == expected_keys
    assert checkpoint.members[0].known_at == checkpoint.members[1].known_at
    inventory = [json.loads(line) for line in (roots["independent"] / "synthetic_membership_inventory.jsonl").read_bytes().splitlines()]
    first = inventory[0]
    assert first["public_available_at"] < _stage2a_timestamp(K) < first["possession_at"]
    assert first["reasons"] == ([] if mode == "historical_reconstruction" else ["possession_after_cutoff"])


def _c1b1a_repin_definition(
    trust: ncen.DiagnosticTrustPin, declaration: dict[str, object], roots: dict[str, Path],
    *, updated_paths: tuple[str, ...] = (),
) -> ncen.DiagnosticTrustPin:
    evidence = roots["independent"]
    path = evidence / "synthetic_membership_definition.json"
    definition = json.loads(path.read_bytes())
    for collection in ("membership_artifacts", "header_artifacts"):
        for descriptor in definition[collection]:
            if descriptor["path"] in updated_paths:
                target = evidence / descriptor["path"]
                descriptor["sha256"] = _stage2a_sha(target)
                descriptor["bytes"] = target.stat().st_size
                if collection == "membership_artifacts":
                    descriptor["rows"] = len(target.read_bytes().splitlines())
    path.write_bytes(ncen._purpose_json_bytes(definition))
    manifest = json.loads(trust.manifest_path.read_bytes())
    role = next(item for item in manifest["roles"] if item["role"] == "synthetic_membership_definition")
    role["pins"][0].update(sha256=_stage2a_sha(path), bytes=path.stat().st_size)
    declaration["pin_roles"] = manifest["roles"]
    for artifact in declaration["input_artifacts"]:
        if artifact["path"] == path.name:
            artifact.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    return _c1a_repin(trust)


@pytest.mark.parametrize("path", [
    "synthetic_membership_definition.json", "synthetic_membership_inventory.jsonl",
    "membership.jsonl", "header-1.json", "cohort.jsonl", "cohort_derivation.json",
])
def test_c1b1a_fixed_external_pin_refuses_mutated_files(path: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    target = roots["independent"] / path
    target.write_bytes(target.read_bytes() + b" ")
    with pytest.raises(ncen.NcenError, match="^diagnostic_checkpoint_pin_mismatch$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("role", ["synthetic_membership_definition", "synthetic_membership_inventory"])
def test_c1b1a_new_roles_mandatory(role: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    manifest["roles"] = [item for item in manifest["roles"] if item["role"] != role]
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match=f"^diagnostic_pin_role_missing:{role}$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


def test_c1b1a_old_trust_version_rejected_before_roles(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    manifest["schema_version"] = "ncen_diagnostic_trust_manifest_v1"
    manifest["roles"] = [item for item in manifest["roles"] if item["role"] != "synthetic_membership_definition"]
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_pin_digest_invalid$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("omission", ["member", "fund_key", "date"])
def test_c1b1a_local_cohort_reseal_cannot_replace_external_pin(omission: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    evidence = roots["independent"]
    path = evidence / "cohort.jsonl"
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    if omission == "member":
        rows = [row for row in rows if row["cik"] != cik(2)]
    elif omission == "fund_key":
        row = next(row for row in rows if row["cik"] == cik(1) and row["R"] == R.isoformat())
        row["fund_keys"] = ["S000000001"]
        payload = {key: value for key, value in row.items() if key not in {"schema_version", "record_type", "record_id"}}
        row.update(ncen._purpose_record("cohort_member", payload))
    else:
        rows = [row for row in rows if row["R"] == R.isoformat()]
    path.write_bytes(ncen._purpose_jsonl_bytes(sorted(rows, key=lambda row: row["record_id"])))
    receipt_path = evidence / "cohort_derivation.json"
    receipt = json.loads(receipt_path.read_bytes())
    receipt["cohort_sha256"] = _stage2a_sha(path)
    receipt["member_count"] = len(rows)
    receipt["fund_key_count"] = sum(len(row["fund_keys"]) for row in rows)
    receipt_path.write_bytes(ncen._purpose_json_bytes(receipt))
    with pytest.raises(ncen.NcenError, match="^diagnostic_checkpoint_pin_mismatch$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


def test_c1b1a_reanchored_receipt_cannot_claim_wrong_cohort_digest(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    receipt_path = roots["independent"] / "cohort_derivation.json"
    receipt = json.loads(receipt_path.read_bytes())
    declaration["cohort_digest"] = receipt["cohort_digest"] = "e" * 64
    receipt_path.write_bytes(ncen._purpose_json_bytes(receipt))
    manifest = json.loads(trust.manifest_path.read_bytes())
    role = next(item for item in manifest["roles"] if item["role"] == "cohort_derivation_receipt")
    role["pins"][0].update(sha256=_stage2a_sha(receipt_path), bytes=receipt_path.stat().st_size)
    declaration["pin_roles"] = manifest["roles"]
    for artifact in declaration["input_artifacts"]:
        if artifact["path"] == receipt_path.name:
            artifact.update(sha256=_stage2a_sha(receipt_path), bytes=receipt_path.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_cohort_baseline_mismatch$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


def test_c1b1a_reanchored_derived_inventory_is_only_a_comparison(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    evidence = roots["independent"]
    path = evidence / "synthetic_membership_inventory.jsonl"
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    rows[0]["fund_key"] = "S000000099"
    path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    receipt_path = evidence / "cohort_derivation.json"
    receipt = json.loads(receipt_path.read_bytes())
    receipt["membership_inventory_sha256"] = _stage2a_sha(path)
    receipt_path.write_bytes(ncen._purpose_json_bytes(receipt))
    manifest = json.loads(trust.manifest_path.read_bytes())
    for name, target in (("synthetic_membership_inventory", path),
                         ("cohort_derivation_receipt", receipt_path)):
        role = next(item for item in manifest["roles"] if item["role"] == name)
        role["pins"][0].update(sha256=_stage2a_sha(target), bytes=target.stat().st_size)
        for artifact in declaration["input_artifacts"]:
            if artifact["path"] == target.name:
                artifact.update(sha256=_stage2a_sha(target), bytes=target.stat().st_size)
    declaration["pin_roles"] = manifest["roles"]
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_membership_inventory_mismatch$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("attack,reason", [
    ("source_omission", "diagnostic_membership_inventory_mismatch"),
    ("source_member_omission", "diagnostic_membership_inventory_mismatch"),
    ("source_fund_omission", "diagnostic_membership_inventory_mismatch"),
    ("source_duplicate", "diagnostic_membership_row_duplicate"),
    ("outcome_field", "diagnostic_membership_schema_invalid"),
    ("malformed_series", "diagnostic_membership_identity_invalid"),
    ("competing_accession", "diagnostic_membership_revision_unsupported"),
    ("date_empty", "diagnostic_membership_date_empty"),
    ("unknown_time", "diagnostic_membership_time_unknown"),
    ("revision", "diagnostic_membership_revision_unsupported"),
    ("header_conflict", "diagnostic_membership_copy_conflict"),
    ("copy_cik_conflict", "diagnostic_membership_copy_conflict"),
    ("copy_date_conflict", "diagnostic_membership_copy_conflict"),
    ("missing_cik", "diagnostic_membership_identity_unknown"),
    ("fallback_bad_series", "diagnostic_membership_identity_invalid"),
])
def test_c1b1a_independently_reanchored_source_refusals(
    attack: str, reason: str, tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    evidence = roots["independent"]
    definition_path = evidence / "synthetic_membership_definition.json"
    definition = json.loads(definition_path.read_bytes())
    rows = [json.loads(line) for line in (evidence / "membership.jsonl").read_bytes().splitlines()]
    paths = ()
    if attack == "source_omission":
        rows = [row for row in rows if row["source_row_id"] != "w-02"]
    elif attack in {"source_member_omission", "source_fund_omission"}:
        omitted = "w-04" if attack == "source_member_omission" else "w-03"
        header_id = "header-3" if attack == "source_member_omission" else "header-2"
        rows = [row for row in rows if row["source_row_id"] != omitted]
        definition["header_artifacts"] = [
            item for item in definition["header_artifacts"] if item["artifact_id"] != header_id
        ]
    elif attack == "source_duplicate":
        rows[1]["source_row_id"] = rows[0]["source_row_id"]
    elif attack == "outcome_field":
        rows[0]["is_default"] = True
    elif attack == "malformed_series":
        rows[0]["series_id"] = "S-invalid"
    elif attack == "fallback_bad_series":
        rows[3]["series_id"] = "invalid"
    elif attack == "competing_accession":
        rows[1]["accession"] = rows[2]["accession"]
        rows[1]["header_artifact_id"] = rows[2]["header_artifact_id"]
    elif attack == "date_empty":
        definition["report_dates"].append((R + dt.timedelta(days=2)).isoformat())
    elif attack == "unknown_time":
        definition["membership_artifacts"][0]["public_at"] = None
    elif attack == "revision":
        header = json.loads((evidence / "header-1.json").read_bytes())
        header["form_type"] = "NPORT-P/A"
        (evidence / "header-1.json").write_bytes(ncen._purpose_json_bytes(header))
        paths = ("header-1.json",)
    elif attack == "missing_cik":
        rows[3]["cik"] = None
        header = json.loads((evidence / "header-3.json").read_bytes())
        header["cik"] = None
        (evidence / "header-3.json").write_bytes(ncen._purpose_json_bytes(header))
        paths = ("header-3.json",)
    else:
        header = json.loads((evidence / "header-1.json").read_bytes())
        if attack == "header_conflict":
            header["acceptance_at"] = _stage2a_timestamp(K - dt.timedelta(days=3))
        elif attack == "copy_cik_conflict":
            header["cik"] = rows[1]["cik"] = cik(3)
        else:
            header["R"] = rows[1]["R"] = (R + dt.timedelta(days=1)).isoformat()
        definition["header_artifacts"].insert(1, {
            **definition["header_artifacts"][0], "artifact_id": "header-1b", "path": "header-1b.json",
        })
        (evidence / "header-1b.json").write_bytes(ncen._purpose_json_bytes(header))
        rows[1]["header_artifact_id"] = "header-1b"
        paths = ("header-1b.json", "membership.jsonl")
    if attack not in {"date_empty", "unknown_time"}:
        (evidence / "membership.jsonl").write_bytes(ncen._purpose_jsonl_bytes(rows))
        paths = (*paths, "membership.jsonl")
    definition_path.write_bytes(ncen._purpose_json_bytes(definition))
    trust = _c1b1a_repin_definition(trust, declaration, roots, updated_paths=paths)
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


def test_c1a_file_backed_package_reaches_closed_c1b_guard(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    admitted = ncen._purpose_admit_trust(
        trust, root=tmp_path / "export", code_root=ROOT, input_roots=roots,
        declaration=declaration,
    )
    assert admitted["logical_ledger"]["digest"] == declaration["exclusion_ledger_digest"]
    pin = ncen.DiagnosticSourceManifestPin(
        roots["ncen-source"] / "manifest.json", declaration["ncen_source_manifest"]["sha256"],
        declaration["ncen_source_manifest"]["bytes"],
    )
    sources = ncen.read_diagnostic_source_rows(pin)
    assert sources.custody_lane() == "synthetic_fixture"
    assert _stage2a_index(acceptance_at=dt.datetime(2026, 3, 1, 17, tzinfo=UTC),
                          header_retrieved_at=K, retrieved_at=K, data_known_at=K).by_cik
    assert _stage2b_cohort((R,)).members
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.write_purpose_diagnostics(
            trusted_run=trust, declaration=declaration,
            output_root=tmp_path / "export", code_root=ROOT,
            input_roots=roots,
        )
    assert not (tmp_path / "export").exists()


@pytest.mark.parametrize("role", [
    "baseline_v3", "runtime_code_manifest", "ncen_dera_artifacts", "acquisition_ledger",
])
def test_c1a_missing_role_refused_before_declaration(role: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    manifest["roles"] = [item for item in manifest["roles"] if item["role"] != role]
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    repinned = ncen.DiagnosticTrustPin(trust.manifest_path, _stage2a_sha(trust.manifest_path),
                                       trust.manifest_path.stat().st_size)
    with pytest.raises(ncen.NcenError, match=f"^diagnostic_pin_role_missing:{role}$"):
        ncen._purpose_admit_trust(repinned, root=tmp_path / "export", code_root=ROOT,
                                  input_roots=roots, declaration=declaration)


def _c1a_repin(trust: ncen.DiagnosticTrustPin) -> ncen.DiagnosticTrustPin:
    return ncen.DiagnosticTrustPin(trust.manifest_path, _stage2a_sha(trust.manifest_path),
                                   trust.manifest_path.stat().st_size)


def _c1a_admit(
    pin: ncen.DiagnosticTrustPin, declaration: dict[str, object], roots: dict[str, Path],
    output: Path,
) -> dict[str, object]:
    return ncen._purpose_admit_trust(pin, root=output, code_root=ROOT,
                                     input_roots=roots, declaration=declaration)


@pytest.mark.parametrize("mutation,reason", [
    ("empty", "diagnostic_pin_role_missing:baseline_v3"),
    ("zero", "diagnostic_pin_digest_invalid"),
    ("duplicate", "diagnostic_pin_role_duplicate:baseline_v3"),
    ("unknown", "diagnostic_pin_role_unknown:invented_role"),
    ("misassigned", "diagnostic_declared_pin_mismatch"),
])
def test_c1a_role_attack_fails_closed(mutation: str, reason: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    roles = manifest["roles"]
    baseline = next(row for row in roles if row["role"] == "baseline_v3")
    if mutation == "empty":
        baseline["pins"] = []
    elif mutation == "zero":
        baseline["pins"][0]["sha256"] = "0" * 64
    elif mutation == "duplicate":
        roles.append(copy.deepcopy(baseline))
    elif mutation == "unknown":
        roles.append({"role": "invented_role", "pins": copy.deepcopy(baseline["pins"])})
    else:
        declaration["baseline_seals"][0]["sha256"] = "a" * 64
    if mutation != "misassigned":
        trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
        trust = _c1a_repin(trust)
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        ncen._purpose_admit_trust(trust, root=tmp_path / "export", code_root=ROOT,
                                  input_roots=roots, declaration=declaration)


@pytest.mark.parametrize("target,reason", [
    ("code", "diagnostic_declared_pin_mismatch"),
    ("ledger", "diagnostic_declared_ledger_mismatch"),
    ("lane", "diagnostic_lane_mismatch"),
    ("baseline", "diagnostic_baseline_seal_mismatch"),
    ("loaded_root", "diagnostic_runtime_code_mismatch"),
    ("source", "diagnostic_manifest_size_mismatch"),
    ("acquisition", "diagnostic_acquisition_sha256_mismatch:scope.json"),
])
def test_c1a_resealed_local_forgeries_cannot_change_external_truth(
    target: str, reason: str, tmp_path: Path,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    code_root = ROOT
    if target == "code":
        declaration["source_code"][0]["sha256"] = "a" * 64
    elif target == "ledger":
        declaration["exclusion_ledger_digest"] = _stage2a_sha(roots["ncen-source"] / "quarantine.json")
        assert declaration["exclusion_ledger_digest"] != json.loads(trust.manifest_path.read_bytes())["logical_ledger"]["digest"]
    elif target == "lane":
        declaration["lane"] = "sealed_source"
    elif target == "baseline":
        manifest = json.loads(trust.manifest_path.read_bytes())
        role = next(row for row in manifest["roles"] if row["role"] == "baseline_stage1")
        role["pins"][0]["sha256"] = "a" * 64
        declaration["pin_roles"] = manifest["roles"]
        declaration["baseline_seals"] = [
            {"name": row["role"], "sha256": row["pins"][0]["sha256"]}
            for row in manifest["roles"] if row["role"].startswith("baseline_")
        ]
        trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
        trust = _c1a_repin(trust)
    elif target == "loaded_root":
        code_root = tmp_path / "fake-root"
        code_root.mkdir()
    elif target == "source":
        with (roots["ncen-source"] / "manifest.json").open("ab") as handle:
            handle.write(b"forged")
    else:
        with (roots["acquisition"] / "scope.json").open("ab") as handle:
            handle.write(b"forged")
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        ncen._purpose_admit_trust(trust, root=tmp_path / "export", code_root=code_root,
                                  input_roots=roots, declaration=declaration)


def test_c1a_root_and_external_manifest_cannot_be_export_owned(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    export_root = roots["independent"].parent
    with pytest.raises(ncen.NcenError, match="^diagnostic_trust_anchor_mismatch$"):
        ncen._purpose_admit_trust(trust, root=export_root, code_root=ROOT,
                                  input_roots=roots, declaration=declaration)


def test_c1a_disk_pin_must_equal_loaded_module_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    monkeypatch.setattr(ncen, "_PURPOSE_LOADED_CODE_SHA256", "a" * 64)
    with pytest.raises(ncen.NcenError, match="^diagnostic_runtime_code_mismatch$"):
        ncen._purpose_admit_trust(trust, root=tmp_path / "export", code_root=ROOT,
                                  input_roots=roots, declaration=declaration)


def test_c1a_mutated_runtime_file_is_not_authorized_by_old_pin(tmp_path: Path) -> None:
    import shutil

    trust, declaration, roots = _c1a_package(tmp_path)
    clone = tmp_path / "changed-code"
    runtime = json.loads((roots["independent"] / "runtime.json").read_bytes())
    for item in runtime["files"]:
        destination = clone / item["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / item["path"], destination)
    with (clone / runtime["launcher"]).open("ab") as handle:
        handle.write(b"\n# changed after independent pin\n")
    with pytest.raises(ncen.NcenError, match="^diagnostic_runtime_code_mismatch$"):
        ncen._purpose_admit_trust(trust, root=tmp_path / "export", code_root=clone,
                                  input_roots=roots, declaration=declaration)


@pytest.mark.parametrize("field", ["source_code", "baseline_seals", "input_artifacts"])
def test_c1a_empty_declaration_arrays_cannot_inherit_authority(field: str, tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    declaration[field] = []
    with pytest.raises(ncen.NcenError, match="^diagnostic_declared_pin_mismatch$"):
        ncen._purpose_admit_trust(trust, root=tmp_path / "export", code_root=ROOT,
                                  input_roots=roots, declaration=declaration)


def test_c1a_misassigned_collection_rejected_against_source_inventory(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    header = next(row for row in manifest["roles"] if row["role"] == "ncen_header_artifacts")
    xml = next(row for row in manifest["roles"] if row["role"] == "ncen_xml_artifacts")
    header["pins"], xml["pins"] = xml["pins"], header["pins"]
    declaration["pin_roles"] = manifest["roles"]
    declaration["input_artifacts"] = sorted(
        declaration["input_artifacts"], key=lambda item: (item["root_id"], item["path"])
    )
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_source_checkpoint_mismatch$"):
        ncen._purpose_admit_trust(_c1a_repin(trust), root=tmp_path / "export",
                                  code_root=ROOT, input_roots=roots, declaration=declaration)


def test_c1a_synthetic_package_cannot_be_relabelled_real(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    manifest["lane"] = declaration["lane"] = "sealed_source"
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_required_pins_unverified$"):
        ncen._purpose_admit_trust(_c1a_repin(trust), root=tmp_path / "export",
                                  code_root=ROOT, input_roots=roots, declaration=declaration)


def test_c1a_validator_cannot_complete_synthetic_v2_envelope(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    export = tmp_path / "export"
    export.mkdir()
    (export / "declaration.json").write_bytes(ncen._purpose_json_bytes(declaration))
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.validate_purpose_export(export, trusted_run=trust, code_root=ROOT,
                                     input_roots=roots)
    (export / "manifest.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_purpose_diagnostics_manifest_v1",
    }))
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_schema_unsupported$"):
        ncen.validate_purpose_export(export, trusted_run=trust, code_root=ROOT,
                                     input_roots=roots)


def test_c1a_old_envelope_validation_never_upgrades_v1(tmp_path: Path) -> None:
    export = tmp_path / "old-envelope"
    export.mkdir()
    (export / "declaration.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_purpose_diagnostics_declaration_v1",
    }))
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_schema_unsupported$"):
        ncen.validate_purpose_export(export, trusted_run=None, code_root=ROOT, input_roots={})


def test_c1a_four_seed_external_fixture_is_identical(tmp_path: Path) -> None:
    pin, declaration, roots = _c1a_package(tmp_path / "shared")
    config_path = tmp_path / "trusted-run.json"
    config_path.write_bytes(ncen._purpose_json_bytes({
        "pin": {"path": str(pin.manifest_path), "sha256": pin.manifest_sha256,
                "bytes": pin.manifest_size},
        "declaration": declaration,
        "roots": {key: str(value) for key, value in roots.items()},
    }))
    runner = r'''
import importlib
import json
import sys
from pathlib import Path
from src.bonds.default_events import ncen

cfg = json.loads(Path(sys.argv[1]).read_bytes())
sys.path.insert(0, str(Path(sys.argv[3]) / "tests"))
module = importlib.import_module("test_bond_default_ncen_purposes")
pin = ncen.DiagnosticTrustPin(Path(cfg["pin"]["path"]), cfg["pin"]["sha256"],
                              cfg["pin"]["bytes"])
declaration = cfg["declaration"]
roots = {key: Path(value) for key, value in cfg["roots"].items()}
admitted = module._c1a_admit(pin, declaration, roots, Path(sys.argv[2]))
baseline = admitted["candidate_snapshot"]
print(json.dumps({"manifest": pin.manifest_sha256,
                  "roles": [item["role"] for item in declaration["pin_roles"]],
                  "ledger": declaration["exclusion_ledger_digest"],
                  "baseline": baseline["input_identities"],
                  "index": [item.accession_number for item in baseline["index_entries"]],
                  "copies": [item.projection_digest for item in baseline["filings"]]}, sort_keys=True))
'''
    results = []
    for seed in (1, 2, 977, 31337):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = str(seed)
        completed = subprocess.run(
            [sys.executable, "-c", runner, str(config_path), str(tmp_path / f"seed-{seed}"), str(ROOT)],
            cwd=ROOT, env=env, check=False, capture_output=True, timeout=120,
        )
        assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
        results.append(json.loads(completed.stdout))
    assert results == [results[0]] * 4


def _q1a_raw_fixture(tmp_path: Path) -> tuple[Path, dict[str, tuple[dict[str, object], ...]]]:
    root = tmp_path / "raw"
    root.mkdir()
    entry = f"N-CEN  Fixture Fund  {cik(1)}  2026-03-01  edgar/data/1/{ACCESSION}.txt\n"
    (root / "form.idx").write_bytes(
        ("Form Type  Company Name  CIK  Date Filed  File Name\n" + "-" * 60 + "\n"
         + entry + entry + "\nmalformed row\n"
         + entry.replace("2026-03-01", "2026-99-99")).encode("latin-1")
    )
    with zipfile.ZipFile(root / "dera.zip", "w") as archive:
        archive.writestr("quarter/SUBMISSION.tsv", (
            b"ACCESSION_NUMBER\tCIK\tFILING_DATE\tREPORT_ENDING_PERIOD\tSUBMISSION_TYPE\n"
            b"valid\t1\t01-MAR-2026\t31-DEC-2025\tN-CEN\n\nmalformed\n"))
        archive.writestr("quarter/auxiliary.txt", b"not consumed")
    (root / "valid.xml").write_bytes(
        f'<edgarSubmission xmlns="{ncen.NCEN_NAMESPACE}"/>'.encode()
    )
    (root / "invalid.xml").write_bytes(b"<edgarSubmission>")
    (root / "header.txt").write_bytes(b"<SEC-HEADER>\n</SEC-HEADER>\n")
    (root / "scope.json").write_bytes(ncen._purpose_json_bytes({
        "schema": "synthetic", "requests": [{"request_id": "one"}],
    }))
    (root / "terminal.json").write_bytes(ncen._purpose_json_bytes({}))
    (root / "SHA256SUMS").write_bytes(
        f'{_stage2a_sha(root / "scope.json")}  scope.json\n'
        f'{_stage2a_sha(root / "terminal.json")}  terminal.json\n'.encode()
    )

    def pin(path: str) -> dict[str, object]:
        file = root / path
        return {"root_id": "raw", "path": path, "sha256": _stage2a_sha(file),
                "bytes": file.stat().st_size}

    (root / "index.json").write_bytes(ncen._purpose_json_bytes({
        "schema_version": "ncen_diagnostic_index_manifest_v1",
        "artifacts": [{"pin": pin("form.idx"), "retrieved_at": _stage2a_timestamp(K)}],
    }))
    manifest = {
        "schema_version": "ncen_diagnostic_raw_audit_manifest_v1",
        "lane": "synthetic_fixture", "purpose": "complete_ncen_candidate_audit",
        "index_manifest_pin": pin("index.json"),
        "acquisition_scope_pin": pin("scope.json"),
        "acquisition_sha256sums_pin": pin("SHA256SUMS"),
        "parser_code_manifest_sha256": "a" * 64,
        "artifacts": [
            {"artifact_id": name, "kind": kind, **pin(path),
             "retrieved_at": _stage2a_timestamp(K) if kind == "header" else None,
             "public_at": None, "data_known_at": None,
             "package_label": "synthetic-quarter" if kind == "dera_zip" else None,
             "accession_claim": ACCESSION if kind == "header" else None,
             "cik_claim": None, "source_url": "https://www.sec.gov/Archives/fixture.txt"
             if kind == "header" else None,
             "header_artifact_ids": [], "acquisition_request_locators": []}
            for name, kind, path in (
                ("a-dera", "dera_zip", "dera.zip"),
                ("b-valid", "edgar_xml", "valid.xml"),
                ("c-invalid", "edgar_xml", "invalid.xml"),
                ("d-header", "header", "header.txt"),
            )
        ],
    }
    (root / "raw.json").write_bytes(ncen._purpose_json_bytes(manifest))
    roles = {
        "ncen_raw_audit_manifest": (pin("raw.json"),),
        "ncen_index_manifest": (pin("index.json"),),
        "ncen_index_artifacts": (pin("form.idx"),),
        "acquisition_scope": (pin("scope.json"),),
        "acquisition_sha256sums": (pin("SHA256SUMS"),),
        "runtime_code_manifest": ({"sha256": "a" * 64},),
    }
    return root, roles


def test_c1b1b_q1a_physical_audit_counts_every_raw_unit(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    rows = ncen._purpose_enumerate_raw_inputs({"raw": root}, roles, {"artifacts": []})
    assert len(rows) == 20
    assert rows == ncen._purpose_enumerate_raw_inputs({"raw": root}, roles, {"artifacts": []})
    assert len({row["physical_input_id"] for row in rows}) == len(rows)
    assert [row["locator"] for row in rows if row["artifact_id"] == "index:raw:form.idx"] == [
        "form.idx:1", "form.idx:2", "form.idx:3", "form.idx:4", "form.idx:5", "form.idx:6",
        "form.idx:7",
    ]
    duplicate_logical = [row for row in rows if row["locator"] in ("form.idx:3", "form.idx:4")]
    assert len(duplicate_logical) == 2
    assert duplicate_logical[0]["raw_unit_sha256"] == duplicate_logical[1]["raw_unit_sha256"]
    assert duplicate_logical[0]["physical_input_id"] != duplicate_logical[1]["physical_input_id"]
    assert next(row for row in rows if row["locator"] == "form.idx:6")["reason_codes"] == [
        "index_line_malformed",
    ]
    assert next(row for row in rows if row["locator"] == "form.idx:7")["reason_codes"] == [
        "index_line_malformed",
    ]
    assert all(set(row) == {"schema_version", "physical_input_id", "root_id", "artifact_id",
                            "container_member", "locator_kind", "locator", "raw_unit_sha256",
                            "unit_kind", "disposition", "reason_codes", "observation_ids", "ledger_locators"}
               for row in rows)
    assert any(row["locator"] == "quarter/SUBMISSION.tsv:3" and row["disposition"] == "parser_quarantine"
               for row in rows)
    assert any(row["locator"] == "quarter/SUBMISSION.tsv:4" and row["disposition"] == "parser_quarantine"
               for row in rows)
    assert any(row["artifact_id"] == "c-invalid" and row["disposition"] == "parser_quarantine"
               for row in rows)
    assert next(row for row in rows if row["artifact_id"] == "d-header")["reason_codes"] == [
        "header_unparseable",
    ]
    assert any(row["locator"] == "quarter/auxiliary.txt" and row["disposition"] == "out_of_scope_content"
               for row in rows)
    assert {row["locator"] for row in rows if row["artifact_id"] == "acquisition:scope.json"} == {
        "/requests/0", "/schema",
    }
    assert {row["locator"] for row in rows if row["artifact_id"] == "acquisition:SHA256SUMS"} == {
        "SHA256SUMS:1", "SHA256SUMS:2",
    }
    assert [row["locator"] for row in rows if row["artifact_id"] == "acquisition:terminal.json"] == [""]


@pytest.mark.parametrize("attack,reason", [
    ("missing", "diagnostic_input_unavailable"),
    ("bytes", "diagnostic_input_bytes_mismatch"),
    ("duplicate_json_key", "diagnostic_pinned_input_invalid"),
    ("noncanonical_json", "diagnostic_pinned_input_invalid"),
    ("deep_json", "diagnostic_pinned_input_invalid"),
    ("wrong_scalar", "diagnostic_pinned_input_invalid"),
    ("orphan_request", "diagnostic_pinned_input_invalid"),
    ("unsafe", "diagnostic_artifact_path_unsafe"),
])
def test_c1b1b_q1a_raw_audit_refusals(attack: str, reason: str, tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    if attack == "missing":
        (root / "invalid.xml").unlink()
    elif attack == "bytes":
        (root / "invalid.xml").write_bytes(b"changed")
    elif attack in {"duplicate_json_key", "noncanonical_json", "wrong_scalar", "deep_json", "orphan_request"}:
        path = root / "raw.json"
        value = json.loads(path.read_bytes())
        if attack == "wrong_scalar":
            value["artifacts"][0]["kind"] = []
            path.write_bytes(ncen._purpose_json_bytes(value))
        elif attack == "orphan_request":
            value["artifacts"][0]["acquisition_request_locators"] = ["/requests/99"]
            path.write_bytes(ncen._purpose_json_bytes(value))
        elif attack == "noncanonical_json":
            path.write_bytes(json.dumps(value, indent=2).encode())
        elif attack == "deep_json":
            path.write_bytes(b"[" * 1100 + b"0" + b"]" * 1100)
        else:
            path.write_bytes(path.read_bytes().replace(b'"lane":"synthetic_fixture"',
                                                  b'"lane":"synthetic_fixture","lane":"synthetic_fixture"'))
        roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                             "bytes": path.stat().st_size,
                                             "sha256": _stage2a_sha(path)},)
    else:
        path = root / "raw.json"
        value = json.loads(path.read_bytes())
        value["artifacts"][0]["path"] = "../dera.zip"
        path.write_bytes(ncen._purpose_json_bytes(value))
        roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                             "bytes": path.stat().st_size,
                                             "sha256": _stage2a_sha(path)},)
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        ncen._purpose_enumerate_raw_inputs({"raw": root}, roles, {"artifacts": []})


def test_c1b1b_q1a_old_trust_and_checkpoint_versions_refuse(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    trust_path = trust.manifest_path
    manifest = json.loads(trust_path.read_bytes())
    manifest["schema_version"] = "ncen_diagnostic_trust_manifest_v2"
    trust_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_pin_digest_invalid$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")

    manifest["schema_version"] = "ncen_diagnostic_trust_manifest_v3"
    checkpoint_path = roots["independent"] / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_bytes())
    checkpoint["schema_version"] = "ncen_diagnostic_baseline_checkpoint_v1"
    checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
    role = next(item for item in manifest["roles"] if item["role"] == "diagnostic_baseline_checkpoint")
    role["pins"][0].update(sha256=_stage2a_sha(checkpoint_path), bytes=checkpoint_path.stat().st_size)
    declaration["pin_roles"] = manifest["roles"]
    for item in declaration["input_artifacts"]:
        if item["path"] == "checkpoint.json":
            item.update(sha256=_stage2a_sha(checkpoint_path), bytes=checkpoint_path.stat().st_size)
    trust_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_checkpoint_schema_unsupported$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


def test_c1b1b_q1a_fixed_pin_rejects_locally_resealed_inventory(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    inventory_path = roots["independent"] / "raw_input_inventory.jsonl"
    rows = [json.loads(line) for line in inventory_path.read_bytes().splitlines()]
    rows.pop()
    inventory_path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    checkpoint_path = roots["independent"] / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_bytes())
    descriptor = next(item for item in checkpoint["files"] if item["path"] == inventory_path.name)
    descriptor.update(sha256=_stage2a_sha(inventory_path), bytes=inventory_path.stat().st_size,
                      rows=len(rows))
    checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
    with pytest.raises(ncen.NcenError, match="^diagnostic_checkpoint_pin_mismatch$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


def test_c1b1b_q1a_duplicate_physical_claim_refuses_under_new_authority(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    evidence = roots["independent"]
    inventory_path = evidence / "raw_input_inventory.jsonl"
    rows = [json.loads(line) for line in inventory_path.read_bytes().splitlines()]
    rows.insert(1, dict(rows[0]))
    inventory_path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    checkpoint_path = evidence / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_bytes())
    descriptor = next(item for item in checkpoint["files"] if item["path"] == inventory_path.name)
    descriptor.update(sha256=_stage2a_sha(inventory_path), bytes=inventory_path.stat().st_size,
                      rows=len(rows))
    checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
    manifest = json.loads(trust.manifest_path.read_bytes())
    role = next(item for item in manifest["roles"] if item["role"] == "diagnostic_baseline_checkpoint")
    role["pins"][0].update(sha256=_stage2a_sha(checkpoint_path), bytes=checkpoint_path.stat().st_size)
    declaration["pin_roles"] = manifest["roles"]
    for item in declaration["input_artifacts"]:
        if item["path"] == "checkpoint.json":
            item.update(sha256=_stage2a_sha(checkpoint_path), bytes=checkpoint_path.stat().st_size)
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    with pytest.raises(ncen.NcenError, match="^diagnostic_raw_physical_duplicate$"):
        _c1a_admit(_c1a_repin(trust), declaration, roots, tmp_path / "export")


def test_c1b1b_q1a_f13_line_limit_refuses_without_truncation(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    monitor = ncen.DiagnosticResourceMonitor(
        ncen.DiagnosticResourceLimits(tsv_line_max_bytes=16), rss_probe=lambda: 0,
    )
    with pytest.raises(ncen.NcenError, match="^diagnostic_index_line_oversized:1$"):
        ncen._purpose_enumerate_raw_inputs({"raw": root}, roles, {"artifacts": []}, monitor=monitor)


def test_c1b1b_q1a_zip_directory_with_bytes_refuses(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    with zipfile.ZipFile(root / "dera.zip", "a") as archive:
        archive.writestr(zipfile.ZipInfo("payload/"), b"hidden bytes")
    manifest_path = root / "raw.json"
    manifest = json.loads(manifest_path.read_bytes())
    dera = next(item for item in manifest["artifacts"] if item["artifact_id"] == "a-dera")
    dera.update(sha256=_stage2a_sha(root / "dera.zip"), bytes=(root / "dera.zip").stat().st_size)
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "bytes": manifest_path.stat().st_size,
                                         "sha256": _stage2a_sha(manifest_path)},)
    with pytest.raises(ncen.ZipSafetyError, match="^zip_directory_nonempty:payload/$"):
        ncen._purpose_enumerate_raw_inputs({"raw": root}, roles, {"artifacts": []})


def test_c1b1b_q1a_physical_line_order_is_numeric(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    index_path = root / "form.idx"
    index_path.write_bytes(index_path.read_bytes() + b"\n" * 10)
    index_manifest_path = root / "index.json"
    index_manifest = json.loads(index_manifest_path.read_bytes())
    index_manifest["artifacts"][0]["pin"].update(
        sha256=_stage2a_sha(index_path), bytes=index_path.stat().st_size,
    )
    index_manifest_path.write_bytes(ncen._purpose_json_bytes(index_manifest))
    raw_manifest_path = root / "raw.json"
    raw_manifest = json.loads(raw_manifest_path.read_bytes())
    raw_manifest["index_manifest_pin"].update(
        sha256=_stage2a_sha(index_manifest_path), bytes=index_manifest_path.stat().st_size,
    )
    raw_manifest_path.write_bytes(ncen._purpose_json_bytes(raw_manifest))
    roles["ncen_index_artifacts"] = (index_manifest["artifacts"][0]["pin"],)
    roles["ncen_index_manifest"] = (raw_manifest["index_manifest_pin"],)
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_manifest_path),
                                         "bytes": raw_manifest_path.stat().st_size},)
    rows = ncen._purpose_enumerate_raw_inputs({"raw": root}, roles, {"artifacts": []})
    assert [row["locator"] for row in rows if row["artifact_id"] == "index:raw:form.idx"] == [
        f"form.idx:{number}" for number in range(1, 18)
    ]


def test_c1b1b_q1a_f6_controls_use_acquisition_canonical_encoding() -> None:
    control = _acq_json({"label": "fa\u00e7ade"})
    assert ncen._purpose_raw_control(control, acquisition=True) == {"label": "fa\u00e7ade"}
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        ncen._purpose_raw_control(control)


def test_c1b1b_q1b_observes_every_physical_unit_without_admission(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    report = ncen._purpose_q1_observations({"raw": root}, roles, {"artifacts": []})
    assert len(report["raw_input_inventory"]) == 20
    assert tuple(len(report[name]) for name in (
        "index_observations", "copy_observations", "header_observations",
    )) == (7, 5, 1)
    assert len([row for row in report["copy_observations"] if row["parse_state"] == "index_only"]) == 2
    assert report["reconciliation"]["schema_version"] == "ncen_diagnostic_source_reconciliation_v2"
    assert report["reconciliation"]["f1_subset_claim"]["verification"] == "unverified_q2"
    assert report["reconciliation"]["acquisition_requests"][0]["refusal_reason"] == (
        "acquisition_request_accession_number_missing"
    )
    assert len(report["reconciliation"]["physical_inputs"]) == 20
    assert len(report["reconciliation"]["parser_quarantine"]) == 9
    assert len([row for row in report["reconciliation"]["parser_quarantine"]
                if "observation_id" in row]) == 3
    assert all(set(row["observation_ids"]) <= {
        item["observation_id"] for family in ("index_observations", "copy_observations", "header_observations")
        for item in report[family]
    } for row in report["raw_input_inventory"])
    entries = [row for row in report["index_observations"] if row["entry"] is not None]
    duplicate = [row for row in entries if row["locator"] in ("form.idx:3", "form.idx:4")]
    assert len(duplicate) == 2
    assert duplicate[0]["observation_id"] != duplicate[1]["observation_id"]
    assert duplicate[0]["equivalence_group_id"] == duplicate[1]["equivalence_group_id"]
    assert {row["locator"] for row in report["index_observations"]} == {
        f"form.idx:{line}" for line in range(1, 8)
    }
    assert {row["parse_disposition"] for row in report["index_observations"]} >= {
        "parsed", "parser_quarantine", "structural_metadata",
    }
    assert any(row["parse_state"] == "unparseable" and row["filing"] is None
               and row["merge_input_kind"] == "baseline_refusal"
               for row in report["copy_observations"])
    assert report == ncen._purpose_q1_observations({"raw": root}, roles, {"artifacts": []})


def test_c1b1b_q1b_retains_actual_quarantined_xml_partial(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    manifest_path = root / "raw.json"
    manifest = json.loads(manifest_path.read_bytes())
    artifact = next(row for row in manifest["artifacts"] if row["artifact_id"] == "c-invalid")
    artifact.update(accession_claim=ACCESSION, source_url="https://www.sec.gov/Archives/fixture.xml",
                    retrieved_at=_stage2a_timestamp(K))
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(manifest_path),
                                         "bytes": manifest_path.stat().st_size},)
    report = ncen._purpose_q1_observations({"raw": root}, roles, {"artifacts": []})
    copy = next(row for row in report["copy_observations"] if row["artifact_id"] == "c-invalid")
    actual = ncen.parse_ncen_primary_doc(
        (root / "invalid.xml").read_bytes(), accession_number=ACCESSION,
        source_url=artifact["source_url"], retrieved_at=K,
    )
    assert copy["parse_state"] == "quarantined_partial"
    assert copy["filing"] == ncen._purpose_baseline_filing(actual)
    assert copy["filing"]["funds"] == []
    assert copy["admission_disposition"] == "audit_only"


@pytest.mark.parametrize("attack,reason", [
    ("drop_physical", "diagnostic_raw_membership_mismatch"),
    ("repeat_physical", "diagnostic_raw_physical_duplicate"),
    ("change_disposition", "diagnostic_quarantine_disposition_mismatch"),
    ("drop_f6", "diagnostic_raw_membership_mismatch"),
    ("drop_request", "diagnostic_raw_membership_mismatch"),
    ("fabricate_filing", "diagnostic_quarantine_disposition_mismatch"),
    ("null_filing", "diagnostic_quarantine_disposition_mismatch"),
    ("numeric_substitution", "diagnostic_quarantine_disposition_mismatch"),
    ("unknown_copy_key", "diagnostic_pinned_input_invalid"),
    ("orphan_observation", "diagnostic_raw_membership_mismatch"),
])
def test_c1b1b_q1b_exact_reconciliation_refuses_reanchored_claims(
    tmp_path: Path, attack: str, reason: str,
) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    report = ncen._purpose_q1_observations(roots, roles, source_manifest)
    assert tuple(len(report[name]) for name in (
        "raw_input_inventory", "index_observations", "copy_observations", "header_observations",
    )) == (179, 8, 7, 1)
    assert any(row["parse_state"] == "index_only" and row["filing"] is None
               and "0000000001-26-000002" in row["accession_claims"]
               for row in report["copy_observations"])
    claimed = json.loads(ncen._purpose_json_bytes(report))
    if attack == "drop_physical":
        claimed["raw_input_inventory"].pop()
    elif attack == "repeat_physical":
        claimed["raw_input_inventory"].insert(1, dict(claimed["raw_input_inventory"][0]))
    elif attack == "change_disposition":
        next(row for row in claimed["raw_input_inventory"] if row["disposition"] == "parsed_evidence")[
            "disposition"] = "parser_quarantine"
    elif attack == "drop_f6":
        claimed["reconciliation"]["f6_exclusions"].pop()
    elif attack == "drop_request":
        claimed["reconciliation"]["acquisition_requests"].clear()
    elif attack == "fabricate_filing":
        claimed["copy_observations"][0]["filing"] = {"status": "parsed"}
    elif attack == "null_filing":
        next(row for row in claimed["copy_observations"] if row["filing"] is not None)["filing"] = None
    elif attack == "numeric_substitution":
        ref = claimed["reconciliation"]["f6_exclusions"][0]["terminal"]
        ref["bytes"] = float(ref["bytes"])
    elif attack == "unknown_copy_key":
        claimed["copy_observations"][0]["outcome_label"] = "invented"
    else:
        claimed["raw_input_inventory"][0]["observation_ids"] = ["unknown"]
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        ncen._purpose_q1_verify_observations(
            roots, roles, source_manifest, ncen._purpose_json_bytes(claimed),
            trusted_run=trust,
        )


def test_c1b1b_q1b_fixed_external_trust_rejects_resealed_raw_role(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    claimed = ncen._purpose_json_bytes(ncen._purpose_q1_observations(
        roots, roles, source_manifest,
    ))
    raw_path = roots["independent"] / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    raw["artifacts"][0]["source_url"] = "https://www.sec.gov/Archives/changed.txt"
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    with pytest.raises(ncen.NcenError, match="^diagnostic_declared_pin_mismatch$"):
        ncen._purpose_q1_verify_observations(
            roots, roles, source_manifest, claimed, trusted_run=trust,
        )


def test_c1b1b_q1b_valid_f6_receipt_and_copy_witnesses(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    report = ncen._purpose_q1_observations(roots, roles, source_manifest)
    assert len(report["reconciliation"]["f6_exclusions"]) == 3
    assert len(report["reconciliation"]["f6_boundaries"]) == 1
    assert any(row["f6_exclusion_locators"] for row in report["reconciliation"]["physical_inputs"])
    assert {row["disposition"] for row in report["reconciliation"]["acquisition_requests"]} == {
        "excluded", "boundary", "verified",
    }
    assert all(row["refusal_reason"] is None for row in report["reconciliation"]["acquisition_requests"])
    assert any(row["source_kind"] == "dera_zip" and row["filing"] is not None
               for row in report["copy_observations"])
    assert any(row["source_kind"] == "edgar_xml" and row["filing"] is not None
               for row in report["copy_observations"])
    assert all(row["admission_disposition"] == (
        "f1_admitted" if row["parse_state"] == "parsed"
        and row["artifact_id"] in report["reconciliation"]["f1_subset_claim"]["artifact_ids"]
        else "audit_only"
    ) for row in report["copy_observations"])
    assert any(row["header"] is not None for row in report["header_observations"])


def test_c1b1b_q1b_unverified_scope_cannot_consume_supplied_f6_ledger(tmp_path: Path) -> None:
    root, roles = _q1a_raw_fixture(tmp_path)
    seal = _acquisition_seal(tmp_path / "other-acquisition")
    ledger = ncen.read_diagnostic_acquisition_ledger(seal.pin)
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        ncen._purpose_q1_observations({"raw": root}, roles, {"artifacts": []}, acquisition_ledger=ledger)


def test_c1b1b_q1b_unexpected_parser_bug_is_not_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected_programming_error")

    monkeypatch.setattr(ncen, "parse_ncen_primary_doc", fail)
    with pytest.raises(RuntimeError, match="^injected_programming_error$"):
        ncen._purpose_q1_observations(roots, roles, source_manifest)


def test_c1b1b_q1b_reanchored_malformed_dera_row_keeps_whole_zip_audit_only(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source = roots["ncen-source"]
    source_manifest_path = source / roles["ncen_source_manifest"][0]["path"]
    source_manifest = json.loads(source_manifest_path.read_bytes())
    dera = next(row for row in source_manifest["artifacts"] if row["kind"] == "dera_zip")
    zip_path = source / dera["path"]
    with zipfile.ZipFile(zip_path) as archive:
        members = [(info.filename, archive.read(info)) for info in archive.infolist()]
    with zipfile.ZipFile(zip_path, "w") as archive:
        for name, data in members:
            archive.writestr(name, data + b"malformed\n" if name.endswith("SUBMISSION.tsv") else data)
    dera.update(sha256=_stage2a_sha(zip_path), bytes=zip_path.stat().st_size)
    source_manifest_path.write_bytes(ncen._purpose_json_bytes(source_manifest))
    roles["ncen_source_manifest"] = ({**roles["ncen_source_manifest"][0],
                                       "sha256": _stage2a_sha(source_manifest_path),
                                       "bytes": source_manifest_path.stat().st_size},)
    roles["ncen_dera_artifacts"] = ({**roles["ncen_dera_artifacts"][0],
                                     "sha256": dera["sha256"], "bytes": dera["bytes"]},)
    raw_path = roots["independent"] / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    audit_dera = next(row for row in raw["artifacts"] if row["artifact_id"] == dera["artifact_id"])
    audit_dera.update(sha256=dera["sha256"], bytes=dera["bytes"])
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    report = ncen._purpose_q1_observations(roots, roles, source_manifest,
                                           verify_f1_subset=False)
    dera_copies = [row for row in report["copy_observations"] if row["source_kind"] == "dera_zip"]
    assert len(dera_copies) == 1
    assert dera_copies[0]["filing"] is None
    assert dera_copies[0]["parse_state"] == "unparseable"
    assert dera_copies[0]["merge_input_kind"] == "baseline_refusal"
    assert dera_copies[0]["admission_disposition"] == "audit_only"
    assert any("row_width_mismatch" in reason for reason in dera_copies[0]["parser_reasons"])
    assert any(row["reason_codes"] == ["tsv_row_width_mismatch"]
               for row in report["raw_input_inventory"])
    with pytest.raises(ncen.NcenError, match="^diagnostic_admission_subset_mismatch$"):
        ncen._purpose_q1_observations(roots, roles, source_manifest)


def test_c1b1b_q1b_reanchored_parsed_later_competitor_is_audit_only(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    evidence = roots["independent"]
    source = roots["ncen-source"]
    accession = "0000000001-26-000006"
    (source / "later.xml").write_bytes((source / "primary_doc.xml").read_bytes())
    raw_path = evidence / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    original = next(row for row in raw["artifacts"] if row["kind"] == "edgar_xml")
    new = {**original, "artifact_id": "z-later", "path": "later.xml",
           "sha256": _stage2a_sha(source / "later.xml"), "bytes": (source / "later.xml").stat().st_size,
           "accession_claim": accession, "header_artifact_ids": []}
    raw["artifacts"].append(new)
    raw["artifacts"].sort(key=lambda row: row["artifact_id"])
    index_path = evidence / "form.idx"
    index_path.write_bytes(index_path.read_bytes() +
                           f"N-CEN/A  Fixture Fund  {cik(1)}  2026-04-01  "
                           f"edgar/data/1/{accession}.txt\n".encode("latin-1"))
    index_manifest_path = evidence / "index-manifest.json"
    index_manifest = json.loads(index_manifest_path.read_bytes())
    index_manifest["artifacts"][0]["pin"].update(
        sha256=_stage2a_sha(index_path), bytes=index_path.stat().st_size,
    )
    index_manifest_path.write_bytes(ncen._purpose_json_bytes(index_manifest))
    roles["ncen_index_artifacts"] = (index_manifest["artifacts"][0]["pin"],)
    roles["ncen_index_manifest"] = ({**roles["ncen_index_manifest"][0],
                                     "sha256": _stage2a_sha(index_manifest_path),
                                     "bytes": index_manifest_path.stat().st_size},)
    raw["index_manifest_pin"] = roles["ncen_index_manifest"][0]
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    report = ncen._purpose_q1_observations(roots, roles, source_manifest,
                                           verify_f1_subset=False)
    competitor = next(row for row in report["copy_observations"] if row["artifact_id"] == "z-later")
    assert competitor["parse_state"] == "parsed"
    assert competitor["filing"]["accession_number"] == accession
    assert competitor["admission_disposition"] == "audit_only"
    assert report["reconciliation"]["f1_subset_claim"]["verification"] == "unverified_q2"
    with pytest.raises(ncen.NcenError, match="^diagnostic_header_binding_missing$"):
        ncen._purpose_q1_observations(roots, roles, source_manifest)


@pytest.mark.parametrize("conflicting", [False, True])
def test_c1b1b_q1b_reanchored_header_duplicates(
    tmp_path: Path, conflicting: bool,
) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    source = roots["ncen-source"]
    raw_path = roots["independent"] / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    original = next(row for row in raw["artifacts"] if row["kind"] == "header")
    old = (source / original["path"]).read_bytes()
    changed = (old.replace(b"<ACCEPTANCE-DATETIME>20260301120000",
                           b"<ACCEPTANCE-DATETIME>20260301120001") if conflicting else old)
    assert (changed != old) == conflicting
    (source / "conflicting-header.txt").write_bytes(changed)
    duplicate = {**original, "artifact_id": "z-conflicting-header", "path": "conflicting-header.txt",
                 "sha256": _stage2a_sha(source / "conflicting-header.txt"), "bytes": len(changed)}
    raw["artifacts"].append(duplicate)
    raw["artifacts"].sort(key=lambda row: row["artifact_id"])
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    if conflicting:
        with pytest.raises(ncen.NcenError, match="^diagnostic_header_conflict$"):
            ncen._purpose_q1_observations(roots, roles, source_manifest)
    else:
        report = ncen._purpose_q1_observations(roots, roles, source_manifest,
                                               verify_f1_subset=False)
        headers = [row for row in report["header_observations"] if row["header"] is not None]
        assert len(headers) == 2
        assert headers[0]["observation_id"] != headers[1]["observation_id"]
        assert headers[0]["representative_observation_id"] == headers[1]["representative_observation_id"]
        assert headers[0]["representative_observation_id"] == min(
            row["observation_id"] for row in headers
        )
        verified = ncen._purpose_q2_candidates(roots, roles, source_manifest,
                                               cohort_ciks={cik(1), cik(2)})
        assert len(verified["headers"]) == 1
        assert len([row for row in verified["audit"]["header_observations"]
                    if row["header"] is not None]) == 2
        assert verified["audit"]["reconciliation"]["f1_subset_claim"]["verification"] == "verified_q2a"


def test_review_q2a_f1_maximal_subset(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source = roots["ncen-source"]
    source_path = source / roles["ncen_source_manifest"][0]["path"]
    raw_path = roots["independent"] / "raw-audit.json"
    manifest = json.loads(source_path.read_bytes())
    audit = json.loads(raw_path.read_bytes())
    original = next(row for row in manifest["artifacts"] if row["kind"] == "dera_zip")
    original_raw = next(row for row in audit["artifacts"] if row["artifact_id"] == original["artifact_id"])

    def repin() -> None:
        source_path.write_bytes(ncen._purpose_json_bytes(manifest))
        raw_path.write_bytes(ncen._purpose_json_bytes(audit))
        for role, path in (("ncen_source_manifest", source_path),
                           ("ncen_raw_audit_manifest", raw_path)):
            roles[role] = ({**roles[role][0], "sha256": _stage2a_sha(path),
                            "bytes": path.stat().st_size},)
        roles["ncen_dera_artifacts"] = tuple(sorted(({
            "root_id": "ncen-source", "path": row["path"], "sha256": row["sha256"],
            "bytes": row["bytes"],
        } for row in manifest["artifacts"] if row["kind"] == "dera_zip"),
            key=lambda row: row["path"]))

    initial = ncen._purpose_q1_observations(roots, roles, manifest)
    assert initial["reconciliation"]["f1_subset_claim"] == {
        "artifact_ids": sorted(row["artifact_id"] for row in manifest["artifacts"]),
        "verification": "verified_q2a",
    }
    assert len(initial["raw_input_inventory"]) == 179
    assert (len(initial["index_observations"]), len(initial["copy_observations"]),
            len(initial["header_observations"])) == (8, 7, 1)
    assert len({row["observation_id"] for row in initial["index_observations"]}) == 8

    # The clean copy is an independent original ZIP; the mixed ZIP keeps one good
    # parser result and one quarantined partial, but contributes no F1 source row.
    clean = {**original, "artifact_id": "dera-clean", "path": "clean.zip"}
    (source / clean["path"]).write_bytes((source / original["path"]).read_bytes())
    with zipfile.ZipFile(source / original["path"]) as archive:
        members = [(item.filename, archive.read(item)) for item in archive.infolist()]
    with zipfile.ZipFile(source / original["path"], "w") as archive:
        for name, data in members:
            if name.endswith("SUBMISSION.tsv"):
                header, example = data.splitlines()[:2]
                fields = example.split(b"\t")
                columns = header.split(b"\t")
                fields[columns.index(b"ACCESSION_NUMBER")] = b"0000000001-26-000099"
                fields[columns.index(b"REPORT_ENDING_PERIOD")] = b"not-a-date"
                data += b"\t".join(fields) + b"\n"
            archive.writestr(name, data)
    original_raw.update(sha256=_stage2a_sha(source / original["path"]),
                        bytes=(source / original["path"]).stat().st_size)
    audit["artifacts"].append({**original_raw, "artifact_id": clean["artifact_id"],
                               "path": clean["path"], "sha256": clean["sha256"],
                               "bytes": clean["bytes"]})
    audit["artifacts"].sort(key=lambda row: row["artifact_id"])
    manifest["artifacts"] = [clean if row["artifact_id"] == original["artifact_id"] else row
                             for row in manifest["artifacts"]]
    repin()
    report = ncen._purpose_q1_observations(roots, roles, manifest)
    assert "dera-clean" in report["reconciliation"]["f1_subset_claim"]["artifact_ids"]
    assert original["artifact_id"] not in report["reconciliation"]["f1_subset_claim"]["artifact_ids"]
    mixed_copies = [row for row in report["copy_observations"]
                    if row["artifact_id"] == original["artifact_id"]]
    assert {row["parse_state"] for row in mixed_copies} == {"parsed", "quarantined_partial"}
    assert any(row["filing"]["accession_number"] == ACCESSION and row["parse_state"] == "parsed"
               for row in mixed_copies)
    assert any("report_period_unparseable" in row["parser_reasons"] for row in mixed_copies)
    assert all(row["admission_disposition"] == "audit_only" for row in mixed_copies)

    eligible = list(manifest["artifacts"])
    mixed = {**original, "sha256": original_raw["sha256"], "bytes": original_raw["bytes"]}
    for altered in (
        [row for row in eligible if row["artifact_id"] != "dera-clean"],
        [*eligible, mixed],
        [mixed, *(row for row in eligible if row["artifact_id"] != "dera-clean")],
    ):
        manifest["artifacts"] = altered
        repin()
        with pytest.raises(ncen.NcenError, match="^diagnostic_admission_subset_mismatch$"):
            ncen._purpose_q1_observations(roots, roles, manifest)
    manifest["artifacts"] = eligible

    # A locally resealed manifest/report cannot move the external trust root.
    repin()
    with pytest.raises(ncen.NcenError, match="^diagnostic_declared_pin_mismatch$"):
        ncen._purpose_q1_verify_observations(
            roots, roles, manifest, ncen._purpose_json_bytes(report), trusted_run=trust,
        )


def test_review_q2a_invalid_xml_keeps_valid_header_but_not_xml(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source = roots["ncen-source"]
    source_path = source / roles["ncen_source_manifest"][0]["path"]
    raw_path = roots["independent"] / "raw-audit.json"
    manifest = json.loads(source_path.read_bytes())
    audit = json.loads(raw_path.read_bytes())
    xml = next(row for row in manifest["artifacts"] if row["kind"] == "edgar_xml")
    (source / xml["path"]).write_bytes(b"<edgarSubmission>")
    raw_xml = next(row for row in audit["artifacts"] if row["artifact_id"] == xml["artifact_id"])
    raw_xml.update(sha256=_stage2a_sha(source / xml["path"]), bytes=(source / xml["path"]).stat().st_size)
    manifest["artifacts"].remove(xml)
    source_path.write_bytes(ncen._purpose_json_bytes(manifest))
    raw_path.write_bytes(ncen._purpose_json_bytes(audit))
    for role, path in (("ncen_source_manifest", source_path), ("ncen_raw_audit_manifest", raw_path)):
        roles[role] = ({**roles[role][0], "sha256": _stage2a_sha(path),
                        "bytes": path.stat().st_size},)
    roles["ncen_xml_artifacts"] = ()
    report = ncen._purpose_q1_observations(roots, roles, manifest)
    assert report["reconciliation"]["f1_subset_claim"] == {
        "artifact_ids": sorted(row["artifact_id"] for row in manifest["artifacts"]),
        "verification": "verified_q2a",
    }
    assert any(row["kind"] == "header" and row["artifact_id"] in
               report["reconciliation"]["f1_subset_claim"]["artifact_ids"]
               for row in audit["artifacts"])
    xml_copy = next(row for row in report["copy_observations"] if row["artifact_id"] == xml["artifact_id"])
    assert xml_copy["parse_state"] == "quarantined_partial"
    assert xml_copy["admission_disposition"] == "audit_only"
    assert any(row["artifact_id"] == xml["artifact_id"] and row["disposition"] == "parser_quarantine"
               for row in report["raw_input_inventory"])
    manifest["artifacts"].append({**xml, "sha256": raw_xml["sha256"], "bytes": raw_xml["bytes"]})
    source_path.write_bytes(ncen._purpose_json_bytes(manifest))
    roles["ncen_source_manifest"] = ({**roles["ncen_source_manifest"][0],
                                      "sha256": _stage2a_sha(source_path),
                                      "bytes": source_path.stat().st_size},)
    with pytest.raises(ncen.NcenError, match="^diagnostic_admission_subset_mismatch$"):
        ncen._purpose_q1_observations(roots, roles, manifest)


@pytest.mark.parametrize("attack", ["wrong_bytes", "wrong_metadata", "raw_only_promoted", "member_metadata"])
def test_review_q2a_resealed_f1_metadata_cannot_override_raw_audit(tmp_path: Path, attack: str) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    source_path = roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]
    raw_path = roots["independent"] / "raw-audit.json"
    manifest = json.loads(source_path.read_bytes())
    audit = json.loads(raw_path.read_bytes())
    xml = next(item for item in manifest["artifacts"] if item["kind"] == "edgar_xml")
    if attack == "wrong_bytes":
        xml["sha256"] = "0" * 64
    elif attack == "wrong_metadata":
        xml["public_at"] = _stage2a_timestamp(K + dt.timedelta(days=2))
    elif attack == "raw_only_promoted":
        raw_xml = next(item for item in audit["artifacts"] if item["artifact_id"] == xml["artifact_id"])
        raw_xml["retrieved_at"] = None
        xml["retrieved_at"] = None
    else:
        dera = next(item for item in manifest["artifacts"] if item["kind"] == "dera_zip")
        dera["members"][0]["header"].append("invented")
    source_path.write_bytes(ncen._purpose_json_bytes(manifest))
    raw_path.write_bytes(ncen._purpose_json_bytes(audit))
    for role, path in (("ncen_source_manifest", source_path), ("ncen_raw_audit_manifest", raw_path)):
        roles[role] = ({**roles[role][0], "sha256": _stage2a_sha(path),
                        "bytes": path.stat().st_size},)
    with pytest.raises(ncen.NcenError, match="^diagnostic_admission_subset_mismatch$"):
        ncen._purpose_q1_observations(roots, roles, manifest)


def test_review_q2a_relabelled_raw_disposition_refuses_fixed_trust(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    report = ncen._purpose_q1_observations(roots, roles, manifest)
    claimed = json.loads(ncen._purpose_json_bytes(report))
    row = next(row for row in claimed["raw_input_inventory"] if row["disposition"] == "parsed_evidence")
    row["disposition"] = "parser_quarantine"
    with pytest.raises(ncen.NcenError, match="^diagnostic_quarantine_disposition_mismatch$"):
        ncen._purpose_q1_verify_observations(
            roots, roles, manifest, ncen._purpose_json_bytes(claimed), trusted_run=trust,
        )


@pytest.mark.parametrize("failure", ["resource", "programming", "hash"])
def test_review_q2a_non_evidence_failure_never_becomes_audit_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {item["role"]: tuple(item["pins"]) for item in declaration["pin_roles"]}
    manifest = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    if failure == "hash":
        dera = next(item for item in manifest["artifacts"] if item["kind"] == "dera_zip")
        with (roots["ncen-source"] / dera["path"]).open("ab") as handle:
            handle.write(b"tampered")
        with pytest.raises(ncen.NcenError, match="^diagnostic_input_bytes_mismatch$"):
            ncen._purpose_q1_observations(roots, roles, manifest)
        return

    def fail(*_args: object, **_kwargs: object) -> tuple[ncen.DiagnosticSourceRow, ...]:
        if failure == "resource":
            raise ncen.NcenError("diagnostic_spill_budget_exceeded")
        raise RuntimeError("unexpected F1 programming error")

    monkeypatch.setattr(ncen, "_diagnostic_dera_rows", fail)
    if failure == "resource":
        with pytest.raises(ncen.NcenError, match="^diagnostic_spill_budget_exceeded$"):
            ncen._purpose_q1_observations(roots, roles, manifest)
    else:
        with pytest.raises(RuntimeError, match="^unexpected F1 programming error$"):
            ncen._purpose_q1_observations(roots, roles, manifest)


def test_review_q2b_candidate_snapshot_keeps_all_index_and_copy_witnesses(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {row["role"]: tuple(row["pins"]) for row in declaration["pin_roles"]}
    source = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    result = ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
    report, merged = result["audit"], result["merged"]
    assert len(result["index_entries"]) == len([row for row in report["index_observations"]
                                                     if row["entry"] is not None and row["entry"]["form_type"] in ncen.NCEN_FORMS])
    assert len(result["filings"]) == len([row for row in report["copy_observations"]
                                          if row["parse_state"] in {"parsed", "quarantined_partial"}])
    assert len(result["filings"]) == 2
    assert len(tuple(merged.filings())) == 6
    assert sum(filing.is_placeholder for filing in merged.filings()) == 5
    assert merged.excluded == {}
    assert {row["observation_id"] for row in report["index_observations"] if row["entry"] is not None
            and row["entry"]["form_type"] in ncen.NCEN_FORMS} <= result["coverage"].keys()
    assert {row["observation_id"] for row in report["copy_observations"]} <= result["coverage"].keys()
    assert len(result["coverage"]) == sum(len(report[key]) for key in (
        "raw_input_inventory", "index_observations", "copy_observations", "header_observations",
    )) + sum(len(report["reconciliation"][key]) for key in (
        "f6_exclusions", "f6_boundaries", "acquisition_requests",
    ))
    assert {f"f6_exclusions:{number}" for number in range(3)} <= result["coverage"].keys()
    assert "f6_boundaries:0" in result["coverage"]
    assert all(value["disposition"] in {"merged", "frozen_index_placeholder", "frozen_exclusion",
                                        "audit_out_of_scope"}
               for value in result["coverage"].values())
    assert all(result["coverage"][row["observation_id"]]["disposition"] != "audit_out_of_scope"
               for row in report["copy_observations"] if cik(1) in row["cik_claims"])
    assert {filing.accession_number for filing in merged.by_cik[cik(1)]} >= {
        ACCESSION, "0000000001-26-000002", "0000000001-26-000005",
    }
    assert any(filing.is_placeholder for filing in merged.by_cik[cik(1)])
    for accession in ("0000000001-26-000002", "0000000001-26-000005"):
        assert any(row.is_placeholder and row.accession_number == accession
                   for row in merged.by_cik[cik(1)])
        assert any(row["entry"] is not None and row["entry"]["accession_number"] == accession
                   for row in report["index_observations"])
    assert report["reconciliation"]["f1_subset_claim"]["verification"] == "verified_q2a"
    assert result["coverage"] == ncen._purpose_q2_candidates(
        roots, roles, source, cohort_ciks={cik(1), cik(2)},
    )["coverage"]


def test_review_q2b_fixed_pin_refuses_omitted_raw_candidate(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    raw_path = roots["independent"] / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    raw["artifacts"] = [row for row in raw["artifacts"] if row["kind"] != "edgar_xml"]
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    with pytest.raises(ncen.NcenError, match="^diagnostic_input_bytes_mismatch$"):
        _c1a_admit(trust, declaration, roots, tmp_path / "export")


@pytest.mark.parametrize("case,reason", [
    ("partial", None),
    ("unparseable_same", "diagnostic_candidate_blocker_unrepresentable"),
    ("unparseable_no_index", "diagnostic_candidate_blocker_unrepresentable"),
    ("unparseable_index", None),
])
def test_review_q2b_authentic_xml_partial_and_unrepresentable_copy(
    tmp_path: Path, case: str, reason: str | None,
) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {row["role"]: tuple(row["pins"]) for row in declaration["pin_roles"]}
    source = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    evidence = roots["independent"]
    raw_path = evidence / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    original = next(row for row in raw["artifacts"] if row["kind"] == "edgar_xml")
    accession = ("0000000001-26-000088" if case == "unparseable_no_index" else
                 "0000000001-26-000002" if case == "unparseable_index" else ACCESSION)
    target = roots["ncen-source"] / "extra.xml"
    target.write_bytes(b"<edgarSubmission>")
    extra = {**original, "artifact_id": "z-extra", "path": target.name,
             "sha256": _stage2a_sha(target), "bytes": target.stat().st_size,
             "accession_claim": accession,
             "cik_claim": None if case == "unparseable_index" else original["cik_claim"],
             "retrieved_at": None if case.startswith("unparseable") else _stage2a_timestamp(K)}
    raw["artifacts"].append(extra)
    raw["artifacts"].sort(key=lambda item: item["artifact_id"])
    if case == "partial":
        index_path = evidence / "form.idx"
        index_path.write_bytes(b"".join(
            line for line in index_path.read_bytes().splitlines(keepends=True)
            if b"0000000001-26-000002" not in line
        ))
        index_manifest_path = evidence / "index-manifest.json"
        index_manifest = json.loads(index_manifest_path.read_bytes())
        index_manifest["artifacts"][0]["pin"].update(
            sha256=_stage2a_sha(index_path), bytes=index_path.stat().st_size,
        )
        index_manifest_path.write_bytes(ncen._purpose_json_bytes(index_manifest))
        roles["ncen_index_artifacts"] = (index_manifest["artifacts"][0]["pin"],)
        roles["ncen_index_manifest"] = ({**roles["ncen_index_manifest"][0],
                                         "sha256": _stage2a_sha(index_manifest_path),
                                         "bytes": index_manifest_path.stat().st_size},)
        raw["index_manifest_pin"] = roles["ncen_index_manifest"][0]
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    if reason is not None:
        with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
            ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
    else:
        result = ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
        partial = next(row for row in result["audit"]["copy_observations"]
                       if row["artifact_id"] == "z-extra")
        if case == "unparseable_index":
            assert partial["filing"] is None
            assert partial["merge_input_kind"] == "frozen_index_placeholder"
            assert result["coverage"][partial["observation_id"]]["disposition"] == "frozen_index_placeholder"
            assert any(item.is_placeholder and item.accession_number == accession
                       for item in result["merged"].by_cik[cik(1)])
            return
        assert partial["parse_state"] == "quarantined_partial"
        assert partial["filing"]["funds"] == []
        assert partial["filing"] == ncen._purpose_baseline_filing(ncen.parse_ncen_primary_doc(
            target.read_bytes(), accession_number=accession, source_url=extra["source_url"], retrieved_at=K,
        ))
        assert result["coverage"][partial["observation_id"]]["disposition"] == "merged"
        assert not next(row for row in result["merged"].by_cik[cik(1)]
                        if row.accession_number == ACCESSION).usable
        assert ncen.effective_filing(result["merged"], cik(1), R, K,
                                    knowledge_mode="historical_reconstruction").reason == (
                                        "effective_filing_quarantined"
                                    )


@pytest.mark.parametrize("case,reason", [
    ("duplicate", None),
    ("identity", "diagnostic_candidate_identity_conflict"),
    ("possession", "diagnostic_index_possession_unrepresentable"),
    ("malformed", "diagnostic_candidate_identity_conflict"),
])
def test_review_q2b_index_witness_identity_and_possession(
    tmp_path: Path, case: str, reason: str | None,
) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {row["role"]: tuple(row["pins"]) for row in declaration["pin_roles"]}
    evidence = roots["independent"]
    index_path = evidence / "form.idx"
    index_bytes = index_path.read_bytes()
    witness = next(line for line in index_bytes.splitlines(keepends=True)
                   if ACCESSION.encode() in line)
    if case == "identity":
        changed = witness.replace(b"  " + cik(1).encode() + b"  ",
                                  b"  " + cik(2).encode() + b"  ")
        assert changed != witness
        witness = changed
    elif case == "malformed":
        witness = b"N-CEN  unassignable index row\n"
    if case != "possession":
        index_path.write_bytes(index_bytes + witness)
    index_manifest_path = evidence / "index-manifest.json"
    index_manifest = json.loads(index_manifest_path.read_bytes())
    if case == "possession":
        second_path = evidence / "second-form.idx"
        second_path.write_bytes(index_bytes.replace(b"2026-03-01", b"2026-03-06"))
        index_manifest["artifacts"].append({"pin": {"root_id": "independent", "path": second_path.name,
                                                    "sha256": _stage2a_sha(second_path),
                                                    "bytes": second_path.stat().st_size},
                                            "retrieved_at": _stage2a_timestamp(K + dt.timedelta(days=11))})
        roles["ncen_index_artifacts"] = tuple(row["pin"] for row in index_manifest["artifacts"])
    else:
        index_manifest["artifacts"][0]["pin"].update(
            sha256=_stage2a_sha(index_path), bytes=index_path.stat().st_size,
        )
        roles["ncen_index_artifacts"] = (index_manifest["artifacts"][0]["pin"],)
    index_manifest_path.write_bytes(ncen._purpose_json_bytes(index_manifest))
    roles["ncen_index_manifest"] = ({**roles["ncen_index_manifest"][0],
                                     "sha256": _stage2a_sha(index_manifest_path),
                                     "bytes": index_manifest_path.stat().st_size},)
    raw_path = evidence / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    raw["index_manifest_pin"] = roles["ncen_index_manifest"][0]
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    source = json.loads((roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]).read_bytes())
    if reason is not None:
        with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
            ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
    else:
        result = ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
        duplicate = [row for row in result["audit"]["index_observations"]
                     if row["entry"] is not None and row["entry"]["accession_number"] == ACCESSION]
        assert len(duplicate) == 2
        assert duplicate[0]["equivalence_group_id"] == duplicate[1]["equivalence_group_id"]
        assert len([item for item in result["merged"].by_cik[cik(1)]
                    if item.accession_number == ACCESSION]) == 1


def test_review_q2b_mixed_dera_keeps_partial_but_never_promotes_f1(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {row["role"]: tuple(row["pins"]) for row in declaration["pin_roles"]}
    source_path = roots["ncen-source"] / roles["ncen_source_manifest"][0]["path"]
    source = json.loads(source_path.read_bytes())
    raw_path = roots["independent"] / "raw-audit.json"
    raw = json.loads(raw_path.read_bytes())
    dera = next(item for item in source["artifacts"] if item["kind"] == "dera_zip")
    package = roots["ncen-source"] / dera["path"]
    accession = "0000000001-26-000099"
    with zipfile.ZipFile(package) as archive:
        members = [(info.filename, archive.read(info)) for info in archive.infolist()]
    with zipfile.ZipFile(package, "w") as archive:
        for name, data in members:
            if name.endswith("SUBMISSION.tsv"):
                columns, sample = data.splitlines()[:2]
                fields = sample.split(b"\t")
                header = columns.split(b"\t")
                fields[header.index(b"ACCESSION_NUMBER")] = accession.encode()
                fields[header.index(b"REPORT_ENDING_PERIOD")] = b"not-a-date"
                data += b"\t".join(fields) + b"\n"
            archive.writestr(name, data)
    raw_dera = next(item for item in raw["artifacts"] if item["artifact_id"] == dera["artifact_id"])
    raw_dera.update(sha256=_stage2a_sha(package), bytes=package.stat().st_size)
    source["artifacts"].remove(dera)
    source_path.write_bytes(ncen._purpose_json_bytes(source))
    roles["ncen_source_manifest"] = ({**roles["ncen_source_manifest"][0],
                                      "sha256": _stage2a_sha(source_path),
                                      "bytes": source_path.stat().st_size},)
    roles["ncen_dera_artifacts"] = ()
    index_path = roots["independent"] / "form.idx"
    index_path.write_bytes(index_path.read_bytes() + f"N-CEN  Fixture Fund  {cik(1)}  2026-03-07  "
                           f"edgar/data/1/{accession}.txt\n".encode("latin-1"))
    index_manifest_path = roots["independent"] / "index-manifest.json"
    index_manifest = json.loads(index_manifest_path.read_bytes())
    index_manifest["artifacts"][0]["pin"].update(
        sha256=_stage2a_sha(index_path), bytes=index_path.stat().st_size,
    )
    index_manifest_path.write_bytes(ncen._purpose_json_bytes(index_manifest))
    roles["ncen_index_artifacts"] = (index_manifest["artifacts"][0]["pin"],)
    roles["ncen_index_manifest"] = ({**roles["ncen_index_manifest"][0],
                                     "sha256": _stage2a_sha(index_manifest_path),
                                     "bytes": index_manifest_path.stat().st_size},)
    raw["index_manifest_pin"] = roles["ncen_index_manifest"][0]
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    result = ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
    mixed = [row for row in result["audit"]["copy_observations"]
             if row["artifact_id"] == dera["artifact_id"]]
    assert {row["parse_state"] for row in mixed} == {"parsed", "quarantined_partial"}
    assert all(row["admission_disposition"] == "audit_only" for row in mixed)
    assert {row["filing"]["accession_number"] for row in mixed} == {ACCESSION, accession}
    assert result["coverage"][next(row["observation_id"] for row in mixed
                                   if row["filing"]["accession_number"] == accession)]["disposition"] == "merged"
    assert accession in {row.accession_number for row in result["merged"].by_cik[cik(1)]}
    sources = ncen.read_diagnostic_source_rows(
        ncen.DiagnosticSourceManifestPin(source_path, _stage2a_sha(source_path), source_path.stat().st_size),
        trusted_acquisition=ncen.DiagnosticAcquisitionPin(
            roots["acquisition"], roles["acquisition_scope"][0]["sha256"],
            roles["acquisition_sha256sums"][0]["sha256"], "synthetic_fixture",
        ),
    )
    assert all(row.artifact_id != dera["artifact_id"] for row in sources.rows)
    selection = ncen.diagnostic_selection(result["merged"], sources, cik(1), R, K,
                                          mode="historical_reconstruction", fund_keys=("S000000001",))
    assert selection.evidence_state == "incomplete"
    assert all(row.artifact_id != dera["artifact_id"] for row in selection.rows)


def test_review_q2b_later_parsed_competitor_wins_frozen_selection(tmp_path: Path) -> None:
    _trust, declaration, roots = _c1a_package(tmp_path)
    roles = {row["role"]: tuple(row["pins"]) for row in declaration["pin_roles"]}
    source_root, evidence = roots["ncen-source"], roots["independent"]
    source_path, raw_path = source_root / roles["ncen_source_manifest"][0]["path"], evidence / "raw-audit.json"
    source, raw = json.loads(source_path.read_bytes()), json.loads(raw_path.read_bytes())
    original_header = next(row for row in source["artifacts"] if row["kind"] == "header")
    original_xml = next(row for row in source["artifacts"] if row["kind"] == "edgar_xml")
    accession = "0000000001-26-000055"
    new_header_path, new_xml_path = source_root / "later-submission.txt", source_root / "later.xml"
    new_header_path.write_bytes((source_root / original_header["path"]).read_bytes()
                                .replace(ACCESSION.encode(), accession.encode())
                                .replace(b"20260301120000", b"20260302120000")
                                .replace(b"20260301\n", b"20260302\n"))
    new_xml_path.write_bytes((source_root / original_xml["path"]).read_bytes())
    accepted = _stage2a_timestamp(dt.datetime(2026, 3, 2, 17, tzinfo=UTC))
    new_header = {**original_header, "artifact_id": "header-later", "path": new_header_path.name,
                  "sha256": _stage2a_sha(new_header_path), "bytes": new_header_path.stat().st_size,
                  "accession_number": accession, "public_at": accepted, "data_known_at": accepted}
    new_xml = {**original_xml, "artifact_id": "xml-later", "path": new_xml_path.name,
               "sha256": _stage2a_sha(new_xml_path), "bytes": new_xml_path.stat().st_size,
               "accession_number": accession, "header_artifact_id": "header-later",
               "retrieved_at": _stage2a_timestamp(K - dt.timedelta(days=1)),
               "public_at": accepted, "data_known_at": accepted}
    source["artifacts"].extend((new_header, new_xml))
    source["artifacts"].sort(key=lambda row: row["artifact_id"])
    source_path.write_bytes(ncen._purpose_json_bytes(source))
    roles["ncen_source_manifest"] = ({**roles["ncen_source_manifest"][0],
                                      "sha256": _stage2a_sha(source_path),
                                      "bytes": source_path.stat().st_size},)
    for kind, artifact in (("ncen_header_artifacts", new_header), ("ncen_xml_artifacts", new_xml)):
        roles[kind] = tuple(sorted((*roles[kind], {"root_id": "ncen-source", "path": artifact["path"],
                                               "sha256": artifact["sha256"], "bytes": artifact["bytes"]}),
                                   key=lambda row: (row["root_id"], row["path"])))
    for artifact in (new_header, new_xml):
        raw["artifacts"].append({"artifact_id": artifact["artifact_id"], "kind": artifact["kind"],
                                 **{key: artifact[key] for key in (
                                     "path", "sha256", "bytes", "retrieved_at", "public_at",
                                     "data_known_at", "source_url",
                                 )}, "root_id": "ncen-source", "package_label": None,
                                 "accession_claim": accession,
                                 "cik_claim": cik(1) if artifact["kind"] == "header" else None,
                                 "header_artifact_ids": (["header-later"] if artifact["kind"] == "edgar_xml" else []),
                                 "acquisition_request_locators": []})
    raw["artifacts"].sort(key=lambda row: row["artifact_id"])
    index_path = evidence / "form.idx"
    index_path.write_bytes(b"".join(line for line in index_path.read_bytes().splitlines(keepends=True)
                                   if b"0000000001-26-000002" not in line)
                           + f"N-CEN  Fixture Fund  {cik(1)}  2026-03-02  "
                             f"edgar/data/1/{accession}.txt\n".encode("latin-1"))
    index_manifest_path = evidence / "index-manifest.json"
    index_manifest = json.loads(index_manifest_path.read_bytes())
    index_manifest["artifacts"][0]["pin"].update(
        sha256=_stage2a_sha(index_path), bytes=index_path.stat().st_size,
    )
    index_manifest_path.write_bytes(ncen._purpose_json_bytes(index_manifest))
    roles["ncen_index_artifacts"] = (index_manifest["artifacts"][0]["pin"],)
    roles["ncen_index_manifest"] = ({**roles["ncen_index_manifest"][0],
                                     "sha256": _stage2a_sha(index_manifest_path),
                                     "bytes": index_manifest_path.stat().st_size},)
    raw["index_manifest_pin"] = roles["ncen_index_manifest"][0]
    raw_path.write_bytes(ncen._purpose_json_bytes(raw))
    roles["ncen_raw_audit_manifest"] = ({**roles["ncen_raw_audit_manifest"][0],
                                         "sha256": _stage2a_sha(raw_path),
                                         "bytes": raw_path.stat().st_size},)
    candidates = ncen._purpose_q2_candidates(roots, roles, source, cohort_ciks={cik(1), cik(2)})
    assert {ACCESSION, accession} <= {item.accession_number for item in candidates["merged"].by_cik[cik(1)]}
    sources = ncen.read_diagnostic_source_rows(
        ncen.DiagnosticSourceManifestPin(source_path, _stage2a_sha(source_path), source_path.stat().st_size),
        trusted_acquisition=ncen.DiagnosticAcquisitionPin(
            roots["acquisition"], roles["acquisition_scope"][0]["sha256"],
            roles["acquisition_sha256sums"][0]["sha256"], "synthetic_fixture",
        ),
    )
    selection = ncen.diagnostic_selection(candidates["merged"], sources, cik(1), R, K,
                                          mode="historical_reconstruction", fund_keys=("S000000001",))
    assert selection.accession_number == accession
    assert selection.selection_reason is None
    assert {item.accession_number for item in selection.dependencies} == {ACCESSION, accession}


def _c1b2a_expected(tmp_path: Path) -> tuple[ncen.DiagnosticTrustPin, ncen.DiagnosticBaselineCheckpoint, ncen.ExpectedPurposeSelections]:
    trust, _declaration, roots = _c1a_package(tmp_path, selected_fixture=True)
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    expected = ncen.replay_purpose_selections(
        baseline, trust, source_index=issuance.source_index_ref,
        membership_checkpoint=issuance.membership_ref,
    )
    return trust, baseline, expected


def test_c1b2a_full_independent_replay_and_removed_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_graph(*_args, **_kwargs):
        raise AssertionError("graph ran before pure replay comparison")

    monkeypatch.setattr(ncen, "project_dependence", no_graph)
    trust, baseline, expected = _c1b2a_expected(tmp_path)
    original = expected.records
    assert len(original["contexts"]) == 2
    assert sum(len(item["selections"]) for item in original["contexts"]) == 3
    assert len(original["ablation_ids"]) == 9
    assert original["candidate_inventory"]["copy_observations"]
    assert original["candidate_inventory"]["index_observations"]
    assert original["sources"]
    assert sum(len(item["incidences"]) for item in original["contexts"]) == 24
    assert baseline.digest == original["baseline_digest"]
    ncen.compare_purpose_selection_records(expected, copy.deepcopy(original))
    removed = copy.deepcopy(original)
    for context in removed["contexts"]:
        for item in context["selections"]:
            if item["selected_source_row_ids"]:
                item.update(selected_accession=None, selected_projection_digest=None,
                            selected_source_row_ids=[], origin_source_row_ids=[],
                            evidence_state="incomplete", selection_reason="no_effective_filing",
                            reasons=["no_effective_filing"])
        context["incidences"] = []
    assert sum(len(item["incidences"]) for item in removed["contexts"]) == 0
    with pytest.raises(ncen.NcenError, match="^diagnostic_selection_replay_mismatch$"):
        ncen.compare_purpose_selection_records(expected, removed)
    assert ncen.replay_purpose_selections(
        baseline, trust,
        source_index=ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)].source_index_ref,
        membership_checkpoint=ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)].membership_ref,
    ).digest == expected.digest


@pytest.mark.parametrize("change", (
    "context", "member", "date", "source", "candidate", "accession", "dependency",
    "knowledge", "reason", "excluded", "origin", "family", "node", "taint",
    "incidence", "role", "attestation", "order", "extra",
))
def test_c1b2a_comparison_rejects_every_semantic_delta(tmp_path: Path, change: str) -> None:
    _trust, _baseline, expected = _c1b2a_expected(tmp_path)
    emitted = expected.records
    context = emitted["contexts"][0]
    selected = context["selections"][0]
    if change == "context":
        context["K"] = "2026-01-01T00:00:00.000000Z"
    elif change == "member":
        context["selections"].pop()
    elif change == "date":
        emitted["contexts"].pop()
    elif change == "source":
        emitted["sources"].pop()
    elif change == "candidate":
        emitted["candidate_inventory"]["copy_observations"].pop()
    elif change == "accession":
        selected["selected_accession"] = None
    elif change == "dependency":
        selected["dependencies"] = []
    elif change == "knowledge":
        selected["knowledge_time"] = None
    elif change == "reason":
        selected["reasons"] = ["forged"]
    elif change == "excluded":
        selected["excluded_source_row_ids"] = ["forged"]
    elif change == "origin":
        selected["origin_source_row_ids"] = ["forged"]
    elif change == "family":
        context["reported_families"][0]["state"] = "forged"
    elif change == "node":
        context["nodes"][0]["voting_series"] = []
    elif change == "taint":
        context["nodes"][0]["has_unknown_dependence"] = not context["nodes"][0]["has_unknown_dependence"]
    elif change in {"incidence", "role", "attestation"}:
        target = next(item for group in emitted["contexts"] for item in group["incidences"])
        if change == "incidence":
            target["identifier_value"] = "altered"
        elif change == "role":
            target["role"] = "current_sub"
        else:
            target["attestation"] = "uncertain"
    elif change == "order":
        emitted["contexts"].reverse()
    elif change == "extra":
        emitted["unexpected"] = True
    with pytest.raises(ncen.NcenError, match="^diagnostic_selection_replay_mismatch$"):
        ncen.compare_purpose_selection_records(expected, emitted)


def test_c1b2a_original_pins_and_detached_payload_only(tmp_path: Path) -> None:
    trust, baseline, expected = _c1b2a_expected(tmp_path)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    detached = ncen._diagnostic_bound_baseline_admission(baseline, trusted_run=trust)
    detached["index_entries"].clear()
    detached["filings"].clear()
    detached["selections"].clear()
    assert ncen.replay_purpose_selections(
        baseline, trust, source_index=issuance.source_index_ref,
        membership_checkpoint=issuance.membership_ref,
    ).digest == expected.digest
    for changed in (
        {"baseline": detached},
        {"trusted_run": dataclasses.replace(trust)},
        {"source_index": dataclasses.replace(issuance.source_index_ref)},
        {"membership_checkpoint": dataclasses.replace(issuance.membership_ref)},
        {"source_index": None},
        {"membership_checkpoint": None},
    ):
        arguments = {"baseline": baseline, "trusted_run": trust,
                     "source_index": issuance.source_index_ref,
                     "membership_checkpoint": issuance.membership_ref, **changed}
        with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
            ncen.replay_purpose_selections(**arguments)


@pytest.mark.parametrize("phase", ("selection_replay_start", "selection_replay_complete"))
def test_c1b2a_resource_check_even_below_row_threshold(tmp_path: Path, phase: str) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path, selected_fixture=True)
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    monitor = ncen.DiagnosticResourceMonitor(
        rss_probe=lambda: monitor.limits.rss_hard_bytes + 1 if monitor.phase == phase else 0,
    )
    with pytest.raises(ncen.NcenError, match=f"^diagnostic_resource_rss_hard_limit_exceeded:{phase}$"):
        ncen.replay_purpose_selections(
            baseline, trust, source_index=issuance.source_index_ref,
            membership_checkpoint=issuance.membership_ref, monitor=monitor,
        )


@pytest.mark.parametrize("omitted", ("parsed_copy", "unknown_time_copy", "later_index", "f6_quarantine"))
def test_c1b2a_fixed_pin_refuses_missing_candidate_or_quarantine(tmp_path: Path, omitted: str) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path)
    if omitted == "f6_quarantine":
        path = roots["acquisition"] / "terminal" / f"{_ACQ_DEFAULT_ENTRIES[0].accession}.json"
        path.write_bytes(path.read_bytes() + b" ")
    else:
        name = "ncen_index_entries.jsonl" if omitted == "later_index" else "ncen_copies.jsonl"
        path = roots["independent"] / name
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        if omitted == "parsed_copy":
            target = next(item for item in rows if item.get("parse_state") == "parsed")
        elif omitted == "unknown_time_copy":
            target = next(item for item in rows if item.get("parse_state") == "index_only"
                          and item.get("accession_claims") == ["0000000001-26-000005"])
        else:
            target = next(item for item in rows if item.get("entry") is not None
                          and item["entry"]["accession_number"] == "0000000001-26-000002")
        rows.remove(target)
        path.write_bytes(ncen._purpose_jsonl_bytes(rows))
    with pytest.raises(ncen.NcenError, match="^diagnostic_"):
        ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)


@pytest.mark.parametrize("mode", ("historical_reconstruction", "current_run"))
@pytest.mark.parametrize("selected_fixture", (False, True))
def test_c1b2a_replays_every_pinned_member_and_competitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, selected_fixture: bool,
) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path, mode=mode, selected_fixture=selected_fixture)
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    original = ncen._diagnostic_bound_baseline_admission(baseline, trusted_run=trust)
    real_merge = ncen.merge_filings
    real_select = ncen.diagnostic_selection
    merge_inputs = []
    visited = []

    def measured_merge(filings, **kwargs):
        copies = tuple(filings)
        entries = tuple(kwargs.pop("index_entries"))
        merge_inputs.append((copies, entries))
        return real_merge(copies, index_entries=entries, **kwargs)

    def measured_select(index, sources, member_cik, report_date, cutoff, **kwargs):
        visited.append((report_date, member_cik, cutoff, kwargs["mode"], kwargs["fund_keys"]))
        return real_select(index, sources, member_cik, report_date, cutoff, **kwargs)

    monkeypatch.setattr(ncen, "merge_filings", measured_merge)
    monkeypatch.setattr(ncen, "diagnostic_selection", measured_select)
    expected = ncen.replay_purpose_selections(
        baseline, trust, source_index=issuance.source_index_ref,
        membership_checkpoint=issuance.membership_ref,
    )
    assert len(merge_inputs) == 1
    assert len(merge_inputs[0][0]) == len(original["filings"])
    assert len(merge_inputs[0][1]) == len(original["index_entries"])
    assert len(visited) == len(issuance.membership_ref.members) == 3
    assert {(date, member_cik) for date, member_cik, *_ in visited} == {
        (member.report_date, member.cik) for member in issuance.membership_ref.members
    }
    assert all(selected_mode == mode for _, _, _, selected_mode, _ in visited)
    assert all(item["mode"] == mode for item in expected.records["contexts"])
    count = sum(len(item["incidences"]) for item in expected.records["contexts"])
    assert count == (24 if selected_fixture and mode == "historical_reconstruction"
                     else 0 if mode == "historical_reconstruction" else 12)


def test_c1b2a_four_fresh_hash_seeds_same_external_pin(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path, selected_fixture=True)
    arguments = json.dumps({"manifest_path": str(trust.manifest_path),
                            "sha256": trust.manifest_sha256, "bytes": trust.manifest_size,
                            "roots": {key: str(value) for key, value in roots.items()}})
    outputs = []
    for seed in (1, 2, 977, 31337):
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-s", "tests/test_bond_default_ncen_purposes.py",
             "-k", "c1b2a_seed_child"], cwd=ROOT,
            env={**os.environ, "PYTHONHASHSEED": str(seed), "NCEN_C1B2A_SEED_ARGS": arguments},
            capture_output=True, text=True, check=False, timeout=60,
        )
        assert completed.returncode == 0, completed.stderr
        outputs.append(tuple(next(line.split(f"{prefix}:", 1)[1]
                                  for line in completed.stdout.splitlines()
                                  if line.startswith(f"{prefix}:"))
                             for prefix in ("REPLAY_DIGEST", "SERIALIZED_REPLAY_DIGEST")))
    assert len(set(outputs)) == 1
    assert all(len(digest) == 64 for digest in outputs[0])
    print("REPLAY_DIGEST:" + outputs[0][0])


def test_c1b2a_seed_child() -> None:
    raw = os.environ.get("NCEN_C1B2A_SEED_ARGS")
    if raw is None:
        return
    arguments = json.loads(raw)
    pin = ncen.DiagnosticTrustPin(Path(arguments["manifest_path"]), arguments["sha256"], arguments["bytes"])
    baseline = ncen.read_diagnostic_baseline_checkpoint(
        pin, code_root=ROOT,
        input_roots={key: Path(path) for key, path in arguments["roots"].items()},
    )
    bound = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    expected = ncen.replay_purpose_selections(
        baseline, pin, source_index=bound.source_index_ref,
        membership_checkpoint=bound.membership_ref,
    )
    inventory, rows = ncen._purpose_serialize_selection_replay(expected)
    rebuilt = ncen._purpose_reconstruct_emitted_replay(inventory, rows)
    assert ncen._diagnostic_canonical(rebuilt) == expected.canonical_bytes
    print("SERIALIZED_REPLAY_DIGEST:" + hashlib.sha256(ncen._purpose_json_bytes(inventory)
          + ncen._purpose_jsonl_bytes(rows)).hexdigest())
    print("REPLAY_DIGEST:" + expected.digest)


def test_c1b2b1_lossless_v2_selection_records(tmp_path: Path) -> None:
    _trust, _baseline, expected = _c1b2a_expected(tmp_path)
    inventory, rows = ncen._purpose_serialize_selection_replay(expected)
    assert set(inventory) == {
        "schema_version", "lane", "baseline_digest", "versions", "ablation_ids",
        "candidate_inventory", "sources", "global_exclusions",
    }
    assert len(rows) == len(expected.records["contexts"]) == 2
    assert all(row["schema_version"] == ncen.DIAGNOSTIC_EXPORT_VERSION
               and row["record_type"] == "selection_context" for row in rows)
    assert sum(len(row["incidences"]) for row in rows) == 24
    emitted = ncen._purpose_reconstruct_emitted_replay(inventory, rows)
    assert ncen._diagnostic_canonical(emitted) == expected.canonical_bytes
    ncen.compare_purpose_selection_records(expected, emitted)


def test_c1b2b1_lossless_incomplete_selection_fixture(tmp_path: Path) -> None:
    trust, _declaration, roots = _c1a_package(tmp_path, selected_fixture=False)
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    expected = ncen.replay_purpose_selections(
        baseline, trust, source_index=issuance.source_index_ref,
        membership_checkpoint=issuance.membership_ref,
    )
    inventory, rows = ncen._purpose_serialize_selection_replay(expected)
    emitted = ncen._purpose_reconstruct_emitted_replay(inventory, rows)
    assert ncen._diagnostic_canonical(emitted) == expected.canonical_bytes


@pytest.mark.parametrize("attack", (
    "missing_inventory", "unknown_inventory", "missing_context", "reordered_contexts",
    "missing_cik", "duplicate_cik", "omitted_fund_key", "duplicate_record_id",
    "renamed_role", "unknown_context_key", "unsorted_source_ids", "bool_in_integer",
    "reordered_selection_source_ids", "lost_incidences", "forged_record_id",
    "candidate_unknown_key", "copy_missing_key", "f6_unknown_key", "reconciliation_unknown_key",
))
def test_c1b2b1_selection_schema_rejects_malformed(
    tmp_path: Path, attack: str,
) -> None:
    _trust, _baseline, expected = _c1b2a_expected(tmp_path)
    inventory, rows = ncen._purpose_serialize_selection_replay(expected)
    inventory, rows = copy.deepcopy(inventory), list(copy.deepcopy(rows))
    target = rows[0]
    if attack == "missing_inventory":
        inventory.pop("candidate_inventory")
    elif attack == "unknown_inventory":
        inventory["unspecified"] = True
    elif attack == "missing_context":
        rows.pop()
    elif attack == "reordered_contexts":
        rows.reverse()
    elif attack == "missing_cik":
        target["selections"].pop()
    elif attack == "duplicate_cik":
        target["selections"].append(copy.deepcopy(target["selections"][0]))
    elif attack == "omitted_fund_key":
        target["selections"][0]["fund_keys"].pop()
    elif attack == "duplicate_record_id":
        rows[1]["record_id"] = rows[0]["record_id"]
    elif attack == "renamed_role":
        target["incidences"][0]["role"] = "other_role"
    elif attack == "unknown_context_key":
        target["new_key"] = "unexpected"
    elif attack == "unsorted_source_ids":
        item = next(item for context in rows for item in context["nodes"]
                    if len(item["source_row_ids"]) >= 2)
        item["source_row_ids"].reverse()
    elif attack == "reordered_selection_source_ids":
        item = next(item for context in rows for item in context["selections"]
                    if len(item["selected_source_row_ids"]) >= 2)
        item["selected_source_row_ids"].reverse()
        target["record_id"] = ncen._purpose_selection_context_record({
            key: value for key, value in target.items()
            if key not in ("schema_version", "record_type", "record_id")
        })["record_id"]
    elif attack == "bool_in_integer":
        target["context_record"]["node_count"] = True
    elif attack == "candidate_unknown_key":
        inventory["candidate_inventory"]["copy_observations"][0]["unverified"] = "x"
    elif attack == "copy_missing_key":
        inventory["candidate_inventory"]["copy_observations"][0].pop("parse_state")
    elif attack == "f6_unknown_key":
        inventory["candidate_inventory"]["reconciliation"]["f6_exclusions"][0]["unverified"] = "x"
    elif attack == "reconciliation_unknown_key":
        inventory["candidate_inventory"]["reconciliation"]["unverified"] = "x"
    elif attack == "lost_incidences":
        for item in rows:
            item["incidences"] = []
            item["record_id"] = ncen._purpose_selection_context_record({
                key: value for key, value in item.items()
                if key not in ("schema_version", "record_type", "record_id")
            })["record_id"]
    else:
        target["record_id"] = "ncenrow:selection_context:" + "0" * 64
    with pytest.raises(ncen.NcenError, match="^diagnostic_(selection_records_invalid|context_coverage_mismatch|selection_replay_mismatch)$"):
        emitted = ncen._purpose_reconstruct_emitted_replay(inventory, rows)
        ncen.compare_purpose_selection_records(expected, emitted)


def test_c1b2b1_public_signature_and_no_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = inspect.signature(ncen.write_purpose_diagnostics)
    validator = inspect.signature(ncen.validate_purpose_export)
    assert list(writer.parameters) == ["trusted_run", "declaration", "output_root", "code_root",
                                       "input_roots", "receipt_path"]
    assert all(parameter.kind == inspect.Parameter.KEYWORD_ONLY
               for parameter in writer.parameters.values())
    assert list(validator.parameters) == ["root", "trusted_run", "code_root", "input_roots",
                                          "receipt_path"]
    assert all(validator.parameters[name].default is inspect.Parameter.empty
               for name in ("trusted_run", "code_root", "input_roots"))
    trust, declaration, roots = _c1a_package(tmp_path, selected_fixture=True)
    output = tmp_path / "no-public-export"
    def forbidden(*_args, **_kwargs):
        raise AssertionError("public guard ran unbounded candidate admission")

    monkeypatch.setattr(ncen, "_purpose_admit_trust", forbidden)
    monkeypatch.setattr(ncen, "_purpose_enumerate_raw_inputs", forbidden)
    monkeypatch.setattr(ncen, "replay_purpose_selections", forbidden)
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.write_purpose_diagnostics(trusted_run=trust, declaration=declaration,
                                       output_root=output, code_root=ROOT, input_roots=roots)
    assert not output.exists()
    with pytest.raises(TypeError):
        ncen.write_purpose_diagnostics(object(), object(), object(), declaration=declaration,
                                       output_root=output, code_root=ROOT, input_roots=roots)
    with pytest.raises(TypeError):
        ncen.write_purpose_diagnostics(trusted_run=trust, declaration=declaration,
                                       output_root=output, code_root=ROOT, input_roots=roots,
                                       predecessor_receipt=tmp_path / "old.json")
    for argument in ("index", "cohort", "sources", "contexts", "incidences",
                     "expected_replay", "allow_synthetic", "resume"):
        with pytest.raises(TypeError):
            ncen.write_purpose_diagnostics(trusted_run=trust, declaration=declaration,
                                           output_root=output, code_root=ROOT, input_roots=roots,
                                           **{argument: object()})
    assert not output.exists()
    assert not (tmp_path / "no-public-export.receipt.json").exists()
    output.mkdir()
    (output / "declaration.json").write_bytes(ncen._purpose_json_bytes(declaration))
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.validate_purpose_export(output, trusted_run=trust, code_root=ROOT, input_roots=roots)
    with pytest.raises(TypeError):
        ncen.validate_purpose_export(output)
    declaration["schema_version"] = "ncen_purpose_diagnostics_declaration_v1"
    (output / "declaration.json").write_bytes(ncen._purpose_json_bytes(declaration))
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_schema_unsupported$"):
        ncen.validate_purpose_export(output, trusted_run=trust, code_root=ROOT, input_roots=roots)


def test_c1b2b1_public_real_lane_refuses_before_output(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    manifest["lane"] = "sealed_source"
    trust.manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    repinned = ncen.DiagnosticTrustPin(trust.manifest_path, _stage2a_sha(trust.manifest_path),
                                       trust.manifest_path.stat().st_size)
    declaration["lane"] = "sealed_source"
    declaration["trusted_run_manifest_sha256"] = repinned.manifest_sha256
    output = tmp_path / "real-hold"
    with pytest.raises(ncen.NcenError, match="^diagnostic_required_pins_unverified$"):
        ncen.write_purpose_diagnostics(trusted_run=repinned, declaration=declaration,
                                       output_root=output, code_root=ROOT, input_roots=roots)
    assert not output.exists()
    assert not (tmp_path / "real-hold.receipt.json").exists()


def test_c1b2b1_closed_control_schemas_and_status(tmp_path: Path) -> None:
    trust, declaration, roots = _c1a_package(tmp_path, selected_fixture=True)
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    replay = ncen.replay_purpose_selections(baseline, trust,
        source_index=issuance.source_index_ref, membership_checkpoint=issuance.membership_ref)
    bindings = {
        "trusted_run_manifest_sha256": trust.manifest_sha256,
        "baseline_digest": baseline.digest,
        "selection_replay_version": ncen.DIAGNOSTIC_SELECTION_REPLAY_VERSION,
        "selection_expected_digest": replay.digest,
        "acceptance_scope": "synthetic_engineering_only",
        "qualification": "NOT_EVALUABLE",
    }
    declaration.update(bindings)
    assert ncen._purpose_declaration(declaration) == declaration
    for key in bindings:
        altered = dict(declaration)
        altered.pop(key)
        with pytest.raises(ncen.NcenError):
            ncen._purpose_declaration(altered)
    with pytest.raises(ncen.NcenError, match="diagnostic_resume_not_supported"):
        ncen._purpose_declaration({**declaration, "predecessor_receipt_sha256": "a" * 64})

    file_types = {**dict(ncen.DIAGNOSTIC_EXPORT_JSONL),
                  "selection_inventory.json": "selection_inventory",
                  "issuer_group_ref.json": "issuer_group_ref",
                  "performance.json": "performance", "checks.json": "checks"}
    manifest = {
        **bindings, "schema_version": ncen.DIAGNOSTIC_MANIFEST_VERSION,
        "declaration_sha256": "a" * 64,
        "baseline_seals": declaration["baseline_seals"],
        "source_code": declaration["source_code"],
        "runtime": {"python_version": "3", "unicode_version": "1", "platform": "test"},
        "input_artifacts": declaration["input_artifacts"],
        "files": [{"path": path, "record_type": kind, "sha256": "a" * 64,
                   "bytes": 16, "rows": 1}
                  for path, kind in sorted(file_types.items())],
        "coverage": {"requested_contexts": 2, "completed_contexts": 2,
                     "missing_contexts": 0, "requested_accessions": 1,
                     "verified_accessions": 1, "quarantined_accessions": 0},
        "status": "staged", "reasons": [], "issuer_group_ref_sha256": "a" * 64,
        "outcome_inputs_used": False, "diagnostic_only": True,
    }
    assert ncen._purpose_manifest(manifest) == manifest
    for forged in ("complete", "diagnostic_synthetic_complete"):
        with pytest.raises(ncen.NcenError, match="manifest_complete_status_forged"):
            ncen._purpose_manifest({**manifest, "status": forged})
    with pytest.raises(ncen.NcenError, match="manifest_files_not_closed_or_sorted"):
        ncen._purpose_manifest({**manifest, "files": manifest["files"][1:]})
    checks = {**bindings, "schema_version": ncen.DIAGNOSTIC_CHECKS_VERSION,
              "status": "staged", "validation_code_sha256": "a" * 64,
              "checks": [{"name": "pure_replay", "status": "passed", "details": "synthetic only"}]}
    assert ncen._purpose_validate_checks(checks) == checks
    with pytest.raises(ncen.NcenError, match="checks_status_invalid"):
        ncen._purpose_validate_checks({**checks, "status": "complete"})
    receipt = {**bindings, "schema_version": ncen.DIAGNOSTIC_RECEIPT_VERSION,
               "artifact_directory": "synthetic", "sha256sums_sha256": "a" * 64,
               "manifest_sha256": "a" * 64, "declaration_sha256": "a" * 64,
               "status": "diagnostic_synthetic_complete", "predecessor_receipt_sha256": None,
               "diagnostic_only": True}
    assert ncen._purpose_receipt(receipt) == receipt
    with pytest.raises(ncen.NcenError, match="receipt_status_invalid"):
        ncen._purpose_receipt({**receipt, "status": "complete"})
    with pytest.raises(ncen.NcenError):
        ncen._purpose_receipt({**receipt, "extra": True})


def _c1b2b2_fixture(tmp_path: Path, *, historical_f09: bool = False,
                    later_parsed: bool = False) -> tuple[
    ncen.DiagnosticTrustPin, dict[str, object], dict[str, Path],
    ncen.ExpectedPurposeSelections,
]:
    trust, declaration, roots = _c1a_package(
        tmp_path, selected_fixture=True, historical_f09=historical_f09,
        later_parsed=later_parsed,
    )
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    expected = ncen.replay_purpose_selections(
        baseline, trust, source_index=issuance.source_index_ref,
        membership_checkpoint=issuance.membership_ref,
    )
    declaration.update({
        "baseline_digest": baseline.digest,
        "selection_replay_version": ncen.DIAGNOSTIC_SELECTION_REPLAY_VERSION,
        "selection_expected_digest": expected.digest,
        "acceptance_scope": "synthetic_engineering_only",
        "qualification": "NOT_EVALUABLE",
    })
    return trust, declaration, roots, expected


def test_c1b2b2_private_stage_serializes_fresh_pinned_replay(tmp_path: Path) -> None:
    trust, declaration, roots, expected = _c1b2b2_fixture(tmp_path)
    output = tmp_path / "staged"
    monitor: ncen.DiagnosticResourceMonitor
    monitor = ncen.DiagnosticResourceMonitor(
        rss_probe=lambda: (512 if monitor.phase == "purpose_stage_graph" else 64) * _MIB,
    )
    artifact = ncen._stage_purpose_diagnostics_v2(
        trusted_run=trust, declaration=declaration, staging_root=output,
        code_root=ROOT, input_roots=roots, monitor=monitor,
    )
    assert artifact.status == "staged" and artifact.root == output
    inventory, contexts = ncen._purpose_serialize_selection_replay(expected)
    assert (output / "selection_inventory.json").read_bytes() == ncen._purpose_json_bytes(inventory)
    assert (output / "selections.jsonl").read_bytes() == ncen._purpose_jsonl_bytes(contexts)
    emitted = ncen._purpose_reconstruct_emitted_replay(
        json.loads((output / "selection_inventory.json").read_bytes()),
        [json.loads(row) for row in (output / "selections.jsonl").read_bytes().splitlines()],
    )
    assert ncen._diagnostic_canonical(emitted) == expected.canonical_bytes
    assert len(emitted["contexts"]) == 2
    assert sum(len(context["incidences"]) for context in emitted["contexts"]) == 24
    manifest = json.loads((output / "manifest.json").read_bytes())
    checks = json.loads((output / "checks.json").read_bytes())
    assert manifest["status"] == checks["status"] == "staged"
    for document in (manifest, checks):
        assert document["trusted_run_manifest_sha256"] == trust.manifest_sha256
        assert document["baseline_digest"] == declaration["baseline_digest"]
        assert document["selection_expected_digest"] == expected.digest
        assert document["acceptance_scope"] == "synthetic_engineering_only"
        assert document["qualification"] == "NOT_EVALUABLE"
    assert {row["path"] for row in manifest["files"]} == {
        *(name for name, _ in ncen.DIAGNOSTIC_EXPORT_JSONL),
        "selection_inventory.json", "issuer_group_ref.json", "performance.json", "checks.json",
    }
    for row in manifest["files"]:
        raw = (output / row["path"]).read_bytes()
        assert row["bytes"] == len(raw) and row["sha256"] == hashlib.sha256(raw).hexdigest()
        assert row["rows"] == (len(raw.splitlines()) if row["path"].endswith("jsonl") else 1)
    performance = json.loads((output / "performance.json").read_bytes())
    assert (performance["raw_nport_parse_calls"], performance["inventory_build_calls"],
            performance["target_vote_calls"]) == (0, 0, 0)
    assert performance["peak_rss_bytes"] == 512 * _MIB
    assert monitor.report()["supervisor_hard_cap"] is False
    for name, _kind in ncen.DIAGNOSTIC_EXPORT_JSONL:
        assert b"default_label" not in (output / name).read_bytes()
    assert not (tmp_path / "staged.receipt.json").exists()
    assert not any("readback_verified" in path.name or path.name.endswith(".partial")
                   for path in output.iterdir())


def test_c1b2b2_stage_rejects_caller_authority_and_unsafe_roots(tmp_path: Path) -> None:
    trust, declaration, roots, _expected = _c1b2b2_fixture(tmp_path)
    arguments = {"trusted_run": trust, "declaration": declaration, "staging_root": tmp_path / "stage",
                 "code_root": ROOT, "input_roots": roots,
                 "monitor": ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB)}
    assert list(inspect.signature(ncen._stage_purpose_diagnostics_v2).parameters) == list(arguments)
    for name in ("index", "cohort", "expected", "contexts", "incidences", "sources"):
        with pytest.raises(TypeError):
            ncen._stage_purpose_diagnostics_v2(**arguments, **{name: object()})
    assert not arguments["staging_root"].exists()
    staged = ncen._stage_purpose_diagnostics_v2(**{
        **arguments, "declaration": MappingProxyType(declaration),
        "staging_root": tmp_path / "read-only-declaration",
    })
    assert staged.status == "staged"
    with pytest.raises(ncen.NcenError, match="^diagnostic_trust_anchor_mismatch$"):
        ncen._stage_purpose_diagnostics_v2(**{**arguments, "trusted_run": dataclasses.replace(
            trust, manifest_sha256="f" * 64)})
    assert not arguments["staging_root"].exists()
    with pytest.raises(ncen.NcenError, match="^diagnostic_trust_anchor_missing$"):
        ncen._stage_purpose_diagnostics_v2(**{**arguments, "trusted_run": None})
    with pytest.raises(ncen.NcenError, match="^diagnostic_fixture_lane_not_exportable$"):
        ncen._stage_purpose_diagnostics_v2(**{**arguments, "declaration": {
            **declaration, "lane": "pure_fixture"}})
    with pytest.raises(ncen.NcenError, match="^diagnostic_required_pins_unverified$"):
        ncen._stage_purpose_diagnostics_v2(**{**arguments, "declaration": {
            **declaration, "lane": "sealed_source"}})
    assert not arguments["staging_root"].exists()
    arguments["staging_root"].mkdir()
    with pytest.raises(ncen.NcenError, match="^purpose_export_directory_must_be_new$"):
        ncen._stage_purpose_diagnostics_v2(**arguments)


def test_c1b2b2_interrupted_stage_leaves_no_receipt_and_cannot_resume(tmp_path: Path) -> None:
    trust, declaration, roots, _expected = _c1b2b2_fixture(tmp_path)
    output = tmp_path / "interrupted"
    monitor = ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB)
    check = monitor.check
    def interrupt_after_first_context(phase: str, artifact_id: str | None = None) -> None:
        check(phase, artifact_id)
        if phase == "purpose_stage_selection_context":
            raise KeyboardInterrupt
    monitor.check = interrupt_after_first_context
    arguments = {"trusted_run": trust, "declaration": declaration, "staging_root": output,
                 "code_root": ROOT, "input_roots": roots, "monitor": monitor}
    with pytest.raises(KeyboardInterrupt):
        ncen._stage_purpose_diagnostics_v2(**arguments)
    assert (output / "declaration.json").is_file()
    assert (output / "selections.jsonl.partial").is_file()
    assert not (output / "manifest.json").exists()
    assert not (tmp_path / "interrupted.receipt.json").exists()
    with pytest.raises(ncen.NcenError, match="^purpose_export_directory_must_be_new$"):
        ncen._stage_purpose_diagnostics_v2(**{**arguments, "monitor": ncen.DiagnosticResourceMonitor(
            rss_probe=lambda: 64 * _MIB)})


def test_c1b2b2_stage_four_seeds_same_pinned_semantics(tmp_path: Path) -> None:
    trust, declaration, roots, expected = _c1b2b2_fixture(tmp_path)
    parameters = json.dumps({"manifest_path": str(trust.manifest_path),
                             "sha256": trust.manifest_sha256, "bytes": trust.manifest_size,
                             "declaration": declaration,
                             "roots": {key: str(value) for key, value in roots.items()},
                             "stage_parent": str(tmp_path)})
    outputs = []
    for seed in (1, 2, 977, 31337):
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-s", "tests/test_bond_default_ncen_purposes.py",
             "-k", "c1b2b2_seed_child"], cwd=ROOT,
            env={**os.environ, "PYTHONHASHSEED": str(seed), "NCEN_C1B2B2_SEED_ARGS": parameters},
            capture_output=True, text=True, check=False, timeout=90,
        )
        assert completed.returncode == 0, completed.stderr
        outputs.append(next(line.split("STAGE_SEMANTICS:", 1)[1]
                            for line in completed.stdout.splitlines()
                            if line.startswith("STAGE_SEMANTICS:")))
    assert outputs == [outputs[0]] * 4
    assert outputs[0].split(":")[0] == expected.digest


def test_c1b2b2_seed_child() -> None:
    raw = os.environ.get("NCEN_C1B2B2_SEED_ARGS")
    if raw is None:
        return
    parameters = json.loads(raw)
    trust = ncen.DiagnosticTrustPin(Path(parameters["manifest_path"]), parameters["sha256"],
                                    parameters["bytes"])
    output = Path(parameters["stage_parent"]) / f"seed-{os.environ['PYTHONHASHSEED']}"
    staged = ncen._stage_purpose_diagnostics_v2(
        trusted_run=trust, declaration=parameters["declaration"], staging_root=output,
        code_root=ROOT, input_roots={key: Path(value) for key, value in parameters["roots"].items()},
        monitor=ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB),
    )
    semantic = hashlib.sha256(b"".join((output / name).read_bytes()
                                     for name, _kind in ncen.DIAGNOSTIC_EXPORT_JSONL)
                              + (output / "selection_inventory.json").read_bytes()).hexdigest()
    print("STAGE_SEMANTICS:" + staged.selection_expected_digest + ":" + semantic)


def _c1b2b3a_stage(tmp_path: Path, *, historical_f09: bool = False,
                   later_parsed: bool = False) -> tuple[
                                                   ncen.DiagnosticTrustPin, dict[str, Path],
                                                   ncen.ExpectedPurposeSelections, Path]:
    trust, declaration, roots, expected = _c1b2b2_fixture(
        tmp_path, historical_f09=historical_f09, later_parsed=later_parsed)
    output = tmp_path / "staged"
    ncen._stage_purpose_diagnostics_v2(
        trusted_run=trust, declaration=declaration, staging_root=output,
        code_root=ROOT, input_roots=roots,
        monitor=ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB),
    )
    return trust, roots, expected, output


def _c1b2b3a_readback(trust: ncen.DiagnosticTrustPin, roots: dict[str, Path],
                       output: Path) -> ncen.SyntheticReadbackEvidence:
    return ncen._validate_purpose_staging_v2(
        trusted_run=trust, staging_root=output, code_root=ROOT, input_roots=roots,
        monitor=ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB),
    )


def _c1b2b3a_reseal(output: Path, changed: dict[str, object]) -> None:
    manifest = json.loads((output / "manifest.json").read_bytes())
    for name, document in changed.items():
        raw = (ncen._purpose_jsonl_bytes(document) if name.endswith(".jsonl")
               else ncen._purpose_json_bytes(document))
        (output / name).write_bytes(raw)
        row = next(row for row in manifest["files"] if row["path"] == name)
        row["sha256"] = hashlib.sha256(raw).hexdigest()
        row["bytes"] = len(raw)
        row["rows"] = len(document) if name.endswith(".jsonl") else 1
    (output / "manifest.json").write_bytes(ncen._purpose_json_bytes(manifest))
    sums = "".join(f"{_stage2a_sha(output / name)}  {name}\n"
                   for name in sorted({*(row["path"] for row in manifest["files"]),
                                       "declaration.json", "manifest.json"}))
    (output / "SHA256SUMS").write_bytes(sums.encode("ascii"))


def _c1b2b3b1_historical_attack(tmp_path: Path, attack: str) -> None:
    """Port the pre-C1a export mutations, never the old public acceptance path."""
    import shutil

    tmp_path.mkdir(parents=True)
    trust, roots, _expected, staged = _c1b2b3a_stage(tmp_path, historical_f09=attack == "f09_fold")

    def copied(name: str) -> Path:
        destination = tmp_path / name
        shutil.copytree(staged, destination)
        return destination

    def refused(output: Path, code: str) -> None:
        with pytest.raises(ncen.NcenError, match=f"^{code}$"):
            _c1b2b3a_readback(trust, roots, output)
        assert not (output.parent / f"{output.name}.receipt.json").exists()

    if attack == "path_traversal":
        # Old test_export_rejects_path_tamper_orphans_duplicates_unsorted_and_symlinks.
        traversal = copied("traversal")
        manifest_path = traversal / "manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        manifest["files"][0]["path"] = "../cohort.jsonl"
        manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
        refused(traversal, "manifest_file_path_unsafe")
    elif attack == "path_symlink":
        symlink = copied("symlink")
        checks_path = symlink / "checks.json"
        checks_path.unlink()
        try:
            checks_path.symlink_to(staged / "checks.json")
        except OSError as exc:
            if os.name == "nt" and getattr(exc, "winerror", None) in (5, 1314):
                pytest.fail(f"historical symlink attack requires symlink permission: {exc}")
            raise
        refused(symlink, "purpose_export_non_regular_file")
    elif attack == "orphan":
        output = copied("orphan")
        path = output / "incidences.jsonl"
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        rows[0]["source_row_id"] = "ncenrow:source_row:" + "9" * 64
        payload = {key: value for key, value in rows[0].items()
                   if key not in ("schema_version", "record_type", "record_id")}
        rows[0] = ncen._purpose_record("incidence", payload)
        contexts = [json.loads(line) for line in (output / "contexts.jsonl").read_bytes().splitlines()]
        context_order = {item["context_id"]: index for index, item in enumerate(contexts)}
        rows.sort(key=lambda item: (context_order[item["context_id"]], item["record_id"]))
        _c1b2b3a_reseal(output, {"incidences.jsonl": rows})
        refused(output, "diagnostic_selection_cross_file_mismatch")
    elif attack == "duplicate":
        output = copied("duplicate")
        rows = [json.loads(line) for line in (output / "sources.jsonl").read_bytes().splitlines()]
        _c1b2b3a_reseal(output, {"sources.jsonl": [*rows, rows[0]]})
        refused(output, "diagnostic_duplicate_record_id:sources.jsonl")
    elif attack == "unsorted":
        output = copied("unsorted")
        rows = [json.loads(line) for line in (output / "nodes.jsonl").read_bytes().splitlines()]
        assert len(rows) > 1
        _c1b2b3a_reseal(output, {"nodes.jsonl": list(reversed(rows))})
        refused(output, "diagnostic_selection_cross_file_mismatch")
    elif attack == "raw_header":
        output = copied("raw-header")
        header = roots["ncen-source"] / "submission.txt"
        header.write_bytes(header.read_bytes() + b"FORGED\n")
        refused(output, "diagnostic_artifact_size_mismatch")
    elif attack == "interrupted":
        output = copied("interrupted")
        (output / "manifest.json").unlink()
        refused(output, "diagnostic_export_incomplete")
        declaration = json.loads((output / "declaration.json").read_bytes())
        with pytest.raises(ncen.NcenError, match="^purpose_export_directory_must_be_new$"):
            ncen._stage_purpose_diagnostics_v2(
                trusted_run=trust, declaration=declaration, staging_root=output,
                code_root=ROOT, input_roots=roots,
                monitor=ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB),
            )
    elif attack == "forged_status_receipt":
        output = copied("forged-status")
        manifest = json.loads((output / "manifest.json").read_bytes())
        manifest["status"] = "complete"
        (output / "manifest.json").write_bytes(ncen._purpose_json_bytes(manifest))
        refused(output, "manifest_complete_status_forged")
        receipt = copied("forged-receipt")
        (receipt / "forged.receipt.json").write_bytes(ncen._purpose_json_bytes({
            "schema_version": ncen.DIAGNOSTIC_RECEIPT_VERSION,
            "status": "complete", "artifact_directory": receipt.name,
        }))
        refused(receipt, "purpose_export_artifact_inventory_not_closed")
    elif attack == "unknown_key":
        output = copied("unknown-key")
        rows = [json.loads(line) for line in (output / "nodes.jsonl").read_bytes().splitlines()]
        rows[0]["unexpected"] = "forbidden"
        _c1b2b3a_reseal(output, {"nodes.jsonl": rows})
        refused(output, "diagnostic_node_keys_invalid")
    elif attack == "source_graph":
        # Old test_validator_replays_source_family_and_cohort_derivations: six distinct forgeries.
        variants = (
            ("source", "sources.jsonl", "diagnostic_selection_cross_file_mismatch"),
            ("quarantine", "exclusions.jsonl", "diagnostic_selection_cross_file_mismatch"),
            ("family", "reported_families.jsonl", "reported_family_declared_invalid"),
            ("cohort", "cohort.jsonl", "diagnostic_cohort_digest_mismatch"),
            ("node", "nodes.jsonl", "diagnostic_selection_cross_file_mismatch"),
            ("context", "contexts.jsonl", "diagnostic_selection_cross_file_mismatch"),
        )
        contexts = [json.loads(line) for line in (staged / "contexts.jsonl").read_bytes().splitlines()]
        context_order = {item["context_id"]: index for index, item in enumerate(contexts)}
        for variant, name, reason in variants:
            output = copied(f"{variant}-forgery")
            rows = [json.loads(line) for line in (output / name).read_bytes().splitlines()]
            if variant == "source":
                invented = dict(rows[0])
                invented["locator"] = "invented/nonexistent#row=999"
                invented["raw_row_sha256"] = "9" * 64
                rows.append(ncen._purpose_record("source_row", {
                    key: value for key, value in invented.items()
                    if key not in ("schema_version", "record_type", "record_id")}))
            elif variant == "quarantine":
                assert any(row["context_id"] is None for row in rows)
                rows = [row for row in rows if row["context_id"] is not None]
            else:
                index = 0
                if variant == "family":
                    index = next(index for index, row in enumerate(rows) if row["state"] == "declared_family")
                    rows[index]["answer"] = "N"
                    rows[index]["label_id"] = "not-a-derived-label"
                elif variant == "cohort":
                    rows[index]["fund_keys"] = ["S999999999"]
                    rows[index]["known_at"] = "2099-01-01T00:00:00.000000Z"
                elif variant == "node":
                    rows[index]["selected_accession"] = "0000000001-26-999999"
                else:
                    rows[index]["knowledge_time"] = "2099-01-01T00:00:00.000000Z"
                rows[index] = ncen._purpose_record(rows[index]["record_type"], {
                    key: value for key, value in rows[index].items()
                    if key not in ("schema_version", "record_type", "record_id")})
            if variant != "quarantine":
                rows.sort(key=lambda row: (context_order.get(row.get("context_id"), -1), row["record_id"])
                          if variant in {"family", "node", "context"} else row["record_id"])
            _c1b2b3a_reseal(output, {name: rows})
            refused(output, reason)
    elif attack == "resealed_selection":
        # A locally resealed export cannot erase selected evidence and all 24 incidences.
        output = copied("resealed-selection")
        selections = [json.loads(line) for line in (output / "selections.jsonl").read_bytes().splitlines()]
        removed = 0
        for index, context in enumerate(selections):
            for selected in context["selections"]:
                removed += len(selected["selected_source_row_ids"])
                selected["selected_source_row_ids"] = []
            context["incidences"] = []
            selections[index] = ncen._purpose_selection_context_record({
                key: value for key, value in context.items()
                if key not in ("schema_version", "record_type", "record_id")})
        assert removed > 0
        incidences = (output / "incidences.jsonl").read_bytes().splitlines()
        assert len(incidences) == 24
        _c1b2b3a_reseal(output, {"selections.jsonl": selections, "incidences.jsonl": []})
        refused(output, "diagnostic_selection_replay_mismatch")
    elif attack == "f09_fold":
        # Old test_review_f09_writer_readback_rejects_resealed_uncertain_merge.
        output = copied("f09-fold")
        contexts = [json.loads(line) for line in (output / "contexts.jsonl").read_bytes().splitlines()]
        incidences = [json.loads(line) for line in (output / "incidences.jsonl").read_bytes().splitlines()]
        assert len(contexts) == 2
        uncertain = [item for item in incidences if item["context_id"] == contexts[0]["context_id"]]
        attested = [item for item in incidences if item["context_id"] == contexts[1]["context_id"]]
        assert uncertain and all(item["attestation"] == "uncertain" for item in uncertain)
        assert attested and all(item["attestation"] == "attested" for item in attested)
        assert {item["key_id"] for item in uncertain if item["key_id"]} & {
            item["key_id"] for item in attested if item["key_id"]
        }
        groups_path = output / "fold_groups.jsonl"
        memberships_path = output / "fold_memberships.jsonl"
        groups = [json.loads(line) for line in groups_path.read_bytes().splitlines()]
        memberships = [json.loads(line) for line in memberships_path.read_bytes().splitlines()]
        scope = ncen.fold_scope_id(
            mode="historical_reconstruction", ablation_id="uncertain_expanded",
            contexts=tuple((dt.date.fromisoformat(item["R"]),
                            ncen._purpose_parse_timestamp(item["K"], code="historical_fold_K"),
                            item["inventory_digest"], item["context_id"]) for item in contexts),
        )
        assert len({item["fold_group_id"] for item in groups if item["fold_scope_id"] == scope}) == 2
        assert len({item["cik"] for item in memberships if item["fold_scope_id"] == scope}) == 2
        assert {item["has_unknown_dependence"] for item in groups if item["fold_scope_id"] == scope} == {
            True, False,
        }
        members = tuple(sorted(item["cik"] for item in memberships if item["fold_scope_id"] == scope))
        merged_id = ncen.fold_group_id(scope_id=scope, members=members)
        merged_group = ncen._purpose_record("fold_group", {
            "fold_scope_id": scope, "fold_group_id": merged_id, "member_count": 2,
            "has_unknown_dependence": True, "usable_for_independence_claim": False,
        })
        merged_memberships = [ncen._purpose_record("fold_membership", {
            "fold_scope_id": scope, "fold_group_id": merged_id, "cik": member,
        }) for member in members]
        groups = [item for item in groups if item["fold_scope_id"] != scope] + [merged_group]
        memberships = [item for item in memberships if item["fold_scope_id"] != scope] + merged_memberships
        _c1b2b3a_reseal(output, {
            "fold_groups.jsonl": sorted(groups, key=lambda item: item["record_id"]),
            "fold_memberships.jsonl": sorted(memberships, key=lambda item: item["record_id"]),
        })
        refused(output, "diagnostic_fold_group_replay_mismatch")
    else:
        raise AssertionError(f"unknown historical attack {attack}")


def test_resealed_selection_and_incidences_refused(tmp_path: Path) -> None:
    _c1b2b3b1_historical_attack(tmp_path / "resealed-selection", "resealed_selection")


def test_c1b2b3a_fresh_process_readback_four_seeds(tmp_path: Path) -> None:
    trust, roots, expected, output = _c1b2b3a_stage(tmp_path)
    staged_bytes = {item.name: _stage2a_sha(item) for item in output.iterdir()}
    parameters = json.dumps({"trust": str(trust.manifest_path), "sha256": trust.manifest_sha256,
                             "bytes": trust.manifest_size, "roots": {key: str(value) for key, value in roots.items()},
                             "output": str(output)})
    results = []
    for seed in (1, 2, 977, 31337):
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-s", "tests/test_bond_default_ncen_purposes.py",
             "-k", "c1b2b3a_readback_child"], cwd=ROOT,
            env={**os.environ, "PYTHONHASHSEED": str(seed), "NCEN_C1B2B3A_CHILD": parameters},
            capture_output=True, text=True, check=False, timeout=90,
        )
        assert completed.returncode == 0, completed.stderr
        results.append(next(line.removeprefix("READBACK_SEMANTICS:")
                            for line in completed.stdout.splitlines()
                            if line.startswith("READBACK_SEMANTICS:")))
    assert results == [expected.digest] * 4
    assert {item.name: _stage2a_sha(item) for item in output.iterdir()} == staged_bytes
    assert not (tmp_path / "staged.receipt.json").exists()
    assert json.loads((output / "manifest.json").read_bytes())["status"] == "staged"
    print("SYNTHETIC_STAGE_READBACK:" + _stage2a_sha(output / "manifest.json") + ":" + expected.digest)


def test_c1b2b3a_readback_child() -> None:
    raw = os.environ.get("NCEN_C1B2B3A_CHILD")
    if raw is None:
        return
    parameters = json.loads(raw)
    pin = ncen.DiagnosticTrustPin(Path(parameters["trust"]), parameters["sha256"], parameters["bytes"])
    output = Path(parameters["output"])
    evidence = _c1b2b3a_readback(pin, {key: Path(path) for key, path in parameters["roots"].items()}, output)
    assert evidence.status == "diagnostic_synthetic_readback_verified"
    assert evidence.acceptance_scope == "synthetic_engineering_only"
    assert evidence.publicly_accepted is False
    assert (evidence.qualification, evidence.resource_qualification, evidence.resume_qualification) == (
        "NOT_EVALUABLE", "NOT_EVALUABLE", "NOT_EVALUABLE")
    assert evidence.context_count == 2 and evidence.incidence_count == 24
    assert evidence.manifest_sha256 == _stage2a_sha(output / "manifest.json")
    assert all(type(getattr(evidence, field.name)) in (str, int, bool)
               for field in dataclasses.fields(evidence))
    assert not (output.parent / "staged.receipt.json").exists()
    print("READBACK_SEMANTICS:" + evidence.selection_replay_digest)


@pytest.mark.parametrize("name,code", (
    ("selection_inventory.json", "diagnostic_selection_inventory_missing"),
    ("selections.jsonl", "diagnostic_selection_records_missing"),
    ("manifest.json", "diagnostic_export_incomplete"),
    ("checks.json", "diagnostic_export_incomplete"),
))
def test_c1b2b3a_missing_control_or_selection(
    tmp_path: Path, name: str, code: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    (output / name).unlink()
    with pytest.raises(ncen.NcenError, match=f"^{code}$"):
        _c1b2b3a_readback(trust, roots, output)
    assert not (tmp_path / "staged.receipt.json").exists()


def test_c1b2b3a_interrupted_stage_is_incomplete(tmp_path: Path) -> None:
    trust, declaration, roots, _expected = _c1b2b2_fixture(tmp_path)
    output = tmp_path / "interrupted"
    monitor = ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB)
    check = monitor.check

    def interrupt(phase: str, artifact_id: str | None = None) -> None:
        check(phase, artifact_id)
        if phase == "purpose_stage_selection_context":
            raise KeyboardInterrupt

    monitor.check = interrupt
    with pytest.raises(KeyboardInterrupt):
        ncen._stage_purpose_diagnostics_v2(
            trusted_run=trust, declaration=declaration, staging_root=output,
            code_root=ROOT, input_roots=roots, monitor=monitor,
        )
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_incomplete$"):
        _c1b2b3a_readback(trust, roots, output)
    assert not (tmp_path / "interrupted.receipt.json").exists()


@pytest.mark.parametrize("attack,code", (
    ("inventory_keys", "diagnostic_selection_records_invalid"),
    ("inventory_type", "diagnostic_selection_records_invalid"),
    ("selection_envelope", "diagnostic_selection_records_invalid"),
    ("selection_type", "diagnostic_selection_records_invalid"),
    ("reordered_contexts", "diagnostic_context_coverage_mismatch"),
    ("duplicate_context", "diagnostic_context_coverage_mismatch"),
    ("missing_context", "diagnostic_context_coverage_mismatch"),
    ("extra_context", "diagnostic_context_coverage_mismatch"),
    ("duplicate_cik", "diagnostic_context_coverage_mismatch"),
    ("missing_cik", "diagnostic_context_coverage_mismatch"),
    ("reordered_cik", "diagnostic_context_coverage_mismatch"),
    ("extra_cik", "diagnostic_context_coverage_mismatch"),
    ("selected_source", "diagnostic_selection_replay_mismatch"),
    ("all_incidences", "diagnostic_selection_replay_mismatch"),
    ("cross_file_source", "diagnostic_selection_cross_file_mismatch"),
    ("cross_file_incidence", "diagnostic_selection_cross_file_mismatch"),
    ("graph_only", "diagnostic_component_evidence_digest_mismatch"),
))
def test_c1b2b3a_resealed_attack_refused(
    tmp_path: Path, attack: str, code: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    inventory = json.loads((output / "selection_inventory.json").read_bytes())
    selections = [json.loads(line) for line in (output / "selections.jsonl").read_bytes().splitlines()]
    changed: dict[str, object] = {}
    if attack == "inventory_keys":
        inventory.pop("candidate_inventory")
        changed["selection_inventory.json"] = inventory
    elif attack == "inventory_type":
        inventory["candidate_inventory"] = []
        changed["selection_inventory.json"] = inventory
    elif attack == "selection_envelope":
        selections[0]["record_id"] = "ncenrow:selection_context:" + "0" * 64
    elif attack == "selection_type":
        selections[0]["selections"][0]["fund_keys"] = [1]
    elif attack == "reordered_contexts":
        selections.reverse()
    elif attack == "duplicate_context":
        selections[1] = copy.deepcopy(selections[0])
    elif attack == "missing_context":
        selections.pop()
    elif attack == "extra_context":
        selections.append(copy.deepcopy(selections[1]))
    elif attack == "duplicate_cik":
        selections[0]["selections"].append(copy.deepcopy(selections[0]["selections"][0]))
    elif attack == "missing_cik":
        selections[0]["selections"].pop()
    elif attack == "reordered_cik":
        selections[0]["selections"].reverse()
    elif attack == "extra_cik":
        selections[0]["selections"].append(copy.deepcopy(selections[0]["selections"][-1]))
        selections[0]["selections"][-1]["cik"] = cik(99)
        selections[0]["selections"][-1]["selection_payload"][1] = cik(99)
    elif attack == "selected_source":
        item = next(item for context in selections for item in context["selections"]
                    if item["selected_source_row_ids"])
        item["selected_source_row_ids"].pop()
    elif attack == "all_incidences":
        for context in selections:
            context["incidences"] = []
        changed["incidences.jsonl"] = []
    elif attack in ("cross_file_source", "cross_file_incidence"):
        name = "sources.jsonl" if attack == "cross_file_source" else "incidences.jsonl"
        rows = [json.loads(line) for line in (output / name).read_bytes().splitlines()]
        rows[0]["locator" if attack == "cross_file_source" else "role"] = (
            "forged:locator" if attack == "cross_file_source" else "terminated_sub")
        payload = {key: value for key, value in rows[0].items()
                   if key not in ("schema_version", "record_type", "record_id")}
        rows[0] = ncen._purpose_record(rows[0]["record_type"], payload)
        rows.sort(key=lambda row: row["record_id"])
        changed[name] = rows
    else:
        rows = [json.loads(line) for line in (output / "components.jsonl").read_bytes().splitlines()]
        rows[0]["evidence_digest"] = "f" * 64
        payload = {key: value for key, value in rows[0].items()
                   if key not in ("schema_version", "record_type", "record_id")}
        rows[0] = ncen._purpose_record("component", payload)
        changed["components.jsonl"] = ncen._purpose_sort_records(
            "components.jsonl", rows,
            context_index={row["context_id"]: index for index, row in enumerate(selections)},
            ablation_index={spec.ablation_id: index for index, spec in enumerate(ncen.DIAGNOSTIC_ABLATIONS)},
            report_index={row["R"]: index for index, row in enumerate(selections)},
        )
    if attack in {"selection_envelope", "selection_type", "reordered_contexts", "duplicate_context", "missing_context",
                  "extra_context", "duplicate_cik", "missing_cik", "reordered_cik", "extra_cik",
                  "selected_source", "all_incidences"}:
        if attack in {"selection_type", "duplicate_cik", "missing_cik", "reordered_cik", "extra_cik",
                      "selected_source", "all_incidences"}:
            for index in (range(len(selections)) if attack == "all_incidences" else (0,)):
                item = selections[index]
                selections[index] = ncen._purpose_selection_context_record({
                    key: value for key, value in item.items()
                    if key not in ("schema_version", "record_type", "record_id")})
        changed["selections.jsonl"] = selections
    _c1b2b3a_reseal(output, changed)
    with pytest.raises(ncen.NcenError, match=f"^{code}$"):
        _c1b2b3a_readback(trust, roots, output)


def test_c1b2b3a_external_competitor_pin_refusal(tmp_path: Path) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    manifest = json.loads(trust.manifest_path.read_bytes())
    index_pin = next(role["pins"][0] for role in manifest["roles"]
                     if role["role"] == "ncen_index_artifacts")
    index_path = roots[index_pin["root_id"]] / index_pin["path"]
    raw = index_path.read_bytes()
    index_path.write_bytes(b"".join(line for line in raw.splitlines(keepends=True)
                                    if b"0000000001-26-000005" not in line))
    with pytest.raises(ncen.NcenError, match="diagnostic_(checkpoint_pin_mismatch|candidate_universe_mismatch)"):
        _c1b2b3a_readback(trust, roots, output)


def test_c1b2b3a_old_schema_and_public_boundary_stay_closed(tmp_path: Path) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.validate_purpose_export(output, trusted_run=trust, code_root=ROOT, input_roots=roots)
    public_output = tmp_path / "public"
    declaration = json.loads((output / "declaration.json").read_bytes())
    with pytest.raises(ncen.NcenError, match="^C1_INCOMPLETE$"):
        ncen.write_purpose_diagnostics(trusted_run=trust, declaration=declaration,
                                       output_root=public_output, code_root=ROOT, input_roots=roots)
    assert not public_output.exists()
    declaration["schema_version"] = "ncen_purpose_diagnostics_declaration_v1"
    (output / "declaration.json").write_bytes(ncen._purpose_json_bytes(declaration))
    with pytest.raises(ncen.NcenError, match="^diagnostic_export_schema_unsupported$"):
        _c1b2b3a_readback(trust, roots, output)


def test_c1b2b3b2_fresh_process_reissues_q3_without_writer_handle(tmp_path: Path) -> None:
    trust, roots, expected, output = _c1b2b3a_stage(tmp_path)
    parameters = json.dumps({
        "trust": str(trust.manifest_path), "sha256": trust.manifest_sha256,
        "bytes": trust.manifest_size, "roots": {key: str(value) for key, value in roots.items()},
        "output": str(output),
    })
    assert "expected" not in inspect.signature(ncen._validate_purpose_staging_v2).parameters
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-s", "tests/test_bond_default_ncen_purposes.py",
         "-k", "c1b2b3b2_fresh_process_child"], cwd=ROOT,
        env={**os.environ, "NCEN_C1B2B3B2_CHILD": parameters},
        capture_output=True, text=True, check=False, timeout=90,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert f"FRESH_READBACK:{expected.digest}" in completed.stdout
    assert not (tmp_path / "staged.receipt.json").exists()


def test_c1b2b3b2_fresh_process_child(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = os.environ.get("NCEN_C1B2B3B2_CHILD")
    if raw is None:
        return
    args = json.loads(raw)
    trust = ncen.DiagnosticTrustPin(Path(args["trust"]), args["sha256"], args["bytes"])
    roots = {key: Path(value) for key, value in args["roots"].items()}
    first = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(first)]
    expected = ncen.replay_purpose_selections(
        first, trust, source_index=issuance.source_index_ref,
        membership_checkpoint=issuance.membership_ref,
    )
    loaded: list[ncen.DiagnosticBaselineCheckpoint] = []
    original = ncen.read_diagnostic_baseline_checkpoint
    original_replay = ncen.replay_purpose_selections
    replay_calls: list[tuple[object, object, object, object]] = []

    def reissue(*args: object, **kwargs: object) -> ncen.DiagnosticBaselineCheckpoint:
        baseline = original(*args, **kwargs)
        loaded.append(baseline)
        return baseline

    def replay(baseline: ncen.DiagnosticBaselineCheckpoint, trusted_run: ncen.DiagnosticTrustPin,
               *, source_index: object, membership_checkpoint: object,
               **kwargs: object) -> ncen.ExpectedPurposeSelections:
        replay_calls.append((baseline, trusted_run, source_index, membership_checkpoint))
        return original_replay(baseline, trusted_run, source_index=source_index,
                               membership_checkpoint=membership_checkpoint, **kwargs)

    monkeypatch.setattr(ncen, "read_diagnostic_baseline_checkpoint", reissue)
    monkeypatch.setattr(ncen, "replay_purpose_selections", replay)
    evidence = _c1b2b3a_readback(trust, roots, Path(args["output"]))
    assert len(loaded) == 1 and loaded[0] is not first
    assert loaded[0].digest == first.digest
    fresh_issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(loaded[0])]
    assert len(replay_calls) == 1
    assert replay_calls[0][0] is loaded[0] and replay_calls[0][1] is trust
    assert replay_calls[0][2] is fresh_issuance.source_index_ref
    assert replay_calls[0][3] is fresh_issuance.membership_ref
    assert evidence.selection_replay_digest == expected.digest
    assert evidence.status == "diagnostic_synthetic_readback_verified"
    print("FRESH_READBACK:" + evidence.selection_replay_digest)


@pytest.mark.parametrize("attack,code", (
    ("context", "diagnostic_context_coverage_mismatch"),
    ("member", "diagnostic_context_coverage_mismatch"),
    ("fund_key", "diagnostic_selection_replay_mismatch"),
    ("dependency", "diagnostic_selection_replay_mismatch"),
))
def test_c1b2b3b2_fixed_pin_coverage_refuses_before_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, attack: str, code: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    selections = [json.loads(line) for line in (output / "selections.jsonl").read_bytes().splitlines()]
    if attack == "context":
        assert len(selections) == 2
        selections.pop()
    elif attack == "member":
        assert len(selections[0]["selections"]) == 2
        removed = selections[0]["selections"].pop()["cik"]
        for field in ("nodes", "reported_families", "incidences", "exclusions"):
            selections[0][field] = [row for row in selections[0][field] if row["cik"] != removed]
    elif attack == "fund_key":
        target = next(context for context in selections
                      if any(len(row["fund_keys"]) > 1 for row in context["selections"]))
        member = next(row for row in target["selections"] if len(row["fund_keys"]) > 1)
        member["fund_keys"].pop()
    else:
        target = next(context for context in selections
                      if any(row["dependencies"] for row in context["selections"]))
        member = next(row for row in target["selections"] if row["dependencies"])
        member["dependencies"].pop()
        member["selection_payload"][8] = [
            [item["accession"], item["role"], item["knowledge_time"]]
            for item in member["dependencies"]]
    if attack != "context":
        changed_context = selections[0] if attack == "member" else target
        index = selections.index(changed_context)
        selections[index] = ncen._purpose_selection_context_record({
            key: value for key, value in changed_context.items()
            if key not in ("schema_version", "record_type", "record_id")})
    _c1b2b3a_reseal(output, {"selections.jsonl": selections})
    inventory = json.loads((output / "selection_inventory.json").read_bytes())
    reconstructed = ncen._purpose_reconstruct_emitted_replay(inventory, selections)
    assert len(reconstructed["contexts"]) == len(selections)
    monkeypatch.setattr(ncen, "_purpose_validate_semantics",
                        lambda *_args, **_kwargs: pytest.fail("graph reached before selection refusal"))
    with pytest.raises(ncen.NcenError, match=f"^{code}$"):
        _c1b2b3a_readback(trust, roots, output)
    assert not (tmp_path / "staged.receipt.json").exists()


@pytest.mark.parametrize("candidate", ("competitor", "unknown_period", "after_k", "f6_scope"))
def test_c1b2b3b2_fixed_pin_candidate_and_f6_tamper(
    tmp_path: Path, candidate: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    if candidate == "f6_scope":
        path = roots["acquisition"] / "scope.json"
        scope = json.loads(path.read_bytes())
        assert scope["requests"]
        scope["requests"].pop()
        path.write_bytes(ncen._purpose_json_bytes(scope))
        reason = "diagnostic_acquisition_sha256_mismatch:scope.json"
    else:
        path = roots["independent"] / "ncen_index_entries.jsonl"
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        accession = {"competitor": "0000000002-26-000003",
                     "unknown_period": "0000000003-26-000004",
                     "after_k": "0000000001-26-000005"}[candidate]
        assert sum(row["entry"] is not None and row["entry"]["accession_number"] == accession
                   for row in rows) == 1
        rows = [row for row in rows if row["entry"] is None
                or row["entry"]["accession_number"] != accession]
        path.write_bytes(ncen._purpose_jsonl_bytes(rows))
        checkpoint_path = roots["independent"] / "checkpoint.json"
        checkpoint = json.loads(checkpoint_path.read_bytes())
        pin = next(row for row in checkpoint["files"] if row["path"] == path.name)
        pin.update(sha256=_stage2a_sha(path), bytes=path.stat().st_size, rows=len(rows))
        checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
        reason = "diagnostic_checkpoint_pin_mismatch"
    with pytest.raises(ncen.NcenError, match=f"^{reason}$"):
        _c1b2b3a_readback(trust, roots, output)
    assert not (tmp_path / "staged.receipt.json").exists()


@pytest.mark.parametrize("foreign", ("trust", "source", "membership", "carrier"))
def test_c1b2b3b2_foreign_q3_authority_never_replays(tmp_path: Path, foreign: str) -> None:
    trust, _declaration, roots, _expected = _c1b2b2_fixture(tmp_path)
    baseline = ncen.read_diagnostic_baseline_checkpoint(trust, code_root=ROOT, input_roots=roots)
    issuance = ncen._DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    arguments = {"source_index": issuance.source_index_ref,
                 "membership_checkpoint": issuance.membership_ref}
    if foreign == "trust":
        trust = dataclasses.replace(trust)
    elif foreign == "source":
        arguments["source_index"] = dataclasses.replace(issuance.source_index_ref)
    elif foreign == "membership":
        arguments["membership_checkpoint"] = dataclasses.replace(issuance.membership_ref)
    else:
        baseline = object.__new__(ncen.DiagnosticBaselineCheckpoint)
    with pytest.raises(ncen.NcenError, match="^diagnostic_baseline_unadmitted$"):
        ncen.replay_purpose_selections(baseline, trust, **arguments)


@pytest.mark.parametrize("name,field", (("sources.jsonl", "locator"),
                                         ("exclusions.jsonl", "reason")))
def test_c1b2b3b2_resealed_cross_file_not_selection_truth(
    tmp_path: Path, name: str, field: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    rows = [json.loads(line) for line in (output / name).read_bytes().splitlines()]
    assert rows
    if name == "exclusions.jsonl":
        assert any(row["context_id"] is None for row in rows)
        rows = [row for row in rows if row["context_id"] is not None]
    else:
        rows[0][field] = "forged-cross-file"
        rows[0] = ncen._purpose_record(rows[0]["record_type"], {
            key: value for key, value in rows[0].items()
            if key not in ("schema_version", "record_type", "record_id")})
    rows.sort(key=lambda row: row["record_id"])
    selection_sha = _stage2a_sha(output / "selections.jsonl")
    _c1b2b3a_reseal(output, {name: rows})
    assert _stage2a_sha(output / "selections.jsonl") == selection_sha
    with pytest.raises(ncen.NcenError, match="^diagnostic_selection_cross_file_mismatch$"):
        _c1b2b3a_readback(trust, roots, output)


def test_c1b2b3b2_synthetic_evidence_never_qualifies_resource(tmp_path: Path) -> None:
    trust, declaration, roots, expected = _c1b2b2_fixture(tmp_path)
    monitor = ncen.DiagnosticResourceMonitor(rss_probe=lambda: 512 * _MIB)
    output = tmp_path / "staged"
    ncen._stage_purpose_diagnostics_v2(trusted_run=trust, declaration=declaration,
        staging_root=output, code_root=ROOT, input_roots=roots, monitor=monitor)
    evidence = _c1b2b3a_readback(trust, roots, output)
    assert evidence.selection_replay_digest == expected.digest
    assert evidence.status == "diagnostic_synthetic_readback_verified"
    assert evidence.acceptance_scope == "synthetic_engineering_only"
    assert evidence.publicly_accepted is False
    assert (evidence.qualification, evidence.resource_qualification,
            evidence.resume_qualification) == ("NOT_EVALUABLE",) * 3
    assert json.loads((output / "performance.json").read_bytes())["peak_rss_bytes"] == 512 * _MIB
    assert not (tmp_path / "staged.receipt.json").exists()


def test_c1b2b3b2_distinct_later_parsed_competitor_under_original_pin(tmp_path: Path) -> None:
    trust, roots, expected, output = _c1b2b3a_stage(tmp_path, later_parsed=True)
    accession = "0000000001-26-000055"
    copies = [json.loads(line) for line in (roots["independent"] / "ncen_copies.jsonl").read_bytes().splitlines()]
    assert any(row["parse_state"] == "parsed" and row["filing"] is not None
               and row["filing"]["accession_number"] == accession for row in copies)
    assert {ACCESSION, accession} <= {row["filing"]["accession_number"] for row in copies
                                   if row["parse_state"] == "parsed" and row["filing"] is not None}
    assert any(selection["selected_accession"] == accession for context in expected.records["contexts"]
               for selection in context["selections"])
    assert _c1b2b3a_readback(trust, roots, output).selection_replay_digest == expected.digest

    copies = [row for row in copies if row["filing"] is None
              or row["filing"]["accession_number"] != accession]
    copy_path = roots["independent"] / "ncen_copies.jsonl"
    copy_path.write_bytes(ncen._purpose_jsonl_bytes(copies))
    checkpoint_path = roots["independent"] / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_bytes())
    descriptor = next(row for row in checkpoint["files"] if row["path"] == copy_path.name)
    descriptor.update(sha256=_stage2a_sha(copy_path), bytes=copy_path.stat().st_size, rows=len(copies))
    checkpoint_path.write_bytes(ncen._purpose_json_bytes(checkpoint))
    with pytest.raises(ncen.NcenError, match="^diagnostic_checkpoint_pin_mismatch$"):
        _c1b2b3a_readback(trust, roots, output)
    assert not (tmp_path / "staged.receipt.json").exists()


@pytest.mark.parametrize("lane,code", (("sealed_source", "diagnostic_declared_pin_mismatch"),
                                       ("unknown_lane", "diagnostic_lane_mismatch")))
def test_c1b2b3b2_resealed_lane_relabel_never_yields_synthetic_evidence(
    tmp_path: Path, lane: str, code: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    declaration_path = output / "declaration.json"
    declaration = json.loads(declaration_path.read_bytes())
    declaration["lane"] = lane
    declaration_path.write_bytes(ncen._purpose_json_bytes(declaration))
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["declaration_sha256"] = _stage2a_sha(declaration_path)
    manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
    _c1b2b3a_reseal(output, {})
    with pytest.raises(ncen.NcenError, match=f"^{code}$"):
        _c1b2b3a_readback(trust, roots, output)
    assert not (tmp_path / "staged.receipt.json").exists()


@pytest.mark.parametrize("control,keys,bad", (
    ("declaration.json", (), []),
    ("declaration.json", ("lane",), []),
    ("declaration.json", ("ncen_source_manifest", "kind"), {}),
    ("declaration.json", ("contexts", 0, "mode"), []),
    ("declaration.json", ("ablations", 0), None),
    ("declaration.json", ("limits", "max_contexts"), True),
    ("declaration.json", ("pin_roles", 0, "pins", 0, "bytes"), False),
    ("manifest.json", ("files", 0, "path"), []),
    ("manifest.json", (), None),
    ("manifest.json", ("files", 0, "record_type"), {}),
    ("manifest.json", ("files", 0, "rows"), True),
    ("manifest.json", ("runtime", "platform"), []),
    ("manifest.json", ("coverage", "requested_contexts"), False),
    ("checks.json", ("checks", 0, "name"), []),
    ("checks.json", (), None),
    ("checks.json", ("checks", 0, "details"), None),
    ("checks.json", ("checks", 0), None),
))
def test_c1b2b_stage5_malformed_control_types_refused_before_q3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, control: str,
    keys: tuple[str | int, ...], bad: object,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    path = output / control
    original_names = [line.split(b"  ", 1)[1].decode("ascii")
                      for line in (output / "SHA256SUMS").read_bytes().splitlines()]
    document = json.loads(path.read_bytes())
    if keys:
        parent = document
        for key in keys[:-1]:
            parent = parent[key]
        parent[keys[-1]] = bad
    else:
        document = bad
    if control == "checks.json":
        _c1b2b3a_reseal(output, {control: document})
    else:
        path.write_bytes(ncen._purpose_json_bytes(document))
        if control == "declaration.json":
            manifest_path = output / "manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            manifest["declaration_sha256"] = _stage2a_sha(path)
            manifest_path.write_bytes(ncen._purpose_json_bytes(manifest))
        sums = "".join(f"{_stage2a_sha(output / name)}  {name}\n" for name in original_names)
        (output / "SHA256SUMS").write_bytes(sums.encode("ascii"))
    monkeypatch.setattr(ncen, "read_diagnostic_baseline_checkpoint",
                        lambda *_args, **_kwargs: pytest.fail("Q3 reached for malformed control"))
    monkeypatch.setattr(ncen, "_purpose_validate_semantics",
                        lambda *_args, **_kwargs: pytest.fail("graph reached for malformed control"))
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        _c1b2b3a_readback(trust, roots, output)


def test_c1b2b_stage5_stager_rejects_wrong_scalar_before_q3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    trust, declaration, roots, _expected = _c1b2b2_fixture(tmp_path)
    declaration["lane"] = []
    output = tmp_path / "bad-control"
    monkeypatch.setattr(ncen, "read_diagnostic_baseline_checkpoint",
                        lambda *_args, **_kwargs: pytest.fail("Q3 reached for malformed declaration"))
    with pytest.raises(ncen.NcenError, match="^diagnostic_pinned_input_invalid$"):
        ncen._stage_purpose_diagnostics_v2(
            trusted_run=trust, declaration=declaration, staging_root=output,
            code_root=ROOT, input_roots=roots,
            monitor=ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB),
        )
    assert not output.exists()


def test_c1b2b_stage5_manifest_evidence_is_from_parsed_bytes(
    tmp_path: Path,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    original_sha = _stage2a_sha(output / "manifest.json")
    monitor = ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB)
    check = monitor.check

    def replace_after_validation(phase: str, artifact_id: str | None = None) -> None:
        check(phase, artifact_id)
        if phase == "purpose_readback_selection_replay":
            (output / "manifest.json").write_bytes(b'{"unvalidated":true}\n')

    monitor.check = replace_after_validation
    try:
        evidence = ncen._validate_purpose_staging_v2(
            trusted_run=trust, staging_root=output, code_root=ROOT,
            input_roots=roots, monitor=monitor,
        )
    except ncen.NcenError:
        pass  # A concurrent change may instead invalidate the stage.
    else:
        assert evidence.manifest_sha256 == original_sha
        assert evidence.manifest_sha256 != _stage2a_sha(output / "manifest.json")
    assert not (tmp_path / "staged.receipt.json").exists()


@pytest.mark.parametrize("name", ("nodes.jsonl", "performance.json"))
def test_c1b2b_stage5_data_parse_hashes_the_same_bytes(
    tmp_path: Path, name: str,
) -> None:
    trust, roots, _expected, output = _c1b2b3a_stage(tmp_path)
    path = output / name
    if name.endswith(".jsonl"):
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        rows[0]["selected_accession"] = "0000000001-26-999999"
        rows[0] = ncen._purpose_record("node", {
            key: value for key, value in rows[0].items()
            if key not in ("schema_version", "record_type", "record_id")})
        replacement = ncen._purpose_jsonl_bytes(rows)
    else:
        performance = json.loads(path.read_bytes())
        performance["peak_rss_bytes"] += 1
        replacement = ncen._purpose_json_bytes(performance)
    monitor = ncen.DiagnosticResourceMonitor(rss_probe=lambda: 64 * _MIB)
    check = monitor.check

    def replace_after_hashes(phase: str, artifact_id: str | None = None) -> None:
        check(phase, artifact_id)
        if phase == "purpose_readback_selection_replay":
            path.write_bytes(replacement)

    monitor.check = replace_after_hashes
    with pytest.raises(ncen.NcenError, match="^manifest_staged_file_mismatch$"):
        ncen._validate_purpose_staging_v2(
            trusted_run=trust, staging_root=output, code_root=ROOT,
            input_roots=roots, monitor=monitor,
        )
