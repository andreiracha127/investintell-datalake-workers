"""Fail-closed Regulation S distribution-series resolver contracts."""

from __future__ import annotations

from datetime import date

import pytest


def _approved_mapping(*, validated_source: bool = True):
    from src.bonds.distribution_series import (
        distribution_snapshot_content_hash,
        DistributionMappingSnapshot,
        DistributionSnapshotApproval,
        DistributionParserObservation,
        DistributionPairDecision,
        DistributionPairIdentifier,
    )

    decision = DistributionPairDecision(
        decision_id="decision-1",
        snapshot_id="snapshot-approved",
        decision_state="approved",
        source_observation_id="parser-observation-1",
        valid_from=date(2024, 1, 1),
    )
    observations = (
        DistributionParserObservation(
            "parser-observation-1", "source-evidence-1", "parser-v1", "page=1;block=1",
            "Rule 144A CUSIP", "123456789", "123456789",
            "validated" if validated_source else "candidate",
        ),
        DistributionParserObservation(
            "parser-observation-2", "source-evidence-1", "parser-v1", "page=1;block=1",
            "Regulation S CUSIP", "987654321", "987654321",
            "validated" if validated_source else "candidate",
        ),
    )
    identifiers = (
        DistributionPairIdentifier(
            identifier_id="144a-cusip",
            decision_id=decision.decision_id,
            source_observation_id="parser-observation-1",
            distribution_rule="rule_144a",
            identifier_kind="cusip9",
            identifier_value="123456789",
            tenure="permanent",
            valid_from=date(2024, 1, 1),
        ),
        DistributionPairIdentifier(
            identifier_id="regs-cusip",
            decision_id=decision.decision_id,
            source_observation_id="parser-observation-2",
            distribution_rule="reg_s",
            identifier_kind="cusip9",
            identifier_value="987654321",
            tenure="permanent",
            valid_from=date(2024, 1, 1),
        ),
    )
    snapshot = DistributionMappingSnapshot(
        snapshot_id="snapshot-approved",
        status="draft",
        content_hash=distribution_snapshot_content_hash("snapshot-approved", (decision,), identifiers),
    )
    approval = DistributionSnapshotApproval(snapshot.snapshot_id, snapshot.content_hash)
    return snapshot, approval, decision, identifiers, observations


def _approval_for_composition(snapshot, decisions, identifiers):
    from src.bonds.distribution_series import (
        DistributionMappingSnapshot,
        DistributionSnapshotApproval,
        distribution_snapshot_content_hash,
    )

    content_hash = distribution_snapshot_content_hash(snapshot.snapshot_id, decisions, identifiers)
    closed_snapshot = DistributionMappingSnapshot(snapshot.snapshot_id, "draft", content_hash)
    return closed_snapshot, DistributionSnapshotApproval(closed_snapshot.snapshot_id, content_hash)


def test_resolver_returns_only_exact_approved_same_pair_reg_s_cusip() -> None:
    from src.bonds.distribution_series import resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()

    result = resolve_reg_s_cusip(
        snapshot_id=snapshot.snapshot_id,
        as_of=date(2024, 6, 1),
        reference_cusip9="123456789",
        snapshots=(snapshot,), approvals=(approval,), parser_observations=observations,
        decisions=(decision,),
        identifiers=identifiers,
    )

    assert result.snapshot_id == "snapshot-approved"
    assert result.decision_id == "decision-1"
    assert result.reference_cusip9 == "123456789"
    assert result.reg_s_cusip9 == "987654321"
    assert result.reg_s_isin is None


def test_resolver_returns_same_block_validated_reg_s_isin() -> None:
    from src.bonds.distribution_series import DistributionPairIdentifier, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    isin_observation = observations[1].__class__(
        "parser-observation-isin", "source-evidence-1", "parser-v1", "page=1;block=1",
        "Regulation S ISIN", "XS1234567890", "XS1234567890", "validated",
    )
    isin_identifier = DistributionPairIdentifier(
        "regs-isin", decision.decision_id, isin_observation.parser_observation_id,
        "reg_s", "isin", "XS1234567890", "permanent", date(2024, 1, 1),
    )
    identifiers += (isin_identifier,)
    snapshot, approval = _approval_for_composition(snapshot, (decision,), identifiers)

    result = resolve_reg_s_cusip(
        snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
        snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
        parser_observations=observations + (isin_observation,),
    )

    assert result.reg_s_cusip9 == "987654321"
    assert result.reg_s_isin == "XS1234567890"


def test_resolver_ignores_cross_block_reg_s_isin() -> None:
    from src.bonds.distribution_series import DistributionPairIdentifier, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    isin_observation = observations[1].__class__(
        "parser-observation-isin", "source-evidence-1", "parser-v1", "page=1;block=2",
        "Regulation S ISIN", "XS1234567890", "XS1234567890", "validated",
    )
    isin_identifier = DistributionPairIdentifier(
        "regs-isin", decision.decision_id, isin_observation.parser_observation_id,
        "reg_s", "isin", "XS1234567890", "permanent", date(2024, 1, 1),
    )
    identifiers += (isin_identifier,)
    snapshot, approval = _approval_for_composition(snapshot, (decision,), identifiers)

    result = resolve_reg_s_cusip(
        snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
        snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
        parser_observations=observations + (isin_observation,),
    )

    assert result.reg_s_cusip9 == "987654321"
    assert result.reg_s_isin is None


def test_resolver_refuses_conflicting_same_block_reg_s_isins() -> None:
    from src.bonds.distribution_series import (
        AmbiguousDistributionMappingError,
        DistributionPairIdentifier,
        resolve_reg_s_cusip,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    isin_observations = (
        observations[1].__class__(
            "parser-observation-isin-1", "source-evidence-1", "parser-v1", "page=1;block=1",
            "Regulation S ISIN", "XS1234567890", "XS1234567890", "validated",
        ),
        observations[1].__class__(
            "parser-observation-isin-2", "source-evidence-1", "parser-v1", "page=1;block=1",
            "Regulation S ISIN", "XS0987654321", "XS0987654321", "validated",
        ),
    )
    isin_identifiers = tuple(
        DistributionPairIdentifier(
            f"regs-isin-{index}", decision.decision_id, observation.parser_observation_id,
            "reg_s", "isin", observation.normalized_value, "permanent", date(2024, 1, 1),
        )
        for index, observation in enumerate(isin_observations, start=1)
    )
    identifiers += isin_identifiers
    snapshot, approval = _approval_for_composition(snapshot, (decision,), identifiers)

    with pytest.raises(AmbiguousDistributionMappingError, match="ambiguous_mapping"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
            parser_observations=observations + isin_observations,
        )


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("Rule 144A CINS", ("rule_144a", "cusip9")),
        ("Rule144A CUSIP", ("rule_144a", "cusip9")),
        ("Regulation S CUSIP", ("reg_s", "cusip9")),
        ("RegulationS CINS", ("reg_s", "cusip9")),
        ("Reg S ISIN", ("reg_s", "isin")),
        ("RegS Common Code", ("reg_s", "common_code")),
        ("144A Common Code", ("rule_144a", "common_code")),
        ("Issuer CUSIP", None),
    ],
)
def test_identifier_label_taxonomy_is_exact_and_never_identifier_shape_inference(
    label: str, expected: tuple[str, str] | None,
) -> None:
    from src.bonds.distribution_series import identifier_kind_from_source_label

    assert identifier_kind_from_source_label(label) == expected


def test_resolver_refuses_unknown_snapshot() -> None:
    from src.bonds.distribution_series import InvalidDistributionSnapshotError, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()

    with pytest.raises(InvalidDistributionSnapshotError, match="snapshot_not_found"):
        resolve_reg_s_cusip(
            snapshot_id="missing", as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers, parser_observations=observations,
        )


def test_resolver_refuses_approval_for_a_different_content_hash() -> None:
    from src.bonds.distribution_series import (
        DistributionSnapshotApproval,
        InvalidDistributionSnapshotError,
        resolve_reg_s_cusip,
    )

    snapshot, _approval, decision, identifiers, observations = _approved_mapping()
    wrong_approval = DistributionSnapshotApproval(snapshot.snapshot_id, "b" * 64)

    with pytest.raises(InvalidDistributionSnapshotError, match="snapshot_not_approved"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(wrong_approval,), decisions=(decision,), identifiers=identifiers,
            parser_observations=observations,
        )


def test_resolver_refuses_forged_matching_stored_hash_for_different_composition() -> None:
    from src.bonds.distribution_series import (
        DistributionMappingSnapshot,
        DistributionSnapshotApproval,
        InvalidDistributionSnapshotError,
        resolve_reg_s_cusip,
    )

    snapshot, _approval, decision, identifiers, observations = _approved_mapping()
    forged_snapshot = DistributionMappingSnapshot(snapshot.snapshot_id, "draft", "f" * 64)
    forged_approval = DistributionSnapshotApproval(snapshot.snapshot_id, "f" * 64)

    with pytest.raises(InvalidDistributionSnapshotError, match="snapshot_content_hash_mismatch"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(forged_snapshot,), approvals=(forged_approval,), decisions=(decision,),
            identifiers=identifiers, parser_observations=observations,
        )


def test_distribution_snapshot_content_hash_changes_with_composition() -> None:
    from src.bonds.distribution_series import (
        DistributionPairDecision,
        DistributionPairIdentifier,
        distribution_snapshot_content_hash,
    )

    snapshot, _approval, decision, identifiers, _observations = _approved_mapping()
    baseline = distribution_snapshot_content_hash(snapshot.snapshot_id, (decision,), identifiers)
    changed_decision = DistributionPairDecision(
        decision.decision_id, decision.snapshot_id, "revoked", decision.source_observation_id,
        decision.valid_from, decision.valid_to, decision.pair_key,
    )
    changed_identifier = DistributionPairIdentifier(
        identifiers[1].identifier_id, identifiers[1].decision_id, identifiers[1].source_observation_id,
        identifiers[1].distribution_rule, identifiers[1].identifier_kind, "111111111",
        identifiers[1].tenure, identifiers[1].valid_from, identifiers[1].valid_to,
    )

    assert baseline != distribution_snapshot_content_hash(snapshot.snapshot_id, (changed_decision,), identifiers)
    assert baseline != distribution_snapshot_content_hash(snapshot.snapshot_id, (decision,), identifiers[:1])
    assert baseline != distribution_snapshot_content_hash(
        snapshot.snapshot_id, (decision,), (identifiers[0], changed_identifier)
    )


def test_arbitrary_matching_draft_and_approval_hash_is_refused() -> None:
    from src.bonds.distribution_series import (
        DistributionMappingSnapshot,
        DistributionSnapshotApproval,
        ImmutableRegistryConflictError,
        validate_distribution_snapshot_approval,
    )

    snapshot, _approval, decision, identifiers, _observations = _approved_mapping()
    forged_snapshot = DistributionMappingSnapshot(snapshot.snapshot_id, "draft", "a" * 64)
    forged_approval = DistributionSnapshotApproval(snapshot.snapshot_id, "a" * 64)

    with pytest.raises(ImmutableRegistryConflictError, match="snapshot_content_hash_mismatch"):
        validate_distribution_snapshot_approval(
            forged_snapshot, forged_approval, (decision,), identifiers
        )


@pytest.mark.parametrize("status", ["draft", "revoked"])
def test_resolver_refuses_draft_or_revoked_snapshot(status: str) -> None:
    from src.bonds.distribution_series import (
        DistributionMappingSnapshot,
        InvalidDistributionSnapshotError,
        resolve_reg_s_cusip,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    snapshot = DistributionMappingSnapshot(snapshot.snapshot_id, status, snapshot.content_hash)
    approvals = () if status == "draft" else (approval,)

    with pytest.raises(InvalidDistributionSnapshotError, match="snapshot_not_approved"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=approvals, decisions=(decision,), identifiers=identifiers, parser_observations=observations,
        )


def test_resolver_refuses_when_decision_has_no_validated_source() -> None:
    from src.bonds.distribution_series import NoValidatedDistributionSourceError, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping(validated_source=False)

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers, parser_observations=observations,
        )


def test_resolver_keeps_isin_or_common_code_pairs_governed_but_cusip_ineligible() -> None:
    from src.bonds.distribution_series import (
        DistributionPairIdentifier,
        NoSupportedRegSCusipError,
        resolve_reg_s_cusip,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    observations = observations[:1] + (
        observations[1].__class__(
            "parser-observation-isin", "source-evidence-1", "parser-v1", "page=1;block=1",
            "Regulation S ISIN", "XS1234567890", "XS1234567890", "validated",
        ),
        observations[1].__class__(
            "parser-observation-common-code", "source-evidence-1", "parser-v1", "page=1;block=1",
            "Regulation S Common Code", "123456789", "123456789", "validated",
        ),
    )
    identifiers = identifiers[:1] + (
        DistributionPairIdentifier(
            identifier_id="regs-isin",
            decision_id=decision.decision_id,
            source_observation_id="parser-observation-isin",
            distribution_rule="reg_s",
            identifier_kind="isin",
            identifier_value="XS1234567890",
            tenure="temporary",
            valid_from=date(2024, 1, 1),
        ),
        DistributionPairIdentifier(
            identifier_id="regs-common-code",
            decision_id=decision.decision_id,
            source_observation_id="parser-observation-common-code",
            distribution_rule="reg_s",
            identifier_kind="common_code",
            identifier_value="123456789",
            tenure="not_stated",
            valid_from=date(2024, 1, 1),
        ),
    )
    snapshot, approval = _approval_for_composition(snapshot, (decision,), identifiers)

    with pytest.raises(NoSupportedRegSCusipError, match="no_supported_reg_s_cusip"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers, parser_observations=observations,
        )


def test_resolver_refuses_ambiguous_reg_s_cusips() -> None:
    from src.bonds.distribution_series import (
        AmbiguousDistributionMappingError,
        DistributionPairDecision,
        DistributionPairIdentifier,
        resolve_reg_s_cusip,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    second = DistributionPairDecision(
        decision_id="decision-2", snapshot_id=snapshot.snapshot_id, decision_state="approved",
        source_observation_id="parser-observation-3",
        valid_from=date(2024, 1, 1),
    )
    second_identifiers = (
        DistributionPairIdentifier("second-144a", second.decision_id, "parser-observation-3", "rule_144a", "cusip9", "123456789", "permanent", date(2024, 1, 1)),
        DistributionPairIdentifier("second-regs", second.decision_id, "parser-observation-4", "reg_s", "cusip9", "111111111", "permanent", date(2024, 1, 1)),
    )
    observations += (
        observations[0].__class__("parser-observation-3", "source-evidence-2", "parser-v1", "page=2;block=1", "Rule 144A CUSIP", "123456789", "123456789", "validated"),
        observations[0].__class__("parser-observation-4", "source-evidence-2", "parser-v1", "page=2;block=1", "Regulation S CUSIP", "111111111", "111111111", "validated"),
    )
    snapshot, approval = _approval_for_composition(
        snapshot, (decision, second), identifiers + second_identifiers
    )

    with pytest.raises(AmbiguousDistributionMappingError, match="ambiguous_mapping"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision, second), identifiers=identifiers + second_identifiers,
            parser_observations=observations,
        )


def test_resolver_does_not_infer_from_unrelated_security_attributes() -> None:
    from src.bonds.distribution_series import NoValidatedDistributionSourceError, resolve_reg_s_cusip

    snapshot, _approval, _decision, _identifiers, _observations = _approved_mapping()
    snapshot, approval = _approval_for_composition(snapshot, (), ())

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(), identifiers=(), parser_observations=(),
        )


def test_resolver_obeys_half_open_identifier_validity() -> None:
    from src.bonds.distribution_series import NoValidatedDistributionSourceError, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    ending = identifiers[1].__class__(
        identifiers[1].identifier_id, identifiers[1].decision_id, identifiers[1].source_observation_id,
        identifiers[1].distribution_rule, identifiers[1].identifier_kind, identifiers[1].identifier_value, identifiers[1].tenure,
        identifiers[1].valid_from, date(2024, 6, 1),
    )
    snapshot, approval = _approval_for_composition(snapshot, (decision,), (identifiers[0], ending))

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=(identifiers[0], ending), parser_observations=observations,
        )


@pytest.mark.parametrize(
    ("source_evidence_id", "block_locator"),
    [("different-document", "page=1;block=1"), ("source-evidence-1", "page=1;block=2")],
)
def test_resolver_refuses_identifier_evidence_not_in_the_same_document_block(
    source_evidence_id: str, block_locator: str,
) -> None:
    from src.bonds.distribution_series import NoValidatedDistributionSourceError, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    mismatched = observations[1].__class__(
        observations[1].parser_observation_id, source_evidence_id, observations[1].parser_version,
        block_locator, observations[1].exact_source_label, observations[1].source_value,
        observations[1].normalized_value, "validated",
    )

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
            parser_observations=(observations[0], mismatched),
        )


def test_resolver_refuses_when_selected_reg_s_identifier_lacks_parser_evidence() -> None:
    from src.bonds.distribution_series import NoValidatedDistributionSourceError, resolve_reg_s_cusip

    snapshot, approval, decision, identifiers, observations = _approved_mapping()

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
            parser_observations=(observations[0],),
        )


@pytest.mark.parametrize("mismatch", ["value", "kind", "side"])
def test_resolver_requires_identifier_to_match_its_validated_explicit_label(mismatch: str) -> None:
    from src.bonds.distribution_series import (
        DistributionPairIdentifier,
        NoValidatedDistributionSourceError,
        resolve_reg_s_cusip,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    adjusted_identifiers = identifiers
    adjusted_observations = observations
    reference = "123456789"
    if mismatch == "value":
        adjusted_identifiers = (
            DistributionPairIdentifier("wrong-value", decision.decision_id, identifiers[0].source_observation_id, "rule_144a", "cusip9", "111111111", "permanent", date(2024, 1, 1)),
            identifiers[1],
        )
        reference = "111111111"
    elif mismatch == "kind":
        adjusted_identifiers = (
            identifiers[0],
            DistributionPairIdentifier("wrong-kind", decision.decision_id, identifiers[1].source_observation_id, "reg_s", "isin", "987654321", "permanent", date(2024, 1, 1)),
        )
    else:
        adjusted_observations = (
            observations[0].__class__(
                observations[0].parser_observation_id, observations[0].source_evidence_id,
                observations[0].parser_version, observations[0].block_locator, "Regulation S CUSIP",
                observations[0].source_value, observations[0].normalized_value, "validated",
            ),
            observations[1],
        )
    snapshot, approval = _approval_for_composition(
        snapshot, (decision,), adjusted_identifiers
    )

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9=reference,
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,),
            identifiers=adjusted_identifiers, parser_observations=adjusted_observations,
        )


def test_bulk_resolver_returns_explicit_partial_mapping_and_typed_omission() -> None:
    from src.bonds.distribution_series import resolve_reg_s_cusip_map

    snapshot, approval, decision, identifiers, observations = _approved_mapping()

    result = resolve_reg_s_cusip_map(
        snapshot_id=snapshot.snapshot_id,
        as_of=date(2024, 6, 1),
        reference_cusip9s=("123456789", "unmapped"),
        snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
        parser_observations=observations,
    )

    assert result.resolutions["123456789"].reg_s_cusip9 == "987654321"
    assert result.reason_by_reference == {"UNMAPPED": "no_validated_source"}


def test_bulk_resolver_marks_execution_cusip_collision_ambiguous() -> None:
    from src.bonds.distribution_series import (
        DistributionPairDecision,
        DistributionPairIdentifier,
        DistributionParserObservation,
        resolve_reg_s_cusip_map,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    other = DistributionPairDecision(
        "decision-2", snapshot.snapshot_id, "approved", "parser-observation-3", date(2024, 1, 1),
    )
    other_identifiers = (
        DistributionPairIdentifier("other-144a", other.decision_id, "parser-observation-3", "rule_144a", "cusip9", "222222222", "permanent", date(2024, 1, 1)),
        DistributionPairIdentifier("other-regs", other.decision_id, "parser-observation-4", "reg_s", "cusip9", "987654321", "permanent", date(2024, 1, 1)),
    )
    other_observations = (
        DistributionParserObservation("parser-observation-3", "source-evidence-2", "parser-v1", "page=2;block=1", "Rule 144A CUSIP", "222222222", "222222222", "validated"),
        DistributionParserObservation("parser-observation-4", "source-evidence-2", "parser-v1", "page=2;block=1", "Regulation S CUSIP", "987654321", "987654321", "validated"),
    )
    snapshot, approval = _approval_for_composition(
        snapshot, (decision, other), identifiers + other_identifiers
    )

    result = resolve_reg_s_cusip_map(
        snapshot_id=snapshot.snapshot_id,
        as_of=date(2024, 6, 1),
        reference_cusip9s=("123456789", "222222222"),
        snapshots=(snapshot,), approvals=(approval,), decisions=(decision, other), identifiers=identifiers + other_identifiers,
        parser_observations=observations + other_observations,
    )

    assert result.resolutions == {}
    assert result.reason_by_reference == {
        "123456789": "ambiguous_mapping_collision",
        "222222222": "ambiguous_mapping_collision",
    }


@pytest.mark.parametrize(
    ("kind", "value", "expected"),
    [
        ("cusip9", "AB12CD345", True),
        ("cusip9", "AB12", False),
        ("cusip9", "000000000", False),
        ("cusip9", "XXXXXXXXX", False),
        ("cusip9", "NNNNNNNNN", False),
        ("cusip9", "999999999", False),
        ("isin", "USG35906AC33", True),
        ("isin", "XS3049816013", True),
        ("isin", "usg35906ac33", False),
        ("isin", "XXXXXXXXXXXX", False),
        ("isin", "123456789012", False),
        ("common_code", "304981598", True),
        ("common_code", "30498159A", False),
    ],
)
def test_identifier_syntax_is_strict_without_new_check_digit_inference(
    kind: str, value: str, expected: bool,
) -> None:
    from src.bonds.distribution_series import identifier_value_has_valid_syntax

    assert identifier_value_has_valid_syntax(kind, value) is expected


def test_resolver_refuses_validated_but_malformed_execution_cusip() -> None:
    from src.bonds.distribution_series import (
        DistributionPairIdentifier,
        DistributionParserObservation,
        NoValidatedDistributionSourceError,
        resolve_reg_s_cusip,
    )

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    malformed_observation = DistributionParserObservation(
        observations[1].parser_observation_id, observations[1].source_evidence_id,
        observations[1].parser_version, observations[1].block_locator, observations[1].exact_source_label,
        observations[1].source_value, "BAD", "validated",
    )
    malformed_identifier = DistributionPairIdentifier(
        identifiers[1].identifier_id, identifiers[1].decision_id, identifiers[1].source_observation_id,
        identifiers[1].distribution_rule, identifiers[1].identifier_kind, "BAD", identifiers[1].tenure,
        identifiers[1].valid_from, identifiers[1].valid_to,
    )
    snapshot, approval = _approval_for_composition(
        snapshot, (decision,), (identifiers[0], malformed_identifier)
    )

    with pytest.raises(NoValidatedDistributionSourceError, match="no_validated_source"):
        resolve_reg_s_cusip(
            snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            snapshots=(snapshot,), approvals=(approval,), decisions=(decision,),
            identifiers=(identifiers[0], malformed_identifier),
            parser_observations=(observations[0], malformed_observation),
        )


def test_bulk_resolver_builds_fact_index_once_for_a_stage_sized_reference_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.bonds.distribution_series as registry

    snapshot, approval, decision, identifiers, observations = _approved_mapping()
    calls = 0
    build_index = registry._build_resolution_index

    def counted_build_index(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        return build_index(*args, **kwargs)

    monkeypatch.setattr(registry, "_build_resolution_index", counted_build_index)
    monkeypatch.setattr(
        registry,
        "resolve_reg_s_cusip",
        lambda **_: (_ for _ in ()).throw(AssertionError("bulk must not invoke scalar resolver")),
    )

    result = registry.resolve_reg_s_cusip_map(
        snapshot_id=snapshot.snapshot_id,
        as_of=date(2024, 6, 1),
        reference_cusip9s=("123456789", *(f"X{index:08d}" for index in range(10_205))),
        snapshots=(snapshot,), approvals=(approval,), decisions=(decision,), identifiers=identifiers,
        parser_observations=observations,
    )

    assert calls == 1
    assert result.resolutions["123456789"].reg_s_cusip9 == "987654321"
    assert len(result.reason_by_reference) == 10_205
