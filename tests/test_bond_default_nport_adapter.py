"""Offline candidate-only N-PORT inventory and ProposalEvidence adapter tests."""

from __future__ import annotations

import dataclasses
import datetime as dt
import importlib.util
import os
import subprocess
import sys
import uuid
from collections.abc import Iterable, Mapping
from pathlib import Path

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from src.bonds.default_events import contracts as c
from src.bonds.default_events import nport, public_ratings, publication
from src.bonds.default_events import sec_acquisition as sa

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "adapter_dera_builder",
    ROOT / "tests" / "fixtures" / "bond_default_events" / "nport" / "dera_builder.py",
)
builder = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(builder)

UTC = dt.timezone.utc
RETRIEVED = dt.datetime(2026, 9, 25, 3, 14, tzinfo=UTC)
K = dt.datetime(2026, 9, 25, 23, 0, tzinfo=UTC)
REPORT_DATE = dt.date(2021, 3, 31)
CUSIP_A = builder.make_cusip("03783310")
CUSIP_B = builder.make_cusip("59491810")
FRAME_NAMES = (
    "source_packages",
    "observations",
    "event_links",
    "adjudications",
    "ncen_filings",
    "family_contexts",
    "family_evidence",
    "proposal_evidence",
    "exchange_relations",
)


def _filing(
    accession: str = "0000000101-21-000001",
    *,
    cik: str = "0000000101",
    series: str | None = "S000000001",
    report_date: str = "31-MAR-2021",
    filing_date: str = "20-MAY-2021",
    sub_type: str = "NPORT-P",
) -> object:
    return builder.Filing(
        accession,
        cik=cik,
        series=series,
        report_date=report_date,
        filing_date=filing_date,
        sub_type=sub_type,
    )


def _dera(
    tmp_path: Path,
    filings: Iterable[object],
    *,
    label: str = "2021q1_nport.zip",
    vintage: str = "2021q1",
    retrieved_at: dt.datetime = RETRIEVED,
) -> nport.DeraPackageResult:
    token = uuid.uuid4().hex[:8]
    path = tmp_path / f"{token}_{label}"
    sha = builder.build_zip(path, builder.tables(filings), vintage=vintage)
    return nport.parse_dera_package(
        path,
        expected_sha256=sha,
        package_label=label,
        output_dir=tmp_path / f"out_{token}",
        work_dir=tmp_path / "work",
        retrieved_at=retrieved_at,
        duckdb_threads=2,
        duckdb_memory_limit="512MB",
    )


def _header(
    accession: str,
    *,
    cik: str = "0000000101",
    form: str = "NPORT-P",
    acceptance_raw: str = "20210520163000",
    retrieved_at: dt.datetime = RETRIEVED,
) -> sa.AcceptanceHeader:
    raw = (
        f"<SEC-HEADER>\n<ACCEPTANCE-DATETIME>{acceptance_raw}\n"
        f"<ACCESSION-NUMBER>{accession}\n<TYPE>{form}\n<CIK>{cik}\n</SEC-HEADER>"
    ).encode()
    return sa.parse_acceptance_header(
        raw,
        accession_number=accession,
        url="https://www.sec.gov/synthetic",
        document_sha256="0" * 64,
        retrieved_at=retrieved_at,
    )


def _xml_bytes(
    *,
    cusip: str = CUSIP_A,
    cik: str = "0000000101",
    series: str = "S000000001",
    report_date: str = "2021-03-31",
    is_default: str = "Y",
    sub_type: str = "NPORT-P",
) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<edgarSubmission xmlns="http://www.sec.gov/edgar/nport" '
        'xmlns:com="http://www.sec.gov/edgar/common" '
        'xmlns:ncom="http://www.sec.gov/edgar/nportcommon">'
        f"<headerData><submissionType>{sub_type}</submissionType><filerInfo><filer>"
        f"<issuerCredentials><cik>{cik}</cik></issuerCredentials>"
        '</filer></filerInfo></headerData>'
        f"<formData><genInfo><regCik>{cik}</regCik><seriesId>{series}</seriesId>"
        f"<repPdDate>{report_date}</repPdDate></genInfo><invstOrSecs>"
        '<invstOrSec><name>Synthetic</name><lei>N/A</lei><title>NOTE</title>'
        f"<cusip>{cusip}</cusip><balance>1000</balance><units>PA</units>"
        '<curCd>USD</curCd><valUSD>990</valUSD><assetCat>DBT</assetCat>'
        '<issuerCat>CORP</issuerCat><debtSec><maturityDt>2030-01-15</maturityDt>'
        '<couponKind>Fixed</couponKind><annualizedRt>5</annualizedRt>'
        f"<isDefault>{is_default}</isDefault><areIntrstPmntsInArrs>N</areIntrstPmntsInArrs>"
        '<isPaidKind>N</isPaidKind></debtSec></invstOrSec>'
        '</invstOrSecs></formData></edgarSubmission>'
    ).encode()


def _public(
    *,
    accession: str = "0000000101-21-000001",
    cusip: str = CUSIP_A,
    cik: str = "0000000101",
    series: str = "S000000001",
    report_date: str = "2021-03-31",
    is_default: str = "Y",
    acceptance_raw: str = "20210520163000",
    retrieved_at: dt.datetime = RETRIEVED,
    first_seen_at: dt.datetime | None = None,
    sub_type: str = "NPORT-P",
) -> nport.PublicAccessionResult:
    return nport.parse_public_accession(
        _xml_bytes(
            cusip=cusip,
            cik=cik,
            series=series,
            report_date=report_date,
            is_default=is_default,
            sub_type=sub_type,
        ),
        header=_header(
            accession,
            cik=cik,
            form=sub_type,
            acceptance_raw=acceptance_raw,
            retrieved_at=retrieved_at,
        ),
        retrieved_at=retrieved_at,
        official_url="https://www.sec.gov/synthetic/primary_doc.xml",
        first_seen_at=first_seen_at,
    )


def _source(
    result: nport.DeraPackageResult | nport.PublicAccessionResult,
    *,
    headers: Mapping[str, sa.AcceptanceHeader] | None = None,
    reconciliations: Iterable[nport.DeraReconciliation] = (),
) -> nport.NportSourceInput:
    return nport.NportSourceInput(
        result=result,
        acceptance_headers={} if headers is None else headers,
        reconciliations=tuple(reconciliations),
    )


def _inventory(
    sources: Iterable[nport.NportSourceInput],
    *,
    report_dates: Iterable[dt.date] = (REPORT_DATE,),
    cutoff: dt.datetime = K,
    mode: str = "current_run",
    max_scan_rows: int = 256,
    max_output_rows: int = 256,
) -> nport.PersistedNportInventory:
    return nport.materialize_nport_inventory(
        sources,
        report_dates=report_dates,
        knowledge_cutoff=cutoff,
        knowledge_mode=mode,
        max_scan_rows=max_scan_rows,
        max_output_rows=max_output_rows,
    )


def _frames(
    inventory: nport.PersistedNportInventory,
    **overrides: Iterable[object],
) -> dict[str, tuple[object, ...]]:
    frames: dict[str, tuple[object, ...]] = {name: () for name in FRAME_NAMES}
    frames["source_packages"] = inventory.source_packages
    frames["observations"] = inventory.observations
    for name, rows in overrides.items():
        frames[name] = tuple(rows)
    return frames


def _candidate(inventory: nport.PersistedNportInventory, cusip: str = CUSIP_A) -> nport.StateProposal:
    votes = nport.build_votes(inventory.observations, resolution=inventory.resolution)
    result = nport.build_consensus_states(
        votes.votes,
        knowledge_cutoff=inventory.knowledge_cutoff,
        family_evidence=None,
        corroborations=(),
        resolved_votes=(),
        disputed_families=votes.disputed_families,
    )
    return next(proposal for proposal in result.proposals if proposal.cusip9 == cusip)


def _error_reason(callable_: object, reason: str) -> None:
    with pytest.raises(nport.NportAdapterError) as exc:
        callable_()
    assert exc.value.reason == reason


def _replace_observation(row: c.CreditObservation, **changes: object) -> c.CreditObservation:
    record = row.to_record()
    record.update(changes)
    return c.CreditObservation.from_record(record)


def test_adapter_error_detail_is_deterministic_and_sanitized() -> None:
    error = nport.NportAdapterError("synthetic_reason", " C:\\secret\\source.xml\nvalue ")
    assert error.reason == "synthetic_reason"
    assert error.detail == "<path> value"
    assert str(error) == "synthetic_reason:<path> value"


def test_inventory_preserves_complete_requested_report_date_and_round_trips(tmp_path: Path) -> None:
    first = _filing().hold(1, CUSIP_A, is_default="Y").hold(2, CUSIP_A, is_default="N")
    first.hold(3, CUSIP_B, is_default="N")
    second = _filing("0000000102-21-000001", cik="0000000102", series=None)
    second.hold(4, None, is_default="BAD").hold(5, CUSIP_B, is_default=None)
    result = _dera(tmp_path, [first, second])

    inventory = _inventory([_source(result)])
    produced = tuple(nport.iter_package_observations(result, cusips=None))

    assert len(inventory.observations) == 5
    assert {
        (row.observation_id, row.row_sha256()) for row in inventory.observations
    } == {
        (row.observation_id, row.row_sha256()) for row in produced
    }
    assert {row.cusip9 for row in inventory.observations} == {CUSIP_A, CUSIP_B, None}
    assert {row.field_presence["nport_is_default"] for row in inventory.observations} == {
        "present",
        "invalid",
        "null",
    }
    assert all(
        c.CreditObservation.from_record(row.to_record()).row_sha256() == row.row_sha256()
        for row in inventory.observations
    )
    assert all(
        c.SourcePackage.from_record(row.to_record()).row_sha256() == row.row_sha256()
        for row in inventory.source_packages
    )
    assert inventory.source_scope_package_ids == (result.source_package.package_id,)


def test_public_xml_inventory_keeps_original_rows_and_ids() -> None:
    result = _public()
    inventory = _inventory([_source(result)])
    assert inventory.observations == result.observations
    assert inventory.source_packages == (result.source_package,)
    assert inventory.observations[0].row_sha256() == result.observations[0].row_sha256()


@pytest.mark.parametrize("name", ["max_scan_rows", "max_output_rows"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_inventory_limits_must_be_positive_integers(
    tmp_path: Path, name: str, value: object
) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A)])
    kwargs = {name: value}
    _error_reason(
        lambda: nport.materialize_nport_inventory(
            [_source(result)],
            report_dates=[REPORT_DATE],
            knowledge_cutoff=K,
            knowledge_mode="current_run",
            **kwargs,
        ),
        "nport_adapter_arguments_invalid",
    )


def test_inventory_limits_refuse_instead_of_truncating(tmp_path: Path) -> None:
    result = _dera(
        tmp_path,
        [_filing().hold(1, CUSIP_A).hold(2, CUSIP_B).hold(3, CUSIP_A)],
    )
    _error_reason(
        lambda: _inventory([_source(result)], max_scan_rows=2),
        "nport_inventory_limit_exceeded",
    )
    _error_reason(
        lambda: _inventory([_source(result)], max_output_rows=2),
        "nport_inventory_limit_exceeded",
    )


def test_inventory_limits_are_cumulative_across_source_metadata(tmp_path: Path) -> None:
    first = _dera(
        tmp_path,
        [_filing("0000000101-21-000001"), _filing("0000000102-21-000001", cik="0000000102")],
        label="metadata-one.zip",
    )
    second = _dera(
        tmp_path,
        [_filing("0000000103-21-000001", cik="0000000103"), _filing("0000000104-21-000001", cik="0000000104")],
        label="metadata-two.zip",
    )
    _error_reason(
        lambda: _inventory([_source(first), _source(second)], max_scan_rows=3),
        "nport_inventory_limit_exceeded",
    )


@pytest.mark.parametrize(
    ("report_dates", "cutoff", "mode"),
    [
        ((), K, "current_run"),
        ((dt.datetime(2021, 3, 31, tzinfo=UTC),), K, "current_run"),
        ((REPORT_DATE, REPORT_DATE), K, "current_run"),
        ((REPORT_DATE,), K.replace(tzinfo=None), "current_run"),
        ((REPORT_DATE,), K, "historical"),
    ],
)
def test_inventory_rejects_invalid_dates_cutoff_and_mode(
    tmp_path: Path,
    report_dates: tuple[object, ...],
    cutoff: dt.datetime,
    mode: str,
) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A)])
    _error_reason(
        lambda: _inventory(
            [_source(result)],
            report_dates=report_dates,
            cutoff=cutoff,
            mode=mode,
        ),
        "nport_adapter_arguments_invalid",
    )


def test_inventory_refuses_missing_source_artifacts_and_projection_count(tmp_path: Path) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A)])
    _error_reason(
        lambda: _inventory([_source(dataclasses.replace(result, source_package=None))]),
        "nport_source_not_persistable",
    )
    missing = dataclasses.replace(result, projection_path=None)
    _error_reason(lambda: _inventory([_source(missing)]), "nport_source_not_persistable")
    mismatch = dataclasses.replace(result, stats={**result.stats, "eligible_rows_projected": 2})
    _error_reason(lambda: _inventory([_source(mismatch)]), "nport_source_projection_mismatch")
    missing_count = dict(result.stats)
    missing_count.pop("eligible_rows_projected")
    _error_reason(
        lambda: _inventory([_source(dataclasses.replace(result, stats=missing_count))]),
        "nport_source_projection_mismatch",
    )
    quarantined = dataclasses.replace(result, status="quarantined", quarantine_reasons=("synthetic",))
    _error_reason(lambda: _inventory([_source(quarantined)]), "nport_source_not_persistable")
    public = _public()
    _error_reason(
        lambda: _inventory([_source(dataclasses.replace(public, header=None))]),
        "nport_source_not_persistable",
    )
    _error_reason(lambda: _inventory([]), "nport_adapter_arguments_invalid")


def test_inventory_refuses_unbound_or_tampered_dera_artifacts(tmp_path: Path) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A)])
    assert result.projection_sha256 and result.accessions_sha256
    _inventory([_source(result)])  # bound and intact: accepted
    legacy = dataclasses.replace(result, projection_sha256=None, accessions_sha256=None)
    _error_reason(lambda: _inventory([_source(legacy)]), "nport_source_artifact_unbound")
    half = dataclasses.replace(result, accessions_sha256=None)
    _error_reason(lambda: _inventory([_source(half)]), "nport_source_artifact_unbound")
    wrong = dataclasses.replace(result, projection_sha256="0" * 64)
    _error_reason(lambda: _inventory([_source(wrong)]), "nport_source_artifact_hash_mismatch")
    with open(result.accessions_path, "ab") as handle:
        handle.write(b"\n")
    _error_reason(lambda: _inventory([_source(result)]), "nport_source_artifact_hash_mismatch")


def test_inventory_refuses_public_result_that_dropped_debt_less_holdings() -> None:
    public = _public()
    dropped = dataclasses.replace(public, stats={**public.stats, "eligible_without_single_debt_section": 1})
    _error_reason(lambda: _inventory([_source(dropped)]), "nport_source_not_persistable")


def test_public_observation_duplicate_and_conflict_handling() -> None:
    result = _public()
    row = result.observations[0]
    duplicate = dataclasses.replace(result, observations=(row, row))
    assert _inventory([_source(duplicate)]).observations == (row,)

    conflict = dataclasses.replace(row, nport_is_default="N")
    conflicting = dataclasses.replace(result, observations=(row, conflict))
    _error_reason(
        lambda: _inventory([_source(conflicting)]),
        "nport_duplicate_identity_conflict",
    )


def test_public_observation_order_does_not_change_inventory() -> None:
    result = _public()
    values = result.observations[0].to_record()
    values.pop("observation_id")
    semantic = result.semantic_shas[0]
    values.update(
        row_locator="holding[2]",
        holding_id="xml:2",
        semantic_key=nport.semantic_key_for(result.header.accession_number, semantic, 1),
    )
    second = c.CreditObservation.create(**values)
    forward = dataclasses.replace(
        result,
        observations=(result.observations[0], second),
        holding_count=2,
        semantic_shas=(semantic, semantic),
        stats={**result.stats, "holdings": 2, "eligible_observations": 2},
    )
    reverse = dataclasses.replace(forward, observations=tuple(reversed(forward.observations)))
    assert _inventory([_source(forward)]).observations == _inventory([_source(reverse)]).observations


def test_duplicate_sources_deduplicate_and_conflicting_package_identity_refuses(tmp_path: Path) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A)])
    inventory = _inventory([_source(result), _source(result)])
    assert len(inventory.source_packages) == 1
    assert len(inventory.observations) == 1

    package = dataclasses.replace(result.source_package, raw_sha256="f" * 64)
    conflict = dataclasses.replace(result, source_package=package)
    _error_reason(
        lambda: _inventory([_source(result), _source(conflict)]),
        "nport_duplicate_identity_conflict",
    )


def test_source_input_snapshots_mutable_header_mapping(tmp_path: Path) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A)])
    header = _header("0000000101-21-000001")
    headers = {header.accession_number: header}
    source = _source(result, headers=headers)
    headers.clear()
    assert tuple(source.acceptance_headers) == (header.accession_number,)
    with pytest.raises(TypeError):
        source.acceptance_headers[header.accession_number] = header


def test_inventory_is_builder_sealed_and_nested_mappings_are_read_only(tmp_path: Path) -> None:
    inventory = _inventory([_source(_dera(tmp_path, [_filing().hold(1, CUSIP_A)]))])
    with pytest.raises(nport.NportAdapterError, match="nport_inventory_sealed"):
        dataclasses.replace(inventory, _seal=object())
    with pytest.raises(TypeError):
        inventory.excluded_counts["x"] = 1
    with pytest.raises(TypeError):
        inventory.resolution.attestations["x"] = ()


def test_source_permutation_is_canonical(tmp_path: Path) -> None:
    one = _dera(tmp_path, [_filing().hold(1, CUSIP_A)], label="one.zip")
    two = _dera(
        tmp_path,
        [_filing("0000000102-21-000001", cik="0000000102").hold(2, CUSIP_B, is_default="N")],
        label="two.zip",
    )
    left = _inventory([_source(one), _source(two)])
    right = _inventory([_source(two), _source(one)])
    assert left.source_packages == right.source_packages
    assert left.observations == right.observations
    assert left.issues == right.issues
    assert dict(left.excluded_counts) == dict(right.excluded_counts)


def test_historical_reconciled_copy_works_but_current_run_requires_possession(
    tmp_path: Path,
) -> None:
    retrieved = dt.datetime(2026, 9, 1, tzinfo=UTC)
    cutoff = dt.datetime(2026, 8, 13, tzinfo=UTC)
    accession = "0000000101-26-000009"
    public = _public(
        accession=accession,
        report_date="2026-06-30",
        acceptance_raw="20260812101500",
        retrieved_at=retrieved,
    )
    twin = _filing(
        accession,
        report_date="30-JUN-2026",
        filing_date="12-AUG-2026",
    )
    twin.hold(
        1,
        CUSIP_A,
        is_default="Y",
        isins=(),
        name="Synthetic",
        title="NOTE",
        balance="1000",
        value="990",
        maturity="15-JAN-2030",
        rate="5",
    )
    dera = _dera(
        tmp_path,
        [twin],
        label="2026q3_nport.zip",
        retrieved_at=retrieved,
    )
    proof = nport.reconcile_dera_with_public(dera, public)
    source = _source(dera, headers={accession: public.header}, reconciliations=(proof,))

    historical = _inventory(
        [source],
        report_dates=[dt.date(2026, 6, 30)],
        cutoff=cutoff,
        mode="historical_reconstruction",
    )
    current = _inventory(
        [source],
        report_dates=[dt.date(2026, 6, 30)],
        cutoff=cutoff,
        mode="current_run",
    )
    assert len(historical.observations) == 1
    assert historical.observations[0].public_available_at == public.header.acceptance_at
    assert current.observations == ()
    assert any(issue.reason == "nport_report_date_without_evidence" for issue in current.issues)


def test_after_cutoff_unplaceable_copy_does_not_block_earlier_snapshot(tmp_path: Path) -> None:
    valid = _public(retrieved_at=dt.datetime(2021, 5, 20, 21, tzinfo=UTC))
    late = _dera(
        tmp_path,
        [_filing("0000000102-21-000001", cik="0000000102", report_date="")],
        label="late-unplaceable.zip",
        retrieved_at=dt.datetime(2021, 6, 1, tzinfo=UTC),
    )
    inventory = _inventory(
        [_source(valid), _source(late)],
        cutoff=dt.datetime(2021, 5, 21, tzinfo=UTC),
    )
    assert len(inventory.observations) == 1
    assert not any(issue.reason == "nport_inventory_unplaceable" for issue in inventory.issues)


def test_current_run_first_seen_and_package_retrieval_guards_are_independent() -> None:
    before = K - dt.timedelta(seconds=1)
    after = K + dt.timedelta(seconds=1)
    unseen = _public(retrieved_at=before, first_seen_at=after)
    unseen_inventory = _inventory([_source(unseen)])
    assert unseen_inventory.observations == ()
    assert unseen_inventory.excluded_counts == {"observation_not_ingested_by_cutoff": 1}

    unpossessed = _public(retrieved_at=after, first_seen_at=before)
    unpossessed_inventory = _inventory([_source(unpossessed)])
    assert unpossessed_inventory.observations == ()
    assert unpossessed_inventory.excluded_counts == {"package_not_retrieved_by_cutoff": 1}

    exact = _public(retrieved_at=K, first_seen_at=K)
    assert len(_inventory([_source(exact)]).observations) == 1


def test_header_without_reconciliation_cannot_backdate_dera(tmp_path: Path) -> None:
    retrieved = dt.datetime(2026, 9, 1, tzinfo=UTC)
    cutoff = dt.datetime(2026, 8, 13, tzinfo=UTC)
    accession = "0000000101-26-000009"
    filing = _filing(
        accession,
        report_date="30-JUN-2026",
        filing_date="12-AUG-2026",
    ).hold(1, CUSIP_A, is_default="Y")
    result = _dera(tmp_path, [filing], label="2026q3_nport.zip", retrieved_at=retrieved)
    source = _source(
        result,
        headers={accession: _header(accession, acceptance_raw="20260812101500")},
    )
    inventory = _inventory(
        [source],
        report_dates=[dt.date(2026, 6, 30)],
        cutoff=cutoff,
        mode="historical_reconstruction",
    )
    assert inventory.observations == ()


def test_original_and_amendment_remain_in_inventory_and_block_date(tmp_path: Path) -> None:
    original = _filing().hold(1, CUSIP_A, is_default="Y")
    amendment = _filing(
        "0000000101-21-000002",
        filing_date="21-MAY-2021",
        sub_type="NPORT-P/A",
    ).hold(2, CUSIP_B, is_default="Y")
    result = _dera(tmp_path, [original, amendment])
    inventory = _inventory([_source(result)])

    assert len(inventory.observations) == 2
    assert {row.accession_number for row in inventory.observations} == {
        original.accession,
        amendment.accession,
    }
    assert inventory.resolution.selected_accessions == {amendment.accession}
    assert {issue.reason for issue in inventory.issues} >= {
        "nport_revision_context_unrepresentable",
    }
    frames = _frames(inventory)
    with pytest.raises(c.ContractError, match="family_universe_not_closed"):
        publication.build_family_frames(
            frames,
            [REPORT_DATE],
            knowledge_cutoff=inventory.knowledge_cutoff,
            knowledge_mode=inventory.knowledge_mode,
        )


def test_conflicting_copy_dates_block_every_affected_requested_date() -> None:
    march = _public(
        accession="0000000101-21-000001",
        report_date="2021-03-31",
        cusip=CUSIP_A,
    )
    june = _public(
        accession="0000000101-21-000001",
        report_date="2021-06-30",
        cusip=CUSIP_A,
    )
    june_candidate = _public(
        accession="0000000102-21-000001",
        cik="0000000102",
        series="S000000002",
        report_date="2021-06-30",
        cusip=CUSIP_B,
    )
    inventory = _inventory(
        [_source(march), _source(june), _source(june_candidate)],
        report_dates=[REPORT_DATE, dt.date(2021, 6, 30)],
    )
    blocked_dates = {
        issue.report_date
        for issue in inventory.issues
        if issue.reason == "nport_revision_selection_ambiguous"
    }
    assert blocked_dates == {REPORT_DATE, dt.date(2021, 6, 30)}
    proposal = _candidate(inventory, CUSIP_B)
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=_frames(inventory),
        ),
        "nport_context_blocked",
    )


def test_late_amendment_does_not_block_earlier_snapshot(tmp_path: Path) -> None:
    original = _public(
        accession="0000000101-21-000001",
        acceptance_raw="20210520163000",
    )
    amendment = _public(
        accession="0000000101-21-000002",
        cusip=CUSIP_B,
        acceptance_raw="20210620163000",
    )
    inventory = _inventory(
        [_source(original), _source(amendment)],
        cutoff=dt.datetime(2021, 5, 21, tzinfo=UTC),
        mode="historical_reconstruction",
    )
    assert {row.accession_number for row in inventory.observations} == {
        "0000000101-21-000001"
    }
    assert not any(issue.report_date == REPORT_DATE for issue in inventory.issues)
    assert inventory.source_scope_package_ids == tuple(
        sorted(
            (original.source_package.package_id, amendment.source_package.package_id),
            key=str,
        )
    )


def test_later_changed_copy_cannot_change_prior_cutoff_candidate() -> None:
    earlier = _public(
        accession="0000000101-21-000001",
        acceptance_raw="20210520163000",
        retrieved_at=dt.datetime(2021, 5, 20, 21, tzinfo=UTC),
        is_default="Y",
    )
    changed = _public(
        accession="0000000101-21-000001",
        acceptance_raw="20210620163000",
        retrieved_at=dt.datetime(2021, 6, 20, 21, tzinfo=UTC),
        is_default="N",
    )
    inventory = _inventory(
        [_source(earlier), _source(changed)],
        cutoff=dt.datetime(2021, 5, 21, tzinfo=UTC),
    )
    proposal = _candidate(inventory)
    assert proposal.proposed_status == "candidate"
    assert proposal.evidence_observation_ids == (
        earlier.observations[0].observation_id,
    )
    assert inventory.source_scope_package_ids == tuple(
        sorted(
            (earlier.source_package.package_id, changed.source_package.package_id),
            key=str,
        )
    )


@pytest.mark.parametrize(
    "filings",
    [
        lambda: [
            _filing().hold(1, CUSIP_A, is_default="Y"),
            _filing("0000000101-21-000002", filing_date="21-MAY-2021").hold(
                2, CUSIP_B, is_default="Y"
            ),
        ],
        lambda: [
            _filing().hold(1, CUSIP_A, is_default="Y"),
            _filing(
                "0000000101-21-000002",
                filing_date="21-MAY-2021",
                sub_type="NPORT-P/A",
            ),
        ],
    ],
)
def test_ambiguous_revision_shapes_are_fully_inventoried_and_blocked(
    tmp_path: Path, filings: object
) -> None:
    inventory = _inventory([_source(_dera(tmp_path, filings()))])
    assert {issue.reason for issue in inventory.issues} >= {
        "nport_revision_context_unrepresentable",
        "nport_revision_selection_ambiguous",
    }
    assert inventory.observations


def test_equal_time_revision_order_is_ambiguous() -> None:
    original = _public(
        accession="0000000101-21-000001",
        acceptance_raw="20210520163000",
    )
    amendment = _public(
        accession="0000000101-21-000002",
        acceptance_raw="20210520163000",
        sub_type="NPORT-P/A",
    )
    inventory = _inventory([_source(original), _source(amendment)])
    family = inventory.resolution.families[0]
    assert family.status == "disputed"
    assert "revision_order_ambiguous" in family.reasons
    assert any(issue.reason == "nport_revision_selection_ambiguous" for issue in inventory.issues)


def test_nonvoting_competing_accession_blocks_even_when_w0_sees_one_vote() -> None:
    original = _public(accession="0000000101-21-000001", is_default="Y")
    amendment = _public(
        accession="0000000101-21-000002",
        acceptance_raw="20210521163000",
        sub_type="NPORT-P/A",
        is_default="",
    )
    inventory = _inventory([_source(original), _source(amendment)])
    assert any(issue.reason == "nport_revision_context_unrepresentable" for issue in inventory.issues)
    contexts, memberships = publication.build_family_frames(
        _frames(inventory),
        [REPORT_DATE],
        knowledge_cutoff=inventory.knowledge_cutoff,
        knowledge_mode=inventory.knowledge_mode,
    )
    assert contexts and memberships
    proposal = nport.StateProposal(
        cusip9=CUSIP_A,
        proposed_status="candidate",
        reviewer_role="policy_rule_engine",
        basis="one_series_one_family",
        onset_lower_exclusive=None,
        onset_upper_inclusive=None,
        timing_class=None,
        left_censored=False,
        evidence_observation_ids=(original.observations[0].observation_id,),
        evidence_known_at=original.observations[0].public_available_at,
        subsequent_y_dates=(),
        credible_n_after_first_y=(),
        conflict_dates=(),
        candidate_y_dates=(REPORT_DATE,),
    )
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=_frames(inventory),
        ),
        "nport_context_blocked",
    )


def test_zero_holding_unplaceable_and_empty_dates_are_typed_issues(tmp_path: Path) -> None:
    unplaceable = _filing(report_date="")
    inventory = _inventory([_source(_dera(tmp_path, [unplaceable]))])
    assert {issue.reason for issue in inventory.issues} == {
        "nport_inventory_unplaceable",
        "nport_report_date_without_evidence",
    }

    placed = _dera(tmp_path, [_filing()], label="zero-holding-placed.zip")
    placed_inventory = _inventory([_source(placed)])
    assert placed_inventory.observations == ()
    assert placed_inventory.source_packages == (placed.source_package,)
    assert {issue.reason for issue in placed_inventory.issues} == {
        "nport_report_date_without_evidence"
    }


def test_missing_package_and_observation_ancestry_refuse(tmp_path: Path) -> None:
    public = _public()
    package = dataclasses.replace(
        public.source_package,
        revision_of_package_id=uuid.uuid4(),
    )
    missing_package = dataclasses.replace(public, source_package=package)
    _error_reason(
        lambda: _inventory([_source(missing_package)]),
        "nport_inventory_lineage_missing",
    )

    observation = dataclasses.replace(
        public.observations[0],
        revision_kind="correction",
        supersedes_observation_id=uuid.uuid4(),
    )
    missing_observation = dataclasses.replace(public, observations=(observation,))
    _error_reason(
        lambda: _inventory([_source(missing_observation)]),
        "nport_inventory_lineage_missing",
    )


def test_candidate_translation_uses_w0_known_time_and_empty_lineage(tmp_path: Path) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")])
    inventory = _inventory([_source(result)])
    proposal = _candidate(inventory)

    persisted = nport.candidate_proposal_evidence(
        dataclasses.replace(proposal, evidence_known_at=K),
        inventory=inventory,
        frames=_frames(inventory),
    )

    assert persisted.proposed_status == "candidate"
    assert persisted.onset_lower_exclusive is None
    assert persisted.onset_upper_inclusive is None
    assert persisted.onset_lower_evidence_ids == ()
    assert persisted.onset_upper_evidence_ids == ()
    assert persisted.family_evidence_ids == ()
    assert persisted.corroboration_adjudication_ids == ()
    assert persisted.evidence_observation_ids == proposal.evidence_observation_ids
    assert persisted.evidence_known_at == inventory.observations[0].public_available_at
    assert persisted.policy_digest == c.POLICY_DIGEST


def test_candidate_translation_validates_all_nine_frames(tmp_path: Path) -> None:
    result = _dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")])
    inventory = _inventory([_source(result)])
    proposal = _candidate(inventory)
    frames = _frames(inventory)
    frames.pop("exchange_relations")
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=frames,
        ),
        "nport_dependency_frames_invalid",
    )
    bad = _frames(inventory, event_links=[inventory.observations[0]])
    _error_reason(
        lambda: nport.candidate_proposal_evidence(proposal, inventory=inventory, frames=bad),
        "nport_dependency_frames_invalid",
    )


def test_candidate_translation_canonicalizes_identical_frame_duplicates(tmp_path: Path) -> None:
    inventory = _inventory([_source(_dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")]))])
    proposal = _candidate(inventory)
    row = inventory.observations[0]
    duplicate = _frames(inventory, observations=[row, row])
    assert nport.candidate_proposal_evidence(
        proposal,
        inventory=inventory,
        frames=duplicate,
    ).evidence_observation_ids == proposal.evidence_observation_ids

    conflicting = dataclasses.replace(row, nport_is_default="N")
    conflict_frames = _frames(inventory, observations=[row, conflicting])
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=conflict_frames,
        ),
        "nport_dependency_frames_invalid",
    )


def test_candidate_translation_requires_full_byte_equal_inventory_frames(tmp_path: Path) -> None:
    filing = _filing().hold(1, CUSIP_A, is_default="Y").hold(2, CUSIP_B, is_default="N")
    inventory = _inventory([_source(_dera(tmp_path, [filing]))])
    proposal = _candidate(inventory)
    frames = _frames(inventory)
    frames["observations"] = tuple(
        row for row in inventory.observations if row.cusip9 == CUSIP_A
    )
    _error_reason(
        lambda: nport.candidate_proposal_evidence(proposal, inventory=inventory, frames=frames),
        "nport_inventory_frame_mismatch",
    )


def test_candidate_translation_diagnoses_stale_and_after_cutoff_before_frame_mismatch(
    tmp_path: Path,
) -> None:
    inventory = _inventory([_source(_dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")]))])
    proposal = _candidate(inventory)
    old = inventory.observations[0]
    values = old.to_record()
    values.pop("observation_id")
    values.update(
        row_locator="adapter-revision",
        revision_kind="correction",
        supersedes_observation_id=str(old.observation_id),
    )
    revision = c.CreditObservation.create(**values)
    stale_frames = _frames(inventory, observations=[old, revision])
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=stale_frames,
        ),
        "proposal_evidence_stale",
    )

    late = _replace_observation(
        old,
        public_available_at="2026-09-26T00:00:00.000000Z",
        first_seen_at="2026-09-26T00:00:00.000000Z",
    )
    late_frames = _frames(inventory, observations=[late])
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=late_frames,
        ),
        "proposal_evidence_after_cutoff",
    )

    retraction_values = old.to_record()
    retraction_values.pop("observation_id")
    retraction_values.update(
        row_locator="adapter-retraction",
        revision_kind="retraction",
        supersedes_observation_id=str(old.observation_id),
    )
    retracted = c.CreditObservation.create(**retraction_values)
    retracted_frames = _frames(inventory, observations=[old, retracted])
    retracted_proposal = dataclasses.replace(
        proposal,
        evidence_observation_ids=(retracted.observation_id,),
    )
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            retracted_proposal,
            inventory=inventory,
            frames=retracted_frames,
        ),
        "proposal_evidence_stale",
    )


def test_candidate_translation_rejects_uncited_unpossessed_current_run_rows() -> None:
    valid = _public(retrieved_at=K - dt.timedelta(hours=1))
    inventory = _inventory([_source(valid)])
    proposal = _candidate(inventory)
    late = _public(
        accession="0000000102-21-000001",
        cik="0000000102",
        series="S000000002",
        report_date="2021-06-30",
        cusip=CUSIP_B,
        retrieved_at=K + dt.timedelta(hours=1),
        first_seen_at=K + dt.timedelta(hours=1),
    )
    frames = _frames(
        inventory,
        source_packages=(*inventory.source_packages, late.source_package),
        observations=(*inventory.observations, *late.observations),
    )
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=frames,
        ),
        "proposal_evidence_after_cutoff",
    )


def test_future_superseding_revision_never_stales_prior_cutoff_evidence() -> None:
    valid = _public(retrieved_at=K - dt.timedelta(hours=1))
    inventory = _inventory([_source(valid)])
    proposal = _candidate(inventory)
    old = inventory.observations[0]
    values = old.to_record()
    values.pop("observation_id")
    values.update(
        row_locator="future-revision",
        revision_kind="correction",
        supersedes_observation_id=str(old.observation_id),
        acceptance_raw=None,
        acceptance_at=None,
        public_available_at="2026-09-26T00:00:00.000000Z",
        public_time_basis="first_verified_retrieval",
        first_seen_at="2026-09-26T00:00:00.000000Z",
    )
    future = c.CreditObservation.create(**values)
    frames = _frames(inventory, observations=[old, future])
    ix = publication.dependency_index(
        frames,
        knowledge_cutoff=inventory.knowledge_cutoff,
        knowledge_mode=inventory.knowledge_mode,
    )
    assert old.observation_id not in ix.stale_obs
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=frames,
        ),
        "proposal_evidence_after_cutoff",
    )


def test_candidate_translation_rejects_wrong_cusip_and_n_only_evidence(tmp_path: Path) -> None:
    filing = _filing().hold(1, CUSIP_A, is_default="Y").hold(2, CUSIP_B, is_default="N")
    inventory = _inventory([_source(_dera(tmp_path, [filing]))])
    proposal = _candidate(inventory)
    n_row = next(row for row in inventory.observations if row.cusip9 == CUSIP_B)
    wrong = dataclasses.replace(
        proposal,
        evidence_observation_ids=(n_row.observation_id,),
    )
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            wrong,
            inventory=inventory,
            frames=_frames(inventory),
        ),
        "proposal_candidate_evidence_mismatch",
    )


def test_candidate_evidence_order_and_duplicates_are_canonical(tmp_path: Path) -> None:
    filing = _filing().hold(1, CUSIP_A, is_default="Y", balance="1000")
    filing.hold(2, CUSIP_A, is_default="Y", balance="2000")
    inventory = _inventory([_source(_dera(tmp_path, [filing]))])
    proposal = _candidate(inventory)
    reordered = dataclasses.replace(
        proposal,
        evidence_observation_ids=(
            proposal.evidence_observation_ids[1],
            proposal.evidence_observation_ids[0],
            proposal.evidence_observation_ids[1],
        ),
    )
    persisted = nport.candidate_proposal_evidence(
        reordered,
        inventory=inventory,
        frames=_frames(inventory),
    )
    assert persisted.evidence_observation_ids == c.sorted_uuids(proposal.evidence_observation_ids)


@pytest.mark.parametrize("mutation", ["subset", "basis", "missing"])
def test_candidate_recomputation_rejects_partial_or_invented_support(
    tmp_path: Path, mutation: str
) -> None:
    filing = _filing().hold(1, CUSIP_A, is_default="Y", balance="1000")
    filing.hold(2, CUSIP_A, is_default="Y", balance="2000")
    inventory = _inventory([_source(_dera(tmp_path, [filing]))])
    proposal = _candidate(inventory)
    if mutation == "subset":
        proposal = dataclasses.replace(
            proposal,
            evidence_observation_ids=proposal.evidence_observation_ids[:1],
        )
        reason = "proposal_candidate_evidence_mismatch"
    elif mutation == "basis":
        proposal = dataclasses.replace(proposal, basis="invented_basis")
        reason = "proposal_candidate_evidence_mismatch"
    else:
        proposal = dataclasses.replace(proposal, evidence_observation_ids=(uuid.uuid4(),))
        reason = "proposal_evidence_not_closed"
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=_frames(inventory),
        ),
        reason,
    )


@pytest.mark.parametrize("values", [("N",), ("Y", "N")])
def test_all_n_or_all_conflict_input_cannot_be_forged_into_candidate(
    tmp_path: Path, values: tuple[str, ...]
) -> None:
    filing = _filing()
    for index, value in enumerate(values, start=1):
        filing.hold(index, CUSIP_A, is_default=value)
    inventory = _inventory([_source(_dera(tmp_path, [filing]))])
    assert not nport.build_consensus_states(
        nport.build_votes(inventory.observations, resolution=inventory.resolution).votes,
        knowledge_cutoff=inventory.knowledge_cutoff,
    ).proposals
    proposal = nport.StateProposal(
        cusip9=CUSIP_A,
        proposed_status="candidate",
        reviewer_role="policy_rule_engine",
        basis="forged",
        onset_lower_exclusive=None,
        onset_upper_inclusive=None,
        timing_class=None,
        left_censored=False,
        evidence_observation_ids=tuple(row.observation_id for row in inventory.observations),
        evidence_known_at=max(row.public_available_at for row in inventory.observations),
        subsequent_y_dates=(),
        credible_n_after_first_y=(),
        conflict_dates=(),
        candidate_y_dates=(REPORT_DATE,),
    )
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=_frames(inventory),
        ),
        "proposal_candidate_evidence_mismatch",
    )


def test_runtime_candidate_adapter_has_no_accepting_builder_or_state_output() -> None:
    import inspect

    source = inspect.getsource(nport.candidate_proposal_evidence)
    assert "derive_state_proposal" not in source
    assert "nport_consensus_state" not in source


def test_multi_copy_candidate_uses_persisted_closure_time_not_vote_scalar(tmp_path: Path) -> None:
    accession = "0000000101-21-000001"
    public = _public(accession=accession, retrieved_at=RETRIEVED)
    filing = _filing(accession).hold(
        1,
        CUSIP_A,
        is_default="Y",
        name="Synthetic",
        title="NOTE",
        balance="1000",
        value="990",
        maturity="15-JAN-2030",
        rate="5",
    )
    dera = _dera(tmp_path, [filing], retrieved_at=RETRIEVED + dt.timedelta(hours=1))
    inventory = _inventory(
        [
            _source(public),
            _source(dera, headers={accession: public.header}),
        ]
    )
    proposal = _candidate(inventory)
    persisted = nport.candidate_proposal_evidence(
        dataclasses.replace(proposal, evidence_known_at=dt.datetime(2000, 1, 1, tzinfo=UTC)),
        inventory=inventory,
        frames=_frames(inventory),
    )
    assert len(persisted.evidence_observation_ids) == 2
    assert proposal.evidence_known_at == min(
        row.public_available_at for row in inventory.observations
    )
    assert persisted.evidence_known_at == max(
        row.public_available_at for row in inventory.observations
    )


@pytest.mark.parametrize(
    "change",
    [
        {"proposed_status": "accepted_state"},
        {"proposed_status": "unknown"},
        {"onset_upper_inclusive": REPORT_DATE},
        {"corroboration_evidence_refs": (str(uuid.uuid4()),)},
        {"corroboration_adjudication_ids": (uuid.uuid4(),)},
        {
            "family_evidence": (
                nport.FamilyEvidence(
                    registrant_cik="0000000101",
                    family_id="synthetic-family",
                    evidence_ref="synthetic-ref",
                    evidence_digest="0" * 64,
                    valid_from=None,
                    valid_to=None,
                    public_available_at=K,
                    known_at=K,
                ),
            )
        },
        {"reviewer_role": "human_reviewer"},
        {"evidence_observation_ids": ()},
    ],
)
def test_candidate_translation_rejects_non_candidate_shape_or_lineage(
    tmp_path: Path, change: dict[str, object]
) -> None:
    inventory = _inventory([_source(_dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")]))])
    proposal = dataclasses.replace(_candidate(inventory), **change)
    with pytest.raises(nport.NportAdapterError):
        nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=_frames(inventory),
        )


def test_candidate_translation_rejects_blocked_context(tmp_path: Path) -> None:
    original = _filing().hold(1, CUSIP_A, is_default="Y")
    amendment = _filing(
        "0000000101-21-000002",
        filing_date="21-MAY-2021",
        sub_type="NPORT-P/A",
    ).hold(2, CUSIP_A, is_default="Y")
    inventory = _inventory([_source(_dera(tmp_path, [original, amendment]))])
    proposal = nport.build_consensus_states(
        nport.build_votes(inventory.observations, resolution=inventory.resolution).votes,
        knowledge_cutoff=inventory.knowledge_cutoff,
    ).proposals[0]
    _error_reason(
        lambda: nport.candidate_proposal_evidence(
            proposal,
            inventory=inventory,
            frames=_frames(inventory),
        ),
        "nport_context_blocked",
    )


def _compose_candidate_bundle(
    inventory: nport.PersistedNportInventory,
    candidate: c.ProposalEvidence,
    *,
    contexts: Iterable[c.FamilyContext] = (),
    memberships: Iterable[c.FamilyMembership] = (),
) -> c.CreditBundle:
    declarations = public_ratings.rating_declarations_record((), ())
    return c.assemble_bundle(
        target_month=dt.date(2021, 3, 1),
        knowledge_cutoff=inventory.knowledge_cutoff,
        knowledge_mode=inventory.knowledge_mode,
        build_scope="limited",
        quality_state="partial",
        code_digest=c.digest_of(["candidate-adapter-test"]),
        panel_publication_id=c.uuid5_of("synthetic_panel", "candidate-adapter-test"),
        panel_grid=(),
        issuer_mapping_digest=None,
        rating_declarations=declarations,
        rating_input_digest=c.rating_input_manifest_digest(declarations, inventory.source_packages),
        validation_receipt=None,
        source_packages=inventory.source_packages,
        observations=inventory.observations,
        event_links=(),
        adjudications=(),
        events=(),
        followups=(),
        exit_evidence=(),
        coverage=(),
        ratings=(),
        ncen_filings=(),
        family_contexts=contexts,
        family_evidence=memberships,
        proposal_evidence=(candidate,),
        exchange_relations=(),
    )


def test_candidate_composes_in_w0_bundle_without_event_or_admitting_decision(
    tmp_path: Path,
) -> None:
    inventory = _inventory([_source(_dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")]))])
    candidate = nport.candidate_proposal_evidence(
        _candidate(inventory),
        inventory=inventory,
        frames=_frames(inventory),
    )
    bundle = _compose_candidate_bundle(inventory, candidate)
    assert bundle.frames["events"] == ()
    assert not any(
        row.status in c.ADMITTING_STATUSES for row in bundle.frames["adjudications"]
    )
    publication.check_bundle(bundle)
    replay = c.CreditBundle.from_json_bytes(bundle.canonical_bytes())
    publication.check_bundle(replay)
    assert replay.canonical_bytes() == bundle.canonical_bytes()


def test_unambiguous_w0_family_frames_do_not_enter_candidate_lineage(tmp_path: Path) -> None:
    inventory = _inventory([_source(_dera(tmp_path, [_filing().hold(1, CUSIP_A, is_default="Y")]))])
    contexts, memberships = publication.build_family_frames(
        _frames(inventory),
        [REPORT_DATE],
        knowledge_cutoff=inventory.knowledge_cutoff,
        knowledge_mode=inventory.knowledge_mode,
    )
    candidate = nport.candidate_proposal_evidence(
        _candidate(inventory),
        inventory=inventory,
        frames=_frames(
            inventory,
            family_contexts=contexts,
            family_evidence=memberships,
        ),
    )
    assert contexts and memberships
    assert candidate.family_evidence_ids == ()
    bundle = _compose_candidate_bundle(
        inventory,
        candidate,
        contexts=contexts,
        memberships=memberships,
    )
    publication.check_bundle(bundle)
    assert bundle.frames["events"] == ()


def test_inventory_and_candidate_are_deterministic_across_hash_seeds(tmp_path: Path) -> None:
    script = r'''
import datetime as dt, json
from src.bonds.default_events import nport
from src.bonds.default_events import sec_acquisition as sa
UTC=dt.timezone.utc
retrieved=dt.datetime(2026,9,25,3,14,tzinfo=UTC)
raw=b"<SEC-HEADER>\n<ACCEPTANCE-DATETIME>20210520163000\n<ACCESSION-NUMBER>0000000101-21-000001\n<TYPE>NPORT-P\n<CIK>0000000101\n</SEC-HEADER>"
h=sa.parse_acceptance_header(raw,accession_number="0000000101-21-000001",url="u",document_sha256="0"*64,retrieved_at=retrieved)
x=''' + repr(_xml_bytes()) + r'''
r=nport.parse_public_accession(x,header=h,retrieved_at=retrieved,official_url="u")
s=nport.NportSourceInput(result=r)
i=nport.materialize_nport_inventory([s],report_dates=[dt.date(2021,3,31),dt.date(2021,6,30)],knowledge_cutoff=dt.datetime(2026,9,25,23,tzinfo=UTC),knowledge_mode="current_run",max_scan_rows=8,max_output_rows=8)
v=nport.build_votes(i.observations,resolution=i.resolution)
p=nport.build_consensus_states(v.votes,knowledge_cutoff=i.knowledge_cutoff).proposals[0]
f={k:() for k in ("source_packages","observations","event_links","adjudications","ncen_filings","family_contexts","family_evidence","proposal_evidence","exchange_relations")}
f["source_packages"]=i.source_packages; f["observations"]=i.observations
o=nport.candidate_proposal_evidence(p,inventory=i,frames=f)
print(json.dumps({"packages":[x.row_sha256() for x in i.source_packages],"observations":[x.row_sha256() for x in i.observations],"proposal":o.row_sha256(),"issues":[dataclasses.asdict(x) for x in i.issues]},sort_keys=True,default=str))
'''
    outputs = []
    for seed in ("1", "999"):
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": str(ROOT)}
        completed = subprocess.run(
            [sys.executable, "-c", "import dataclasses\n" + script],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        outputs.append(completed.stdout.strip())
    assert outputs[0] == outputs[1]
