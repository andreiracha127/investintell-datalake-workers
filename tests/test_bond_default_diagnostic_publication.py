"""Offline contract tests for the diagnostic (coverage-only) release module.

No database is needed here; ``test_bond_default_diagnostic_publication_db.py`` exercises the SQL
mirror (it loads :func:`coverage_only_bundle` from this module).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import diagnostic_publication as d
from src.bonds.default_events import public_ratings as pr

UTC = dt.timezone.utc
T0 = dt.date(2026, 8, 1)
K0 = dt.datetime(2026, 9, 25, 0, 0, tzinfo=UTC)
OBSERVED = dt.datetime(2026, 9, 25, 0, 0, tzinfo=UTC)
MANIFEST_SHA = hashlib.sha256(b"sanitized-frontier-manifest-test").hexdigest()
DEFAULT_START_COUNTS = (4, 3, 3, 2, 2, 1)


def _sha(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def diagnostic_cusip(number: int) -> str:
    base = f"ZZ#DIA{number:02d}"
    return base + c.cusip_check_digit(base)


def frontier_records(
    *, observed_at: dt.datetime = OBSERVED, q3: bool = True
) -> list[dict]:
    """The six inventory records of the first real delivery (synthetic test values)."""

    def rec(**kw):
        return d.frontier_record(
            observed_at=observed_at, manifest_sha256=MANIFEST_SHA, **kw
        )

    return [
        rec(
            source="sec_nport",
            inventory_kind="dera_packages",
            frontier=dt.date(2026, 6, 30),
            state="inventory_only",
            filing_count=None,
            reason_codes=["nport_q3_unavailable_at_observation"] if q3 else [],
            basis="N-PORT filings through 2026-06-30; Q3 unavailable at observation",
            code="nport_dera_packages",
            source_key="nport.dera_packages",
        ),
        rec(
            source="sec_ncen",
            inventory_kind="dera_packages",
            frontier=dt.date(2026, 6, 30),
            state="inventory_only",
            filing_count=None,
            basis="N-CEN package period through 2026-Q2",
            code="ncen_dera_packages",
            source_key="ncen.dera_packages",
        ),
        rec(
            source="sec_ncen",
            inventory_kind="form_index",
            frontier=dt.date(2026, 9, 24),
            state="inventory_only",
            filing_count=None,
            basis="N-CEN form index frontier",
            code="ncen_form_index",
            source_key="ncen.form_index",
        ),
        rec(
            source="sec_edgar",
            inventory_kind="submissions",
            frontier=dt.date(2026, 9, 24),
            state="inventory_only",
            filing_count=None,
            basis="EDGAR submissions artifact",
            code="edgar_submissions",
            source_key="edgar.submissions",
        ),
        rec(
            source="sec_edgar",
            inventory_kind="census",
            frontier=dt.date(2026, 8, 31),
            state="inventory_only",
            filing_count=495,
            basis="EDGAR census target end 2026-08-31 filings not distinct defaults",
            code="edgar_census",
            source_key="edgar.census",
        ),
        rec(
            source="agency_rocr",
            inventory_kind="agency_history",
            frontier=None,
            state="unavailable",
            filing_count=None,
            reason_codes=["rating_source_unavailable"],
            basis="No local agency history inspected",
            code="agency_history",
            source_key="agency.history",
        ),
    ]


def _cell_records(records: list[dict], source: str) -> list[dict]:
    return [r for r in records if source == "all" or r["source"] == source]


def coverage_only_bundle(
    *,
    target_month: dt.date = T0,
    knowledge_cutoff: dt.datetime = K0,
    start_counts: tuple[int, ...] = DEFAULT_START_COUNTS,
    label: str = "a",
    panel_publication_id: uuid.UUID | None = None,
    records: list[dict] | None = None,
    mutate_cells=None,
) -> c.CreditBundle:
    """A structurally valid coverage-only, partial/limited bundle: empty frames, 2 missing ratings per key."""
    outcome_months = len(start_counts)
    grid_months = [
        c.add_months(target_month, -outcome_months + i)
        for i in range(outcome_months + 1)
    ]
    counts_by_month = dict(zip(grid_months, (*start_counts, 0), strict=True))
    max_keys = max(start_counts)
    cusips = [diagnostic_cusip(i) for i in range(1, max_keys + 1)]
    grid = [
        (cusips[i], month)
        for month in grid_months
        for i in range(counts_by_month[month])
    ]
    # The target month snapshot has keys too (its starts are not counted): give it two.
    grid += [(cusips[i], target_month) for i in range(2)]
    grid = sorted(set(grid))
    records = records if records is not None else frontier_records()
    cells: list[c.CoverageCell] = []
    frontier_date = {
        "all": None,
        "sec_nport": dt.date(2026, 6, 30),
        "sec_edgar": dt.date(2026, 9, 24),
        "agency_rocr": None,
    }
    for j in range(outcome_months):
        period = grid_months[j + 1]
        exposed = start_counts[j]
        for source, event_type in d.COVERAGE_CELLS:
            cells.append(
                c.CoverageCell(
                    period_label=period.strftime("%Y-%m"),
                    source=source,
                    event_type=event_type,
                    rating_stratum="unknown",
                    exposure_cohort="all",
                    state="unavailable",
                    denominator_basis="panel_exposure",
                    denominator_count=exposed,
                    exposed_issue_months=exposed,
                    event_count=0,
                    unlinked_count=0,
                    date_uncertain_count=0,
                    unknown_outcome_issue_months=exposed,
                    source_frontier=frontier_date[source],
                    lag_p50_days=None,
                    lag_p90_days=None,
                    lag_max_days=None,
                    rationale=d.coverage_rationale(
                        frontiers=_cell_records(records, source),
                        reason_codes=["outcomes_unascertained"]
                        if source == "all"
                        else [],
                    ),
                    validation_receipt_digest=None,
                )
            )
    if mutate_cells is not None:
        cells = mutate_cells(cells)
    ratings = [
        c.RatingGridRow(
            cusip_id=cusip,
            month=month,
            view_kind=view,
            bucket=None,
            state="missing",
            action_date=None,
            public_known_at=None,
            agency_source_ids=(),
            binding_link_ids=(),
            coverage_frontier=None,
            action_input_digest=None,
            default_overlay_episode_id=None,
        )
        for cusip, month in grid
        for view in ("effective_audit", "public_pit")
    ]
    declarations = pr.rating_declarations_record((), ())
    return c.assemble_bundle(
        target_month=target_month,
        knowledge_cutoff=knowledge_cutoff,
        knowledge_mode="current_run",
        build_scope="limited",
        quality_state="partial",
        code_digest=_sha(f"code-{label}"),
        panel_publication_id=panel_publication_id or c.uuid5_of("diag_panel", label),
        panel_grid=grid,
        issuer_mapping_digest=None,
        rating_declarations=declarations,
        rating_input_digest=c.rating_input_manifest_digest(declarations, []),
        validation_receipt=None,
        source_packages=[],
        observations=[],
        event_links=[],
        adjudications=[],
        events=[],
        followups=[],
        exit_evidence=[],
        coverage=cells,
        ratings=ratings,
        ncen_filings=[],
        family_contexts=[],
        family_evidence=[],
        proposal_evidence=[],
        exchange_relations=[],
    )


def projection_of(bundle: c.CreditBundle) -> d.DiagnosticProjection:
    return d.derive_projection_from_bundle(bundle)


def frontier_digest_of(bundle: c.CreditBundle) -> str:
    return d.frontier_manifest_digest(
        d.frontier_records_of(
            bundle.frames["coverage"],
            knowledge_cutoff=bundle.manifest["knowledge_cutoff"],
        )
    )  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Pins and hygiene
# ---------------------------------------------------------------------------
def test_diagnostic_sql_digest_is_pinned_lf_normalized_and_outside_the_four_file_digest(
    tmp_path,
):
    assert d.diagnostic_sql_digest() == d.DIAGNOSTIC_SQL_DIGEST
    assert (
        d.SQL_PATH.name == "bond_default_diagnostic_release_v1.sql"
        and d.SQL_PATH not in c.SQL_PATHS
    )
    crlf = tmp_path / "crlf.sql"
    crlf.write_bytes(d.SQL_PATH.read_bytes().replace(b"\n", b"\r\n"))
    assert d.diagnostic_sql_digest(crlf) == d.DIAGNOSTIC_SQL_DIGEST
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", d.DIAGNOSTIC_SQL_DIGEST)
    assert (
        len(c.SQL_FILES) == 4
    )  # the fifth file is never added to the pinned installer list


def test_sql_file_is_schema_public_only_and_never_grants_broad_readers():
    text = d.SQL_PATH.read_text(encoding="utf-8")
    assert "SET search_path FROM CURRENT" not in text
    for grant in re.findall(r"GRANT EXECUTE ON FUNCTION[^;]+;", text):
        assert "PUBLIC" not in grant
    reader_grants = [
        g
        for g in re.findall(r"GRANT EXECUTE ON FUNCTION[^;]+;", text)
        if "bond_default_diagnostic_reader" in g
    ]
    assert (
        len(reader_grants) == 1
        and "bond_default_current_diagnostic_release()" in reader_grants[0]
    )
    assert "bond_credit_reader TO" not in text and "TO app_runtime" not in text


def test_every_definer_function_captures_the_search_path_explicitly():
    text = d.SQL_PATH.read_text(encoding="utf-8")
    bodies = re.split(r"CREATE OR REPLACE FUNCTION ", text)[1:]
    assert len(bodies) >= 24
    for body in bodies:
        head = body.split("AS $$", 1)[0]
        assert "SET search_path = public, pg_temp" in head, body[:60]
        assert "SET search_path FROM CURRENT" not in head


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------
def test_derive_projection_from_a_coverage_only_bundle():
    bundle = coverage_only_bundle()
    projection = projection_of(bundle)
    assert (
        projection.schema_version
        == d.DISPLAY_VERSION
        == "bond_default_events_display_v1"
    )
    assert (projection.product, projection.tier, projection.display_mode) == (
        "bond_default_events_diagnostic_v1",
        "experimental_partial",
        "coverage_only",
    )
    assert (
        projection.quality_state,
        projection.build_scope,
        projection.recommendation_eligible,
    ) == ("partial", "limited", False)
    assert (
        len(projection.coverage) == 6 * len(DEFAULT_START_COUNTS)
        and projection.accepted_events == ()
    )
    assert projection.counts.panel_grid_keys == len(bundle.panel_grid)
    assert projection.counts.candidate_issue_months == sum(DEFAULT_START_COUNTS)
    assert projection.counts.unknown_outcome_issue_months == sum(DEFAULT_START_COUNTS)
    assert (
        projection.counts.unresolved_events is None
        and projection.counts.censored_issue_months is None
    )
    assert [(f.source, f.inventory_kind) for f in projection.source_frontiers] == [
        ("agency_rocr", "agency_history"),
        ("sec_edgar", "census"),
        ("sec_edgar", "submissions"),
        ("sec_ncen", "dera_packages"),
        ("sec_ncen", "form_index"),
        ("sec_nport", "dera_packages"),
    ]
    assert set(d.BASE_LIMITATIONS) | {"nport_q3_unavailable_at_observation"} <= set(
        projection.limitations
    )
    assert set(projection.limitations) <= set(d.LIMITATION_CODES)
    assert list(projection.limitations) == sorted(projection.limitations)
    keys = [cell.sort_key() for cell in projection.coverage]
    assert keys == sorted(keys)
    # Exposure follows the panel start keys of the previous month, never a Cartesian product or a sum of sources.
    all_cells = [
        cell
        for cell in projection.coverage
        if (cell.source, cell.event_type) == ("all", "all")
    ]
    assert [cell.exposed_issue_months for cell in all_cells] == list(
        DEFAULT_START_COUNTS
    )
    assert all(
        cell.state == "unavailable" and cell.denominator_basis == "panel_exposure"
        for cell in projection.coverage
    )


def test_projection_json_roundtrip_digest_and_canonical_bytes_are_stable():
    projection = projection_of(coverage_only_bundle())
    obj = projection.to_json_obj()
    assert set(obj) == {
        "schema_version",
        "product",
        "tier",
        "display_mode",
        "quality_state",
        "build_scope",
        "recommendation_eligible",
        "source_frontiers",
        "coverage",
        "accepted_events",
        "counts",
        "limitations",
    }
    again = d.DiagnosticProjection.from_json_obj(obj)
    assert again == projection and again.digest == projection.digest == c.digest_of(obj)
    assert projection.canonical_bytes() == c.canonical_json_bytes(obj)
    assert (
        b"NaN" not in projection.canonical_bytes()
        and b"Infinity" not in projection.canonical_bytes()
    )
    assert not re.search(
        rb"\d\.\d", projection.canonical_bytes().replace(b".000000Z", b"")
    )  # integers only
    # Order of frames and cells never changes the projection.
    shuffled = d.DiagnosticProjection(
        source_frontiers=tuple(reversed(projection.source_frontiers)),
        coverage=tuple(reversed(projection.coverage)),
        counts=projection.counts,
        limitations=tuple(reversed(projection.limitations)),
    )
    assert shuffled == projection and shuffled.digest == projection.digest


def test_deriving_twice_is_deterministic_and_independent_of_rationale_record_order():
    a = projection_of(coverage_only_bundle(records=frontier_records()))
    b = projection_of(coverage_only_bundle(records=list(reversed(frontier_records()))))
    assert a == b and a.digest == b.digest


def test_public_projection_carries_no_internal_identifier_path_or_manifest_hash():
    text = projection_of(coverage_only_bundle()).canonical_bytes().decode()
    assert (
        MANIFEST_SHA not in text
        and "source_key" not in text
        and "manifest_sha256" not in text
    )
    assert (
        "nport.dera_packages" not in text
        and '"basis"' not in text
        and '"code"' not in text
    )
    assert "/" not in text and "\\" not in text
    assert not re.search(
        r"[0-9a-f]{8}-[0-9a-f]{4}-", text
    )  # no UUIDs (release/publication ids are added by the DB)


@pytest.mark.parametrize(
    "change,reason",
    [
        (lambda o: o.update(extra=1), "keys"),
        (lambda o: o.update(accepted_events=[{"cusip9": "x"}]), "accepted_events"),
        (lambda o: o.update(tier="qualified"), "diagnostic_contract_unsupported"),
        (
            lambda o: o.update(schema_version="bond_default_events_display_v2"),
            "diagnostic_contract_unsupported",
        ),
        (lambda o: o.update(display_mode="events"), "diagnostic_contract_unsupported"),
        (
            lambda o: o.update(recommendation_eligible=True),
            "diagnostic_contract_unsupported",
        ),
        (lambda o: o.update(limitations=["not_a_code"]), "unknown_code"),
        (
            lambda o: o.update(limitations=["not_for_recommendation", "coverage_only"]),
            "sorted_unique",
        ),
        (lambda o: o["counts"].update(accepted_events=1), "counts"),
        (lambda o: o["counts"].update(unresolved_events=0), "counts"),
        (lambda o: o["counts"].update(candidate_issue_months=-1), "non_negative_int"),
        (lambda o: o["counts"].update(candidate_issue_months=True), "non_negative_int"),
        (lambda o: o["counts"].update(candidate_issue_months=1.5), "non_negative_int"),
        (lambda o: o["coverage"][0].update(private_id="abc"), "coverage:keys"),
        (lambda o: o["coverage"][0].update(state="qualified"), "constants"),
        (lambda o: o["coverage"][0].update(event_count=1), "events_not_zero"),
        (lambda o: o["coverage"][0].update(period_label="2026-13"), "period_label"),
        (
            lambda o: o["source_frontiers"][0].update(observed_at="2026-09-25"),
            "timestamp",
        ),
        (
            lambda o: o["source_frontiers"][0].update(inventory_kind="pickle"),
            "source_kind",
        ),
        (lambda o: o["source_frontiers"][0].update(frontier="2026-02-30"), "date_text"),
    ],
)
def test_projection_decoder_rejects_unknown_keys_values_and_types(change, reason):
    obj = projection_of(coverage_only_bundle()).to_json_obj()
    change(obj)
    with pytest.raises(d.DiagnosticError) as info:
        d.DiagnosticProjection.from_json_obj(obj)
    assert reason in str(info.value)


def test_nonfinite_numbers_and_floats_cannot_reach_the_canonical_form():
    obj = projection_of(coverage_only_bundle()).to_json_obj()
    obj["counts"]["panel_grid_keys"] = float("nan")
    with pytest.raises(d.DiagnosticError):
        d.DiagnosticProjection.from_json_obj(obj)
    with pytest.raises(c.ContractError):
        c.canonical_json_bytes(obj)


def test_constants_are_frozen_and_unsupported_versions_are_contract_errors():
    projection = projection_of(coverage_only_bundle())
    with pytest.raises(AttributeError):
        projection.tier = "qualified"  # type: ignore[misc]
    with pytest.raises(d.DiagnosticError) as info:
        d.DiagnosticProjection(
            source_frontiers=projection.source_frontiers,
            coverage=projection.coverage,
            counts=projection.counts,
            limitations=projection.limitations,
            schema_version="bond_default_events_display_v2",
        )
    assert info.value.reason == "diagnostic_contract_unsupported"
    with pytest.raises(d.DiagnosticError):
        d.DiagnosticProjection(
            source_frontiers=projection.source_frontiers,
            coverage=projection.coverage,
            counts=projection.counts,
            limitations=projection.limitations,
            accepted_events=(object(),),
        )  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Frontier records and derivation guards
# ---------------------------------------------------------------------------
def test_frontier_record_and_rationale_validation():
    rec = frontier_records()[0]
    assert set(rec) == {
        "basis",
        "code",
        "filing_count",
        "frontier",
        "inventory_kind",
        "manifest_sha256",
        "observed_at",
        "reason_codes",
        "source",
        "source_key",
        "state",
    }
    text = d.coverage_rationale(
        frontiers=[rec], reason_codes=["outcomes_unascertained"]
    )
    records, codes = d.parse_rationale(text)
    assert records == (rec,) and codes == ("outcomes_unascertained",)
    assert text == c.canonical_json_bytes(c.load_json_strict(text)).decode()

    def build(**changes):
        base = {
            "source": "sec_edgar",
            "inventory_kind": "census",
            "frontier": None,
            "observed_at": OBSERVED,
            "state": "inventory_only",
            "filing_count": None,
            "reason_codes": (),
            "basis": "ok",
            "code": "census_code",
            "manifest_sha256": MANIFEST_SHA,
            "source_key": "edgar.census",
        }
        return d.frontier_record(**{**base, **changes})

    for bad in (
        {"source": "sec_ncen"},
        {"inventory_kind": "pickle"},
        {"state": "qualified"},
        {"basis": "a/b"},
        {"basis": "C:\\raw\\path"},
        {"code": "Has Space"},
        {"manifest_sha256": MANIFEST_SHA[:12]},
        {"source_key": "../x"},
        {"filing_count": -1},
        {"filing_count": True},
        {"observed_at": dt.datetime(2026, 9, 25)},  # noqa: DTZ001 - naive on purpose
        {"reason_codes": ("bogus",)},
    ):
        with pytest.raises(d.DiagnosticError):
            build(**bad)
    for text_bad in (
        "not json",
        "[]",
        '{"format":"x","frontiers":[],"reason_codes":[]}',
        '{"format":"bond_default_coverage_rationale_v1","frontiers":[],"reason_codes":[],"extra":1}',
        '{"format":"bond_default_coverage_rationale_v1","frontiers":[{"source":"sec_nport"}],"reason_codes":[]}',
        '{"format":"bond_default_coverage_rationale_v1","frontiers":[],"reason_codes":["b","a"]}',
    ):
        with pytest.raises(d.DiagnosticError):
            d.parse_rationale(text_bad)


def test_frontier_manifest_digest_ignores_record_order_and_binds_every_field():
    records = frontier_records()
    digest = d.frontier_manifest_digest(records)
    assert digest == d.frontier_manifest_digest(list(reversed(records)))
    changed = [dict(r) for r in records]
    changed[0]["frontier"] = "2026-06-29"
    assert d.frontier_manifest_digest(changed) != digest
    changed = [dict(r) for r in records]
    changed[3]["manifest_sha256"] = hashlib.sha256(b"other").hexdigest()
    assert d.frontier_manifest_digest(changed) != digest


@pytest.mark.parametrize(
    "name,mutate,reason",
    [
        (
            "wrong_state",
            lambda cells: [_swap(cells[0], state="partial")] + cells[1:],
            "cell_shape",
        ),
        (
            "denominator",
            lambda cells: [_swap(cells[0], denominator_count=99)] + cells[1:],
            "cell_shape",
        ),
        (
            "event_count",
            lambda cells: [_swap(cells[0], event_count=1)] + cells[1:],
            "cell_shape",
        ),
        (
            "lag",
            lambda cells: (
                [_swap(cells[0], lag_p50_days=1, lag_p90_days=1, lag_max_days=1)]
                + cells[1:]
            ),
            "cell_shape",
        ),
        (
            "stratum",
            lambda cells: [_swap(cells[0], rating_stratum="HY")] + cells[1:],
            "cell_shape",
        ),
        (
            "source_event",
            lambda cells: (
                [_swap(cells[0], source="sec_nport", event_type="bankruptcy")]
                + cells[1:]
            ),
            "cell_shape",
        ),
        ("missing_cell", lambda cells: cells[1:], "cells_per_period"),
        (
            "gap_period",
            lambda cells: [x for x in cells if x.period_label != "2026-05"],
            "periods_not_contiguous",
        ),
        (
            "exposure",
            lambda cells: (
                [
                    _swap(
                        cells[0],
                        exposed_issue_months=9,
                        denominator_count=9,
                        unknown_outcome_issue_months=9,
                    )
                ]
                + cells[1:]
            ),
            "exposure_not_panel_start_count",
        ),
    ],
)
def test_derivation_rejects_cells_that_do_not_fit_the_coverage_only_contract(
    name, mutate, reason
):
    bundle = coverage_only_bundle(mutate_cells=mutate)
    with pytest.raises(d.DiagnosticError) as info:
        d.derive_projection_from_bundle(bundle)
    assert reason in str(info.value), name


def _swap(cell, **changes):
    from dataclasses import replace

    return replace(cell, **changes)


def test_derivation_requires_the_last_period_to_be_the_target_month_and_frontiers_before_the_cutoff():
    good = coverage_only_bundle()
    later_cutoff = coverage_only_bundle(
        knowledge_cutoff=K0 - dt.timedelta(days=2), label="early"
    )
    with pytest.raises(d.DiagnosticError, match="frontier_observed_after_cutoff"):
        d.derive_projection_from_bundle(later_cutoff)
    with pytest.raises(d.DiagnosticError, match="last_period_not_target_month"):
        d.derive_projection(
            target_month=dt.date(2026, 9, 1),
            knowledge_cutoff=K0,
            panel_grid_count=1,
            coverage=good.frames["coverage"],
        )  # type: ignore[arg-type]


def test_inconsistent_or_missing_frontier_records_are_refused():
    records = frontier_records()
    other = dict(records[0], frontier="2026-06-29")
    inconsistent = coverage_only_bundle(records=records + [other])
    with pytest.raises(d.DiagnosticError, match="frontier_records_inconsistent"):
        d.derive_projection_from_bundle(inconsistent)
    with pytest.raises(d.DiagnosticError, match="no_frontier_records"):
        d.derive_projection_from_bundle(coverage_only_bundle(records=[]))


def test_bundle_check_rejects_events_nonmissing_ratings_and_qualified_confusion():
    good = coverage_only_bundle()
    from dataclasses import replace

    def with_ratings(bundle, ratings):
        return c.assemble_bundle(
            target_month=T0,
            knowledge_cutoff=K0,
            knowledge_mode="current_run",
            build_scope="limited",
            quality_state="partial",
            code_digest=bundle.manifest["code_digest"],
            panel_publication_id=bundle.manifest["panel_publication_id"],
            panel_grid=bundle.panel_grid,
            issuer_mapping_digest=None,
            rating_declarations=bundle.manifest["rating_declarations"],
            rating_input_digest=bundle.manifest["rating_input_digest"],
            validation_receipt=None,
            source_packages=[],
            observations=[],
            event_links=[],
            adjudications=[],
            events=[],
            followups=[],
            exit_evidence=[],
            coverage=bundle.frames["coverage"],
            ratings=ratings,
            ncen_filings=[],
            family_contexts=[],
            family_evidence=[],
            proposal_evidence=[],
            exchange_relations=[],
        )

    ratings = list(good.frames["ratings"])
    ratings[0] = replace(ratings[0], state="stale")
    with pytest.raises(d.DiagnosticError, match="ratings_not_all_missing"):
        d.derive_projection_from_bundle(with_ratings(good, ratings))
    with pytest.raises(d.DiagnosticError, match="ratings_not_all_missing"):
        d.derive_projection_from_bundle(with_ratings(good, ratings[:-2]))


# ---------------------------------------------------------------------------
# Release identity
# ---------------------------------------------------------------------------
IDENTITY = {
    "publication_id": uuid.UUID("11111111-2222-8333-9444-555555555555"),
    "publication_fingerprint": _sha("fp"),
    "target_month": T0,
    "knowledge_cutoff": K0,
    "policy_digest": _sha("policy"),
    "contract_digest": _sha("contract"),
    "source_frontier_manifest_digest": _sha("frontier"),
    "projection_digest": _sha("projection"),
    "diagnostic_sql_digest": _sha("sql"),
}


def test_release_id_is_a_deterministic_uuid_v5_over_identity_without_audit_fields():
    identity = d.release_identity(**IDENTITY)
    assert (
        "created_at" not in identity
        and "created_by" not in identity
        and "promoted_at" not in identity
    )
    release_id = d.release_id_for(identity)
    assert release_id == d.release_id_for(dict(reversed(list(identity.items()))))
    assert release_id.version == 5 and release_id.variant == uuid.RFC_4122
    assert release_id != IDENTITY["publication_id"]
    for name in (
        "publication_fingerprint",
        "policy_digest",
        "contract_digest",
        "source_frontier_manifest_digest",
        "projection_digest",
        "diagnostic_sql_digest",
    ):
        assert (
            d.release_id_for(d.release_identity(**{**IDENTITY, name: _sha("other")}))
            != release_id
        ), name
    assert (
        d.release_id_for(
            d.release_identity(**{**IDENTITY, "target_month": dt.date(2026, 9, 1)})
        )
        != release_id
    )
    assert (
        d.release_id_for(
            d.release_identity(
                **{**IDENTITY, "knowledge_cutoff": K0 + dt.timedelta(seconds=1)}
            )
        )
        != release_id
    )
    assert (
        d.release_id_for(
            d.release_identity(**{**IDENTITY, "publication_id": uuid.uuid4()})
        )
        != release_id
    )
    # Golden vector: SQL bond_default_diag_release_id_for must produce this value (asserted in the DB suite).
    assert str(release_id) == GOLDEN_RELEASE_ID


GOLDEN_RELEASE_ID = "a0039431-f4bf-58b0-90fe-bb6995110f50"


# ---------------------------------------------------------------------------
# API guards that need no database
# ---------------------------------------------------------------------------
def test_install_requires_public_autocommit_and_the_pinned_digest(monkeypatch):
    with pytest.raises(ValueError, match="diagnostic_schema_must_be_public"):
        d.install_diagnostic_schema(SimpleNamespace(autocommit=True), schema="bond")
    with pytest.raises(ValueError, match="autocommit_connection_required_for_ddl"):
        d.install_diagnostic_schema(SimpleNamespace(autocommit=False), schema="public")
    monkeypatch.setattr(d, "DIAGNOSTIC_SQL_DIGEST", _sha("tampered"))
    with pytest.raises(d.DiagnosticError, match="diagnostic_sql_digest_not_pinned"):
        d.install_diagnostic_schema(SimpleNamespace(autocommit=True), schema="public")


def test_dml_helpers_validate_arguments_before_touching_the_connection():
    class Boom:
        def execute(self, *a, **k):
            raise AssertionError("connection must not be used")

    with pytest.raises(TypeError):
        d.prepare_diagnostic(
            Boom(),
            schema="public",
            publication_id=uuid.uuid4(),
            projection={},  # type: ignore[arg-type]
            source_frontier_manifest_digest=_sha("x"),
        )
    with pytest.raises(d.DiagnosticError, match="invalid_argument"):
        d.revoke_diagnostic(
            Boom(), schema="public", release_id=uuid.uuid4(), reason_code="Bad Reason"
        )


def test_module_never_commits_or_installs_implicitly():
    source = Path(d.__file__).read_text(encoding="utf-8")
    # The DML entry points (prepare .. read_current) run in the caller's transaction and never install.
    dml = source.split("def prepare_diagnostic", 1)[1].split(
        "def read_current_diagnostic", 1
    )[0]
    assert (
        "commit(" not in dml and ".transaction(" not in dml and "autocommit" not in dml
    )
    assert (
        "install_diagnostic_schema(" not in dml
        and "harden_installed_privileges(" not in dml
    )
    assert "commit(" not in source and "autocommit =" not in source


# ---------------------------------------------------------------------------
# Installed-object manifest and privilege helpers (offline part)
# ---------------------------------------------------------------------------
def test_installed_manifest_is_the_exact_name_set_read_from_the_sql_files():
    manifest = d.installed_manifest()
    assert len(manifest.tables) == 23 and len(manifest.functions) == 121
    assert manifest.tables == tuple(sorted(manifest.tables))
    assert manifest.functions == tuple(sorted(manifest.functions))
    assert {
        "bond_default_diagnostic_releases",
        "bond_default_diagnostic_pointer",
        "bond_default_diagnostic_revocations",
        "bond_credit_publications",
        "bond_rating_history_public_v1",
    } <= set(manifest.tables)
    assert {
        "bond_default_current_diagnostic_release",
        "bond_default_prepare_diagnostic",
        "bond_credit_expected_pins",
        "bond_credit_read_coverage",
    } <= set(manifest.functions)
    assert not any("*" in n or "%" in n for n in manifest.tables + manifest.functions)
    assert (
        manifest == d.installed_manifest()
        and manifest.digest() == d.installed_manifest().digest()
    )
    assert (
        manifest.digest() != d.installed_manifest(c.SQL_PATHS).digest()
    )  # the diagnostic file is in scope
    assert len(manifest.grants) == 20
    for statement in manifest.grants:
        assert statement.startswith("GRANT ") and statement.endswith(";")
        assert statement.split(" TO ")[-1].rstrip(";").replace("\n", " ").split(",")
    assert d.INSTALLED_SQL_PATHS == (*c.SQL_PATHS, d.SQL_PATH)
    assert set(d.INTENDED_ROLES) == {
        "bond_credit_writer",
        "bond_credit_reader",
        "bond_credit_auditor",
        "bond_default_diagnostic_reader",
    }


@pytest.mark.parametrize(
    "statement",
    [
        "GRANT SELECT ON bond_x TO PUBLIC;",
        "GRANT SELECT ON bond_x TO app_runtime;",
        "GRANT SELECT ON bond_x TO bond_credit_reader WITH GRANT OPTION;",
        "GRANT SELECT ON bond_x TO bond_credit_reader, app_runtime;",
    ],
)
def test_manifest_refuses_grants_to_anything_but_the_intended_roles(
    tmp_path, statement
):
    bad = tmp_path / "bad.sql"
    bad.write_text(
        "CREATE TABLE IF NOT EXISTS bond_x (id integer);\n"
        "CREATE OR REPLACE FUNCTION bond_f() RETURNS integer AS $$ SELECT 1 $$;\n"
        + statement
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(d.DiagnosticError, match="privileges_manifest_invalid"):
        d.installed_manifest([bad])
    good = tmp_path / "good.sql"
    good.write_text(
        "CREATE TABLE IF NOT EXISTS bond_x (id integer);\n"
        "CREATE OR REPLACE FUNCTION bond_f() RETURNS integer AS $$ SELECT 1 $$;\n"
        "GRANT SELECT ON bond_x TO bond_credit_reader, bond_credit_writer;\n",
        encoding="utf-8",
    )
    manifest = d.installed_manifest([good])
    assert (manifest.tables, manifest.functions) == (("bond_x",), ("bond_f",))


def test_privilege_helpers_validate_arguments_before_touching_the_connection():
    class Boom:
        autocommit = False

        def execute(self, *a, **k):
            raise AssertionError("connection must not be used")

    with pytest.raises(ValueError, match="privileges_schema_must_be_public"):
        d.harden_installed_privileges(Boom(), schema="other")
    with pytest.raises(ValueError, match="autocommit_connection_required_for_ddl"):
        d.harden_installed_privileges(Boom(), schema="public")
    with pytest.raises(ValueError, match="privileges_schema_must_be_public"):
        d.verify_installed_privileges(Boom(), schema="other")


def test_privilege_helpers_do_not_alter_default_privileges_or_use_patterns():
    source = Path(d.__file__).read_text(encoding="utf-8")
    assert "ALTER DEFAULT PRIVILEGES" not in source.upper()
    body = source.split("def harden_installed_privileges", 1)[1].split(
        "def verify_installed_privileges", 1
    )[0]
    assert "LIKE" not in body.upper() and "~" not in body


# ---------------------------------------------------------------------------
# L5 (cell frontier provenance) and L9 (strict types) parity checks
# ---------------------------------------------------------------------------
def _records_for(source: str, records: list[dict]) -> list[dict]:
    return [r for r in records if r["source"] == source]


@pytest.mark.parametrize(
    "name,mutate,reason",
    [
        (
            "all_all_carries_frontier",
            lambda cells: (
                [_swap(cells[0], source_frontier=dt.date(2026, 9, 24))] + cells[1:]
            ),
            "cell_frontier",
        ),
        (
            "source_cell_foreign_record",
            lambda cells: [
                _swap(
                    x,
                    rationale=d.coverage_rationale(
                        frontiers=frontier_records(), reason_codes=[]
                    ),
                )
                if (x.source, x.event_type) == ("sec_nport", "default_state")
                else x
                for x in cells
            ],
            "cell_frontier",
        ),
        (
            "source_cell_without_records",
            lambda cells: [
                _swap(x, rationale=d.coverage_rationale(frontiers=[], reason_codes=[]))
                if x.source == "agency_rocr"
                else x
                for x in cells
            ],
            "cell_frontier",
        ),
        (
            "frontier_not_in_cell_records",
            lambda cells: [
                _swap(x, source_frontier=dt.date(2026, 1, 1))
                if x.source == "sec_nport"
                else x
                for x in cells
            ],
            "cell_frontier",
        ),
        (
            "two_manifest_hashes",
            lambda cells: [
                _swap(
                    x,
                    rationale=d.coverage_rationale(
                        frontiers=[
                            d.frontier_record(
                                source="sec_nport",
                                inventory_kind="dera_packages",
                                frontier=dt.date(2026, 6, 30),
                                observed_at=OBSERVED,
                                state="inventory_only",
                                filing_count=None,
                                reason_codes=["nport_q3_unavailable_at_observation"],
                                basis="N-PORT filings through 2026-06-30; Q3 unavailable at observation",
                                code="nport_dera_packages",
                                manifest_sha256=hashlib.sha256(b"other").hexdigest(),
                                source_key="nport.dera_packages",
                            )
                        ],
                        reason_codes=[],
                    ),
                )
                if (x.source, x.event_type) == ("sec_nport", "default_state")
                and x.period_label == "2026-08"
                else x
                for x in cells
            ],
            "frontier_records_inconsistent|manifest_sha256_not_single",
        ),
    ],
)
def test_source_cells_must_carry_their_own_source_frontier_records(
    name, mutate, reason
):
    bundle = coverage_only_bundle(mutate_cells=mutate)
    with pytest.raises(d.DiagnosticError) as info:
        d.derive_projection_from_bundle(bundle)
    assert re.search(reason, str(info.value)), (name, str(info.value))


def test_a_null_source_frontier_is_allowed_on_source_cells():
    bundle = coverage_only_bundle(
        mutate_cells=lambda cells: [
            _swap(x, source_frontier=None) if x.source == "sec_edgar" else x
            for x in cells
        ]
    )
    projection = d.derive_projection_from_bundle(bundle)
    assert all(
        cell.source_frontier is None
        for cell in projection.coverage
        if cell.source in ("all", "sec_edgar")
    )


@pytest.mark.parametrize(
    "path,value",
    [
        (("counts", "accepted_events"), False),
        (("counts", "accepted_events"), 0.0),
        (("counts", "accepted_events"), True),
        (("counts", "unresolved_events"), 0),
        (("counts", "censored_issue_months"), False),
        (("recommendation_eligible",), 0),
        (("recommendation_eligible",), 0.0),
        (("coverage", 0, "event_count"), False),
        (("coverage", 0, "denominator_count"), 1.0),
        (("counts", "panel_grid_keys"), 2_147_483_648),
    ],
)
def test_strict_bool_int_parity_in_the_json_decoder(path, value):
    obj = projection_of(coverage_only_bundle()).to_json_obj()
    target = obj
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(d.DiagnosticError):
        d.DiagnosticProjection.from_json_obj(obj)


def test_non_ascii_digits_are_not_timestamps_dates_or_periods():
    arabic = "\u0662\u0660\u0662\u0666-08-01"  # Arabic-Indic digits
    obj = projection_of(coverage_only_bundle()).to_json_obj()
    obj["source_frontiers"][0]["frontier"] = arabic
    with pytest.raises(d.DiagnosticError, match="date_text_expected"):
        d.DiagnosticProjection.from_json_obj(obj)
    obj = projection_of(coverage_only_bundle()).to_json_obj()
    obj["source_frontiers"][0]["observed_at"] = (
        "\u0662\u0660\u0662\u0666-09-25T00:00:00.000000Z"
    )
    with pytest.raises(d.DiagnosticError, match="timestamp_text_expected"):
        d.DiagnosticProjection.from_json_obj(obj)
    assert not d._PERIOD.fullmatch("\u0662\u0660\u0662\u0666-08")


def test_sql_and_python_share_the_integer_ceiling_and_bigint_counts():
    text = d.SQL_PATH.read_text(encoding="utf-8")
    assert d._INT_MAX == 2_147_483_647 and text.count("2147483647") >= 2
    assert "2::bigint * pub.panel_grid_count::bigint" in text
    assert "::integer <> pub.panel_grid_count" not in text
    assert "COALESCE(c.relacl, pg_catalog.acldefault('r'" in text
    assert "COALESCE(p.proacl, pg_catalog.acldefault('f'" in text
