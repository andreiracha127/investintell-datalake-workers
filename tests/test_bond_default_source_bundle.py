"""Coverage-only source-frontier bundle builder (``source_bundle``): manifest, panel read, composition.

The pure suites need no database. The DB-backed suites run only against an explicit loopback
``BOND_DEFAULT_TEST_DATABASE_URL`` whose database name contains ``disposable`` (same guard as
``test_bond_default_publication_db``); they create minimal panel tables in ``public`` from the
production DDL text of ``schemas/bond_panel_v1.sql`` and remove them again. A skipped DB suite is
not acceptance evidence.
"""

from __future__ import annotations

import ast
import builtins
import copy
import datetime as dt
import importlib.util
import json
import os
import pickle
import re
import sys
import uuid
from collections.abc import Iterable, Mapping
from pathlib import Path

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import diagnostic_publication as dp
from src.bonds.default_events import publication as pub
from src.bonds.default_events import source_bundle as sb

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = (
    ROOT / "contracts" / "bonds" / "default_events_frontier_manifest_2026-08.json"
)
PANEL_SQL = ROOT / "schemas" / "bond_panel_v1.sql"
SOURCE_PATH = ROOT / "src" / "bonds" / "default_events" / "source_bundle.py"
UTC = dt.timezone.utc
T = dt.date(2026, 8, 1)
K = dt.datetime(2026, 9, 29, 18, 0, 0, tzinfo=UTC)
CODE_DIGEST = "sha256:" + "ab" * 32
PROD_PANEL_ID = uuid.UUID("65156481-8cb4-52b5-8676-cf77edc5644f")
MONTHS = sb.expected_grid_months(T)
DB_ENV = "BOND_DEFAULT_TEST_DATABASE_URL"


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------
def load_manifest() -> dict:
    return c.load_json_strict(MANIFEST_PATH.read_bytes())


def redigest(doc: dict) -> dict:
    doc["digest"] = c.document_digest(doc, "digest")
    return doc


def manifest_for(
    panel_id: uuid.UUID = PROD_PANEL_ID,
    *,
    max_grid_rows: int = 1_200_000,
    max_bundle_bytes: int = 8 << 30,
    mutate=None,
) -> dict:
    doc = copy.deepcopy(load_manifest())
    doc["panel"]["expected_publication_id"] = str(panel_id)
    doc["limits"] = {
        "max_grid_rows": max_grid_rows,
        "max_bundle_bytes": max_bundle_bytes,
    }
    if mutate is not None:
        mutate(doc)
    return redigest(doc)


def validate(doc: Mapping, **kwargs):
    args = {
        "target_month": T,
        "knowledge_cutoff": K,
        "expected_panel_publication_id": PROD_PANEL_ID,
    }
    args.update(kwargs)
    return sb.validate_source_manifest(doc, **args)


# ---------------------------------------------------------------------------
# Pure panel stand-in (no database)
# ---------------------------------------------------------------------------
def make_cusip(index: int) -> str:
    base = f"{index:08d}"
    return base + c.cusip_check_digit(base)


def fake_panel(
    counts: Mapping[dt.date, int], panel_id: uuid.UUID = PROD_PANEL_ID
) -> sb.PanelRead:
    grid = sorted(
        ((make_cusip(i), month) for month, n in counts.items() for i in range(n)),
        key=lambda item: f"{item[0]}|{item[1].isoformat()}",
    )
    publication = sb.PanelPublication(
        panel_id, None, sb.PANEL_CURRENT_CONFIG, MONTHS[0], MONTHS[-1]
    )
    count = sb.PanelCount(
        chain=(publication,),
        month_counts=tuple(sorted(counts.items())),
        winning_depth_counts=(len(grid),),
        eligibility_counts=(("included", len(grid)),),
    )
    return sb.PanelRead(panel_id, count, tuple(grid), c.grid_digest(grid))


def varied_counts(base: int = 2) -> dict[dt.date, int]:
    """Deliberately different per-month counts: exposure must follow the panel, not a product."""
    return {month: base + (index % 5) for index, month in enumerate(MONTHS)}


def compose(panel: sb.PanelRead, manifest_doc: Mapping, **overrides):
    manifest = validate(
        manifest_doc, expected_panel_publication_id=panel.publication_id
    )
    coverage = sb.build_coverage(panel.month_counts, manifest, T)
    args = {
        "target_month": T,
        "knowledge_cutoff": K,
        "knowledge_mode": "current_run",
        "code_digest": CODE_DIGEST,
    }
    args.update(overrides)
    return sb.compose_bundle(panel, manifest, coverage, **args)


# ---------------------------------------------------------------------------
# The committed manifest
# ---------------------------------------------------------------------------
def test_committed_manifest_is_valid_sanitized_and_self_pinned():
    raw = MANIFEST_PATH.read_bytes()
    assert b"\r" not in raw and raw.endswith(b"\n")
    doc = c.load_json_strict(raw)
    manifest = validate(doc, expected_panel_publication_id=PROD_PANEL_ID)
    assert manifest.digest == doc["digest"] == c.document_digest(doc, "digest")
    assert sorted(manifest.frontiers) == list(sb.REQUIRED_FRONTIER_KEYS)
    text = raw.decode("utf-8")
    assert not re.search(r"[A-Za-z]:\\|/Users/|/home/|/tmp/|AppData|\.pkl|pickle", text)
    assert manifest.latest_observation() <= K
    for record in manifest.frontiers.values():
        assert record.state in ("inventory_only", "unavailable")
    shas = re.findall(r'"[a-z_0-9]*sha256": "([^"]*)"', text)
    assert len(shas) > 60 and all(
        re.fullmatch(r"[0-9a-f]{64}", value) for value in shas
    )


def test_committed_manifest_limits_bracket_the_measured_production_grid():
    limits = (
        validate(load_manifest()).max_grid_rows,
        validate(load_manifest()).max_bundle_bytes,
    )
    measured_g = 1_127_685  # production bond_panel_v1 pointer 65156481, 2021-08..2026-08 (read-only preflight)
    assert measured_g <= limits[0] <= int(measured_g * 1.10)
    estimate = sb.estimate_bundle_bytes(measured_g, 700_000)
    assert estimate <= limits[1] <= int(estimate * 1.30)


def test_committed_manifest_facts():
    doc = load_manifest()
    by_key = {f["source_key"]: f for f in doc["frontiers"]}
    assert doc["target_month"] == "2026-08-01" and doc["panel"]["months"] == 61
    nport = by_key["sec_nport.dera_packages"]
    assert (
        nport["frontier"] == "2026-06-30" and len(nport["evidence"]["artifacts"]) == 27
    )
    assert nport["evidence"]["absence_probe"]["http_status"] == 404
    assert "nport_q3_unavailable_at_observation" in nport["reason_codes"]
    ncen = by_key["sec_ncen.dera_packages"]
    assert ncen["frontier"] == "2026-06-30" and len(ncen["evidence"]["artifacts"]) == 32
    assert by_key["sec_ncen.form_index"]["frontier"] == "2026-09-24"
    assert by_key["sec_edgar.submissions"]["evidence"]["artifacts"][0]["sha256"] == (
        "3f894f8c1ef56c0068f7f8a4cd994dc0fb2619946bd23f7affb90384de6fecb8"
    )
    census = by_key["sec_edgar.census"]
    assert census["evidence"]["artifacts"][0]["sha256"] == (
        "b863eae9f346f9c007baaa383a13a5e7e3e2278e7dd585730eb038f3b9e7427e"
    )
    assert (
        census["evidence"]["item_1_03_filings"],
        census["evidence"]["item_2_04_filings"],
    ) == (495, 766)
    agency = by_key["agency_rocr.agency_history"]
    assert agency["state"] == "unavailable" and agency["frontier"] is None
    assert "rating_rights_unverified" in agency["reason_codes"]
    assert all(f["ingestion"] == "not_ingested" for f in doc["frontiers"])


# ---------------------------------------------------------------------------
# Manifest validation: corrupt, late, unsanitized
# ---------------------------------------------------------------------------
def _set_frontier(key: str, **changes):
    def mutate(doc):
        for item in doc["frontiers"]:
            if item["source_key"] == key:
                item.update(changes)

    return mutate


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            _set_frontier(
                "sec_nport.dera_packages", observed_at="2026-09-29T18:00:00.000001Z"
            ),
            "observed_after_cutoff",
        ),
        (
            _set_frontier(
                "sec_nport.dera_packages", observed_at="2026-09-25T03:16:02Z"
            ),
            "timestamp_not_canonical",
        ),
        (_set_frontier("sec_nport.dera_packages", state="qualified"), "state_invalid"),
        (
            _set_frontier("sec_nport.dera_packages", ingestion="ingested"),
            "ingestion_invalid",
        ),
        (
            _set_frontier("sec_nport.dera_packages", reason_codes=["made_up"]),
            "reason_codes_invalid",
        ),
        (
            _set_frontier(
                "sec_nport.dera_packages",
                reason_codes=["inventory_not_ingested", "coverage_only"],
            ),
            "reason_codes_invalid",
        ),
        (
            _set_frontier("sec_nport.dera_packages", filing_count=-1),
            "filing_count_invalid",
        ),
        (
            _set_frontier("sec_nport.dera_packages", filing_count=True),
            "filing_count_invalid",
        ),
        (
            _set_frontier("sec_nport.dera_packages", basis="C:\\Users\\andre\\x"),
            "frontier_record_invalid",
        ),
        (
            _set_frontier("sec_nport.dera_packages", basis="see /home/andre/nport"),
            "frontier_record_invalid",
        ),
        (
            _set_frontier("sec_nport.dera_packages", basis="from ncen_index.pkl"),
            "forbidden_text",
        ),
        (
            _set_frontier("sec_nport.dera_packages", basis="C1 payload consensus"),
            "forbidden_text",
        ),
        (_set_frontier("sec_nport.dera_packages", frontier=None), "frontier_required"),
        (
            _set_frontier("sec_nport.dera_packages", source_key="sec_nport.census"),
            "source_key_mismatch",
        ),
        (_set_frontier("sec_nport.dera_packages", evidence={}), "evidence_required"),
        (
            _set_frontier(
                "sec_nport.dera_packages", evidence={"packages_json_sha256": "48bdb7bc"}
            ),
            "sha256_not_complete",
        ),
        (
            _set_frontier(
                "sec_nport.dera_packages",
                evidence={"artifacts": [{"sha256": "f" * 63}]},
            ),
            "sha256_not_complete",
        ),
        (lambda d: d["frontiers"].pop(), "frontier_set_mismatch"),
        (
            lambda d: d["frontiers"].append(dict(d["frontiers"][0])),
            "source_key_duplicate",
        ),
        (lambda d: d.update(extra=1), "fields_mismatch"),
        (lambda d: d.update(version="v0"), "version_unsupported"),
        (lambda d: d.update(mode="ingest"), "product_or_mode_mismatch"),
        (lambda d: d.update(target_month="2026-07-01"), "target_month_mismatch"),
        (lambda d: d["panel"].update(months=60), "panel_mismatch"),
        (lambda d: d["panel"].update(first_month="2021-09-01"), "panel_mismatch"),
        (
            lambda d: d["panel"].update(expected_publication_id=str(uuid.uuid4())),
            "panel_publication_mismatch",
        ),
        (lambda d: d["limits"].update(max_grid_rows=0), "limit_invalid"),
        (lambda d: d["limits"].update(max_bundle_bytes="8"), "limit_invalid"),
        (lambda d: d["limits"].update(max_grid_rows=True), "limit_invalid"),
    ],
)
def test_corrupt_or_late_manifest_is_rejected(mutate, code):
    doc = manifest_for(mutate=mutate)
    with pytest.raises(sb.SourceManifestError, match=code):
        validate(doc)


def test_non_canonical_float_in_the_manifest_is_rejected():
    doc = copy.deepcopy(load_manifest())
    doc["frontiers"][0]["evidence"] = {"ratio": 1.5}
    with pytest.raises(sb.SourceManifestError, match="manifest_not_canonical"):
        validate(doc)


def test_digest_tampering_is_rejected_without_redigest():
    doc = copy.deepcopy(load_manifest())
    doc["frontiers"][0]["basis"] = "tampered after sealing"
    with pytest.raises(sb.SourceManifestError, match="manifest_digest_mismatch"):
        validate(doc)
    doc = copy.deepcopy(load_manifest())
    doc["digest"] = "sha256:" + "0" * 64
    with pytest.raises(sb.SourceManifestError, match="manifest_digest_mismatch"):
        validate(doc)
    doc["digest"] = "sha256:abc"
    with pytest.raises(sb.SourceManifestError, match="manifest_digest_invalid"):
        validate(doc)


def test_manifest_observed_exactly_at_cutoff_is_allowed_and_one_tick_later_is_not():
    latest = validate(load_manifest()).latest_observation()
    validate(load_manifest(), knowledge_cutoff=latest)
    with pytest.raises(sb.SourceManifestError, match="observed_after_cutoff"):
        validate(
            load_manifest(), knowledge_cutoff=latest - dt.timedelta(microseconds=1)
        )


def test_cutoff_and_pins_are_required():
    with pytest.raises(
        sb.SourceBundleError, match="knowledge_cutoff_timezone_required"
    ):
        validate(
            load_manifest(),
            knowledge_cutoff=dt.datetime.fromisoformat("2026-09-29T18:00:00"),
        )
    with pytest.raises(sb.SourceManifestError, match="panel_publication_mismatch"):
        validate(load_manifest(), expected_panel_publication_id=uuid.uuid4())
    with pytest.raises(sb.SourceManifestError, match="target_month_mismatch"):
        validate(load_manifest(), target_month=dt.date(2026, 7, 1))


# ---------------------------------------------------------------------------
# Pure composition
# ---------------------------------------------------------------------------
def test_compose_is_deterministic_and_carries_the_real_pins():
    panel = fake_panel(varied_counts())
    one = compose(panel, manifest_for())
    two = compose(fake_panel(varied_counts()), manifest_for())
    assert one.canonical_bytes() == two.canonical_bytes()
    assert one.bundle_digest() == two.bundle_digest()
    assert one.publication_id == two.publication_id
    m = one.manifest
    assert (
        m["policy_digest"] == c.POLICY_DIGEST
        and m["contract_digest"] == c.SCHEMA_DIGEST
    )
    assert m["sql_digest"] == c.sql_digest() and m["code_digest"] == CODE_DIGEST
    assert (m["quality_state"], m["build_scope"], m["knowledge_mode"]) == (
        "partial",
        "limited",
        "current_run",
    )
    assert m["issuer_mapping_digest"] is None and m["validation_digest"] is None
    assert m.receipt() is None
    assert m["panel_publication_id"] == PROD_PANEL_ID
    assert (
        m["panel_grid_count"] == len(panel.grid)
        and m["panel_grid_digest"] == panel.grid_digest
    )
    assert c.normalize_rating_declarations_record(m["rating_declarations"]) == {
        "version": c.RATING_DECLARATIONS_VERSION,
        "rating_scopes": [],
        "uncleared_rating_sources": [],
    }
    one.verify_frames_against_manifest()
    pub.check_bundle(one)
    pub.verify_schema(one)


def test_identity_moves_with_the_frozen_inputs():
    panel = fake_panel(varied_counts())
    base = compose(panel, manifest_for())
    later = compose(panel, manifest_for(), knowledge_cutoff=K + dt.timedelta(seconds=1))
    other_code = compose(panel, manifest_for(), code_digest="sha256:" + "cd" * 32)
    other_manifest = compose(
        panel, manifest_for(mutate=_set_frontier("sec_edgar.census", filing_count=1087))
    )
    assert (
        len({base.publication_id, later.publication_id, other_code.publication_id}) == 3
    )
    # The frozen fingerprint (contracts.FINGERPRINT_FIELDS) binds inputs, not output frames: a changed
    # frontier manifest under the same T/K/panel/code does NOT move the publication id, but it changes
    # the coverage digest, so a store refuses the replay as a publication collision (fail-loud).
    assert other_manifest.publication_id == base.publication_id
    assert (
        other_manifest.manifest["coverage_digest"] != base.manifest["coverage_digest"]
    )
    assert pub.persisted_identity(other_manifest) != pub.persisted_identity(base)
    store = pub.InMemoryPublicationStore()
    assert pub.prepare_bundle(store, base) == "inserted"
    with pytest.raises(pub.PublicationCollision):
        pub.prepare_bundle(store, other_manifest)


def test_exactly_two_missing_rating_rows_per_grid_key_and_empty_evidence_frames():
    panel = fake_panel(varied_counts())
    bundle = compose(panel, manifest_for())
    g = len(panel.grid)
    ratings = bundle.frames["ratings"]
    assert len(ratings) == 2 * g
    assert {(r.cusip_id, r.month, r.view_kind) for r in ratings} == {
        (cusip, month, view) for cusip, month in panel.grid for view in sb.RATING_VIEWS
    }
    for row in ratings:
        assert row.state == "missing" and row.bucket is None and row.action_date is None
        assert (
            row.public_known_at is None
            and row.agency_source_ids == ()
            and row.binding_link_ids == ()
        )
        assert row.coverage_frontier is None and row.action_input_digest is None
        assert row.default_overlay_episode_id is None
    empty = (
        "source_packages",
        "observations",
        "event_links",
        "adjudications",
        "ncen_filings",
        "events",
        "followups",
        "exit_evidence",
        "family_contexts",
        "family_evidence",
        "proposal_evidence",
        "exchange_relations",
        "publication_sources",
    )
    assert all(bundle.frames[name] == () for name in empty)
    assert bundle.manifest["rating_input_digest"] == c.rating_input_manifest_digest(
        bundle.manifest["rating_declarations"], ()
    )


def test_coverage_cells_follow_panel_start_exposure_not_the_grid_product():
    counts = varied_counts()
    panel = fake_panel(counts)
    bundle = compose(panel, manifest_for())
    cells = bundle.frames["coverage"]
    assert len(cells) == 60 * 6
    outcome = sb.outcome_months(T)
    assert outcome[0] == dt.date(2021, 9, 1) and outcome[-1] == T and len(outcome) == 60
    seen = set()
    for cell in cells:
        seen.add((cell.source, cell.event_type))
        month = dt.date(int(cell.period_label[:4]), int(cell.period_label[5:]), 1)
        starts = counts[sb.add_months(month, -1)]
        assert (
            cell.denominator_count
            == cell.exposed_issue_months
            == cell.unknown_outcome_issue_months
            == starts
        )
        assert (cell.event_count, cell.unlinked_count, cell.date_uncertain_count) == (
            0,
            0,
            0,
        )
        assert (cell.rating_stratum, cell.exposure_cohort, cell.state) == (
            "unknown",
            "all",
            "unavailable",
        )
        assert (
            cell.denominator_basis == "panel_exposure"
            and cell.validation_receipt_digest is None
        )
        assert (cell.lag_p50_days, cell.lag_p90_days, cell.lag_max_days) == (
            None,
            None,
            None,
        )
    assert seen == {
        ("all", "all"),
        ("sec_nport", "default_state"),
        ("sec_edgar", "bankruptcy"),
        ("sec_edgar", "payment_default"),
        ("sec_edgar", "distressed_exchange"),
        ("agency_rocr", "agency_issue_default"),
    }
    # candidate issue months exclude the target-month snapshot (60 starts) and are not summed across sources
    all_cells = [cell for cell in cells if cell.source == "all"]
    assert sum(cell.exposed_issue_months for cell in all_cells) == sum(
        counts[m] for m in MONTHS[:-1]
    )
    frontier = {(cell.source, cell.event_type): cell.source_frontier for cell in cells}
    assert frontier[("sec_nport", "default_state")] == dt.date(2026, 6, 30)
    assert frontier[("sec_edgar", "bankruptcy")] == dt.date(2026, 8, 31)
    assert (
        frontier[("agency_rocr", "agency_issue_default")] is None
        and frontier[("all", "all")] is None
    )


def test_frontier_provenance_is_embedded_canonically_in_the_rationale():
    manifest = validate(manifest_for())
    bundle = compose(fake_panel(varied_counts()), manifest_for())
    one_month = {
        (cell.source, cell.event_type): cell
        for cell in bundle.frames["coverage"]
        if cell.period_label == "2026-08"
    }
    for cell in one_month.values():
        assert cell.rationale.encode("utf-8") == c.canonical_json_bytes(
            json.loads(cell.rationale)
        )
        payload = json.loads(cell.rationale)
        assert payload["format"] == dp.RATIONALE_FORMAT
        assert not re.search(r"[A-Za-z]:\\|/Users/|/home/|/tmp/", cell.rationale)
        records, codes = dp.parse_rationale(cell.rationale)
        assert {r["manifest_sha256"] for r in records} == {manifest.digest_hex}
        assert "coverage_only" in codes and "outcomes_unascertained" in codes
    all_records, _ = dp.parse_rationale(one_month[("all", "all")].rationale)
    assert sorted(r["source_key"] for r in all_records) == list(
        sb.REQUIRED_FRONTIER_KEYS
    )
    nport, _ = dp.parse_rationale(one_month[("sec_nport", "default_state")].rationale)
    assert [r["source_key"] for r in nport] == ["sec_nport.dera_packages"]
    assert (
        dict(nport[0])
        == manifest.frontiers["sec_nport.dera_packages"].rationale_record()
    )
    assert "ingestion" not in nport[0] and "evidence" not in nport[0]
    agency, agency_codes = dp.parse_rationale(
        one_month[("agency_rocr", "agency_issue_default")].rationale
    )
    assert agency[0]["state"] == "unavailable" and agency[0]["frontier"] is None
    assert "rating_rights_unverified" in agency_codes
    edgar, _ = dp.parse_rationale(one_month[("sec_edgar", "payment_default")].rationale)
    assert [r["source_key"] for r in edgar] == [
        "sec_edgar.census",
        "sec_edgar.submissions",
    ]
    embedded = dp.frontier_records_of(bundle.frames["coverage"], knowledge_cutoff=K)
    assert [r["source_key"] for r in embedded] == [
        "agency_rocr.agency_history",
        "sec_edgar.census",
        "sec_edgar.submissions",
        "sec_ncen.dera_packages",
        "sec_ncen.form_index",
        "sec_nport.dera_packages",
    ]


def test_diagnostic_projection_is_coverage_only_and_matches_the_full_offline_derivation():
    counts = varied_counts()
    doc = manifest_for()
    bundle = compose(fake_panel(counts), doc)
    projection = sb.build_diagnostic_projection(bundle, doc)
    assert isinstance(projection, dp.DiagnosticProjection)
    # the diagnostic layer's own full offline check (iterates the rating rows) agrees exactly
    assert projection == dp.derive_projection_from_bundle(bundle)
    record = projection.to_json_obj()
    assert record["display_mode"] == "coverage_only" and record["accepted_events"] == []
    block = record["counts"]
    assert block["panel_grid_keys"] == sum(counts.values())
    assert block["candidate_issue_months"] == block["unknown_outcome_issue_months"]
    assert block["candidate_issue_months"] == sum(counts[m] for m in MONTHS[:-1])
    assert block["accepted_events"] == 0 and block["unresolved_events"] is None
    assert block["censored_issue_months"] is None
    assert len(record["coverage"]) == 360 and len(record["source_frontiers"]) == 6
    assert {
        "coverage_only",
        "no_adjudicated_events",
        "not_for_expected_loss",
        "not_for_recommendation",
        "inventory_not_ingested",
        "nport_q3_unavailable_at_observation",
        "rating_rights_unverified",
    } <= set(record["limitations"])
    dumped = json.dumps(record)
    assert (
        "sha256" not in dumped and "rationale" not in dumped and '"basis"' not in dumped
    )
    assert not re.search(r"[A-Za-z]:\\|/Users/|/home/", dumped)
    # frontier digest handed to prepare_diagnostic follows the embedded records, not the file hash
    digest = sb.source_frontier_manifest_digest(bundle)
    assert digest == dp.frontier_manifest_digest(
        dp.frontier_records_of(bundle.frames["coverage"], knowledge_cutoff=K)
    )
    assert digest != validate(doc).digest


def test_projection_refuses_a_manifest_that_differs_from_the_embedded_records():
    doc = manifest_for()
    bundle = compose(fake_panel(varied_counts()), doc)
    other = manifest_for(mutate=_set_frontier("sec_edgar.census", filing_count=1087))
    with pytest.raises(
        sb.SourceManifestError, match="coverage_manifest_records_mismatch"
    ):
        sb.build_diagnostic_projection(bundle, other)
    late = manifest_for(
        mutate=_set_frontier(
            "sec_edgar.census", observed_at="2026-10-01T00:00:00.000000Z"
        )
    )
    with pytest.raises(sb.SourceManifestError, match="observed_after_cutoff"):
        sb.build_diagnostic_projection(bundle, late)


def test_size_estimate_matches_the_real_canonical_bundle():
    bundle = compose(fake_panel(varied_counts(3)), manifest_for())
    cov = sb.coverage_canonical_bytes(bundle.frames["coverage"])
    estimate = sb.estimate_bundle_bytes(len(bundle.panel_grid), cov)
    actual = len(bundle.canonical_bytes())
    assert actual <= estimate <= actual * 1.15, (actual, estimate)


def test_bundle_bound_refusal_precedes_assembly_and_key_fetch(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("assembly/key fetch must not run after a bound breach")

    monkeypatch.setattr(sb.c, "assemble_bundle", boom)
    monkeypatch.setattr(sb, "_fetch_grid", boom)
    with pytest.raises(sb.BoundsExceeded, match="bundle_bytes_exceed_bound"):
        sb.enforce_bundle_bound(1_127_685, 1_000_000, 512 << 20)
    assert sb.enforce_bundle_bound(1_127_685, 1_000_000, 1 << 30) > 600_000_000


# ---------------------------------------------------------------------------
# Pure ancestry walk
# ---------------------------------------------------------------------------
def _pubs(*specs):
    """specs: (name, parent_name|None, config, status) in any order; returns fetcher + ids."""
    ids = {name: uuid.uuid4() for name, *_ in specs}
    rows = {
        ids[name]: {
            "publication_id": ids[name],
            "parent_publication_id": None if parent is None else ids[parent],
            "publication_status": status,
            "config_hash": config,
            "first_month": MONTHS[0],
            "last_closed_month": MONTHS[-1],
        }
        for name, parent, config, status in specs
    }
    return rows.get, ids


CUR, LEG = sb.PANEL_CURRENT_CONFIG, sb.PANEL_LEGACY_CONFIG


def test_walk_ancestry_head_to_root_with_compatible_configs():
    fetch, ids = _pubs(
        ("a", "b", CUR, "validated"),
        ("b", "r", CUR, "validated"),
        ("r", None, LEG, "validated"),
    )
    chain = sb.walk_ancestry(fetch, ids["a"])
    assert [p.publication_id for p in chain] == [ids["a"], ids["b"], ids["r"]]
    fetch, ids = _pubs(("a", "r", LEG, "validated"), ("r", None, LEG, "validated"))
    assert len(sb.walk_ancestry(fetch, ids["a"])) == 2


@pytest.mark.parametrize(
    ("specs", "code"),
    [
        (
            (("a", "b", CUR, "validated"), ("b", "a", CUR, "validated")),
            "panel_ancestry_cycle",
        ),
        ((("a", "a", CUR, "validated"),), "panel_ancestry_cycle"),
        (
            (("a", "b", CUR, "validated"), ("b", None, CUR, "prepared")),
            "panel_ancestry_unvalidated",
        ),
        (
            (("a", "b", LEG, "validated"), ("b", None, CUR, "validated")),
            "panel_ancestry_config_incompatible",
        ),
        (
            (
                ("a", "b", CUR, "validated"),
                ("b", None, "180a82b3f1413d43", "validated"),
            ),
            "panel_ancestry_config_incompatible",
        ),
        ((("a", None, "180a82b3f1413d43", "validated"),), "panel_config_unsupported"),
        ((("a", None, CUR, "failed"),), "panel_ancestry_unvalidated"),
    ],
)
def test_walk_ancestry_rejects_broken_cyclic_or_incompatible_chains(specs, code):
    fetch, ids = _pubs(*specs)
    with pytest.raises(sb.PanelReadError, match=code):
        sb.walk_ancestry(fetch, ids["a"])


def test_walk_ancestry_rejects_a_missing_parent_and_an_over_deep_chain():
    fetch, ids = _pubs(("a", "b", CUR, "validated"), ("b", None, CUR, "validated"))
    rows = {pid: fetch(pid) for pid in ids.values()}
    rows[ids["a"]] = {**rows[ids["a"]], "parent_publication_id": uuid.uuid4()}
    with pytest.raises(sb.PanelReadError, match="panel_ancestry_broken"):
        sb.walk_ancestry(rows.get, ids["a"])
    chain_ids = [uuid.uuid4() for _ in range(sb.MAX_ANCESTRY_DEPTH + 2)]
    deep = {
        pid: {
            "publication_id": pid,
            "parent_publication_id": chain_ids[i + 1]
            if i + 1 < len(chain_ids)
            else None,
            "publication_status": "validated",
            "config_hash": CUR,
            "first_month": MONTHS[0],
            "last_closed_month": MONTHS[-1],
        }
        for i, pid in enumerate(chain_ids)
    }
    with pytest.raises(sb.PanelReadError, match="panel_ancestry_too_deep"):
        sb.walk_ancestry(deep.get, chain_ids[0])


def test_month_arithmetic_and_grid_window():
    assert sb.add_months(dt.date(2026, 1, 1), -1) == dt.date(2025, 12, 1)
    assert MONTHS[0] == dt.date(2021, 8, 1) and MONTHS[-1] == T and len(MONTHS) == 61


# ---------------------------------------------------------------------------
# No file / pickle / C1 access in the builder
# ---------------------------------------------------------------------------
def test_source_module_has_no_file_socket_or_pickle_access():
    tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and not node.level
    }
    assert not imported & {
        "pickle",
        "shelve",
        "marshal",
        "socket",
        "urllib",
        "http",
        "requests",
        "httpx",
        "subprocess",
        "os",
        "io",
    }
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "open" not in calls and "eval" not in calls and "exec" not in calls
    text = SOURCE_PATH.read_text(encoding="utf-8")
    assert "read_text" not in text and "np.load" not in text
    # the only file read is the producer-tree digest (code_digest), never a data file
    readers = [
        fn.name
        for fn in ast.walk(tree)
        if isinstance(fn, ast.FunctionDef)
        and any(
            isinstance(n, ast.Attribute) and n.attr == "read_bytes"
            for n in ast.walk(fn)
        )
    ]
    assert readers == ["code_digest"]


def test_compose_validate_and_project_touch_no_pickle_socket_or_data_file(monkeypatch):
    import socket

    doc = manifest_for()  # the manifest file is read before the guards are armed
    real_open = builtins.open
    real_read_bytes = Path.read_bytes
    opened: list[str] = []

    def guarded_open(file, *args, **kwargs):
        opened.append(str(file))
        if not str(file).endswith(".sql"):
            raise AssertionError(f"unexpected file open: {file}")
        return real_open(file, *args, **kwargs)

    def guarded_read_bytes(self):
        opened.append(str(self))
        if not str(self).endswith(".sql"):
            raise AssertionError(f"unexpected file read: {self}")
        return real_read_bytes(self)

    def forbidden(*args, **kwargs):
        raise AssertionError("forbidden call")

    monkeypatch.setattr(builtins, "open", guarded_open)
    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    monkeypatch.setattr(pickle, "load", forbidden)
    monkeypatch.setattr(pickle, "loads", forbidden)
    monkeypatch.setattr(pickle, "Unpickler", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    bundle = compose(fake_panel(varied_counts()), doc)
    sb.build_diagnostic_projection(bundle, doc)
    assert all(name.endswith(".sql") for name in opened)


# ---------------------------------------------------------------------------
# Argument gates of the public entry point
# ---------------------------------------------------------------------------
def test_entry_point_argument_gates():
    kwargs = {
        "conn": None,
        "target_month": T,
        "knowledge_cutoff": K,
        "expected_panel_publication_id": PROD_PANEL_ID,
        "code_digest": CODE_DIGEST,
    }
    doc = manifest_for()
    with pytest.raises(sb.SourceBundleError, match="knowledge_mode_unsupported"):
        sb.build_bundle_from_sources(
            doc, knowledge_mode="historical_reconstruction", **kwargs
        )
    with pytest.raises(sb.SourceBundleError, match="target_month_invalid"):
        sb.build_bundle_from_sources(
            doc, **{**kwargs, "target_month": dt.date(2026, 8, 2)}
        )
    with pytest.raises(sb.SourceBundleError, match="code_digest_invalid"):
        sb.build_bundle_from_sources(doc, **{**kwargs, "code_digest": "abc"})
    with pytest.raises(
        sb.SourceBundleError, match="knowledge_cutoff_timezone_required"
    ):
        sb.build_bundle_from_sources(
            doc,
            **{
                **kwargs,
                "knowledge_cutoff": dt.datetime.fromisoformat("2026-09-29T00:00:00"),
            },
        )
    late = manifest_for(
        mutate=_set_frontier(
            "sec_edgar.census", observed_at="2026-10-01T00:00:00.000000Z"
        )
    )
    with pytest.raises(sb.SourceManifestError, match="observed_after_cutoff"):
        sb.build_bundle_from_sources(
            late, **kwargs
        )  # conn=None: refused before any database use


# ---------------------------------------------------------------------------
# Disposable PostgreSQL: pinned panel read, ancestry, bounds, persistence
# ---------------------------------------------------------------------------
def _load_db_helpers():
    spec = importlib.util.spec_from_file_location(
        "bond_default_db_helpers",
        Path(__file__).resolve().parent / "test_bond_default_publication_db.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = (
        module  # dataclasses resolve string annotations through sys.modules
    )
    spec.loader.exec_module(module)
    return module


def panel_ddl() -> list[str]:
    """Production ``CREATE TABLE`` text of the three panel tables the builder reads."""
    text = PANEL_SQL.read_text(encoding="utf-8")
    statements = []
    for name in (
        "bond_panel_publications",
        "bond_panel_app_pointer",
        "bond_panel_snapshot",
    ):
        match = re.search(
            rf"CREATE TABLE IF NOT EXISTS {name} \(\n.*?\n\);\n", text, re.DOTALL
        )
        assert match, name
        statements.append(match.group(0))
    return statements


PANEL_TABLES = (
    "bond_panel_app_pointer",
    "bond_panel_snapshot",
    "bond_panel_publications",
)


def install_panel_tables(conn) -> None:
    for name in PANEL_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS public.{name} CASCADE")
    for statement in panel_ddl():
        conn.execute(statement)


def drop_panel_tables(conn) -> None:
    for name in PANEL_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS public.{name} CASCADE")


def insert_publication(
    conn,
    pid,
    parent=None,
    *,
    config=CUR,
    status="validated",
    last_closed=T,
    open_month=None,
    fingerprint=None,
) -> None:
    if parent is not None and open_month is None:
        open_month = sb.add_months(last_closed, 1)
    conn.execute(
        "INSERT INTO public.bond_panel_publications (publication_id, parent_publication_id, publication_status, "
        "config_hash, input_fingerprint, code_revision, first_month, last_closed_month, open_month, snapshot_rows, "
        "rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence, validated_at) "
        "VALUES (%s,%s,%s,%s,%s,'test',%s,%s,%s,1,1,1,1,'{}','{}', "
        "CASE WHEN %s::text = 'validated' THEN now() END)",
        [
            pid,
            parent,
            status,
            config,
            fingerprint or uuid.uuid4().hex + uuid.uuid4().hex,
            MONTHS[0],
            last_closed,
            open_month,
            status,
        ],
    )


def insert_snapshot(conn, pid, rows: Iterable[tuple[dt.date, str, str]]) -> None:
    with (
        conn.cursor() as cur,
        cur.copy(
            "COPY public.bond_panel_snapshot (publication_id, month, cusip_id, eligibility_state, eligibility_reason, payload) "
            "FROM STDIN"
        ) as copy,
    ):
        for month, cusip, state in rows:
            reason = "eligible" if state == "included" else "test_excluded"
            copy.write_row((pid, month, cusip, state, reason, "{}"))


def set_pointer(conn, pid) -> None:
    conn.execute("DELETE FROM public.bond_panel_app_pointer")
    conn.execute(
        "INSERT INTO public.bond_panel_app_pointer (product, publication_id) VALUES ('bond_panel_v1', %s)",
        [pid],
    )


@pytest.fixture(scope="module")
def db_target():
    raw = os.environ.get(DB_ENV)
    if raw is None:
        pytest.skip(
            f"{DB_ENV} not set; the disposable DB suite refuses any ambient DSN"
        )
    pytest.importorskip("psycopg")
    helpers = _load_db_helpers()
    try:
        target = helpers.check_test_dsn(raw)
    except helpers.UnsafeTestDsn as exc:
        pytest.fail(f"refusing {DB_ENV}: {exc}")
    return helpers, target


@pytest.fixture
def conn(db_target):
    helpers, target = db_target
    with helpers.connect_disposable(target, autocommit=True) as admin:
        install_panel_tables(admin)
    connection = helpers.connect_disposable(
        target
    )  # non-autocommit, like the worker's connection
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()
        with helpers.connect_disposable(target, autocommit=True) as admin:
            drop_panel_tables(admin)


def commit(connection) -> None:
    connection.commit()


def seed_flat_panel(
    connection, counts: Mapping[dt.date, int], *, pid=None, excluded_every: int = 0
):
    """One root publication carrying ``counts`` keys per month, pointer on it."""
    pid = pid or uuid.uuid4()
    insert_publication(connection, pid, None, config=LEG)
    insert_snapshot(
        connection,
        pid,
        (
            (
                month,
                make_cusip(i),
                "excluded"
                if excluded_every and i % excluded_every == 0
                else "included",
            )
            for month, n in counts.items()
            for i in range(n)
        ),
    )
    set_pointer(connection, pid)
    commit(connection)
    return pid


def read(connection, pid, **overrides):
    args = {
        "expected_panel_publication_id": pid,
        "target_month": T,
        "max_grid_rows": 10_000_000,
    }
    args.update(overrides)
    return sb.read_panel(connection, **args)


def test_read_panel_returns_the_pinned_grid_and_restores_the_connection(conn):
    counts = varied_counts()
    pid = seed_flat_panel(conn, counts, excluded_every=3)
    before = (conn.isolation_level, conn.read_only, conn.autocommit)
    panel = read(conn, pid)
    assert (conn.isolation_level, conn.read_only, conn.autocommit) == before
    assert conn.info.transaction_status.name == "IDLE"
    assert dict(panel.counts.month_counts) == counts
    assert panel.counts.grid_count == len(panel.grid) == sum(counts.values())
    assert panel.grid == tuple(
        sorted(panel.grid, key=lambda item: f"{item[0]}|{item[1].isoformat()}")
    )
    assert panel.grid_digest == c.grid_digest(panel.grid)
    # no eligibility filter: excluded keys are part of the grid and counted as such
    states = dict(panel.counts.eligibility_counts)
    assert (
        states["excluded"] > 0
        and states["included"] > 0
        and sum(states.values()) == len(panel.grid)
    )
    assert panel.counts.winning_depth_counts == (len(panel.grid),)


def test_read_panel_is_read_only_repeatable_read(conn):
    seed_flat_panel(conn, varied_counts())
    with sb._begin_read_only(conn):
        assert conn.execute("SHOW transaction_read_only").fetchone()[0] == "on"
        assert (
            conn.execute("SHOW transaction_isolation").fetchone()[0]
            == "repeatable read"
        )
        import psycopg

        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("DELETE FROM public.bond_panel_snapshot")
    assert (
        conn.execute("SELECT count(*) FROM public.bond_panel_snapshot").fetchone()[0]
        > 0
    )
    conn.rollback()


def test_read_panel_refuses_a_connection_in_a_transaction(conn):
    pid = seed_flat_panel(conn, varied_counts())
    conn.execute("SELECT 1")
    with pytest.raises(sb.PanelReadError, match="panel_connection_not_idle"):
        read(conn, pid)
    conn.rollback()


def test_overlay_ancestry_nearest_depth_wins_and_untouched_parent_months_remain_visible(
    conn,
):
    root, mid, head = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    insert_publication(conn, root, None, config=LEG, last_closed=MONTHS[-3])
    insert_publication(conn, mid, root, config=CUR, last_closed=MONTHS[-2])
    insert_publication(conn, head, mid, config=CUR, last_closed=T)
    old_months, recent = MONTHS[:-3], MONTHS[-3:]
    insert_snapshot(
        conn,
        root,
        ((m, make_cusip(i), "included") for m in old_months for i in range(4)),
    )
    # the mid overlay rebuilds the last two closed months for keys 0..2 and adds a new key 7 (excluded)
    insert_snapshot(
        conn,
        mid,
        ((m, make_cusip(i), "excluded") for m in recent[:2] for i in (0, 1, 2, 7)),
    )
    # the head overlay rebuilds the target month for keys 0..1 and re-covers key 0 of an older overlay month
    insert_snapshot(
        conn,
        head,
        [
            (T, make_cusip(0), "included"),
            (T, make_cusip(1), "excluded"),
            (recent[1], make_cusip(0), "included"),
        ],
    )
    # a root row of a month the mid overlay also rebuilt: nearest (mid) must win, not the root
    insert_snapshot(
        conn,
        root,
        [
            (recent[0], make_cusip(0), "included"),
            (recent[0], make_cusip(5), "included"),
        ],
    )
    set_pointer(conn, head)
    commit(conn)
    panel = read(conn, head)
    keys = set(panel.grid)
    assert (make_cusip(7), recent[0]) in keys and (make_cusip(5), recent[0]) in keys
    assert (
        make_cusip(3),
        old_months[0],
    ) in keys  # untouched parent month stays visible
    assert len(panel.counts.chain) == 3
    # winners per depth: head decides 3 keys, mid decides the remaining mid keys, root the rest
    head_keys = 3
    mid_keys = (
        8 - 1 - 0
    )  # 4 keys x 2 months, minus the (recent[1], key 0) re-covered by head
    assert panel.counts.winning_depth_counts[0] == head_keys
    assert panel.counts.winning_depth_counts[1] == mid_keys
    assert (
        panel.counts.winning_depth_counts[2]
        == sum(panel.counts.winning_depth_counts) - head_keys - mid_keys
    )
    # (recent[1], key 0) is 'excluded' in mid but 'included' at the head: nearest depth => included;
    # winners: root 233 included; mid 7 excluded; head 2 included + 1 excluded (T, key 1)
    assert dict(panel.counts.eligibility_counts) == {"included": 235, "excluded": 8}
    assert panel.counts.grid_count == len(panel.grid) == 243


def test_excluded_keys_are_retained_in_the_grid_and_the_bundle(conn):
    counts = {month: 4 for month in MONTHS}
    pid = seed_flat_panel(conn, counts, excluded_every=2)
    panel = read(conn, pid)
    assert len(panel.grid) == 4 * 61
    bundle = compose(panel, manifest_for(pid))
    excluded = {(make_cusip(i), m) for m in MONTHS for i in range(4) if i % 2 == 0}
    assert excluded <= set(bundle.panel_grid)
    assert {(r.cusip_id, r.month) for r in bundle.frames["ratings"]} >= excluded


def test_pointer_and_publication_mismatches_stop_the_run(conn):
    counts = varied_counts()
    pid = seed_flat_panel(conn, counts)
    with pytest.raises(sb.PanelReadError, match="panel_pointer_mismatch"):
        read(conn, uuid.uuid4())
    conn.execute("DELETE FROM public.bond_panel_app_pointer")
    commit(conn)
    with pytest.raises(sb.PanelReadError, match="panel_pointer_mismatch"):
        read(conn, pid)
    conn.execute(
        "INSERT INTO public.bond_panel_app_pointer (product, publication_id) VALUES ('bond_panel_v1', %s)",
        [pid],
    )
    conn.execute(
        "UPDATE public.bond_panel_publications SET publication_status='prepared', validated_at=NULL WHERE publication_id=%s",
        [pid],
    )
    commit(conn)
    with pytest.raises(sb.PanelReadError, match="panel_ancestry_unvalidated"):
        read(conn, pid)
    assert conn.info.transaction_status.name == "IDLE"


def test_missing_month_and_unclosed_target_month_stop_the_run(conn):
    counts = varied_counts()
    del counts[MONTHS[10]]
    pid = seed_flat_panel(conn, counts)
    with pytest.raises(sb.PanelReadError, match="panel_month_missing"):
        read(conn, pid)
    assert conn.info.transaction_status.name == "IDLE"


def test_unclosed_target_month_is_refused(conn):
    pid = uuid.uuid4()
    insert_publication(conn, pid, None, config=LEG, last_closed=MONTHS[-2])
    insert_snapshot(
        conn, pid, ((m, make_cusip(i), "included") for m in MONTHS for i in range(2))
    )
    set_pointer(conn, pid)
    commit(conn)
    with pytest.raises(sb.PanelReadError, match="panel_target_month_not_closed"):
        read(conn, pid)


def test_cyclic_and_broken_db_ancestry_is_rejected(conn):
    a, b = uuid.uuid4(), uuid.uuid4()
    insert_publication(conn, a, None, config=CUR)
    insert_publication(conn, b, a, config=CUR)
    conn.execute(
        "UPDATE public.bond_panel_publications SET parent_publication_id=%s, open_month=%s WHERE publication_id=%s",
        [b, sb.add_months(T, 1), a],
    )
    insert_snapshot(conn, a, ((m, make_cusip(0), "included") for m in MONTHS))
    set_pointer(conn, b)
    commit(conn)
    with pytest.raises(sb.PanelReadError, match="panel_ancestry_cycle"):
        read(conn, b)
    conn.execute(
        "UPDATE public.bond_panel_publications SET parent_publication_id=NULL, open_month=NULL WHERE publication_id=%s",
        [a],
    )
    conn.execute(
        "UPDATE public.bond_panel_publications SET publication_status='prepared', validated_at=NULL WHERE publication_id=%s",
        [a],
    )
    commit(conn)
    with pytest.raises(sb.PanelReadError, match="panel_ancestry_unvalidated"):
        read(conn, b)


def test_grid_bound_refusal_precedes_key_fetch_and_assembly(conn, monkeypatch):
    counts = varied_counts()
    pid = seed_flat_panel(conn, counts)
    g = sum(counts.values())

    def boom(*args, **kwargs):
        raise AssertionError("must not fetch keys or assemble after a bound breach")

    monkeypatch.setattr(sb, "_fetch_grid", boom)
    monkeypatch.setattr(sb.c, "assemble_bundle", boom)
    with pytest.raises(sb.BoundsExceeded, match="grid_rows_exceed_bound"):
        read(conn, pid, max_grid_rows=g - 1)
    doc = manifest_for(pid, max_grid_rows=g - 1)
    with pytest.raises(sb.BoundsExceeded, match="grid_rows_exceed_bound"):
        sb.build_bundle_from_sources(
            doc,
            conn=conn,
            target_month=T,
            knowledge_cutoff=K,
            expected_panel_publication_id=pid,
            code_digest=CODE_DIGEST,
        )
    with pytest.raises(sb.BoundsExceeded, match="bundle_bytes_exceed_bound"):
        sb.build_bundle_from_sources(
            manifest_for(pid, max_bundle_bytes=1000),
            conn=conn,
            target_month=T,
            knowledge_cutoff=K,
            expected_panel_publication_id=pid,
            code_digest=CODE_DIGEST,
        )
    assert conn.info.transaction_status.name == "IDLE"


def test_invalid_cusip_in_the_panel_stops_the_run(conn):
    pid = uuid.uuid4()
    insert_publication(conn, pid, None, config=LEG)
    insert_snapshot(
        conn,
        pid,
        (
            (m, cusip, "included")
            for m in MONTHS
            for cusip in (make_cusip(1), "12345678X")
        ),
    )
    set_pointer(conn, pid)
    commit(conn)
    with pytest.raises(sb.PanelReadError, match="panel_invalid_cusip9"):
        read(conn, pid)


def test_build_from_sources_end_to_end_is_deterministic_and_stable_under_replay(conn):
    counts = varied_counts()
    pid = seed_flat_panel(conn, counts, excluded_every=4)
    doc = manifest_for(pid)
    kwargs = {
        "conn": conn,
        "target_month": T,
        "knowledge_cutoff": K,
        "expected_panel_publication_id": pid,
        "code_digest": CODE_DIGEST,
    }
    first = sb.build_bundle_from_sources(doc, **kwargs)
    second = sb.build_bundle_from_sources(copy.deepcopy(doc), **kwargs)
    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.publication_id == second.publication_id
    assert len(first.frames["ratings"]) == 2 * sum(counts.values())
    pub.check_bundle(first)
    projection = sb.build_diagnostic_projection(first, doc)
    assert projection.counts.panel_grid_keys == sum(counts.values())


def test_persisted_build_validates_structurally_and_elects_no_pointer(conn, db_target):
    helpers, target = db_target
    schema = "bdsb_" + uuid.uuid4().hex[:10]
    counts = {month: 3 + (i % 3) for i, month in enumerate(MONTHS)}
    pid = seed_flat_panel(conn, counts, excluded_every=2)
    doc = manifest_for(pid)
    with helpers.connect_disposable(target, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
        pub.install_schema(admin, schema)
    try:
        bundle = sb.build_bundle_from_sources(
            doc,
            conn=conn,
            target_month=T,
            knowledge_cutoff=K,
            expected_panel_publication_id=pid,
            code_digest=CODE_DIGEST,
        )
        store = pub.PostgresPublicationStore(conn, schema)
        assert pub.prepare_bundle(store, bundle) == "inserted"
        assert pub.prepare_bundle(store, bundle) == "replayed"
        state = pub.validate_bundle(store, bundle.publication_id)
        assert (state.lifecycle_state, state.quality_state, state.build_scope) == (
            "validated",
            "partial",
            "limited",
        )
        pointer = conn.execute(
            f"SELECT count(*) FROM {schema}.bond_credit_current_pointer"
        ).fetchone()[0]
        assert pointer == 0
        conn.commit()
        # a partial/limited build can never be promoted through the qualified path
        with pytest.raises(pub.PublicationError):
            pub.promote_bundle(store, bundle.publication_id, expected_pointer=None)
        assert (
            conn.execute(
                f"SELECT count(*) FROM {schema}.bond_credit_current_pointer"
            ).fetchone()[0]
            == 0
        )
        conn.rollback()
    finally:
        conn.rollback()
        with helpers.connect_disposable(target, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")


# ---------------------------------------------------------------------------
# code_digest: deterministic producer-tree pin
# ---------------------------------------------------------------------------
def _copy_code_tree(dest: Path) -> None:
    import shutil

    for rel in sb.code_digest_files():
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(c.ROOT / rel, target)


def test_code_digest_file_set_is_exact_sorted_and_excludes_pycache():
    files = sb.code_digest_files()
    assert list(files) == sorted(files) and len(set(files)) == len(files)
    assert all("__pycache__" not in f and not f.endswith(".pyc") for f in files)
    assert {
        "src/workers/bond_default_events.py",
        "src/bonds/default_events/source_bundle.py",
        "src/bonds/default_events/contracts.py",
        "src/bonds/default_events/diagnostic_publication.py",
        "schemas/bond_default_diagnostic_release_v1.sql",
        "contracts/bonds/default_events_frontier_manifest_2026-08.json",
        *(f"schemas/{name}" for name in c.SQL_FILES),
    } <= set(files)
    assert sum(1 for f in files if f.startswith("schemas/")) == 5
    assert all(
        f.startswith(("src/bonds/default_events/", "schemas/", "contracts/bonds/"))
        or f == "src/workers/bond_default_events.py"
        for f in files
    )
    assert not any(f.startswith("tests/") for f in files)


def test_code_digest_is_deterministic_lf_normalized_and_content_sensitive(tmp_path):
    real = sb.code_digest()
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", real) and real == sb.code_digest()
    _copy_code_tree(tmp_path)
    assert sb.code_digest(tmp_path) == real  # same content, other root, same digest
    module = tmp_path / "src" / "bonds" / "default_events" / "source_bundle.py"
    original = module.read_bytes()
    module.write_bytes(original.replace(b"\n", b"\r\n"))  # a CRLF checkout
    assert sb.code_digest(tmp_path) == real
    (tmp_path / "src" / "bonds" / "default_events" / "__pycache__").mkdir()
    (tmp_path / "src" / "bonds" / "default_events" / "__pycache__" / "x.py").write_text(
        "x"
    )
    (tmp_path / "contracts" / "bonds" / "__pycache__").mkdir()
    (tmp_path / "contracts" / "bonds" / "__pycache__" / "y.json").write_text("{}")
    (tmp_path / "contracts" / "bonds" / "stale.pyc").write_bytes(b"\x00")
    assert sb.code_digest(tmp_path) == real  # caches are excluded
    module.write_bytes(original + b"# drift\n")
    assert sb.code_digest(tmp_path) != real
    module.write_bytes(original)
    (tmp_path / "src" / "bonds" / "default_events" / "new_module.py").write_text(
        "x = 1\n"
    )
    assert sb.code_digest(tmp_path) != real  # a new module changes the pin
    (tmp_path / "src" / "bonds" / "default_events" / "new_module.py").unlink()
    manifest = tmp_path / "contracts" / "bonds" / MANIFEST_PATH.name
    manifest.write_bytes(manifest.read_bytes() + b" ")
    assert (
        sb.code_digest(tmp_path) != real
    )  # the frontier manifest is part of the pinned tree
    manifest.write_bytes(manifest.read_bytes().rstrip(b" "))
    assert sb.code_digest(tmp_path) == real


def test_code_digest_fails_loudly_on_a_missing_file_or_tree(tmp_path):
    _copy_code_tree(tmp_path)
    (tmp_path / "schemas" / c.SQL_FILES[0]).unlink()
    with pytest.raises(sb.SourceBundleError, match="code_digest_file_unreadable"):
        sb.code_digest(tmp_path)
    with pytest.raises(sb.SourceBundleError):
        sb.code_digest(tmp_path / "nowhere")


def test_the_pinned_code_digest_value_is_printed_for_the_record(capsys):
    # Not an assertion of a constant (the digest moves with every reviewed change of the tree).
    value = sb.code_digest()
    with capsys.disabled():
        print(f"\ncode_digest(current tree) = {value}")
    assert value.startswith("sha256:")
