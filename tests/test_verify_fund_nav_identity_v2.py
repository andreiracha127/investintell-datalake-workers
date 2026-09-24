"""Independent A1–A8 audit: non-circularity, mutation detection and gates."""

from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
import os
import stat
import uuid
from pathlib import Path

import pytest

from scripts import fund_nav_readiness_schema as operator
from scripts import generate_fund_nav_policy_v1 as generator
from scripts import verify_fund_nav_identity_v2 as verifier
from src.workers._nav_policy import (
    CATALOG_EVIDENCE_REFERENCE,
    FUNDS_QUERY,
    IDENTITY_FAILURE_CODES,
    IDENTITY_QUERY,
    INSTRUMENTS_QUERY,
    SOURCE_QUERY_SHA256,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
    uuid_set_digest,
)
from tests._nav_identity_fixtures import (
    catalog,
    entity,
    only,
    synthetic_figi,
    synthetic_isin,
    wrong_check,
)

START = dt.date(2024, 1, 1)
END = dt.date(2027, 12, 31)
OBSERVED = dt.datetime(2026, 9, 24, 12, 0, tzinfo=dt.timezone.utc)
CAPTURED = "2026-09-24T12:05:00+00:00"
LABELS = {1: "Large Blend", 2: "Government Bond", 3: "Technology", 4: "Real Estate"}
SLEEVES = {
    "Large Blend": "equity",
    "Government Bond": "fixed_income",
    "Technology": "thematic",
    "Real Estate": "alternatives",
}


@pytest.fixture(scope="module")
def calendar():
    return generator.build_calendar(START, END)


def _rich_entities():
    """ACTIVE across ISIN strata/types plus every lifecycle category."""
    return [
        entity(1),  # both
        entity(2, reg_isin=None, cusip=None),  # IU-only
        entity(3, iu_isin=None),  # registry-only
        entity(4, iu_isin=None, reg_isin=None, cusip=None),  # neither
        entity(5, fund_type="mutual_fund"),
        entity(6, fund_type="mutual_fund", reg_isin=None, cusip=None),
        entity(7, fund_type="mutual_fund", iu_isin=None, figi=synthetic_figi(7)),
        entity(8, fund_type="mutual_fund", iu_isin=None, reg_isin=None, cusip=None),
        entity(9, fund_type="mmf"),
        entity(10, iu_isin=wrong_check(synthetic_isin(10))),  # isin.checksum
        entity(11, ticker="DUP"),
        entity(12, ticker="DUP"),  # ticker.global_conflict ×2
        entity(13, status="candidate"),
        entity(14, active=None),
        entity(15, active=False),
        only(entity(16, active=False), fund=False, registry=False),  # INACTIVE
        only(entity(17), registry=False),
    ]


def _build(calendar, entities=None, *, extra=None, observed=OBSERVED):
    rows = catalog(*(entities or _rich_entities()), **(extra or {}))
    policy = generator.build_policy(
        calendar, *rows, observed, "current-daily-nav-xnys-usd-adjusted", "2026-09-24.2"
    )
    snapshot = generator.build_source_snapshot(policy, *rows)
    return generator.canonical_json(policy), generator.canonical_json(snapshot), rows


def _gates(report):
    return {name: gate["status"] for name, gate in report["gates"].items()}


def _failed_checks(report, gate):
    return sorted(
        k for k, ok in report["gates"][gate].get("checks", {}).items() if not ok
    )


def _config(**overrides):
    config = {
        "audit_config_version": verifier.AUDIT_CONFIG_VERSION,
        "audit_contract_version": verifier.AUDIT_CONTRACT_VERSION,
        "active_ceiling": 5103,
        "structural_baseline": None,
        "canary_salt": "nav-policy-v2-canary-2026-09-24",
        "sec": {"max_synced_age_days": 7},
        "builder": {
            "light_revision": "a" * 40,
            "cohort_query": "SELECT instrument_id, strategy_label FROM cohort",
            "cohort_parameters": {},
            "label_to_sleeve": SLEEVES,
            "stage1_quotas": {"equity": 1, "fixed_income": 1},
            "margin_fraction": "1/10",
        },
    }
    config.update(overrides)
    return json.dumps(config).encode()


def _live(rows, *, cohort=None, sec=None):
    """A fake READ ONLY capture with database-shaped values (uuid.UUID)."""
    instruments, funds, identity = rows
    if cohort is None:
        cohort = [
            {"instrument_id": uuid.UUID(int=n), "strategy_label": LABELS[1 + (n % 2)]}
            for n in range(1, 18)
        ] + [{"instrument_id": uuid.UUID(int=99), "strategy_label": "Unclassified"}]
    if sec is None:
        sec = [
            {"class_id": f"C{n:09d}", "series_id": f"S{n:09d}", "ticker": f"T{n}"}
            for n in range(1, 9)
        ]
    return {
        "captured_at": CAPTURED,
        "sources": {"instruments": instruments, "funds": funds, "identity": identity},
        "cohort": {"state": "captured", "rows": cohort},
        "sec": {
            "state": "captured",
            "rows": sec,
            "lineage": {
                "row_count": len(sec),
                "max_synced_at": "2026-09-23T00:00:00+00:00",
                "max_source_period_end": "2026-06-30",
            },
        },
    }


def _strict_report(calendar, entities=None, *, live_kwargs=None, config=None):
    policy_raw, snapshot_raw, rows = _build(calendar, entities)
    previous = _previous(active_ids=(1,), inactive_ids=(16,), calendar=calendar)
    config_raw = config or _config()
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, **(live_kwargs or {})),
        previous_raw=previous,
        config_raw=config_raw,
    )
    return report, policy_raw, snapshot_raw, rows, previous, config_raw


def _canary_of(report, policy_raw, config_raw):
    digest = hashlib.sha256(verifier.report_bytes(report)).hexdigest()
    return verifier.build_canary(
        report, policy_raw, json.loads(config_raw), audit_report_sha256=digest
    )


def _previous(active_ids=(), inactive_ids=(), unknown_ids=(), calendar=None):
    """A v1-shaped artifact read as data only (never re-verified by v2 code)."""
    rows = [
        {"instrument_id": str(uuid.UUID(int=n)), "fund_status": status}
        for status, ids in (
            ("ACTIVE", active_ids),
            ("INACTIVE", inactive_ids),
            ("UNKNOWN", unknown_ids),
        )
        for n in ids
    ]
    document = {
        "generator_version": "fund-nav-policy-generator-v1",
        "instrument_evidence": rows,
        "generation": {"policy_hash": "1" * 64},
    }
    if calendar is not None:
        for field in (
            "calendar_id",
            "calendar_version",
            "calendar_source",
            "calendar_digest",
            "calendar_session_count",
            "coverage_start",
            "coverage_end",
            "sessions",
        ):
            document[field] = calendar[field]
    return json.dumps(document, sort_keys=True).encode()


# ── non-circularity ─────────────────────────────────────────────────────────
def test_verifier_imports_only_the_custody_writer_from_generator_code():
    tree = ast.parse(Path(verifier.__file__).read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [(alias.name, None) for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            imported += [(node.module, alias.name) for alias in node.names]
    modules = {module for module, _ in imported}
    assert "src.workers._nav_policy" not in modules
    assert not any(module and module.startswith("src.") for module in modules)
    assert "scripts.fund_nav_readiness_schema" not in modules
    from_generator = {
        name
        for module, name in imported
        if module == "scripts.generate_fund_nav_policy_v1"
    }
    assert from_generator == {"write_artifact", "PolicyGenerationError"}
    assert ("scripts.generate_fund_nav_policy_v1", None) not in imported
    identifiers = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    forbidden = {
        "classify_catalog",
        "_first_failure",
        "_CatalogIndex",
        "isin_problem",
        "cusip_problem",
        "figi_problem",
        "canonical_source_rows",
        "validate_generation_v2",
        "uuid_set_digest",
        "generator",
    }
    assert not identifiers & forbidden


def test_contract_texts_match_generator_without_import_coupling():
    assert verifier.GATE_CODES == IDENTITY_FAILURE_CODES
    assert verifier.QUERY_SHA256 == SOURCE_QUERY_SHA256
    assert (
        verifier.SOURCE_SQL["instruments"],
        verifier.SOURCE_SQL["funds"],
        verifier.SOURCE_SQL["identity"],
    ) == (
        INSTRUMENTS_QUERY,
        FUNDS_QUERY,
        IDENTITY_QUERY,
    )
    assert verifier.EXPECTED_REFERENCE == CATALOG_EVIDENCE_REFERENCE
    assert verifier.EXPECTED_GENERATOR == generator.GENERATOR_VERSION


# ── offline core ────────────────────────────────────────────────────────────
def test_rich_fixture_passes_offline_core_and_strict_requires_every_gate(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    report = verifier.audit(policy_raw, snapshot_raw=snapshot_raw)
    assert _gates(report) == {
        "A1": "PASS",
        "A2": "PASS",
        "A3": "NOT_EVALUATED",
        "A4": "PASS",
        "A5": "PASS",
        "A6": "PASS",
        "A7": "NOT_EVALUATED",
        "A8": "NOT_EVALUATED",
    }
    assert verifier.verdict_of(report, strict=False) is True
    assert verifier.verdict_of(report, strict=True) is False
    counts = report["counts"]
    assert counts["fund_status"] == {"ACTIVE": 9, "INACTIVE": 1, "UNKNOWN": 7}
    assert counts["identity_first_failure"] == {
        "activity.not_active": 1,
        "activity.unknown": 1,
        "cardinality.registry_missing": 1,
        "isin.checksum": 1,
        "registry.status_not_canonical": 1,
        "ticker.global_conflict": 2,
    }
    assert counts["isin_presence_active"] == {
        "both": 3,
        "iu_only": 2,
        "neither": 2,
        "registry_only": 2,
    }
    assert report["details"]["A4"]["structural_pre_claims"] == 10


@pytest.mark.parametrize(
    "patch",
    [
        "isin_checksum_ignored",
        "global_ownership_ignored",
        "duplicate_iu_inactive",
        "activity_ignored",
        "projection_check_skipped",
        "registry_status_ignored",
    ],
)
def test_deliberate_classifier_bugs_produce_self_consistent_artifacts_the_verifier_rejects(
    calendar, monkeypatch, patch
):
    entities = _rich_entities()
    extra = {}
    if patch == "isin_checksum_ignored":
        monkeypatch.setattr(generator, "isin_problem", lambda value: None)
    elif patch == "global_ownership_ignored":
        monkeypatch.setattr(
            generator._CatalogIndex, "sole_owner", lambda self, owner, kind, value: True
        )
    elif patch == "duplicate_iu_inactive":
        duplicate = only(entity(40, active=False), fund=False, registry=False)
        entities.append(duplicate)
        extra = {"iu": [duplicate[0]]}
        monkeypatch.setattr(
            generator,
            "_is_inactive",
            lambda index, owner: (
                index.iu[owner][0]["is_active"] is False and not index.funds.get(owner)
            ),
        )
    elif patch == "activity_ignored":
        original = generator._first_failure

        def skip_activity(index, owner, *, claims=True):
            code = original(index, owner, claims=claims)
            return None if code and code.startswith("activity.") else code

        monkeypatch.setattr(generator, "_first_failure", skip_activity)
    elif patch == "projection_check_skipped":
        divergent = entity(41)
        divergent[1]["ticker"] = "T-VIEW-DRIFT"
        entities.append(divergent)
        monkeypatch.setattr(
            generator, "_assert_projection_consistent", lambda index: None
        )
    elif patch == "registry_status_ignored":
        original = generator._first_failure

        def skip_status(index, owner, *, claims=True):
            code = original(index, owner, claims=claims)
            return None if code == "registry.status_not_canonical" else code

        monkeypatch.setattr(generator, "_first_failure", skip_status)
    policy_raw, snapshot_raw, _ = _build(calendar, entities, extra=extra)
    operator._policy(json.loads(policy_raw))  # internally consistent document
    report = verifier.audit(policy_raw, snapshot_raw=snapshot_raw)
    assert (
        report["gates"]["A5"]["status"] == "FAIL"
        or report["gates"]["A6"]["status"] == "FAIL"
    )
    assert verifier.verdict_of(report, strict=False) is False


def test_verifier_mutation_disagrees_with_a_correct_generator(calendar, monkeypatch):
    entities = [
        *_rich_entities(),
        entity(30, iu_isin=wrong_check(synthetic_isin(30)), reg_isin=None, cusip=None),
    ]
    policy_raw, snapshot_raw, _ = _build(calendar, entities)
    assert (
        verifier.audit(policy_raw, snapshot_raw=snapshot_raw)["gates"]["A5"]["status"]
        == "PASS"
    )
    monkeypatch.setattr(verifier, "isin_status", lambda value: None)
    report = verifier.audit(policy_raw, snapshot_raw=snapshot_raw)
    assert {"lifecycle_equal", "reason_counts_equal", "active_set_equal"} <= set(
        _failed_checks(report, "A5")
    )


def test_rehashed_lifecycle_tamper_passes_hashes_but_fails_independence(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    row = next(
        r for r in policy["instrument_evidence"] if r["fund_status"] == "UNKNOWN"
    )
    row.update(
        fund_status="ACTIVE",
        valuation_frequency="daily",
        identity_verified=True,
        return_basis_verified=True,
        currency_verified=True,
    )
    generation = policy["generation"]
    generation["instrument_evidence_digest"] = instrument_evidence_digest(
        policy["instrument_evidence"]
    )
    generation["policy_hash"] = policy_content_digest(policy)
    generation["generation_sha256"] = generation_metadata_digest(generation)
    report = verifier.audit(generator.canonical_json(policy), snapshot_raw=snapshot_raw)
    assert report["gates"]["A5"]["status"] == "FAIL"
    assert report["gates"]["A6"]["status"] == "FAIL"
    assert _failed_checks(report, "A1") == ["snapshot_link"]  # artifact bytes changed


def test_counts_and_bytes_tamper_fail_integrity_and_partition(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    policy["generation"]["counts"]["identity_first_failure"][
        "ticker.global_conflict"
    ] = 1
    report = verifier.audit(generator.canonical_json(policy), snapshot_raw=snapshot_raw)
    assert "generation_digest" in _failed_checks(report, "A1")
    assert "first_failure_equals_independent" in _failed_checks(report, "A2")
    report = verifier.audit(
        policy_raw.rstrip(b"\n") + b" \n", snapshot_raw=snapshot_raw
    )
    assert "canonical_bytes" in _failed_checks(report, "A1")


def test_retired_v1_policy_is_blocked_not_reinterpreted(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    policy["generator_version"] = policy["generation"]["generator_version"] = (
        "fund-nav-policy-generator-v1"
    )
    with pytest.raises(verifier.AuditBlocked, match="retired_generator_version"):
        verifier.audit(generator.canonical_json(policy), snapshot_raw=snapshot_raw)


def test_snapshot_tamper_and_source_type_errors(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    snapshot = json.loads(snapshot_raw)
    snapshot["sources"]["identity"][0]["figi"] = synthetic_figi(99)
    report = verifier.audit(policy_raw, snapshot_raw=generator.canonical_json(snapshot))
    assert {"snapshot_link", "source_snapshot_sha256"} <= set(
        _failed_checks(report, "A1")
    )
    snapshot = json.loads(snapshot_raw)
    snapshot["sources"]["instruments"][0]["is_active"] = 1
    with pytest.raises(verifier.AuditBlocked, match="source_value_type_invalid"):
        verifier.audit(policy_raw, snapshot_raw=generator.canonical_json(snapshot))
    snapshot = json.loads(snapshot_raw)
    snapshot["sources"]["funds"][0]["extra"] = "x"
    with pytest.raises(verifier.AuditBlocked, match="source_row_shape_invalid"):
        verifier.audit(policy_raw, snapshot_raw=generator.canonical_json(snapshot))


# ── A2/A3 continuity against a hash-pinned previous artifact ────────────────
def test_previous_active_preserved_explained_and_inactive_delta_decomposed(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    # Previously ACTIVE: 1 (kept), 10 (now isin.checksum), 77 (gone from sources).
    # Previously INACTIVE: 16 (kept), 15 (gained funds_v), 14 (activity changed).
    previous = _previous(
        active_ids=(1, 10, 77),
        inactive_ids=(16, 15, 14),
        unknown_ids=(2,),
        calendar=calendar,
    )
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        previous_raw=previous,
        config_raw=_config(expected_previous_inactive=3),
    )
    assert report["gates"]["A3"]["status"] == "PASS"
    assert report["details"]["A3"] == {
        "previous_active": 3,
        "preserved": 1,
        "explained": {"absent_from_sources": 1, "isin.checksum": 1},
        "unexplained": 0,
    }
    continuity = report["details"]["A2"]["inactive_continuity"]
    assert continuity["exits"] == {"activity_changed": 1, "gained_funds_v": 1}
    assert continuity["entries"] == {}
    assert report["gates"]["A2"]["status"] == "PASS"
    assert report["gates"]["A1"]["checks"]["hash_distinct_from_previous"] is True
    assert report["gates"]["A1"]["checks"]["calendar_identical_to_previous"] is True
    pinned_wrong = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        previous_raw=previous,
        config_raw=_config(expected_previous_inactive=8979),
    )
    assert "previous_inactive_pin" in _failed_checks(pinned_wrong, "A2")


def test_previous_active_lost_without_source_reason_is_unexplained(
    calendar, monkeypatch
):
    original = generator._first_failure

    def drop_one(index, owner, *, claims=True):
        return (
            "ticker.missing"
            if owner == str(uuid.UUID(int=1))
            else original(index, owner, claims=claims)
        )

    monkeypatch.setattr(generator, "_first_failure", drop_one)
    policy_raw, snapshot_raw, _ = _build(calendar)
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, previous_raw=_previous(active_ids=(1,))
    )
    assert report["gates"]["A3"]["status"] == "FAIL"
    assert report["details"]["A3"]["unexplained"] == 1


# ── A4 ceiling and structural baseline ──────────────────────────────────────
def test_ceiling_and_structural_baseline_acknowledgement(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    over = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, config_raw=_config(active_ceiling=8)
    )
    assert "active_within_ceiling" in _failed_checks(over, "A4")
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(structural_baseline=5103),
    )
    assert "structural_baseline_delta_acknowledged" in _failed_checks(report, "A4")
    delta = report["details"]["A4"]["structural_baseline_delta"]
    acknowledged = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(structural_baseline=5103, accepted_structural_delta=delta),
    )
    assert acknowledged["gates"]["A4"]["status"] == "PASS"


# ── A7/A8 over one live read-only capture ───────────────────────────────────
def test_live_capture_same_snapshot_passes_all_gates_and_builds_canary(calendar):
    report, policy_raw, snapshot_raw, rows, previous, config_raw = _strict_report(
        calendar
    )
    assert set(_gates(report).values()) == {"PASS"}, _gates(report)
    sleeves = report["details"]["A7"]["sleeves"]
    assert (
        sleeves["equity"]["required"] == 2 and sleeves["fixed_income"]["required"] == 2
    )
    assert (
        report["details"]["A7"]["exclusion_reasons"][
            "uncertified|not_in_evidence_universe"
        ]
        == 1
    )
    assert report["details"]["A8"]["outcomes"] == {"matched": 8, "missing": 1}
    canary = _canary_of(report, policy_raw, config_raw)
    again, *_ = _strict_report(calendar)
    assert canary == _canary_of(again, policy_raw, config_raw)
    assert canary["size"] == canary["cohort_size"] == 8
    assert (
        canary["strata"]["both|etf"] == 1
        and canary["strata"]["neither|mutual_fund"] == 1
    )
    assert len(canary["allowlist"]) <= verifier.CANARY_MAX
    # F2: the manifest is bound to the dossier, the capture and the eligible cohort.
    assert (
        canary["audit_report_sha256"]
        == hashlib.sha256(verifier.report_bytes(report)).hexdigest()
    )
    assert (
        canary["capture_bundle_sha256"]
        == report["inputs"]["capture"]["capture_bundle_sha256"]
    )
    assert canary["eligible_cohort_sha256"] == uuid_set_digest(canary["allowlist"])
    assert canary["audit_contract_sha256"] == verifier.AUDIT_CONTRACT_SHA256


def test_live_only_capture_without_snapshot_file(calendar):
    policy_raw, _, rows = _build(calendar)
    report = verifier.audit(policy_raw, live=_live(rows), config_raw=_config())
    assert (
        report["gates"]["A1"]["status"] == "PASS"
        and report["gates"]["A7"]["status"] == "PASS"
    )


def test_source_drift_fails_and_blocks_a7_a8(calendar):
    policy_raw, snapshot_raw, rows = _build(calendar)
    drifted = copy.deepcopy(rows)
    drifted[0][0]["currency"] = "EUR"
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=_live(drifted), config_raw=_config()
    )
    assert "no_source_drift" in _failed_checks(report, "A5")
    assert report["gates"]["A7"] == {
        "status": "NOT_EVALUATED",
        "code": "live_capture_absent_or_drifted",
        "checks": {},
    }
    assert report["gates"]["A8"]["status"] == "NOT_EVALUATED"


# ── F4: the Stage-1 margin is a contract constant ───────────────────────────
def test_a7_margin_arithmetic_is_exact_and_insufficient_sleeve_stops(calendar):
    policy_raw, snapshot_raw, rows = _build(calendar)
    quotas = {"alternatives": 6, "equity": 8, "fixed_income": 14, "thematic": 2}
    config = json.loads(_config())
    config["builder"]["stage1_quotas"] = quotas
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows),
        config_raw=json.dumps(config).encode(),
    )
    required = {
        sleeve: item["required"]
        for sleeve, item in report["details"]["A7"]["sleeves"].items()
    }
    assert required == {
        "alternatives": 7,
        "equity": 9,
        "fixed_income": 16,
        "thematic": 3,
    }
    assert report["gates"]["A7"]["status"] == "FAIL"
    config["builder"]["stage1_quotas"] = {"equity": 30}
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows),
        config_raw=json.dumps(config).encode(),
    )
    assert report["details"]["A7"]["sleeves"]["equity"]["required"] == 33


def test_fixed_income_quota_14_with_15_available_blocks_at_required_16(calendar):
    entities = [entity(n) for n in range(1, 16)]
    policy_raw, snapshot_raw, rows = _build(calendar, entities)
    cohort = [
        {"instrument_id": uuid.UUID(int=n), "strategy_label": "Government Bond"}
        for n in range(1, 16)
    ]
    config = json.loads(_config())
    config["builder"]["stage1_quotas"] = {"fixed_income": 14}
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, cohort=cohort, sec=[]),
        config_raw=json.dumps(config).encode(),
    )
    assert report["details"]["A7"]["sleeves"]["fixed_income"] == {
        "quota": 14,
        "margin": 2,
        "required": 16,
        "available_daily_active": 15,
        "sufficient": False,
    }
    assert _failed_checks(report, "A7") == ["sleeve_quota_margin"]
    assert report["details"]["A7"]["margin_contract"] == "1/10"


@pytest.mark.parametrize("margin", ["0", "-1/10", "1/20", "0.1", 0.1, None, "missing"])
def test_margin_other_than_contract_constant_is_rejected(calendar, margin):
    policy_raw, snapshot_raw, _ = _build(calendar)
    config = json.loads(_config())
    if margin == "missing":
        config["builder"].pop("margin_fraction")
    else:
        config["builder"]["margin_fraction"] = margin
    with pytest.raises(verifier.AuditBlocked, match="audit_config_margin_invalid"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )


@pytest.mark.parametrize("version", [None, "nav-identity-audit-contract-v1", 2])
def test_audit_contract_version_is_required_and_pinned(calendar, version):
    policy_raw, snapshot_raw, _ = _build(calendar)
    config = json.loads(_config())
    if version is None:
        config.pop("audit_contract_version")
    else:
        config["audit_contract_version"] = version
    with pytest.raises(verifier.AuditBlocked, match="audit_contract_version_invalid"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )


def test_repository_template_passes_config_validation():
    raw = (
        Path(verifier.__file__).parents[1] / "configs" / "nav_identity_audit_v2.json"
    ).read_bytes()
    config = verifier._load_config(raw)
    assert (
        config["builder"]["light_revision"]
        == "aeb5933754f46af997198a0ab82c14e108a4553a"
    )
    assert config["builder"]["margin_fraction"] == "1/10"
    assert config["sec"]["max_synced_age_days"] == 7
    assert config["audit_contract_version"] == verifier.AUDIT_CONTRACT_VERSION


def test_a7_every_cohort_exclusion_carries_a_reason(calendar):
    policy_raw, snapshot_raw, rows = _build(calendar)
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=_live(rows), config_raw=_config()
    )
    reasons = report["details"]["A7"]["exclusion_reasons"]
    assert (
        reasons["fixed_income|ticker.global_conflict"]
        + reasons["equity|ticker.global_conflict"]
        == 2
    )
    assert reasons["equity|inactive"] == 1  # entity 16
    assert reasons["fixed_income|not_daily"] == 1  # entity 9 (mmf)
    total_members = report["details"]["A7"]["cohort_rows"]
    active_daily = sum(
        v
        for k, v in report["details"]["A7"]["partition"].items()
        if k.endswith("|ACTIVE_DAILY") and not k.startswith("uncertified")
    )
    assert sum(reasons.values()) + active_daily == total_members
    duplicate = _live(
        rows,
        cohort=[{"instrument_id": uuid.UUID(int=1), "strategy_label": "Large Blend"}]
        * 2,
    )
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=duplicate, config_raw=_config()
    )
    assert _failed_checks(report, "A7") == ["cohort_unique", "sleeve_quota_margin"]
    assert report["details"]["A7"]["duplicate_uuids"] == 1
    assert report["inputs"]["capture"]["cohort"]["row_count"] == 2  # multiplicity kept


@pytest.mark.parametrize(
    "sec_rows,registry_class,expected",
    [
        (
            [{"class_id": "C1", "series_id": "S000000001", "ticker": "T1"}],
            "C1",
            "matched",
        ),
        (
            [{"class_id": "C1", "series_id": "S-OTHER", "ticker": "T1"}],
            "C1",
            "contradiction",
        ),
        (
            [{"class_id": "C1", "series_id": "S000000001", "ticker": "OTHER"}],
            "C1",
            "contradiction",
        ),
        ([], "C1", "missing"),
        (
            [{"class_id": "C9", "series_id": "S-OTHER", "ticker": "T1"}],
            None,
            "contradiction",
        ),
        (
            [{"class_id": "C9", "series_id": "S000000001", "ticker": "T1"}],
            None,
            "matched",
        ),
        (
            [
                {"class_id": "C8", "series_id": "S000000001", "ticker": "T1"},
                {"class_id": "C9", "series_id": "S000000001", "ticker": "T1"},
            ],
            None,
            "ambiguous_consistent",
        ),
        ([], None, "missing"),
        (
            [
                {"class_id": "C1", "series_id": "S000000001", "ticker": "T1"},
                {"class_id": "C9", "series_id": "S000000001", "ticker": "T1"},
            ],
            "C1",
            "contradiction",  # the ticker belongs to another SEC class
        ),
        ([{"class_id": "C1", "series_id": None, "ticker": "T1"}], "C1", "partial"),
        ([{"class_id": "C9", "series_id": None, "ticker": "T1"}], None, "partial"),
        (
            [
                {"class_id": "C1", "series_id": "S000000001", "ticker": "T1"},
                {"class_id": "C1", "series_id": "S000000001", "ticker": "T1"},
            ],
            "C1",
            "ambiguous_consistent",
        ),
    ],
)
def test_a8_sec_class_series_ticker_outcomes(
    calendar, sec_rows, registry_class, expected
):
    policy_raw, snapshot_raw, rows = _build(
        calendar, [entity(1, class_id=registry_class)]
    )
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, sec=sec_rows, cohort=[]),
        config_raw=_config(),
    )
    assert report["details"]["A8"]["outcomes"] == {expected: 1}
    assert report["gates"]["A8"]["status"] == (
        "FAIL" if expected == "contradiction" else "PASS"
    )


# ── F1: SEC freshness contract ───────────────────────────────────────────────
@pytest.mark.parametrize(
    "case,status,code,failed",
    [
        ("criterion_absent", "NOT_EVALUATED", "sec_freshness_criterion_absent", []),
        ("criterion_null", "NOT_EVALUATED", "sec_freshness_criterion_absent", []),
        ("lineage_null", "NOT_EVALUATED", "sec_lineage_unverifiable", []),
        ("lineage_naive", "NOT_EVALUATED", "sec_lineage_unverifiable", []),
        ("lineage_garbage", "NOT_EVALUATED", "sec_lineage_unverifiable", []),
        ("capture_naive", "NOT_EVALUATED", "sec_decision_time_unverifiable", []),
        ("stale", "FAIL", None, ["fresh_within_max_age"]),
        ("future", "FAIL", None, ["synced_not_in_future"]),
        ("boundary", "PASS", None, []),
        ("current", "PASS", None, []),
    ],
)
def test_a8_freshness_contract(calendar, case, status, code, failed):
    policy_raw, snapshot_raw, rows = _build(calendar)
    live = _live(rows)
    config = json.loads(_config())
    lineage = live["sec"]["lineage"]
    if case == "criterion_absent":
        config.pop("sec")
    elif case == "criterion_null":
        config["sec"] = {"max_synced_age_days": None}
    elif case == "lineage_null":
        lineage["max_synced_at"] = None
    elif case == "lineage_naive":
        lineage["max_synced_at"] = "2026-09-23T00:00:00"
    elif case == "lineage_garbage":
        lineage["max_synced_at"] = "yesterday"
    elif case == "capture_naive":
        live["captured_at"] = "2026-09-24T12:05:00"
    elif case == "stale":
        lineage["max_synced_at"] = "2026-09-17T12:04:59+00:00"  # 7d + 1s
    elif case == "future":
        lineage["max_synced_at"] = "2026-09-24T12:05:01+00:00"
    elif case == "boundary":
        lineage["max_synced_at"] = "2026-09-17T08:05:00-04:00"  # exactly 7 days
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=live,
        previous_raw=_previous(active_ids=(1,), inactive_ids=(16,), calendar=calendar),
        config_raw=json.dumps(config).encode(),
    )
    gate = report["gates"]["A8"]
    assert gate["status"] == status
    assert gate.get("code") == code
    assert _failed_checks(report, "A8") == failed
    if status != "NOT_EVALUATED":
        assert report["details"]["A8"]["freshness"]["decision_at"] == CAPTURED
    assert verifier.verdict_of(report, strict=True) is (status == "PASS")


@pytest.mark.parametrize("value", [0, -3, True, "7", 7.0])
def test_sec_freshness_criterion_must_be_a_positive_int(calendar, value):
    policy_raw, snapshot_raw, _ = _build(calendar)
    config = json.loads(_config())
    config["sec"] = {"max_synced_age_days": value}
    with pytest.raises(
        verifier.AuditBlocked, match="audit_config_sec_freshness_invalid"
    ):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )


@pytest.mark.parametrize(
    "sec_state,code",
    [
        (
            {"state": "unavailable", "code": "sec_relation_missing"},
            "sec_relation_missing",
        ),
        (
            {"state": "unavailable", "code": "sec_privilege_missing"},
            "sec_privilege_missing",
        ),
        (
            {"state": "failed", "code": "sec_query_failed:57014"},
            "sec_query_failed:57014",
        ),
    ],
)
def test_a8_unavailable_source_is_not_evaluated_not_zero(calendar, sec_state, code):
    policy_raw, snapshot_raw, rows = _build(calendar)
    live = _live(rows)
    live["sec"] = sec_state
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=live, config_raw=_config()
    )
    assert report["gates"]["A8"]["status"] == "NOT_EVALUATED"
    assert report["gates"]["A8"]["code"] == code


# ── F2: canonical private capture bundle ────────────────────────────────────
def test_capture_bundle_is_canonical_order_independent_and_reloadable(calendar):
    policy_raw, snapshot_raw, rows = _build(calendar)
    config = json.loads(_config())
    live = _live(rows)
    raw = verifier.capture_bytes(live, config)
    shuffled = copy.deepcopy(live)
    shuffled["cohort"]["rows"].reverse()
    shuffled["sec"]["rows"].reverse()
    for name in shuffled["sources"]:
        shuffled["sources"][name] = list(reversed(shuffled["sources"][name]))
    assert verifier.capture_bytes(shuffled, config) == raw
    bundle = verifier.load_capture(raw, config)
    assert bundle["cohort"]["row_count"] == 18 and bundle["sec"]["row_count"] == 8
    assert (
        bundle["source_snapshot_sha256"]
        == json.loads(policy_raw)["generation"]["source_snapshot_sha256"]
    )
    from_file = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, capture_raw=raw, config_raw=_config()
    )
    from_live = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=live, config_raw=_config()
    )
    assert verifier.report_bytes(from_file) == verifier.report_bytes(from_live)
    assert (
        from_file["inputs"]["capture"]["capture_bundle_sha256"]
        == hashlib.sha256(raw).hexdigest()
    )
    with pytest.raises(verifier.AuditBlocked, match="capture_source_ambiguous"):
        verifier.audit(policy_raw, live=live, capture_raw=raw, config_raw=_config())


def test_swapping_a_cohort_member_changes_bound_digests(calendar):
    """Reviewer repro: same aggregates, different eligible member → new hashes."""
    entities = [entity(n) for n in range(1, 5)]
    policy_raw, snapshot_raw, rows = _build(calendar, entities)

    def run(members):
        cohort = [
            {"instrument_id": uuid.UUID(int=n), "strategy_label": "Large Blend"}
            for n in members
        ]
        return verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            live=_live(rows, cohort=cohort, sec=[]),
            config_raw=_config(),
        )

    first, second = run((1, 2, 3)), run((1, 2, 4))
    assert first["details"]["A7"]["sleeves"] == second["details"]["A7"]["sleeves"]
    assert first["details"]["A7"]["partition"] == second["details"]["A7"]["partition"]
    for key in ("eligible_cohort_sha256", "cohort_rows_sha256"):
        assert first["details"]["A7"][key] != second["details"]["A7"][key]
    assert (
        first["inputs"]["capture"]["capture_bundle_sha256"]
        != second["inputs"]["capture"]["capture_bundle_sha256"]
    )
    assert verifier.report_bytes(first) != verifier.report_bytes(second)
    sec_a = run((1,))
    live = _live(
        rows, cohort=[], sec=[{"class_id": "C9", "series_id": None, "ticker": "ZZZ"}]
    )
    sec_b = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=live, config_raw=_config()
    )
    assert sec_a["details"]["A8"]["outcomes"] == sec_b["details"]["A8"]["outcomes"]
    assert (
        sec_a["inputs"]["capture"]["sec"]["rows_sha256"]
        != sec_b["inputs"]["capture"]["sec"]["rows_sha256"]
    )


@pytest.mark.parametrize(
    "tamper,code",
    [
        ("bytes", "capture_not_canonical"),
        ("cohort_row", "capture_bundle_invalid"),
        ("cohort_digest", "capture_bundle_invalid"),
        ("sec_row", "capture_bundle_invalid"),
        ("source_row", "capture_bundle_invalid"),
        ("source_digest", "capture_bundle_invalid"),
        ("kind", "capture_contract_invalid"),
        ("query", "capture_cohort_query_mismatch"),
    ],
)
def test_capture_bundle_tamper_is_blocked(calendar, tamper, code):
    policy_raw, snapshot_raw, rows = _build(calendar)
    config = json.loads(_config())
    raw = verifier.capture_bytes(_live(rows), config)
    bundle = json.loads(raw)
    if tamper == "bytes":
        tampered = raw.replace(b"\n", b" \n")
    else:
        if tamper == "cohort_row":
            bundle["cohort"]["rows"][0]["strategy_label"] = "Technology"
        elif tamper == "cohort_digest":
            bundle["cohort"]["rows_sha256"] = "0" * 64
        elif tamper == "sec_row":
            bundle["sec"]["rows"][0]["ticker"] = "OTHER"
        elif tamper == "source_row":
            bundle["sources"]["funds"][0]["fund_type"] = "mmf"
        elif tamper == "source_digest":
            bundle["source_snapshot_sha256"] = "0" * 64
        elif tamper == "kind":
            bundle["kind"] = "other"
        elif tamper == "query":
            config["builder"]["cohort_query"] = (
                "SELECT instrument_id, strategy_label FROM other"
            )
        tampered = (
            json.dumps(bundle, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    with pytest.raises(verifier.AuditBlocked, match=code):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            capture_raw=tampered,
            config_raw=json.dumps(config).encode(),
        )


def test_invalid_cohort_rows_are_not_evaluated_and_still_hashed(calendar):
    policy_raw, snapshot_raw, rows = _build(calendar)
    live = _live(rows, cohort=[{"instrument_id": "not-a-uuid", "strategy_label": "x"}])
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=live, config_raw=_config()
    )
    assert report["gates"]["A7"] == {
        "status": "NOT_EVALUATED",
        "code": "cohort_uuid_invalid",
        "checks": {},
    }
    assert report["inputs"]["capture"]["cohort"]["state"] == "invalid"


# ── F3: every declared generation count is recounted independently ─────────
def _rehash_counts(policy: dict) -> bytes:
    generation = policy["generation"]
    generation["generation_sha256"] = generation_metadata_digest(generation)
    return generator.canonical_json(policy)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c.__setitem__("active", c["active"] + 1),
        lambda c: c.__setitem__("active_daily", c["active_daily"] - 1),
        lambda c: c.__setitem__("instruments_universe", c["instruments_universe"] + 1),
        lambda c: c.__setitem__("funds_v", c["funds_v"] - 1),
        lambda c: c.__setitem__("instrument_identity", c["instrument_identity"] + 2),
        lambda c: c.__setitem__("instrument_evidence", c["instrument_evidence"] + 1),
        lambda c: c.__setitem__("valuation_frequency", {"daily": 9, "unknown": 8}),
        lambda c: c.__setitem__("valuation_frequency", {"daily": 8, "weekly": 9}),
        lambda c: c.__setitem__("inactive_reason", {"inactive_without_funds_v": 2}),
        lambda c: c.__setitem__("inactive_reason", {}),
        lambda c: c.__setitem__("structural_pre_claims_daily", 1),
        lambda c: c["isin_presence_active_daily"].__setitem__("both", 0),
        lambda c: c.__setitem__("active", True),
        lambda c: c.__setitem__("active", -1),
        lambda c: c.__setitem__("active", 9.0),
        lambda c: c.__setitem__("unexpected_key", 0),
        lambda c: c.pop("instrument_identity"),
    ],
)
def test_counts_tampered_with_recomputed_hashes_fail_independent_recount(
    calendar, mutate
):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    mutate(policy["generation"]["counts"])
    tampered = _rehash_counts(policy)
    report = verifier.audit(tampered, snapshot_raw=snapshot_raw, config_raw=_config())
    assert report["gates"]["A1"]["checks"]["generation_digest"] is True
    assert "generation_counts_exact" in _failed_checks(report, "A2")
    assert report["details"]["A2"]["count_mismatch_keys"]


def test_active_mmf_with_weekly_frequency_is_a_lifecycle_mismatch(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    row = next(
        r
        for r in policy["instrument_evidence"]
        if r["instrument_id"] == str(uuid.UUID(int=9))
    )
    assert (row["fund_status"], row["valuation_frequency"]) == ("ACTIVE", "unknown")
    row["valuation_frequency"] = "weekly"
    generation = policy["generation"]
    generation["instrument_evidence_digest"] = instrument_evidence_digest(
        policy["instrument_evidence"]
    )
    generation["policy_hash"] = policy_content_digest(policy)
    generation["generation_sha256"] = generation_metadata_digest(generation)
    report = verifier.audit(
        generator.canonical_json(policy),
        snapshot_raw=snapshot_raw,
        config_raw=_config(),
    )
    assert {"evidence_digest", "policy_hash", "generation_digest"}.isdisjoint(
        _failed_checks(report, "A1")
    )
    assert "lifecycle_equal" in _failed_checks(report, "A5")
    assert "flags_consistent" in _failed_checks(report, "A6")
    assert report["details"]["A5"]["lifecycle_mismatch"] == 1


def test_evidence_row_extra_field_or_timestamp_drift_fails_shape(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    for mutate in (
        lambda r: r.__setitem__("note", "x"),
        lambda r: r.__setitem__("known_at", "2020-01-01T00:00:00+00:00"),
    ):
        policy = json.loads(policy_raw)
        mutate(policy["instrument_evidence"][0])
        generation = policy["generation"]
        generation["instrument_evidence_digest"] = instrument_evidence_digest(
            policy["instrument_evidence"]
        )
        generation["policy_hash"] = policy_content_digest(policy)
        generation["generation_sha256"] = generation_metadata_digest(generation)
        report = verifier.audit(
            generator.canonical_json(policy),
            snapshot_raw=snapshot_raw,
            config_raw=_config(),
        )
        assert "evidence_shape" in _failed_checks(report, "A1")


# ── F6: bounded cohort capture ──────────────────────────────────────────────
def test_drain_bounded_stops_at_sentinel_without_reading_everything():
    served = []

    def fetchmany(size):
        served.append(size)
        return [object()] * size  # an infinite result set

    with pytest.raises(verifier._RowCeilingExceeded):
        verifier.drain_bounded(fetchmany, ceiling=100_000, batch=5000)
    assert sum(served) == 105_000 and set(served) == {5000}
    assert verifier.drain_bounded(
        _chunks([[1, 1, 1], [2, 2], []]), ceiling=5, batch=3
    ) == [1, 1, 1, 2, 2]
    with pytest.raises(verifier._RowCeilingExceeded):
        verifier.drain_bounded(_chunks([[1, 1, 1], [2, 2, 2], []]), ceiling=5, batch=3)


def _chunks(batches):
    iterator = iter(batches)
    return lambda size: next(iterator)


@pytest.mark.parametrize(
    "query,code",
    [
        ("SELECT 1; DROP TABLE x", "audit_config_cohort_query_unsafe"),
        ("SELECT 1 -- trailing", "audit_config_cohort_query_unsafe"),
        ("SELECT /* c */ 1", "audit_config_cohort_query_unsafe"),
        (
            "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
            "audit_config_builder_invalid",
        ),
        (
            "SELECT * FROM (WITH d AS (DELETE FROM t RETURNING id) SELECT id FROM d) q",
            "audit_config_cohort_query_unsafe",
        ),
        ("SELECT nextval('s')", "audit_config_cohort_query_unsafe"),
        ("SELECT pg_advisory_lock(1)", "audit_config_cohort_query_unsafe"),
        ("SELECT pg_sleep(100)", "audit_config_cohort_query_unsafe"),
        ("SELECT * FROM t FOR UPDATE", "audit_config_cohort_query_unsafe"),
        ("SELECT * FROM t FOR NO KEY UPDATE", "audit_config_cohort_query_unsafe"),
        ("SELECT dblink_exec('x')", "audit_config_cohort_query_unsafe"),
        ("  select instrument_id from t", None),
        ("SELECT instrument_id FROM t ORDER BY 1 LIMIT 10", None),
        ("UPDATE t SET x=1", "audit_config_builder_invalid"),
    ],
)
def test_cohort_query_validation(query, code):
    assert verifier.cohort_query_problem(query) == code


# ── F5: the dossier is always written before any canary decision ───────────
def test_canary_requires_strict_pass(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    report = verifier.audit(policy_raw, snapshot_raw=snapshot_raw, config_raw=_config())
    with pytest.raises(
        verifier.AuditBlocked, match="canary_requires_strict_audit_pass"
    ):
        verifier.build_canary(
            report, policy_raw, json.loads(_config()), audit_report_sha256="0" * 64
        )


def test_canary_is_capped_covers_strata_and_ignores_input_order(calendar):
    entities = [
        entity(n, fund_type="etf" if n % 2 else "mutual_fund") for n in range(1, 41)
    ]
    policy_raw, snapshot_raw, rows = _build(calendar, entities)
    cohort = [
        {"instrument_id": uuid.UUID(int=n), "strategy_label": LABELS[1 + (n % 4)]}
        for n in range(1, 41)
    ]
    config = _config()
    previous = _previous(active_ids=(1,), calendar=calendar)
    first = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, cohort=cohort),
        previous_raw=previous,
        config_raw=config,
    )
    assert set(_gates(first).values()) == {"PASS"}, _gates(first)
    manifest = _canary_of(first, policy_raw, config)
    second = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, cohort=list(reversed(cohort))),
        previous_raw=previous,
        config_raw=config,
    )
    assert _canary_of(second, policy_raw, config) == manifest
    assert manifest["size"] == 20 and manifest["cohort_size"] == 40
    assert manifest["salt"] == "nav-policy-v2-canary-2026-09-24"


# ── CLI and custody (POSIX) ─────────────────────────────────────────────────
def _custody(tmp_path):
    root = tmp_path / "custody"
    root.mkdir(mode=0o700)
    return root


def _cli_inputs(root, calendar, *, entities=None, config=None, live_kwargs=None):
    policy_raw, snapshot_raw, rows = _build(calendar, entities)
    (root / "policy.json").write_bytes(policy_raw)
    (root / "source.json").write_bytes(snapshot_raw)
    config_raw = config or _config()
    (root / "config.json").write_bytes(config_raw)
    previous = _previous(active_ids=(1,), inactive_ids=(16,), calendar=calendar)
    (root / "v1.json").write_bytes(previous)
    capture = verifier.capture_bytes(
        _live(rows, **(live_kwargs or {})), json.loads(config_raw)
    )
    (root / "capture.json").write_bytes(capture)
    base = [
        "--policy-file",
        str(root / "policy.json"),
        "--source-snapshot-file",
        str(root / "source.json"),
        "--audit-config",
        str(root / "config.json"),
        "--previous-policy-file",
        str(root / "v1.json"),
        "--previous-policy-sha256",
        hashlib.sha256(previous).hexdigest(),
        "--custody-root",
        str(root),
    ]
    return base, policy_raw, capture


@pytest.mark.skipif(os.name != "posix", reason="custody writer is POSIX-only")
def test_cli_writes_private_dossier_and_prints_aggregates_only(
    calendar, tmp_path, capsys
):
    root = _custody(tmp_path)
    base, policy_raw, _ = _cli_inputs(root, calendar)
    assert verifier.main([*base, "--output", str(root / "audit.json")]) == 0
    printed = capsys.readouterr().out
    summary = json.loads(printed)
    assert summary["gates"]["A7"] == "NOT_EVALUATED" and summary["status"] == "pass"
    assert (
        str(uuid.UUID(int=1)) not in printed
        and "T1" not in printed
        and synthetic_isin(1) not in printed
    )
    dossier = json.loads((root / "audit.json").read_bytes())
    assert dossier["audit_version"] == "nav-identity-audit-v2"
    assert dossier["inputs"]["audit_contract_sha256"] == verifier.AUDIT_CONTRACT_SHA256
    assert (
        dossier["inputs"]["policy_artifact_sha256"]
        == hashlib.sha256(policy_raw).hexdigest()
    )
    assert stat.S_IMODE((root / "audit.json").stat().st_mode) == 0o600
    assert (
        summary["audit_sha256"]
        == hashlib.sha256((root / "audit.json").read_bytes()).hexdigest()
    )
    assert (
        verifier.main([*base, "--output", str(root / "audit-strict.json"), "--strict"])
        == 3
    )
    capsys.readouterr()
    assert (root / "audit-strict.json").exists()  # failed strict audit is persisted
    assert verifier.main([*base, "--output", str(root / "audit.json")]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "artifact_already_exists"
    base[base.index("--previous-policy-sha256") + 1] = "0" * 64
    assert verifier.main([*base, "--output", str(root / "audit-2.json")]) == 2
    assert (
        json.loads(capsys.readouterr().out)["code"] == "previous_policy_sha256_mismatch"
    )
    assert not (root / "audit-2.json").exists()


@pytest.mark.skipif(os.name != "posix", reason="custody writer is POSIX-only")
def test_cli_strict_pass_with_capture_file_writes_bound_canary(
    calendar, tmp_path, capsys
):
    root = _custody(tmp_path)
    base, policy_raw, capture = _cli_inputs(root, calendar)
    args = [
        *base,
        "--capture-file",
        str(root / "capture.json"),
        "--output",
        str(root / "audit.json"),
        "--canary-output",
        str(root / "canary.json"),
    ]
    assert verifier.main(args) == 0, capsys.readouterr().out
    printed = capsys.readouterr().out
    summary = json.loads(printed)
    assert summary["strict"] is True and summary["canary"]["status"] == "written"
    assert str(uuid.UUID(int=1)) not in printed
    canary = json.loads((root / "canary.json").read_bytes())
    audit_bytes = (root / "audit.json").read_bytes()
    assert canary["audit_report_sha256"] == hashlib.sha256(audit_bytes).hexdigest()
    assert canary["capture_bundle_sha256"] == hashlib.sha256(capture).hexdigest()
    dossier = json.loads(audit_bytes)
    assert dossier["canary_requested"] is True
    assert (
        dossier["details"]["A7"]["eligible_cohort_sha256"]
        == canary["eligible_cohort_sha256"]
    )
    for name in ("audit.json", "canary.json"):
        assert stat.S_IMODE((root / name).stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="custody writer is POSIX-only")
@pytest.mark.parametrize(
    "failure", ["sec_contradiction", "insufficient_sleeve", "stale_sec"]
)
def test_cli_strict_failure_persists_dossier_and_refuses_canary(
    calendar, tmp_path, capsys, failure
):
    root = _custody(tmp_path)
    config = json.loads(_config())
    live_kwargs = {}
    if failure == "sec_contradiction":
        live_kwargs["sec"] = [
            {"class_id": "C000000001", "series_id": "S-FOREIGN", "ticker": "T1"}
        ]
    elif failure == "insufficient_sleeve":
        config["builder"]["stage1_quotas"] = {"equity": 30}
    base, _, capture = _cli_inputs(
        root, calendar, config=json.dumps(config).encode(), live_kwargs=live_kwargs
    )
    if failure == "stale_sec":
        bundle = json.loads(capture)
        live = _live(_build(calendar)[2])
        live["sec"]["lineage"]["max_synced_at"] = "2026-01-01T00:00:00+00:00"
        (root / "capture.json").unlink()
        (root / "capture.json").write_bytes(verifier.capture_bytes(live, config))
        assert bundle["sec"]["lineage"]["max_synced_at"] != "2026-01-01T00:00:00+00:00"
    args = [
        *base,
        "--capture-file",
        str(root / "capture.json"),
        "--output",
        str(root / "audit.json"),
        "--canary-output",
        str(root / "canary.json"),
    ]
    assert verifier.main(args) == 3
    summary = json.loads(capsys.readouterr().out)
    assert summary["canary"] == {
        "status": "refused",
        "code": "canary_requires_strict_audit_pass",
    }
    assert (root / "audit.json").exists() and not (root / "canary.json").exists()
    dossier = json.loads((root / "audit.json").read_bytes())
    assert dossier["result"] == "fail" and dossier["canary_requested"] is True
    failing = {
        name for name, gate in dossier["gates"].items() if gate["status"] != "PASS"
    }
    expected = {
        "sec_contradiction": {"A8"},
        "insufficient_sleeve": {"A7"},
        "stale_sec": {"A8"},
    }
    assert failing == expected[failure]
    if failure == "sec_contradiction":
        assert dossier["differences"]["sec_contradictions"] == [str(uuid.UUID(int=1))]


@pytest.mark.skipif(os.name != "posix", reason="custody writer is POSIX-only")
def test_cli_output_guards_run_before_any_work(calendar, tmp_path, capsys, monkeypatch):
    root = _custody(tmp_path)
    base, _, _ = _cli_inputs(root, calendar)
    monkeypatch.setenv("NAV_FAKE_AUDIT_DSN", "postgresql://fake.invalid/never")
    calls = []
    monkeypatch.setattr(verifier, "capture_live", lambda *a: calls.append(a))
    cases = [
        (
            ["--output", str(root / "x.json"), "--canary-output", str(root / "x.json")],
            "output_paths_collide",
        ),
        (["--output", str(root / "policy.json")], "artifact_already_exists"),
        (
            ["--output", str(root / "y.json"), "--dsn-env", "NAV_FAKE_AUDIT_DSN"],
            "capture_output_required",
        ),
        (
            [
                "--output",
                str(root / "y.json"),
                "--capture-output",
                str(root / "c2.json"),
            ],
            "capture_output_requires_dsn",
        ),
        (
            [
                "--output",
                str(root / "y.json"),
                "--dsn-env",
                "NAV_FAKE_AUDIT_DSN",
                "--capture-output",
                str(root / "c3.json"),
                "--capture-file",
                str(root / "capture.json"),
            ],
            "capture_source_ambiguous",
        ),
    ]
    for extra, code in cases:
        assert verifier.main([*base, *extra]) == 2
        assert json.loads(capsys.readouterr().out)["code"] == code
    assert not calls and not (root / "y.json").exists()


@pytest.mark.skipif(os.name != "posix", reason="custody writer is POSIX-only")
def test_cli_live_capture_is_persisted_before_audit(
    calendar, tmp_path, capsys, monkeypatch
):
    root = _custody(tmp_path)
    base, _, capture = _cli_inputs(root, calendar)
    rows = _build(calendar)[2]
    monkeypatch.setenv("NAV_FAKE_AUDIT_DSN", "postgresql://fake.invalid/never")
    monkeypatch.setattr(verifier, "capture_live", lambda dsn, config: _live(rows))
    seen = {}
    original = verifier.audit

    def spy(*args, **kwargs):
        seen["capture_exists"] = (root / "capture-live.json").exists()
        return original(*args, **kwargs)

    monkeypatch.setattr(verifier, "audit", spy)
    args = [
        *base,
        "--dsn-env",
        "NAV_FAKE_AUDIT_DSN",
        "--capture-output",
        str(root / "capture-live.json"),
        "--output",
        str(root / "audit.json"),
        "--strict",
    ]
    assert verifier.main(args) == 0, capsys.readouterr().out
    printed = capsys.readouterr().out
    assert seen == {"capture_exists": True}
    assert (root / "capture-live.json").read_bytes() == capture
    assert stat.S_IMODE((root / "capture-live.json").stat().st_mode) == 0o600
    assert "fake.invalid" not in printed and str(uuid.UUID(int=1)) not in printed
