"""Independent A1–A8 audit: non-circularity, mutation detection and gates."""

from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
import os
import random
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
    IDENTITY_CONTRACT_VERSION,
    IDENTITY_FAILURE_CODES,
    IDENTITY_QUERY,
    INSTRUMENTS_QUERY,
    SEC_QUERY,
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
    sec_row,
    synthetic_figi,
    synthetic_isin,
    wrong_check,
)

START = dt.date(2024, 1, 1)
END = dt.date(2027, 12, 31)
OBSERVED = dt.datetime(2026, 9, 24, 12, 0, tzinfo=dt.timezone.utc)
CAPTURED = "2026-09-24T12:05:00+00:00"
V2_CATALOG_REFERENCE = (
    "nav-current-catalog-snapshot-v2:public.instruments_universe+public.funds_v+"
    "public.instrument_identity:w1-tiingo-adjusted-daily-v1:current_only_not_pit:"
    "identity=registry-ticker-series-claims-v2"
)
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
        calendar, *rows, observed, "current-daily-nav-xnys-usd-adjusted", "2026-09-25.3"
    )
    snapshot = generator.build_source_snapshot(policy, *rows)
    return generator.canonical_json(policy), generator.canonical_json(snapshot), rows


def _gates(report):
    return {name: gate["status"] for name, gate in report["gates"].items()}


def _failed_checks(report, gate):
    return sorted(
        k for k, ok in report["gates"][gate].get("checks", {}).items() if not ok
    )


SEC_CONFIG = {
    "source_contract": "sec-company-tickers-mf-class-map-v1",
    "relation": "public.sec_company_tickers_mf",
    "timestamp_column": "updated_at",
    "query_contract_sha256": verifier.SEC_QUERY_CONTRACT_SHA256,
    "max_synced_age_days": 7,
    "exclusion_fraction": {"numerator": 1, "denominator": 10},
    "conflict_ceiling": 0,
}


def _lineage(rows):
    """Lineage aggregate exactly describing the given rows (as the DB would)."""
    instants = [row["synced_at"] for row in rows]
    return {
        "row_count": len(rows),
        "min_synced_at": min(instants) if rows else None,
        "max_synced_at": max(instants) if rows else None,
    }


def _config(**overrides):
    config = {
        "audit_config_version": verifier.AUDIT_CONFIG_VERSION,
        "audit_contract_version": verifier.AUDIT_CONTRACT_VERSION,
        "structural_daily_ceiling": 5103,
        "structural_baseline": None,
        "canary_salt": "nav-policy-v3-canary-2026-09-25",
        "sec": dict(SEC_CONFIG),
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
    """A fake READ ONLY capture with database-shaped values (uuid.UUID,
    aware datetimes). By default the SEC part is the policy's own SEC rows."""
    instruments, funds, identity, policy_sec = rows
    if cohort is None:
        cohort = [
            {"instrument_id": uuid.UUID(int=n), "strategy_label": LABELS[1 + (n % 2)]}
            for n in range(1, 18)
        ] + [{"instrument_id": uuid.UUID(int=99), "strategy_label": "Unclassified"}]
    if sec is None:
        # Same snapshot as the policy: every rich ACTIVE (MMF included) matched.
        sec = [dict(row) for row in policy_sec]
    return {
        "captured_at": CAPTURED,
        "sources": {"instruments": instruments, "funds": funds, "identity": identity},
        "cohort": {"state": "captured", "rows": cohort},
        "sec": {"state": "captured", "rows": sec, "lineage": _lineage(sec)},
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
        "policy_id": "current-daily-nav-xnys-usd-adjusted",
        "policy_version": "2026-09-23.1",
        "generator_version": "fund-nav-policy-generator-v1",
        "instrument_evidence": rows,
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
            "timezone",
            "source_reference",
            "valid_through",
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
        "validate_generation_v3",
        "uuid_set_digest",
        "_SecIndex",
        "_sec_first_failure",
        "SEC_FAILURE_CODES",
        "SEC_GAP_CODES",
        "SEC_CONFLICT_CODES",
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
    assert verifier.SOURCE_SQL["sec"] == SEC_QUERY == verifier.SEC_SQL
    assert verifier.EXPECTED_REFERENCE == CATALOG_EVIDENCE_REFERENCE
    assert verifier.EXPECTED_GENERATOR == generator.GENERATOR_VERSION
    assert verifier.SNAPSHOT_KIND == generator.SOURCE_SNAPSHOT_KIND
    contract = verifier.AUDIT_CONTRACT["generation"]
    assert contract["generator_version"] == generator.GENERATOR_VERSION
    assert contract["catalog_query_version"] == generator.CURRENT_CATALOG_QUERY_VERSION
    assert contract["identity_contract_version"] == IDENTITY_CONTRACT_VERSION
    assert verifier.AUDIT_CONTRACT["catalog_source_query_sha256"] == (
        SOURCE_QUERY_SHA256
    )


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
    # B (v1 structural daily, no ISIN) ⊇ P (v2 pre-claims daily) ⊇ D (ACTIVE daily)
    a4 = report["details"]["A4"]
    assert (
        a4["baseline_count"],
        a4["structural_daily_count"],
        a4["active_daily_count"],
    ) == (
        11,
        9,
        8,
    )
    assert a4["pre_claim_first_failure"] == {
        "cardinality.registry_missing": 1,
        "registry.status_not_canonical": 1,
    }
    assert a4["claim_first_failure"] == {"isin.checksum": 1}
    assert a4["sec_first_failure"] == {}
    assert a4["ceiling_population"] == "structural_pre_claims_daily"
    assert set(a4) == set(verifier.A4_DETAIL_KEYS)


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


@pytest.mark.parametrize(
    "field,value",
    [
        ("generator_version", "fund-nav-policy-generator-v1"),
        ("generator_version", "fund-nav-policy-generator-v2"),
        ("source_query_version", "nav-current-catalog-snapshot-v2"),
    ],
)
def test_retired_v1_v2_policy_is_blocked_not_reinterpreted(calendar, field, value):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    if field == "generator_version":
        policy["generator_version"] = value
    policy["generation"][field] = value
    with pytest.raises(verifier.AuditBlocked, match="retired_generator_version"):
        verifier.audit(generator.canonical_json(policy), snapshot_raw=snapshot_raw)


@pytest.mark.parametrize(
    "version",
    ["fund-nav-policy-generator-v2", "fund-nav-policy-generator-v3", "other"],
)
def test_only_the_published_v1_is_a_previous_policy(calendar, version):
    """A never-published v2 (or any later) artifact is never the previous policy."""
    policy_raw, snapshot_raw, _ = _build(calendar)
    previous = json.loads(_previous(active_ids=(1,), calendar=calendar))
    previous["generator_version"] = version
    with pytest.raises(verifier.AuditBlocked, match="previous_policy_retired"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            previous_raw=json.dumps(previous).encode(),
        )
    previous["generator_version"] = "fund-nav-policy-generator-v1"
    previous["generation"] = {"generator_version": version}
    with pytest.raises(verifier.AuditBlocked, match="previous_policy_retired"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            previous_raw=json.dumps(previous).encode(),
        )
    # Labels stripped, but the v2/v3 catalog evidence reference gives it away.
    previous.pop("generator_version")
    previous.pop("generation")
    for reference in (V2_CATALOG_REFERENCE, CATALOG_EVIDENCE_REFERENCE):
        for row in previous["instrument_evidence"]:
            row["evidence_reference"] = reference
        with pytest.raises(verifier.AuditBlocked, match="previous_policy_retired"):
            verifier.audit(
                policy_raw,
                snapshot_raw=snapshot_raw,
                previous_raw=json.dumps(previous).encode(),
            )
    # The hand-authored pre-generator pointer (no generator metadata) is v1.
    for row in previous["instrument_evidence"]:
        row["evidence_reference"] = "fixture-previous-identity"
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        previous_raw=json.dumps(previous).encode(),
    )
    assert report["inputs"]["previous_policy_identity"][1] == "2026-09-23.1"


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
    with pytest.raises(verifier.AuditBlocked, match="source_snapshot_contract_invalid"):
        verifier.audit(policy_raw, snapshot_raw=generator.canonical_json(snapshot))
    snapshot = json.loads(snapshot_raw)
    snapshot["sources"]["funds"][0]["extra"] = "x"
    with pytest.raises(verifier.AuditBlocked, match="source_snapshot_contract_invalid"):
        verifier.audit(policy_raw, snapshot_raw=generator.canonical_json(snapshot))


# ── Round2 F4/F5: hostile top-level shapes and full snapshot linkage ────────
@pytest.mark.parametrize(
    "raw",
    [b"[]", b"null", b"1", b'"x"', b'{"a":1,"a":2}', b'{"a":NaN}', b"\xff", b"{"],
)
def test_hostile_policy_snapshot_previous_roots_block_with_static_codes(calendar, raw):
    policy_raw, snapshot_raw, _ = _build(calendar)
    for kwargs, code in (
        ({"snapshot_raw": raw}, "source_snapshot_contract_invalid"),
        (
            {"snapshot_raw": snapshot_raw, "previous_raw": raw},
            "previous_policy_invalid",
        ),
    ):
        with pytest.raises(verifier.AuditBlocked, match=code):
            verifier.audit(policy_raw, **kwargs)
    with pytest.raises(verifier.AuditBlocked) as exc:
        verifier.audit(raw, snapshot_raw=snapshot_raw)
    assert str(exc.value) in ("policy_not_json", "artifact_not_object")
    for loader, code in (
        (lambda: generator.verify_artifact(json.loads(raw)), "artifact_not_object"),
        (
            lambda: generator.verify_source_snapshot(
                json.loads(raw), json.loads(policy_raw)
            ),
            "source_snapshot_contract_invalid",
        ),
    ):
        try:
            value = json.loads(raw)
        except ValueError:
            continue
        if isinstance(value, dict):
            continue
        with pytest.raises(generator.PolicyGenerationError, match=code):
            loader()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s["sources"].__setitem__("funds", "not-a-list"),
        lambda s: s["sources"].__setitem__("funds", [1, 2]),
        lambda s: s["sources"].pop("identity"),
        lambda s: s["sources"].__setitem__("extra", []),
        lambda s: s.__setitem__("row_counts", []),
        lambda s: s.__setitem__(
            "row_counts", {"funds": True, "identity": 1, "instruments": 1, "sec": 1}
        ),
        lambda s: s.__setitem__(
            "row_counts", {"funds": 1, "identity": 1, "instruments": 1}
        ),
        lambda s: s["sources"].pop("sec"),
        lambda s: s["sources"]["sec"][0].__setitem__(
            "synced_at", "2026-09-23T00:00:00+00:00"
        ),
        lambda s: s["sources"]["sec"][0].__setitem__("synced_at", None),
        lambda s: s["sources"]["sec"][0].__setitem__("ticker", 5),
        lambda s: s["sources"]["sec"][0].__setitem__("extra", "x"),
        lambda s: s.__setitem__("sources", []),
        lambda s: s.pop("policy_id"),
        lambda s: s.__setitem__("unexpected", 1),
    ],
)
def test_malformed_snapshot_blocks_before_lookup_in_both_verifiers(calendar, mutate):
    policy_raw, snapshot_raw, _ = _build(calendar)
    snapshot = json.loads(snapshot_raw)
    mutate(snapshot)
    raw = json.dumps(snapshot, sort_keys=True).encode()
    with pytest.raises(verifier.AuditBlocked, match="source_snapshot_contract_invalid"):
        verifier.audit(policy_raw, snapshot_raw=raw)
    with pytest.raises(
        generator.PolicyGenerationError, match="source_snapshot_contract_invalid"
    ):
        generator.verify_source_snapshot(snapshot, json.loads(policy_raw))


@pytest.mark.parametrize(
    "field,value",
    [
        ("policy_id", "other-policy"),
        ("policy_version", "2026-09-24.9"),
        ("generator_version", "fund-nav-policy-generator-v1"),
        ("generator_version", "fund-nav-policy-generator-v2"),
        ("source_query_version", "nav-current-catalog-snapshot-v1"),
        ("source_query_version", "nav-current-catalog-snapshot-v2"),
        ("row_counts", {"funds": 1, "identity": 1, "instruments": 1, "sec": 1}),
    ],
)
def test_snapshot_link_covers_identity_versions_and_row_counts(calendar, field, value):
    policy_raw, snapshot_raw, _ = _build(calendar)
    snapshot = json.loads(snapshot_raw)
    snapshot[field] = value
    raw = generator.canonical_json(snapshot)
    report = verifier.audit(policy_raw, snapshot_raw=raw)
    assert "snapshot_link" in _failed_checks(report, "A1")
    with pytest.raises(generator.PolicyGenerationError):
        generator.verify_source_snapshot(snapshot, json.loads(policy_raw), raw=raw)


def test_generator_cli_verify_blocks_hostile_roots(tmp_path, capsys):
    target = tmp_path / "x.json"
    for raw in (b"[]", b'{"a":1,"a":2}', b"null"):
        target.write_bytes(raw)
        assert generator.main(["verify", "--policy-file", str(target)]) == 2
        assert json.loads(capsys.readouterr().out)["code"] == "artifact_not_object"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.pop("policy_id"),
        lambda p: p.__setitem__("policy_version", ""),
        lambda p: p.__setitem__("instrument_evidence", {"not": "a list"}),
        lambda p: p["instrument_evidence"].__setitem__(0, "row"),
        lambda p: p.__setitem__("generation", None),
    ],
)
def test_malformed_previous_policy_is_blocked(calendar, mutate):
    policy_raw, snapshot_raw, _ = _build(calendar)
    previous = json.loads(_previous(active_ids=(1,), calendar=calendar))
    mutate(previous)
    with pytest.raises(verifier.AuditBlocked, match="previous_policy_invalid"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            previous_raw=json.dumps(previous).encode(),
        )


# ── Round2 F2: calendar/deadline continuity ────────────────────────────────
@pytest.mark.parametrize(
    "field,value",
    [
        ("timezone", "America/Chicago"),
        ("source_reference", "other-reference"),
        ("valid_through", "2027-12-31T18:05:00-05:00"),  # same instant, other text
        ("valid_through", "2028-01-03T23:05:00+00:00"),
        ("calendar_digest", "0" * 64),
    ],
)
def test_previous_continuity_is_exact_text(calendar, field, value):
    policy_raw, snapshot_raw, _ = _build(calendar)
    previous = json.loads(_previous(active_ids=(1,), calendar=calendar))
    previous[field] = value
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        previous_raw=json.dumps(previous).encode(),
    )
    assert "calendar_identical_to_previous" in _failed_checks(report, "A1")
    previous.pop(field)
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        previous_raw=json.dumps(previous).encode(),
    )
    assert "calendar_identical_to_previous" in _failed_checks(report, "A1")


@pytest.mark.parametrize(
    "mutate,check",
    [
        (
            lambda p: p.__setitem__("valid_through", "2028-01-03T23:05:00+00:00"),
            "valid_through_equals_last_deadline",
        ),
        (
            lambda p: p.__setitem__("valid_through", "2027-12-31T23:05:00"),
            "valid_through_equals_last_deadline",
        ),
        (
            lambda p: p.__setitem__("publication_state", "draft"),
            "publication_state_approved",
        ),
        (lambda p: p.__setitem__("timezone", "UTC"), "timezone_new_york"),
        (lambda p: p.__setitem__("source_reference", " "), "source_reference_present"),
    ],
)
def test_policy_state_timezone_reference_and_deadline_are_checked(
    calendar, mutate, check
):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    mutate(policy)
    generation = policy["generation"]
    generation["policy_hash"] = policy_content_digest(policy)
    generation["generation_sha256"] = generation_metadata_digest(generation)
    report = verifier.audit(generator.canonical_json(policy), snapshot_raw=snapshot_raw)
    assert check in _failed_checks(report, "A1")


def test_valid_through_equal_instant_other_offset_matches_deadline(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    policy = json.loads(policy_raw)
    policy["valid_through"] = "2027-12-31T18:05:00-05:00"
    assert verifier._deadline_check(policy) is True


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
def test_ceiling_bounds_structural_daily_not_active(calendar):
    """Reviewer repro: D=8, P=9 — a ceiling of 8 fails even though D <= 8."""
    policy_raw, snapshot_raw, _ = _build(calendar)
    over = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(structural_daily_ceiling=8),
    )
    assert _failed_checks(over, "A4") == ["structural_daily_within_ceiling"]
    assert over["details"]["A4"]["active_daily_count"] == 8
    exact = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(structural_daily_ceiling=9),
    )
    assert exact["gates"]["A4"]["status"] == "PASS"
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(structural_baseline=5103),
    )
    assert _failed_checks(report, "A4") == ["structural_baseline_accepted"]
    delta = report["details"]["A4"]["structural_delta"]
    assert delta == 11 - 5103
    acknowledged = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(structural_baseline=5103, accepted_structural_delta=delta),
    )
    assert acknowledged["gates"]["A4"]["status"] == "PASS"
    # An accepted baseline delta never raises the ceiling on P.
    both = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        config_raw=_config(
            structural_daily_ceiling=8,
            structural_baseline=5103,
            accepted_structural_delta=delta,
        ),
    )
    assert _failed_checks(both, "A4") == ["structural_daily_within_ceiling"]


@pytest.mark.parametrize(
    "overrides,code",
    [
        ({"active_ceiling": 5103}, "audit_config_ceiling_key_retired"),
        ({"structural_daily_ceiling": None}, "audit_config_ceiling_invalid"),
        ({"structural_daily_ceiling": True}, "audit_config_ceiling_invalid"),
        ({"structural_daily_ceiling": -1}, "audit_config_ceiling_invalid"),
        ({"structural_daily_ceiling": 5103.0}, "audit_config_ceiling_invalid"),
    ],
)
def test_ceiling_config_is_renamed_and_strict(calendar, overrides, code):
    policy_raw, snapshot_raw, _ = _build(calendar)
    with pytest.raises(verifier.AuditBlocked, match=code):
        verifier.audit(
            policy_raw, snapshot_raw=snapshot_raw, config_raw=_config(**overrides)
        )
    config = json.loads(_config())
    config.pop("structural_daily_ceiling")
    with pytest.raises(verifier.AuditBlocked, match="audit_config_ceiling_invalid"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )


@pytest.mark.parametrize(
    "patch,check",
    [
        ("p_outside_b", "structural_daily_subset_of_baseline"),
        ("b_minus_p_claim", "baseline_first_failure_closure"),
    ],
)
def test_a4_closure_detects_inconsistent_populations(
    calendar, monkeypatch, patch, check
):
    policy_raw, snapshot_raw, _ = _build(calendar)
    original = verifier.Catalog.v1_structural_sets
    if patch == "p_outside_b":

        def shrunk(self):
            total, daily = original(self)
            return total, daily - {str(uuid.UUID(int=1))}
    else:

        def shrunk(self):  # B gains UUID 14, whose first failure is activity.*
            total, daily = original(self)
            return total, daily | {str(uuid.UUID(int=14))}

    monkeypatch.setattr(verifier.Catalog, "v1_structural_sets", shrunk)
    report = verifier.audit(policy_raw, snapshot_raw=snapshot_raw)
    assert check in _failed_checks(report, "A4")


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
    # MMF included: A8 corroborates every ACTIVE at the capture instant.
    assert report["details"]["A8"]["outcomes"] == {
        "matched": 9,
        **{code: 0 for code in verifier.SEC_CODES},
    }
    assert report["details"]["A8"]["exclusions"] == {
        "by_code": {code: 0 for code in verifier.SEC_CODES},
        "c_size": 9,
        "bound": 0,
        "stale": 0,
        "missing": 0,
        "integrity": 0,
    }
    assert "gap_ceiling" not in report["details"]["A8"]
    assert "conflict_ceiling" not in report["details"]["A8"]
    assert set(report["details"]) == set(
        verifier.AUDIT_CONTRACT["dossier"]["detail_keys"]
    )
    canary = _canary_of(report, policy_raw, config_raw)
    assert canary["selection_sha256"] == report["details"]["canary_selection_sha256"]
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
        live=_live(rows, cohort=cohort),
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
    configs = Path(verifier.__file__).parents[1] / "configs"
    raw = (configs / "nav_identity_audit_v3.json").read_bytes()
    config = verifier._load_config(raw)
    assert (
        config["builder"]["light_revision"]
        == "aeb5933754f46af997198a0ab82c14e108a4553a"
    )
    assert config["builder"]["margin_fraction"] == "1/10"
    assert config["sec"]["max_synced_age_days"] == 7
    assert config["sec"]["exclusion_fraction"] == {"numerator": 1, "denominator": 10}
    assert config["sec"]["conflict_ceiling"] == 0
    assert "gap_ceiling" not in config["sec"]
    assert config["audit_contract_version"] == verifier.AUDIT_CONTRACT_VERSION
    assert config["audit_contract_version"] == "nav-identity-audit-contract-v3-round7"
    assert config["audit_config_version"] == "nav-identity-audit-config-v3"
    assert config["canary_salt"] == "nav-policy-v3-canary-2026-09-25"
    assert operator.AUDIT_CONFIG.name == "nav_identity_audit_v3.json"
    # The v2 config stays only as a historical, non-publishable record.
    with pytest.raises(verifier.AuditBlocked):
        verifier._load_config((configs / "nav_identity_audit_v2.json").read_bytes())


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


# ── identity v3: SEC classified independently at generated_at, A8 at capture ─
FRESH_AT_TAU = OBSERVED - dt.timedelta(days=1)
OLD_AT_TAU = OBSERVED - dt.timedelta(days=8)
CONTROL = entity(900)


def _sec_audit(
    calendar,
    subject_kwargs=None,
    sec=(),
    *,
    config=None,
    captured=CAPTURED,
    extra_entities=(),
    control=True,
):
    """UUID 1 against the given SEC rows (+ a corroborated control UUID 900).

    The capture carries exactly the policy's four-source snapshot, so A5 sees
    no drift and A8 re-judges every ACTIVE at ``captured``.
    """
    entities = [entity(1, **(subject_kwargs or {})), *extra_entities]
    rows_sec = list(sec)
    if control:
        entities.append(CONTROL)
        rows_sec.append(sec_row(900, synced=FRESH_AT_TAU))
    policy_raw, snapshot_raw, rows = _build(calendar, entities, extra={"sec": rows_sec})
    live = _live(rows, cohort=[])
    live["captured_at"] = captured
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=live,
        config_raw=config or _config(),
    )
    return report, json.loads(policy_raw)


def _outcome(report):
    return {k: v for k, v in report["details"]["A8"]["outcomes"].items() if v}


SEC_MATRIX = [
    ([sec_row(1, synced=FRESH_AT_TAU)], None, None),
    ([sec_row(1, synced=FRESH_AT_TAU)], {"class_id": "C000000001"}, None),
    (
        [sec_row(1, class_id=" c000000001 ", ticker="t1", synced=FRESH_AT_TAU)],
        None,
        None,
    ),
    ([], {"class_id": "S000000001"}, "sec.declared_class_invalid"),
    (
        [sec_row(1, synced=FRESH_AT_TAU)],
        {"class_id": "C1"},
        "sec.declared_class_invalid",
    ),
    (
        [sec_row(1, class_id="S000000001:T1", synced=FRESH_AT_TAU)],
        None,
        "sec.poisoned_mapping",
    ),
    ([sec_row(1, series="SX", synced=FRESH_AT_TAU)], None, "sec.poisoned_mapping"),
    (
        [
            sec_row(1, synced=FRESH_AT_TAU),
            sec_row(2, class_id="C2", ticker="T1", synced=FRESH_AT_TAU),
        ],
        None,
        "sec.poisoned_mapping",
    ),
    ([sec_row(1, series="S000000009", synced=FRESH_AT_TAU)], None, "sec.contradiction"),
    (
        [sec_row(1, synced=FRESH_AT_TAU)],
        {"class_id": "C000000002"},
        "sec.contradiction",
    ),
    (
        [
            sec_row(1, synced=FRESH_AT_TAU),
            sec_row(1, ticker="OTHER", synced=FRESH_AT_TAU),
        ],
        {"class_id": "C000000001"},
        "sec.contradiction",
    ),
    (
        [
            sec_row(1, synced=FRESH_AT_TAU),
            sec_row(2, series="S000000001", ticker="T1", synced=FRESH_AT_TAU),
        ],
        None,
        "sec.ambiguous",
    ),
    (
        [sec_row(1, synced=FRESH_AT_TAU), sec_row(1, synced=FRESH_AT_TAU)],
        None,
        "sec.ambiguous",
    ),
    ([sec_row(1, class_id=None, synced=FRESH_AT_TAU)], None, "sec.incomplete"),
    ([sec_row(1, class_id="", synced=FRESH_AT_TAU)], None, "sec.incomplete"),
    (
        [
            sec_row(1, synced=FRESH_AT_TAU),
            sec_row(2, series=None, ticker="T1", synced=FRESH_AT_TAU),
        ],
        None,
        "sec.incomplete",
    ),
    # A stale row of a reused ticker never blocks the fresh correct mapping.
    (
        [sec_row(1, synced=FRESH_AT_TAU), sec_row(2, ticker="T1", synced=OLD_AT_TAU)],
        None,
        None,
    ),
    ([sec_row(1, synced=OLD_AT_TAU)], None, "sec.stale"),
    ([], None, "sec.missing"),
    ([sec_row(2, series="S000000001", synced=FRESH_AT_TAU)], None, "sec.missing"),
]


@pytest.mark.parametrize("sec,kwargs,code", SEC_MATRIX)
def test_sec_exclusions_recomputed_independently_and_bounded_by_ceilings(
    calendar, sec, kwargs, code
):
    report, policy = _sec_audit(calendar, kwargs, sec)
    gates = _gates(report)
    # Generator and independent auditor agree on the SEC first failure.
    for gate in ("A1", "A2", "A4", "A5", "A6"):
        assert gates[gate] == "PASS", (gate, report["gates"][gate])
    first = report["counts"]["identity_first_failure"]
    assert first == ({code: 1} if code else {})
    assert policy["generation"]["counts"]["identity_first_failure"] == first
    exclusions = report["details"]["A8"]["exclusions"]
    assert sum(exclusions["by_code"].values()) == (1 if code else 0)
    # N = subject + control = 2 funds at the SEC stage, B = 2 // 10 = 0.
    assert (exclusions["c_size"], exclusions["bound"]) == (2, 0)
    if code is None:
        assert gates["A8"] == "PASS" and _outcome(report) == {"matched": 2}
    elif code == "sec.stale":
        assert (exclusions["stale"], exclusions["missing"]) == (1, 0)
        assert _failed_checks(report, "A8") == ["sec_stale_within_bound"]
        assert _outcome(report) == {"matched": 1}
    elif code == "sec.missing":
        assert (exclusions["stale"], exclusions["missing"]) == (0, 1)
        assert _failed_checks(report, "A8") == ["sec_missing_within_bound"]
    else:
        # Any integrity exclusion (incomplete included) fails at ceiling 0.
        assert code in verifier.SEC_INTEGRITY
        assert exclusions["integrity"] == 1 and gates["A8"] == "FAIL"
        assert _failed_checks(report, "A8") == ["sec_integrity_zero"]
    a4 = report["details"]["A4"]
    assert a4["sec_first_failure"] == ({code: 1} if code else {})
    if code:
        dossier_private = report["differences"]["sec"]["excluded_at_generation"]
        assert dossier_private == {str(uuid.UUID(int=1)): code}


def test_generator_and_auditor_sec_judges_agree_on_random_catalogs():
    """Two independent implementations of the written rule never diverge."""
    rng = random.Random(20260925)
    classes = ["C000000001", "C000000002", "c000000003", "C12", "S000000001", "", None]
    series = ["S000000001", "S000000002", "SX", "", None]
    tickers = ["T1", "t1 ", "T2", "", None]
    ages = [
        dt.timedelta(0),
        dt.timedelta(days=1),
        dt.timedelta(days=7),
        dt.timedelta(days=7, microseconds=1),
        dt.timedelta(days=30),
    ]
    for _ in range(3000):
        rows = [
            sec_row(
                0,
                class_id=rng.choice(classes),
                series=rng.choice(series),
                ticker=rng.choice(tickers),
                synced=OBSERVED - rng.choice(ages),
            )
            for _ in range(rng.randint(1, 5))
        ]
        # An unrelated fresh anchor keeps the source itself fresh (no abort).
        rows.append(sec_row(9, synced=OBSERVED - dt.timedelta(days=1)))
        rng.shuffle(rows)
        canonical = generator.canonical_source_rows(rows, "sec")
        declared = rng.choice(["C000000001", "C000000002", "C1", None, None])
        index = generator._SecIndex(canonical, OBSERVED)
        judge = verifier.SecJudge(canonical)
        expected = index.first_failure("T1", "S000000001", declared)
        actual, _instant = judge.judge("T1", "S000000001", declared, OBSERVED)
        assert actual == expected, (rows, declared)


@pytest.mark.parametrize("mode", ["over_exclusion", "under_exclusion"])
def test_generator_sec_mutation_is_caught_by_the_independent_audit(
    calendar, monkeypatch, mode
):
    original = generator._SecIndex.first_failure
    if mode == "over_exclusion":

        def mutated(self, ticker, series, declared):
            return (
                "sec.missing"
                if ticker == "T1"
                else original(self, ticker, series, declared)
            )
    else:

        def mutated(self, ticker, series, declared):
            return None

    monkeypatch.setattr(generator._SecIndex, "first_failure", mutated)
    sec = [sec_row(1, synced=FRESH_AT_TAU)] if mode == "over_exclusion" else []
    report, policy = _sec_audit(calendar, None, sec)
    operator._policy(policy)  # the mutated artifact is internally consistent
    assert {"lifecycle_equal", "reason_counts_equal", "active_set_equal"} <= set(
        _failed_checks(report, "A5")
    )
    if mode == "under_exclusion":
        assert "active_passes_every_gate" in _failed_checks(report, "A6")
    assert verifier.verdict_of(report, strict=False) is False


def test_auditor_sec_mutation_disagrees_with_a_correct_generator(calendar, monkeypatch):
    report, _ = _sec_audit(calendar, None, [sec_row(1, synced=OLD_AT_TAU)])
    assert report["gates"]["A5"]["status"] == "PASS"
    monkeypatch.setattr(
        verifier.SecJudge, "judge", lambda self, t, s, k, at: (None, at)
    )
    report, _ = _sec_audit(calendar, None, [sec_row(1, synced=OLD_AT_TAU)])
    assert {"lifecycle_equal", "reason_counts_equal"} <= set(
        _failed_checks(report, "A5")
    )


def _bound_audit(
    calendar,
    *,
    active,
    stale=0,
    missing=0,
    incomplete=0,
    mmf=0,
    non_sec_unknown=0,
    inactive=0,
):
    """``active`` corroborated funds (the first ``mmf`` of them MMF) plus
    ``stale``/``missing``/``incomplete`` SEC exclusions, non-SEC UNKNOWN and
    INACTIVE funds that never reach the SEC stage. Returns (report, policy)."""
    entities, sec, n = [], [], 1
    for index in range(active):
        entities.append(entity(n, fund_type="mmf" if index < mmf else "etf"))
        sec.append(sec_row(n, synced=FRESH_AT_TAU))
        n += 1
    for _ in range(stale):
        entities.append(entity(n))
        sec.append(sec_row(n, synced=OLD_AT_TAU))
        n += 1
    for _ in range(missing):
        entities.append(entity(n))
        n += 1
    for _ in range(incomplete):
        entities.append(entity(n))
        sec.append(sec_row(n, class_id=None, synced=FRESH_AT_TAU))
        n += 1
    for _ in range(non_sec_unknown):
        entities.append(entity(n, active=None))  # activity.unknown, never SEC
        sec.append(sec_row(n, synced=OLD_AT_TAU))  # would be stale if judged
        n += 1
    for _ in range(inactive):
        entities.append(only(entity(n, active=False), fund=False, registry=False))
        n += 1
    # One fresh row no fund maps to: the newest source row is fresh even with
    # no ACTIVE, so only A8 decides (a stale source would abort generation).
    sec.append(sec_row(n + 5000, synced=FRESH_AT_TAU))
    policy_raw, snapshot_raw, rows = _build(calendar, entities, extra={"sec": sec})
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, cohort=[]),
        config_raw=_config(),
    )
    for gate in ("A1", "A2", "A4", "A5", "A6"):
        if active or gate != "A2":  # A2 needs a non-empty ACTIVE set
            assert report["gates"][gate]["status"] == "PASS", (
                gate,
                report["gates"][gate],
            )
    return report, json.loads(policy_raw)


@pytest.mark.parametrize(
    "counts,bound,failed",
    [
        # N in 1..9: B = 0, one stale or one missing fails; zero passes.
        ({"active": 9}, 0, []),
        ({"active": 8, "stale": 1}, 0, ["sec_stale_within_bound"]),
        ({"active": 8, "missing": 1}, 0, ["sec_missing_within_bound"]),
        ({"active": 1}, 0, []),
        # N = 10: B = 1 (exact floor), 1 passes and 2 fails, per family.
        ({"active": 9, "stale": 1}, 1, []),
        ({"active": 8, "stale": 2}, 1, ["sec_stale_within_bound"]),
        ({"active": 9, "missing": 1}, 1, []),
        ({"active": 8, "missing": 2}, 1, ["sec_missing_within_bound"]),
        # N = 19: B = 1; N = 20: B = 2.
        ({"active": 18, "stale": 1}, 1, []),
        ({"active": 17, "stale": 2}, 1, ["sec_stale_within_bound"]),
        ({"active": 18, "stale": 2}, 2, []),
        ({"active": 17, "stale": 3}, 2, ["sec_stale_within_bound"]),
        # S = B and M = B at once pass (S + M = 2B: no combined ceiling).
        ({"active": 16, "stale": 2, "missing": 2}, 2, []),
        # S = B + 1 with M = 0, and M = B + 1 with S = 0, fail independently.
        ({"active": 17, "stale": 3, "missing": 0}, 2, ["sec_stale_within_bound"]),
        ({"active": 17, "stale": 0, "missing": 3}, 2, ["sec_missing_within_bound"]),
        (
            {"active": 14, "stale": 3, "missing": 3},
            2,
            ["sec_missing_within_bound", "sec_stale_within_bound"],
        ),
        # Integrity K = 1 fails even with stale and missing within B.
        (
            {"active": 16, "stale": 1, "missing": 1, "incomplete": 1},
            1,
            ["sec_integrity_zero"],
        ),
        # MMF ACTIVE counts in N: N = 10 (1 MMF) gives B = 1.
        ({"active": 9, "mmf": 1, "stale": 1}, 1, []),
        # Non-SEC UNKNOWN and INACTIVE never count: N = 9, B = 0.
        (
            {"active": 8, "stale": 1, "non_sec_unknown": 3, "inactive": 2},
            0,
            ["sec_stale_within_bound"],
        ),
        # N not a multiple of 10: N = 13 -> B = 1.
        ({"active": 12, "missing": 1}, 1, []),
        ({"active": 11, "missing": 2}, 1, ["sec_missing_within_bound"]),
        # N = 0 (nobody reaches SEC) never passes active_nonempty.
        (
            {"active": 0, "non_sec_unknown": 2},
            0,
            ["active_nonempty", "all_active_matched"],
        ),
        # ACTIVE = 0 with C > 0 also fails (and the exclusion is over B = 0).
        (
            {"active": 0, "stale": 2},
            0,
            ["active_nonempty", "all_active_matched", "sec_stale_within_bound"],
        ),
        # N = 1000, B = 100: stale and missing are never summed nor offset.
        (
            {"active": 799, "stale": 100, "missing": 101},
            100,
            ["sec_missing_within_bound"],
        ),
        (
            {"active": 799, "stale": 101, "missing": 100},
            100,
            ["sec_stale_within_bound"],
        ),
        ({"active": 800, "stale": 100, "missing": 100}, 100, []),
    ],
)
def test_sec_exclusions_proportional_bound_boundaries(calendar, counts, bound, failed):
    report, policy = _bound_audit(calendar, **counts)
    exclusions = report["details"]["A8"]["exclusions"]
    stale, missing = counts.get("stale", 0), counts.get("missing", 0)
    integrity = counts.get("incomplete", 0)
    c_size = counts["active"] + stale + missing + integrity
    assert exclusions == {
        "by_code": {
            code: {
                "sec.stale": stale,
                "sec.missing": missing,
                "sec.incomplete": integrity,
            }.get(code, 0)
            for code in verifier.SEC_CODES
        },
        "c_size": c_size,
        "bound": bound,
        "stale": stale,
        "missing": missing,
        "integrity": integrity,
    }
    assert bound == c_size // 10
    assert _failed_checks(report, "A8") == failed
    assert report["gates"]["A8"]["status"] == ("FAIL" if failed else "PASS")
    # Conservation: N = A + S + M + K and N = structural - claim failures.
    counts_doc = policy["generation"]["counts"]
    assert report["counts"]["fund_status"].get("ACTIVE", 0) == counts["active"]
    assert c_size == counts_doc["active"] + stale + missing + integrity
    assert (
        counts_doc["structural_pre_claims"] - counts_doc["structural_claim_failures"]
        == c_size
    )
    assert counts_doc["structural_sec_failures"] == stale + missing + integrity


def test_round6_gap_ceiling_semantics_no_longer_pass(calendar):
    """23 missing funds passed Round6 A8; with N = 24 (B = 2) they now FAIL."""
    report, _ = _bound_audit(calendar, active=1, missing=23)
    assert report["details"]["A8"]["exclusions"]["bound"] == 2
    assert _failed_checks(report, "A8") == ["sec_missing_within_bound"]


@pytest.mark.parametrize(
    "mutate",
    [
        # missing -> stale and stale -> missing (totals preserved).
        lambda e: e.update(
            by_code={**e["by_code"], "sec.missing": 0, "sec.stale": 1},
            missing=0,
            stale=1,
        ),
        lambda e: e.update(bound=0),
        lambda e: e.update(c_size=11),
        lambda e: e.update(integrity=1),
        lambda e: e.update(missing=True),
        lambda e: e.update(by_code={**e["by_code"], "sec.unknown_code": 0}),
        lambda e: e["by_code"].pop("sec.ambiguous"),
    ],
    ids=[
        "missing_to_stale",
        "bound_zero",
        "c_size_plus_one",
        "integrity_one",
        "missing_bool",
        "unknown_sec_code",
        "missing_code_key",
    ],
)
def test_operator_recounts_the_exclusions_from_the_policy(calendar, mutate):
    """Pure operator recount (N, B, S, M, K, F) against the policy; the dossier's
    arithmetic and booleans are never trusted."""
    report, policy = _bound_audit(calendar, active=9, missing=1)
    assert report["gates"]["A8"]["status"] == "PASS"
    operator._validate_sec_exclusions(report, policy)
    broken = copy.deepcopy(report)
    mutate(broken["details"]["A8"]["exclusions"])
    with pytest.raises(ValueError, match="audit_dossier_invalid"):
        operator._validate_sec_exclusions(broken, policy)


@pytest.mark.parametrize(
    "mutate",
    [
        # stale -> missing in the POLICY counts only (evidence untouched).
        lambda c: c["identity_first_failure"].update(
            {"sec.missing": 1, "sec.stale": 0}
        ),
        # Inflate A (the denominator) without evidence behind it.
        lambda c: c.update(active=c["active"] + 5),
        lambda c: c["fund_status"].update(ACTIVE=c["fund_status"]["ACTIVE"] + 5),
        lambda c: c.update(structural_sec_failures=c["structural_sec_failures"] + 1),
        lambda c: c.update(
            structural_daily_sec_failures=c["structural_daily_sec_failures"] + 1
        ),
        lambda c: c["identity_first_failure"].update({"sec.unknown_code": 1}),
        lambda c: c.update(active=True),
    ],
    ids=[
        "policy_stale_to_missing",
        "policy_active_inflated",
        "policy_status_active_inflated",
        "policy_sec_total_inconsistent",
        "policy_daily_sec_inconsistent",
        "policy_unknown_sec_code",
        "policy_active_bool",
    ],
)
def test_operator_rejects_incoherent_policy_counts(calendar, mutate):
    report, policy = _bound_audit(calendar, active=9, stale=1)
    operator._validate_sec_exclusions(report, policy)
    broken = copy.deepcopy(policy)
    counts = broken["generation"]["counts"]
    counts["identity_first_failure"] = dict(counts["identity_first_failure"])
    mutate(counts)
    with pytest.raises(
        ValueError, match="audit_policy_counts_invalid|audit_dossier_invalid"
    ):
        operator._validate_sec_exclusions(report, broken)


def test_auditor_rejects_a_policy_reason_swap_against_the_source(calendar):
    """policy + digests re-derived with stale -> missing: the independent
    recomputation from the source refuses it (A2/A5), never trusting the policy."""
    policy_raw, snapshot_raw, rows = _build(
        calendar,
        [entity(n) for n in range(1, 10)],
        extra={
            "sec": [sec_row(n, synced=FRESH_AT_TAU) for n in range(1, 9)]
            + [sec_row(9, synced=OLD_AT_TAU)]
        },
    )
    policy = json.loads(policy_raw)
    assert policy["generation"]["counts"]["identity_first_failure"] == {"sec.stale": 1}
    policy["generation"]["counts"]["identity_first_failure"] = {"sec.missing": 1}
    report = verifier.audit(
        _rehash_counts(policy),
        snapshot_raw=snapshot_raw,
        live=_live(rows, cohort=[]),
        config_raw=_config(),
    )
    assert report["gates"]["A1"]["checks"]["generation_digest"] is True
    assert "generation_counts_exact" in _failed_checks(report, "A2")
    assert verifier.verdict_of(report, strict=False) is False


def test_exclusion_projection_mismatch_is_a_static_block(calendar, monkeypatch):
    """Two projections of one verdict disagreeing is a defect, never a FAIL."""
    original = verifier._expected_counts

    def shifted(*args, **kwargs):
        counts = original(*args, **kwargs)
        counts["structural_sec_failures"] += 1
        return counts

    monkeypatch.setattr(verifier, "_expected_counts", shifted)
    with pytest.raises(verifier.AuditBlocked, match="a8_exclusion_projection_mismatch"):
        _bound_audit(calendar, active=9, stale=1)


def test_sec_ageing_between_generation_and_capture_fails_a8(calendar):
    """Fresh at generated_at, older than 7 days at the capture instant."""
    edge = OBSERVED - dt.timedelta(days=7) + dt.timedelta(minutes=1)
    report, policy = _sec_audit(calendar, None, [sec_row(1, synced=edge)])
    assert _outcome(report) == {"matched": 1, "sec.stale": 1}
    assert _failed_checks(report, "A8") == ["all_active_matched"]
    assert report["gates"]["A5"]["status"] == "PASS"
    assert report["differences"]["sec"]["per_active_outcome"] == {
        str(uuid.UUID(int=1)): "sec.stale"
    }
    statuses = {r["fund_status"] for r in policy["instrument_evidence"]}
    assert statuses == {"ACTIVE"}  # the policy itself is never rewritten


@pytest.mark.parametrize(
    "synced,failed",
    [
        (dt.datetime(2026, 9, 17, 12, 5, tzinfo=dt.timezone.utc), []),  # exactly 7d
        (
            dt.datetime(2026, 9, 17, 12, 4, 59, 999999, tzinfo=dt.timezone.utc),
            ["all_active_matched"],
        ),
    ],
)
def test_a8_freshness_boundary_at_the_capture_instant(calendar, synced, failed):
    report, _ = _sec_audit(calendar, None, [sec_row(1, synced=synced)])
    assert _failed_checks(report, "A8") == failed
    freshness = report["details"]["A8"]["freshness"]
    assert freshness["decision_at"] == CAPTURED
    assert freshness["max_synced_age_days"] == 7
    if not failed:
        assert dt.datetime.fromisoformat(freshness["valid_until"]) == (
            synced + dt.timedelta(days=7)
        )


def test_a8_capture_before_a_row_fails_future_and_generation_order(calendar):
    earlier = "2026-09-24T11:00:00+00:00"
    report, _ = _sec_audit(
        calendar,
        None,
        [sec_row(1, synced=dt.datetime(2026, 9, 24, 11, 30, tzinfo=dt.timezone.utc))],
        captured=earlier,
    )
    assert "generation_precedes_capture" in _failed_checks(report, "A5")
    assert {"synced_not_in_future", "source_fresh_within_max_age"} <= set(
        _failed_checks(report, "A8")
    )


def test_sec_drift_between_generation_and_capture_fails_a5(calendar):
    policy_raw, snapshot_raw, rows = _build(calendar)
    live = _live(rows)
    live["sec"]["rows"][0]["synced_at"] += dt.timedelta(microseconds=1)
    live["sec"]["lineage"] = _lineage(live["sec"]["rows"])
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=live, config_raw=_config()
    )
    assert _failed_checks(report, "A5") == ["no_source_drift"]
    for gate in ("A7", "A8"):
        assert report["gates"][gate] == {
            "status": "NOT_EVALUATED",
            "code": "live_capture_absent_or_drifted",
            "checks": {},
        }


def test_a8_unrelated_poison_and_incomplete_rows_are_counted_not_fatal(calendar):
    report, _ = _sec_audit(
        calendar,
        None,
        [
            sec_row(1, synced=FRESH_AT_TAU),
            sec_row(7, class_id="S000000007:T7", synced=FRESH_AT_TAU),
            sec_row(8, series="", ticker="T8", synced=FRESH_AT_TAU),
        ],
    )
    assert report["gates"]["A8"]["status"] == "PASS"
    assert report["details"]["A8"]["invalid_source_rows"] == {
        "incomplete": 1,
        "poisoned": 1,
    }
    assert _outcome(report) == {"matched": 2}


def test_a8_never_passes_vacuously_without_active(calendar):
    report, _ = _sec_audit(
        calendar, {"active": None}, [sec_row(1, synced=FRESH_AT_TAU)], control=False
    )
    assert report["gates"]["A8"]["status"] == "FAIL"
    assert {"active_nonempty", "all_active_matched"} <= set(
        _failed_checks(report, "A8")
    )


@pytest.mark.parametrize(
    "mutate,code",
    [
        (lambda s: s.__setitem__("sec", []), "sec_source_empty"),
        (
            lambda s: s["sec"].append(
                {
                    "class_id": "C000000077",
                    "series_id": "S000000077",
                    "ticker": "T77",
                    "synced_at": "2026-09-24T12:00:00.000001+00:00",
                }
            ),
            "sec_source_future",
        ),
        (
            lambda s: s.__setitem__(
                "sec",
                [
                    dict(row, synced_at="2026-09-01T00:00:00.000000+00:00")
                    for row in s["sec"]
                ],
            ),
            "sec_source_stale",
        ),
    ],
)
def test_sec_source_defect_at_generated_at_blocks_the_audit(calendar, mutate, code):
    policy_raw, snapshot_raw, _ = _build(calendar)
    snapshot = json.loads(snapshot_raw)
    mutate(snapshot["sources"])
    with pytest.raises(verifier.AuditBlocked, match=code):
        verifier.audit(policy_raw, snapshot_raw=generator.canonical_json(snapshot))


@pytest.mark.parametrize(
    "mutate,code",
    [
        (
            lambda live: live["sec"]["rows"][0].__setitem__(
                "synced_at", dt.datetime(2026, 9, 23)
            ),
            "sec_timestamp_not_aware",
        ),
        (
            lambda live: live["sec"]["rows"][0].__setitem__("synced_at", "yesterday"),
            "sec_timestamp_not_canonical",
        ),
        (
            lambda live: live["sec"]["rows"][0].__setitem__("synced_at", None),
            "sec_timestamp_not_canonical",
        ),
        (
            lambda live: live["sec"]["rows"][0].__setitem__("ticker", 5),
            "sec_source_shape_invalid",
        ),
        (
            lambda live: live["sec"]["rows"][0].pop("synced_at"),
            "sec_source_shape_invalid",
        ),
        (
            lambda live: live["sec"]["lineage"].__setitem__("row_count", 7),
            "sec_lineage_mismatch",
        ),
        (
            lambda live: live["sec"]["lineage"].__setitem__(
                "max_synced_at", dt.datetime(2026, 9, 24, tzinfo=dt.timezone.utc)
            ),
            "sec_lineage_mismatch",
        ),
        (
            lambda live: live["sec"]["lineage"].__setitem__("max_synced_at", None),
            "sec_lineage_mismatch",
        ),
        (
            lambda live: live["sec"]["lineage"].pop("min_synced_at"),
            "sec_lineage_invalid",
        ),
        (
            lambda live: live["sec"]["lineage"].__setitem__("row_count", True),
            "sec_lineage_invalid",
        ),
        (
            lambda live: live.__setitem__(
                "sec", {"state": "unavailable", "code": "sec_relation_missing"}
            ),
            "sec_relation_missing",
        ),
        (
            lambda live: live.__setitem__(
                "sec", {"state": "unavailable", "code": "sec_privilege_missing"}
            ),
            "sec_privilege_missing",
        ),
        (
            lambda live: live.__setitem__(
                "sec", {"state": "failed", "code": "sec_row_ceiling_exceeded"}
            ),
            "sec_row_ceiling_exceeded",
        ),
    ],
)
def test_malformed_or_unavailable_sec_capture_is_drift_not_zero(calendar, mutate, code):
    policy_raw, snapshot_raw, rows = _build(calendar)
    live = _live(rows)
    mutate(live)
    report = verifier.audit(
        policy_raw, snapshot_raw=snapshot_raw, live=live, config_raw=_config()
    )
    captured = report["inputs"]["capture"]["sec"]
    assert (captured["state"] in ("invalid", "unavailable", "failed")) and (
        captured["code"] == code
    )
    assert captured["rows_sha256"] is None and captured["row_count"] is None
    assert report["inputs"]["capture"]["source_snapshot_sha256"] is None
    assert "no_source_drift" in _failed_checks(report, "A5")
    assert report["gates"]["A8"]["status"] == "NOT_EVALUATED"
    assert verifier.verdict_of(report, strict=True) is False
    # Without the snapshot file the lifecycle cannot be recomputed at all.
    with pytest.raises(verifier.AuditBlocked, match="sec_source_not_captured"):
        verifier.audit(policy_raw, live=live, config_raw=_config())


def test_sec_config_block_is_mandatory(calendar):
    policy_raw, snapshot_raw, _ = _build(calendar)
    config = json.loads(_config())
    config.pop("sec")
    with pytest.raises(verifier.AuditBlocked, match="audit_config_sec_invalid"):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("max_synced_age_days", 8, "audit_config_sec_freshness_invalid"),
        ("max_synced_age_days", 0, "audit_config_sec_freshness_invalid"),
        ("max_synced_age_days", True, "audit_config_sec_freshness_invalid"),
        ("max_synced_age_days", "7", "audit_config_sec_freshness_invalid"),
        ("max_synced_age_days", None, "audit_config_sec_freshness_invalid"),
        ("relation", "public.sec_fund_classes", "audit_config_sec_source_invalid"),
        (
            "relation",
            "public.fund_classes_latest_mv",
            "audit_config_sec_source_invalid",
        ),
        ("timestamp_column", "fetched_at", "audit_config_sec_source_invalid"),
        ("source_contract", "other", "audit_config_sec_source_invalid"),
        ("query_contract_sha256", "0" * 64, "audit_config_sec_source_invalid"),
        ("exclusion_fraction", None, "audit_config_sec_fraction_invalid"),
        ("exclusion_fraction", "1/10", "audit_config_sec_fraction_invalid"),
        ("exclusion_fraction", 0.1, "audit_config_sec_fraction_invalid"),
        ("exclusion_fraction", [1, 10], "audit_config_sec_fraction_invalid"),
        (
            "exclusion_fraction",
            {"numerator": 2, "denominator": 20},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 1, "denominator": 9},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 0, "denominator": 10},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": True, "denominator": 10},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 1.0, "denominator": 10},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": "1", "denominator": "10"},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 1},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 1, "denominator": 10, "minimum": 1},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 1, "denominator": 0},
            "audit_config_sec_fraction_invalid",
        ),
        (
            "exclusion_fraction",
            {"numerator": 1, "denominator": 10.0},
            "audit_config_sec_fraction_invalid",
        ),
        ("exclusion_fraction", True, "audit_config_sec_fraction_invalid"),
        ("conflict_ceiling", 1, "audit_config_sec_ceiling_invalid"),
        ("conflict_ceiling", 0.0, "audit_config_sec_ceiling_invalid"),
        ("conflict_ceiling", None, "audit_config_sec_ceiling_invalid"),
        ("conflict_ceiling", "0", "audit_config_sec_ceiling_invalid"),
        ("conflict_ceiling", False, "audit_config_sec_ceiling_invalid"),
        ("conflict_ceiling", None, "audit_config_sec_ceiling_invalid"),
    ],
)
def test_sec_config_is_pinned_to_the_contract(calendar, field, value, code):
    policy_raw, snapshot_raw, _ = _build(calendar)
    config = json.loads(_config())
    config["sec"][field] = value
    with pytest.raises(verifier.AuditBlocked, match=code):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )
    config["sec"].pop(field)
    with pytest.raises(verifier.AuditBlocked, match=code):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )


@pytest.mark.parametrize("value", [23, 25, 0, None, "23", True])
@pytest.mark.parametrize("with_fraction", [True, False])
def test_round6_gap_ceiling_key_is_retired_and_blocks(calendar, value, with_fraction):
    """A Round6 config never validates as Round7, even next to the fraction."""
    policy_raw, snapshot_raw, _ = _build(calendar)
    config = json.loads(_config())
    config["sec"]["gap_ceiling"] = value
    if not with_fraction:
        config["sec"].pop("exclusion_fraction")
    with pytest.raises(
        verifier.AuditBlocked, match="audit_config_sec_gap_ceiling_retired"
    ):
        verifier.audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            config_raw=json.dumps(config).encode(),
        )
    with pytest.raises(ValueError, match="audit_config_sec_invalid"):
        operator._validate_audit_config(json.dumps(config).encode())


def test_round6_repository_config_bytes_are_rejected_by_both(calendar):
    """The exact Round6 v3 config (contract round6 + gap 23) blocks everywhere."""
    config = json.loads(_config())
    config["audit_contract_version"] = "nav-identity-audit-contract-v3-round6"
    config["sec"].pop("exclusion_fraction")
    config["sec"]["gap_ceiling"] = 23
    raw = json.dumps(config).encode()
    with pytest.raises(verifier.AuditBlocked, match="audit_contract_version_invalid"):
        verifier._load_config(raw)
    with pytest.raises(ValueError, match="audit_config_invalid"):
        operator._validate_audit_config(raw)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(exclusion_fraction={"numerator": 1, "denominator": 9}),
        lambda s: s.update(exclusion_fraction={"numerator": True, "denominator": 10}),
        lambda s: s.update(exclusion_fraction={"numerator": 1, "denominator": 10.0}),
        lambda s: s.update(exclusion_fraction="1/10"),
        lambda s: s.update(exclusion_fraction=0.1),
        lambda s: s.update(exclusion_fraction=None),
        lambda s: s.update(exclusion_fraction={"numerator": 2, "denominator": 20}),
        lambda s: s.update(exclusion_fraction={"numerator": 1, "denominator": 0}),
        lambda s: s.update(
            exclusion_fraction={"numerator": 1, "denominator": 10, "extra": 0}
        ),
        lambda s: s.pop("exclusion_fraction"),
        lambda s: s.update(conflict_ceiling=False),
        lambda s: s.update(conflict_ceiling=None),
        lambda s: s.update(conflict_ceiling=1),
        lambda s: s.update(gap_ceiling=25),
        lambda s: s.update(max_synced_age_days=8),
        lambda s: s.update(relation="public.sec_fund_classes"),
    ],
)
def test_operator_validates_the_audit_config_independently(mutate):
    config = json.loads(_config())
    operator._validate_audit_config(json.dumps(config).encode())
    mutate(config["sec"])
    with pytest.raises(ValueError, match="audit_config_sec_invalid"):
        operator._validate_audit_config(json.dumps(config).encode())


def test_prohibited_sec_sources_are_absent_from_runtime_code():
    """A8 reads only public.sec_company_tickers_mf; no fallback exists."""
    sources = [
        Path(verifier.__file__).read_text(encoding="utf-8"),
        (Path(verifier.__file__).parent / "nav_identity_audit_contract.py").read_text(
            encoding="utf-8"
        ),
        Path(operator.__file__).read_text(encoding="utf-8"),
    ]
    for text in sources:
        assert "fund_classes_latest_mv" not in text
        assert "sec_fund_classes" not in text
    assert verifier.SEC_SQL.count("FROM public.sec_company_tickers_mf") == 1
    assert "updated_at AS synced_at" in verifier.SEC_SQL
    assert "LIMIT 100001" in verifier.SEC_SQL


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
    assert bundle["cohort"]["row_count"] == 18 and bundle["sec"]["row_count"] == 15
    assert bundle["sec"]["relation"] == "public.sec_company_tickers_mf"
    assert bundle["sec"]["timestamp_column"] == "updated_at"
    assert bundle["kind"] == "nav-identity-audit-capture-v3-round7"
    assert bundle["sec"]["rows"] == json.loads(snapshot_raw)["sources"]["sec"]
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
            live=_live(rows, cohort=cohort),
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

    # Same A8 outcomes, different SEC snapshot rows → different pinned digests.
    def with_sec(extra_rows):
        raw_policy, raw_snapshot, sec_rows = _build(
            calendar, entities, extra={"sec_extra": extra_rows}
        )
        return verifier.audit(
            raw_policy,
            snapshot_raw=raw_snapshot,
            live=_live(sec_rows, cohort=[]),
            config_raw=_config(),
        )

    sec_a, sec_b = with_sec([]), with_sec([sec_row(77)])
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
        ("kind_round1", "capture_contract_invalid"),
        ("kind_v2_round2", "capture_contract_invalid"),
        ("sec_time_text", "capture_bundle_invalid"),
        ("query", "capture_cohort_query_mismatch"),
        ("sec_relation", "capture_sec_contract_invalid"),
        ("sec_timestamp_column", "capture_sec_contract_invalid"),
        ("sec_query", "capture_sec_contract_invalid"),
        ("sec_lineage", "capture_bundle_invalid"),
        ("duplicate_key", "capture_not_json"),
    ],
)
def test_capture_bundle_tamper_is_blocked(calendar, tamper, code):
    policy_raw, snapshot_raw, rows = _build(calendar)
    config = json.loads(_config())
    raw = verifier.capture_bytes(_live(rows), config)
    bundle = json.loads(raw)
    if tamper == "bytes":
        tampered = raw.replace(b"\n", b" \n")
    elif tamper == "duplicate_key":
        tampered = raw.replace(
            b'{"audit_contract_version"', b'{"kind":"x","audit_contract_version"', 1
        )
    else:
        sec = bundle["sec"]
        if tamper == "kind_round1":
            bundle["kind"] = "nav-identity-audit-capture-v2"
        elif tamper == "kind_v2_round2":
            bundle["kind"] = "nav-identity-audit-capture-v2-round2"
        elif tamper == "sec_time_text":
            sec["rows"][0]["synced_at"] = sec["rows"][0]["synced_at"].replace(
                ".000000+00:00", "+00:00"
            )
        elif tamper == "sec_relation":
            sec["relation"] = "public.sec_fund_classes"
        elif tamper == "sec_timestamp_column":
            sec["timestamp_column"] = "fetched_at"
        elif tamper == "sec_query":
            sec["query_sha256"] = "0" * 64
        elif tamper == "sec_lineage":
            sec["lineage"]["row_count"] = sec["lineage"]["row_count"] + 1
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
        live=_live(rows, cohort=list(reversed(cohort)), sec=list(reversed(rows[3]))),
        previous_raw=previous,
        config_raw=config,
    )
    assert _canary_of(second, policy_raw, config) == manifest
    assert manifest["size"] == 20 and manifest["cohort_size"] == 40
    assert manifest["salt"] == "nav-policy-v3-canary-2026-09-25"


def test_canary_never_contains_sec_excluded_funds(calendar):
    """SEC-excluded funds are UNKNOWN, so they never reach the eligible cohort."""
    entities = [
        entity(n, fund_type="etf" if n % 2 else "mutual_fund") for n in range(1, 45)
    ]
    excluded = {41, 42, 43, 44}
    sec = [
        row for row in catalog(*entities)[3] if int(row["ticker"][1:]) not in excluded
    ]
    sec.append(sec_row(43, synced=OBSERVED - dt.timedelta(days=30)))  # stale
    policy_raw, snapshot_raw, rows = _build(calendar, entities, extra={"sec": sec})
    cohort = [
        {"instrument_id": uuid.UUID(int=n), "strategy_label": LABELS[1 + (n % 4)]}
        for n in range(1, 45)
    ]
    config = _config()
    report = verifier.audit(
        policy_raw,
        snapshot_raw=snapshot_raw,
        live=_live(rows, cohort=cohort),
        previous_raw=_previous(active_ids=(1,), calendar=calendar),
        config_raw=config,
    )
    assert set(_gates(report).values()) == {"PASS"}, _gates(report)
    assert report["details"]["A8"]["exclusions"]["by_code"]["sec.missing"] == 3
    assert report["details"]["A8"]["exclusions"]["by_code"]["sec.stale"] == 1
    manifest = _canary_of(report, policy_raw, config)
    excluded_ids = {str(uuid.UUID(int=n)) for n in excluded}
    assert not excluded_ids & set(manifest["allowlist"])
    assert manifest["cohort_size"] == 40
    assert report["details"]["A7"]["eligible_cohort_size"] == 40


# ── CLI and custody (POSIX) ─────────────────────────────────────────────────
def _custody(tmp_path):
    root = tmp_path / "custody"
    root.mkdir(mode=0o700)
    return root


def _cli_inputs(
    root, calendar, *, entities=None, config=None, live_kwargs=None, extra=None
):
    policy_raw, snapshot_raw, rows = _build(calendar, entities, extra=extra)
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
    assert dossier["audit_version"] == "nav-identity-audit-v3"
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
    extra = None
    if failure == "sec_contradiction":
        # Excluded at generation (UNKNOWN sec.contradiction): integrity ceiling 0.
        rich = catalog(*_rich_entities())[3]
        extra = {
            "sec": [
                sec_row(1, series="S000000099"),
                *[row for row in rich if row["ticker"] != "T1"],
            ]
        }
    elif failure == "insufficient_sleeve":
        config["builder"]["stage1_quotas"] = {"equity": 30}
    elif failure == "stale_sec":
        # Fresh at generated_at (12:00), older than 7 days at capture (12:05).
        edge = OBSERVED - dt.timedelta(days=7) + dt.timedelta(minutes=1)
        extra = {"sec_at": edge}
    base, _, capture = _cli_inputs(
        root, calendar, config=json.dumps(config).encode(), extra=extra
    )
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
        assert dossier["differences"]["sec"]["excluded_at_generation"] == {
            str(uuid.UUID(int=1)): "sec.contradiction"
        }
        assert dossier["differences"]["sec"]["per_active_outcome"] == {}
        assert dossier["details"]["A8"]["exclusions"]["integrity"] == 1
    if failure == "stale_sec":
        assert set(dossier["differences"]["sec"]["per_active_outcome"].values()) == {
            "sec.stale"
        }


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


# ── Round3 F5: non-object siblings at every nested path, through the CLIs ───
def _nested_paths(value, prefix=()):
    """Every object-key path (lists: their first element) of a JSON document."""
    if isinstance(value, dict):
        for key in sorted(value):
            yield (*prefix, key)
            yield from _nested_paths(value[key], (*prefix, key))
    elif isinstance(value, list) and value:
        yield (*prefix, 0)
        yield from _nested_paths(value[0], (*prefix, 0))


def _replaced(document, path, value):
    mutated = copy.deepcopy(document)
    target = mutated
    for step in path[:-1]:
        target = target[step]
    target[path[-1]] = value
    return mutated


def _cli_json(capsys, code: int) -> dict:
    printed = capsys.readouterr().out
    assert code in (0, 2, 3), printed
    return json.loads(printed)  # one static JSON line, never a traceback


@pytest.mark.skipif(os.name != "posix", reason="custody writer is POSIX-only")
@pytest.mark.parametrize("sibling", [[], 5], ids=["list", "number"])
def test_cli_nested_non_object_siblings_never_raise(
    calendar, tmp_path, capsys, sibling
):
    """[]/5 at every nested path of the canonical policy, snapshot and previous
    reaches the auditor CLI, the generator verify CLI and the operator parser
    as a static blocked/failed result (no AttributeError traceback)."""
    root = _custody(tmp_path)
    base, policy_raw, _ = _cli_inputs(root, calendar)
    policy = json.loads(policy_raw)
    snapshot = json.loads((root / "source.json").read_bytes())
    previous = json.loads((root / "v1.json").read_bytes())
    counter = iter(range(100_000))

    def write(document) -> Path:
        path = root / f"fuzz-{next(counter)}.json"
        path.write_bytes(generator.canonical_json(document))
        return path

    def audit_cli(args):
        output = root / f"fuzz-audit-{next(counter)}.json"
        return _cli_json(capsys, verifier.main([*args, "--output", str(output)]))

    def swap(args, flag, path):
        args = list(args)
        args[args.index(flag) + 1] = str(path)
        return args

    for path in _nested_paths(policy):
        mutated = _replaced(policy, path, sibling)
        target = write(mutated)
        audit_cli(swap(base, "--policy-file", target))
        _cli_json(
            capsys,
            generator.main(
                [
                    "verify",
                    "--policy-file",
                    str(target),
                    "--source-snapshot-file",
                    str(root / "source.json"),
                ]
            ),
        )
        try:
            operator._policy(generator.canonical_json(mutated))
        except (ValueError, TypeError, KeyError):
            pass  # the operator CLI reports these as static codes
    for path in _nested_paths(snapshot):
        target = write(_replaced(snapshot, path, sibling))
        audit_cli(swap(base, "--source-snapshot-file", target))
        _cli_json(
            capsys,
            generator.main(
                [
                    "verify",
                    "--policy-file",
                    str(root / "policy.json"),
                    "--source-snapshot-file",
                    str(target),
                ]
            ),
        )
    for path in _nested_paths(previous):
        raw = generator.canonical_json(_replaced(previous, path, sibling))
        target = root / f"fuzz-prev-{next(counter)}.json"
        target.write_bytes(raw)
        args = swap(base, "--previous-policy-file", target)
        args = swap(args, "--previous-policy-sha256", hashlib.sha256(raw).hexdigest())
        audit_cli(args)


# ── Round3 F1/F5/F6: operator receipt validation offline (no database) ─────
def _operator_receipt(root, calendar, capsys, monkeypatch):
    """A strict offline audit + canary chain in custody, pinned for the operator."""
    base, _, _ = _cli_inputs(root, calendar)
    for name in ("policy.json", "capture.json"):
        (root / name).chmod(0o600)
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
    capsys.readouterr()
    monkeypatch.setattr(operator, "AUDIT_CONFIG", root / "config.json")
    monkeypatch.delenv("NAV_READINESS_DATABASE_URL", raising=False)
    return {
        "policy": root / "policy.json",
        "audit_dossier": root / "audit.json",
        "canary_manifest": root / "canary.json",
        "capture": root / "capture.json",
    }


def _operator_args(root, files):
    args = [
        "--schema",
        "nav_offline",
        "--expected-sql-sha256",
        hashlib.sha256(operator.DDL.read_bytes()).hexdigest(),
        "--custody-root",
        str(root),
    ]
    for key in ("policy", "audit_dossier", "canary_manifest", "capture"):
        flag = key.replace("_", "-")
        args += [
            f"--{flag}-file",
            str(files[key]),
            f"--{flag}-sha256",
            hashlib.sha256(files[key].read_bytes()).hexdigest(),
        ]
    return args


def _private(root, name, data: bytes) -> Path:
    path = root / name
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
    return path


def _relinked(root, files, *, dossier=None, capture=None, tag="x"):
    """Consistently rewrite dossier/capture and relink the canary manifest."""
    files = dict(files)
    capture_doc = json.loads(files["capture"].read_bytes())
    dossier_doc = json.loads(files["audit_dossier"].read_bytes())
    manifest_doc = json.loads(files["canary_manifest"].read_bytes())
    if capture is not None:
        capture(capture_doc)
        raw = generator.canonical_json(capture_doc)
        files["capture"] = _private(root, f"capture-{tag}.json", raw)
        dossier_doc["inputs"]["capture"]["capture_bundle_sha256"] = hashlib.sha256(
            raw
        ).hexdigest()
        dossier_doc["inputs"]["capture"]["captured_at"] = capture_doc["captured_at"]
        manifest_doc["capture_bundle_sha256"] = hashlib.sha256(raw).hexdigest()
    if dossier is not None:
        dossier(dossier_doc)
    raw = generator.canonical_json(dossier_doc)
    files["audit_dossier"] = _private(root, f"audit-{tag}.json", raw)
    manifest_doc["audit_report_sha256"] = hashlib.sha256(raw).hexdigest()
    files["canary_manifest"] = _private(
        root, f"canary-{tag}.json", generator.canonical_json(manifest_doc)
    )
    return files


def _shift(value: str, **delta) -> str:
    return (dt.datetime.fromisoformat(value) + dt.timedelta(**delta)).isoformat()


@pytest.mark.skipif(os.name != "posix", reason="custody reader is POSIX-only")
@pytest.mark.parametrize(
    "case,code",
    [
        # A fully valid chain passes every file check; only the DSN is absent.
        ("valid", "KeyError"),
        ("capture_cohort_list", "audit_dossier_invalid"),
        ("capture_sec_number", "audit_dossier_invalid"),
        ("capture_inputs_list", "audit_dossier_invalid"),
        ("a7_list", "audit_dossier_invalid"),
        ("a7_number", "audit_dossier_invalid"),
        ("a7_missing_field", "audit_dossier_invalid"),
        ("deadline_plus_one_day", "audit_sec_deadline_inconsistent"),
        ("deadline_same_instant_other_offset", "KeyError"),
        ("decision_not_capture", "audit_receipt_time_invalid"),
        ("min_matched_after_decision", "audit_receipt_time_invalid"),
        ("min_matched_older_than_max_age", "audit_receipt_time_invalid"),
        ("max_matched_before_min", "audit_receipt_time_invalid"),
        ("lineage_after_decision", "audit_receipt_time_invalid"),
        ("capture_before_generation", "audit_receipt_time_invalid"),
        ("naive_capture_time", "audit_dossier_invalid"),
        # Round4 T9: Round3 dossiers are never accepted (version or literal).
        ("round3_contract_version", "audit_contract_mismatch"),
        ("round3_contract_sha256", "audit_contract_mismatch"),
        # Round5 A14: Round4 dossiers are never accepted either.
        ("round4_contract_version", "audit_contract_mismatch"),
        ("round4_contract_sha256", "audit_contract_mismatch"),
        # Round6 (identity v3): the blocked v2/Round5 audit is never accepted.
        ("round5_contract_version", "audit_contract_mismatch"),
        ("round5_contract_sha256", "audit_contract_mismatch"),
        # Round7: the Round6 contract (gap ceiling 23) is never accepted.
        ("round6_contract_version", "audit_contract_mismatch"),
        ("round6_contract_sha256", "audit_contract_mismatch"),
        # SEC exclusions must equal the policy's SEC first failures and the
        # operator's own recount of N, B, S, M and K.
        ("a8_exclusion_not_in_policy", "audit_dossier_invalid"),
        ("a8_bound_raised", "audit_dossier_invalid"),
        ("a8_c_size_inflated", "audit_dossier_invalid"),
        ("a8_stale_total_inconsistent", "audit_dossier_invalid"),
        ("a8_integrity_bool", "audit_dossier_invalid"),
        ("a8_missing_text", "audit_dossier_invalid"),
        ("a8_by_code_extra_key", "audit_dossier_invalid"),
        ("a8_round6_exclusion_shape", "audit_dossier_invalid"),
        ("a8_round6_ceiling_details", "audit_dossier_invalid"),
        ("a8_exclusions_missing", "audit_dossier_invalid"),
        ("a8_v2_detail_shape", "audit_dossier_invalid"),
        ("dossier_counts_status_mismatch", "audit_dossier_invalid"),
        ("dossier_counts_first_mismatch", "audit_dossier_invalid"),
        ("a4_structural_daily_count_mismatch", "audit_dossier_invalid"),
        ("a4_sec_first_failure_bool", "audit_dossier_invalid"),
        # A config whose hash was realigned with a tampered payload blocks.
        ("config_hash_realigned_fraction", "audit_config_sec_invalid"),
        ("config_hash_realigned_gap_ceiling", "audit_config_sec_invalid"),
        ("config_hash_realigned_round6", "audit_config_invalid"),
        ("config_hash_realigned_denominator_zero", "audit_config_sec_invalid"),
        ("config_hash_realigned_conflict_null", "audit_config_sec_invalid"),
        # A Round6 manifest (hash-linked to the Round7 dossier) is refused.
        ("manifest_round6_contract", "canary_manifest_invalid"),
    ],
)
def test_operator_receipt_nested_shapes_and_time_window_offline(
    calendar, tmp_path, capsys, monkeypatch, case, code
):
    root = _custody(tmp_path)
    files = _operator_receipt(root, calendar, capsys, monkeypatch)

    def freshness(mutate):
        return lambda d: mutate(d["details"]["A8"]["freshness"])

    def exclusions(mutate):
        return lambda d: mutate(d["details"]["A8"]["exclusions"])

    mutations = {
        "capture_cohort_list": {
            "dossier": lambda d: d["inputs"]["capture"].update(cohort=[])
        },
        "capture_sec_number": {
            "dossier": lambda d: d["inputs"]["capture"].update(sec=5)
        },
        "capture_inputs_list": {"dossier": lambda d: d["inputs"].update(capture=[])},
        "a7_list": {"dossier": lambda d: d["details"].update(A7=[])},
        "a7_number": {"dossier": lambda d: d["details"].update(A7=5)},
        "a7_missing_field": {
            "dossier": lambda d: d["details"]["A7"].pop("cohort_rows_sha256")
        },
        "deadline_plus_one_day": {
            "dossier": freshness(
                lambda f: f.update(valid_until=_shift(f["valid_until"], days=1))
            )
        },
        "deadline_same_instant_other_offset": {
            "dossier": freshness(
                lambda f: f.update(
                    valid_until=dt.datetime.fromisoformat(f["valid_until"])
                    .astimezone(dt.timezone(dt.timedelta(hours=-4)))
                    .isoformat()
                )
            )
        },
        "decision_not_capture": {
            "dossier": freshness(
                lambda f: f.update(decision_at=_shift(f["decision_at"], seconds=1))
            )
        },
        "min_matched_after_decision": {
            "dossier": freshness(
                lambda f: f.update(
                    min_matched_synced_at=_shift(f["decision_at"], seconds=1),
                    valid_until=_shift(f["decision_at"], days=7, seconds=1),
                )
            )
        },
        "min_matched_older_than_max_age": {
            "dossier": freshness(
                lambda f: f.update(
                    min_matched_synced_at=_shift(f["decision_at"], days=-7, seconds=-1),
                    valid_until=_shift(f["decision_at"], seconds=-1),
                )
            )
        },
        "max_matched_before_min": {
            "dossier": freshness(
                lambda f: f.update(
                    max_matched_synced_at=_shift(f["min_matched_synced_at"], seconds=-1)
                )
            )
        },
        "lineage_after_decision": {
            "dossier": freshness(
                lambda f: f.update(
                    lineage_max_synced_at=_shift(f["decision_at"], seconds=1)
                )
            )
        },
        "naive_capture_time": {
            "dossier": lambda d: d["inputs"]["capture"].update(
                captured_at="2026-09-24T12:05:00"
            )
        },
        "round3_contract_version": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_version="nav-identity-audit-contract-v2-round3"
            )
        },
        "round3_contract_sha256": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_sha256=(
                    "1ef74c426526f5308223921c8297516c9fa4ac4034737fad59cad668f73b0001"
                )
            )
        },
        "round4_contract_version": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_version="nav-identity-audit-contract-v2-round4"
            )
        },
        "round4_contract_sha256": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_sha256=(
                    "64c75b3db696132e435523e0f3d072309fc78c72069c2d8942bf7ef89bfd68db"
                )
            )
        },
        "round5_contract_version": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_version="nav-identity-audit-contract-v2-round5"
            )
        },
        "round5_contract_sha256": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_sha256=(
                    "0d237212e1e95a6e1e0494e071170d174481c29662a27b7bf9e7ac0bca3ec2c7"
                )
            )
        },
        "round6_contract_version": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_version="nav-identity-audit-contract-v3-round6"
            )
        },
        "round6_contract_sha256": {
            "dossier": lambda d: d["inputs"].update(
                audit_contract_sha256=(
                    "24a2c2fb989ef832f778a69d887d870c6e990573e21cd7564403862eb461b1ff"
                )
            )
        },
        "a8_exclusion_not_in_policy": {
            "dossier": exclusions(
                lambda e: e.update(
                    by_code={**e["by_code"], "sec.missing": 1},
                    missing=1,
                    c_size=e["c_size"] + 1,
                )
            )
        },
        "a8_bound_raised": {"dossier": exclusions(lambda e: e.update(bound=1))},
        "a8_c_size_inflated": {
            "dossier": exclusions(lambda e: e.update(c_size=10, bound=1))
        },
        "a8_stale_total_inconsistent": {
            "dossier": exclusions(lambda e: e.update(stale=1))
        },
        "a8_integrity_bool": {
            "dossier": exclusions(lambda e: e.update(integrity=False))
        },
        "a8_missing_text": {"dossier": exclusions(lambda e: e.update(missing="0"))},
        "a8_by_code_extra_key": {
            "dossier": exclusions(
                lambda e: e.update(by_code={**e["by_code"], "sec.other": 0})
            )
        },
        "a8_round6_exclusion_shape": {
            "dossier": lambda d: d["details"]["A8"].update(
                exclusions={
                    "by_code": d["details"]["A8"]["exclusions"]["by_code"],
                    "gap": 0,
                    "conflict": 0,
                }
            )
        },
        "a8_round6_ceiling_details": {
            "dossier": lambda d: d["details"]["A8"].update(
                gap_ceiling=23, conflict_ceiling=0
            )
        },
        "a8_exclusions_missing": {
            "dossier": lambda d: d["details"]["A8"].pop("exclusions")
        },
        "dossier_counts_status_mismatch": {
            "dossier": lambda d: d["counts"]["fund_status"].update(
                ACTIVE=d["counts"]["fund_status"]["ACTIVE"] + 1
            )
        },
        "dossier_counts_first_mismatch": {
            "dossier": lambda d: d["counts"]["identity_first_failure"].update(
                {"sec.stale": 1}
            )
        },
        "a4_structural_daily_count_mismatch": {
            "dossier": lambda d: d["details"]["A4"].update(
                structural_daily_count=d["details"]["A4"]["structural_daily_count"] + 1
            )
        },
        "a4_sec_first_failure_bool": {
            "dossier": lambda d: d["details"]["A4"].update(
                sec_first_failure={"sec.stale": True}
            )
        },
        "a8_v2_detail_shape": {
            "dossier": lambda d: d["details"]["A8"].update(
                active_poisoned_count=0, excluded_poisoned_count=0
            )
        },
    }
    config_tampering = {
        "config_hash_realigned_fraction": lambda c: c["sec"].update(
            exclusion_fraction={"numerator": 1, "denominator": 5}
        ),
        "config_hash_realigned_gap_ceiling": lambda c: c["sec"].update(gap_ceiling=23),
        "config_hash_realigned_round6": lambda c: c.update(
            audit_contract_version="nav-identity-audit-contract-v3-round6"
        ),
        "config_hash_realigned_denominator_zero": lambda c: c["sec"].update(
            exclusion_fraction={"numerator": 1, "denominator": 0}
        ),
        "config_hash_realigned_conflict_null": lambda c: c["sec"].update(
            conflict_ceiling=None
        ),
    }
    if case == "capture_before_generation":
        # Consistent capture/dossier/manifest rewrite: the capture (and so the
        # A8 decision) now precedes the policy generation instant.
        earlier = "2026-09-24T11:59:00+00:00"
        files = _relinked(
            root,
            files,
            capture=lambda c: c.update(captured_at=earlier),
            dossier=freshness(lambda f: f.update(decision_at=earlier)),
            tag=case,
        )
    elif case in config_tampering:
        # The pinned config file itself is tampered and every hash that binds
        # it (dossier, manifest) is realigned: only the independent operator
        # validation of the pinned bytes can refuse it.
        config = json.loads((root / "config.json").read_bytes())
        config_tampering[case](config)
        tampered = _private(root, f"config-{case}.json", json.dumps(config).encode())
        digest = hashlib.sha256(tampered.read_bytes()).hexdigest()
        monkeypatch.setattr(operator, "AUDIT_CONFIG", tampered)
        files = _relinked(
            root,
            files,
            dossier=lambda d: d["inputs"].update(audit_config_sha256=digest),
            tag=case,
        )
        manifest = json.loads(files["canary_manifest"].read_bytes())
        manifest["audit_config_sha256"] = digest
        files["canary_manifest"] = _private(
            root, f"canary-{case}-cfg.json", generator.canonical_json(manifest)
        )
    elif case == "manifest_round6_contract":
        manifest = json.loads(files["canary_manifest"].read_bytes())
        manifest.update(
            audit_contract_version="nav-identity-audit-contract-v3-round6",
            audit_contract_sha256=(
                "24a2c2fb989ef832f778a69d887d870c6e990573e21cd7564403862eb461b1ff"
            ),
        )
        files = dict(files)
        files["canary_manifest"] = _private(
            root, f"canary-{case}.json", generator.canonical_json(manifest)
        )
    elif case != "valid":
        files = _relinked(root, files, **mutations[case], tag=case)
    result = operator.main(_operator_args(root, files))
    out = json.loads(capsys.readouterr().out)
    assert (result, out["code"], out["dml_committed"]) == (2, code, False), out
    if code == "KeyError":
        assert set(out["audit"]) == set(operator.AUDIT_RECEIPT_KEYS)
        assert out["audit"]["captured_at"] == out["audit"]["decision_at"] == CAPTURED


@pytest.mark.skipif(os.name != "posix", reason="custody reader is POSIX-only")
def test_operator_custody_fifo_is_rejected_without_blocking(
    calendar, tmp_path, capsys, monkeypatch
):
    """A writer-less FIFO in custody is refused at once (bounded subprocess)."""
    import subprocess
    import sys
    import time

    root = _custody(tmp_path)
    files = _operator_receipt(root, calendar, capsys, monkeypatch)
    args = _operator_args(root, files)
    fifo = root / "fifo.json"
    os.mkfifo(fifo, 0o600)
    args[args.index("--audit-dossier-file") + 1] = str(fifo)
    env = {
        key: value
        for key, value in os.environ.items()
        if key != "NAV_READINESS_DATABASE_URL"
    }
    started = time.monotonic()
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.fund_nav_readiness_schema", *args],
        cwd=Path(operator.__file__).parents[1],
        env=env,
        capture_output=True,
        timeout=60,
        check=False,
    )
    elapsed = time.monotonic() - started
    out = json.loads(completed.stdout)
    assert (completed.returncode, out["code"], out["dml_committed"]) == (
        2,
        "custody_file_not_regular",
        False,
    ), completed.stderr
    assert elapsed < 30 and b"Traceback" not in completed.stderr
    # In-process, the reader classifies the FIFO before any read.
    with pytest.raises(ValueError, match="custody_file_not_regular"):
        operator._read_custody(operator._custody_root(str(root)), str(fifo))
