"""Frozen contracts for public bond default evidence (policy, bundle schema, rows)."""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import subprocess
import sys
import uuid
from pathlib import Path

import jsonschema
import pytest

from src.bonds.default_events import contracts as c

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "bond_default_events"
UTC = dt.timezone.utc


def _load_synthetic():
    spec = importlib.util.spec_from_file_location("bond_default_synthetic", FIXTURES / "synthetic.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


syn = _load_synthetic()


@pytest.fixture(scope="module")
def qualified():
    return syn.build_bundle()


@pytest.fixture(scope="module")
def policy():
    return c.load_policy()


# ---------------------------------------------------------------------------
# Frozen documents and hash procedure
# ---------------------------------------------------------------------------
def test_policy_digest_is_pinned_and_embedded(policy):
    assert policy["digest"] == c.POLICY_DIGEST
    assert c.document_digest(policy, "digest") == c.POLICY_DIGEST
    assert policy["policy_id"] == c.POLICY_ID and policy["status"] == "frozen"


def test_policy_digest_is_independent_of_formatting_but_not_content(tmp_path):
    document = c.load_json_strict(c.POLICY_PATH.read_bytes())
    reformatted = tmp_path / "policy.json"
    reformatted.write_text(json.dumps(document, indent=7).replace("\n", "\r\n"), encoding="utf-8")
    assert c.load_policy(reformatted)["digest"] == c.POLICY_DIGEST
    document["nport"]["consensus_rule"] += " (edited)"
    tampered = tmp_path / "tampered.json"
    tampered.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(c.ContractError, match="policy_embedded_digest_mismatch"):
        c.load_policy(tampered)


def test_bundle_schema_is_the_rendering_of_the_row_specs():
    committed = c.load_bundle_schema()
    rendered = c.render_bundle_schema()
    rendered["x-digest"] = c.document_digest(rendered, "x-digest")
    assert committed == rendered
    assert committed["x-digest"] == c.SCHEMA_DIGEST
    assert committed["x-policy-digest"] == c.POLICY_DIGEST
    jsonschema.Draft202012Validator.check_schema(committed)


def test_json_loading_and_canonicalization_are_strict():
    with pytest.raises(c.ContractError, match="duplicate_json_key"):
        c.load_json_strict('{"a": 1, "a": 2}')
    with pytest.raises(c.ContractError, match="non_finite"):
        c.load_json_strict('{"a": NaN}')
    with pytest.raises(c.ContractError, match="float_not_canonical"):
        c.canonical_json_bytes({"a": 0.5})
    assert c.canonical_json_bytes({"b": [2, 1], "a": "é"}) == '{"a":"é","b":[2,1]}'.encode()


def test_exactly_four_additive_sql_files_and_line_ending_neutral_digest(tmp_path):
    assert [p.name for p in c.SQL_PATHS] == [
        "bond_default_sources_v1.sql",
        "bond_credit_publications_v1.sql",
        "bond_default_events_v1.sql",
        "bond_rating_history_public_v1.sql",
    ]
    copies = []
    for path in c.SQL_PATHS:
        copy = tmp_path / path.name
        copy.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        copies.append(copy)
    assert c.sql_digest(copies) == c.sql_digest()
    for path in c.SQL_PATHS:
        code = "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())
        assert "sec_derived_publications" not in code
        assert "bond_rating_history_v1 " not in code
        assert "REVOKE CREATE" not in code.upper() and "ALTER TABLE" not in code.upper()
        assert "DROP TABLE" not in code.upper()


# ---------------------------------------------------------------------------
# Policy <-> code mirrors
# ---------------------------------------------------------------------------
def test_enumerations_mirror_the_frozen_policy(policy):
    e = c.ENUMS
    assert sorted(policy["adjudication"]["statuses"]) == sorted(e["adjudication_status"])
    assert set(policy["adjudication"]["statuses"]) == {
        "candidate", "accepted_state", "accepted_event", "accepted_evidence", "nonqualifying", "disputed",
        "retracted",
    }
    # Policy 1.1.0 (W0 amendment 1): evidence-only subjects and N-CEN families are mirrored.
    assert policy["policy_version"] == c.POLICY_VERSION == "1.1.0"
    assert policy["amendment_1"]["wire_version"] == c.CONTRACT_VERSION
    assert policy["amendment_1"]["family_rule_version"] == c.FAMILY_RULE_VERSION
    assert c.EVIDENCE_SUBJECT_KINDS == {"issue_scope", "followup_continuity", "exchange_pairing", "corroboration"}
    assert c.EVIDENCE_SUBJECT_KINDS < set(e["subject_kind"])
    assert "accepted_evidence" in policy["adjudication"]["reviewer_roles"]["human_reviewer"]
    for family in ("sec_ncen_dera", "sec_ncen_public_xml", "sec_ncen_acceptance_header"):
        assert family in e["source_family"] and family in policy["public_source_policy"]["source_families"]
    assert sorted(policy["timing"]["classes"]) == sorted(e["timing_class"])
    assert sorted(policy["primary_types"]) == sorted(e["primary_type"])
    assert sorted(policy["corroboration_flags"]) == sorted(e["corroboration_flag"])
    assert sorted(policy["links"]["statuses"]) == sorted(e["link_status"])
    assert sorted(policy["links"]["affected_scopes"]) == sorted(e["affected_scope"])
    assert sorted(policy["followup"]["statuses"]) == sorted(e["followup_status"])
    assert sorted(policy["followup"]["completeness_bases"]) == sorted(e["completeness_basis"])
    assert sorted(policy["exit_evidence"]["primary_reasons"]) == sorted(e["exit_reason"])
    assert sorted(policy["exit_evidence"]["flags"]) == sorted(e["exit_flag"])
    cov = policy["coverage"]
    assert sorted(cov["states"]) == sorted(e["coverage_state"])
    assert sorted(cov["sources"]) == sorted(e["coverage_source"])
    assert sorted(cov["event_types"]) == sorted(e["coverage_event_type"])
    assert sorted(cov["rating_strata"]) == sorted(e["rating_stratum"])
    assert sorted(cov["exposure_cohorts"]) == sorted(e["exposure_cohort"])
    assert sorted(cov["denominator_bases"]) == sorted(e["denominator_basis"])
    rating = policy["rating_policy"]
    assert sorted(rating["view_kinds"]) == sorted(e["view_kind"])
    assert sorted(rating["states"]) == sorted(e["rating_state"])
    assert sorted(rating["buckets"]) == sorted(e["rating_bucket"])
    pub = policy["publication"]
    assert pub["product"] == c.PRODUCT
    assert sorted(pub["quality_states"]) == sorted(e["quality_state"])
    assert sorted(pub["knowledge_modes"]) == sorted(e["knowledge_mode"])
    assert sorted(pub["build_scopes"]) == sorted(e["build_scope"])
    assert sorted(policy["public_time_bases"]) == sorted(e["public_time_basis"])
    assert sorted(policy["nport"]["field_presence_states"]) == sorted(e["field_presence_state"])
    assert sorted(policy["public_source_policy"]["rights_states"]) == sorted(e["rights_state"])
    for role, statuses in policy["adjudication"]["reviewer_roles"].items():
        assert set(statuses) == c.REVIEWER_ROLE_STATUSES[role]


def test_source_family_rights_gate_mirrors_policy(policy):
    families = policy["public_source_policy"]["source_families"]
    assert set(families) == set(c.SOURCE_FAMILY_POLICY) == set(c.ENUMS["source_family"])
    for family, spec in families.items():
        allowed, _role, needs_ref = c.SOURCE_FAMILY_POLICY[family]
        assert set(spec["allowed_rights_states"]) == allowed
        assert bool(spec.get("requires_rights_ref", False)) == needs_ref
    assert c.SOURCE_FAMILY_POLICY["agency_rocr_xbrl"][0] == {"approved"}


def test_nport_flag_semantics_are_frozen(policy):
    mapping = policy["nport"]["flag_mapping"]
    assert mapping["IS_DEFAULT"]["form_item"] == "C.9.c"
    assert mapping["ARE_ANY_INTEREST_PAYMENT"]["form_item"] == "C.9.d"
    assert mapping["IS_ANY_PORTION_INTEREST_PAID"]["form_item"] == "C.9.e"
    assert {v["observation_column"] for v in mapping.values()} == set(c.NPORT_FLAG_FIELDS)
    never = " ".join(policy["never_default_labels"])
    assert "C.9.d" in never and "C.9.e" in never and "IS_PAID_KIND" not in json.dumps(policy)
    rule = policy["nport"]["consensus_rule"]
    for phrase in ("two distinct series", "two independently evidenced", "no unresolved N vote",
                   "no majority vote", "unknown family identity cannot supply"):
        assert phrase in rule
    assert "max(public_available_at of every relied-on observation revision, link_known_at)" in (
        policy["temporal_admission"]["evidence_known_at"]
    )


# ---------------------------------------------------------------------------
# Identifiers and time
# ---------------------------------------------------------------------------
def test_cusip_check_digit_known_answers_and_rejections():
    # Hand-computed: Z=35->8, Z*2=70->7, #=38->11, S*2=56->11, Y=34->7, N*2=46->10, 0, 1*2=2; sum 56.
    assert c.cusip_check_digit("ZZ#SYN01") == "4"
    assert syn.CUSIP_A == "ZZ#SYN014"
    assert c.is_valid_cusip9("ZZ9SYN016")
    assert not c.is_valid_cusip9("ZZ9SYN017")
    assert not c.is_valid_cusip9("zz9syn016")
    assert not c.is_valid_cusip9("ZZ9SYN01")
    for number in range(1, 60):
        cusip = syn.synthetic_cusip(number)
        assert c.is_valid_cusip9(cusip)
        wrong = cusip[:8] + str((int(cusip[8]) + 1) % 10)
        assert not c.is_valid_cusip9(wrong)


def test_isin_luhn_and_us_ca_conversion():
    assert c.is_valid_isin("USZZ9SYN0168")
    assert c.cusip_from_isin("USZZ9SYN0168") == "ZZ9SYN016"
    assert c.cusip_from_isin("CAZZ9SYN0168") is None  # Luhn differs for CA prefix
    assert not c.is_valid_isin("USZZ9SYN0167")
    assert c.cusip_from_isin("XSZZ9SYN0168") is None


def test_edgar_acceptance_is_new_york_wall_time_with_dst():
    assert c.edgar_acceptance_to_utc("20260518163015") == dt.datetime(2026, 5, 18, 20, 30, 15, tzinfo=UTC)
    assert c.edgar_acceptance_to_utc("20260115163015") == dt.datetime(2026, 1, 15, 21, 30, 15, tzinfo=UTC)
    # Fall-back ambiguity resolves to the later (conservative) instant.
    assert c.edgar_acceptance_to_utc("20261101013000") == dt.datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    with pytest.raises(c.ContractError, match="nonexistent"):
        c.edgar_acceptance_to_utc("20260308023000")
    for bad in ("2026-05-18 16:30:15", "20261301000000", "2026051816301",
                "20260518240000", "20260518163060", "٢٠٢٦٠٥١٨١٦٣٠١٥"):
        with pytest.raises(c.ContractError):
            c.edgar_acceptance_to_utc(bad)


def test_date_only_public_time_is_next_day_boundary():
    assert c.date_only_public_available_at(dt.date(2026, 7, 2), "America/New_York") == dt.datetime(
        2026, 7, 3, 4, 0, tzinfo=UTC
    )
    assert c.date_only_public_available_at(dt.date(2026, 12, 31), "UTC") == dt.datetime(2027, 1, 1, tzinfo=UTC)


def test_timing_class_derivation():
    assert c.derive_timing_class(None, dt.date(2026, 3, 31)) == "prevalent"
    assert c.derive_timing_class(dt.date(2026, 5, 14), dt.date(2026, 5, 15)) == "incident"
    assert c.derive_timing_class(dt.date(2026, 4, 30), dt.date(2026, 5, 31)) == "incident"
    assert c.derive_timing_class(dt.date(2026, 3, 31), dt.date(2026, 6, 30)) == "interval_uncertain"
    assert c.derive_timing_class(dt.date(2026, 4, 29), dt.date(2026, 5, 1)) == "interval_uncertain"
    with pytest.raises(c.ContractError):
        c.derive_timing_class(dt.date(2026, 5, 15), dt.date(2026, 5, 15))


def test_month_helpers():
    assert c.add_months(dt.date(2026, 8, 1), -60) == dt.date(2021, 8, 1)
    assert c.month_end(dt.date(2024, 2, 1)) == dt.date(2024, 2, 29)
    assert c.next_month_boundary_utc(dt.date(2026, 12, 1)) == dt.datetime(2027, 1, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Row contracts
# ---------------------------------------------------------------------------
def _package_values(**changes):
    values = {n: getattr(syn.build_bundle().frames["source_packages"][0], n) for n, _ in c.SourcePackage.SPEC}
    values.pop("package_id")
    values.update(changes)
    return values


def test_source_package_rights_gate_and_identity():
    ok = c.SourcePackage.create(**_package_values())
    assert ok.package_id == c.SourcePackage.derive_id(ok.source_family, ok.external_id, ok.content_sha256)
    with pytest.raises(c.ContractError, match="rights_state"):
        c.SourcePackage.create(**_package_values(source_family="agency_rocr_xbrl", rights_state="unverified",
                                                 rights_ref="x"))
    with pytest.raises(c.ContractError, match="rights_ref"):
        c.SourcePackage.create(**_package_values(source_family="agency_rocr_xbrl", rights_state="approved",
                                                 rights_ref=None))
    with pytest.raises(c.ContractError, match="rights_state"):
        c.SourcePackage.create(**_package_values(rights_state="approved"))
    for locator in ("../escape.bin", "/abs/path", "C:\\abs", "a/../b"):
        with pytest.raises(c.ContractError, match="raw_locator"):
            c.SourcePackage.create(**_package_values(raw_locator=locator))
    with pytest.raises(c.ContractError, match="first_verified_public_at"):
        c.SourcePackage.create(**_package_values(first_verified_public_at=dt.datetime(2030, 1, 1, tzinfo=UTC)))
    with pytest.raises(c.ContractError, match="package_id:not_derived"):
        c.SourcePackage(package_id=uuid.uuid4(), **_package_values())


def _obs_values(bundle, index=0, **changes):
    row = bundle.frames["observations"][index]
    values = {n: getattr(row, n) for n, _ in c.CreditObservation.SPEC}
    values.pop("observation_id")
    values.update(changes)
    return values


def _nport_row(bundle):
    return next(i for i, o in enumerate(bundle.frames["observations"]) if o.observation_kind == "nport_holding")


def test_observation_nport_field_presence_is_lossless(qualified):
    i = _nport_row(qualified)
    c.CreditObservation.create(**_obs_values(qualified, i))
    presence = {"nport_arrears_or_deferral": "absent", "nport_is_default": "invalid", "nport_paid_in_kind": "null"}
    c.CreditObservation.create(**_obs_values(
        qualified, i, field_presence=presence, nport_arrears_or_deferral=None, nport_is_default="y ",
        nport_paid_in_kind=None,
    ))
    with pytest.raises(c.ContractError, match="field_presence"):
        c.CreditObservation.create(**_obs_values(qualified, i, field_presence={**presence, "nport_is_default": "present"},
                                                 nport_is_default="y ", nport_arrears_or_deferral=None,
                                                 nport_paid_in_kind=None))
    with pytest.raises(c.ContractError, match="field_presence"):
        c.CreditObservation.create(**_obs_values(qualified, i, field_presence={"nport_is_default": "present"}))


def test_observation_identity_and_time_rules(qualified):
    i = _nport_row(qualified)
    with pytest.raises(c.ContractError, match="cusip9"):
        c.CreditObservation.create(**_obs_values(qualified, i, cusip9=syn.synthetic_cusip(9)))
    with pytest.raises(c.ContractError, match="valid_cusip9"):
        c.CreditObservation.create(**_obs_values(qualified, i, cusip9=syn.CUSIP_B[:8] + "0"))
    with pytest.raises(c.ContractError, match="acceptance_at"):
        c.CreditObservation.create(**_obs_values(
            qualified, i, acceptance_at=dt.datetime(2026, 5, 20, 10, 15, tzinfo=UTC),
            public_available_at=dt.datetime(2026, 5, 20, 10, 15, tzinfo=UTC)))
    edgar = next(i for i, o in enumerate(qualified.frames["observations"]) if o.observation_kind == "edgar_passage")
    with pytest.raises(c.ContractError, match="nport_flags"):
        c.CreditObservation.create(**_obs_values(qualified, edgar, nport_is_default="Y"))
    with pytest.raises(c.ContractError, match="document_passage"):
        c.CreditObservation.create(**_obs_values(qualified, edgar, document_sha256=None))
    with pytest.raises(c.ContractError, match="supersedes"):
        c.CreditObservation.create(**_obs_values(qualified, edgar, revision_kind="amendment"))
    isin_row = _obs_values(qualified, i, cusip_raw=None, isin_raw=None)
    with pytest.raises(c.ContractError, match="cusip9"):
        c.CreditObservation.create(**isin_row)


def test_identity_encoding_is_unambiguous_for_delimiter_bearing_components(qualified):
    package_id = qualified.frames["observations"][0].package_id
    left = c.CreditObservation.derive_id(package_id, "a|b", "c")
    right = c.CreditObservation.derive_id(package_id, "a", "b|c")
    assert left != right
    assert c.uuid5_of("k", "a|b", "c") != c.uuid5_of("k", "a", "b|c")
    assert c.uuid5_of("k", "a", "") != c.uuid5_of("k", "a")
    assert c.identity_name("observation", "x|y") == '["observation","x|y"]'
    assert c.SourcePackage.derive_id("f|x", "y", "0" * 64) != c.SourcePackage.derive_id(
        "f", "x|y", "0" * 64
    )
    i = _nport_row(qualified)
    obs_left = c.CreditObservation.create(**_obs_values(
        qualified, i, member_name="HOLDING|A.tsv", row_locator="row|1"))
    obs_right = c.CreditObservation.create(**_obs_values(
        qualified, i, member_name="HOLDING", row_locator="A.tsv|row|1"))
    assert obs_left.observation_id != obs_right.observation_id
    with pytest.raises(c.ContractError, match="identity_component_not_text"):
        c.uuid5_of("k", 1)  # type: ignore[arg-type]


def test_links_and_adjudication_role_matrix(qualified):
    link = next(x for x in qualified.frames["event_links"] if x.status == "admitted")
    values = {n: getattr(link, n) for n, _ in c.EventLink.SPEC if n != "link_id"}
    assert c.EventLink.create(**values).link_id == link.link_id
    with pytest.raises(c.ContractError, match="identity_evidence_refs"):
        c.EventLink.create(**{**values, "identity_evidence_refs": ()})
    with pytest.raises(c.ContractError, match="array_must_be_sorted_unique"):
        c.EventLink.create(**{**values, "identity_evidence_refs": ("b", "a")})
    with pytest.raises(c.ContractError, match="link_id:not_derived"):
        c.EventLink(link_id=uuid.uuid4(), **values)
    accepted = next(a for a in qualified.frames["adjudications"] if a.status == "accepted_event")
    base = {n: getattr(accepted, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"}
    with pytest.raises(c.ContractError, match="reviewer_role"):
        c.Adjudication.create(**{**base, "reviewer_role": "extraction_proposer"})
    with pytest.raises(c.ContractError, match="reviewer_role"):
        c.Adjudication.create(**{**base, "reviewer_role": "policy_rule_engine"})
    with pytest.raises(c.ContractError, match="admission"):
        c.Adjudication.create(**{**base, "link_ids": ()})
    with pytest.raises(c.ContractError, match="subject_kind"):
        c.Adjudication.create(**{**base, "subject_kind": "candidate"})
    c.Adjudication.create(**{**base, "status": "retracted", "reviewer_role": "policy_rule_engine"})


def test_default_episode_rules(qualified):
    event = next(e for e in qualified.frames["events"] if e.primary_type == "payment_default")
    assert event.timing_class == "incident" and event.onset_date == dt.date(2026, 5, 15)
    with pytest.raises(c.ContractError, match="timing_class"):
        syn.replace_row(event, timing_class="interval_uncertain")
    with pytest.raises(c.ContractError, match="onset_date"):
        syn.replace_row(event, onset_date=dt.date(2026, 5, 14))
    with pytest.raises(c.ContractError, match="admission_status"):
        syn.replace_row(event, admission_status="accepted_state")
    with pytest.raises(c.ContractError, match="evidence_known_at"):
        syn.replace_row(event, link_known_at=event.evidence_known_at + dt.timedelta(seconds=1))
    with pytest.raises(c.ContractError, match="event_input_digest"):
        syn.replace_row(event, event_input_digest="sha256:" + "0" * 64)
    with pytest.raises(c.ContractError, match="resolution"):
        syn.replace_row(event, resolution_date=dt.date(2026, 6, 1))
    with pytest.raises(c.ContractError, match="resolution"):
        syn.replace_row(event, resolution_date=dt.date(2026, 6, 1), resolution_refs=event.evidence_observation_ids)
    with pytest.raises(c.ContractError, match="onset_lower_evidence_ids:required_iff_lower_bound"):
        syn.replace_row(event, onset_lower_evidence_ids=())
    with pytest.raises(c.ContractError, match="onset_lower_evidence_ids:not_in_event_evidence"):
        syn.replace_row(event, onset_lower_evidence_ids=(uuid.uuid4(),))
    state = next(e for e in qualified.frames["events"] if e.primary_type == "default_state")
    assert state.timing_class == "prevalent" and state.onset_lower_exclusive is None
    assert state.onset_lower_evidence_ids == ()
    with pytest.raises(c.ContractError, match="onset_lower_evidence_ids:required_iff_lower_bound"):
        syn.replace_row(state, onset_lower_evidence_ids=state.evidence_observation_ids[:1])


def test_receipt_surveillance_window_rules(qualified):
    receipt = qualified.manifest.receipt()
    values = {n: getattr(receipt, n) for n, _ in receipt.SPEC}
    with pytest.raises(c.ContractError, match="surveillance_window_both_or_none"):
        c.ValidationReceipt(**{**values, "surveillance_start_exclusive": dt.date(2026, 3, 31)})
    with pytest.raises(c.ContractError, match="surveillance_window_not_increasing"):
        c.ValidationReceipt(**{**values, "surveillance_start_exclusive": dt.date(2026, 6, 30),
                               "surveillance_end_inclusive": dt.date(2026, 6, 30)})
    windowed = c.ValidationReceipt(**{**values, **syn.SURVEILLANCE_WINDOW})
    assert windowed.surveils(True, dt.date(2026, 3, 31), dt.date(2026, 6, 30))
    assert not windowed.surveils(False, dt.date(2026, 3, 31), dt.date(2026, 6, 30))
    assert not windowed.surveils(True, dt.date(2026, 3, 30), dt.date(2026, 6, 30))
    assert not c.ValidationReceipt(**{**values, **syn.SURVEILLANCE_WINDOW, "verdict": "partial"}).surveils(
        True, dt.date(2026, 4, 30), dt.date(2026, 5, 31))
    record = dict(qualified.manifest.values)
    record.update({f: None for f in (*c.VALIDATION_FIELDS, *c.VALIDATION_RATING_FIELDS, "validation_digest")})
    record.update({"validation_surveillance_start_exclusive": dt.date(2026, 3, 31),
                   "validation_surveillance_end_inclusive": dt.date(2026, 6, 30)})
    with pytest.raises(c.ContractError, match="surveillance_window_without_receipt"):
        c.BundleManifest(record)


def test_followup_exit_coverage_rating_and_receipt_rules(qualified):
    follow = next(f for f in qualified.frames["followups"] if f.status == "unknown")
    with pytest.raises(c.ContractError, match="completeness_basis"):
        syn.replace_row(follow, completeness_basis="continuous_document")
    with pytest.raises(c.ContractError, match="followup:evidence_required"):
        syn.replace_row(follow, status="nondefault_continuous", completeness_basis="continuous_document")
    exit_row = qualified.frames["exit_evidence"][0]
    with pytest.raises(c.ContractError, match="next_observed_month"):
        syn.replace_row(exit_row, next_observed_month=dt.date(2026, 9, 1))
    reentry = syn.replace_row(exit_row, flags=tuple(sorted({*exit_row.flags, "observed_gap_reentry"})),
                              next_observed_month=c.add_months(exit_row.last_panel_month, 3), gap_months=2)
    assert reentry.gap_months == 2
    with pytest.raises(c.ContractError, match="gap_months"):
        syn.replace_row(reentry, gap_months=3)
    cell = next(x for x in qualified.frames["coverage"] if x.state == "qualified")
    with pytest.raises(c.ContractError, match="qualified_requires_receipt"):
        syn.replace_row(cell, validation_receipt_digest=None)
    with pytest.raises(c.ContractError, match="lag_summary"):
        syn.replace_row(cell, lag_p50_days=10)
    with pytest.raises(c.ContractError, match="period_label"):
        syn.replace_row(cell, period_label=None)
    rated = next(r for r in qualified.frames["ratings"] if r.state == "observed" and r.view_kind == "public_pit")
    with pytest.raises(c.ContractError, match="public_pit"):
        syn.replace_row(rated, public_known_at=dt.datetime(2026, 5, 1, tzinfo=UTC))
    syn.replace_row(rated, view_kind="effective_audit", public_known_at=dt.datetime(2026, 5, 1, tzinfo=UTC))
    with pytest.raises(c.ContractError, match="action_date"):
        syn.replace_row(rated, action_date=dt.date(2026, 5, 1))
    missing = next(r for r in qualified.frames["ratings"] if r.state == "missing")
    with pytest.raises(c.ContractError, match="bucket"):
        syn.replace_row(missing, bucket="D")
    with pytest.raises(c.ContractError, match="positive_evidence"):
        c.ValidationReceipt(receipt_id=uuid.uuid4(), verdict="qualified", scope="s", evidence_digest=c.digest_of(1),
                            reviewer_id="r", issued_at=dt.datetime(2026, 1, 1, tzinfo=UTC),
                            positive_evidence_count=0)


# ---------------------------------------------------------------------------
# Bundles, identity and shared fixtures
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("name", sorted(syn.FIXTURES))
def test_committed_fixtures_equal_builder_and_validate(name):
    committed = c.load_json_strict((FIXTURES / name).read_bytes())
    built = syn.FIXTURES[name]()
    assert committed == built.to_json_obj(), "regenerate with synthetic.py after intentional contract changes"
    decoded = c.CreditBundle.from_json_obj(committed)
    assert decoded.canonical_bytes() == built.canonical_bytes()


def test_fixtures_are_synthetic_only():
    for name in syn.FIXTURES:
        text = (FIXTURES / name).read_text(encoding="utf-8")
        payload = c.load_json_strict(text)
        for cusip, _month in payload["panel_grid"]:
            assert cusip.startswith("ZZ#SYN")
        for row in payload["frames"]["observations"]:
            assert row["cusip9"] is None or row["cusip9"].startswith("ZZ#SYN")
            for cik in (row["issuer_cik"], row["registrant_cik"]):
                assert cik is None or cik.startswith("99999999")
        assert "http" not in text


def test_publication_identity_is_input_fingerprint_without_prepared_at(qualified):
    assert "prepared_at" not in c.FINGERPRINT_FIELDS
    assert not {"events_digest", "ratings_digest", "coverage_digest"} & set(c.FINGERPRINT_FIELDS)
    m = qualified.manifest
    assert m["publication_id"] == c.publication_id_for(c.fingerprint_digest(m.values))
    altered = syn.build_bundle(alter_output=True)
    assert altered.publication_id == qualified.publication_id
    assert altered.bundle_digest() != qualified.bundle_digest()
    later_k = syn.build_bundle(knowledge_cutoff=dt.datetime(2026, 9, 26, tzinfo=UTC))
    later_t = syn.build_bundle(target_month=dt.date(2026, 7, 1))
    historical = syn.reassemble(qualified, knowledge_mode="historical_reconstruction")
    assert len({qualified.publication_id, later_k.publication_id, later_t.publication_id,
                historical.publication_id}) == 4
    assert syn.build_bundle().bundle_digest() == qualified.bundle_digest()


def test_publication_id_is_uuidv8_of_the_fingerprint_bits(qualified):
    fingerprint = qualified.manifest["fingerprint_digest"]
    pid = qualified.publication_id
    assert pid.version == 8 and pid.variant == uuid.RFC_4122
    raw, bits = pid.bytes, bytes.fromhex(fingerprint[7:39])
    assert raw[:6] == bits[:6] and raw[7] == bits[7] and raw[9:] == bits[9:]
    assert raw[6] == (bits[6] & 0x0F) | 0x80 and raw[8] == (bits[8] & 0x3F) | 0x80
    assert c.publication_id_for("sha256:" + "f" * 64) == uuid.UUID("ffffffff-ffff-8fff-bfff-ffffffffffff")
    assert c.publication_id_for("sha256:" + "0" * 64) == uuid.UUID("00000000-0000-8000-8000-000000000000")
    for bad in ("sha256:" + "F" * 64, "f" * 64, "sha256:abc", None):
        with pytest.raises(c.ContractError, match="fingerprint_digest_expected"):
            c.publication_id_for(bad)  # type: ignore[arg-type]


def test_row_encoding_is_length_prefixed_type_tagged_and_unambiguous():
    assert c.encode_value(["a", None, 5, True, {"b": "1", "aa": "é"}]) == "A5:S1:aNI1:5B4:trueM2:S2:aaS2:éS1:bS1:1"
    assert c.encode_value({"b": "x", "aa": "y", "Z": "z", "é": "w"}) == "M4:S1:ZS1:zS2:aaS1:yS1:bS1:xS2:éS1:w"
    assert c.encode_value("é中\U0001d11e") == "S9:é中\U0001d11e"
    assert c.encode_value([]) == "A0:" and c.encode_value({}) == "M0:" and c.encode_value("") == "S0:"
    assert c.encode_value("5") != c.encode_value(5) != c.encode_value(True)
    assert c.encode_value(["a|b", "c"]) != c.encode_value(["a", "b|c"])
    assert c.row_encoding({"x": "ab", "y": ""}, ["x", "y"]) != c.row_encoding({"x": "a", "y": "b"}, ["x", "y"])
    assert c.row_encoding({"x": None, "y": "N"}, ["x", "y"]) == b"NS1:N"
    for bad in (0.5, b"x", {1: "a"}):
        with pytest.raises(c.ContractError, match="row_encoding"):
            c.encode_value(bad)


def test_row_hashes_receipt_and_fingerprint_use_the_row_encoding(qualified):
    for name, rows in qualified.frames.items():
        names = [n for n, _ in c.FRAME_TYPES[name].SPEC]
        for row in rows:
            assert row.row_sha256() == c.sha256_hex(c.row_encoding(row.to_record(), names))
    m = qualified.manifest
    receipt = m.receipt()
    assert receipt is not None
    assert m["validation_digest"] == "sha256:" + c.sha256_hex(
        c.row_encoding(receipt.to_record(), [n for n, _ in c.ValidationReceipt.SPEC]))
    assert m["fingerprint_digest"] == "sha256:" + c.sha256_hex(
        c.row_encoding(c.fingerprint_payload(m.values), c.FINGERPRINT_FIELDS))


def test_sql_frame_spec_block_is_the_rendering_of_the_row_specs():
    text = c.SQL_PATHS[1].read_text(encoding="utf-8").replace("\r\n", "\n")
    assert text.count(c.SQL_FRAME_SPEC_BEGIN) == 1 and text.count(c.SQL_FRAME_SPEC_END) == 1
    assert c.render_sql_frame_specs() in text
    specs = c.sql_frame_specs()
    assert set(specs) == set(c.FRAME_TYPES) | {"validation_receipt", "fingerprint"}
    assert specs["validation_receipt"][5] == "t:issued_at" and "t:knowledge_cutoff" in specs["fingerprint"]


def test_manifest_derivations_are_enforced(qualified):
    values = dict(qualified.manifest.values)
    for field, bad in (
        ("publication_id", uuid.uuid4()),
        ("fingerprint_digest", "sha256:" + "1" * 64),
        ("validation_digest", "sha256:" + "2" * 64),
        ("product", "other"),
    ):
        with pytest.raises(c.ContractError):
            c.BundleManifest({**values, field: bad})
    with pytest.raises(c.ContractError, match="validation_receipt_all_or_none"):
        c.BundleManifest({**values, "validation_scope": None})


def test_bundle_frames_must_be_canonically_ordered(qualified):
    frames = dict(qualified.frames)
    frames["ratings"] = tuple(reversed(frames["ratings"]))
    with pytest.raises(c.ContractError, match="sorted_unique"):
        c.CreditBundle(qualified.manifest, qualified.panel_grid, frames)
    with pytest.raises(c.ContractError, match="panel_grid"):
        c.CreditBundle(qualified.manifest, tuple(reversed(qualified.panel_grid)), qualified.frames)


def test_decoded_bundles_are_frozen_and_verifiable(qualified):
    decoded = c.CreditBundle.from_json_obj(qualified.to_json_obj())
    assert decoded.canonical_bytes() == qualified.canonical_bytes()
    decoded.verify_frames_against_manifest()
    for mapping in (decoded.frames, decoded.manifest.values, decoded.frames["source_packages"][0].member_sha256s,
                    decoded.frames["observations"][0].field_presence,
                    decoded.manifest["rating_declarations"],
                    decoded.manifest["rating_declarations"]["rating_scopes"][0]):
        with pytest.raises(TypeError):
            mapping["x"] = None  # type: ignore[index]
    assert isinstance(decoded.frames["events"], tuple) and isinstance(decoded.panel_grid, tuple)


def test_frame_verification_detects_divergence_from_the_manifest(qualified):
    frames = dict(qualified.frames)
    frames["ratings"] = frames["ratings"][:-1]
    with pytest.raises(c.ContractError, match="ratings:count_does_not_match_manifest"):
        c.CreditBundle(qualified.manifest, qualified.panel_grid, frames).verify_frames_against_manifest()
    with pytest.raises(c.ContractError, match="panel_grid_does_not_match_manifest"):
        c.CreditBundle(qualified.manifest, qualified.panel_grid[:-1], qualified.frames).verify_frames_against_manifest()


def _regen_module():
    spec = importlib.util.spec_from_file_location(
        "regen_bond_default_contracts", ROOT / "scripts" / "regen_bond_default_contracts.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_regeneration_check_reports_no_stale_artifact():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "regen_bond_default_contracts.py"), "--check"],
        capture_output=True, text=True, timeout=300, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_regeneration_check_flags_divergent_bytes(tmp_path):
    regen = _regen_module()
    current = tmp_path / "current.json"
    current.write_bytes(b'{\r\n  "a": 1\r\n}\r\n')
    stale = tmp_path / "stale.json"
    stale.write_bytes(b"{}\n")
    missing = tmp_path / "missing.json"
    outputs = {current: b'{\n  "a": 1\n}\n', stale: b'{\n  "a": 1\n}\n', missing: b"{}\n"}
    assert regen.stale_paths(outputs) == [stale, missing]
    assert set(regen.expected_outputs()) == {
        c.POLICY_PATH, c.SCHEMA_PATH, c.SQL_PATHS[1], ROOT / "src" / "bonds" / "default_events" / "contracts.py",
        *(FIXTURES / name for name in syn.FIXTURES),
    }


def test_schema_rejects_structural_violations(qualified):
    payload = json.loads(json.dumps(qualified.to_json_obj()))
    c.validate_against_schema(payload)
    cases = []
    extra = json.loads(json.dumps(payload))
    extra["frames"]["events"][0]["unexpected"] = 1
    cases.append(extra)
    missing = json.loads(json.dumps(payload))
    del missing["frames"]["ratings"][0]["state"]
    cases.append(missing)
    enum = json.loads(json.dumps(payload))
    enum["frames"]["adjudications"][0]["status"] = "accepted"
    cases.append(enum)
    ts = json.loads(json.dumps(payload))
    ts["manifest"]["knowledge_cutoff"] = "2026-09-25T00:00:00+00:00"
    cases.append(ts)
    frame = json.loads(json.dumps(payload))
    frame["frames"]["unknown_frame"] = []
    cases.append(frame)
    no_declarations = json.loads(json.dumps(payload))
    del no_declarations["manifest"]["rating_declarations"]
    cases.append(no_declarations)
    for case in cases:
        with pytest.raises(c.ContractError, match="schema_violation"):
            c.validate_against_schema(case)


def test_rating_declaration_manifest_rejects_malformed_or_mismatched_records(qualified):
    values = dict(qualified.manifest.values)
    malformed = {
        "version": c.RATING_DECLARATIONS_VERSION,
        "rating_scopes": [{"agency_name": "SYNTHETIC-AGENCY", "rating_type": "long_term", "scale": "global"}],
        "uncleared_rating_sources": [{
            "source_ref": "SYNTHETIC-MIRROR-1",
            "rights_state": "unverified",
            "coverage_start": "2026-02-30",
            "coverage_end": "2026-03-31",
        }],
    }
    with pytest.raises(c.ContractError, match="rating_declarations"):
        c.BundleManifest({**values, "rating_declarations": malformed})
    with pytest.raises(c.ContractError, match="rating_declarations_digest_not_derived"):
        c.BundleManifest({**values, "rating_declarations_digest": "sha256:" + "0" * 64})


def test_bundle_json_roundtrip_is_byte_stable(qualified):
    text = json.dumps(qualified.to_json_obj(), indent=3)
    decoded = c.CreditBundle.from_json_bytes(text.encode("utf-8"))
    assert decoded.canonical_bytes() == qualified.canonical_bytes()
    assert decoded.bundle_digest() == qualified.bundle_digest()
