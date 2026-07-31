"""An activated state is representable — in the contract and in the calibration.

Two closed loops are dissolved here.

**Contract.** `contracts/quant-engine/v2` pinned the blocked state into the
schema with `const`: `runtime_activation: {const: false}`, `a5_status: {const:
"blocked"}`, `db_write: {const: "none"}`, and so on. The `oneOf` had three
variants and none of them admitted an activated result, so a job that produced
`runtime_activation: true` was invalid *by contract*. `db_write: "none"` as a
`const` does not prevent a bad write — it prevents any write. v3 replaces those
pins with the real domains and moves the decision to the execution envelope,
where `preflight.validate_runtime_mode` checks REPORTED against DECLARED. Still
fail-closed, both directions.

**Calibration.** `src/calibration_candidate.py` carried five institutional
limits as the string literal `"explicitly_unset"`, a rejection rule whose only
trigger was that they were unset, and `final_approval_allowed: False` written as
a literal. Nothing could set them, so approval was blocked by an unsatisfiable
condition. The limits are configuration now and the verdict is derived.

v1 and v2 stay byte-frozen: the live certified packs are hash-bound to v2.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "contracts" / "quant-engine" / "v2"
V3 = ROOT / "contracts" / "quant-engine" / "v3"

sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))

from investintell_quant_engine import preflight  # noqa: E402
from src import calibration_candidate as cc  # noqa: E402


def _schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _fixture(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# The contract: no `const`-pinned governance flag survives in v3.
# --------------------------------------------------------------------------- #
_BLOCKED_BY_CONST = (
    "runtime_activation",
    "freeze_ready",
    "a5_status",
    "official_result",
    "allocator_publish",
    "db_write",
    "production_endpoint_activation",
)


@pytest.mark.parametrize("field", _BLOCKED_BY_CONST)
def test_v3_result_schema_has_no_const_pinned_governance_flag(field: str) -> None:
    schema = _schema(V3 / "job-result.schema.json")
    for variant, definition in schema["$defs"].items():
        spec = definition.get("properties", {}).get(field)
        if spec is None:
            continue
        assert "const" not in spec, (
            f"{variant}.{field} is still const-pinned in v3: an activated result "
            "would be invalid by contract, which is a governance decision the "
            "schema must not make"
        )
        assert "enum" in spec or spec.get("type") == "boolean", (
            f"{variant}.{field} must declare a real domain (enum or boolean)"
        )


def test_v3_engine_manifest_runtime_activation_is_a_boolean() -> None:
    schema = _schema(V3 / "engine-manifest.schema.json")
    assert schema["properties"]["runtime_activation"] == {"type": "boolean"}


def test_v3_keeps_offline_as_a_real_invariant() -> None:
    """No-network is a property of the engine, not a governance flag; it stays."""
    assert _schema(V3 / "engine-manifest.schema.json")["properties"]["offline"] == {"const": True}
    request = _schema(V3 / "job-request.schema.json")
    for definition in request["$defs"].values():
        spec = definition.get("properties", {}).get("offline")
        if spec is not None:
            assert spec == {"const": True}


@pytest.mark.parametrize(
    "fixture_name",
    [
        "job-result.activated.json",
        "job-result.metric-backtest.activated.json",
        "engine-manifest.activated.json",
    ],
)
def test_an_activated_artifact_validates_under_v3(fixture_name: str) -> None:
    payload = _fixture(V3 / "fixtures" / "valid" / fixture_name)
    name = (
        "engine-manifest.schema.json"
        if fixture_name.startswith("engine-manifest")
        else "job-result.schema.json"
    )
    jsonschema.validate(payload, _schema(V3 / name))


def test_the_same_activated_result_is_refused_by_v2() -> None:
    """The delta is real: v2 could not express it at all."""
    payload = _fixture(V3 / "fixtures" / "valid" / "job-result.activated.json")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V2 / "job-result.schema.json"))


@pytest.mark.parametrize(
    "fixture_name",
    ["job-result.a5-status-out-of-enum.json", "job-result.db-write-out-of-enum.json"],
)
def test_v3_still_refuses_values_outside_the_declared_domain(fixture_name: str) -> None:
    payload = _fixture(V3 / "fixtures" / "invalid" / fixture_name)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V3 / "job-result.schema.json"))


def test_v3_schema_ids_are_bumped() -> None:
    for name in ("job-request.schema.json", "job-result.schema.json", "engine-manifest.schema.json"):
        assert "/v3/" in _schema(V3 / name)["$id"], name


def test_v1_and_v2_bundles_stay_verifiable_and_untouched() -> None:
    """Editing v2 in place would falsify every certified pack's contract pin."""
    from investintell_quant_engine.contract_bundle import verify_bundle

    for version, expected in (
        ("v1", "sha256:4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"),
        ("v2", "sha256:db85c58968becd890d49d0a022b54b9493449e8c9ff444c88da10678c5d6f53b"),
    ):
        result = verify_bundle(ROOT / "contracts" / "quant-engine" / version)
        assert result["ok"], result
        assert result["bundle_sha256"] == expected


def test_v3_bundle_verifies() -> None:
    from investintell_quant_engine.contract_bundle import verify_bundle

    result = verify_bundle(V3)
    assert result["ok"], result
    assert result["contract_version"] == "3.0.0"


# --------------------------------------------------------------------------- #
# Preflight: the decision moved to the envelope, and is still fail-closed.
# --------------------------------------------------------------------------- #
_OFFLINE_REPORT = {
    "runtime_activation": False,
    "freeze_ready": False,
    "a5_status": "blocked",
    "official_result": False,
    "allocator_publish": False,
    "db_write": "none",
    "production_endpoint_activation": "none",
}
_ACTIVATED_REPORT = {
    "runtime_activation": True,
    "freeze_ready": True,
    "a5_status": "active",
    "official_result": True,
    "allocator_publish": True,
    "db_write": "publication",
    "production_endpoint_activation": "live",
}


def test_offline_evidence_mode_accepts_a_blocked_report() -> None:
    preflight.validate_runtime_mode(_OFFLINE_REPORT, mode=preflight.OFFLINE_EVIDENCE)


def test_activated_mode_accepts_an_activated_report() -> None:
    preflight.validate_runtime_mode(_ACTIVATED_REPORT, mode=preflight.ACTIVATED)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("runtime_activation", True),
        ("a5_status", "active"),
        ("official_result", True),
        ("allocator_publish", True),
        ("db_write", "publication"),
        ("production_endpoint_activation", "live"),
    ],
)
def test_offline_evidence_mode_still_refuses_any_activation(field: str, value) -> None:
    """The old guarantee, unchanged: an offline job cannot report activation."""
    report = dict(_OFFLINE_REPORT, **{field: value})
    with pytest.raises(preflight.RuntimeModeError, match="runtime mode inconsistency"):
        preflight.validate_runtime_mode(report, mode=preflight.OFFLINE_EVIDENCE)


@pytest.mark.parametrize(
    ("field", "value"),
    [("runtime_activation", False), ("a5_status", "blocked"), ("db_write", "none")],
)
def test_activated_mode_refuses_an_inconsistent_report(field: str, value) -> None:
    """Fail-closed in the other direction too: the inconsistency is the error."""
    report = dict(_ACTIVATED_REPORT, **{field: value})
    with pytest.raises(preflight.RuntimeModeError, match="runtime mode inconsistency"):
        preflight.validate_runtime_mode(report, mode=preflight.ACTIVATED)


def test_the_default_mode_is_the_conservative_one() -> None:
    with pytest.raises(preflight.RuntimeModeError):
        preflight.validate_runtime_mode(_ACTIVATED_REPORT)


def test_an_unknown_mode_is_refused() -> None:
    with pytest.raises(preflight.RuntimeModeError, match="unknown runtime mode"):
        preflight.validate_runtime_mode(_OFFLINE_REPORT, mode="whatever")


def test_validate_runtime_disabled_still_behaves_for_existing_callers() -> None:
    preflight.validate_runtime_disabled(_OFFLINE_REPORT)
    with pytest.raises(ValueError):
        preflight.validate_runtime_disabled(_ACTIVATED_REPORT)


# --------------------------------------------------------------------------- #
# Fail-closed on SILENCE, not just on contradiction (review threads 1 and 4).
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "report",
    [
        {},
        {"runtime_activation": False},
        {"a5_status": "blocked"},
        {"freeze_ready": False},
    ],
    ids=["empty", "only-runtime-activation", "only-a5", "only-freeze-ready"],
)
def test_a_partial_report_cannot_assert_a_mode(report: dict) -> None:
    """An absent governance field is not an assertion. `{}` must never validate."""
    with pytest.raises(preflight.RuntimeModeError, match="is missing"):
        preflight.validate_runtime_mode(report, mode=preflight.OFFLINE_EVIDENCE)


@pytest.mark.parametrize(
    "report",
    [{}, {"runtime_activation": False}, {"runtime_activation": False, "a5_status": "blocked"}],
    ids=["empty", "partial", "missing-freeze-ready"],
)
def test_the_legacy_guard_still_rejects_missing_governance_fields(report: dict) -> None:
    """`validate_runtime_disabled` kept its exact contract: present AND off.

    The old body was `report.get(...) is not False`, so a missing field raised.
    This is the guard the quant-engine runners depend on; it must not have become
    permissive when it gained a mode.
    """
    with pytest.raises(ValueError, match="is missing"):
        preflight.validate_runtime_disabled(report)


@pytest.mark.parametrize(
    "omitted",
    ["official_result", "allocator_publish", "db_write", "production_endpoint_activation"],
)
def test_a_half_declared_publication_result_is_refused(omitted: str) -> None:
    """The publication flags travel together or not at all."""
    report = {k: v for k, v in _ACTIVATED_REPORT.items() if k != omitted}
    with pytest.raises(preflight.RuntimeModeError, match="partially declared"):
        preflight.validate_runtime_mode(report, mode=preflight.ACTIVATED)


def test_a_shape_without_publication_flags_is_still_assertable() -> None:
    """A parity result has no publication flags; absence of the whole set is fine."""
    preflight.validate_runtime_mode(
        {"runtime_activation": True, "a5_status": "active"}, mode=preflight.ACTIVATED
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("allocator_publish", False),
        ("production_endpoint_activation", "none"),
        ("freeze_ready", False),
    ],
)
def test_activated_mode_refuses_deactivated_publication_values(field: str, value) -> None:
    report = dict(_ACTIVATED_REPORT, **{field: value})
    with pytest.raises(preflight.RuntimeModeError, match="runtime mode inconsistency"):
        preflight.validate_runtime_mode(report, mode=preflight.ACTIVATED)


def test_offline_mode_refuses_a_productive_classification() -> None:
    """v3 widened `classification`; that must not let offline evidence self-label."""
    report = dict(_OFFLINE_REPORT, classification="productive_result")
    with pytest.raises(preflight.RuntimeModeError, match="forbidden"):
        preflight.validate_runtime_mode(report, mode=preflight.OFFLINE_EVIDENCE)


def test_activated_mode_refuses_an_evidence_only_classification() -> None:
    report = dict(_ACTIVATED_REPORT, classification="metric_evidence_only")
    with pytest.raises(preflight.RuntimeModeError, match="forbidden"):
        preflight.validate_runtime_mode(report, mode=preflight.ACTIVATED)


def test_the_bundled_activated_fixtures_satisfy_activated_mode() -> None:
    """A fixture named `activated` must survive the activated envelope.

    It shipped with `a5_status: "blocked"`, so the canonical activated example
    contradicted the mode it was meant to demonstrate.
    """
    for name in ("job-result.activated.json", "job-result.metric-backtest.activated.json"):
        payload = _fixture(V3 / "fixtures" / "valid" / name)
        preflight.validate_runtime_mode(payload, mode=preflight.ACTIVATED)


def test_the_bundled_offline_fixtures_satisfy_offline_mode() -> None:
    for name in ("job-result.passed.json", "job-result.metric-backtest.json"):
        payload = _fixture(V3 / "fixtures" / "valid" / name)
        preflight.validate_runtime_mode(payload, mode=preflight.OFFLINE_EVIDENCE)


# --------------------------------------------------------------------------- #
# Contradictory states must not validate (review threads 11, 12, 13).
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "fixture_name",
    [
        "job-result.dry-run-failed-but-verified.json",
        "job-result.dry-run-verified-with-errors.json",
        "job-result.metric-backtest-productive-but-failed.json",
        "job-result.metric-backtest-evidence-but-publishing.json",
        "job-result.parity-activated-but-a5-blocked.json",
    ],
)
def test_v3_refuses_contradictory_states(fixture_name: str) -> None:
    """Widening an enum must not make nonsense expressible.

    A failed dry run that calls itself verified, a verified dry run that reports
    errors, a productive backtest that failed, evidence-only output that writes a
    publication, an activated parity run with A5 blocked — each of these was
    accepted once the `const` pins became enums, and each is now tied together by
    an if/then rule.
    """
    payload = _fixture(V3 / "fixtures" / "invalid" / fixture_name)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V3 / "job-result.schema.json"))


# --------------------------------------------------------------------------- #
# Request <-> result pairing, and the sleeve a productive run consumes.
# --------------------------------------------------------------------------- #
def test_an_activated_request_may_declare_an_approved_sleeve() -> None:
    """The product fix: publication no longer has to disown its own input.

    `sleeve.status` was pinned to `candidate_not_approved` with a `const`, so the
    only expressible metric-backtest request declared the sleeve UNAPPROVED —
    including the request that pairs with a `productive_result` publishing live
    outputs. A run that publishes must be able to say the sleeve was approved.
    """
    request = _fixture(V3 / "fixtures" / "valid" / "job-request.metric-backtest.activated.json")
    jsonschema.validate(request, _schema(V3 / "job-request.schema.json"))
    assert request["runtime_mode"] == "activated"
    assert request["sleeve"]["status"] == "approved"


def test_an_offline_request_still_requires_the_candidate_sleeve() -> None:
    """Both directions: evidence cannot borrow production's approval."""
    payload = _fixture(
        V3
        / "fixtures"
        / "invalid"
        / "job-request.metric-backtest-offline-with-approved-sleeve.json"
    )
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V3 / "job-request.schema.json"))


def test_an_activated_request_refuses_an_unapproved_sleeve() -> None:
    payload = _fixture(
        V3
        / "fixtures"
        / "invalid"
        / "job-request.metric-backtest-activated-with-unapproved-sleeve.json"
    )
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V3 / "job-request.schema.json"))


def test_an_unstated_request_mode_is_the_conservative_one() -> None:
    request = _fixture(V3 / "fixtures" / "valid" / "job-request.metric-backtest.json")
    assert "runtime_mode" not in request
    assert preflight.request_runtime_mode(request) == preflight.OFFLINE_EVIDENCE
    assert request["sleeve"]["status"] == "candidate_not_approved"


@pytest.mark.parametrize(
    ("request_name", "result_name"),
    [
        ("job-request.metric-backtest.json", "job-result.metric-backtest.json"),
        (
            "job-request.metric-backtest.activated.json",
            "job-result.metric-backtest.activated.json",
        ),
        ("job-request.metric-backtest.offline-explicit.json", "job-result.metric-backtest.json"),
    ],
)
def test_matching_request_and_result_pair_cleanly(request_name: str, result_name: str) -> None:
    preflight.validate_request_result_pair(
        _fixture(V3 / "fixtures" / "valid" / request_name),
        _fixture(V3 / "fixtures" / "valid" / result_name),
    )


@pytest.mark.parametrize(
    ("request_name", "result_name"),
    [
        ("job-request.metric-backtest.json", "job-result.metric-backtest.activated.json"),
        ("job-request.metric-backtest.activated.json", "job-result.metric-backtest.json"),
    ],
    ids=["offline-request-productive-result", "activated-request-evidence-result"],
)
def test_a_mismatched_pair_is_refused(request_name: str, result_name: str) -> None:
    """Either half can be self-consistent while the PAIR is nonsense."""
    with pytest.raises(preflight.RuntimeModeError):
        preflight.validate_request_result_pair(
            _fixture(V3 / "fixtures" / "valid" / request_name),
            _fixture(V3 / "fixtures" / "valid" / result_name),
        )


def test_a_request_without_a_sleeve_status_cannot_assert_a_mode() -> None:
    request = _fixture(V3 / "fixtures" / "valid" / "job-request.metric-backtest.activated.json")
    del request["sleeve"]["status"]
    with pytest.raises(preflight.RuntimeModeError, match="declares no sleeve status"):
        preflight.validate_sleeve_governance(request)


# --------------------------------------------------------------------------- #
# The dry-run variant couples its governance fields like the others.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "fixture_name",
    [
        "job-result.dry-run-activated-but-a5-blocked.json",
        "job-result.dry-run-blocked-but-freeze-ready.json",
    ],
)
def test_the_dry_run_variant_refuses_split_governance(fixture_name: str) -> None:
    """`runtime_activation: true` with `a5_status: "blocked"` used to validate."""
    payload = _fixture(V3 / "fixtures" / "invalid" / fixture_name)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V3 / "job-result.schema.json"))


# --------------------------------------------------------------------------- #
# Baseline certification evidence is ADMISSIBLE, not merely described.
# --------------------------------------------------------------------------- #
def _golden_manifest() -> dict:
    return json.loads(
        (
            ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack" / "manifest.json"
        ).read_text(encoding="utf-8")
    )


def _pack_manifest_schema() -> dict:
    return json.loads(
        (ROOT / "schemas" / "input_packs" / "input_pack_manifest.schema.json").read_text(
            encoding="utf-8"
        )
    )


def test_the_hashed_config_scope_matches_what_the_image_copies() -> None:
    """Hash scope and COPY scope must be the same set, in BOTH directions.

    Hashing less than the image ships lets a sibling change the image under a
    still-valid hash. Shipping more than calibration needs drags unrelated sweep
    configs into the pin — and broadening BOTH to `configs/` is what silently
    invalidated the committed calibration evidence, since ten a31/a4 YAMLs already
    lived there at the recorded engine commit.
    """
    assert "configs/calibration" in cc.DOCKER_CONTEXT_PATHS
    assert "configs" not in cc.DOCKER_CONTEXT_PATHS
    for dockerfile in ("docker/quant-engine/Dockerfile", "docker/railway-ci/Dockerfile"):
        text = (ROOT / dockerfile).read_text(encoding="utf-8")
        assert "COPY configs/calibration /app/configs/calibration" in text
        assert "COPY configs /app/configs" not in text


def test_the_committed_calibration_evidence_still_validates_its_context_hash() -> None:
    """The recorded docker_context_sha256 must still recompute at its own commit.

    `verify_calibration_artifacts.py` never calls `validate_docker_context_sha256`
    — it only checks file digests and the two `ok` flags — so a stale context hash
    passes that gate silently and only surfaces when someone re-runs calibration
    with the recorded manifest values. This is the check that would have caught it.
    """
    manifest = json.loads(
        (
            ROOT
            / "artifacts"
            / "calibration"
            / "open_macro_v03_calibration_001"
            / "calibration_manifest.json"
        ).read_text(encoding="utf-8")
    )
    recomputed = cc.committed_docker_context_sha256(manifest["engine_commit"])
    assert manifest["docker_context_sha256"] == recomputed, (
        "the committed calibration evidence records a context hash that no longer "
        "recomputes at its engine_commit; changing DOCKER_CONTEXT_PATHS invalidated "
        "measured evidence"
    )


def _pack_with_baselines(tmp_path: Path, entries, *, place: bool = True, content=None) -> Path:
    """A copy of the golden pack, optionally carrying real baseline artifacts.

    ``entries`` receives the pack root and returns the manifest declaration, so a
    test can declare something other than what it placed. ``content`` maps a
    reference id to the artifact body, so a test can make two artifacts share
    bytes — and therefore a digest.
    """
    from src.input_packs.hashing import file_sha256
    from src.input_packs.manifest import build_manifest

    pack = tmp_path / "pack"
    shutil.copytree(
        ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack", pack
    )
    if place:
        rels = []
        for reference_id in cc.BASELINE_REFERENCE_IDS:
            rel = cc.baseline_artifact_path(reference_id)
            (pack / rel).parent.mkdir(parents=True, exist_ok=True)
            body = (
                content(reference_id)
                if content is not None
                else json.dumps({"reference_id": reference_id}, indent=2) + "\n"
            )
            (pack / rel).write_text(body, encoding="utf-8")
            rels.append(rel)
        table = json.loads((pack / "table_hashes.json").read_text(encoding="utf-8"))
        for rel in rels:
            table["tables"].append(
                {
                    "name": f"baseline:{Path(rel).stem}",
                    "path": rel,
                    "rows": 1,
                    "sha256": file_sha256(pack / rel),
                }
            )
        (pack / "table_hashes.json").write_text(
            json.dumps(table, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    manifest = json.loads((pack / "manifest.json").read_text(encoding="utf-8"))
    manifest["certified_baseline_references"] = entries(pack)
    manifest = build_manifest(pack, manifest)
    (pack / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return pack


def _honest_entries(pack: Path) -> list[dict]:
    from src.input_packs.hashing import file_sha256

    return [
        {
            "reference_id": reference_id,
            "path": cc.baseline_artifact_path(reference_id),
            "sha256": file_sha256(pack / cc.baseline_artifact_path(reference_id)),
        }
        for reference_id in cc.BASELINE_REFERENCE_IDS
    ]


def test_a_pack_carrying_real_baseline_artifacts_certifies_and_still_verifies(
    tmp_path: Path,
) -> None:
    """The positive case has to actually work, and the pack must still verify.

    The namespace is under `reports/` rather than `data/` on purpose: the P0 data
    layer is exactly the nine source tables plus derived features, and the
    verifier rejects anything else under `data/`. A certified reference is
    certification evidence, which is what `reports/` already carries — so this
    needs no verifier change and no measured surface moves.
    """
    from src.input_packs.verifier import verify_pack

    pack = _pack_with_baselines(tmp_path, _honest_entries)
    assert cc.pack_certifies_baseline_references(pack) is True
    assert verify_pack(pack)["ok"] is True, "a certified pack must remain verifiable"


def test_the_committed_pack_certifies_nothing() -> None:
    """Fail-closed for the undeclared: today's pack blocks, by reading."""
    assert (
        cc.pack_certifies_baseline_references(
            ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack"
        )
        is False
    )


def test_a_fabricated_baseline_digest_does_not_certify_the_pack(tmp_path: Path) -> None:
    """A digest must name bytes the pack carries — not any well-formed hex."""
    pack = _pack_with_baselines(
        tmp_path,
        lambda _pack: [
            {
                "reference_id": reference_id,
                "path": cc.baseline_artifact_path(reference_id),
                "sha256": "a" * 64,
            }
            for reference_id in cc.BASELINE_REFERENCE_IDS
        ],
    )
    assert cc.pack_certifies_baseline_references(pack) is False


def test_a_declaration_without_baseline_artifacts_does_not_certify(tmp_path: Path) -> None:
    """A manifest-only claim certifies nothing when no baseline content exists."""
    pack = _pack_with_baselines(
        tmp_path,
        lambda _pack: [
            {
                "reference_id": reference_id,
                "path": cc.baseline_artifact_path(reference_id),
                "sha256": "b" * 64,
            }
            for reference_id in cc.BASELINE_REFERENCE_IDS
        ],
        place=False,
    )
    assert cc.pack_certifies_baseline_references(pack) is False


def test_the_digest_of_a_non_baseline_artifact_does_not_certify(tmp_path: Path) -> None:
    """THE fail-open this closes.

    Membership alone let the digest of any carried artifact — an ordinary
    canonical table, even a schema file — be mapped to the three reference ids and
    certify a pack holding no baseline content at all.
    """

    def borrowed(pack: Path) -> list[dict]:
        table = json.loads((pack / "table_hashes.json").read_text(encoding="utf-8"))
        other = next(
            entry["sha256"]
            for entry in table["tables"]
            if entry["path"].startswith("data/canonical/")
        )
        return [
            {
                "reference_id": reference_id,
                "path": cc.baseline_artifact_path(reference_id),
                "sha256": other,
            }
            for reference_id in cc.BASELINE_REFERENCE_IDS
        ]

    assert cc.pack_certifies_baseline_references(_pack_with_baselines(tmp_path, borrowed)) is False


def test_a_path_outside_the_baseline_namespace_does_not_certify(tmp_path: Path) -> None:
    def outside(pack: Path) -> list[dict]:
        from src.input_packs.hashing import file_sha256

        return [
            {
                "reference_id": reference_id,
                "path": f"data/canonical/{reference_id}.json",
                "sha256": file_sha256(pack / cc.baseline_artifact_path(reference_id)),
            }
            for reference_id in cc.BASELINE_REFERENCE_IDS
        ]

    assert cc.pack_certifies_baseline_references(_pack_with_baselines(tmp_path, outside)) is False


def test_a_reference_aimed_at_another_references_artifact_does_not_certify(
    tmp_path: Path,
) -> None:
    """The path is DERIVED from the id, so it cannot be aimed elsewhere."""

    def swapped(pack: Path) -> list[dict]:
        from src.input_packs.hashing import file_sha256

        ids = list(cc.BASELINE_REFERENCE_IDS)
        return [
            {
                "reference_id": ids[i],
                "path": cc.baseline_artifact_path(ids[(i + 1) % len(ids)]),
                "sha256": file_sha256(pack / cc.baseline_artifact_path(ids[(i + 1) % len(ids)])),
            }
            for i in range(len(ids))
        ]

    assert cc.pack_certifies_baseline_references(_pack_with_baselines(tmp_path, swapped)) is False


def test_a_declared_artifact_whose_bytes_are_gone_does_not_certify(tmp_path: Path) -> None:
    """The digest is re-checked against the bytes, not just against the table."""
    pack = _pack_with_baselines(tmp_path, _honest_entries)
    assert cc.pack_certifies_baseline_references(pack) is True

    # `file_sha256` canonicalizes *.json, so whitespace is deliberately not a
    # mutation; bytes that are gone are.
    (pack / cc.baseline_artifact_path(cc.BASELINE_REFERENCE_IDS[0])).unlink()
    assert cc.pack_certifies_baseline_references(pack) is False


def test_baseline_artifacts_with_identical_bytes_both_certify(tmp_path: Path) -> None:
    """Two references may legitimately share content — and therefore a digest.

    The proven set was a `{digest: path}` map, so the second artifact evicted the
    first and its reference was rejected even though `table_hashes.json` declared
    and verified BOTH paths. Membership is by `(digest, path)` pair now.
    """
    from src.input_packs.hashing import file_sha256
    from src.input_packs.verifier import verify_pack

    pack = _pack_with_baselines(
        tmp_path,
        _honest_entries,
        content=lambda _reference_id: json.dumps({"baseline": "identical"}, indent=2) + "\n",
    )

    digests = {
        file_sha256(pack / cc.baseline_artifact_path(reference_id))
        for reference_id in cc.BASELINE_REFERENCE_IDS
    }
    assert len(digests) == 1, "this test is meaningless unless the digests collide"

    assert cc.pack_certifies_baseline_references(pack) is True
    assert verify_pack(pack)["ok"] is True


def test_a_borrowed_digest_is_still_refused_when_digests_collide(tmp_path: Path) -> None:
    """Pair membership must not have loosened the check it replaced."""

    def borrowed(pack: Path) -> list[dict]:
        table = json.loads((pack / "table_hashes.json").read_text(encoding="utf-8"))
        other = next(
            entry["sha256"]
            for entry in table["tables"]
            if entry["path"].startswith("data/canonical/")
        )
        return [
            {
                "reference_id": reference_id,
                "path": cc.baseline_artifact_path(reference_id),
                "sha256": other,
            }
            for reference_id in cc.BASELINE_REFERENCE_IDS
        ]

    pack = _pack_with_baselines(
        tmp_path,
        borrowed,
        content=lambda _reference_id: json.dumps({"baseline": "identical"}, indent=2) + "\n",
    )
    assert cc.pack_certifies_baseline_references(pack) is False


def test_the_manifest_schema_requires_the_namespaced_path() -> None:
    schema = _pack_manifest_schema()
    base = _golden_manifest()
    jsonschema.validate(base, schema)

    def declared(entries: list[dict]) -> dict:
        return dict(base, certified_baseline_references=entries)

    jsonschema.validate(
        declared(
            [
                {
                    "reference_id": name,
                    "path": cc.baseline_artifact_path(name),
                    "sha256": "a" * 64,
                }
                for name in cc.BASELINE_REFERENCE_IDS
            ]
        ),
        schema,
    )

    for bad in (
        [{"reference_id": n, "sha256": "a" * 64} for n in cc.BASELINE_REFERENCE_IDS],
        [
            {"reference_id": n, "path": f"data/baselines/{n}.json", "sha256": "a" * 64}
            for n in cc.BASELINE_REFERENCE_IDS
        ],
        [
            {"reference_id": n, "path": cc.baseline_artifact_path(n), "sha256": "nope"}
            for n in cc.BASELINE_REFERENCE_IDS
        ],
    ):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(declared(bad), schema)


@pytest.mark.parametrize("mode", ["activated", "offline_evidence"])
def test_a_pair_with_no_sleeve_object_is_refused(mode: str) -> None:
    """Omitting the whole object must not skip the governance it carries.

    The guard was gated on `"sleeve" in request`, so an activated request with the
    entire object omitted skipped sleeve governance and paired happily with a
    productive result — fail-open on the exact field it exists to catch.
    """
    name = (
        "job-request.metric-backtest.activated.json"
        if mode == "activated"
        else "job-request.metric-backtest.json"
    )
    result_name = (
        "job-result.metric-backtest.activated.json"
        if mode == "activated"
        else "job-result.metric-backtest.json"
    )
    request = _fixture(V3 / "fixtures" / "valid" / name)
    del request["sleeve"]
    with pytest.raises(preflight.RuntimeModeError, match="sleeve"):
        preflight.validate_request_result_pair(
            request, _fixture(V3 / "fixtures" / "valid" / result_name)
        )


# --------------------------------------------------------------------------- #
# Calibration: institutional limits are parameters, the verdict is derived.
# --------------------------------------------------------------------------- #
def test_no_explicitly_unset_literal_survives_in_the_calibration_module() -> None:
    """No limit is ASSIGNED the unsatisfiable literal any more.

    The module docstring still explains what the literal was; what must not come
    back is a limit whose value is that string.
    """
    source = (ROOT / "src" / "calibration_candidate.py").read_text(encoding="utf-8")
    for number, line in enumerate(source.splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        assert ': "explicitly_unset"' not in line and '= "explicitly_unset"' not in line, (
            f"src/calibration_candidate.py:{number} still assigns the unsatisfiable "
            f"literal: {stripped!r}. Institutional limits are configuration now "
            "(configs/calibration/institutional_limits.json)."
        )


def test_the_mandate_is_configured_and_numeric() -> None:
    limits = cc.load_institutional_limits()
    assert set(limits) == {
        "daily_cvar_95",
        "beta",
        "max_drawdown",
        "turnover",
        "exposure_bounds",
    }
    for name, spec in limits.items():
        assert isinstance(spec["limit"], (int, float)), name


def test_a_missing_config_reports_unset_instead_of_passing(tmp_path: Path) -> None:
    """Removing the mandate does not open approval; it blocks, per limit, honestly."""
    limits = cc.load_institutional_limits(tmp_path / "absent.json")
    assert limits == {}

    evaluation = cc.evaluate_institutional_limits(_rows(), limits)
    assert set(evaluation) == set(cc.REQUIRED_INSTITUTIONAL_LIMITS)
    assert {e["status"] for e in evaluation.values()} == {"unset"}
    assert cc.final_approval_blockers(evaluation, baseline_references_certified=True) == [
        f"institutional_limit_{name}_unset"
        for name in sorted(cc.REQUIRED_INSTITUTIONAL_LIMITS)
    ]


@pytest.mark.parametrize("omitted", cc.REQUIRED_INSTITUTIONAL_LIMITS)
def test_an_omitted_mandate_entry_blocks_like_a_null_one(omitted: str) -> None:
    """Silence in the config file is not consent.

    Omitting a key produced no evaluation entry at all, so it produced no blocker:
    a mandate with only the measurable turnover limit left would have returned an
    empty blocker list and opened final approval.
    """
    limits = {k: v for k, v in cc.load_institutional_limits().items() if k != omitted}
    evaluation = cc.evaluate_institutional_limits(_rows(), limits)
    assert evaluation[omitted]["status"] == "unset"
    assert f"institutional_limit_{omitted}_unset" in cc.final_approval_blockers(
        evaluation, baseline_references_certified=True
    )


def test_a_violated_limit_fails_the_invariant_report(tmp_path: Path) -> None:
    """A breach must flip the BOOLEAN, not just carry a status string.

    `verify_calibration_artifacts.py` only reads `invariant["ok"]`, so a violation
    recorded solely as text would have shipped as a passing calibration.
    """
    rows = _rows()
    limits = dict(cc.load_institutional_limits())
    limits["turnover"] = dict(limits["turnover"], limit=0.0001)
    evaluation = cc.evaluate_institutional_limits(rows, limits)
    assert evaluation["turnover"]["status"] == "violated"

    config = cc.default_config(_SUMMARY, merge_commit="deadbeef", institutional_limits=limits)
    report = cc.build_invariant_report(
        output_dir=tmp_path,
        generated_files=[],
        config=config,
        candidate_rows=rows,
        network="none",
        db_access=False,
        input_pack_mount="read_only",
        evaluation=evaluation,
        blockers=cc.final_approval_blockers(evaluation),
    )
    assert report["checks"]["constraints_respected"] is False
    assert report["checks"]["institutional_limits_not_violated"] is False
    assert report["ok"] is False


def test_an_unmeasurable_limit_does_not_fake_a_violation(tmp_path: Path) -> None:
    """`unset` and `not_evaluable` block approval, but they are not breaches."""
    rows = _rows()
    evaluation = cc.evaluate_institutional_limits(rows, cc.load_institutional_limits())
    config = cc.default_config(_SUMMARY, merge_commit="deadbeef")
    report = cc.build_invariant_report(
        output_dir=tmp_path,
        generated_files=[],
        config=config,
        candidate_rows=rows,
        network="none",
        db_access=False,
        input_pack_mount="read_only",
        evaluation=evaluation,
        blockers=cc.final_approval_blockers(evaluation),
    )
    assert report["checks"]["constraints_respected"] is True
    assert report["ok"] is True
    assert cc.final_approval_blockers(evaluation), "approval still blocked, for true reasons"


_SUMMARY = {
    "as_of": "2026-06-26",
    "input_pack_id": "x",
    "input_pack_sha256": "y",
    "source_snapshot_sha256": "z",
    "contract_bundle_sha256": "w",
}


def _rows() -> list[dict]:
    grid = cc.default_parameter_grid()
    metrics = {
        "macro": {"delta_mean": 0.01},
        "fund_return": {"mean": 0.001},
        "market_return": {"mean": 0.002},
    }
    return cc.candidate_metrics(grid, metrics)


def test_a_configured_measurable_limit_is_actually_measured() -> None:
    evaluation = cc.evaluate_institutional_limits(_rows(), cc.load_institutional_limits())
    assert evaluation["turnover"]["status"] == "within"
    assert evaluation["turnover"]["limit"] == 0.5


def test_a_violated_limit_is_reported_as_violated() -> None:
    limits = dict(cc.load_institutional_limits())
    limits["turnover"] = dict(limits["turnover"], limit=0.0001)
    evaluation = cc.evaluate_institutional_limits(_rows(), limits)
    assert evaluation["turnover"]["status"] == "violated"
    assert evaluation["turnover"]["violations"]
    assert "institutional_limit_turnover_violated" in cc.final_approval_blockers(evaluation)


def test_a_limit_the_evidence_cannot_measure_says_so() -> None:
    """Null honesty: `not_evaluable` is a fact about coverage, not a verdict."""
    evaluation = cc.evaluate_institutional_limits(_rows(), cc.load_institutional_limits())
    assert evaluation["daily_cvar_95"]["status"] == "not_evaluable"
    assert "does not measure" in evaluation["daily_cvar_95"]["reason"]


def test_final_approval_is_derived_and_opens_by_itself() -> None:
    """The point: no literal stands between a clean candidate and approval."""
    evaluation = cc.evaluate_institutional_limits(_rows(), cc.load_institutional_limits())
    assert cc.final_approval_blockers(evaluation), "today real blockers still stand"

    measured = {name: dict(entry, status="within") for name, entry in evaluation.items()}
    assert cc.final_approval_blockers(measured, baseline_references_certified=True) == []

    selected, _ = cc.selected_and_rejected(
        _rows(),
        evaluation=measured,
        blockers=cc.final_approval_blockers(measured, baseline_references_certified=True),
    )
    assert selected["final_approval_allowed"] is True
    assert "no standing blocker" in selected["selection_reason"]


def test_rejection_reasons_are_computed_not_fixed() -> None:
    rows = _rows()
    evaluation = cc.evaluate_institutional_limits(rows, cc.load_institutional_limits())
    blockers = cc.final_approval_blockers(evaluation)
    _, rejected = cc.selected_and_rejected(rows, evaluation=evaluation, blockers=blockers)
    reasons = {r["reason"] for r in rejected["rejections"]}
    assert reasons
    assert all("explicitly_unset" not in reason for reason in reasons)


def test_the_rejection_rule_tests_violation() -> None:
    config = cc.default_config(
        {
            "as_of": "2026-06-26",
            "input_pack_id": "x",
            "input_pack_sha256": "y",
            "source_snapshot_sha256": "z",
            "contract_bundle_sha256": "w",
        },
        merge_commit="deadbeef",
    )
    rules = config["rejection_rules"]
    assert "institutional_limit_violation" in rules
    assert "institutional_limits_explicitly_unset_blocks_final_approval" not in rules
    assert config["constraints"]["institutional_limits"] == cc.load_institutional_limits()
