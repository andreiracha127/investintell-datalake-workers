"""The certified input pack registry: one promotable source of pack identity.

Renewing a certified pack used to require a coordinated edit of five files
(builder, P1 verifier, P0 verifier, live validation, tests). This suite proves
the registry replaces that with publish + promote, and — just as important —
that dissolving the coordination did NOT dissolve any of the integrity checks:

* every declared sha256 is recomputed from the committed pack tree;
* a manifest that contradicts its registry entry is rejected;
* the lifecycle invariants hold (one current per profile, no orphan predecessor,
  no second prepared entry);
* promotion produces a new auditable revision chained by the previous registry
  digest, not a mutable pointer without identity.
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from pathlib import Path

import pytest

from src.input_packs import registry as reg

ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# The committed registry is real: the declarations match the bytes.
# --------------------------------------------------------------------------- #
def test_committed_registry_loads_and_validates() -> None:
    registry = reg.load_registry()
    assert registry.revision >= 1
    assert registry.entries


def test_every_declared_hash_is_recomputed_from_the_committed_tree() -> None:
    """The registry declares; the pack bytes prove. This is the anti-forgery half."""
    result = reg.verify_registry()
    assert result["ok"], result["errors"]
    assert set(result["checked"]) == {e.pack_id for e in reg.load_registry().entries}


def test_each_profile_has_exactly_one_current_pack() -> None:
    registry = reg.load_registry()
    for profile in registry.profiles:
        if registry.for_profile(profile):
            assert registry.current(profile).state == "current"


def test_retired_packs_stay_verifiable() -> None:
    """Historical evidence must still replay, so a retired pack is not deleted."""
    registry = reg.load_registry()
    retired = [e for e in registry.entries if e.state == "retired"]
    assert retired, "expected at least one retired pack (the _002 P1 pack)"
    for entry in retired:
        assert entry.dir.is_dir()
        assert entry.pack_id in registry.versions(entry.profile)


def test_current_p1_pack_is_the_one_the_runtime_worker_consumes() -> None:
    from harness.direct_activation import live_validation as lv

    entry = reg.current_pack(reg.P1_PROFILE)
    assert lv.PACK == entry.dir
    assert lv.PACK_SHA256_PIN == entry.input_pack_sha256


def test_consumers_resolve_their_identity_from_the_registry() -> None:
    from harness.p1_pack import build as p1_build
    from harness.p1_pack import verifier as p1_verifier
    from src.input_packs import verifier as p0_verifier

    p1 = reg.current_pack(reg.P1_PROFILE)
    p0 = reg.current_pack(reg.P0_PROFILE)

    assert p1_build.INPUT_PACK_ID == p1.pack_id
    assert p1_build.INPUT_PACK_VERSION == p1.pack_version
    assert p1_build.CONTRACT_BUNDLE_SHA256 == p1.contract_bundle_sha256
    assert p1_verifier.INPUT_PACK_ID == p1.pack_id
    assert p1_verifier.CERTIFIED_PACK_VERSIONS == reg.load_registry().versions(reg.P1_PROFILE)
    assert p0_verifier.P0_INPUT_PACK_ID == p0.pack_id


#: The identity symbols that used to be retyped in five files for every renewal.
_IDENTITY_ASSIGNMENT = re.compile(
    r"^\s*(INPUT_PACK_ID|INPUT_PACK_VERSION|P0_INPUT_PACK_ID|PACK|REAL_PACK|"
    r"PACK_SHA256_PIN|CERTIFIED_PACK_VERSIONS|CERTIFIED_PACK_IDENTITIES|"
    r"CONTRACT_BUNDLE_SHA256|REQUIRED_SOURCE_EXPORT_ID|REQUIRED_DB_SOURCE)\s*[:=]"
)

_CONSUMERS = (
    "harness/p1_pack/build.py",
    "harness/p1_pack/verifier.py",
    "harness/direct_activation/live_validation.py",
    "src/input_packs/verifier.py",
    "tests/test_p1_pack.py",
)


@pytest.mark.parametrize("rel", _CONSUMERS)
def test_no_consumer_assigns_a_pack_identity_by_hand_any_more(rel: str) -> None:
    """The regression guard for the finding: a renewal must not edit these files.

    Prose and historical provenance strings may still mention a pack by name; an
    identity ASSIGNMENT may not.
    """
    for number, line in enumerate((ROOT / rel).read_text(encoding="utf-8").splitlines(), 1):
        if not _IDENTITY_ASSIGNMENT.match(line):
            continue
        assert "certified_input_pack_00" not in line, (
            f"{rel}:{number} assigns a certified pack id by hand: {line.strip()!r}. "
            "Publish it in contracts/input-packs/registry.json instead."
        )
        assert not re.search(r'=\s*"[0-9a-f]{64}"', line), (
            f"{rel}:{number} assigns a certified pack digest by hand: {line.strip()!r}. "
            "Publish it in contracts/input-packs/registry.json instead."
        )


# --------------------------------------------------------------------------- #
# Invariants — the registry refuses to be inconsistent.
# --------------------------------------------------------------------------- #
def _tmp_registry(tmp_path: Path) -> Path:
    target = tmp_path / "registry.json"
    shutil.copyfile(reg.REGISTRY_PATH, target)
    return target


def _mutate(path: Path, fn) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    fn(payload)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_two_current_packs_in_one_profile_is_rejected(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)

    def make_two_current(payload: dict) -> None:
        for entry in payload["entries"]:
            if entry["profile"] == "open_macro_v03_p1":
                entry["state"] = "current"

    _mutate(path, make_two_current)
    with pytest.raises(reg.RegistryError, match="exactly one current"):
        reg.load_registry(path)


def test_two_prepared_packs_in_one_profile_is_rejected(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)

    def make_two_prepared(payload: dict) -> None:
        clone = dict(payload["entries"][-1])
        clone["pack_id"] = clone["pack_id"] + "_a"
        clone["state"] = "prepared"
        other = dict(clone)
        other["pack_id"] = clone["pack_id"] + "_b"
        payload["entries"].extend([clone, other])

    _mutate(path, make_two_prepared)
    with pytest.raises(reg.RegistryError, match="more than one prepared"):
        reg.load_registry(path)


def test_duplicate_pack_id_is_rejected(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)
    _mutate(path, lambda payload: payload["entries"].append(dict(payload["entries"][0])))
    with pytest.raises(reg.RegistryError, match="duplicate pack_id"):
        reg.load_registry(path)


def test_non_sha256_digest_is_rejected(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)

    def break_hash(payload: dict) -> None:
        payload["entries"][0]["input_pack_sha256"] = "not-a-hash"

    _mutate(path, break_hash)
    with pytest.raises(reg.RegistryError, match="not a 64-hex sha256"):
        reg.load_registry(path)


def test_orphan_predecessor_is_rejected(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)

    def orphan(payload: dict) -> None:
        payload["entries"][-1]["predecessor"] = "open_macro_v03_certified_input_pack_999"

    _mutate(path, orphan)
    with pytest.raises(reg.RegistryError, match="predecessor"):
        reg.load_registry(path)


def test_escaping_pack_path_is_rejected(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)

    def escape(payload: dict) -> None:
        payload["entries"][0]["pack_path"] = "../../etc"

    _mutate(path, escape)
    with pytest.raises(reg.RegistryError, match="repo-relative"):
        reg.load_registry(path)


def test_a_pack_whose_bytes_diverge_from_its_declaration_fails_verification(
    tmp_path: Path,
) -> None:
    path = _tmp_registry(tmp_path)

    def wrong_digest(payload: dict) -> None:
        payload["entries"][-1]["input_pack_sha256"] = "0" * 64

    _mutate(path, wrong_digest)
    result = reg.verify_registry(reg.load_registry(path))
    assert not result["ok"]
    assert any(
        "recomputed aggregate" in e or "manifest input_pack_sha256" in e
        for e in result["errors"]
    )


def test_a_manifest_cannot_impersonate_another_pack(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)

    def rename(payload: dict) -> None:
        payload["entries"][-1]["pack_id"] = "open_macro_v03_certified_input_pack_impostor"

    _mutate(path, rename)
    result = reg.verify_registry(reg.load_registry(path))
    assert not result["ok"]
    assert any("cannot impersonate" in e for e in result["errors"])


# --------------------------------------------------------------------------- #
# Promotion — the renewal path, end to end, without touching a source file.
# --------------------------------------------------------------------------- #
def _synthetic_pack(pack_dir: Path, *, pack_id: str, like: reg.PackEntry) -> None:
    """A minimal but REAL pack tree: the registry hashes it like any other."""
    from src.input_packs.manifest import compute_input_pack_sha256

    (pack_dir / "reports").mkdir(parents=True)
    (pack_dir / "reports" / "certification_summary.json").write_text(
        json.dumps({"input_pack_id": pack_id}, indent=2) + "\n", encoding="utf-8"
    )
    manifest = {
        "input_pack_id": pack_id,
        "input_pack_version": like.pack_version + 1,
        "as_of": like.as_of,
        "canonical_snapshot_sha256": like.canonical_snapshot_sha256,
        "contract_bundle_sha256": like.contract_bundle_sha256,
        "runtime_activation": False,
        "input_pack_sha256": "",
    }
    manifest["input_pack_sha256"] = compute_input_pack_sha256(pack_dir, manifest)
    (pack_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def test_publish_then_promote_is_the_whole_renewal_path(tmp_path: Path) -> None:
    """Renewing a pack: publish a prepared entry, promote it. No source edit.

    Both steps go through the real code: ``publish`` reads the identity from the
    pack's own manifest (it never invents one) and ``promote`` re-verifies every
    declared digest against the bytes before accepting the new revision.
    """
    path = _tmp_registry(tmp_path)
    before = reg.load_registry(path)
    previous = before.current(reg.P1_PROFILE)

    new_id = "open_macro_v03_certified_input_pack_test_renewal"
    pack = ROOT / "fixtures" / "p1_packs" / new_id
    try:
        _synthetic_pack(pack, pack_id=new_id, like=previous)

        published = reg.publish(
            pack_id=new_id,
            pack_dir=pack,
            profile=reg.P1_PROFILE,
            registry_path=path,
            note="test renewal",
            at="2026-08-01",
        )
        assert published["ok"], published["errors"]
        assert reg.load_registry(path).entry(new_id).state == "prepared"
        # Publishing alone changes nothing about what runtime consumes.
        assert reg.load_registry(path).current(reg.P1_PROFILE).pack_id == previous.pack_id

        promoted = reg.promote(new_id, registry_path=path, at="2026-08-02", note="test promotion")
        assert promoted["ok"], promoted["errors"]
    finally:
        shutil.rmtree(pack, ignore_errors=True)

    after = reg.load_registry(path)
    assert after.current(reg.P1_PROFILE).pack_id == new_id
    assert after.entry(previous.pack_id).state == "retired"
    assert after.entry(previous.pack_id).retired_at == "2026-08-02"
    assert after.entry(new_id).predecessor == previous.pack_id
    assert after.revision == before.revision + 2  # publish, then promote

    record = after.promotion_log[-1]
    assert record["action"] == "promote"
    assert record["pack_id"] == new_id
    assert record["retired"] == previous.pack_id
    # Every non-seed revision carries the digest of the registry body it
    # replaced: an auditable chain, not an anonymous mutable pointer.
    assert all(
        r.get("previous_registry_sha256") for r in after.promotion_log if r["action"] != "seed"
    )


def test_publish_refuses_a_manifest_that_does_not_match_the_requested_id(
    tmp_path: Path,
) -> None:
    path = _tmp_registry(tmp_path)
    previous = reg.load_registry(path).current(reg.P1_PROFILE)
    pack = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_test_mismatch"
    try:
        _synthetic_pack(pack, pack_id="something_else", like=previous)
        with pytest.raises(reg.RegistryError, match="!= requested"):
            reg.publish(
                pack_id="open_macro_v03_certified_input_pack_test_mismatch",
                pack_dir=pack,
                profile=reg.P1_PROFILE,
                registry_path=path,
            )
    finally:
        shutil.rmtree(pack, ignore_errors=True)


def test_promoting_a_pack_that_is_not_prepared_is_refused(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)
    current = reg.load_registry(path).current(reg.P1_PROFILE)
    with pytest.raises(reg.RegistryError, match="only a prepared pack"):
        reg.promote(current.pack_id, registry_path=path)


def test_promoting_an_unknown_pack_is_refused(tmp_path: Path) -> None:
    path = _tmp_registry(tmp_path)
    with pytest.raises(reg.RegistryError, match="not in the certified input pack registry"):
        reg.promote("open_macro_v03_certified_input_pack_999", registry_path=path)


# --------------------------------------------------------------------------- #
# The governance stance is a declaration, not a literal in the verifier.
# --------------------------------------------------------------------------- #
def test_a_manifest_that_contradicts_its_declared_stance_is_rejected(tmp_path: Path) -> None:
    """Certified and usable are no longer mutually exclusive — but lying still fails.

    Before, the builder forced ``runtime_activation = False`` and the verifier
    only returned ``ok`` when it read ``False``, so a certified pack was by
    definition a pack that could never be used. The stance now comes from the
    registry entry and the verifier checks the manifest AGAINST that
    declaration, which stays fail-closed.
    """
    from harness.p1_pack import verifier as p1_verifier

    entry = reg.current_pack(reg.P1_PROFILE)
    pack = tmp_path / entry.pack_id
    shutil.copytree(entry.dir, pack)

    manifest_path = pack / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["runtime_activation"] = True
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    result = p1_verifier.verify_pack(pack)
    assert not result["ok"]
    assert not result["runtime_activation_ok"]
    assert any("runtime_activation" in e for e in result["expected_content_errors"])


def test_an_activated_stance_is_representable_in_the_registry(tmp_path: Path) -> None:
    """A declared-activated pack is expressible without editing any schema or verifier."""
    path = _tmp_registry(tmp_path)

    def activate(payload: dict) -> None:
        for entry in payload["entries"]:
            if entry["profile"] == "open_macro_v03_p1" and entry["state"] == "current":
                entry["governance"]["runtime_activation"] = True

    _mutate(path, activate)
    registry = reg.load_registry(path)
    assert registry.current(reg.P1_PROFILE).governance["runtime_activation"] is True


def test_the_builder_no_longer_overwrites_a_declared_stance() -> None:
    """``build_manifest`` defaults the stance instead of forcing it."""
    from src.input_packs.manifest import build_manifest

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "reports").mkdir()
        (root / "reports" / "x.json").write_text("{}", encoding="utf-8")

        defaulted = build_manifest(root, {"input_pack_id": "x", "input_pack_sha256": ""})
        assert defaulted["runtime_activation"] is False

        declared = build_manifest(
            root,
            {"input_pack_id": "x", "input_pack_sha256": "", "runtime_activation": True},
        )
        assert declared["runtime_activation"] is True
