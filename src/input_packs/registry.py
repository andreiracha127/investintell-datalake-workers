"""The single promotable registry of Certified Input Packs.

Before this module the identity of a certified pack — its id, version, path,
aggregate sha256, contract binding, source export and cut — was duplicated
across five places that had to be edited in lock-step to renew a pack:

* ``harness/p1_pack/build.py``                    (builder)
* ``harness/p1_pack/verifier.py``                 (verifier registry)
* ``harness/direct_activation/live_validation.py`` (live validation + the pin the
  runtime worker imports)
* ``src/input_packs/verifier.py``                 (P0 verifier)
* ``tests/test_p1_pack.py``                       (test pins)

Renewing a pack therefore meant a coordinated multi-file code change, and the
verifier accepted only a statically enumerated set of ids. That is what made a
recertification unpromotable in practice.

Why a file registry and not a database table
--------------------------------------------
Every one of those consumers reads the pack **from the repo tree, offline**: the
verifier is documented as never connecting to external systems, the builder is a
pure file transformation, the live validation runs read-only over committed
inputs, and the worker verifies the committed pack bytes *before any DB access*.
A DB table could not be consulted at the point where these gates run, so the
registry is a committed JSON document with a code loader. The lifecycle is the
same one the derived-publication protocol uses for bonds
(``prepared -> current -> retired``), and promotion produces a new auditable
registry revision rather than mutating an anonymous ``current`` pointer.

What stays
----------
The sha256 pins are anti-forgery scaffolding, not ceremony: they stay mandatory
and they stay verified. The registry *declares* an identity; the verifiers still
recompute the aggregate hash over the pack tree and refuse a mismatch, and
``verify_registry`` recomputes every declared hash against the bytes on disk.

Renewing a pack is now:

1. build the new pack (``python -m harness.p1_pack.build --out ...``);
2. publish it: add an entry with ``state: "prepared"``
   (``python -m src.input_packs.registry publish ...``);
3. promote it: ``python -m src.input_packs.registry promote <pack_id>`` — the
   prepared entry becomes ``current``, the previous ``current`` becomes
   ``retired``, the registry revision is bumped and the promotion is appended to
   an auditable log chained by the sha256 of the previous registry body.

No consumer needs editing for any of that.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "contracts" / "input-packs" / "registry.json"

ARTIFACT_TYPE = "certified_input_pack_registry"
SCHEMA_VERSION = 1

#: The lifecycle a pack entry moves through. ``prepared`` is published but not
#: yet serving; ``current`` is the one entry per profile runtime consumes;
#: ``retired`` stays verifiable so historical evidence still replays.
STATES = ("prepared", "current", "retired")

_REQUIRED_FIELDS = (
    "pack_id",
    "pack_version",
    "profile",
    "pack_path",
    "input_pack_sha256",
    "canonical_snapshot_sha256",
    "contract_dir",
    "contract_bundle_sha256",
    "as_of",
    "state",
)

_SHA256_FIELDS = ("input_pack_sha256", "canonical_snapshot_sha256", "contract_bundle_sha256")


class RegistryError(ValueError):
    """The pack registry is malformed, inconsistent, or does not hold an entry."""


@dataclass(frozen=True)
class PackEntry:
    """One certified pack identity, as declared by the registry."""

    pack_id: str
    pack_version: int
    profile: str
    pack_path: str
    input_pack_sha256: str
    canonical_snapshot_sha256: str
    contract_dir: str
    contract_bundle_sha256: str
    as_of: str
    state: str
    #: The governance stance the pack was certified UNDER, as a declaration the
    #: verifier compares the manifest against. It lives here — one editable,
    #: promotable place — instead of as literals inside the verifier, so a pack
    #: certified under a different stance is representable without a code change.
    #: The check stays fail-closed: a manifest that does not match its declared
    #: stance is rejected.
    governance: dict[str, Any] | None = None
    predecessor: str | None = None
    source_repo: str | None = None
    source_commit: str | None = None
    source_export_id: str | None = None
    db_source: str | None = None
    promoted_at: str | None = None
    retired_at: str | None = None
    note: str | None = None

    @property
    def dir(self) -> Path:
        """Absolute path to the committed pack tree."""
        return ROOT / self.pack_path

    @property
    def contract_bundle_dir(self) -> Path:
        return ROOT / self.contract_dir


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _entry_from_payload(payload: dict[str, Any]) -> PackEntry:
    missing = [field for field in _REQUIRED_FIELDS if payload.get(field) is None]
    if missing:
        raise RegistryError(f"registry entry missing required fields: {', '.join(missing)}")
    known = set(PackEntry.__dataclass_fields__)
    unknown = sorted(set(payload) - known)
    if unknown:
        raise RegistryError(
            f"registry entry {payload['pack_id']!r} carries unknown fields: {', '.join(unknown)}"
        )
    return PackEntry(**{k: v for k, v in payload.items() if k in known})


@dataclass(frozen=True)
class Registry:
    """The loaded registry: entries plus the invariants that make it trustworthy."""

    revision: int
    profiles: dict[str, Any]
    entries: tuple[PackEntry, ...]
    promotion_log: tuple[dict[str, Any], ...]
    path: Path

    def entry(self, pack_id: str) -> PackEntry:
        for candidate in self.entries:
            if candidate.pack_id == pack_id:
                return candidate
        raise RegistryError(
            f"{pack_id!r} is not in the certified input pack registry "
            f"(known: {', '.join(e.pack_id for e in self.entries)})"
        )

    def current(self, profile: str) -> PackEntry:
        for candidate in self.entries:
            if candidate.profile == profile and candidate.state == "current":
                return candidate
        raise RegistryError(f"no current certified input pack for profile {profile!r}")

    def prepared(self, profile: str) -> PackEntry | None:
        for candidate in self.entries:
            if candidate.profile == profile and candidate.state == "prepared":
                return candidate
        return None

    def for_profile(self, profile: str) -> tuple[PackEntry, ...]:
        return tuple(e for e in self.entries if e.profile == profile)

    def versions(self, profile: str) -> dict[str, int]:
        """``{pack_id: pack_version}`` for every non-retired-or-retired entry of a
        profile: the set of ids a verifier accepts (historical packs stay
        verifiable so past evidence replays)."""
        return {e.pack_id: e.pack_version for e in self.for_profile(profile)}

    def body(self) -> dict[str, Any]:
        """The canonical payload the promotion log chains over (log excluded)."""
        return {
            "artifact_type": ARTIFACT_TYPE,
            "schema_version": SCHEMA_VERSION,
            "revision": self.revision,
            "profiles": self.profiles,
            "entries": [_entry_to_payload(e) for e in self.entries],
        }

    def body_sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(self.body(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


def _entry_to_payload(entry: PackEntry) -> dict[str, Any]:
    payload = {
        field: getattr(entry, field)
        for field in PackEntry.__dataclass_fields__
        if getattr(entry, field) is not None
    }
    # required-but-nullable declarations stay explicit so an absent value is a
    # declared absence, not a silently missing key
    for field in ("predecessor", "source_export_id", "db_source"):
        payload.setdefault(field, None)
    return payload


def _validate(entries: Iterable[PackEntry], profiles: dict[str, Any]) -> None:
    entries = tuple(entries)
    ids = [e.pack_id for e in entries]
    duplicates = sorted({pid for pid in ids if ids.count(pid) > 1})
    if duplicates:
        raise RegistryError(f"duplicate pack_id in registry: {', '.join(duplicates)}")

    for entry in entries:
        if entry.state not in STATES:
            raise RegistryError(
                f"{entry.pack_id}: state {entry.state!r} is not one of {', '.join(STATES)}"
            )
        if entry.profile not in profiles:
            raise RegistryError(f"{entry.pack_id}: unknown profile {entry.profile!r}")
        for field in _SHA256_FIELDS:
            value = getattr(entry, field)
            if not _is_sha256(value):
                raise RegistryError(f"{entry.pack_id}: {field} is not a 64-hex sha256: {value!r}")
        if Path(entry.pack_path).is_absolute() or ".." in Path(entry.pack_path).parts:
            raise RegistryError(f"{entry.pack_id}: pack_path must be repo-relative: {entry.pack_path!r}")
        try:
            dt.date.fromisoformat(entry.as_of)
        except ValueError as exc:
            raise RegistryError(f"{entry.pack_id}: as_of is not an ISO date: {entry.as_of!r}") from exc
        if entry.predecessor is not None and entry.predecessor not in ids:
            raise RegistryError(
                f"{entry.pack_id}: predecessor {entry.predecessor!r} is not in the registry"
            )
        if entry.predecessor == entry.pack_id:
            raise RegistryError(f"{entry.pack_id}: cannot be its own predecessor")

    for profile in profiles:
        of_profile = [e for e in entries if e.profile == profile]
        if not of_profile:
            continue
        currents = [e.pack_id for e in of_profile if e.state == "current"]
        if len(currents) != 1:
            raise RegistryError(
                f"profile {profile!r} must have exactly one current pack, found {currents!r}"
            )
        prepared = [e.pack_id for e in of_profile if e.state == "prepared"]
        if len(prepared) > 1:
            raise RegistryError(
                f"profile {profile!r} has more than one prepared pack: {prepared!r} "
                "(promote or retire one before publishing another)"
            )


def load_registry(path: str | Path | None = None) -> Registry:
    """Load and validate the registry. Raises ``RegistryError`` on any violation."""
    registry_path = Path(path) if path is not None else REGISTRY_PATH
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RegistryError(f"cannot read pack registry {registry_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RegistryError(f"pack registry {registry_path} is not valid JSON: {exc}") from exc

    if payload.get("artifact_type") != ARTIFACT_TYPE:
        raise RegistryError(f"unexpected artifact_type {payload.get('artifact_type')!r}")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RegistryError(f"unsupported registry schema_version {payload.get('schema_version')!r}")

    profiles = payload.get("profiles") or {}
    if not isinstance(profiles, dict) or not profiles:
        raise RegistryError("registry must declare at least one profile")

    entries = tuple(_entry_from_payload(item) for item in payload.get("entries", []))
    _validate(entries, profiles)

    return Registry(
        revision=int(payload["revision"]),
        profiles=profiles,
        entries=entries,
        promotion_log=tuple(payload.get("promotion_log", [])),
        path=registry_path,
    )


# --------------------------------------------------------------------------- #
# Convenience accessors — what the consumers actually import.
# --------------------------------------------------------------------------- #
P0_PROFILE = "open_macro_v03_p0"
P1_PROFILE = "open_macro_v03_p1"


def current_pack(profile: str) -> PackEntry:
    return load_registry().current(profile)


def current_pack_dir(profile: str) -> Path:
    return current_pack(profile).dir


# --------------------------------------------------------------------------- #
# Verification: the declared hashes are checked against the bytes on disk.
# --------------------------------------------------------------------------- #
def verify_registry(registry: Registry | None = None) -> dict[str, Any]:
    """Recompute every declared pack hash from the committed tree.

    The registry is a declaration; this is the proof. A pack whose manifest or
    recomputed aggregate diverges from the declared ``input_pack_sha256`` fails
    here, and CI runs it on every quant change.
    """
    from src.input_packs.hashing import load_json
    from src.input_packs.manifest import compute_input_pack_sha256

    reg = registry if registry is not None else load_registry()
    errors: list[str] = []
    checked: list[str] = []

    for entry in reg.entries:
        pack_dir = entry.dir
        if not pack_dir.is_dir():
            errors.append(f"{entry.pack_id}: pack_path {entry.pack_path} does not exist")
            continue
        manifest_path = pack_dir / "manifest.json"
        if not manifest_path.is_file():
            errors.append(f"{entry.pack_id}: {entry.pack_path}/manifest.json is missing")
            continue
        manifest = load_json(manifest_path)

        if manifest.get("input_pack_id") != entry.pack_id:
            errors.append(
                f"{entry.pack_id}: manifest input_pack_id {manifest.get('input_pack_id')!r} "
                "does not match the registry entry (a manifest cannot impersonate another pack)"
            )
        for field in ("input_pack_sha256", "canonical_snapshot_sha256", "contract_bundle_sha256"):
            declared = getattr(entry, field)
            actual = manifest.get(field)
            if actual != declared:
                errors.append(
                    f"{entry.pack_id}: manifest {field} {actual!r} != registry {declared!r}"
                )
        if manifest.get("as_of") != entry.as_of:
            errors.append(
                f"{entry.pack_id}: manifest as_of {manifest.get('as_of')!r} != registry {entry.as_of!r}"
            )
        try:
            recomputed = compute_input_pack_sha256(pack_dir, manifest)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{entry.pack_id}: cannot recompute aggregate sha256: {exc}")
        else:
            if recomputed != entry.input_pack_sha256:
                errors.append(
                    f"{entry.pack_id}: recomputed aggregate {recomputed} != registry "
                    f"{entry.input_pack_sha256} (a pack file diverges from the certified tree)"
                )
        checked.append(entry.pack_id)

    return {"ok": not errors, "revision": reg.revision, "checked": checked, "errors": errors}


# --------------------------------------------------------------------------- #
# Publication and promotion — the whole renewal path.
# --------------------------------------------------------------------------- #
def _write(registry_path: Path, payload: dict[str, Any]) -> None:
    text = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    registry_path.write_text(text, encoding="utf-8", newline="")


def _reload_payload(registry_path: Path) -> dict[str, Any]:
    return json.loads(registry_path.read_text(encoding="utf-8"))


def publish(
    *,
    pack_id: str,
    pack_dir: str | Path,
    profile: str,
    registry_path: str | Path | None = None,
    note: str | None = None,
    at: str | None = None,
) -> dict[str, Any]:
    """Add a ``prepared`` entry for a freshly built pack, read from its manifest.

    Everything but the profile and the note is taken from the pack's own
    manifest: the registry never invents an identity, it records the one the
    builder stamped and the verifier can reproduce.
    """
    from src.input_packs.hashing import load_json

    path = Path(registry_path) if registry_path is not None else REGISTRY_PATH
    reg = load_registry(path)
    if profile not in reg.profiles:
        raise RegistryError(f"unknown profile {profile!r}")
    if any(e.pack_id == pack_id for e in reg.entries):
        raise RegistryError(f"{pack_id!r} is already in the registry")

    root = Path(pack_dir)
    manifest = load_json(root / "manifest.json")
    if manifest.get("input_pack_id") != pack_id:
        raise RegistryError(
            f"manifest input_pack_id {manifest.get('input_pack_id')!r} != requested {pack_id!r}"
        )
    try:
        rel = root.resolve().relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise RegistryError(f"pack_dir must live inside the repo: {root}") from exc

    source = load_json(root / "SOURCE.json") if (root / "SOURCE.json").is_file() else {}
    export = source.get("p1_export", {}) if isinstance(source, dict) else {}
    predecessor = reg.current(profile).pack_id if reg.for_profile(profile) else None

    entry = {
        "pack_id": pack_id,
        "pack_version": manifest.get("input_pack_version"),
        "profile": profile,
        "pack_path": rel,
        "input_pack_sha256": manifest.get("input_pack_sha256"),
        "canonical_snapshot_sha256": manifest.get("canonical_snapshot_sha256"),
        "contract_dir": reg.current(profile).contract_dir if reg.for_profile(profile) else None,
        "contract_bundle_sha256": manifest.get("contract_bundle_sha256"),
        "as_of": manifest.get("as_of"),
        "source_repo": manifest.get("source_repo"),
        "source_commit": manifest.get("source_commit"),
        "source_export_id": export.get("export_id"),
        "db_source": export.get("db_source"),
        "state": "prepared",
        "predecessor": predecessor,
        "note": note,
    }

    payload = _reload_payload(path)
    payload["entries"].append({k: v for k, v in entry.items() if v is not None or k in
                               ("predecessor", "source_export_id", "db_source")})
    payload["revision"] = int(payload["revision"]) + 1
    payload["promotion_log"].append(
        {
            "revision": payload["revision"],
            "action": "publish",
            "pack_id": pack_id,
            "at": at or dt.date.today().isoformat(),
            "note": note,
            "previous_registry_sha256": reg.body_sha256(),
        }
    )
    _write(path, payload)
    result = verify_registry(load_registry(path))
    if not result["ok"]:
        raise RegistryError(f"registry is invalid after publish: {result['errors']}")
    return result


def promote(
    pack_id: str,
    *,
    registry_path: str | Path | None = None,
    note: str | None = None,
    at: str | None = None,
) -> dict[str, Any]:
    """Promote a ``prepared`` entry to ``current``; retire the previous current.

    Produces a NEW registry revision and appends an auditable promotion record
    chained by the sha256 of the previous registry body — not a mutable pointer
    without identity.
    """
    path = Path(registry_path) if registry_path is not None else REGISTRY_PATH
    reg = load_registry(path)
    entry = reg.entry(pack_id)
    if entry.state != "prepared":
        raise RegistryError(
            f"{pack_id!r} is {entry.state!r}; only a prepared pack can be promoted"
        )
    previous = reg.current(entry.profile)
    previous_sha = reg.body_sha256()
    when = at or dt.date.today().isoformat()

    payload = _reload_payload(path)
    for item in payload["entries"]:
        if item["pack_id"] == pack_id:
            item["state"] = "current"
            item["promoted_at"] = when
        elif item["pack_id"] == previous.pack_id:
            item["state"] = "retired"
            item["retired_at"] = when
    payload["revision"] = int(payload["revision"]) + 1
    payload["promotion_log"].append(
        {
            "revision": payload["revision"],
            "action": "promote",
            "pack_id": pack_id,
            "profile": entry.profile,
            "retired": previous.pack_id,
            "at": when,
            "note": note,
            "previous_registry_sha256": previous_sha,
        }
    )
    _write(path, payload)
    result = verify_registry(load_registry(path))
    if not result["ok"]:
        raise RegistryError(f"registry is invalid after promotion: {result['errors']}")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("verify", help="recompute every declared pack hash from the tree")

    p_show = sub.add_parser("show", help="print the registry state")
    p_show.add_argument("--profile", default=None)

    p_pub = sub.add_parser("publish", help="add a prepared entry from a built pack")
    p_pub.add_argument("--pack-id", required=True)
    p_pub.add_argument("--pack-dir", required=True)
    p_pub.add_argument("--profile", required=True)
    p_pub.add_argument("--note", default=None)

    p_prom = sub.add_parser("promote", help="promote a prepared entry to current")
    p_prom.add_argument("pack_id")
    p_prom.add_argument("--note", default=None)

    args = parser.parse_args(argv)

    if args.command == "verify":
        result = verify_registry()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1
    if args.command == "show":
        reg = load_registry()
        entries = reg.for_profile(args.profile) if args.profile else reg.entries
        print(
            json.dumps(
                {
                    "revision": reg.revision,
                    "entries": [_entry_to_payload(e) for e in entries],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "publish":
        result = publish(
            pack_id=args.pack_id,
            pack_dir=args.pack_dir,
            profile=args.profile,
            note=args.note,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1
    if args.command == "promote":
        result = promote(args.pack_id, note=args.note)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1
    return 2


if __name__ == "__main__":  # pragma: no cover - CLI
    sys.exit(main())
