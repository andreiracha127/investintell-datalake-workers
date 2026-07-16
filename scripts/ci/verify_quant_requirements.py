"""Verify that every quant input requirement has a compatible exact lock pin."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version


ROOT = Path(__file__).resolve().parents[2]


def _requirements(path: Path) -> dict[str, Requirement]:
    result: dict[str, Requirement] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        try:
            requirement = Requirement(line)
        except InvalidRequirement as exc:
            raise ValueError(f"{path}:{line_number}: {exc}") from exc
        name = canonicalize_name(requirement.name)
        if name in result:
            raise ValueError(f"{path}:{line_number}: duplicate requirement {name}")
        result[name] = requirement
    return result


def verify(input_path: Path, lock_path: Path) -> None:
    inputs = _requirements(input_path)
    locks = _requirements(lock_path)
    errors: list[str] = []

    for name in sorted(inputs.keys() - locks.keys()):
        errors.append(f"missing lock pin for {inputs[name]}")
    locked_versions: dict[str, Version] = {}
    for name, locked in sorted(locks.items()):
        specifiers = list(locked.specifier)
        if (
            len(specifiers) != 1
            or specifiers[0].operator != "=="
            or "*" in specifiers[0].version
        ):
            errors.append(f"lock requirement must use one exact pin: {locked}")
            continue
        try:
            version = Version(specifiers[0].version)
        except InvalidVersion:
            errors.append(f"invalid locked version: {locked}")
            continue
        locked_versions[name] = version

    for name in sorted(inputs.keys() & locked_versions.keys()):
        source = inputs[name]
        locked = locks[name]
        version = locked_versions[name]
        if source.extras != locked.extras:
            errors.append(f"lock extras differ: {locked} vs {source}")
            continue
        if version not in source.specifier:
            errors.append(f"{locked} does not satisfy {source}")

    if errors:
        raise ValueError("\n".join(errors))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=ROOT / "requirements.quant-engine.in",
    )
    parser.add_argument(
        "--lock",
        type=Path,
        default=ROOT / "requirements.quant-engine.lock",
    )
    args = parser.parse_args(argv)
    try:
        verify(args.input, args.lock)
    except (OSError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
