"""Offline owner-evidence validator. No environment, network, DB or pointer access.

Run directly with ``python -m src.workers.bond_default_owner_evidence``.
This preparatory worker is deliberately NOT registered in the daily dispatcher.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from src.bonds.default_owner_evidence import (
    OwnerEvidenceError,
    binding_warnings,
    build_bundle,
    canonical_json,
    decode_json,
    verify_bundle,
)

MAX_INPUT_BYTES = 64 * 1024 * 1024


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--mode", choices=("validate", "dry-run"), default="validate")
    result.add_argument("--input", required=True, type=Path)
    result.add_argument("--owner-sub", required=True, help="Independently configured WorkOS subject; not a token")
    result.add_argument("--code-revision", required=True, help="Source revision label; uncommitted code is additionally SHA-pinned")
    result.add_argument("--knowledge-cutoff", required=True, help="Explicit aware ISO timestamp")
    result.add_argument("--rating-binding", type=Path, help="Optional internal-rating reference JSON")
    result.add_argument("--output", type=Path, help="Explicit local artifact write only; never installs or publishes")
    return result


def _read(path: Path) -> dict:
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise OwnerEvidenceError("input_size_limit_exceeded")
    data = path.read_bytes()
    if len(data) > MAX_INPUT_BYTES:
        raise OwnerEvidenceError("input_size_limit_exceeded")
    return decode_json(data.decode("utf-8"))


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        bundle = build_bundle(
            _read(args.input), expected_owner_sub=args.owner_sub,
            code_revision=args.code_revision, knowledge_cutoff=args.knowledge_cutoff,
            rating_binding=_read(args.rating_binding) if args.rating_binding is not None else None,
        )
        verify_bundle(bundle, expected_owner_sub=args.owner_sub)
        if args.output is not None:
            # Avoid replacing any existing operator artifact implicitly.
            with args.output.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(canonical_json(bundle) + "\n")
        summary = {
            "mode": args.mode, "state": "offline_verified_not_persisted",
            "publication_id": bundle["publication_id"], "policy_digest": bundle["policy_digest"],
            "bundle_sha256": bundle["bundle_sha256"], "issuer_mapping_digest": bundle["issuer_mapping_digest"],
            "proposal_count": bundle["proposal_count"], "decision_count": bundle["decision_count"],
            "accepted_event_count": bundle["accepted_event_count"],
            "warnings": binding_warnings(bundle["rating_binding"]),
            "economic_authority": False, "pointer_changed": False,
        }
        print(json.dumps(summary, sort_keys=True))
        return 0
    except OwnerEvidenceError as exc:
        print(json.dumps({"state": "refused", "code": exc.code}, sort_keys=True))
        return 4
    except (OSError, UnicodeError):
        print(json.dumps({"state": "refused", "code": "local_artifact_io_failed"}, sort_keys=True))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
