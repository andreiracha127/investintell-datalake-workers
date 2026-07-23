"""CLI for local, deterministic N-PORT V2 COPY artifact generation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.nport.local_v2_generator import (  # noqa: E402
    DuplicatePrimaryKeyError,
    GenerationResult,
    SourceHashMismatch,
    generate,
)

__all__ = ["DuplicatePrimaryKeyError", "GenerationResult", "SourceHashMismatch", "generate", "main"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--parser-version", required=True)
    parser.add_argument("--publication-id", required=True)
    parser.add_argument("--expected-hashes-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--generator-version", required=True)
    parser.add_argument("--config-version", required=True)
    args = parser.parse_args(argv)
    result = generate(
        source_dir=args.source_dir, source_run_id=args.source_run_id, package_id=args.package_id,
        package_sha256=args.package_sha256, parser_version=args.parser_version,
        publication_id=args.publication_id,
        expected_hashes=json.loads(args.expected_hashes_json.read_text(encoding="utf-8")),
        output_dir=args.output_dir, generator_version=args.generator_version, config_version=args.config_version,
    )
    print(result.manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
