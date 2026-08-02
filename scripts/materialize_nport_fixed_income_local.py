"""Narrow operator CLI for the local N-PORT fixed-income materializer."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.nport.fixed_income_local_materializer import (  # noqa: E402
    BuildIdentity,
    ResourceConfig,
    bootstrap_local_materializer,
    compute_local,
    extract_sources,
    install_local_oracle,
    install_product_schema,
    publish_artifact,
)


def _identity(args: argparse.Namespace) -> BuildIdentity:
    return BuildIdentity(
        args.source_publication_id,
        args.source_run_id,
        args.source_package_id,
        args.target_publication_id,
        args.as_of,
        args.contract_digest,
    )


def _config(args: argparse.Namespace) -> ResourceConfig:
    return ResourceConfig(
        memory_limit=args.memory_limit,
        temp_directory=args.temp_directory,
        postgres_image_digest=args.postgres_image_digest,
        postgres_server_fingerprint=args.postgres_server_fingerprint,
        postgres_major=args.postgres_major,
        cpu_limit=args.cpu_limit,
        max_temp_directory_size=args.max_temp_directory_size,
        statement_timeout_ms=args.statement_timeout_ms,
        local_build_statement_timeout_ms=args.local_build_statement_timeout_ms,
        lock_timeout_ms=args.lock_timeout_ms,
        idle_transaction_timeout_ms=args.idle_transaction_timeout_ms,
        client_watchdog_seconds=args.client_watchdog_seconds,
        work_mem=args.work_mem,
        max_parallel_workers_per_gather=args.max_parallel_workers_per_gather,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-publication-id", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--source-package-id", required=True)
    parser.add_argument("--target-publication-id", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--contract-digest", required=True)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--postgres-image-digest", default="")
    parser.add_argument("--postgres-server-fingerprint", default="")
    parser.add_argument("--postgres-major", type=int, default=18)
    parser.add_argument("--cpu-limit", type=float, default=2.0)
    parser.add_argument("--temp-directory", required=True)
    parser.add_argument("--max-temp-directory-size", default="100GB")
    parser.add_argument("--statement-timeout-ms", type=int, default=1_800_000)
    parser.add_argument("--local-build-statement-timeout-ms", type=int, default=21_600_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=10000)
    parser.add_argument("--idle-transaction-timeout-ms", type=int, default=120000)
    parser.add_argument("--client-watchdog-seconds", type=int, default=28_800)
    # The oracle is sort- and hash-heavy; on a local scratch database these are
    # the two knobs that decide whether it spills to disk and how wide it runs.
    parser.add_argument("--work-mem", default="256MB")
    parser.add_argument("--max-parallel-workers-per-gather", type=int, default=2)
    sub = parser.add_subparsers(dest="command", required=True)
    extract = sub.add_parser("extract")
    extract.add_argument("--dsn", required=True)
    extract.add_argument("--output-dir", required=True)
    bootstrap = sub.add_parser("bootstrap", help="attest a PostgreSQL 18 local oracle database")
    bootstrap.add_argument("--local-dsn", required=True)
    bootstrap.add_argument("--local-run-uuid", required=True)
    compute = sub.add_parser(
        "compute", help="run only against an attested local PostgreSQL instance"
    )
    compute.add_argument("--extraction-dir", required=True)
    compute.add_argument("--output-dir", required=True)
    compute.add_argument(
        "--local-dsn",
        required=True,
        help="local-only PostgreSQL DSN; never provide a production DSN",
    )
    compute.add_argument(
        "--local-run-uuid", required=True, help="must equal the local sentinel run UUID"
    )
    compute.add_argument("--worker-sha")
    publish = sub.add_parser("publish")
    publish.add_argument("--dsn", required=True)
    publish.add_argument("--artifact-dir", required=True)
    args = parser.parse_args()
    identity, config = _identity(args), _config(args)
    if args.command == "bootstrap":
        bootstrap_local_materializer(
            local_dsn=args.local_dsn,
            local_run_uuid=args.local_run_uuid,
            identity=identity,
            config=config,
        )
        install_local_oracle(
            local_dsn=args.local_dsn,
            local_run_uuid=args.local_run_uuid,
            config=config,
        )
    elif args.command == "extract":
        extract_sources(
            dsn=args.dsn,
            identity=identity,
            output_dir=Path(args.output_dir),
            resource_config=config,
        )
    elif args.command == "compute":
        worker_sha = (
            args.worker_sha
            or subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        )
        compute_local(
            identity=identity,
            extraction_dir=Path(args.extraction_dir),
            output_dir=Path(args.output_dir),
            local_dsn=args.local_dsn,
            local_run_uuid=args.local_run_uuid,
            worker_sha=worker_sha,
            resource_config=config,
        )
    else:
        with psycopg.connect(args.dsn) as connection:
            # The DDL is a PREREQUISITE of publishing, not an ambient assumption:
            # the publish path calls nport_fixed_income_assert_publication_complete,
            # so a restore into a database that never had this schema applied would
            # fail with 42883 rather than publish. Applying it here (idempotent)
            # mirrors what the in-database worker does in install_schema().
            with connection.cursor() as cursor:
                install_product_schema(cursor)
            connection.commit()
            publish_artifact(
                connection=connection,
                artifact_dir=Path(args.artifact_dir),
                identity=identity,
                resource_config=config,
            )


if __name__ == "__main__":
    main()
