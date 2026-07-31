# ruff: noqa: E701, E702
"""Execution boundary and artifact protocol for N-PORT fixed-income publications.

Two producers share this module:

* ``src.workers.nport_fixed_income_serving`` -- the REGISTERED worker.  It
  installs the sha256-pinned builder (:func:`install_builder`) into the target
  database and runs it there, publishing under the shared
  ``prepared -> validated -> current`` derived-publication protocol.
* the extract/compute/publish artifact path below -- the original offline route,
  where the builder runs in a separately-attested local PostgreSQL and only a
  verified artifact bundle is COPYed into production.

Both paths execute the SAME pinned SQL and honour the SAME integrity scaffolding
(contract digest, builder sha256, build-identity pinning, immutable manifest).
The offline route is kept because it is still the only way to rebuild from a
frozen artifact; it is no longer the only way to produce the product.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tempfile
import threading
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT / "contracts" / "nport-fixed-income-features" / "v2" / "contract.json"
)
BUILDER_SQL_PATH = ROOT / "src" / "nport" / "sql" / "nport_fixed_income_features_builder.sql"
# Historical alias: the resource used to live under ``sql/local_only`` and was
# reachable only from the offline route.  Same bytes, same sha256 pin.
LOCAL_ORACLE_PATH = BUILDER_SQL_PATH
# Re-approved 2026-07-31: coverage stops materializing absence per position.
# The debut production build was cancelled after 2h+ of CPU; the dominant cost
# was one coverage row per holding per metric key -- 173,716 rows per snapshot,
# 45.6M rows, 20 GB of table plus 18 GB of primary key -- while the six
# relations carrying real values total ~260 MB. The builder now persists only
# rows that carry a value and writes the fund x metric rollup the dossier reads,
# counting absence there. No reported figure changes.
#
# Re-approved 2026-07-25: the multi-referenced CTEs carry AS MATERIALIZED. Without
# it PostgreSQL inlines snapshot_holdings into each UNION ALL branch, re-running
# the 4.1M-row holdings/bridge join nine times in a single statement. MATERIALIZED
# is a planner directive only — the rows the oracle produces are unchanged.
APPROVED_LOCAL_ORACLE_SHA256 = "5bbf9116faec249b13cd092b9278b22f46fea6a3e86a8a1e1ca7d69112429831"
CONTRACT_DIGEST = (
    "sha256:797332a98c62c3843ea1f870a61dca3c67fe5a4bd012aa7d978913ca120be563"
)
TARGET_RELATIONS = (
    "nport_fixed_income_features",
    "nport_fixed_income_key_rate_sensitivities_v2",
    "nport_fixed_income_credit_spread_sensitivities_v2",
    "nport_fixed_income_balance_sheet_primitives_v2",
    "nport_fixed_income_debt_flag_features_v2",
    "nport_fixed_income_repo_lending_primitives_v2",
    "nport_fixed_income_repo_lending_reported_flags_v2",
    "nport_fixed_income_metric_coverage_v2",
)
# The fund x metric coverage rollup. It is NOT a contract relation -- the frozen
# producer contract still describes exactly the eight above -- but it IS part of
# every publication, because the builder no longer materializes absence per
# position: the counts of what was absent live here and nowhere else. An
# artifact that carried only the eight would publish a rollup rebuilt from
# pruned rows, i.e. coverage_ratio 1 for partially covered metrics and no row at
# all for metrics with nothing reported. So it travels with the artifact.
COVERAGE_ROLLUP_RELATION = "nport_fixed_income_metric_coverage_snapshot_v1"
COVERAGE_ROLLUP_COLUMNS = (
    "publication_id", "source_holdings_publication_id", "source_run_id", "series_id",
    "report_date", "accession_number", "metric_family", "metric_key", "numerator",
    "denominator", "denominator_unit", "coverage_ratio", "availability_state",
    "methodology_version", "exclusions", "source_row_count", "reported_row_count",
    "missing_reason_counts",
)
COVERAGE_ROLLUP_KEYS = (
    "publication_id", "series_id", "report_date", "accession_number",
    "metric_family", "metric_key",
)
# What a publication physically consists of: the contract relations plus the
# rollup. Every artifact/manifest invariant below is stated over THIS tuple.
PUBLISHED_RELATIONS = (*TARGET_RELATIONS, COVERAGE_ROLLUP_RELATION)

# Raw inputs are intentionally base relations; no current/contract view may hide a join.
SOURCE_RELATIONS = (
    "sec_nport_instrument_class_bridge",
    "sec_nport_holdings_v2",
    "nport_raw_rows",
)
RAW_SOURCE_TABLE_ALLOWLIST = (
    "INTEREST_RATE_RISK.tsv", "FUND_REPORTED_INFO.tsv", "BORROW_AGGREGATE.tsv",
    "REPURCHASE_AGREEMENT.tsv", "REPURCHASE_COLLATERAL.tsv", "REPURCHASE_COUNTERPARTY.tsv",
    "SECURITIES_LENDING.tsv",
)


class ArtifactIntegrityError(ValueError):
    pass


class PublicationConflictError(RuntimeError):
    pass


@dataclass(frozen=True)
class BuildIdentity:
    source_publication_id: str
    source_run_id: str
    source_package_id: str
    target_publication_id: str
    as_of_date: str
    contract_digest: str

    def validate(self) -> None:
        for value in (
            self.source_publication_id,
            self.source_run_id,
            self.source_package_id,
            self.target_publication_id,
        ):
            uuid.UUID(value)
        date.fromisoformat(self.as_of_date)
        if self.contract_digest != CONTRACT_DIGEST:
            raise ArtifactIntegrityError("unexpected fixed-income contract digest")


@dataclass(frozen=True)
class ResourceConfig:
    memory_limit: str
    temp_directory: str
    postgres_image_digest: str = ""
    postgres_server_fingerprint: str = ""
    postgres_major: int = 18
    cpu_limit: float = 2.0
    max_temp_directory_size: str = "20GB"
    minimum_free_bytes: int = 2 * 1024**3
    statement_timeout_ms: int = 1_800_000
    local_build_statement_timeout_ms: int = 21_600_000
    lock_timeout_ms: int = 10_000
    idle_transaction_timeout_ms: int = 120_000
    client_watchdog_seconds: int = 28_800
    work_mem: str = "256MB"
    temp_file_limit: str = "20GB"
    max_parallel_workers_per_gather: int = 2

    def validate(self, *, local: bool = False) -> None:
        if self.cpu_limit <= 0 or self.client_watchdog_seconds <= 0:
            raise ValueError("cpu_limit and client_watchdog_seconds must be positive")
        for value in (
            self.statement_timeout_ms,
            self.local_build_statement_timeout_ms,
            self.lock_timeout_ms,
            self.idle_transaction_timeout_ms,
        ):
            if value <= 0:
                raise ValueError("all PostgreSQL timeouts must be nonzero")
        if self.max_parallel_workers_per_gather < 0:
            raise ValueError("max_parallel_workers_per_gather must be nonnegative")
        if local and (
            self.postgres_major != 18
            or not self.postgres_image_digest.startswith("sha256:")
            or len(self.postgres_image_digest) != 71
            or not self.postgres_server_fingerprint
        ):
            raise ArtifactIntegrityError("local PostgreSQL 18 image digest and server fingerprint are required")
        if (
            local
            and self.client_watchdog_seconds * 1000
            < self.local_build_statement_timeout_ms
        ):
            raise ArtifactIntegrityError(
                "local client watchdog must not expire before the local build timeout"
            )


@dataclass(frozen=True)
class MaterializationResult:
    output_files: Mapping[str, Path]
    manifest_path: Path


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def postgres_server_fingerprint(cursor: Any) -> str:
    """Hash the PostgreSQL settings that can affect oracle semantics."""
    cursor.execute(
        """SELECT current_setting('server_version_num'),
                  current_setting('server_encoding'),
                  d.datlocprovider,
                  d.datcollate,
                  d.datctype,
                  current_setting('TimeZone'),
                  current_setting('DateStyle'),
                  current_setting('IntervalStyle'),
                  current_setting('extra_float_digits'),
                  current_setting('standard_conforming_strings')
           FROM pg_database d
           WHERE d.datname = current_database()"""
    )
    row = cursor.fetchone()
    if row is None:
        raise ArtifactIntegrityError("unable to fingerprint local PostgreSQL")
    payload = {
        "server_version_num": row[0],
        "server_encoding": row[1],
        "locale_provider": row[2],
        "collation": row[3],
        "ctype": row[4],
        "timezone": row[5],
        "date_style": row[6],
        "interval_style": row[7],
        "extra_float_digits": row[8],
        "standard_conforming_strings": row[9],
    }
    return "sha256:" + hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def _assert_local_postgres_runtime(cursor: Any, config: ResourceConfig) -> None:
    cursor.execute("SELECT current_setting('server_version_num')::integer")
    if cursor.fetchone()[0] // 10_000 != config.postgres_major:
        raise ArtifactIntegrityError("local PostgreSQL major version mismatch")
    if postgres_server_fingerprint(cursor) != config.postgres_server_fingerprint:
        raise ArtifactIntegrityError("local PostgreSQL settings fingerprint mismatch")


def _contract() -> dict[str, Any]:
    document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    if document.get("digest") != CONTRACT_DIGEST:
        raise ArtifactIntegrityError(
            "contract file digest differs from pinned contract"
        )
    return document


def _contract_relation(name: str) -> dict[str, Any]:
    for relation in _contract()["relations"]:
        if relation["name"] == name:
            return relation
    raise ArtifactIntegrityError(f"contract lacks relation {name}")


def _contract_columns() -> dict[str, tuple[str, ...]]:
    return {
        name: tuple(column["name"] for column in _contract_relation(name)["columns"])
        for name in TARGET_RELATIONS
    }


def _published_columns() -> dict[str, tuple[str, ...]]:
    """Contract columns, plus the rollup's own (it has no contract entry)."""
    return {**_contract_columns(), COVERAGE_ROLLUP_RELATION: COVERAGE_ROLLUP_COLUMNS}


def _contract_keys() -> dict[str, tuple[str, ...]]:
    return {name: tuple(_contract_relation(name)["keys"]) for name in TARGET_RELATIONS}


def _published_keys() -> dict[str, tuple[str, ...]]:
    return {**_contract_keys(), COVERAGE_ROLLUP_RELATION: COVERAGE_ROLLUP_KEYS}


def _set_client_safe_timeouts(
    cursor: Any,
    config: ResourceConfig,
    *,
    statement_timeout_ms: int | None = None,
) -> None:
    config.validate()
    for setting, value in (
        ("lock_timeout", config.lock_timeout_ms),
        ("statement_timeout", statement_timeout_ms or config.statement_timeout_ms),
        ("idle_in_transaction_session_timeout", config.idle_transaction_timeout_ms),
    ):
        if value <= 0:
            raise ValueError(f"{setting} must be nonzero")
        cursor.execute("SELECT set_config(%s::text,%s::text,true)", (setting, f"{value}ms"))


def _set_local_timeouts(
    cursor: Any,
    config: ResourceConfig,
    *,
    statement_timeout_ms: int | None = None,
) -> None:
    _set_client_safe_timeouts(
        cursor,
        config,
        statement_timeout_ms=statement_timeout_ms,
    )
    for setting, value in (
        ("work_mem", config.work_mem),
        ("temp_file_limit", config.temp_file_limit),
        ("max_parallel_workers_per_gather", str(config.max_parallel_workers_per_gather)),
        ("max_parallel_workers", str(config.max_parallel_workers_per_gather)),
    ):
        cursor.execute("SELECT set_config(%s::text,%s::text,true)", (setting, value))


def _disk_guard(config: ResourceConfig) -> None:
    config.validate()
    target = Path(config.temp_directory)
    target.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(target).free < config.minimum_free_bytes:
        raise OSError("insufficient free disk for local PostgreSQL materializer")


def _run_with_watchdog(connection: Any, seconds: int, operation: Any) -> Any:
    """Client-side cancellation is deliberate: server timeout is not our only guard."""
    timer = threading.Timer(seconds, connection.cancel)
    timer.daemon = True
    timer.start()
    try:
        return operation()
    finally:
        timer.cancel()


def _assert_oracle_resource() -> None:
    if not BUILDER_SQL_PATH.is_file():
        raise ArtifactIntegrityError("packaged fixed-income builder SQL is missing")
    if sha256_file(BUILDER_SQL_PATH) != APPROVED_LOCAL_ORACLE_SHA256:
        raise ArtifactIntegrityError("fixed-income builder SQL is not the approved version")


def install_builder(cursor: Any) -> str:
    """Install the pinned ``build_nport_fixed_income_features`` into the target DB.

    Fail-closed on the pin FIRST: the sha256 of the packaged SQL is the only
    thing standing between "the approved builder" and "whatever is on disk", so
    a drifted resource must never reach ``EXECUTE``.  Idempotent (the resource is
    a single ``CREATE OR REPLACE FUNCTION``) and callable from any database --
    the local-vs-production distinction was never enforced by this function, it
    was enforced by refusing to define the function in production at all.

    Returns the verified sha256 so callers can record it in a manifest.
    """
    _assert_oracle_resource()
    cursor.execute(BUILDER_SQL_PATH.read_text(encoding="utf-8"))
    return APPROVED_LOCAL_ORACLE_SHA256


def _copy_field(value: Any) -> str:
    if value is None:
        return r"\N"
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _write_copy_gzip(
    path: Path, columns: Sequence[str], keys: Sequence[str], rows: Any
) -> tuple[int, str | None, str | None]:
    count, first_key, last_key = 0, None, None
    with (
        path.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as output,
    ):
        output.write(("\t".join(columns) + "\n").encode())
        for row in rows:
            output.write(("\t".join(_copy_field(item) for item in row) + "\n").encode())
            rendered_key = canonical_json(tuple(row[columns.index(key)] for key in keys))
            first_key = first_key or rendered_key
            last_key = rendered_key
            count += 1
    return count, first_key, last_key


def _assert_local_materializer(cursor: Any, expected_run_uuid: str) -> None:
    """Reject a production endpoint even if operators accidentally pass its DSN."""
    cursor.execute("SELECT current_database(), inet_server_addr()::text")
    database, host = cursor.fetchone()
    if not database.startswith(("nport_local_", "fi_local_")) or host in {
        "10.158.0.2",
        "localhost.production",
    }:
        raise ArtifactIntegrityError("local PostgreSQL database identity rejected")
    cursor.execute(
        "SELECT local_materializer, run_uuid::text FROM nport_local_materializer_sentinel LIMIT 1"
    )
    sentinel = cursor.fetchone()
    if sentinel != (True, expected_run_uuid):
        raise ArtifactIntegrityError("local PostgreSQL sentinel/run identity rejected")


LOCAL_RAW_DDL = """
CREATE TABLE IF NOT EXISTS nport_raw_rows (
  -- Preserve the immutable production lineage key. An identity column would
  -- reject explicit raw_row_id values during COPY.
  raw_row_id bigint PRIMARY KEY,
  ingestion_run_id uuid NOT NULL, source_file_id uuid NOT NULL,
  source_row_number bigint NOT NULL, source_sha256 char(64), loaded_at timestamptz,
  parser_version text, source_table text NOT NULL, original_lexical_row jsonb,
  typed_projection jsonb NOT NULL, parse_status text, parse_errors jsonb,
  candidate_key_evidence jsonb, accession_number text, holding_id text,
  UNIQUE (source_file_id, source_row_number)
);
"""


def _install_local_raw_views(cursor: Any) -> None:
    for view, source_table in (
        ("nport_interest_rate_risk_raw", "INTEREST_RATE_RISK.tsv"),
        ("nport_fund_reported_info_raw", "FUND_REPORTED_INFO.tsv"),
        ("nport_borrow_aggregate_raw", "BORROW_AGGREGATE.tsv"),
        ("nport_repurchase_agreement_raw", "REPURCHASE_AGREEMENT.tsv"),
        ("nport_repurchase_collateral_raw", "REPURCHASE_COLLATERAL.tsv"),
        ("nport_repurchase_counterparty_raw", "REPURCHASE_COUNTERPARTY.tsv"),
        ("nport_securities_lending_raw", "SECURITIES_LENDING.tsv"),
    ):
        cursor.execute(
            sql.SQL("CREATE OR REPLACE VIEW {} AS SELECT * FROM nport_raw_rows "
                    "WHERE source_table = {}").format(
                sql.Identifier(view), sql.Literal(source_table)
            )
        )


def bootstrap_local_materializer(
    *,
    local_dsn: str,
    local_run_uuid: str,
    identity: BuildIdentity,
    config: ResourceConfig,
    ddl_files: Sequence[Path] | None = None,
) -> None:
    """Provision the sentinel and a caller-selected minimal schema in a local DB only.

    The exact source run/package/publication is recreated as validated/current;
    the target is prepared.  The physical raw input table is intentionally local
    and has no production foreign keys, while the repository publication,
    holdings, and feature DDL is installed unchanged.
    """
    identity.validate()
    config.validate(local=True)
    _disk_guard(config)
    _assert_oracle_resource()
    ddl_files = ddl_files or (
        ROOT / "schemas" / "sec_source_manifests.sql",
        ROOT / "schemas" / "sec_derived_publications.sql",
        ROOT / "schemas" / "nport_holdings_v2.sql",
        ROOT / "schemas" / "nport_fixed_income_features.sql",
    )
    with psycopg.connect(local_dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            _set_local_timeouts(cursor, config)
            _assert_local_postgres_runtime(cursor, config)
            cursor.execute("SELECT current_database(), inet_server_addr()::text")
            database, host = cursor.fetchone()
            if not database.startswith(("nport_local_", "fi_local_")) or host == "10.158.0.2":
                raise ArtifactIntegrityError("refusing to bootstrap non-local PostgreSQL")
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended('nport-fi-local-bootstrap',0))")
            cursor.execute("""CREATE TABLE IF NOT EXISTS nport_local_materializer_sentinel(
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                local_materializer boolean NOT NULL CHECK(local_materializer),
                run_uuid uuid NOT NULL UNIQUE)""")
            cursor.execute("DELETE FROM nport_local_materializer_sentinel")
            cursor.execute(
                "INSERT INTO nport_local_materializer_sentinel(singleton,local_materializer,run_uuid) VALUES(true,true,%s)",
                (local_run_uuid,),
            )
            for ddl_file in ddl_files:
                cursor.execute(Path(ddl_file).read_text(encoding="utf-8"))
            cursor.execute(LOCAL_RAW_DDL)
            _install_local_raw_views(cursor)
            cursor.execute(
                """INSERT INTO sec_ingestion_runs
                (run_id,source_family,package_sha256,parser_version,source_quarter,
                 package_relative_path)
                VALUES(%s,'nport-local','%s','local-materializer','2026Q3',
                       'local/nport')
                ON CONFLICT (run_id) DO NOTHING""" % ("%s", "0" * 64),
                (identity.source_run_id,),
            )
            cursor.execute(
                "UPDATE sec_ingestion_runs SET current_state='loading' WHERE run_id=%s AND current_state='discovered'",
                (identity.source_run_id,),
            )
            # The disposable local snapshot has no source-file accounting tables to
            # reconcile.  Use the same guarded state transition with its required
            # local validation token, rather than bypassing lifecycle triggers.
            cursor.execute(
                "INSERT INTO sec_raw_validation_tokens(run_id,backend_pid) VALUES(%s,pg_backend_pid())",
                (identity.source_run_id,),
            )
            cursor.execute(
                "UPDATE sec_ingestion_runs SET current_state='raw_validated',raw_validated_at=now() WHERE run_id=%s AND current_state='loading'",
                (identity.source_run_id,),
            )
            cursor.execute(
                "DELETE FROM sec_raw_validation_tokens WHERE run_id=%s AND backend_pid=pg_backend_pid()",
                (identity.source_run_id,),
            )
            cursor.execute(
                """INSERT INTO sec_source_packages
                (package_id,source_family,source_quarter,package_relative_path,package_sha256,package_state,run_id)
                VALUES(%s,'nport-local','2026Q3','local/nport','%s','loaded',%s)
                ON CONFLICT (package_id) DO NOTHING""" % ("%s", "0" * 64, "%s"),
                (identity.source_package_id, identity.source_run_id),
            )
            cursor.execute(
                """INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint,lifecycle_state,validated_at)
                VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s,'prepared',NULL)
                ON CONFLICT (publication_id) DO NOTHING""",
                (identity.source_publication_id, identity.source_run_id, identity.source_package_id, "0" * 64),
            )
            cursor.execute(
                "SELECT lifecycle_state,source_run_id,source_package_id FROM sec_derived_publications WHERE publication_id=%s",
                (identity.source_publication_id,),
            )
            if cursor.fetchone() != (
                "prepared", uuid.UUID(identity.source_run_id), uuid.UUID(identity.source_package_id)
            ):
                raise ArtifactIntegrityError("bootstrap source publication differs from BuildIdentity")
            cursor.execute(
                """INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
                VALUES(%s,'nport_fixed_income_features_v1',1,%s,%s,%s)
                ON CONFLICT (publication_id) DO NOTHING""",
                (identity.target_publication_id, identity.source_run_id, identity.source_package_id, "0" * 64),
            )
            _assert_local_materializer(cursor, local_run_uuid)
        connection.commit()


def _provenance(cursor: Any, identity: BuildIdentity) -> tuple[Any, Any, Any]:
    cursor.execute(
        "SELECT raw_validated_at FROM sec_ingestion_runs WHERE run_id=%s",
        (identity.source_run_id,),
    )
    run = cursor.fetchone()
    cursor.execute(
        "SELECT run_id FROM sec_source_packages WHERE package_id=%s",
        (identity.source_package_id,),
    )
    package = cursor.fetchone()
    cursor.execute(
        """SELECT p.source_run_id,p.source_package_id,p.lifecycle_state,c.publication_id
        FROM sec_derived_publications p
        LEFT JOIN sec_derived_current_pointers c
          ON c.product='sec_nport_holdings_v2' AND c.publication_id=p.publication_id
        WHERE p.publication_id=%s AND p.product='sec_nport_holdings_v2'""",
        (identity.source_publication_id,),
    )
    publication = cursor.fetchone()
    expected_run = uuid.UUID(identity.source_run_id)
    if (
        not run
        or run[0] is None
        or package != (expected_run,)
        or publication != (
            expected_run,
            uuid.UUID(identity.source_package_id),
            "validated",
            uuid.UUID(identity.source_publication_id),
        )
    ):
        raise ArtifactIntegrityError(
            "source is not a validated, package-pinned holdings publication"
        )
    return run, package, publication


def _source_columns(cursor: Any, relation: str) -> tuple[str, ...]:
    cursor.execute(
        """SELECT column_name FROM information_schema.columns
        WHERE table_schema=current_schema() AND table_name=%s ORDER BY ordinal_position""",
        (relation,),
    )
    columns = tuple(row[0] for row in cursor.fetchall())
    if not columns:
        raise ArtifactIntegrityError(f"source relation missing: {relation}")
    return columns


def _validate_local_source(cursor: Any, identity: BuildIdentity) -> None:
    """The imported source stays mutable until all three physical COPY loads finish."""
    cursor.execute(
        "SELECT source_run_id,source_package_id,lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
        (identity.source_publication_id,),
    )
    if cursor.fetchone() != (
        uuid.UUID(identity.source_run_id),
        uuid.UUID(identity.source_package_id),
        "prepared",
    ):
        raise ArtifactIntegrityError("local source must be the prepared BuildIdentity publication")
    cursor.execute("SELECT sec_validate_derived_publication(%s)", (identity.source_publication_id,))
    cursor.execute(
        "SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)",
        (identity.source_publication_id,),
    )


def extract_sources(
    *,
    dsn: str,
    identity: BuildIdentity,
    output_dir: Path,
    resource_config: ResourceConfig,
) -> Mapping[str, Path]:
    """Bounded, read-only snapshot extraction; each COPY touches exactly one base relation."""
    identity.validate()
    _disk_guard(resource_config)
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite extraction: {output_dir}")
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}-", dir=output_dir.parent)
    )
    filters = {
        "sec_nport_holdings_v2": ("publication_id", identity.source_publication_id),
        "sec_nport_instrument_class_bridge": (
            "publication_id",
            identity.source_publication_id,
        ),
        "nport_raw_rows": ("ingestion_run_id", identity.source_run_id),
    }
    try:
        with psycopg.connect(
            dsn,
            autocommit=False,
            application_name=f"nport-fi-extract-{uuid.uuid4().hex}",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                _set_client_safe_timeouts(cursor, resource_config)
                before = _provenance(cursor, identity)
                paths: dict[str, Path] = {}
                for relation, (filter_column, filter_value) in filters.items():
                    columns = _source_columns(cursor, relation)
                    if filter_column not in columns:
                        raise ArtifactIntegrityError(
                            f"{relation} lacks required provenance column"
                        )
                    quoted = ",".join(f'"{column}"' for column in columns)
                    path = staging / f"{relation}.tsv.gz"
                    # Explicit catalog-derived columns; no join, aggregate, window, ordering, or view.
                    sql = f'COPY (SELECT {quoted} FROM {relation} WHERE "{filter_column}" = %s'
                    params: tuple[Any, ...] = (filter_value,)
                    if relation == "nport_raw_rows":
                        if "source_table" not in columns:
                            raise ArtifactIntegrityError("nport_raw_rows lacks source_table")
                        sql += ' AND "source_table" = ANY(%s)'
                        params += (list(RAW_SOURCE_TABLE_ALLOWLIST),)
                    sql += ") TO STDOUT WITH (FORMAT text, HEADER true)"
                    with (
                        cursor.copy(
                            sql,
                            params,
                        ) as copy,
                        path.open("wb") as raw_stream,
                        gzip.GzipFile(filename="", mode="wb", fileobj=raw_stream, mtime=0) as stream,
                    ):
                        for chunk in copy:
                            stream.write(chunk)
                    paths[relation] = path
                if _provenance(cursor, identity) != before:
                    raise ArtifactIntegrityError(
                        "source provenance changed during extraction"
                    )
        staging.rename(output_dir)
        return {name: output_dir / path.name for name, path in paths.items()}
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def install_local_oracle(
    *, local_dsn: str, local_run_uuid: str, config: ResourceConfig
) -> None:
    """Install the heavy builder only after local sentinel attestation."""
    with psycopg.connect(local_dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            _set_local_timeouts(cursor, config)
            _assert_local_postgres_runtime(cursor, config)
            _assert_local_materializer(cursor, local_run_uuid)
            cursor.execute(LOCAL_ORACLE_PATH.read_text(encoding="utf-8"))
        connection.commit()


def _copy_source_into_local(cursor: Any, relation: str, path: Path) -> None:
    columns = _source_columns(cursor, relation)
    quoted = ",".join(f'"{column}"' for column in columns)
    with (
        gzip.open(path, "rb") as source,
        cursor.copy(
            f"COPY {relation} ({quoted}) FROM STDIN WITH (FORMAT text, HEADER true)"
        ) as copy,
    ):
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            copy.write(chunk)


# The seven raw views each filter nport_raw_rows by source_table, and the oracle
# reads them across thirteen UNION ALL branches. Without this index every branch
# sequentially scans ~6M rows; without ANALYZE the planner has no statistics at
# all, because COPY does not produce any. Both are load-time facts about the
# local scratch database, not changes to what the oracle computes.
_LOCAL_LOAD_INDEXES = (
    (
        "nport_raw_rows",
        "CREATE INDEX IF NOT EXISTS nport_raw_rows_local_source_table_idx "
        "ON nport_raw_rows (source_table, accession_number, holding_id)",
    ),
)


def _prepare_local_statistics(cursor: Any) -> None:
    """Index and analyse the freshly copied snapshots before the oracle runs."""
    for _relation, statement in _LOCAL_LOAD_INDEXES:
        cursor.execute(statement)
    for relation in SOURCE_RELATIONS:
        cursor.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(relation)))


def _copy_query_to_gzip(
    cursor: Any, sql: str, params: Sequence[Any], path: Path
) -> None:
    """Keep PostgreSQL COPY and gzip streaming; never materialize result rows in Python."""
    with (
        path.open("wb") as raw_stream,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw_stream, mtime=0) as stream,
        cursor.copy(sql, params) as copy,
    ):
        for chunk in copy:
            stream.write(chunk)


def compute_local(
    *,
    identity: BuildIdentity,
    extraction_dir: Path,
    output_dir: Path,
    local_dsn: str,
    local_run_uuid: str,
    worker_sha: str = "0" * 40,
    resource_config: ResourceConfig | None = None,
) -> MaterializationResult:
    """Load immutable inputs into local PostgreSQL, run the guarded oracle, and export exact payloads."""
    identity.validate()
    config = resource_config or ResourceConfig(
        memory_limit="8GB",
        temp_directory=str(Path(output_dir).parent / ".fi-local-tmp"),
    )
    if len(worker_sha) not in (40, 64) or any(
        char not in "0123456789abcdef" for char in worker_sha.lower()
    ):
        raise ValueError("worker_sha must be hexadecimal")
    config.validate(local=True)
    _disk_guard(config)
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing artifact: {output_dir}")
    sources = {name: Path(extraction_dir) / f"{name}.tsv.gz" for name in SOURCE_RELATIONS}
    if missing := [name for name, path in sources.items() if not path.is_file()]:
        raise FileNotFoundError(f"missing extracted sources: {missing}")
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}-", dir=output_dir.parent)
    )
    try:
        with psycopg.connect(
            local_dsn,
            autocommit=False,
            application_name=f"nport-fi-local-{local_run_uuid}",
        ) as connection:
            with connection.cursor() as cursor:
                _set_local_timeouts(cursor, config, statement_timeout_ms=config.local_build_statement_timeout_ms)
                _assert_local_postgres_runtime(cursor, config)
                _assert_local_materializer(cursor, local_run_uuid)
                for relation, path in sources.items():
                    _copy_source_into_local(cursor, relation, path)
                _prepare_local_statistics(cursor)
                _validate_local_source(cursor, identity)
                _provenance(cursor, identity)
                _run_with_watchdog(
                    connection,
                    config.client_watchdog_seconds,
                    lambda: cursor.execute(
                        "SELECT build_nport_fixed_income_features(%s,%s)",
                        (identity.target_publication_id, identity.as_of_date),
                    ),
                )
                columns, keys, output_meta, files = (
                    _published_columns(),
                    _published_keys(),
                    {},
                    {},
                )
                for relation in PUBLISHED_RELATIONS:
                    selected, order = (
                        ",".join(f'"{name}"' for name in columns[relation]),
                        ",".join(f'"{name}"' for name in keys[relation]),
                    )
                    path = staging / f"{relation}.tsv.gz"
                    cursor.execute(
                        f"SELECT count(*) FROM {relation} WHERE publication_id=%s",
                        (identity.target_publication_id,),
                    )
                    count = cursor.fetchone()[0]
                    _run_with_watchdog(
                        connection,
                        config.client_watchdog_seconds,
                        lambda: _copy_query_to_gzip(
                            cursor,
                            f"COPY (SELECT {selected} FROM {relation} WHERE publication_id=%s ORDER BY {order}) TO STDOUT WITH (FORMAT text, HEADER true)",
                            (identity.target_publication_id,),
                            path,
                        ),
                    )
                    files[relation] = path
                    output_meta[relation] = {
                        "count": count,
                        "sha256": sha256_file(path),
                        "filename": path.name,
                        "columns": columns[relation],
                    }
            connection.rollback()  # Local load/oracle output is disposable; only artifacts leave this database.
        manifest = build_manifest(
            identity=identity,
            worker_sha=worker_sha,
            source_files=sources,
            output_files=files,
            resource_config=config,
            output_meta=output_meta,
        )
        (staging / "manifest.json").write_text(
            canonical_json(manifest) + "\n", encoding="utf-8"
        )
        staging.rename(output_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return MaterializationResult(
        {name: output_dir / path.name for name, path in files.items()},
        output_dir / "manifest.json",
    )


def build_manifest(
    *,
    identity: BuildIdentity,
    worker_sha: str,
    source_files: Mapping[str, Path],
    output_files: Mapping[str, Path],
    resource_config: ResourceConfig,
    output_counts: Mapping[str, int] | None = None,
    output_meta: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    identity.validate()
    resource_config.validate(local=True)
    if set(source_files) != set(SOURCE_RELATIONS):
        raise ArtifactIntegrityError("manifest requires exactly the three physical source relations")
    if set(output_files) != set(PUBLISHED_RELATIONS):
        raise ArtifactIntegrityError("manifest requires exactly the published relations")
    outputs = output_meta or {
        name: {
            "count": (output_counts or {}).get(name, 0),
            "sha256": sha256_file(path),
            "filename": path.name,
            "columns": _published_columns().get(name, ()),
        }
        for name, path in output_files.items()
    }
    if set(outputs) != set(PUBLISHED_RELATIONS):
        raise ArtifactIntegrityError("manifest output metadata must cover exactly the published relations")
    contract_columns = _published_columns()
    for name in PUBLISHED_RELATIONS:
        if tuple(outputs[name].get("columns", ())) != contract_columns[name]:
            raise ArtifactIntegrityError(f"manifest columns differ from contract: {name}")
    body = {
        "format": "nport-fixed-income-local-postgres/v3",
        "identity": asdict(identity),
        "contract_digest": identity.contract_digest,
        "worker_sha": worker_sha,
        "engine": {
            "kind": "postgresql-local",
            "postgres_major": 18,
            "postgres_image_digest": resource_config.postgres_image_digest,
            "postgres_server_fingerprint": resource_config.postgres_server_fingerprint,
            "oracle_sha256": sha256_file(LOCAL_ORACLE_PATH),
        },
        "resources": asdict(resource_config),
        "inputs": {
            name: {"filename": path.name, "sha256": sha256_file(path)}
            for name, path in sorted(source_files.items())
        },
        "outputs": dict(sorted(outputs.items())),
    }
    body["manifest_sha256"] = hashlib.sha256(canonical_json(body).encode()).hexdigest()
    return body


def verify_manifest(
    manifest: Mapping[str, Any],
    files: Mapping[str, Path],
    *,
    identity: BuildIdentity | None = None,
) -> None:
    unsigned = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    if (
        manifest.get("manifest_sha256")
        != hashlib.sha256(canonical_json(unsigned).encode()).hexdigest()
    ):
        raise ArtifactIntegrityError("manifest hash mismatch")
    if manifest.get("format") != "nport-fixed-income-local-postgres/v3":
        raise ArtifactIntegrityError("unexpected manifest format")
    manifest_identity = manifest.get("identity")
    if not isinstance(manifest_identity, Mapping):
        raise ArtifactIntegrityError("manifest identity is missing")
    BuildIdentity(**manifest_identity).validate()
    if identity and manifest_identity != asdict(identity):
        raise ArtifactIntegrityError("manifest identity mismatch")
    if manifest.get("contract_digest") != CONTRACT_DIGEST:
        raise ArtifactIntegrityError("manifest contract digest mismatch")
    worker_sha = manifest.get("worker_sha")
    if not isinstance(worker_sha, str) or len(worker_sha) not in (40, 64) or any(
        char not in "0123456789abcdef" for char in worker_sha
    ):
        raise ArtifactIntegrityError("manifest worker SHA is invalid")
    engine = manifest.get("engine")
    if not isinstance(engine, Mapping) or (
        engine.get("kind") != "postgresql-local"
        or engine.get("postgres_major") != 18
        or engine.get("oracle_sha256") != APPROVED_LOCAL_ORACLE_SHA256
        or not isinstance(engine.get("postgres_image_digest"), str)
        or not engine["postgres_image_digest"].startswith("sha256:")
        or len(engine["postgres_image_digest"]) != 71
        or not engine.get("postgres_server_fingerprint")
    ):
        raise ArtifactIntegrityError("manifest does not attest the approved PostgreSQL 18 oracle")
    resources = manifest.get("resources")
    if not isinstance(resources, Mapping):
        raise ArtifactIntegrityError("manifest resources are missing")
    ResourceConfig(**resources).validate(local=True)
    if set(manifest.get("inputs", {})) != set(SOURCE_RELATIONS):
        raise ArtifactIntegrityError("manifest input set is not exact")
    if set(manifest.get("outputs", {})) != set(PUBLISHED_RELATIONS):
        raise ArtifactIntegrityError("manifest output set is not exact")
    contract_columns = _published_columns()
    if set(files) != set(PUBLISHED_RELATIONS):
        raise ArtifactIntegrityError("payload file set is not exact")
    for name, path in files.items():
        output = manifest["outputs"].get(name, {})
        if output.get("filename") != path.name or path.name != f"{name}.tsv.gz":
            raise ArtifactIntegrityError(f"manifest payload filename is invalid: {name}")
        if not isinstance(output.get("count"), int) or output["count"] < 0:
            raise ArtifactIntegrityError(f"manifest output count is invalid: {name}")
        if tuple(output.get("columns", ())) != contract_columns[name]:
            raise ArtifactIntegrityError(f"manifest columns differ from contract: {name}")
        if output.get("sha256") != sha256_file(path):
            raise ArtifactIntegrityError(f"payload sha256 mismatch: {name}")
        with gzip.open(path, "rt", encoding="utf-8", newline="") as payload:
            header = payload.readline().rstrip("\r\n").split("\t")
        if tuple(header) != contract_columns[name]:
            raise ArtifactIntegrityError(f"payload header differs from contract: {name}")


def _expected_counts(manifest: Mapping[str, Any]) -> dict[str, int]:
    """The per-relation row counts the manifest attests (already contract-checked).

    Over PUBLISHED_RELATIONS, i.e. the coverage rollup is INCLUDED, deliberately:
    it is part of the publication and it is the relation the reader actually
    consumes, so a storage closure that omitted it would attest everything
    except what is served.
    """
    return {relation: int(manifest["outputs"][relation]["count"]) for relation in PUBLISHED_RELATIONS}


def _verify_storage_requested(explicit: bool | None) -> bool:
    """Whether this run must re-count the relations instead of reading the closure.

    ``NPORT_FI_VERIFY_STORAGE`` is the operator switch for a restore, a DR drill
    or an audit; the explicit argument wins when given.
    """
    if explicit is not None:
        return explicit
    return os.getenv("NPORT_FI_VERIFY_STORAGE", "").strip().lower() in {"1", "true", "yes", "on"}


def _recorded_closure(cursor: Any, publication_id: str) -> tuple[str, dict[str, int]] | None:
    cursor.execute(
        "SELECT manifest_sha256, relation_counts FROM nport_fixed_income_publication_closures "
        "WHERE publication_id=%s",
        (publication_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0], {str(k): int(v) for k, v in dict(row[1]).items()}


def _record_closure(cursor: Any, publication_id: str, manifest_hash: str,
                    counts: Mapping[str, int]) -> None:
    """Persist the storage closure for a validated publication.

    ``ON CONFLICT DO NOTHING`` because the closure is immutable by trigger: a
    concurrent replay that recorded it first is the same fact, not a conflict.
    """
    cursor.execute(
        "INSERT INTO nport_fixed_income_publication_closures"
        "(publication_id, manifest_sha256, relation_counts) VALUES(%s,%s,%s::jsonb) "
        "ON CONFLICT (publication_id) DO NOTHING",
        (publication_id, manifest_hash, canonical_json(dict(counts))),
    )


def _count_relations(cursor: Any, publication_id: str, expected: Mapping[str, int]) -> None:
    """Full storage verification: one count per published relation, all must match.

    Published, not contract: the coverage rollup is counted here too (see
    ``_expected_counts``).
    """
    for relation in PUBLISHED_RELATIONS:
        cursor.execute(
            f"SELECT count(*) FROM {relation} WHERE publication_id=%s", (publication_id,)
        )
        if cursor.fetchone()[0] != expected[relation]:
            raise PublicationConflictError(f"matching manifest has divergent count: {relation}")


def publish_artifact(
    *,
    connection: psycopg.Connection[Any],
    artifact_dir: Path,
    identity: BuildIdentity,
    resource_config: ResourceConfig,
    product: str = "nport_fixed_income_features_v1",
    failure_after_relation: str | None = None,
    verify_storage: bool | None = None,
) -> None:
    """Copy a fully verified artifact; never invoke raw relations or the builder here.

    ``verify_storage`` forces the full per-relation recount on an idempotent
    replay (default: the ``NPORT_FI_VERIFY_STORAGE`` environment switch). The
    normal replay path re-proves storage from the recorded closure instead --
    see ``_record_closure`` and the closure DDL for why that is equivalent."""
    artifact_dir = Path(artifact_dir)
    files = {name: artifact_dir / f"{name}.tsv.gz" for name in PUBLISHED_RELATIONS}
    manifest = json.loads((artifact_dir / "manifest.json").read_text(encoding="utf-8"))
    verify_manifest(manifest, files, identity=identity)
    manifest_hash = manifest["manifest_sha256"]
    columns = _published_columns()
    with connection.transaction(), connection.cursor() as cursor:
        _set_client_safe_timeouts(cursor, resource_config)
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (product,)
        )
        existing_sql = "SELECT manifest_sha256 FROM nport_fixed_income_publication_manifests WHERE publication_id=%s"
        cursor.execute(existing_sql, (identity.target_publication_id,))
        existing = cursor.fetchone()
        if existing:
            if existing[0] == manifest_hash:
                cursor.execute(
                    """SELECT p.lifecycle_state,c.publication_id
                    FROM sec_derived_publications p
                    LEFT JOIN sec_derived_current_pointers c
                      ON c.product=p.product AND c.publication_id=p.publication_id
                    WHERE p.publication_id=%s""",
                    (identity.target_publication_id,),
                )
                if cursor.fetchone() != ("validated", uuid.UUID(identity.target_publication_id)):
                    raise PublicationConflictError("matching manifest is not the validated current target")
                expected = _expected_counts(manifest)
                # Storage re-proof. The published relations (the eight contract
                # ones plus the coverage rollup) are frozen for a validated
                # publication -- every UPDATE/DELETE is rejected by the row guards,
                # an INSERT needs the parent 'prepared', and TRUNCATE is refused --
                # so a closure recorded while the publication was already validated
                # remains true. Reading it is ONE indexed row instead of nine full
                # counts, one of them over the metric coverage relation.
                # No closure (a publication from before this record existed, or a
                # forced audit) falls back to the counts and then records one, so
                # the cost is paid once, never per replay.
                closure = None if _verify_storage_requested(verify_storage) else _recorded_closure(
                    cursor, identity.target_publication_id)
                if closure is not None:
                    recorded_hash, recorded_counts = closure
                    if recorded_hash != manifest_hash or recorded_counts != expected:
                        raise PublicationConflictError(
                            "recorded storage closure diverges from the matching manifest")
                    return
                _count_relations(cursor, identity.target_publication_id, expected)
                _record_closure(cursor, identity.target_publication_id, manifest_hash, expected)
                return
            raise PublicationConflictError(
                "target publication already has a distinct manifest"
            )
        _provenance(cursor, identity)
        cursor.execute(
            "SELECT COALESCE(max(publication_version),0)+1 FROM sec_derived_publications WHERE product=%s",
            (product,),
        )
        version = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO sec_derived_publications(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) VALUES(%s,%s,%s,%s,%s,%s)",
            (
                identity.target_publication_id,
                product,
                version,
                identity.source_run_id,
                identity.source_package_id,
                manifest_hash,
            ),
        )
        cursor.execute(
            "INSERT INTO nport_fixed_income_feature_builds(publication_id,source_holdings_publication_id,as_of_date) VALUES(%s,%s,%s)",
            (
                identity.target_publication_id,
                identity.source_publication_id,
                identity.as_of_date,
            ),
        )
        for relation, path in files.items():
            quoted = ",".join(f'"{name}"' for name in columns[relation])
            stage = f"nport_fi_stage_{PUBLISHED_RELATIONS.index(relation)}"
            cursor.execute(
                f"CREATE TEMP TABLE {stage} AS SELECT {quoted} FROM {relation} WITH NO DATA"
            )
            with (
                gzip.open(path, "rb") as stream,
                cursor.copy(
                    f"COPY {stage} ({quoted}) FROM STDIN WITH (FORMAT text, HEADER true)"
                ) as copy,
            ):
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    copy.write(chunk)
            key_columns = ",".join(
                f'"{name}"' for name in _published_keys()[relation]
            )
            cursor.execute(
                f"SELECT count(*),count(DISTINCT ({key_columns})),"
                f"count(*) FILTER (WHERE publication_id<>%s) FROM {stage}",
                (identity.target_publication_id,),
            )
            count, distinct, wrong_publication = cursor.fetchone()
            if (
                count != manifest["outputs"][relation]["count"]
                or count != distinct
                or wrong_publication != 0
            ):
                raise ArtifactIntegrityError(f"invalid stage count/keys: {relation}")
            cursor.execute(
                f"INSERT INTO {relation} ({quoted}) SELECT {quoted} FROM {stage}"
            )
            if cursor.rowcount != count:
                raise PublicationConflictError(
                    f"unexpected target conflict: {relation}"
                )
            if relation == failure_after_relation:
                raise RuntimeError(f"injected publish failure after {relation}")
        cursor.execute(
            "INSERT INTO nport_fixed_income_publication_manifests(publication_id,manifest_sha256,manifest) VALUES(%s,%s,%s::jsonb)",
            (identity.target_publication_id, manifest_hash, canonical_json(manifest)),
        )
        cursor.execute(
            "SELECT sec_validate_derived_publication(%s)",
            (identity.target_publication_id,),
        )
        # Record the storage closure AFTER validation (its guard demands a
        # validated publication) and inside the same transaction that wrote the
        # rows: from here on the row guards refuse every further write, so these
        # counts -- already asserted against the staged payload above -- are final.
        # The first idempotent replay is therefore O(1) too, never one full pass.
        _record_closure(
            cursor, identity.target_publication_id, manifest_hash, _expected_counts(manifest)
        )
        cursor.execute(
            "SELECT sec_set_current_derived_publication(%s,%s)",
            (product, identity.target_publication_id),
        )
